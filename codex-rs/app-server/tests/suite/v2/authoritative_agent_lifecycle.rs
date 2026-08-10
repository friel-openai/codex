use super::checkpoint_test_support::test_segment_state_checkpoint;
use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::TestAppServerBuilder;
use app_test_support::write_models_cache;
use chrono::Utc;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CollabAgentStatus;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::SubAgentActivityKind;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::CompactedItem;
use codex_protocol::protocol::EventMsg as CoreEventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionSource as CoreSessionSource;
use codex_protocol::protocol::SubAgentActivityEvent as CoreSubAgentActivityEvent;
use codex_protocol::protocol::SubAgentActivityKind as CoreSubAgentActivityKind;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use codex_state::StateRuntime;
use codex_state::ThreadGoalStatus;
use codex_state::ThreadMetadataBuilder;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::FreezeRolloutSegmentParams;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadStore;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use tempfile::TempDir;
use tokio::time::timeout;
use uuid::Uuid;

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const HISTORICAL_SUBAGENT_ACTIVITY_COUNT: usize = 4_096;
const COLD_DESCENDANT_COUNT: usize = 4_096;
const COLD_BRANCH_COUNT: usize = 64;
const COLD_LEAVES_PER_BRANCH: usize = 63;
const CHECKPOINT_ROTATION_COUNT: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct NormalizedAgentMember {
    id: String,
    parent_id: String,
    path: String,
    status: String,
}

fn app_server_builder(codex_home: &Path) -> TestAppServerBuilder {
    let builder = TestAppServer::builder()
        .with_codex_home(codex_home)
        .without_managed_config();
    match std::env::var_os("FRODEX_SUBAGENT_LIFECYCLE_APP_SERVER") {
        Some(program) => builder
            .with_program(Path::new(&program))
            .with_plugin_startup_tasks(),
        None => builder,
    }
}

async fn list_app_members(
    app: &mut TestAppServer,
    root_thread_id: ThreadId,
) -> Result<Vec<NormalizedAgentMember>> {
    let response: ThreadListResponse = app
        .request(|request_id| ClientRequest::ThreadList {
            request_id,
            params: ThreadListParams {
                cursor: None,
                limit: Some(200),
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                section_id: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
                parent_thread_id: None,
                ancestor_thread_id: Some(root_thread_id.to_string()),
            },
        })
        .await?;
    assert_eq!(response.next_cursor, None);
    response
        .data
        .into_iter()
        .map(|thread| {
            let path = match thread.source {
                SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    agent_path: Some(agent_path),
                    ..
                }) => agent_path.to_string(),
                source => anyhow::bail!("current member had unexpected source: {source:?}"),
            };
            let status = match thread
                .agent_status
                .context("relation member must include agentStatus")?
            {
                CollabAgentStatus::PendingInit => "pending_init",
                CollabAgentStatus::Running => "running",
                CollabAgentStatus::Interrupted => "interrupted",
                CollabAgentStatus::Completed => "completed",
                CollabAgentStatus::Errored => "errored",
                CollabAgentStatus::Shutdown => "shutdown",
                CollabAgentStatus::NotFound => "not_found",
            };
            Ok(NormalizedAgentMember {
                id: thread.id,
                parent_id: thread
                    .parent_thread_id
                    .context("relation member must include parentThreadId")?,
                path,
                status: status.to_string(),
            })
        })
        .collect()
}

fn normalize_model_members(output: &Value) -> Result<Vec<NormalizedAgentMember>> {
    let mut members = output["agents"]
        .as_array()
        .context("list_agents must return agents")?
        .iter()
        .map(|agent| {
            Ok(NormalizedAgentMember {
                id: agent["agent_id"]
                    .as_str()
                    .context("list_agents agent_id")?
                    .to_string(),
                parent_id: agent["parent_agent_id"]
                    .as_str()
                    .context("list_agents parent_agent_id")?
                    .to_string(),
                path: agent["agent_name"]
                    .as_str()
                    .context("list_agents agent_name")?
                    .to_string(),
                status: match &agent["agent_status"] {
                    Value::String(status) => status.clone(),
                    Value::Object(status) if status.contains_key("completed") => {
                        "completed".to_string()
                    }
                    Value::Object(status) if status.contains_key("errored") => {
                        "errored".to_string()
                    }
                    status => anyhow::bail!("unexpected list_agents status: {status}"),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    members.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(members)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the test helper mirrors one model tool invocation"
)]
async fn invoke_model_tool(
    app: &mut TestAppServer,
    server: &wiremock::MockServer,
    thread_id: &str,
    prompt: &str,
    call_id: &str,
    namespace: &str,
    tool_name: &str,
    arguments: Value,
) -> Result<Value> {
    let arguments = serde_json::to_string(&arguments)?;
    let prompt_match = prompt.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(prompt_match.as_str())
        },
        responses::sse(vec![
            responses::ev_response_created(&format!("{call_id}-request")),
            responses::ev_function_call_with_namespace(call_id, namespace, tool_name, &arguments),
            responses::ev_completed(&format!("{call_id}-request")),
        ]),
    )
    .await;
    let call_id_match = call_id.to_string();
    let result_request = responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(call_id_match.as_str())
        },
        responses::sse(vec![
            responses::ev_response_created(&format!("{call_id}-complete")),
            responses::ev_assistant_message(
                &format!("{call_id}-message"),
                &format!("completed {tool_name}"),
            ),
            responses::ev_completed(&format!("{call_id}-complete")),
        ]),
    )
    .await;

    timeout(
        RPC_TIMEOUT,
        app.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: prompt.to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    let output = result_request
        .function_call_output_text(call_id)
        .with_context(|| format!("missing tool output for {call_id}"))?;
    Ok(serde_json::from_str(&output)?)
}

async fn list_model_members(
    app: &mut TestAppServer,
    server: &wiremock::MockServer,
    thread_id: &str,
    phase: &str,
) -> Result<Vec<NormalizedAgentMember>> {
    let output = invoke_model_tool(
        app,
        server,
        thread_id,
        &format!("authoritative membership list {phase}"),
        &format!("authoritative-list-{phase}"),
        "collaboration",
        "list_agents",
        json!({"limit": 25}),
    )
    .await?;
    assert_eq!(output["next_cursor"], Value::Null);
    normalize_model_members(&output)
}

async fn loaded_descendants(
    app: &mut TestAppServer,
    root_thread_id: ThreadId,
) -> Result<Vec<String>> {
    let request_id = app
        .send_thread_loaded_list_request(ThreadLoadedListParams {
            cursor: None,
            limit: None,
            ancestor_thread_id: Some(root_thread_id.to_string()),
        })
        .await?;
    let response: ThreadLoadedListResponse =
        timeout(RPC_TIMEOUT, app.read_response(request_id)).await??;
    Ok(response.data)
}

fn count_subagent_activity(response: &ThreadForkResponse) -> usize {
    response
        .thread
        .turns
        .iter()
        .flat_map(|turn| turn.items.iter())
        .filter(|item| matches!(item, ThreadItem::SubAgentActivity { .. }))
        .count()
}

fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

#[tokio::test]
async fn current_agent_relation_uses_canonical_fallback_for_corrupt_indexed_metadata() -> Result<()>
{
    const ROOT_PROMPT: &str = "spawn one agent before corrupting indexed display metadata";
    const CHILD_PROMPT: &str = "complete the corrupt metadata parity fixture";
    const SPAWN_CALL_ID: &str = "corrupt-metadata-spawn";

    let codex_home = TempDir::new()?;
    let server = responses::start_mock_server().await;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .with_extra_config("[features.multi_agent_v2]\nenabled = true")
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;
    let spawn_args = serde_json::to_string(&json!({
        "message": CHILD_PROMPT,
        "task_name": "corrupt_metadata_worker",
        "fork_turns": "none",
    }))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| String::from_utf8_lossy(&request.body).contains(ROOT_PROMPT),
        responses::sse(vec![
            responses::ev_response_created("corrupt-metadata-root-spawn"),
            responses::ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                "collaboration",
                "spawn_agent",
                &spawn_args,
            ),
            responses::ev_completed("corrupt-metadata-root-spawn"),
        ]),
    )
    .await;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(CHILD_PROMPT) && !body.contains(SPAWN_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("corrupt-metadata-child"),
            responses::ev_assistant_message(
                "corrupt-metadata-child-message",
                "corrupt metadata worker complete",
            ),
            responses::ev_completed("corrupt-metadata-child"),
        ]),
    )
    .await;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(SPAWN_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("corrupt-metadata-root-complete"),
            responses::ev_assistant_message("corrupt-metadata-root-message", "worker started"),
            responses::ev_completed("corrupt-metadata-root-complete"),
        ]),
    )
    .await;

    let mut app = app_server_builder(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread: root, .. } = app
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.4".to_string()),
            ..Default::default()
        })
        .await?;
    let root_thread_id = ThreadId::from_string(&root.id)?;
    let turn: TurnStartResponse = app
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: root.id.clone(),
                input: vec![UserInput::Text {
                    text: ROOT_PROMPT.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    let child_thread_id = timeout(RPC_TIMEOUT, async {
        loop {
            let completed: ItemCompletedNotification =
                app.read_notification("item/completed").await?;
            if let ThreadItem::SubAgentActivity {
                id,
                kind: SubAgentActivityKind::Started,
                agent_thread_id,
                ..
            } = completed.item
                && id == SPAWN_CALL_ID
            {
                return Ok::<ThreadId, anyhow::Error>(ThreadId::from_string(&agent_thread_id)?);
            }
        }
    })
    .await??;
    let _: TurnCompletedNotification = timeout(RPC_TIMEOUT, async {
        loop {
            let completed: TurnCompletedNotification =
                app.read_notification("turn/completed").await?;
            if completed.turn.id == turn.turn.id {
                return Ok::<_, anyhow::Error>(completed);
            }
        }
    })
    .await??;
    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let pool = sqlite.open_read_write_pool(&sqlite.state_db_path()).await?;
    sqlx::query("UPDATE threads SET history_mode = 'corrupt' WHERE id = ?")
        .bind(child_thread_id.to_string())
        .execute(&pool)
        .await?;

    let model_members = list_model_members(&mut app, &server, &root.id, "corrupt-metadata").await?;
    let mut app_members = list_app_members(&mut app, root_thread_id).await?;
    app_members.sort_by(|left, right| left.path.cmp(&right.path));
    assert_eq!(app_members, model_members);
    assert_eq!(app_members.len(), 1);
    assert_eq!(app_members[0].id, child_thread_id.to_string());
    Ok(())
}

#[tokio::test]
#[ignore = "manual authoritative subagent lifecycle E2E"]
async fn authoritative_agent_lifecycle_survives_fork_recursive_close_and_restart_with_large_history()
-> Result<()> {
    const ROOT_PROMPT: &str = "create the historical worker and list it immediately";
    const CHILD_PROMPT: &str = "remain active until the immediate membership comparison finishes";
    const SPAWN_CALL_ID: &str = "authoritative-spawn-historical-worker";
    const INITIAL_LIST_CALL_ID: &str = "authoritative-initial-list";

    let temp_home = TempDir::new()?;
    let explicit_output_root =
        std::env::var_os("FRODEX_SUBAGENT_LIFECYCLE_OUTPUT_ROOT").map(PathBuf::from);
    let (codex_home, report_path) = match explicit_output_root {
        Some(output_root) => {
            let run_root = output_root.join(format!("run-{}", Uuid::new_v4()));
            let codex_home = run_root.join("codex-home");
            std::fs::create_dir_all(&codex_home)?;
            (codex_home, Some(run_root.join("report.json")))
        }
        None => (temp_home.path().to_path_buf(), None),
    };

    let server = responses::start_mock_server().await;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .with_extra_config(
            "[features]\ngoals = true\ngoal_supervisor = false\n\n[features.multi_agent_v2]\nenabled = true",
        )
        .write(&codex_home)?;
    write_models_cache(&codex_home)?;

    let spawn_args = serde_json::to_string(&json!({
        "message": CHILD_PROMPT,
        "task_name": "historical_worker",
        "fork_turns": "none",
    }))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| String::from_utf8_lossy(&request.body).contains(ROOT_PROMPT),
        responses::sse(vec![
            responses::ev_response_created("authoritative-root-spawn"),
            responses::ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                "collaboration",
                "spawn_agent",
                &spawn_args,
            ),
            responses::ev_completed("authoritative-root-spawn"),
        ]),
    )
    .await;
    responses::mount_response_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(CHILD_PROMPT) && !body.contains(SPAWN_CALL_ID)
        },
        responses::sse_response(responses::sse(vec![
            responses::ev_response_created("authoritative-worker-response"),
            responses::ev_assistant_message(
                "authoritative-worker-message",
                "historical worker complete",
            ),
            responses::ev_completed("authoritative-worker-response"),
        ]))
        .set_delay(Duration::from_secs(3)),
    )
    .await;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(SPAWN_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("authoritative-root-list"),
            responses::ev_function_call_with_namespace(
                INITIAL_LIST_CALL_ID,
                "collaboration",
                "list_agents",
                "{}",
            ),
            responses::ev_completed("authoritative-root-list"),
        ]),
    )
    .await;
    let initial_list_result = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(INITIAL_LIST_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("authoritative-root-finished"),
            responses::ev_assistant_message(
                "authoritative-root-message",
                "initial membership captured",
            ),
            responses::ev_completed("authoritative-root-finished"),
        ]),
    )
    .await;

    let initialize_started = Instant::now();
    let mut app = app_server_builder(&codex_home).build_initialized().await?;
    let first_initialize_ms = millis(initialize_started.elapsed());
    let ThreadStartResponse { thread: root, .. } = app
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.4".to_string()),
            ..Default::default()
        })
        .await?;
    let root_thread_id = ThreadId::from_string(&root.id)?;
    let ThreadStartResponse {
        thread: checkpoint_probe,
        ..
    } = app
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.4".to_string()),
            ..Default::default()
        })
        .await?;
    let checkpoint_probe_thread_id = ThreadId::from_string(&checkpoint_probe.id)?;
    assert_eq!(
        list_model_members(
            &mut app,
            &server,
            &checkpoint_probe.id,
            "checkpoint-probe-initialize",
        )
        .await?,
        Vec::new()
    );
    let turn: TurnStartResponse = app
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: root.id.clone(),
                input: vec![UserInput::Text {
                    text: ROOT_PROMPT.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    let worker_id = timeout(RPC_TIMEOUT, async {
        loop {
            let completed: ItemCompletedNotification =
                app.read_notification("item/completed").await?;
            if let ThreadItem::SubAgentActivity {
                id,
                kind: SubAgentActivityKind::Started,
                agent_thread_id,
                ..
            } = completed.item
                && id == SPAWN_CALL_ID
            {
                return Ok::<ThreadId, anyhow::Error>(ThreadId::from_string(&agent_thread_id)?);
            }
        }
    })
    .await??;
    let immediate_app_members = list_app_members(&mut app, root_thread_id).await?;
    assert_eq!(immediate_app_members.len(), 1);
    assert_eq!(immediate_app_members[0].id, worker_id.to_string());
    let initial_model_output = timeout(RPC_TIMEOUT, async {
        loop {
            if let Some(output) =
                initial_list_result.function_call_output_text(INITIAL_LIST_CALL_ID)
            {
                return Ok::<Value, anyhow::Error>(serde_json::from_str(&output)?);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let initial_model_members = normalize_model_members(&initial_model_output)?;
    let initial_app_members = list_app_members(&mut app, root_thread_id).await?;
    assert_eq!(initial_model_members, initial_app_members);
    timeout(RPC_TIMEOUT, async {
        loop {
            let completed: TurnCompletedNotification =
                app.read_notification("turn/completed").await?;
            if completed.thread_id == root.id && completed.turn.id == turn.turn.id {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;
    tokio::time::sleep(Duration::from_secs(4)).await;
    timeout(RPC_TIMEOUT, app.shutdown_gracefully()).await??;
    drop(app);

    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.as_path().abs());
    let state = StateRuntime::init(sqlite.clone(), "mock_provider".to_string()).await?;
    let root_rollout_path = root.path.clone().context("root rollout path")?;
    let historical_transcript_seed_started = Instant::now();
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.clone(),
            sqlite,
            default_model_provider_id: "mock_provider".to_string(),
        },
        Some(state.clone()),
    );
    store
        .resume_thread(ResumeThreadParams {
            thread_id: root_thread_id,
            rollout_path: Some(root_rollout_path),
            history: None,
            include_archived: false,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(codex_home.clone()),
                model_provider: "mock_provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await?;
    let checkpoint_probe_rollout_path = checkpoint_probe
        .path
        .clone()
        .context("checkpoint probe rollout path")?;
    store
        .resume_thread(ResumeThreadParams {
            thread_id: checkpoint_probe_thread_id,
            rollout_path: Some(checkpoint_probe_rollout_path),
            history: None,
            include_archived: false,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(codex_home.clone()),
                model_provider: "mock_provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await?;
    let historical_turn_id = "authoritative-large-historical-transcript";
    let historical_started_at = Utc::now();
    let mut historical_items = Vec::with_capacity(HISTORICAL_SUBAGENT_ACTIVITY_COUNT + 2);
    historical_items.push(RolloutItem::EventMsg(CoreEventMsg::TurnStarted(
        TurnStartedEvent {
            turn_id: historical_turn_id.to_string(),
            trace_id: None,
            started_at: Some(historical_started_at.timestamp()),
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        },
    )));
    for index in 0..HISTORICAL_SUBAGENT_ACTIVITY_COUNT {
        let agent_path = AgentPath::root()
            .join(&format!("historical_transcript_{index:04}"))
            .map_err(anyhow::Error::msg)?;
        historical_items.push(RolloutItem::EventMsg(CoreEventMsg::SubAgentActivity(
            CoreSubAgentActivityEvent {
                event_id: format!("historical-transcript-{index:04}"),
                occurred_at_ms: historical_started_at.timestamp_millis(),
                agent_thread_id: ThreadId::new(),
                agent_path,
                kind: CoreSubAgentActivityKind::Started,
            },
        )));
    }
    historical_items.push(RolloutItem::EventMsg(CoreEventMsg::TurnComplete(
        TurnCompleteEvent {
            turn_id: historical_turn_id.to_string(),
            last_agent_message: None,
            error: None,
            started_at: Some(historical_started_at.timestamp()),
            completed_at: Some(Utc::now().timestamp()),
            duration_ms: Some(0),
            time_to_first_token_ms: None,
        },
    )));
    store
        .append_items(AppendThreadItemsParams {
            thread_id: checkpoint_probe_thread_id,
            items: historical_items.clone(),
        })
        .await?;
    store.flush_thread(checkpoint_probe_thread_id).await?;
    let checkpoint_rotation_started = Instant::now();
    let first_window_id = Uuid::now_v7();
    let mut previous_window_id = None;
    let mut checkpoint_predecessor_paths = Vec::with_capacity(CHECKPOINT_ROTATION_COUNT);
    for rotation_index in 0..CHECKPOINT_ROTATION_COUNT {
        let window_id = Uuid::now_v7();
        let checkpoint = test_segment_state_checkpoint(
            CompactedItem {
                message: String::new(),
                replacement_history: Some(Vec::new()),
                window_number: Some(u64::try_from(rotation_index + 1)?),
                first_window_id: Some(first_window_id.to_string()),
                previous_window_id: previous_window_id.map(|id: Uuid| id.to_string()),
                window_id: Some(window_id.to_string()),
                segment_state_checkpoint: None,
            },
            /*previous_turn_settings*/ None,
            /*world_state*/ None,
            /*reference_context*/ None,
        )?;
        let frozen = store
            .freeze_thread_segment(
                checkpoint_probe_thread_id,
                FreezeRolloutSegmentParams::rotate(checkpoint),
            )
            .await?;
        checkpoint_predecessor_paths.push(frozen.reference.rollout_path);
        previous_window_id = Some(window_id);
    }
    assert_eq!(
        checkpoint_predecessor_paths.len(),
        CHECKPOINT_ROTATION_COUNT
    );
    let checkpoint_rotation_ms = millis(checkpoint_rotation_started.elapsed());
    store.shutdown_thread(checkpoint_probe_thread_id).await?;

    store
        .append_items(AppendThreadItemsParams {
            thread_id: root_thread_id,
            items: historical_items,
        })
        .await?;
    store.flush_thread(root_thread_id).await?;
    store.shutdown_thread(root_thread_id).await?;
    let historical_transcript_seed_ms = millis(historical_transcript_seed_started.elapsed());

    let seed_started = Instant::now();
    let created_at = Utc::now();
    let worker_path = AgentPath::try_from("/root/historical_worker").map_err(anyhow::Error::msg)?;
    let mut cold_ids = Vec::with_capacity(COLD_DESCENDANT_COUNT);
    for branch in 0..COLD_BRANCH_COUNT {
        let branch_id = ThreadId::new();
        let branch_path = worker_path
            .join(&format!("cold_{branch:02}"))
            .map_err(anyhow::Error::msg)?;
        let mut branch_builder = ThreadMetadataBuilder::new(
            branch_id,
            codex_home
                .join("historical")
                .join(format!("{branch_id}.jsonl")),
            created_at,
            CoreSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_id,
                depth: 2,
                agent_path: Some(branch_path.clone()),
                agent_nickname: Some(format!("cold-{branch:02}")),
                agent_role: None,
            }),
        );
        branch_builder.model_provider = Some("mock_provider".to_string());
        branch_builder.cwd = codex_home.clone();
        branch_builder.cli_version = Some("0.0.0".to_string());
        state
            .upsert_thread(&branch_builder.build("mock_provider"))
            .await?;
        state
            .upsert_thread_spawn_edge(worker_id, branch_id, DirectionalThreadSpawnEdgeStatus::Open)
            .await?;
        cold_ids.push(branch_id);

        for leaf in 0..COLD_LEAVES_PER_BRANCH {
            let leaf_id = ThreadId::new();
            let leaf_path = branch_path
                .join(&format!("leaf_{leaf:02}"))
                .map_err(anyhow::Error::msg)?;
            let mut leaf_builder = ThreadMetadataBuilder::new(
                leaf_id,
                codex_home
                    .join("historical")
                    .join(format!("{leaf_id}.jsonl")),
                created_at,
                CoreSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: branch_id,
                    depth: 3,
                    agent_path: Some(leaf_path),
                    agent_nickname: Some(format!("leaf-{branch:02}-{leaf:02}")),
                    agent_role: None,
                }),
            );
            leaf_builder.model_provider = Some("mock_provider".to_string());
            leaf_builder.cwd = codex_home.clone();
            leaf_builder.cli_version = Some("0.0.0".to_string());
            state
                .upsert_thread(&leaf_builder.build("mock_provider"))
                .await?;
            state
                .upsert_thread_spawn_edge(
                    branch_id,
                    leaf_id,
                    DirectionalThreadSpawnEdgeStatus::Open,
                )
                .await?;
            cold_ids.push(leaf_id);
        }
    }
    assert_eq!(cold_ids.len(), COLD_DESCENDANT_COUNT);
    let unrelated_id = ThreadId::new();
    let unrelated_path =
        AgentPath::try_from("/root/unrelated_historical").map_err(anyhow::Error::msg)?;
    let mut unrelated_builder = ThreadMetadataBuilder::new(
        unrelated_id,
        codex_home
            .join("historical")
            .join(format!("{unrelated_id}.jsonl")),
        created_at,
        CoreSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: root_thread_id,
            depth: 1,
            agent_path: Some(unrelated_path),
            agent_nickname: Some("unrelated-historical".to_string()),
            agent_role: None,
        }),
    );
    unrelated_builder.model_provider = Some("mock_provider".to_string());
    unrelated_builder.cwd = codex_home.clone();
    unrelated_builder.cli_version = Some("0.0.0".to_string());
    state
        .upsert_thread(&unrelated_builder.build("mock_provider"))
        .await?;
    state
        .upsert_thread_spawn_edge(
            root_thread_id,
            unrelated_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await?;
    let cleanup_ids = [worker_id, cold_ids[0], cold_ids[1]];
    let mut cleanup_goal_ids = Vec::new();
    for thread_id in cleanup_ids {
        let goal = state
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "pause this goal when its agent subtree closes",
                ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await?;
        state
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                thread_id,
                &goal.goal_id,
                Some(Utc::now().timestamp_millis() + 60_000),
            )
            .await?;
        state
            .thread_queue()
            .enqueue(thread_id, r#"{"input":"queued before recursive close"}"#)
            .await?;
        cleanup_goal_ids.push((thread_id, goal.goal_id));
    }
    state
        .mark_backfill_complete(/*last_watermark*/ None)
        .await?;
    let open_before = state
        .list_thread_spawn_descendants_with_status(
            root_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await?;
    assert_eq!(open_before.len(), COLD_DESCENDANT_COUNT + 2);
    let seed_ms = millis(seed_started.elapsed());
    state.close().await;

    let mut unavailable_checkpoint_predecessors = Vec::with_capacity(CHECKPOINT_ROTATION_COUNT);
    for (index, predecessor_path) in checkpoint_predecessor_paths.iter().enumerate() {
        let unavailable_path = predecessor_path.with_extension(format!(
            "jsonl.authoritative-checkpoint-unavailable-{index}"
        ));
        std::fs::rename(predecessor_path, &unavailable_path)?;
        unavailable_checkpoint_predecessors.push((predecessor_path.clone(), unavailable_path));
    }
    assert_eq!(
        unavailable_checkpoint_predecessors.len(),
        CHECKPOINT_ROTATION_COUNT
    );

    let checkpoint_initialize_started = Instant::now();
    let mut checkpoint_app = app_server_builder(&codex_home).build_initialized().await?;
    let checkpoint_restart_initialize_ms = millis(checkpoint_initialize_started.elapsed());
    let checkpoint_resume_started = Instant::now();
    let checkpoint_resume_request = checkpoint_app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: checkpoint_probe.id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse = timeout(
        RPC_TIMEOUT,
        checkpoint_app.read_response(checkpoint_resume_request),
    )
    .await??;
    let checkpoint_resume_ms = millis(checkpoint_resume_started.elapsed());
    assert_eq!(
        loaded_descendants(&mut checkpoint_app, checkpoint_probe_thread_id).await?,
        Vec::<String>::new()
    );
    assert_eq!(
        list_app_members(&mut checkpoint_app, checkpoint_probe_thread_id).await?,
        Vec::new()
    );
    for (predecessor_path, unavailable_path) in &unavailable_checkpoint_predecessors {
        assert!(
            !predecessor_path.exists(),
            "latest-state resume unexpectedly restored predecessor {}",
            predecessor_path.display()
        );
        assert!(unavailable_path.exists());
    }
    timeout(RPC_TIMEOUT, checkpoint_app.shutdown_gracefully()).await??;
    drop(checkpoint_app);
    for (predecessor_path, unavailable_path) in &unavailable_checkpoint_predecessors {
        std::fs::rename(unavailable_path, predecessor_path)?;
    }

    let resume_initialize_started = Instant::now();
    let mut app = app_server_builder(&codex_home).build_initialized().await?;
    let restart_initialize_ms = millis(resume_initialize_started.elapsed());
    let resume_started = Instant::now();
    let resume_request = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: root.id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse = timeout(RPC_TIMEOUT, app.read_response(resume_request)).await??;
    let resume_ms = millis(resume_started.elapsed());
    assert_eq!(
        loaded_descendants(&mut app, root_thread_id).await?,
        Vec::<String>::new()
    );
    let app_before_close = list_app_members(&mut app, root_thread_id).await?;
    assert_eq!(app_before_close, Vec::new());
    let model_before_close =
        list_model_members(&mut app, &server, &root.id, "before-close").await?;
    assert_eq!(model_before_close, app_before_close);

    let fork_started = Instant::now();
    let fork_request = app
        .send_thread_fork_request(ThreadForkParams {
            thread_id: root.id.clone(),
            ..Default::default()
        })
        .await?;
    let fork: ThreadForkResponse = timeout(RPC_TIMEOUT, app.read_response(fork_request)).await??;
    let fork_ms = millis(fork_started.elapsed());
    let fork_historical_subagent_activity_count = count_subagent_activity(&fork);
    assert_eq!(
        fork_historical_subagent_activity_count,
        HISTORICAL_SUBAGENT_ACTIVITY_COUNT + 1,
        "fork must retain the synthetic transcript and original worker activity"
    );
    let fork_thread_id = ThreadId::from_string(&fork.thread.id)?;
    let fork_model_members =
        list_model_members(&mut app, &server, &fork.thread.id, "fork-before-close").await?;
    let fork_app_members = list_app_members(&mut app, fork_thread_id).await?;
    assert_eq!(fork_model_members, fork_app_members);
    assert_eq!(fork_app_members, Vec::new());

    const POST_FORK_CHILD_PROMPT: &str = "complete the post-fork worker";
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(POST_FORK_CHILD_PROMPT) && !body.contains("authoritative-post-fork-spawn")
        },
        responses::sse(vec![
            responses::ev_response_created("post-fork-worker-complete"),
            responses::ev_assistant_message(
                "post-fork-worker-message",
                "post-fork worker complete",
            ),
            responses::ev_completed("post-fork-worker-complete"),
        ]),
    )
    .await;
    let post_fork_spawn = invoke_model_tool(
        &mut app,
        &server,
        &fork.thread.id,
        "spawn exactly one worker after the root fork",
        "authoritative-post-fork-spawn",
        "collaboration",
        "spawn_agent",
        json!({
            "message": POST_FORK_CHILD_PROMPT,
            "task_name": "post_fork_worker",
            "fork_turns": "none",
        }),
    )
    .await?;
    assert_eq!(post_fork_spawn["task_name"], "/root/post_fork_worker");
    let post_fork_app_members = timeout(RPC_TIMEOUT, async {
        loop {
            let members = list_app_members(&mut app, fork_thread_id).await?;
            if members.len() == 1 && members[0].status == "completed" {
                return Ok::<Vec<NormalizedAgentMember>, anyhow::Error>(members);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let post_fork_model_members =
        list_model_members(&mut app, &server, &fork.thread.id, "fork-after-spawn").await?;
    assert_eq!(post_fork_model_members, post_fork_app_members);
    assert_eq!(post_fork_app_members.len(), 1);
    assert_eq!(post_fork_app_members[0].parent_id, fork.thread.id);
    assert_eq!(post_fork_app_members[0].path, "/root/post_fork_worker");
    assert_eq!(
        list_app_members(&mut app, root_thread_id).await?,
        Vec::new()
    );

    let close_started = Instant::now();
    let close_output = invoke_model_tool(
        &mut app,
        &server,
        &root.id,
        "permanently close the historical worker subtree",
        "authoritative-close-worker-subtree",
        "frodex",
        "close_agent",
        json!({"target": worker_id.to_string()}),
    )
    .await?;
    let close_ms = millis(close_started.elapsed());
    assert_eq!(close_output["closed_agents"], COLD_DESCENDANT_COUNT + 1);
    assert_eq!(close_output["closed_edges"], COLD_DESCENDANT_COUNT + 1);
    assert_eq!(
        close_output["newly_closed_edges"],
        COLD_DESCENDANT_COUNT + 1
    );
    assert_eq!(close_output["paused_goals"], cleanup_ids.len());
    assert_eq!(close_output["cleared_queued_items"], cleanup_ids.len());
    assert_eq!(close_output["evicted_identities"], 1);
    assert_eq!(
        list_app_members(&mut app, root_thread_id).await?,
        Vec::new()
    );
    assert_eq!(
        loaded_descendants(&mut app, root_thread_id).await?,
        Vec::<String>::new()
    );

    let closed_resume_request = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: worker_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let closed_resume_error: JSONRPCError = timeout(
        RPC_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(closed_resume_request)),
    )
    .await??;
    assert!(
        closed_resume_error
            .error
            .message
            .contains("permanently closed"),
        "unexpected closed-thread resume error: {}",
        closed_resume_error.error.message
    );

    for (method, params) in [
        (
            "thread/goal/set",
            json!({
                "threadId": worker_id.to_string(),
                "objective": "must not reactivate a closed agent",
                "status": "active",
            }),
        ),
        (
            "thread/goal/clear",
            json!({"threadId": worker_id.to_string()}),
        ),
    ] {
        let request_id = app.send_raw_request(method, Some(params)).await?;
        let error: JSONRPCError = timeout(
            RPC_TIMEOUT,
            app.read_stream_until_error_message(RequestId::Integer(request_id)),
        )
        .await??;
        assert!(
            error.error.message.contains("permanently closed"),
            "unexpected {method} error: {}",
            error.error.message
        );
    }
    timeout(RPC_TIMEOUT, app.shutdown_gracefully()).await??;
    drop(app);

    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.as_path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    let open_after = state
        .list_thread_spawn_descendants_with_status(
            root_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await?;
    assert_eq!(open_after, vec![unrelated_id]);
    let closed_after = state
        .list_thread_spawn_descendants_with_status(
            root_thread_id,
            DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await?;
    assert_eq!(closed_after.len(), COLD_DESCENDANT_COUNT + 1);
    for (thread_id, goal_id) in &cleanup_goal_ids {
        assert_eq!(
            state
                .thread_goals()
                .get_thread_goal(*thread_id)
                .await?
                .context("closed thread goal must remain auditable")?
                .status,
            ThreadGoalStatus::Paused
        );
        assert_eq!(
            state
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(*thread_id, goal_id)
                .await?,
            None
        );
        assert_eq!(
            state.thread_queue().list_page(*thread_id, 0, 10).await?,
            Vec::new()
        );
    }
    state.close().await;

    let durability_started = Instant::now();
    let mut app = app_server_builder(&codex_home).build_initialized().await?;
    for thread_id in [&root.id, &fork.thread.id] {
        let resume_request = app
            .send_thread_resume_request(ThreadResumeParams {
                thread_id: thread_id.clone(),
                exclude_turns: true,
                ..Default::default()
            })
            .await?;
        let _: ThreadResumeResponse =
            timeout(RPC_TIMEOUT, app.read_response(resume_request)).await??;
    }
    let durability_resume_ms = millis(durability_started.elapsed());
    let root_after_restart =
        list_model_members(&mut app, &server, &root.id, "after-restart-root").await?;
    let fork_after_restart =
        list_model_members(&mut app, &server, &fork.thread.id, "after-restart-fork").await?;
    assert_eq!(
        root_after_restart,
        list_app_members(&mut app, root_thread_id).await?
    );
    assert_eq!(
        fork_after_restart,
        list_app_members(&mut app, fork_thread_id).await?
    );
    assert_eq!(root_after_restart, Vec::new());
    assert_eq!(fork_after_restart, Vec::new());
    assert_eq!(
        loaded_descendants(&mut app, root_thread_id).await?,
        Vec::<String>::new()
    );
    timeout(RPC_TIMEOUT, app.shutdown_gracefully()).await??;

    let report = json!({
        "test": "authoritative_agent_lifecycle_survives_fork_recursive_close_and_restart_with_large_history",
        "sourceLabel": std::env::var("FRODEX_SOURCE_LABEL").ok(),
        "appServerProgram": std::env::var("FRODEX_SUBAGENT_LIFECYCLE_APP_SERVER").ok(),
        "codexHome": codex_home,
        "rootThreadId": root.id,
        "checkpointProbeThreadId": checkpoint_probe.id,
        "forkThreadId": fork.thread.id,
        "workerThreadId": worker_id,
        "historicalSubAgentActivityItemsSeeded": HISTORICAL_SUBAGENT_ACTIVITY_COUNT,
        "forkHistoricalSubAgentActivityItems": fork_historical_subagent_activity_count,
        "segmentCheckpointRotations": CHECKPOINT_ROTATION_COUNT,
        "unavailablePredecessorsDuringLatestStateResume": unavailable_checkpoint_predecessors.len(),
        "historicalColdDescendants": COLD_DESCENDANT_COUNT,
        "openBeforeClose": open_before.len(),
        "openAfterClose": open_after.len(),
        "closedAfterClose": closed_after.len(),
        "initialModelMembers": initial_model_members,
        "initialAppMembers": initial_app_members,
        "modelBeforeClose": model_before_close,
        "appBeforeClose": app_before_close,
        "forkModelMembers": fork_model_members,
        "forkAppMembers": fork_app_members,
        "postForkModelMembers": post_fork_model_members,
        "postForkAppMembers": post_fork_app_members,
        "closeReport": close_output,
        "timingsMs": {
            "firstInitialize": first_initialize_ms,
            "historicalTranscriptSeed": historical_transcript_seed_ms,
            "segmentCheckpointRotation": checkpoint_rotation_ms,
            "checkpointProbeInitialize": checkpoint_restart_initialize_ms,
            "checkpointProbeResume": checkpoint_resume_ms,
            "seed": seed_ms,
            "restartInitialize": restart_initialize_ms,
            "resume": resume_ms,
            "fork": fork_ms,
            "close": close_ms,
            "durabilityResume": durability_resume_ms,
        },
    });
    if let Some(report_path) = report_path {
        let parent = report_path
            .parent()
            .context("report path must have a parent")?;
        std::fs::create_dir_all(parent)?;
        std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
    }
    println!("E2E_RESULT {}", serde_json::to_string(&report)?);
    Ok(())
}
