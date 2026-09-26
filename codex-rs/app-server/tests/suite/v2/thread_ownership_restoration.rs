//! Ownership changes must retain the original task and its durable configuration.

use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::AskForApproval;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::SandboxMode;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSource;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(20);

/// Cold sources must be found on disk; ephemeral sources must become durable on promotion.
#[derive(Clone, Copy, PartialEq)]
enum SourceState {
    Loaded,
    Stored,
    Ephemeral,
    Descendants,
}

#[test_case::test_case(SourceState::Loaded; "loaded")]
#[test_case::test_case(SourceState::Stored; "stored")]
#[test_case::test_case(SourceState::Ephemeral; "ephemeral")]
#[test_case::test_case(SourceState::Descendants; "descendants_and_ancestor_cycle")]
#[tokio::test]
async fn adoption_and_promotion_preserve_disk_history_and_configuration(
    source_state: SourceState,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    let original_cwd = home.path().join("original-workspace");
    let parent_cwd = home.path().join("parent-workspace");
    std::fs::create_dir(&original_cwd)?;
    std::fs::create_dir(&parent_cwd)?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::Collab)
        .with_extra_config("[features.multi_agent_v2]\nenabled = true\nenable_thread_adoption = true")
        .with_extra_config(&format!("[model_providers.original_provider]\nname = \"Original\"\nbase_url = \"{}/v1\"\nwire_api = \"responses\"\nrequest_max_retries = 0\nstream_max_retries = 0", server.uri()))
        .write(home.path())?;
    let mut client = app(home.path()).await?;
    let original: ThreadStartResponse = client
        .request(|request_id| ClientRequest::ThreadStart {
            request_id,
            params: ThreadStartParams {
                cwd: Some(original_cwd.to_string_lossy().into_owned()),
                model: Some("gpt-5.6-sol".to_string()),
                model_provider: Some("original_provider".to_string()),
                thread_source: Some(ThreadSource::User),
                approval_policy: Some(AskForApproval::OnRequest),
                sandbox: Some(SandboxMode::ReadOnly),
                config: Some(HashMap::from([(
                    "model_reasoning_effort".to_string(),
                    json!("high"),
                )])),
                ephemeral: Some(source_state == SourceState::Ephemeral),
                history_mode: Some(ThreadHistoryMode::Paginated),
                ..Default::default()
            },
        })
        .await?;
    let original_id = original.thread.id.clone();
    assert_eq!(original.thread.source, SessionSource::VsCode);
    answer(&server, &original_id, "original answer").await;
    turn(&mut client, &original_id, "original task input").await?;
    let seeded = if source_state == SourceState::Ephemeral {
        assert!(original.thread.path.is_none());
        original.thread.clone()
    } else {
        let seeded = read(&mut client, &original_id).await?;
        assert_message(&seeded, "original answer");
        seeded
    };
    let initial_bytes = seeded.path.as_ref().map(std::fs::read).transpose()?;
    let descendant = if source_state == SourceState::Descendants {
        let root = original_id.clone();
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                !matches_thread(request, &root)
                    && request
                        .headers
                        .get("session-id")
                        .and_then(|value| value.to_str().ok())
                        == Some(root.as_str())
            },
            responses::sse(vec![
                responses::ev_response_created("leaf"),
                responses::ev_assistant_message("leaf", "descendant answer"),
                responses::ev_completed("leaf"),
            ]),
        )
        .await;
        let spawn = tool(
            &server,
            &original_id,
            "spawn-leaf",
            "collaboration",
            "spawn_agent",
            json!({"task_name": "leaf", "fork_turns": "none", "message": "complete leaf task"}),
        )
        .await;
        turn(
            &mut client,
            &original_id,
            "create a descendant before adoption",
        )
        .await?;
        assert_eq!(
            tool_output(&spawn, "spawn-leaf")?["task_name"],
            "/root/leaf"
        );
        let children = members(&mut client, &original_id).await?;
        assert_eq!(children.len(), 1);
        let leaf = wait_for_answer(&mut client, &children[0], "descendant answer").await?;
        let leaf_bytes = std::fs::read(leaf.path.as_ref().context("persisted descendant")?)?;
        let ancestor_cycle = tool(&server, &leaf.id, "ancestor-cycle", "frodex", "adopt_agent", json!({"existing_thread_id": original_id, "task_name": "ancestor", "message": "must not be delivered"})).await;
        tool(
            &server,
            &original_id,
            "trigger-cycle",
            "collaboration",
            "followup_task",
            json!({"target": "leaf", "message": "try to adopt your ancestor"}),
        )
        .await;
        turn(&mut client, &original_id, "check ancestor-cycle rejection").await?;
        let unchanged_leaf = wait_for_answer(&mut client, &leaf.id, "done").await?;
        let error = ancestor_cycle
            .function_call_output_text("ancestor-cycle")
            .context("ancestor-cycle result")?;
        assert!(error.contains("ancestor"), "{error}");
        assert_eq!(
            members(&mut client, &original_id).await?,
            vec![leaf.id.clone()]
        );
        assert_eq!(
            unchanged_leaf.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_prefix(&leaf, Some(&leaf_bytes), &unchanged_leaf)?;
        answer(&server, &original_id, "acknowledged descendant completion").await;
        turn(
            &mut client,
            &original_id,
            "acknowledge the completed descendant before transferring ownership",
        )
        .await?;
        Some((leaf, leaf_bytes))
    } else {
        None
    };
    if source_state == SourceState::Stored {
        client.shutdown_gracefully().await?;
        client = app(home.path()).await?;
        let loaded: Value = client
            .request(|request_id| ClientRequest::ThreadLoadedList {
                request_id,
                params: Default::default(),
            })
            .await?;
        assert_eq!(loaded["data"], json!([]));
    }
    let parent: ThreadStartResponse = client
        .request(|request_id| ClientRequest::ThreadStart {
            request_id,
            params: ThreadStartParams {
                cwd: Some(parent_cwd.to_string_lossy().into_owned()),
                model: Some("gpt-5.5".to_string()),
                sandbox: Some(SandboxMode::WorkspaceWrite),
                ..Default::default()
            },
        })
        .await?;
    let parent_id = &parent.thread.id;
    assert_ne!(parent.cwd, original.cwd);
    assert_ne!(parent.model, original.model);
    assert_ne!(parent.model_provider, original.model_provider);
    answer(&server, &original_id, "adopted answer").await;
    let adopted_result = tool(
        &server, parent_id, "adopt", "frodex", "adopt_agent",
        json!({"existing_thread_id": original_id, "task_name": "worker", "message": "adopted task input"}),
    ).await;
    turn(&mut client, parent_id, "adopt the original task").await?;
    assert_eq!(
        tool_output(&adopted_result, "adopt")?["task_name"],
        "/root/worker"
    );
    let adopted = wait_for_answer(&mut client, &original_id, "adopted answer").await?;
    assert_eq!(adopted.id, original_id);
    assert_eq!(
        adopted.parent_thread_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert_eq!(adopted.session_id, *parent_id);
    assert_eq!(adopted.cwd, original.cwd);
    assert_eq!(adopted.model_provider, original.model_provider);
    assert_message(&adopted, "original answer");
    assert_prefix(&seeded, initial_bytes.as_deref(), &adopted)?;
    assert_eq!(
        members(&mut client, parent_id).await?,
        vec![original_id.clone()]
    );
    if let Some((leaf, bytes)) = &descendant {
        let adopted_leaf = read(&mut client, &leaf.id).await?;
        assert_eq!(
            adopted_leaf.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(adopted_leaf.session_id, *parent_id);
        assert_prefix(leaf, Some(bytes), &adopted_leaf)?;
        assert_eq!(
            members(&mut client, &original_id).await?,
            vec![leaf.id.clone()]
        );
        answer(&server, &leaf.id, "descendant after adoption").await;
        tool(
            &server,
            parent_id,
            "followup-leaf",
            "collaboration",
            "followup_task",
            json!({"target": "worker/leaf", "message": "continue the existing descendant"}),
        )
        .await;
        turn(
            &mut client,
            parent_id,
            "follow up with the adopted descendant",
        )
        .await?;
        wait_for_answer(&mut client, &leaf.id, "descendant after adoption").await?;
    }

    answer(&server, &original_id, "follow-up answer").await;
    let followup = tool(
        &server,
        parent_id,
        "followup",
        "collaboration",
        "followup_task",
        json!({"target": "worker", "message": "follow-up task input"}),
    )
    .await;
    turn(&mut client, parent_id, "follow up with worker").await?;
    assert_eq!(
        followup.function_call_output_text("followup").as_deref(),
        Some("")
    );
    wait_for_answer(&mut client, &original_id, "follow-up answer").await?;

    let promoted_result = tool(
        &server,
        parent_id,
        "promote",
        "frodex",
        "promote_agent",
        json!({"target": "worker"}),
    )
    .await;
    turn(&mut client, parent_id, "promote worker").await?;
    assert_eq!(
        tool_output(&promoted_result, "promote")?["thread_id"],
        original_id
    );
    let promoted = read(&mut client, &original_id).await?;
    assert_eq!(promoted.id, original_id);
    assert!(!promoted.ephemeral);
    assert_eq!(promoted.parent_thread_id, None);
    assert_eq!(promoted.session_id, original_id);
    assert_eq!(promoted.source, original.thread.source);
    assert_eq!(promoted.thread_source, original.thread.thread_source);
    assert_eq!(promoted.can_accept_direct_input, Some(true));
    assert!(members(&mut client, parent_id).await?.is_empty());
    if let Some((leaf, bytes)) = &descendant {
        let promoted_leaf = read(&mut client, &leaf.id).await?;
        assert_eq!(
            promoted_leaf.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(promoted_leaf.session_id, original_id);
        assert_prefix(leaf, Some(bytes), &promoted_leaf)?;
        assert_message(&promoted_leaf, "descendant after adoption");
        assert_eq!(
            members(&mut client, &original_id).await?,
            vec![leaf.id.clone()]
        );
    }
    assert_prefix(&seeded, initial_bytes.as_deref(), &promoted)?;
    let durable_path = promoted
        .path
        .clone()
        .context("promotion must materialize original task")?;
    let durable_bytes = std::fs::read(&durable_path)?;
    let parent_before = read(&mut client, parent_id).await?;
    answer(&server, &original_id, "independent answer").await;
    turn(&mut client, &original_id, "independent task input").await?;
    assert_eq!(
        read(&mut client, parent_id).await?.turns,
        parent_before.turns
    );
    client.shutdown_gracefully().await?;
    client = app(home.path()).await?;
    let resumed: ThreadResumeResponse = client
        .request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: original_id.clone(),
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(resumed.thread.id, original_id);
    assert_eq!(resumed.thread.path.as_ref(), Some(&durable_path));
    assert_eq!(resumed.thread.parent_thread_id, None);
    assert_eq!(resumed.thread.session_id, original_id);
    assert_eq!(resumed.thread.source, original.thread.source);
    assert_eq!(resumed.thread.thread_source, original.thread.thread_source);
    assert_eq!(resumed.model, original.model);
    assert_eq!(resumed.model_provider, original.model_provider);
    assert_eq!(resumed.cwd, original.cwd);
    assert_eq!(resumed.reasoning_effort, original.reasoning_effort);
    assert_eq!(resumed.approval_policy, original.approval_policy);
    assert_eq!(resumed.sandbox, original.sandbox);
    assert_eq!(
        resumed.active_permission_profile,
        original.active_permission_profile
    );
    for message in [
        "original answer",
        "adopted answer",
        "follow-up answer",
        "independent answer",
    ] {
        assert_message(&resumed.thread, message);
    }
    assert!(std::fs::read(&durable_path)?.starts_with(&durable_bytes));
    if let Some((leaf, bytes)) = &descendant {
        let restarted_leaf = read(&mut client, &leaf.id).await?;
        assert_eq!(
            restarted_leaf.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(restarted_leaf.session_id, original_id);
        assert_prefix(leaf, Some(bytes), &restarted_leaf)?;
        assert_message(&restarted_leaf, "descendant answer");
        assert_message(&restarted_leaf, "descendant after adoption");
    }
    answer(&server, &original_id, "restart answer").await;
    turn(&mut client, &original_id, "input after restart").await?;
    assert_message(&read(&mut client, &original_id).await?, "restart answer");
    client.shutdown_gracefully().await?;
    Ok(())
}

async fn app(home: &Path) -> Result<TestAppServer> {
    TestAppServer::builder()
        .with_codex_home(home)
        .without_auto_env()
        .build_initialized()
        .await
}

#[tokio::test]
async fn promotion_materializes_the_original_ephemeral_child() -> Result<()> {
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.6-sol")
        .enable_feature(Feature::Collab)
        .with_extra_config(
            "[features.multi_agent_v2]\nenabled = true\nenable_thread_adoption = true",
        )
        .write(home.path())?;
    let mut client = app(home.path()).await?;
    let parent: ThreadStartResponse = client
        .request(|request_id| ClientRequest::ThreadStart {
            request_id,
            params: ThreadStartParams {
                ephemeral: Some(true),
                ..Default::default()
            },
        })
        .await?;
    let root = parent.thread.id.clone();
    responses::mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            !matches_thread(request, &root)
                && request
                    .headers
                    .get("session-id")
                    .and_then(|value| value.to_str().ok())
                    == Some(root.as_str())
        },
        responses::sse(vec![
            responses::ev_response_created("ephemeral-child"),
            responses::ev_assistant_message("ephemeral-child", "ephemeral child answer"),
            responses::ev_completed("ephemeral-child"),
        ]),
    )
    .await;
    let spawn = tool(&server, &parent.thread.id, "spawn-ephemeral", "collaboration", "spawn_agent", json!({"task_name": "leaf", "fork_turns": "none", "message": "complete the ephemeral task"})).await;
    turn(&mut client, &parent.thread.id, "spawn an ephemeral child").await?;
    assert_eq!(
        tool_output(&spawn, "spawn-ephemeral")?["task_name"],
        "/root/leaf"
    );
    let children = members(&mut client, &parent.thread.id).await?;
    assert_eq!(children.len(), 1);
    let child_id = &children[0];
    let before: ThreadReadResponse = client
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: child_id.clone(),
                include_turns: false,
            },
        })
        .await?;
    assert!(before.thread.ephemeral);
    assert!(before.thread.path.is_none());
    let promotion = tool(
        &server,
        &parent.thread.id,
        "promote-ephemeral",
        "frodex",
        "promote_agent",
        json!({"target": "leaf"}),
    )
    .await;
    turn(
        &mut client,
        &parent.thread.id,
        "promote the ephemeral child",
    )
    .await?;
    assert_eq!(
        tool_output(&promotion, "promote-ephemeral")?["thread_id"],
        *child_id
    );
    let promoted = read(&mut client, child_id).await?;
    assert_eq!(promoted.id, *child_id);
    assert!(!promoted.ephemeral);
    assert_eq!(promoted.parent_thread_id, None);
    assert_eq!(promoted.session_id, *child_id);
    assert_eq!(promoted.source, SessionSource::Cli);
    assert_eq!(promoted.can_accept_direct_input, Some(true));
    assert_message(&promoted, "ephemeral child answer");
    let path = promoted.path.as_ref().context("promoted child rollout")?;
    let bytes = std::fs::read(path)?;
    answer(&server, child_id, "promoted child direct answer").await;
    turn(&mut client, child_id, "continue independently").await?;
    assert!(members(&mut client, &parent.thread.id).await?.is_empty());
    client.shutdown_gracefully().await?;
    client = app(home.path()).await?;
    let resumed: ThreadResumeResponse = client
        .request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: child_id.clone(),
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(resumed.thread.id, *child_id);
    assert_eq!(resumed.thread.path.as_ref(), Some(path));
    assert_eq!(resumed.thread.session_id, *child_id);
    assert_eq!(resumed.thread.parent_thread_id, None);
    assert_message(&resumed.thread, "ephemeral child answer");
    assert_message(&resumed.thread, "promoted child direct answer");
    assert!(std::fs::read(path)?.starts_with(&bytes));
    client.shutdown_gracefully().await?;
    Ok(())
}

#[tokio::test]
async fn adoption_waits_for_the_active_source_turn_without_interrupting_it() -> Result<()> {
    let (release, held) = tokio::sync::oneshot::channel();
    let (source_server, _) = start_streaming_sse_server(vec![
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![responses::ev_response_created("active-source")]),
            },
            StreamingSseChunk {
                gate: Some(held),
                body: responses::sse(vec![
                    responses::ev_assistant_message("original", "original completed answer"),
                    responses::ev_completed("active-source"),
                ]),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![
                responses::ev_response_created("adopted-source"),
                responses::ev_assistant_message("adopted", "adopted completed answer"),
                responses::ev_completed("adopted-source"),
            ]),
        }],
    ])
    .await;
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).with_model("gpt-5.6-sol")
        .enable_feature(Feature::Collab)
        .with_extra_config("[features.multi_agent_v2]\nenabled = true\nenable_thread_adoption = true")
        .with_extra_config(&format!("[model_providers.source]\nname = \"Source\"\nbase_url = \"{}/v1\"\nwire_api = \"responses\"\nrequest_max_retries = 0\nstream_max_retries = 0", source_server.uri()))
        .write(home.path())?;
    let mut client = app(home.path()).await?;
    let original: ThreadStartResponse = client
        .request(|request_id| ClientRequest::ThreadStart {
            request_id,
            params: ThreadStartParams {
                model_provider: Some("source".to_string()),
                ..Default::default()
            },
        })
        .await?;
    let parent: ThreadStartResponse = client
        .request(|request_id| ClientRequest::ThreadStart {
            request_id,
            params: Default::default(),
        })
        .await?;
    let source_turn: TurnStartResponse = client
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: original.thread.id.clone(),
                input: vec![UserInput::Text {
                    text: "finish this turn before adoption".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    timeout(TIMEOUT, source_server.wait_for_request_count(1)).await?;
    let result = tool(&server, &parent.thread.id, "adopt-active", "frodex", "adopt_agent", json!({"existing_thread_id": original.thread.id, "task_name": "worker", "message": "continue after the original turn"})).await;
    let _: TurnStartResponse = client
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: parent.thread.id.clone(),
                input: vec![UserInput::Text {
                    text: "adopt the active source".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    timeout(TIMEOUT, async {
        while !server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .any(|request| matches_thread(request, &parent.thread.id))
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let active = read(&mut client, &original.thread.id).await?;
    assert_eq!(active.parent_thread_id, None);
    assert_eq!(active.source, SessionSource::VsCode);
    assert_eq!(active.path, original.thread.path);
    assert!(
        active
            .turns
            .iter()
            .any(|turn| turn.id == source_turn.turn.id && turn.status == TurnStatus::InProgress)
    );
    assert_eq!(source_server.requests().await.len(), 1);
    assert!(result.function_call_output_text("adopt-active").is_none());
    release
        .send(())
        .expect("original response remains connected");
    // Adoption can unload and reload the source while transferring ownership.
    // Its result, not a concurrent history read, marks completion of the handoff.
    wait_for_answer(&mut client, &parent.thread.id, "done").await?;
    let adopted =
        wait_for_answer(&mut client, &original.thread.id, "adopted completed answer").await?;
    assert_eq!(
        tool_output(&result, "adopt-active")?["task_name"],
        "/root/worker"
    );
    assert_eq!(adopted.id, original.thread.id);
    assert_eq!(adopted.path, original.thread.path);
    assert_eq!(
        adopted.parent_thread_id.as_deref(),
        Some(parent.thread.id.as_str())
    );
    assert!(
        adopted
            .turns
            .iter()
            .any(|turn| turn.id == source_turn.turn.id && turn.status == TurnStatus::Completed)
    );
    assert_message(&adopted, "original completed answer");
    assert_eq!(source_server.requests().await.len(), 2);
    client.shutdown_gracefully().await?;
    source_server.shutdown().await;
    Ok(())
}

async fn turn(client: &mut TestAppServer, id: &str, text: &str) -> Result<()> {
    let completed = timeout(
        TIMEOUT,
        client.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: id.to_string(),
            input: vec![UserInput::Text {
                text: text.to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    Ok(())
}

async fn read(client: &mut TestAppServer, id: &str) -> Result<Thread> {
    let read: ThreadReadResponse = client
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: id.to_string(),
                include_turns: true,
            },
        })
        .await?;
    Ok(read.thread)
}

async fn members(client: &mut TestAppServer, id: &str) -> Result<Vec<String>> {
    let params = serde_json::from_value(json!({"parentThreadId": id, "limit": 100}))?;
    let listed: ThreadListResponse = client
        .request(|request_id| ClientRequest::ThreadList { request_id, params })
        .await?;
    Ok(listed.data.into_iter().map(|thread| thread.id).collect())
}

fn has_message(thread: &Thread, expected: &str) -> bool {
    thread
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .any(|item| matches!(item, ThreadItem::AgentMessage { text, .. } if text == expected))
}

fn assert_message(thread: &Thread, expected: &str) {
    assert!(
        has_message(thread, expected),
        "task {} lost {expected:?}",
        thread.id
    );
}

async fn wait_for_answer(client: &mut TestAppServer, id: &str, expected: &str) -> Result<Thread> {
    timeout(TIMEOUT, async {
        loop {
            let thread = read(client, id).await?;
            if has_message(&thread, expected)
                && thread
                    .turns
                    .iter()
                    .all(|turn| turn.status == TurnStatus::Completed)
            {
                return Ok(thread);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

fn assert_prefix(before: &Thread, bytes: Option<&[u8]>, after: &Thread) -> Result<()> {
    if let (Some(path), Some(bytes)) = (&before.path, bytes) {
        assert_eq!(after.path.as_ref(), Some(path));
        assert!(
            std::fs::read(path)?.starts_with(bytes),
            "ownership change rewrote original rollout"
        );
    }
    Ok(())
}

fn matches_thread(request: &wiremock::Request, id: &str) -> bool {
    ["thread-id", "x-client-request-id"].into_iter().any(|key| {
        request
            .headers
            .get(key)
            .and_then(|value| value.to_str().ok())
            == Some(id)
    })
}

async fn answer(server: &wiremock::MockServer, id: &str, text: &str) {
    let id = id.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| matches_thread(request, &id),
        responses::sse(vec![
            responses::ev_response_created(text),
            responses::ev_assistant_message(text, text),
            responses::ev_completed(text),
        ]),
    )
    .await;
}

async fn tool(
    server: &wiremock::MockServer,
    id: &str,
    call: &str,
    namespace: &str,
    name: &str,
    args: Value,
) -> responses::ResponseMock {
    let target = id.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| matches_thread(request, &target),
        responses::sse(vec![
            responses::ev_response_created(call),
            responses::ev_function_call_with_namespace(call, namespace, name, &args.to_string()),
            responses::ev_completed(call),
        ]),
    )
    .await;
    let target = id.to_string();
    let expected_call = call.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            matches_thread(request, &target)
                && serde_json::from_slice::<Value>(&request.body)
                    .ok()
                    .is_some_and(|body| {
                        body["input"].as_array().is_some_and(|items| {
                            items.iter().any(|item| {
                                item["type"] == "function_call_output"
                                    && item["call_id"] == expected_call
                            })
                        })
                    })
        },
        responses::sse(vec![
            responses::ev_response_created("ack"),
            responses::ev_assistant_message("ack", "done"),
            responses::ev_completed("ack"),
        ]),
    )
    .await
}

fn tool_output(mock: &responses::ResponseMock, call: &str) -> Result<Value> {
    let output = mock
        .function_call_output_text(call)
        .context("missing ownership tool output")?;
    serde_json::from_str(&output).with_context(|| format!("{call} failed: {output}"))
}
