use super::*;
use core_test_support::process::process_is_alive;
use core_test_support::process::wait_for_process_exit;
use core_test_support::stdio_server_bin;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
#[cfg(target_os = "linux")]
use std::collections::BTreeSet;
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
use tokio::sync::oneshot;

async fn shared_mcp_process_id(thread: &CodexThread) -> u64 {
    thread
        .call_mcp_tool(
            "retirement",
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await
        .expect("shared MCP call should succeed")
        .structured_content
        .as_ref()
        .and_then(|value| value.get("pid"))
        .and_then(serde_json::Value::as_u64)
        .expect("shared MCP response should include a process ID")
}

#[cfg(target_os = "linux")]
fn open_rollout_descriptor_paths(codex_home: &Path) -> BTreeSet<PathBuf> {
    std::fs::read_dir("/proc/self/fd")
        .expect("process descriptors should be readable")
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .filter(|target| {
            target.starts_with(codex_home)
                && target.to_string_lossy().contains("/sessions/")
                && target.to_string_lossy().contains(".jsonl")
        })
        .collect()
}

#[test]
fn finished_goal_supervisor_releases_shared_mcp_lease() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "finished_goal_supervisor_releases_shared_mcp_lease",
        finished_goal_supervisor_releases_shared_mcp_lease_inner(),
    )
}

async fn finished_goal_supervisor_releases_shared_mcp_lease_inner() -> anyhow::Result<()> {
    let iterations = std::env::var("CODEX_TEST_GOAL_SUPERVISOR_RETIREMENT_ITERATIONS")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(3);
    assert!(iterations > 0);
    let mut responses = vec![vec![StreamingSseChunk {
        gate: None,
        body: sse(vec![
            ev_response_created("sibling"),
            ev_completed("sibling"),
        ]),
    }]];
    let mut releases = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let (release, gate) = oneshot::channel();
        releases.push(release);
        responses.push(vec![StreamingSseChunk {
            gate: Some(gate),
            body: sse(vec![ev_response_created("helper"), ev_completed("helper")]),
        }]);
    }
    let (server, _completions) = start_streaming_sse_server(responses).await;
    let (home, mut config) = test_config().await;
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.mcp_servers.set(HashMap::from([(
        "retirement".to_string(),
        McpServerConfig {
            transport: McpServerTransportConfig::Stdio {
                command: stdio_server_bin()?,
                args: Vec::new(),
                env: Some(HashMap::from([(
                    "MCP_TEST_AGENT_TREE_TOOLS".to_string(),
                    "1".to_string(),
                )])),
                env_vars: Vec::new(),
                cwd: None,
            },
            environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
            enabled: true,
            required: true,
            auth: Default::default(),
            supports_parallel_tool_calls: false,
            omit_tools_from: None,
            disabled_reason: None,
            startup_timeout_sec: Some(Duration::from_secs(5)),
            tool_timeout_sec: Some(Duration::from_secs(5)),
            default_tools_approval_mode: None,
            enabled_tools: None,
            disabled_tools: None,
            scopes: None,
            oauth: None,
            oauth_resource: None,
            tools: HashMap::new(),
        },
    )]))?;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let agent_control = parent_thread.session.services.agent_control.clone();
    let sibling_thread_id = agent_control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("sibling task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions::default(),
        )
        .await?
        .thread_id;
    let sibling_thread = harness.manager.get_thread(sibling_thread_id).await?;
    wait_for_turn_complete(sibling_thread.as_ref()).await;
    let pid = shared_mcp_process_id(parent_thread.as_ref()).await;
    assert_eq!(shared_mcp_process_id(sibling_thread.as_ref()).await, pid);
    #[cfg(target_os = "linux")]
    let baseline = open_rollout_descriptor_paths(harness.config.codex_home.as_path());

    for (iteration, release) in releases.into_iter().enumerate() {
        let mut helper_config = harness.config.clone();
        helper_config.ephemeral = true;
        let helper_thread_id = agent_control
            .spawn_agent_with_metadata(
                helper_config,
                text_input("supervise"),
                Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id,
                    depth: 1,
                    agent_path: Some(
                        AgentPath::root()
                            .join("goal_supervisor")
                            .expect("supervisor path"),
                    ),
                    agent_nickname: None,
                    agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
                })),
                SpawnAgentOptions::default(),
            )
            .await?
            .thread_id;
        let retained_helper = harness.manager.get_thread(helper_thread_id).await?;
        timeout(
            Duration::from_secs(5),
            server.wait_for_request_count(iteration + 2),
        )
        .await?;
        // Normal ephemeral helpers stay memory-only. Force the historical materialized
        // case so this regression cannot pass without ever acquiring a rollout descriptor.
        retained_helper
            .session
            .try_ensure_rollout_materialized(PersistContext::Standard)
            .await?;
        retained_helper.session.flush_rollout().await?;
        #[cfg(target_os = "linux")]
        let helper_rollout_path = retained_helper
            .session
            .current_rollout_path()
            .await?
            .expect("materialized helper rollout");
        #[cfg(target_os = "linux")]
        let helper_rollout_path = std::fs::canonicalize(helper_rollout_path)?;
        #[cfg(target_os = "linux")]
        let active_paths = open_rollout_descriptor_paths(harness.config.codex_home.as_path());
        #[cfg(target_os = "linux")]
        assert!(
            active_paths.contains(&helper_rollout_path),
            "active helper must own its rollout descriptor: helper={helper_rollout_path:?}, open={active_paths:?}"
        );
        assert_eq!(shared_mcp_process_id(retained_helper.as_ref()).await, pid);
        assert_eq!(retained_helper.agent_status().await, AgentStatus::Running);
        let state_db = harness.state_db.as_ref().expect("persisted spawn edges");
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                helper_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await?;
        assert!(
            state_db
                .list_thread_spawn_children_with_status(
                    parent_thread_id,
                    DirectionalThreadSpawnEdgeStatus::Open,
                )
                .await?
                .contains(&helper_thread_id)
        );

        // Retain the listener's Arc while retiring an active helper. Bookkeeping removal
        // alone cannot close its rollout writer or release the shared MCP lease.
        agent_control
            .finish_internal_helper_thread(helper_thread_id)
            .await?;
        assert_thread_not_loaded(&harness.manager, helper_thread_id).await;
        assert!(
            state_db
                .list_thread_spawn_children_with_status(
                    parent_thread_id,
                    DirectionalThreadSpawnEdgeStatus::Closed,
                )
                .await?
                .contains(&helper_thread_id)
        );
        assert!(
            !state_db
                .list_thread_spawn_children_with_status(
                    parent_thread_id,
                    DirectionalThreadSpawnEdgeStatus::Open,
                )
                .await?
                .contains(&helper_thread_id)
        );
        release.send(()).expect("helper response is still pending");
        timeout(
            Duration::from_secs(5),
            retained_helper.wait_until_terminated(),
        )
        .await?;
        timeout(Duration::from_secs(2), async {
            loop {
                let event = retained_helper
                    .next_event()
                    .await
                    .expect("retained helper events");
                if matches!(event.msg, EventMsg::TurnComplete(_)) {
                    break;
                }
            }
        })
        .await?;
        #[cfg(target_os = "linux")]
        {
            let after = timeout(Duration::from_secs(2), async {
                loop {
                    let paths = open_rollout_descriptor_paths(harness.config.codex_home.as_path());
                    if !paths.contains(&helper_rollout_path) {
                        break paths;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            assert!(
                after.is_subset(&baseline),
                "retired helpers must not accumulate rollout descriptors"
            );
        }
    }
    assert_eq!(shared_mcp_process_id(parent_thread.as_ref()).await, pid);
    assert_eq!(shared_mcp_process_id(sibling_thread.as_ref()).await, pid);
    sibling_thread.shutdown_and_wait().await?;
    assert!(
        process_is_alive(&pid.to_string())?,
        "parent lease must keep MCP alive"
    );
    parent_thread.shutdown_and_wait().await?;
    wait_for_process_exit(&pid.to_string()).await?;
    server.shutdown().await;
    Ok(())
}
