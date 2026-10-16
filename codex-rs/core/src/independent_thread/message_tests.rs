use super::*;
use crate::CodexAppsToolsCache;
use crate::ThreadManager;
use crate::build_models_manager;
use crate::init_state_db;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use crate::state::TaskKind;
use crate::tasks::SessionTask;
use crate::tasks::SessionTaskResult;
use crate::thread_manager::NewThread;
use crate::thread_manager::StartThreadOptions;
use crate::thread_manager::thread_store_from_config;
use crate::tools::context::ToolCallSource;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::sse;
use core_test_support::test_codex::run_test_with_large_stack;
use serde_json::json;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use wiremock::MockServer;

#[test_case::test_case(ThreadHistoryMode::Legacy; "legacy")]
#[test_case::test_case(ThreadHistoryMode::Paginated; "paginated")]
fn idle_delivery_preserves_destination_and_persists_attribution(
    history_mode: ThreadHistoryMode,
) -> anyhow::Result<()> {
    run_test_with_large_stack("idle independent message", move || async move {
        let server = MockServer::start().await;
        let response = mount_sse_once(
            &server,
            sse(vec![
                ev_response_created("message-response"),
                ev_assistant_message("message-answer", "received"),
                ev_completed("message-response"),
            ]),
        )
        .await;
        let host = TestHost::new(&server, history_mode).await?;
        let before = host.destination.thread.config_snapshot().await;
        let receipt = host
            .send(host.destination.thread_id, "handoff-marker")
            .await?;
        assert_eq!(receipt["status"], "started");
        assert_eq!(
            receipt["source_thread_id"],
            host.source.thread_id.to_string()
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let EventMsg::TurnComplete(event) =
                    host.destination.thread.next_event().await?.msg
                {
                    assert_eq!(event.turn_id, receipt["turn_id"].as_str().unwrap());
                    assert!(event.error.is_none());
                    break;
                }
            }
            anyhow::Ok(())
        })
        .await??;
        let request = response.single_request().body_json();
        let delivered = request["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "function_call_output" && item["namespace"] == "frodex")
            .expect("delivery retains tool-output provenance");
        assert_eq!(delivered["name"], "send_message_to_thread");
        let content: serde_json::Value =
            serde_json::from_str(delivered["output"].as_str().unwrap())?;
        assert_eq!(
            content["source_thread_id"],
            host.source.thread_id.to_string()
        );
        assert_eq!(content["input"], "handoff-marker");
        let after = host.destination.thread.config_snapshot().await;
        assert_eq!(after.model, before.model);
        assert_eq!(after.approval_policy, before.approval_policy);
        assert_eq!(after.cwd(), before.cwd());
        assert_eq!(after.ephemeral, before.ephemeral);
        assert!(
            request
                .to_string()
                .contains("destination-instructions-marker")
        );
        assert!(!request.to_string().contains("sender-instructions-marker"));
        host.destination.thread.flush_rollout().await?;
        let saved = host.destination.thread.read_thread(true, true).await?;
        assert!(saved.rollout_path.is_some());
        // Thread reads must expose the sender even after the model turn completes.
        let saved_json = serde_json::to_string(&saved)?;
        assert!(saved_json.contains("handoff-marker"));
        assert!(saved_json.contains(&host.source.thread_id.to_string()));
        host.shutdown().await
    })
}

#[test_case::test_case(TaskKind::Regular, true; "steers regular")]
#[test_case::test_case(TaskKind::Review, false; "does not interrupt review")]
fn active_delivery_preserves_running_task(kind: TaskKind, steerable: bool) -> anyhow::Result<()> {
    run_test_with_large_stack("active independent message", move || async move {
        let server = MockServer::start().await;
        let host = TestHost::new(&server, ThreadHistoryMode::Legacy).await?;
        let (_, mut context) = make_session_and_context().await;
        context.sub_id = "active-destination-turn".to_string();
        host.destination
            .thread
            .session
            .spawn_task(Arc::new(context), Vec::new(), WaitingTask(kind))
            .await;
        let result = host
            .send(host.destination.thread_id, "steering-marker")
            .await;
        if steerable {
            let receipt = result?;
            assert_eq!(receipt["status"], "steered");
            assert_eq!(receipt["turn_id"], "active-destination-turn");
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("ActiveTurnNotSteerable")
            );
        }
        let pending = host
            .destination
            .thread
            .session
            .input_queue
            .get_pending_input(&host.destination.thread.session.active_turn)
            .await
            .0;
        assert_eq!(pending.len(), usize::from(steerable));
        if steerable {
            let crate::session::TurnInput::FunctionCallOutput(output) = &pending[0] else {
                panic!("steering must retain tool-output provenance");
            };
            assert!(
                matches!(&output.item, ResponseItem::FunctionCallOutput { namespace: Some(namespace), .. } if namespace == "frodex")
            );
        }
        assert!(
            host.destination
                .thread
                .session
                .active_turn
                .lock()
                .await
                .is_some()
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        host.shutdown().await
    })
}

#[test]
fn unloaded_foreign_and_subagent_destinations_are_rejected() -> anyhow::Result<()> {
    run_test_with_large_stack("independent message boundaries", || async {
        let server = MockServer::start().await;
        let host = TestHost::new(&server, ThreadHistoryMode::Legacy).await?;
        let foreign = TestHost::new(&server, ThreadHistoryMode::Legacy).await?;
        assert!(
            host.send(foreign.destination.thread_id, "not delivered")
                .await
                .unwrap_err()
                .to_string()
                .contains("loaded in this app-server process")
        );
        let child = host
            .manager
            .start_thread(StartThreadOptions {
                session_source: Some(SessionSource::SubAgent(SubAgentSource::Other(
                    "helper".to_string(),
                ))),
                ..StartThreadOptions::new(host.turn.config.as_ref().clone())
            })
            .await?;
        assert!(
            host.send(child.thread_id, "not delivered")
                .await
                .unwrap_err()
                .to_string()
                .contains("destination must be a root thread")
        );
        // Even a handler retained from a root turn must check the current caller.
        assert!(
            host.invoke_for(
                &child,
                json!({"thread_id": host.destination.thread_id, "prompt": "not sent"})
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("only root threads")
        );
        child.thread.shutdown_and_wait().await?;
        host.destination.thread.shutdown_and_wait().await?;
        host.manager
            .remove_thread(&host.destination.thread_id)
            .await;
        assert!(
            host.send(host.destination.thread_id, "not resumed")
                .await
                .unwrap_err()
                .to_string()
                .contains("resume it in the host first")
        );
        assert!(
            host.manager
                .get_thread(host.destination.thread_id)
                .await
                .is_err()
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        host.source.thread.shutdown_and_wait().await?;
        foreign.shutdown().await
    })
}

#[test]
fn independent_and_ownership_tools_share_strict_frodex_namespace() -> anyhow::Result<()> {
    run_test_with_large_stack("independent tool registration", || async {
        let server = MockServer::start().await;
        let mut host = TestHost::new(&server, ThreadHistoryMode::Legacy).await?;
        let turn = Arc::get_mut(&mut host.turn).expect("unique test turn");
        let config = Arc::make_mut(&mut turn.config);
        config.tool_registry.error_on_tool_collisions = true;
        config.tool_registry.turn_metadata_includes_tool_info = true;
        config.multi_agent_v2.enable_thread_adoption = true;
        config
            .features
            .enable(codex_features::Feature::MultiAgentV2)
            .expect("multi agent feature");
        turn.multi_agent_version = config.multi_agent_version_from_features();
        let step = StepContext::for_test(Arc::clone(&host.turn));
        let router = crate::tools::spec_plan::build_tool_router(
            &host.source.thread.session,
            &host.turn,
            host.turn.model_info(),
            &step.environments,
            &step.mcp,
            false,
            &host.turn.extension_data,
            None,
        )?;
        let names = router.registered_tool_names_for_test();
        for name in [
            "adopt_agent",
            "promote_agent",
            "close_agent",
            "send_message_to_thread",
        ] {
            assert!(
                names.contains(&ToolName::namespaced("frodex", name)),
                "missing {name}"
            );
        }
        assert!(server.received_requests().await.unwrap().is_empty());
        host.shutdown().await
    })
}

#[test]
fn invalid_arguments_have_no_delivery_effect() -> anyhow::Result<()> {
    run_test_with_large_stack("independent message validation", || async {
        let server = MockServer::start().await;
        let host = TestHost::new(&server, ThreadHistoryMode::Legacy).await?;
        for arguments in [
            json!({"thread_id": host.destination.thread_id, "prompt": " \n\t"}),
            json!({"thread_id": "not-a-thread", "prompt": "hello"}),
            json!({"thread_id": host.destination.thread_id, "prompt": "hello", "source_thread_id": "forged"}),
            json!({"thread_id": host.destination.thread_id, "prompt": "hello", "model": "override"}),
            json!({"thread_id": host.destination.thread_id, "prompt": "x".repeat(10_001)}),
            json!({"thread_id": host.destination.thread_id, "prompt": "x".repeat(10_000)}),
            json!({"thread_id": host.destination.thread_id, "prompt": "\0".repeat(2_000)}),
        ] {
            assert!(host.invoke(arguments).await.is_err());
        }
        assert!(server.received_requests().await.unwrap().is_empty());
        host.shutdown().await
    })
}

/// Isolated persisted roots using the production host capability and a mock model.
struct TestHost {
    _fixture: Session,
    manager: Arc<ThreadManager>,
    source: NewThread,
    destination: NewThread,
    turn: Arc<TurnContext>,
}

impl TestHost {
    async fn new(server: &MockServer, history_mode: ThreadHistoryMode) -> anyhow::Result<Self> {
        let (fixture, turn) = make_session_and_context().await;
        let mut config = turn.config.as_ref().clone();
        config.ephemeral = false;
        config.model_provider.base_url = Some(server.uri());
        config.model_provider.supports_websockets = false;
        let state = init_state_db(&config).await.expect("test state database");
        let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
        let manager = Arc::new_cyclic(|manager| {
            let mut builder = ExtensionRegistryBuilder::default();
            crate::independent_thread::install(&mut builder, manager.clone());
            ThreadManager::new(
                &config,
                Arc::clone(&auth),
                build_models_manager(&config, Arc::clone(&auth)),
                CodexAppsToolsCache::default(),
                SessionSource::Exec,
                Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
                Arc::new(builder.build()),
                Arc::new(crate::test_support::EmptyUserInstructionsProvider),
                None,
                crate::passthrough_image_store(),
                thread_store_from_config(&config, Some(state)),
                None,
                "message-test".to_string(),
                None,
                None,
            )
        });
        config.developer_instructions = Some("sender-instructions-marker".to_string());
        let source = manager
            .start_thread(StartThreadOptions {
                history_mode: Some(history_mode),
                ..StartThreadOptions::new(config.clone())
            })
            .await?;
        config.developer_instructions = Some("destination-instructions-marker".to_string());
        let destination = manager
            .start_thread(StartThreadOptions {
                history_mode: Some(history_mode),
                ..StartThreadOptions::new(config)
            })
            .await?;
        Ok(Self {
            _fixture: fixture,
            manager,
            source,
            destination,
            turn: Arc::new(turn),
        })
    }

    async fn invoke(&self, arguments: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        self.invoke_for(&self.source, arguments).await
    }

    async fn invoke_for(
        &self,
        source: &NewThread,
        arguments: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let mut registry = ToolRegistry::default();
        register(&source.thread.session, &self.turn, &mut registry);
        let handler = registry
            .remove(&ToolName::namespaced("frodex", "send_message_to_thread"))
            .expect("root messaging registered");
        let output = handler
            .handle(ToolInvocation {
                session: Arc::clone(&source.thread.session),
                step_context: StepContext::for_test(Arc::clone(&self.turn)),
                turn: Arc::clone(&self.turn),
                cancellation_token: CancellationToken::new(),
                tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
                call_id: "message-call".to_string(),
                tool_name: ToolName::namespaced("frodex", "send_message_to_thread"),
                source: ToolCallSource::Direct,
                payload: ToolPayload::Function {
                    arguments: arguments.to_string(),
                },
            })
            .await?;
        Ok(serde_json::from_str(&output.log_output())?)
    }

    async fn send(&self, destination: ThreadId, prompt: &str) -> anyhow::Result<serde_json::Value> {
        self.invoke(json!({"thread_id": destination, "prompt": prompt}))
            .await
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        self.source.thread.shutdown_and_wait().await?;
        self.destination.thread.shutdown_and_wait().await?;
        Ok(())
    }
}

/// Holds the recipient turn open until ordinary cancellation.
struct WaitingTask(TaskKind);

impl SessionTask for WaitingTask {
    fn kind(&self) -> TaskKind {
        self.0
    }
    fn span_name(&self) -> &'static str {
        "session_task.independent_message_test"
    }
    async fn run(
        self: Arc<Self>,
        _session: Arc<Session>,
        _ctx: Arc<TurnContext>,
        _input: Vec<crate::session::TurnInput>,
        cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        cancellation_token.cancelled().await;
        Ok(None)
    }
}
