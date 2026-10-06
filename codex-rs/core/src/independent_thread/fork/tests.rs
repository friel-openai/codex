//! Deterministic tests of the tool's persisted history and independent lifecycle.

use super::*;
use crate::CodexAppsToolsCache;
use crate::ThreadManager;
use crate::build_models_manager;
use crate::init_state_db;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use crate::thread_manager::NewThread;
use crate::thread_manager::thread_store_from_config;
use crate::tools::context::ToolCallSource;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::TurnStartAdmission;
use codex_history::RolloutItem;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_model_provider::create_model_provider;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Personality;
use codex_protocol::dynamic_tools::DynamicToolFunctionSpec;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::TurnStartedEvent;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::sse;
use core_test_support::test_codex::run_test_with_large_stack;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use wiremock::MockServer;

/// The initial assignment is a required input; placement defaults to automatic.
#[test]
fn arguments_preserve_the_approved_defaults() {
    let args: Args = serde_json::from_value(json!({"prompt": "inspect"})).unwrap();
    assert!(args.inherit_section);
    assert!(args.title.is_none());
    assert!(args.section.is_none());
    let args: Args =
        serde_json::from_value(json!({"prompt": "inspect", "inherit_section": false})).unwrap();
    assert!(!args.inherit_section);
    let args: Args = serde_json::from_value(
        json!({"prompt": "inspect", "inherit_section": false, "section": "Research"}),
    )
    .unwrap();
    assert_eq!(args.section.as_deref(), Some("Research"));
    assert!(serde_json::from_value::<Args>(json!({"title": "missing assignment"})).is_err());
    assert!(
        serde_json::from_value::<Args>(json!({"prompt": "inspect", "thread_id": "other"})).is_err()
    );
}

/// Blank destinations are rejected before the fork host can create a thread.
#[test_case::test_case(json!({"prompt": "inspect", "section": " \t\n"}), "section must not be empty"; "blank_section")]
#[test_case::test_case(json!({"prompt": "x".repeat(10_000)}), "JSON-encoded attribution"; "encoded_attribution_limit")]
#[test_case::test_case(json!({"prompt": "\0".repeat(2_000)}), "JSON-encoded attribution"; "escaped_prompt_limit")]
#[tokio::test]
async fn invalid_fork_arguments_are_rejected_before_creation(
    arguments: serde_json::Value,
    expected: &str,
) {
    let (session, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let invocation = ToolInvocation {
        session: Arc::new(session),
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
        call_id: "blank-section".to_string(),
        tool_name: ToolName::namespaced("frodex", "fork_thread"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: arguments.to_string(),
        },
    };
    let handler = Handler {
        host: Arc::new(Host {
            manager: Weak::new(),
        }),
    };
    let Err(error) = handler.handle_call(invocation).await else {
        panic!("blank section must be rejected");
    };
    assert!(error.to_string().contains(expected));
}

/// Both persistence formats must exclude the unfinished turn and keep root identity.
#[test_case::test_case(ThreadHistoryMode::Legacy, false; "legacy")]
#[test_case::test_case(ThreadHistoryMode::Paginated, false; "paginated")]
#[test_case::test_case(ThreadHistoryMode::Legacy, true; "failed_placement")]
fn fork_runs_from_completed_history_without_a_goal(
    history_mode: ThreadHistoryMode,
    placement_fails: bool,
) -> anyhow::Result<()> {
    run_test_with_large_stack("independent fork", move || async move {
        let server = MockServer::start().await;
        let response = mount_sse_once(
            &server,
            sse(vec![
                ev_response_created("fork-response"),
                ev_assistant_message("fork-message", "assignment complete"),
                ev_completed("fork-response"),
            ]),
        )
        .await;
        let (_fixture, mut turn) = make_session_and_context().await;
        let mut config = turn.config.as_ref().clone();
        config.ephemeral = false;
        config.model_provider.base_url = Some(server.uri());
        config.model_provider.supports_websockets = false;
        turn.provider = create_model_provider(config.model_provider.clone(), None);
        turn.config = Arc::new(config.clone());
        turn.history_mode = history_mode;
        turn.sub_id = "unfinished".to_string();
        if placement_fails {
            turn.dynamic_tools = ["list_threads", "move_thread_to_sidebar_section"]
                .into_iter()
                .map(|name| {
                    DynamicToolSpec::Function(DynamicToolFunctionSpec {
                        name: format!("codex_app__{name}"),
                        description: name.to_string(),
                        input_schema: json!({"type": "object"}),
                        defer_loading: false,
                    })
                })
                .collect();
        }
        let state = init_state_db(&config).await.expect("test state database");
        let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
        let manager = Arc::new(ThreadManager::new(
            &config,
            Arc::clone(&auth),
            build_models_manager(&config, auth),
            CodexAppsToolsCache::default(),
            SessionSource::Exec,
            Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            codex_extension_api::empty_extension_registry(),
            Arc::new(crate::test_support::EmptyUserInstructionsProvider),
            None,
            crate::passthrough_image_store(),
            thread_store_from_config(&config, Some(Arc::clone(&state))),
            None,
            "fork-test".to_string(),
            None,
            None,
        ));
        let source = manager
            .start_thread(StartThreadOptions {
                history_mode: Some(history_mode),
                dynamic_tools: turn.dynamic_tools.clone(),
                thread_extension_init: {
                    let mut init = ExtensionDataInit::new();
                    init.insert(codex_extension_api::ToolPolicy {
                        allowed_tools: Some(vec![
                            ToolName::namespaced("frodex", "fork_thread"),
                            ToolName::plain("codex_app__list_threads"),
                            ToolName::plain("codex_app__move_thread_to_sidebar_section"),
                        ]),
                        ..Default::default()
                    });
                    init
                },
                ..StartThreadOptions::new(config)
            })
            .await?;
        let mut collaboration_mode = turn.collaboration_mode();
        collaboration_mode.mode = ModeKind::Plan;
        source
            .thread
            .update_thread_settings(ThreadSettingsOverrides {
                collaboration_mode: Some(collaboration_mode.clone()),
                personality: Some(Personality::Friendly),
                ..Default::default()
            })
            .await?;
        let turn = source
            .thread
            .session
            .new_turn_with_default_settings("unfinished".to_string(), Default::default())
            .await;
        source
            .thread
            .append_rollout_items(&[
                user_message("completed-history-marker"),
                turn_started("unfinished"),
                user_message("unfinished-history-marker"),
            ])
            .await?;
        source.thread.flush_rollout().await?;
        let goal = state
            .thread_goals()
            .replace_thread_goal(
                source.thread_id,
                "parent-only-goal",
                codex_state::ThreadGoalStatus::Active,
                None,
            )
            .await?;
        let mut created = manager.subscribe_thread_created();
        let mut arguments = json!({"prompt": "new-assignment-marker", "title": "Independent test"});
        if placement_fails {
            arguments["section"] = json!("Research");
            arguments["inherit_section"] = json!(false);
        }
        let invocation = tool_invocation(&source, turn, arguments);
        let expected_model = invocation.step_context.settings.model_info.slug.clone();
        let expected_policy = invocation.step_context.settings.approval_policy();
        let expected_cwd = invocation.turn.config.cwd.clone();
        if placement_fails {
            *source.thread.session.active_turn.lock().await =
                Some(crate::state::ActiveTurn::default());
        }
        let client = async {
            if placement_fails {
                loop {
                    let event = source.thread.next_event().await.unwrap();
                    if let EventMsg::ItemStarted(event) = event.msg
                        && let TurnItem::DynamicToolCall(call) = event.item
                    {
                        source
                            .thread
                            .session
                            .notify_dynamic_tool_response(
                                &call.id,
                                DynamicToolResponse {
                                    content_items: Vec::new(),
                                    success: false,
                                },
                            )
                            .await;
                        break;
                    }
                }
            }
        };
        let handler = Handler {
            host: Arc::new(Host {
                manager: Arc::downgrade(&manager),
            }),
        };
        let call = handler.handle_call(invocation);
        tokio::pin!(call);
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = &mut call => result,
                () = client => call.await,
            }
        })
        .await?;
        let result = result?;
        let result: serde_json::Value = serde_json::from_str(&result.log_output())?;
        assert_eq!(result["status"], "started");
        assert_eq!(
            result["section"]["status"],
            if placement_fails { "failed" } else { "skipped" }
        );
        let fork_id = ThreadId::from_string(result["thread_id"].as_str().unwrap())?;
        assert_ne!(fork_id, source.thread_id);
        assert_eq!(created.try_recv()?, fork_id);
        let fork = manager.get_thread(fork_id).await?;
        let settings = fork.config_snapshot().await;
        assert_eq!(settings.parent_thread_id, None);
        assert!(!settings.session_source.is_non_root_agent());
        assert_eq!(settings.model, expected_model);
        assert_eq!(settings.approval_policy, expected_policy);
        assert_eq!(settings.collaboration_mode, collaboration_mode);
        assert_eq!(settings.personality, Some(Personality::Friendly));
        assert_eq!(
            fork.session.tool_policy.allowed_tools,
            source.thread.session.tool_policy.allowed_tools
        );
        assert_eq!(settings.cwd(), &expected_cwd);
        assert!(!settings.ephemeral);
        source.thread.shutdown_and_wait().await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let EventMsg::TurnComplete(event) = fork.next_event().await?.msg {
                    assert_eq!(
                        event.last_agent_message.as_deref(),
                        Some("assignment complete")
                    );
                    assert!(event.error.is_none());
                    break;
                }
            }
            anyhow::Ok(())
        })
        .await??;
        fork.flush_rollout().await?;
        let stored = fork.read_thread(true, false).await?;
        assert_eq!(stored.forked_from_id, Some(source.thread_id));
        assert_eq!(stored.name.as_deref(), Some("Independent test"));
        assert!(stored.rollout_path.is_some());
        let captured = response.single_request();
        let assignment = captured
            .input()
            .into_iter()
            .find(|item| {
                item.get("name").and_then(serde_json::Value::as_str) == Some("fork_thread")
            })
            .expect("attributed fork assignment");
        assert_eq!(assignment["type"], "function_call_output");
        assert_eq!(assignment["namespace"], "frodex");
        let delivered: serde_json::Value =
            serde_json::from_str(assignment["output"].as_str().expect("assignment JSON"))?;
        assert_eq!(
            delivered,
            json!({
                "source_thread_id": source.thread_id,
                "input": "new-assignment-marker",
            })
        );
        let request = captured.body_json().to_string();
        assert!(request.contains("completed-history-marker"));
        assert!(request.contains("new-assignment-marker"));
        assert!(!request.contains("unfinished-history-marker"));
        assert_eq!(state.thread_goals().get_thread_goal(fork_id).await?, None);
        assert_eq!(
            state
                .thread_goals()
                .get_thread_goal(source.thread_id)
                .await?,
            Some(goal)
        );
        fork.shutdown_and_wait().await?;
        Ok(())
    })
}

/// A host drain rejects assignment submission without hiding the saved fork ID.
#[test]
fn rejected_start_returns_the_created_thread() -> anyhow::Result<()> {
    run_test_with_large_stack("rejected fork start", || async {
        let (_fixture, mut turn) = make_session_and_context().await;
        let mut config = turn.config.as_ref().clone();
        config.ephemeral = false;
        turn.config = Arc::new(config.clone());
        turn.history_mode = ThreadHistoryMode::Legacy;
        turn.sub_id = "unfinished".to_string();
        let state = init_state_db(&config).await.expect("test state database");
        let gate = Arc::new(Admission(AtomicBool::new(false)));
        let mut extensions = ExtensionRegistryBuilder::default();
        extensions.turn_start_admission(gate);
        let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
        let manager = Arc::new(ThreadManager::new(
            &config,
            Arc::clone(&auth),
            build_models_manager(&config, auth),
            CodexAppsToolsCache::default(),
            SessionSource::Exec,
            Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            Arc::new(extensions.build()),
            Arc::new(crate::test_support::EmptyUserInstructionsProvider),
            None,
            crate::passthrough_image_store(),
            thread_store_from_config(&config, Some(state)),
            None,
            "fork-rejected-test".to_string(),
            None,
            None,
        ));
        let source = manager
            .start_thread(StartThreadOptions {
                history_mode: Some(ThreadHistoryMode::Legacy),
                ..StartThreadOptions::new(config)
            })
            .await?;
        source
            .thread
            .append_rollout_items(&[turn_started("unfinished")])
            .await?;
        let result = Handler {
            host: Arc::new(Host {
                manager: Arc::downgrade(&manager),
            }),
        }
        .handle_call(tool_invocation(
            &source,
            Arc::new(turn),
            json!({"prompt": "new assignment", "inherit_section": false}),
        ))
        .await?;
        let result: serde_json::Value = serde_json::from_str(&result.log_output())?;
        assert_eq!(result["status"], "created_not_started");
        assert!(result["error"].as_str().unwrap().contains("ServerDraining"));
        assert!(result.get("turn_id").is_none());
        let fork_id = ThreadId::from_string(result["thread_id"].as_str().unwrap())?;
        let fork = manager.get_thread(fork_id).await?;
        let persisted = fork.read_thread(true, true).await?;
        assert!(persisted.rollout_path.is_some());
        assert!(!persisted.history.expect("persisted fork history").items.iter().any(|item| {
            matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(event)) if event.turn_id == "unfinished")
        }), "the durable fork must not reintroduce its parent's open turn");
        source.thread.shutdown_and_wait().await?;
        fork.shutdown_and_wait().await?;
        Ok(())
    })
}

/// Runtime membership, not the captured caller turn, decides root eligibility.
#[test]
fn subagent_cannot_fork_with_a_root_turn_context() -> anyhow::Result<()> {
    run_test_with_large_stack("independent fork root restriction", || async {
        let (_fixture, turn) = make_session_and_context().await;
        let config = turn.config.as_ref().clone();
        let manager = Arc::new(ThreadManager::with_models_provider_and_home_for_tests(
            CodexAuth::from_api_key("dummy"),
            config.model_provider.clone(),
            config.codex_home.to_path_buf(),
            Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        ));
        let source = manager
            .start_thread(StartThreadOptions {
                session_source: Some(SessionSource::SubAgent(
                    codex_protocol::protocol::SubAgentSource::Other("helper".to_string()),
                )),
                ..StartThreadOptions::new(config)
            })
            .await?;
        let mut created = manager.subscribe_thread_created();
        let result = Handler {
            host: Arc::new(Host {
                manager: Arc::downgrade(&manager),
            }),
        }
        .handle_call(tool_invocation(
            &source,
            Arc::new(turn),
            json!({"prompt": "not started"}),
        ))
        .await;
        let Err(error) = result else {
            panic!("subagent must not fork a root")
        };
        assert!(error.to_string().contains("only root threads"));
        assert!(created.try_recv().is_err());
        source.thread.shutdown_and_wait().await?;
        Ok(())
    })
}

/// Test host whose drain gate does not depend on timing.
#[derive(Debug)]
struct Admission(
    /// Whether the host accepts new model turns.
    AtomicBool,
);

impl TurnStartAdmission for Admission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        self.0
            .load(Ordering::Relaxed)
            .then(|| Box::new(()) as Box<dyn Send>)
    }
}

/// Builds a direct call from a live source and captured turn context.
fn tool_invocation(
    source: &NewThread,
    turn: Arc<TurnContext>,
    arguments: serde_json::Value,
) -> ToolInvocation {
    ToolInvocation {
        session: Arc::clone(&source.thread.session),
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
        call_id: "fork-call".to_string(),
        tool_name: ToolName::namespaced("frodex", "fork_thread"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: arguments.to_string(),
        },
    }
}

/// Supplies a persisted turn boundary without asking a model to run the source.
fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        root_turn_id: None,
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

/// Supplies distinguishable history for the inherited-prefix assertion.
fn user_message(text: &str) -> RolloutItem {
    RolloutItem::ResponseItem(
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}
