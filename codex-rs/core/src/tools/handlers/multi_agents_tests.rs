use super::*;
use crate::ThreadManager;
use crate::config::AgentRoleConfig;
use crate::config::DEFAULT_AGENT_MAX_DEPTH;
use crate::function_tool::FunctionCallError;
use crate::init_state_db;
use crate::session::tests::make_session_and_context;
use crate::session_prefix::format_subagent_notification_message;
use crate::thread_manager::thread_store_from_config;
use crate::tools::context::ToolOutput;
use crate::tools::handlers::multi_agents_v2::CloseAgentHandler as CloseAgentHandlerV2;
use crate::tools::handlers::multi_agents_v2::FollowupTaskHandler as FollowupTaskHandlerV2;
use crate::tools::handlers::multi_agents_v2::ListAgentsHandler as ListAgentsHandlerV2;
use crate::tools::handlers::multi_agents_v2::SendMessageHandler as SendMessageHandlerV2;
use crate::tools::handlers::multi_agents_v2::SpawnAgentHandler as SpawnAgentHandlerV2;
use crate::tools::handlers::multi_agents_v2::WaitAgentHandler as WaitAgentHandlerV2;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_extension_api::empty_extension_registry;
use codex_features::Feature;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_model_provider::create_model_provider;
use codex_model_provider_info::built_in_model_providers;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::config_types::ShellEnvironmentPolicy;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::models::SandboxEnforcement;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::FileSystemAccessMode;
use codex_protocol::protocol::FileSystemPath;
use codex_protocol::protocol::FileSystemSandboxEntry;
use codex_protocol::protocol::FileSystemSandboxPolicy;
use codex_protocol::protocol::InitialHistory;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::NetworkSandboxPolicy;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::user_input::UserInput;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use core_test_support::TempDirExt;
use pretty_assertions::assert_eq;
use serde::Deserialize;
use serde_json::json;
use serial_test::serial;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn run_large_stack_async<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    // Spawn-agent tests exercise the full multi-agent spawn future. In debug builds that future can
    // overflow libtest's default thread stack before the regression assertion runs.
    std::thread::Builder::new()
        .name("multi-agent-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(test());
        })
        .expect("multi-agent test thread should start")
        .join()
        .expect("multi-agent test thread should finish");
}

fn invocation(
    session: Arc<crate::session::session::Session>,
    turn: Arc<TurnContext>,
    tool_name: &str,
    payload: ToolPayload,
) -> ToolInvocation {
    ToolInvocation {
        session,
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
        call_id: "call-1".to_string(),
        tool_name: codex_tools::ToolName::plain(tool_name),
        source: crate::tools::context::ToolCallSource::Direct,
        payload,
    }
}

fn function_payload(args: serde_json::Value) -> ToolPayload {
    ToolPayload::Function {
        arguments: args.to_string(),
    }
}

fn parse_agent_id(id: &str) -> ThreadId {
    ThreadId::from_string(id).expect("agent id should be valid")
}

fn thread_manager() -> ThreadManager {
    ThreadManager::with_models_provider_for_tests(
        CodexAuth::from_api_key("dummy"),
        built_in_model_providers(/* openai_base_url */ /*openai_base_url*/ None)["openai"].clone(),
    )
}

async fn spawned_thread_id_after(
    manager: &ThreadManager,
    before_thread_ids: &[ThreadId],
) -> ThreadId {
    let mut spawned_thread_ids = manager
        .list_thread_ids()
        .await
        .into_iter()
        .filter(|thread_id| !before_thread_ids.contains(thread_id))
        .collect::<Vec<_>>();
    spawned_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(
        spawned_thread_ids.len(),
        1,
        "spawn_agent should add exactly one child thread"
    );
    spawned_thread_ids
        .pop()
        .expect("spawned thread id should be present")
}

async fn install_role_with_model_override(turn: &mut TurnContext) -> String {
    let role_name = "fork-context-role".to_string();
    tokio::fs::create_dir_all(&turn.config.codex_home)
        .await
        .expect("codex home should be created");
    let role_config_path = turn
        .config
        .codex_home
        .as_path()
        .join("fork-context-role.toml");
    tokio::fs::write(
        &role_config_path,
        r#"model = "gpt-5-role-override"
model_provider = "ollama"
model_reasoning_effort = "minimal"
"#,
    )
    .await
    .expect("role config should be written");

    let mut config = (*turn.config).clone();
    config.agent_roles.insert(
        role_name.clone(),
        AgentRoleConfig {
            description: Some("Role with model overrides".to_string()),
            config_file: Some(role_config_path),
            nickname_candidates: None,
        },
    );
    turn.config = Arc::new(config);

    role_name
}

async fn start_supervised_goal_parent_for_test() -> (
    ThreadManager,
    ThreadId,
    Arc<crate::session::session::Session>,
    crate::StateDbHandle,
    String,
    codex_protocol::protocol::ThreadGoal,
) {
    let (_session, turn) = make_session_and_context().await;
    let mut config = turn.config.as_ref().clone();
    config
        .features
        .enable(Feature::Goals)
        .expect("test config should allow goals feature update");
    config
        .features
        .enable(Feature::GoalSupervisor)
        .expect("test config should allow goal supervisor feature update");
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow multi-agent v2 feature update");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow sqlite feature update");
    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let parent = manager
        .start_thread(config)
        .await
        .expect("parent thread should start");
    let parent_thread_id = parent.thread_id;
    let parent_session = parent.thread.codex.session.clone();
    let parent_metadata = codex_state::ThreadMetadataBuilder::new(
        parent_thread_id,
        parent_session
            .get_config()
            .await
            .codex_home
            .join(format!("{parent_thread_id}.jsonl"))
            .to_path_buf(),
        chrono::Utc::now(),
        SessionSource::Exec,
    )
    .build("openai");
    state_db
        .upsert_thread(&parent_metadata)
        .await
        .expect("parent thread metadata should exist before inserting a goal");
    let goal = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_thread_id,
            "Finish the supervised goal",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("goal should be created");
    let protocol_goal = crate::goals::protocol_goal_from_state(goal.clone());

    (
        manager,
        parent_thread_id,
        parent_session,
        state_db,
        goal.goal_id,
        protocol_goal,
    )
}

async fn start_goal_supervisor_helper_for_test() -> (
    ThreadManager,
    ThreadId,
    Arc<crate::session::session::Session>,
    crate::StateDbHandle,
    String,
) {
    let (manager, parent_thread_id, parent_session, state_db, goal_id, protocol_goal) =
        start_supervised_goal_parent_for_test().await;
    let before_thread_ids = manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_session,
        &goal_id,
        &protocol_goal,
    )
    .await
    .expect("supervisor helper should start");

    let helper_thread_id = spawned_thread_id_after(&manager, &before_thread_ids).await;
    let helper = manager
        .get_thread(helper_thread_id)
        .await
        .expect("supervisor helper thread should exist");

    (
        manager,
        parent_thread_id,
        helper.codex.session.clone(),
        state_db,
        goal_id,
    )
}

#[tokio::test]
async fn goal_supervisor_checkin_spawns_hidden_full_history_helper_once() {
    let (manager, parent_thread_id, helper_session, state_db, goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;
    let parent_thread = manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent thread should exist");
    let parent_session = parent_thread.codex.session.clone();

    // Goal supervisors are implementation details, but they must still be
    // full-history forks with the parent's prompt cache key. If this diverges,
    // the supervisor request gets a different prompt prefix from its parent and
    // loses the cache win that replaced watchdog pseudo-agents.
    assert_eq!(
        helper_session.prompt_cache_key(),
        parent_session.prompt_cache_key()
    );
    assert_eq!(
        manager
            .agent_control()
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await,
        Some(parent_thread_id)
    );

    let snapshot = manager
        .agent_control()
        .get_agent_config_snapshot(helper_thread_id)
        .await
        .expect("helper config snapshot should exist");
    match snapshot.session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: actual_parent_thread_id,
            depth,
            agent_path,
            agent_nickname,
            agent_role,
        }) => {
            assert_eq!(actual_parent_thread_id, parent_thread_id);
            assert_eq!(depth, 1);
            assert_eq!(
                agent_path,
                Some(AgentPath::try_from("/root/goal_supervisor").expect("supervisor path"))
            );
            assert!(
                agent_nickname.is_some(),
                "AgentControl assigns a nickname to every ThreadSpawn entry, including hidden goal supervisor helpers"
            );
            assert_eq!(
                agent_role,
                Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string())
            );
        }
        other => panic!("supervisor helper should use ThreadSpawn source, got {other:?}"),
    }

    let list_output = ListAgentsHandlerV2
        .handle(invocation(
            parent_session.clone(),
            parent_session.new_default_turn().await,
            "list_agents",
            function_payload(json!({})),
        ))
        .await
        .expect("list_agents should succeed");
    let (list_content, success) = expect_text_output(list_output);
    let result: ListAgentsResult =
        serde_json::from_str(&list_content).expect("list_agents result should be json");
    assert_eq!(success, Some(true));
    assert_eq!(
        result
            .agents
            .iter()
            .map(|agent| agent.agent_name.as_str())
            .collect::<Vec<_>>(),
        vec!["/root"],
        "goal supervisor helpers must stay hidden from model-visible agent listings"
    );

    let before_second_checkin = manager.list_thread_ids().await;
    let goal = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await
        .expect("goal should read")
        .expect("goal should exist");
    let protocol_goal = crate::goals::protocol_goal_from_state(goal);
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_session,
        &goal_id,
        &protocol_goal,
    )
    .await
    .expect("duplicate check-in should be a no-op while helper is active");
    assert_eq!(
        manager.list_thread_ids().await,
        before_second_checkin,
        "goal supervisor must not spawn duplicate helper threads while an active helper is still running"
    );
}

#[tokio::test]
#[serial]
async fn materialized_ephemeral_parent_starts_goal_supervisor_checkin() {
    struct EnvVarGuard {
        key: &'static str,
        original: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.original {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    let _materialize_ephemeral_rollouts =
        EnvVarGuard::set("CODEX_MATERIALIZE_EPHEMERAL_ROLLOUTS", "1");
    let (_session, turn) = make_session_and_context().await;
    let mut config = turn.config.as_ref().clone();
    config.ephemeral = true;
    config
        .features
        .enable(Feature::Goals)
        .expect("test config should allow goals feature update");
    config
        .features
        .enable(Feature::GoalSupervisor)
        .expect("test config should allow goal supervisor feature update");
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow multi-agent v2 feature update");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow sqlite feature update");
    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let parent = manager
        .start_thread(config)
        .await
        .expect("materialized ephemeral parent thread should start");
    let parent_thread_id = parent.thread_id;
    let parent_session = parent.thread.codex.session.clone();
    parent_session.ensure_rollout_materialized().await;
    let rollout_path = parent_session
        .current_rollout_path()
        .await
        .expect("materialized ephemeral thread should report rollout path")
        .expect("materialized ephemeral thread should have rollout path");
    let parent_metadata = codex_state::ThreadMetadataBuilder::new(
        parent_thread_id,
        rollout_path,
        chrono::Utc::now(),
        SessionSource::Exec,
    )
    .build("openai");
    state_db
        .upsert_thread(&parent_metadata)
        .await
        .expect("materialized ephemeral parent metadata should exist before inserting a goal");
    let goal = parent_session
        .set_thread_goal(
            parent_session.new_default_turn().await.as_ref(),
            crate::goals::SetGoalRequest {
                objective: Some("Finish the supervised goal".to_string()),
                status: None,
                token_budget: None,
            },
        )
        .await
        .expect("goal should be created");
    assert!(
        parent_session
            .state_db_for_thread_goals()
            .await
            .expect("materialized ephemeral goal state db lookup should succeed")
            .is_some(),
        "regression guard: app-backed forked agents are ephemeral but materialized; their active goals must still spawn cache-aligned supervisor helpers"
    );
    let goal_id = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await
        .expect("materialized ephemeral parent goal should read")
        .expect("materialized ephemeral parent goal should exist")
        .goal_id;

    let before_thread_ids = manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent_session, &goal_id, &goal)
        .await
        .expect("materialized ephemeral goal supervisor check-in should succeed");

    let helper_thread_id = spawned_thread_id_after(&manager, &before_thread_ids).await;
    let helper = manager
        .get_thread(helper_thread_id)
        .await
        .expect("goal supervisor helper should spawn for materialized ephemeral parent");
    assert_eq!(
        helper.codex.session.prompt_cache_key(),
        parent_session.prompt_cache_key(),
        "regression guard: app-backed forked agents are ephemeral but materialized; their active goals must still spawn cache-aligned supervisor helpers"
    );
}

#[tokio::test]
async fn goal_supervisor_plain_completion_reschedules_checkin() {
    let (manager, parent_thread_id, helper_session, _state_db, _goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;
    let before_finish = manager.list_thread_ids().await;

    assert!(
        manager
            .agent_control()
            .finish_goal_supervisor_helper(helper_thread_id)
            .await,
        "completion of a goal supervisor helper should clear the active helper"
    );
    assert_eq!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );

    timeout(Duration::from_secs(3), async {
        loop {
            let thread_ids = manager.list_thread_ids().await;
            if thread_ids.iter().any(|id| !before_finish.contains(id)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect(
        "a goal supervisor helper that exits without a supervisor tool must schedule the next check-in",
    );

    let after_finish = manager.list_thread_ids().await;
    let new_helper_id = after_finish
        .into_iter()
        .find(|id| !before_finish.contains(id))
        .expect("rescheduled supervisor helper should exist");
    assert_eq!(
        manager
            .agent_control()
            .goal_supervisor_parent_for_helper(new_helper_id)
            .await,
        Some(parent_thread_id)
    );
}

fn expect_text_output<T>(output: T) -> (String, Option<bool>)
where
    T: ToolOutput,
{
    let response = output.to_response_item(
        "call-1",
        &ToolPayload::Function {
            arguments: "{}".to_string(),
        },
    );
    match response {
        ResponseInputItem::FunctionCallOutput { output, .. }
        | ResponseInputItem::CustomToolCallOutput { output, .. } => {
            let content = match output.body {
                FunctionCallOutputBody::Text(text) => text,
                FunctionCallOutputBody::ContentItems(items) => {
                    codex_protocol::models::function_call_output_content_items_to_text(&items)
                        .unwrap_or_default()
                }
            };
            (content, output.success)
        }
        other => panic!("expected function output, got {other:?}"),
    }
}

#[derive(Debug, Deserialize)]
struct ListAgentsResult {
    agents: Vec<ListedAgentResult>,
}

#[derive(Debug, Deserialize)]
struct ListedAgentResult {
    agent_name: String,
    agent_status: serde_json::Value,
    last_task_message: Option<String>,
}

#[tokio::test]
async fn handler_rejects_non_function_payloads() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        ToolPayload::Custom {
            input: "hello".to_string(),
        },
    );
    let Err(err) = SpawnAgentHandler::default().handle(invocation).await else {
        panic!("payload should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "collab handler received unsupported payload".to_string()
        )
    );
}

#[tokio::test]
async fn spawn_agent_rejects_empty_message() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({"message": "   "})),
    );
    let Err(err) = SpawnAgentHandler::default().handle(invocation).await else {
        panic!("empty message should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("Empty message can't be sent to an agent".to_string())
    );
}

#[tokio::test]
async fn spawn_agent_rejects_when_message_and_items_are_both_set() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({
            "message": "hello",
            "items": [{"type": "mention", "name": "drive", "path": "app://drive"}]
        })),
    );
    let Err(err) = SpawnAgentHandler::default().handle(invocation).await else {
        panic!("message+items should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Provide either message or items, but not both".to_string()
        )
    );
}

#[tokio::test]
async fn spawn_agent_uses_explorer_role_and_preserves_approval_policy() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
        nickname: Option<String>,
    }

    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let agent_control = manager.agent_control();
    session.services.agent_control = agent_control.clone();
    let mut config = (*turn.config).clone();
    let provider_info =
        built_in_model_providers(/* openai_base_url */ /*openai_base_url*/ None)["ollama"].clone();
    config.model_provider_id = "ollama".to_string();
    config.model_provider = provider_info.clone();
    config
        .permissions
        .approval_policy
        .set(AskForApproval::OnRequest)
        .expect("approval policy should be set");
    turn.approval_policy
        .set(AskForApproval::OnRequest)
        .expect("approval policy should be set");
    turn.provider = create_model_provider(provider_info, turn.auth_manager.clone());
    turn.config = Arc::new(config);

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({
            "message": "inspect this repo",
            "agent_type": "explorer"
        })),
    );
    let output = SpawnAgentHandler::default()
        .handle(invocation)
        .await
        .expect("spawn_agent should succeed");
    let (content, _) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn_agent result should be json");
    let agent_id = parse_agent_id(&result.agent_id);
    assert!(
        result
            .nickname
            .as_deref()
            .is_some_and(|nickname| !nickname.is_empty())
    );
    let snapshot = manager
        .get_thread(agent_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;
    assert_eq!(snapshot.approval_policy, AskForApproval::OnRequest);
    assert_eq!(snapshot.model_provider_id, "ollama");
}

#[tokio::test]
async fn spawn_agent_fork_context_rejects_agent_type_override() {
    let (mut session, mut turn) = make_session_and_context().await;
    let role_name = install_role_with_model_override(&mut turn).await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let err = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "agent_type": role_name,
                "fork_context": true
            })),
        ))
        .await
        .err()
        .expect("fork_context should reject agent_type overrides");

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Full-history forked agents inherit the parent agent type, model, and reasoning effort; omit agent_type, model, and reasoning_effort, or spawn without a full-history fork.".to_string(),
        )
    );
}

#[tokio::test]
async fn spawn_agent_fork_context_rejects_child_model_overrides() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;

    let err = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "model": "gpt-5-child-override",
                "reasoning_effort": "low",
                "fork_context": true
            })),
        ))
        .await
        .err()
        .expect("forked spawn should reject child model overrides");

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Full-history forked agents inherit the parent agent type, model, and reasoning effort; omit agent_type, model, and reasoning_effort, or spawn without a full-history fork.".to_string(),
        )
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_fork_turns_all_rejects_agent_type_override() {
    let (mut session, mut turn) = make_session_and_context().await;
    let role_name = install_role_with_model_override(&mut turn).await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    let turn = TurnContext {
        config: Arc::new(config),
        ..turn
    };

    let err = SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "fork_context_v2",
                "agent_type": role_name,
                "fork_turns": "all"
            })),
        ))
        .await
        .err()
        .expect("fork_turns=all should reject agent_type overrides");

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Full-history forked agents inherit the parent agent type, model, and reasoning effort; omit agent_type, model, and reasoning_effort, or spawn without a full-history fork.".to_string(),
        )
    );
}

#[tokio::test]
async fn multi_agent_v2_forked_spawn_starts_with_user_input() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "answer the question.",
                "task_name": "factorial_sum_agent",
                "fork_turns": "all"
            })),
        ))
        .await
        .expect("forked spawn should succeed");

    assert!(manager.captured_ops().into_iter().any(|(thread_id, op)| {
        thread_id != root.thread_id
            && matches!(
                op,
                Op::UserInput { items, .. }
                    if items == vec![UserInput::Text {
                        text: "answer the question.".to_string(),
                        text_elements: Vec::new(),
                    }]
            )
    }));
}

#[tokio::test]
async fn multi_agent_v2_spawn_defaults_to_full_fork_and_rejects_child_model_overrides() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let err = SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "fork_context_v2",
                "model": "gpt-5-child-override",
                "reasoning_effort": "low"
            })),
        ))
        .await
        .err()
        .expect("default full fork should reject child model overrides");

    assert_eq!(
        err,
            FunctionCallError::RespondToModel(
            "Full-history forked agents inherit the parent agent type, model, and reasoning effort; omit agent_type, model, and reasoning_effort, or spawn without a full-history fork.".to_string(),
        )
    );
}

#[tokio::test]
async fn spawn_agent_service_tier_override_validates_the_effective_child_model() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
    }

    {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        let root = manager
            .start_thread((*turn.config).clone())
            .await
            .expect("root thread should start");
        session.services.agent_control = manager.agent_control();
        session.conversation_id = root.thread_id;

        let output = SpawnAgentHandler::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "inspect this repo",
                    "model": "gpt-5.4",
                    "service_tier": ServiceTier::Fast.request_value()
                })),
            ))
            .await
            .expect("spawn_agent should accept a supported explicit service tier");
        let (content, _) = expect_text_output(output);
        let result: SpawnAgentResult =
            serde_json::from_str(&content).expect("spawn_agent result should be json");
        let snapshot = manager
            .get_thread(parse_agent_id(&result.agent_id))
            .await
            .expect("spawned agent thread should exist")
            .config_snapshot()
            .await;

        assert_eq!(
            snapshot.service_tier,
            Some(ServiceTier::Fast.request_value().to_string())
        );
    }

    {
        let (session, turn) = make_session_and_context().await;
        let err = SpawnAgentHandler::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "inspect this repo",
                    "model": "gpt-5.4",
                    "service_tier": "turbo"
                })),
            ))
            .await
            .err()
            .expect("unknown service tier should be rejected");

        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "Service tier `turbo` is not supported for model `gpt-5.4`. Supported service tiers: priority"
                    .to_string()
            )
        );
    }

    {
        let (session, turn) = make_session_and_context().await;
        let err = SpawnAgentHandler::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "inspect this repo",
                    "model": "gpt-5.3-codex",
                    "service_tier": ServiceTier::Fast.request_value()
                })),
            ))
            .await
            .err()
            .expect("tier unsupported by the final child model should be rejected");

        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "Service tier `priority` is not supported for model `gpt-5.3-codex`. Supported service tiers: none"
                    .to_string()
            )
        );
    }
}

#[tokio::test]
async fn spawn_agent_service_tier_inheritance_preserves_supported_or_configured_tiers() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
    }

    {
        let (mut session, turn) = make_session_and_context().await;
        let mut turn = turn
            .with_model("gpt-5.4".to_string(), &session.services.models_manager)
            .await;
        let mut config = (*turn.config).clone();
        config.service_tier = Some(ServiceTier::Fast.request_value().to_string());
        turn.config = Arc::new(config);
        let manager = thread_manager();
        let root = manager
            .start_thread((*turn.config).clone())
            .await
            .expect("root thread should start");
        session.services.agent_control = manager.agent_control();
        session.conversation_id = root.thread_id;

        let output = SpawnAgentHandler::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({"message": "inspect this repo"})),
            ))
            .await
            .expect("spawn_agent should inherit a supported parent service tier");
        let (content, _) = expect_text_output(output);
        let result: SpawnAgentResult =
            serde_json::from_str(&content).expect("spawn_agent result should be json");
        let snapshot = manager
            .get_thread(parse_agent_id(&result.agent_id))
            .await
            .expect("spawned agent thread should exist")
            .config_snapshot()
            .await;

        assert_eq!(
            snapshot.service_tier,
            Some(ServiceTier::Fast.request_value().to_string())
        );
    }

    {
        let (mut session, turn) = make_session_and_context().await;
        let mut turn = turn
            .with_model("gpt-5.4".to_string(), &session.services.models_manager)
            .await;
        let mut config = (*turn.config).clone();
        config.service_tier = Some(ServiceTier::Fast.request_value().to_string());
        turn.config = Arc::new(config);
        let manager = thread_manager();
        let root = manager
            .start_thread((*turn.config).clone())
            .await
            .expect("root thread should start");
        session.services.agent_control = manager.agent_control();
        session.conversation_id = root.thread_id;

        let output = SpawnAgentHandler::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "inspect this repo",
                    "model": "gpt-5.3-codex"
                })),
            ))
            .await
            .expect("spawn_agent should clear unsupported inherited service tier");
        let (content, _) = expect_text_output(output);
        let result: SpawnAgentResult =
            serde_json::from_str(&content).expect("spawn_agent result should be json");
        let snapshot = manager
            .get_thread(parse_agent_id(&result.agent_id))
            .await
            .expect("spawned agent thread should exist")
            .config_snapshot()
            .await;

        assert_eq!(snapshot.service_tier, None);
    }

    {
        let (mut session, mut turn) = make_session_and_context().await;
        tokio::fs::create_dir_all(&turn.config.codex_home)
            .await
            .expect("codex home should be created");
        let role_config_path = turn
            .config
            .codex_home
            .as_path()
            .join("service-tier-role.toml");
        tokio::fs::write(
            &role_config_path,
            r#"model = "gpt-5.4"
service_tier = "priority"
"#,
        )
        .await
        .expect("role config should be written");

        let role_name = "service-tier-role".to_string();
        let mut config = (*turn.config).clone();
        config.agent_roles.insert(
            role_name.clone(),
            AgentRoleConfig {
                description: Some("Role with a child service tier".to_string()),
                config_file: Some(role_config_path),
                nickname_candidates: None,
            },
        );
        turn.config = Arc::new(config);
        let manager = thread_manager();
        let root = manager
            .start_thread((*turn.config).clone())
            .await
            .expect("root thread should start");
        session.services.agent_control = manager.agent_control();
        session.conversation_id = root.thread_id;

        let output = SpawnAgentHandler::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "inspect this repo",
                    "agent_type": role_name
                })),
            ))
            .await
            .expect("spawn_agent should preserve the child role service tier");
        let (content, _) = expect_text_output(output);
        let result: SpawnAgentResult =
            serde_json::from_str(&content).expect("spawn_agent result should be json");
        let snapshot = manager
            .get_thread(parse_agent_id(&result.agent_id))
            .await
            .expect("spawned agent thread should exist")
            .config_snapshot()
            .await;

        assert_eq!(
            snapshot.service_tier,
            Some(ServiceTier::Fast.request_value().to_string())
        );
    }
}

#[tokio::test]
async fn spawn_agent_role_service_tier_falls_back_to_supported_parent_tier() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
    }

    let (mut session, turn) = make_session_and_context().await;
    let mut turn = turn
        .with_model("gpt-5.4".to_string(), &session.services.models_manager)
        .await;
    tokio::fs::create_dir_all(&turn.config.codex_home)
        .await
        .expect("codex home should be created");
    let role_config_path = turn.config.codex_home.as_path().join("tiered-role.toml");
    tokio::fs::write(
        &role_config_path,
        r#"model = "gpt-5.4"
service_tier = "turbo"
"#,
    )
    .await
    .expect("role config should be written");

    let role_name = "tiered-role".to_string();
    let mut config = (*turn.config).clone();
    config.service_tier = Some(ServiceTier::Fast.request_value().to_string());
    config.agent_roles.insert(
        role_name.clone(),
        AgentRoleConfig {
            description: Some("Role with an unsupported child tier".to_string()),
            config_file: Some(role_config_path),
            nickname_candidates: None,
        },
    );
    turn.config = Arc::new(config);
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "agent_type": role_name
            })),
        ))
        .await
        .expect("spawn_agent should fall back to the supported parent tier");
    let (content, _) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn_agent result should be json");
    let snapshot = manager
        .get_thread(parse_agent_id(&result.agent_id))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    assert_eq!(
        snapshot.service_tier,
        Some(ServiceTier::Fast.request_value().to_string())
    );
}

#[tokio::test]
async fn spawn_agent_role_service_tier_does_not_hide_invalid_spawn_request() {
    let (session, mut turn) = make_session_and_context().await;
    tokio::fs::create_dir_all(&turn.config.codex_home)
        .await
        .expect("codex home should be created");
    let role_config_path = turn.config.codex_home.as_path().join("tiered-role.toml");
    tokio::fs::write(
        &role_config_path,
        r#"model = "gpt-5.4"
service_tier = "priority"
"#,
    )
    .await
    .expect("role config should be written");

    let role_name = "tiered-role".to_string();
    let mut config = (*turn.config).clone();
    config.agent_roles.insert(
        role_name.clone(),
        AgentRoleConfig {
            description: Some("Role with a supported child tier".to_string()),
            config_file: Some(role_config_path),
            nickname_candidates: None,
        },
    );
    turn.config = Arc::new(config);

    let result = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "agent_type": role_name,
                "service_tier": "turbo"
            })),
        ))
        .await;

    assert_eq!(
        result.err(),
        Some(FunctionCallError::RespondToModel(
            "Service tier `turbo` is not supported for model `gpt-5.4`. Supported service tiers: priority"
                .to_string()
        ))
    );
}

#[tokio::test]
async fn spawn_agent_full_history_fork_accepts_explicit_service_tier() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
    }

    let (mut session, turn) = make_session_and_context().await;
    let turn = turn
        .with_model("gpt-5.4".to_string(), &session.services.models_manager)
        .await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "fork_context": true,
                "service_tier": ServiceTier::Fast.request_value()
            })),
        ))
        .await
        .expect("full-history fork should accept explicit service tier");
    let (content, _) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn_agent result should be json");
    let snapshot = manager
        .get_thread(parse_agent_id(&result.agent_id))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    assert_eq!(
        snapshot.service_tier,
        Some(ServiceTier::Fast.request_value().to_string())
    );
}

#[tokio::test]
async fn multi_agent_v2_full_history_fork_accepts_explicit_service_tier() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        task_name: String,
    }

    let (mut session, turn) = make_session_and_context().await;
    let mut turn = turn
        .with_model("gpt-5.4".to_string(), &session.services.models_manager)
        .await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    let output = SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "fork_with_tier",
                "service_tier": ServiceTier::Fast.request_value()
            })),
        ))
        .await
        .expect("multi-agent v2 full-history fork should accept explicit service tier");
    let (content, _) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn_agent result should be json");
    let child_thread_id = session
        .services
        .agent_control
        .resolve_agent_reference(
            session.conversation_id,
            &turn.session_source,
            result.task_name.as_str(),
        )
        .await
        .expect("spawned task name should resolve");
    let snapshot = manager
        .get_thread(child_thread_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    assert_eq!(
        snapshot.service_tier,
        Some(ServiceTier::Fast.request_value().to_string())
    );
}

#[test]
fn multi_agent_v2_spawn_watchdog_role_is_rejected_in_goal_supervisor_mode() {
    run_large_stack_async(|| async {
        let (session, turn) = make_session_and_context().await;
        let Err(err) = SpawnAgentHandlerV2::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "check in later",
                    "task_name": "watchdog",
                    "agent_type": "watchdog"
                })),
            ))
            .await
        else {
            panic!("goal supervisor mode should reject public watchdog spawns");
        };

        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "watchdogs have been replaced by goal supervisor mode; use create_goal or /goal to supervise long-running work".to_string()
            )
        );
    });
}

#[test]
fn spawn_agent_watchdog_role_is_rejected_in_goal_supervisor_mode() {
    run_large_stack_async(|| async {
        let (session, turn) = make_session_and_context().await;
        let Err(err) = SpawnAgentHandler::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "check in later",
                    "agent_type": "watchdog"
                })),
            ))
            .await
        else {
            panic!("goal supervisor mode should reject public watchdog spawns");
        };

        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "watchdogs have been replaced by goal supervisor mode; use create_goal or /goal to supervise long-running work".to_string()
            )
        );
    });
}

#[test]
fn multi_agent_v2_spawn_partial_fork_turns_allows_agent_type_override() {
    run_large_stack_async(|| async {
        let (mut session, mut turn) = make_session_and_context().await;
        let role_name = install_role_with_model_override(&mut turn).await;
        let manager = thread_manager();
        let root = manager
            .start_thread((*turn.config).clone())
            .await
            .expect("root thread should start");
        session.services.agent_control = manager.agent_control();
        session.conversation_id = root.thread_id;
        let mut config = (*turn.config).clone();
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("test config should allow feature update");
        let turn = TurnContext {
            config: Arc::new(config),
            ..turn
        };

        let output = SpawnAgentHandlerV2::default()
            .handle(invocation(
                Arc::new(session),
                Arc::new(turn),
                "spawn_agent",
                function_payload(json!({
                    "message": "inspect this repo",
                    "task_name": "partial_fork",
                    "agent_type": role_name,
                    "fork_turns": "1"
                })),
            ))
            .await
            .expect("partial fork should allow agent_type overrides");
        let (content, _) = expect_text_output(output);
        let result: serde_json::Value =
            serde_json::from_str(&content).expect("spawn_agent result should be json");
        assert_eq!(result["task_name"], "/root/partial_fork");
        let agent_id = manager
            .captured_ops()
            .into_iter()
            .map(|(thread_id, _)| thread_id)
            .find(|thread_id| *thread_id != root.thread_id)
            .expect("spawned agent should receive an op");
        let snapshot = manager
            .get_thread(agent_id)
            .await
            .expect("spawned agent thread should exist")
            .config_snapshot()
            .await;

        assert_eq!(snapshot.model, "gpt-5-role-override");
        assert_eq!(snapshot.model_provider_id, "ollama");
        assert_eq!(snapshot.reasoning_effort, Some(ReasoningEffort::Minimal));
    });
}

#[tokio::test]
async fn spawn_agent_returns_agent_id_without_task_name() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: serde_json::Value =
        serde_json::from_str(&content).expect("spawn_agent result should be json");

    assert!(result["agent_id"].is_string());
    assert!(result.get("task_name").is_none());
    assert!(result.get("nickname").is_some());
    assert_eq!(success, Some(true));
}

#[tokio::test]
async fn supervisor_followup_task_parent_wakes_parent_and_finishes_helper() {
    let (manager, parent_thread_id, helper_session, _state_db, _goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;
    let helper_turn = helper_session.new_default_turn().await;

    let output = FollowupTaskHandlerV2
        .handle(invocation(
            helper_session.clone(),
            helper_turn.clone(),
            "followup_task",
            function_payload(json!({
                "target": "parent",
                "message": "continue the supervised goal"
            })),
        ))
        .await
        .expect("supervisor helper should wake its parent");
    assert!(
        output.terminal_no_response(),
        "regression guard: supervisor followup_task target=parent must end the helper turn without a function_call_output; otherwise the helper can keep sampling and send duplicate parent instructions"
    );
    let (_, success) = expect_text_output(output);

    assert_eq!(success, Some(true));
    assert_eq!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );
    let expected = InterAgentCommunication::new(
        AgentPath::try_from("/root/goal_supervisor").expect("supervisor path"),
        AgentPath::root(),
        Vec::new(),
        "continue the supervised goal".to_string(),
        /*trigger_turn*/ true,
    );
    assert!(manager.captured_ops().into_iter().any(|(thread_id, op)| {
        thread_id == parent_thread_id
            && matches!(op, Op::InterAgentCommunication { communication } if communication == expected)
    }));
    helper_session
        .send_event(
            helper_turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: helper_turn.sub_id.clone(),
                last_agent_message: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !manager.captured_ops().into_iter().any(|(thread_id, op)| {
            thread_id == parent_thread_id
                && matches!(
                    op,
                    Op::InterAgentCommunication { communication }
                        if communication.content.contains("<subagent_notification>")
                )
        }),
        "regression guard: a goal supervisor helper's terminal TurnComplete must not enqueue generic subagent_notification messages after followup_task target=parent"
    );
}

#[tokio::test]
async fn supervisor_followup_context_identifies_previous_supervisor_message() {
    let (manager, parent_thread_id, helper_session, _state_db, _goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_turn = helper_session.new_default_turn().await;
    let parent_thread = manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent thread should exist");
    let parent_session = parent_thread.codex.session.clone();
    let parent_goal = parent_session
        .state_db_for_thread_goals()
        .await
        .expect("goal state db should open")
        .expect("goal state db should exist")
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await
        .expect("goal should read")
        .map(crate::goals::protocol_goal_from_state)
        .expect("goal should exist");

    FollowupTaskHandlerV2
        .handle(invocation(
            helper_session.clone(),
            helper_turn,
            "followup_task",
            function_payload(json!({
                "target": "parent",
                "message": "continue the supervised goal"
            })),
        ))
        .await
        .expect("supervisor helper should wake its parent");

    let parent_turn = parent_session.new_default_turn().await;
    parent_session
        .send_event(
            parent_turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: parent_turn.sub_id.clone(),
                last_agent_message: Some("parent continued".to_string()),
                completed_at: Some(1_769_256_000),
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
    let continuity = crate::goal_supervisor::supervisor_continuity_context_item(
        &parent_session,
        &parent_goal,
        &[codex_protocol::protocol::RolloutItem::EventMsg(
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: parent_turn.sub_id.clone(),
                last_agent_message: Some("parent continued".to_string()),
                completed_at: Some(1_769_256_000),
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )],
    )
    .await;
    let codex_protocol::protocol::RolloutItem::ResponseItem(ResponseItem::Message {
        role,
        content,
        ..
    }) = continuity
    else {
        panic!("continuity context should be a developer message");
    };
    let [ContentItem::InputText { text }] = content.as_slice() else {
        panic!("continuity context should contain one text item");
    };
    assert_eq!(role, "developer");
    assert!(text.contains("# Goal Supervisor Continuity"));
    assert!(text.contains("\"supervisor_identity\": \"/root/goal_supervisor\""));
    assert!(text.contains("\"kind\": \"followup_task\""));
    assert!(text.contains("\"delivered_parent_message\""));
    assert!(text.contains("\"last_parent_message_at_utc\""));
}

#[tokio::test]
async fn supervisor_checkin_does_not_spawn_duplicate_helper_while_one_is_active() {
    let (manager, parent_thread_id, helper_session, state_db, goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let parent_session = manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent thread should exist")
        .codex
        .session
        .clone();
    let goal = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await
        .expect("goal should read")
        .expect("goal should exist");
    let protocol_goal = crate::goals::protocol_goal_from_state(goal);
    let mut before_thread_ids = manager.list_thread_ids().await;
    before_thread_ids.sort_by_key(ToString::to_string);

    // A goal can receive several idle signals before the supervisor helper
    // finishes. The second signal must observe `active_helper_id` and avoid
    // creating another internal helper, or the parent sees duplicate guidance
    // and prompt-cache behavior diverges between helpers.
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_session,
        &goal_id,
        &protocol_goal,
    )
    .await
    .expect("duplicate supervisor check-in should be a no-op");

    let mut after_thread_ids = manager.list_thread_ids().await;
    after_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(after_thread_ids, before_thread_ids);
    assert_ne!(
        manager
            .agent_control()
            .get_status(helper_session.conversation_id)
            .await,
        AgentStatus::NotFound
    );
}

#[tokio::test]
async fn supervisor_checkin_spawns_while_user_visible_subagent_is_running() {
    let (manager, parent_thread_id, parent_session, _state_db, goal_id, protocol_goal) =
        start_supervised_goal_parent_for_test().await;
    let worker_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(AgentPath::try_from("/root/worker").expect("worker path")),
        agent_nickname: None,
        agent_role: None,
    });
    manager
        .agent_control()
        .spawn_agent_with_metadata(
            (*parent_session.get_config().await).clone(),
            vec![UserInput::Text {
                text: "real worker task".to_string(),
                text_elements: Vec::new(),
            }]
            .into(),
            Some(worker_source),
            crate::agent::control::SpawnAgentOptions::default(),
        )
        .await
        .expect("worker should spawn");
    let mut before_thread_ids = manager.list_thread_ids().await;
    before_thread_ids.sort_by_key(ToString::to_string);

    // Goal supervisor check-ins must still run while real subagents are
    // active. The supervisor needs to inspect that live work and decide
    // whether to snooze, redirect, or wake the parent.
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_session,
        &goal_id,
        &protocol_goal,
    )
    .await
    .expect("supervisor check-in should spawn alongside real subagents");

    let mut after_thread_ids = manager.list_thread_ids().await;
    after_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(after_thread_ids.len(), before_thread_ids.len() + 1);
}

#[tokio::test]
async fn supervisor_checkin_honors_persisted_snooze_before_spawning_helper() {
    let (manager, parent_thread_id, parent_session, state_db, goal_id, protocol_goal) =
        start_supervised_goal_parent_for_test().await;
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            &goal_id,
            Some(chrono::Utc::now().timestamp_millis() + 60_000),
        )
        .await
        .expect("snooze state should persist");
    let mut before_thread_ids = manager.list_thread_ids().await;
    before_thread_ids.sort_by_key(ToString::to_string);

    // This covers the resume case: a new Session has no in-memory
    // `snoozed_until`, so the scheduler must consult
    // thread_goal_supervisor_state before creating another helper.
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_session,
        &goal_id,
        &protocol_goal,
    )
    .await
    .expect("persisted supervisor snooze should defer helper spawn");

    let mut after_thread_ids = manager.list_thread_ids().await;
    after_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(after_thread_ids, before_thread_ids);
}

#[tokio::test]
async fn supervisor_helper_is_hidden_from_list_agents() {
    let (manager, parent_thread_id, helper_session, _state_db, _goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let parent_session = manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent thread should exist")
        .codex
        .session
        .clone();

    let output = ListAgentsHandlerV2
        .handle(invocation(
            parent_session.clone(),
            parent_session.new_default_turn().await,
            "list_agents",
            function_payload(json!({})),
        ))
        .await
        .expect("list_agents should succeed");
    let (content, success) = expect_text_output(output);
    let result: ListAgentsResult =
        serde_json::from_str(&content).expect("list_agents result should be json");

    assert_eq!(success, Some(true));
    assert!(
        !result
            .agents
            .iter()
            .any(|agent| agent.agent_name == "/root/goal_supervisor"
                || agent.agent_name == helper_session.conversation_id.to_string()),
        "goal supervisor helpers are internal scheduling work and must not be targetable through list_agents"
    );
}

#[tokio::test]
async fn supervisor_send_message_parent_is_rejected_and_keeps_helper_active() {
    let (manager, _parent_thread_id, helper_session, _state_db, _goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;

    let Err(err) = SendMessageHandlerV2
        .handle(invocation(
            helper_session.clone(),
            helper_session.new_default_turn().await,
            "send_message",
            function_payload(json!({
                "target": "parent",
                "message": "queued supervisor update"
            })),
        ))
        .await
    else {
        panic!("supervisor helper send_message to parent should be rejected");
    };

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "supervisor check-in threads must use followup_task with target `parent` to message their parent."
                .to_string()
        )
    );
    assert_ne!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );
}

#[tokio::test]
async fn supervisor_snooze_finishes_helper_and_persists_snooze() {
    let (manager, parent_thread_id, helper_session, state_db, goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;
    let parent_thread = manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent thread should exist");

    let output = SupervisorSnoozeHandler
        .handle(invocation(
            helper_session.clone(),
            helper_session.new_default_turn().await,
            "snooze",
            function_payload(json!({
                "delay_seconds": 30,
                "reason": "worker is still active"
            })),
        ))
        .await
        .expect("supervisor helper should snooze");
    assert!(
        output.terminal_no_response(),
        "regression guard: supervisor.snooze must end the helper turn without a function_call_output; otherwise a snoozed helper can keep sampling and emit an extra ping"
    );
    let (content, success) = expect_text_output(output);
    let result: serde_json::Value = serde_json::from_str(&content).expect("result should be json");

    assert_eq!(success, Some(true));
    assert_eq!(result, json!({"delay_seconds": 30}));
    assert_eq!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );
    assert!(
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, &goal_id)
            .await
            .expect("snooze state should read")
            .is_some(),
        "supervisor.snooze must persist snooze state so a resumed session does not immediately retrigger"
    );
    assert!(
        !manager
            .captured_ops()
            .iter()
            .any(|(thread_id, op)| *thread_id == parent_thread_id
                && matches!(op, Op::InterAgentCommunication { .. })),
        "supervisor.snooze should not wake the parent"
    );
    let warning = timeout(Duration::from_secs(10), async {
        loop {
            let event = parent_thread
                .next_event()
                .await
                .expect("parent event stream should stay open");
            if let EventMsg::Warning(warning) = event.msg {
                break warning.message;
            }
        }
    })
    .await
    .expect("parent should receive supervisor snooze warning");
    assert_eq!(
        warning, "Supervisor snoozed for 30s.",
        "regression guard: legacy watchdogs showed a visible snooze cell; supervisor.snooze must carry that UI signal forward without waking the parent"
    );

    let before_idle_check = manager.list_thread_ids().await;
    parent_thread
        .codex
        .session
        .goal_runtime_apply(crate::goals::GoalRuntimeEvent::MaybeContinueIfIdle)
        .await
        .expect("idle goal check should succeed");
    assert_eq!(
        manager.list_thread_ids().await,
        before_idle_check,
        "persisted supervisor snooze state must prevent an immediate replacement helper"
    );

    let parent_goal = parent_thread
        .codex
        .session
        .state_db_for_thread_goals()
        .await
        .expect("goal state db should open")
        .expect("goal state db should exist")
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await
        .expect("goal should read")
        .map(crate::goals::protocol_goal_from_state)
        .expect("goal should exist");
    let continuity = crate::goal_supervisor::supervisor_continuity_context_item(
        &parent_thread.codex.session,
        &parent_goal,
        &[codex_protocol::protocol::RolloutItem::EventMsg(
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: "parent-turn".to_string(),
                last_agent_message: Some("parent continued".to_string()),
                completed_at: Some(0),
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )],
    )
    .await;
    let codex_protocol::protocol::RolloutItem::ResponseItem(ResponseItem::Message {
        content, ..
    }) = continuity
    else {
        panic!("continuity context should be a developer message");
    };
    let [ContentItem::InputText { text }] = content.as_slice() else {
        panic!("continuity context should contain one text item");
    };
    assert!(text.contains("\"kind\": \"snooze\""));
    assert!(text.contains("\"snooze_count_since_last_parent_message\": 1"));
    assert!(text.contains("\"snoozed_seconds_since_last_parent_message\": 30"));
}

#[tokio::test]
async fn supervisor_close_self_marks_goal_complete_notifies_parent_and_clears_snooze() {
    let (manager, parent_thread_id, helper_session, state_db, goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, &goal_id, Some(123_456))
        .await
        .expect("snooze state should persist before close");

    let output = SupervisorSelfCloseHandler
        .handle(invocation(
            helper_session.clone(),
            helper_session.new_default_turn().await,
            "close_self",
            function_payload(json!({"message": "goal is complete"})),
        ))
        .await
        .expect("supervisor helper should self-close");
    assert!(
        output.terminal_no_response(),
        "regression guard: supervisor.close_self must end the helper turn without a function_call_output; otherwise a closed helper can run another turn"
    );
    let (content, success) = expect_text_output(output);
    let result: serde_json::Value = serde_json::from_str(&content).expect("result should be json");

    assert_eq!(success, Some(true));
    assert_eq!(result, json!({"completed": true}));
    assert_eq!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );
    let goal = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await
        .expect("goal should read")
        .expect("goal should still exist");
    assert_eq!(goal.status, codex_state::ThreadGoalStatus::Complete);
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, &goal_id)
            .await
            .expect("snooze state should read"),
        None,
        "supervisor.close_self must clear persisted snooze state so a completed goal does not reschedule"
    );
    let parent_thread = manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent thread should exist");
    let parent_pending_input = parent_thread
        .codex
        .session
        .input_queue
        .get_pending_input(&parent_thread.codex.session.active_turn)
        .await;
    assert!(
        parent_pending_input.iter().any(|item| matches!(
            item,
            crate::session::TurnInput::ResponseItem(
                ResponseItem::Message { content, .. },
            )
                if InterAgentCommunication::from_message_content(content).is_some_and(|communication| {
                    communication.author.as_str() == "/root/goal_supervisor"
                        && communication.recipient == AgentPath::root()
                        && communication.other_recipients.is_empty()
                        && communication.content == "goal is complete"
                        && communication.trigger_turn
                })
        )),
        "supervisor.close_self with a message must queue a parent turn as /root/goal_supervisor; pending_input={parent_pending_input:#?}"
    );
    let before_idle_check = manager.list_thread_ids().await;
    parent_thread
        .codex
        .session
        .goal_runtime_apply(crate::goals::GoalRuntimeEvent::MaybeContinueIfIdle)
        .await
        .expect("idle goal check should succeed");
    assert_eq!(
        manager.list_thread_ids().await,
        before_idle_check,
        "completed goals must not schedule another supervisor helper after supervisor.close_self"
    );
}

#[tokio::test]
async fn supervisor_close_self_ignores_replaced_goal() {
    let (manager, parent_thread_id, helper_session, state_db, old_goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;
    let replacement_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_thread_id,
            "replacement goal",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("replacement goal should persist");

    let output = SupervisorSelfCloseHandler
        .handle(invocation(
            helper_session.clone(),
            helper_session.new_default_turn().await,
            "close_self",
            function_payload(json!({"message": "stale helper complete"})),
        ))
        .await
        .expect("stale supervisor helper should finish without completing the replacement goal");
    let (content, success) = expect_text_output(output);
    let result: serde_json::Value = serde_json::from_str(&content).expect("result should be json");

    assert_eq!(success, Some(true));
    assert_eq!(result, json!({"completed": false}));
    assert_eq!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );
    let current_goal = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await
        .expect("goal should read")
        .expect("replacement goal should still exist");
    assert_eq!(current_goal, replacement_goal);
    let parent_thread = manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent thread should exist");
    let parent_pending_input = parent_thread
        .codex
        .session
        .input_queue
        .get_pending_input(&parent_thread.codex.session.active_turn)
        .await;
    assert!(
        parent_pending_input.is_empty(),
        "stale supervisor.close_self must not send its final message to the parent after the goal has been replaced; pending_input={parent_pending_input:#?}"
    );
    assert_ne!(
        old_goal_id, replacement_goal.goal_id,
        "test setup must create a distinct replacement goal"
    );
}

#[tokio::test]
async fn supervisor_snooze_does_not_snooze_replacement_goal() {
    let (manager, parent_thread_id, helper_session, state_db, old_goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;
    let replacement_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_thread_id,
            "replacement goal",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("replacement goal should persist");

    let Err(err) = SupervisorSnoozeHandler
        .handle(invocation(
            helper_session.clone(),
            helper_session.new_default_turn().await,
            "snooze",
            function_payload(json!({"delay_seconds": 30})),
        ))
        .await
    else {
        panic!("stale supervisor helper should not snooze a replacement goal");
    };

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "supervisor.snooze is only available in goal supervisor check-in threads.".to_string()
        )
    );
    assert_eq!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(
                parent_thread_id,
                &replacement_goal.goal_id
            )
            .await
            .expect("replacement snooze state should read"),
        None,
        "stale supervisor.snooze must not persist snooze state for the replacement goal"
    );
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, &old_goal_id)
            .await
            .expect("old snooze state should read"),
        None,
        "stale supervisor.snooze must not persist snooze state for the old goal either"
    );
}

#[tokio::test]
async fn supervisor_compact_parent_context_submits_compaction_and_finishes_helper() {
    let (manager, parent_thread_id, helper_session, _state_db, _goal_id) =
        start_goal_supervisor_helper_for_test().await;
    let helper_thread_id = helper_session.conversation_id;

    let output = SupervisorCompactParentContextHandler
        .handle(invocation(
            helper_session.clone(),
            helper_session.new_default_turn().await,
            "compact_parent_context",
            function_payload(json!({
                "reason": "parent is repeating the same step",
                "evidence": "same command appeared in consecutive turns"
            })),
        ))
        .await
        .expect("supervisor helper should request parent compaction");
    assert!(
        output.terminal_no_response(),
        "regression guard: supervisor.compact_parent_context must end the helper turn without a function_call_output; otherwise a compacting helper can continue after scheduling compaction"
    );
    let (content, success) = expect_text_output(output);
    let result: serde_json::Value =
        serde_json::from_str(&content).expect("compact_parent_context result should be json");
    let submission_id = result
        .get("submission_id")
        .and_then(|value| value.as_str())
        .expect("submitted compaction should include a submission id");
    assert!(!submission_id.is_empty());

    assert_eq!(success, Some(true));
    assert_eq!(
        result,
        json!({
            "kind": "submitted",
            "parent_thread_id": parent_thread_id.to_string(),
            "submission_id": submission_id,
        })
    );
    assert_eq!(
        manager.agent_control().get_status(helper_thread_id).await,
        AgentStatus::NotFound
    );
    assert!(
        manager
            .captured_ops()
            .iter()
            .any(|(thread_id, op)| { *thread_id == parent_thread_id && matches!(op, Op::Compact) })
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_requires_task_name() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({
            "message": "inspect this repo"
        })),
    );
    let Err(err) = SpawnAgentHandlerV2::default().handle(invocation).await else {
        panic!("missing task_name should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = err else {
        panic!("missing task_name should surface as a model-facing error");
    };
    assert!(message.contains("missing field `task_name`"));
}

#[tokio::test]
async fn multi_agent_v2_spawn_rejects_legacy_items_field() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({
            "message": "inspect this repo",
            "items": [{"type": "text", "text": "inspect this repo"}],
            "task_name": "worker"
        })),
    );
    let Err(err) = SpawnAgentHandlerV2::default().handle(invocation).await else {
        panic!("legacy items field should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = err else {
        panic!("legacy items field should surface as a model-facing error");
    };
    assert!(message.contains("unknown field `items`"));
}

#[tokio::test]
async fn spawn_agent_errors_when_manager_dropped() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({"message": "hello"})),
    );
    let Err(err) = SpawnAgentHandler::default().handle(invocation).await else {
        panic!("spawn should fail without a manager");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("collab manager unavailable".to_string())
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_returns_path_and_send_message_accepts_relative_path() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        task_name: String,
        nickname: Option<String>,
    }

    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let session = Arc::new(session);
    let turn = Arc::new(turn);
    let spawn_output = SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "test_process"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");
    let (content, _) = expect_text_output(spawn_output);
    let spawn_result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn result should parse");
    assert_eq!(spawn_result.task_name, "/root/test_process");
    assert!(spawn_result.nickname.is_some());

    let child_thread_id = session
        .services
        .agent_control
        .resolve_agent_reference(
            session.conversation_id,
            &turn.session_source,
            "test_process",
        )
        .await
        .expect("relative path should resolve");
    let child_snapshot = manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist")
        .config_snapshot()
        .await;
    assert_eq!(
        child_snapshot.session_source.get_agent_path().as_deref(),
        Some("/root/test_process")
    );
    assert!(manager.captured_ops().iter().any(|(id, op)| {
        *id == child_thread_id
            && matches!(
                op,
                Op::InterAgentCommunication { communication }
                    if communication.author == AgentPath::root()
                        && communication.recipient.as_str() == "/root/test_process"
                        && communication.other_recipients.is_empty()
                        && communication.content == "inspect this repo"
                        && communication.trigger_turn
            )
    }));

    SendMessageHandlerV2
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "send_message",
            function_payload(json!({
                "target": "test_process",
                "message": "continue"
            })),
        ))
        .await
        .expect("send_message should accept v2 path");

    assert!(manager.captured_ops().iter().any(|(id, op)| {
        *id == child_thread_id
            && matches!(
                op,
                Op::InterAgentCommunication { communication }
                    if communication.author == AgentPath::root()
                        && communication.recipient.as_str() == "/root/test_process"
                        && communication.other_recipients.is_empty()
                        && communication.content == "continue"
                        && !communication.trigger_turn
            )
    }));
}

#[tokio::test]
async fn multi_agent_v2_spawn_rejects_legacy_fork_context() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let err = SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "worker",
                "fork_context": true
            })),
        ))
        .await
        .err()
        .expect("legacy fork_context should be rejected");

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "fork_context is not supported in MultiAgentV2; use fork_turns instead".to_string()
        )
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_rejects_invalid_fork_turns_string() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let err = SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "worker",
                "fork_turns": "banana"
            })),
        ))
        .await
        .err()
        .expect("invalid fork_turns should be rejected");

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "fork_turns must be `none`, `all`, or a positive integer string".to_string()
        )
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_rejects_zero_fork_turns() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let err = SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "worker",
                "fork_turns": "0"
            })),
        ))
        .await
        .err()
        .expect("zero turn count should be rejected");

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "fork_turns must be `none`, `all`, or a positive integer string".to_string()
        )
    );
}

#[tokio::test]
async fn multi_agent_v2_send_message_accepts_root_target_from_child() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let child_path = AgentPath::try_from("/root/worker").expect("agent path");
    let child_thread_id = session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            (*turn.config).clone(),
            vec![UserInput::Text {
                text: "inspect this repo".to_string(),
                text_elements: Vec::new(),
            }]
            .into(),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root.thread_id,
                depth: 1,
                agent_path: Some(child_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
            crate::agent::control::SpawnAgentOptions::default(),
        )
        .await
        .expect("worker spawn should succeed")
        .thread_id;
    session.conversation_id = child_thread_id;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 1,
        agent_path: Some(child_path.clone()),
        agent_nickname: None,
        agent_role: None,
    });

    SendMessageHandlerV2
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "send_message",
            function_payload(json!({
                "target": "/root",
                "message": "done"
            })),
        ))
        .await
        .expect("send_message should accept the root agent path");

    assert!(manager.captured_ops().iter().any(|(id, op)| {
        *id == root.thread_id
            && matches!(
                op,
                Op::InterAgentCommunication { communication }
                    if communication.author == child_path
                        && communication.recipient == AgentPath::root()
                        && communication.other_recipients.is_empty()
                        && communication.content == "done"
                        && !communication.trigger_turn
            )
    }));
}

#[tokio::test]
async fn multi_agent_v2_followup_task_rejects_root_target_from_child() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let child_path = AgentPath::try_from("/root/worker").expect("agent path");
    let child_thread_id = session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            (*turn.config).clone(),
            vec![UserInput::Text {
                text: "inspect this repo".to_string(),
                text_elements: Vec::new(),
            }]
            .into(),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root.thread_id,
                depth: 1,
                agent_path: Some(child_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
            crate::agent::control::SpawnAgentOptions::default(),
        )
        .await
        .expect("worker spawn should succeed")
        .thread_id;
    session.conversation_id = child_thread_id;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 1,
        agent_path: Some(child_path),
        agent_nickname: None,
        agent_role: None,
    });

    let Err(err) = FollowupTaskHandlerV2
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "followup_task",
            function_payload(json!({
                "target": "/root",
                "message": "run this",
            })),
        ))
        .await
    else {
        panic!("followup_task should reject the root target");
    };

    assert_eq!(
        err,
        FunctionCallError::RespondToModel("Tasks can't be assigned to the root agent".to_string())
    );
    let root_ops = manager
        .captured_ops()
        .into_iter()
        .filter_map(|(id, op)| (id == root.thread_id).then_some(op))
        .collect::<Vec<_>>();
    assert!(!root_ops.iter().any(|op| matches!(op, Op::Interrupt)));
    assert!(
        !root_ops
            .iter()
            .any(|op| matches!(op, Op::InterAgentCommunication { .. }))
    );
}

#[tokio::test]
async fn multi_agent_v2_followup_task_rejects_parent_target_from_non_supervisor_child() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let child_path = AgentPath::try_from("/root/worker").expect("agent path");
    let child_thread_id = session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            (*turn.config).clone(),
            vec![UserInput::Text {
                text: "inspect this repo".to_string(),
                text_elements: Vec::new(),
            }]
            .into(),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root.thread_id,
                depth: 1,
                agent_path: Some(child_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
            crate::agent::control::SpawnAgentOptions::default(),
        )
        .await
        .expect("worker spawn should succeed")
        .thread_id;
    session.conversation_id = child_thread_id;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 1,
        agent_path: Some(child_path),
        agent_nickname: None,
        agent_role: None,
    });

    let Err(err) = FollowupTaskHandlerV2
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "followup_task",
            function_payload(json!({
                "target": "parent",
                "message": "wake up"
            })),
        ))
        .await
    else {
        panic!("non-supervisor followup_task should reject the direct parent target");
    };

    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Only supervisor check-in threads can use followup_task with target `parent`; use send_message for parent updates."
                .to_string()
        )
    );
}

#[tokio::test]
async fn multi_agent_v2_list_agents_returns_completed_status_and_last_task_message() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    turn.config = Arc::new(config);

    let session = Arc::new(session);
    let turn = Arc::new(turn);
    let spawn_output = SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");
    let _ = expect_text_output(spawn_output);

    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker path should resolve");
    let child_thread = manager
        .get_thread(agent_id)
        .await
        .expect("child thread should exist");
    let child_turn = child_thread.codex.session.new_default_turn().await;
    child_thread
        .codex
        .session
        .send_event(
            child_turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: child_turn.sub_id.clone(),
                last_agent_message: Some("done".to_string()),
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;

    let output = ListAgentsHandlerV2
        .handle(invocation(
            session,
            turn,
            "list_agents",
            function_payload(json!({})),
        ))
        .await
        .expect("list_agents should succeed");
    let (content, success) = expect_text_output(output);
    let result: ListAgentsResult =
        serde_json::from_str(&content).expect("list_agents result should be json");

    let agent_names = result
        .agents
        .iter()
        .map(|agent| agent.agent_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(agent_names, vec!["/root", "/root/worker"]);
    let root_agent = result
        .agents
        .iter()
        .find(|agent| agent.agent_name == "/root")
        .expect("root agent should be listed");
    assert_eq!(root_agent.last_task_message.as_deref(), Some("Main thread"));
    let worker = result
        .agents
        .iter()
        .find(|agent| agent.agent_name == "/root/worker")
        .expect("worker agent should be listed");
    assert_eq!(worker.agent_status, json!({"completed": "done"}));
    assert_eq!(
        worker.last_task_message.as_deref(),
        Some("inspect this repo")
    );
    assert_eq!(success, Some(true));
}

#[tokio::test]
async fn multi_agent_v2_list_agents_filters_by_relative_path_prefix() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    turn.config = Arc::new(config.clone());

    let researcher_path = AgentPath::from_string("/root/researcher".to_string()).expect("path");
    let worker_path = AgentPath::from_string("/root/researcher/worker".to_string()).expect("path");
    session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            config.clone(),
            vec![UserInput::Text {
                text: "research".to_string(),
                text_elements: Vec::new(),
            }]
            .into(),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root.thread_id,
                depth: 1,
                agent_path: Some(researcher_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
            crate::agent::control::SpawnAgentOptions::default(),
        )
        .await
        .expect("researcher agent should spawn");
    session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            config,
            vec![UserInput::Text {
                text: "build".to_string(),
                text_elements: Vec::new(),
            }]
            .into(),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root.thread_id,
                depth: 2,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
            crate::agent::control::SpawnAgentOptions::default(),
        )
        .await
        .expect("worker agent should spawn");

    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 1,
        agent_path: Some(researcher_path),
        agent_nickname: None,
        agent_role: None,
    });

    let output = ListAgentsHandlerV2
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "list_agents",
            function_payload(json!({
                "path_prefix": "worker"
            })),
        ))
        .await
        .expect("list_agents should succeed");
    let (content, _) = expect_text_output(output);
    let result: ListAgentsResult =
        serde_json::from_str(&content).expect("list_agents result should be json");

    assert_eq!(result.agents.len(), 1);
    assert_eq!(result.agents[0].agent_name, worker_path.as_str());
    assert_eq!(result.agents[0].last_task_message.as_deref(), Some("build"));
}

#[tokio::test]
async fn multi_agent_v2_list_agents_omits_closed_agents() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    turn.config = Arc::new(config);

    let session = Arc::new(session);
    let turn = Arc::new(turn);
    let spawn_output = SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");
    let _ = expect_text_output(spawn_output);

    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker path should resolve");
    session
        .services
        .agent_control
        .close_agent(agent_id)
        .await
        .expect("close_agent should succeed");

    let output = ListAgentsHandlerV2
        .handle(invocation(
            session,
            turn,
            "list_agents",
            function_payload(json!({})),
        ))
        .await
        .expect("list_agents should succeed");
    let (content, _) = expect_text_output(output);
    let result: ListAgentsResult =
        serde_json::from_str(&content).expect("list_agents result should be json");

    assert_eq!(result.agents.len(), 1);
    assert_eq!(result.agents[0].agent_name, "/root");
    assert_eq!(
        result.agents[0].last_task_message.as_deref(),
        Some("Main thread")
    );
}

#[tokio::test]
async fn multi_agent_v2_send_message_rejects_legacy_items_field() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = turn.config.as_ref().clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "boot worker",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
    let invocation = invocation(
        session,
        turn,
        "send_message",
        function_payload(json!({
            "target": agent_id.to_string(),
            "items": [
                {"type": "mention", "name": "drive", "path": "app://google_drive"},
                {"type": "text", "text": "read the folder"}
            ]
        })),
    );

    let Err(err) = SendMessageHandlerV2.handle(invocation).await else {
        panic!("legacy items field should be rejected in v2");
    };
    let FunctionCallError::RespondToModel(message) = err else {
        panic!("legacy items field should surface as a model-facing error");
    };
    assert!(message.contains("unknown field `items`"));
}

#[tokio::test]
async fn multi_agent_v2_send_message_rejects_interrupt_parameter() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = turn.config.as_ref().clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "boot worker",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");

    let invocation = invocation(
        session,
        turn,
        "send_message",
        function_payload(json!({
            "target": agent_id.to_string(),
            "message": "continue",
            "interrupt": true
        })),
    );

    let Err(err) = SendMessageHandlerV2.handle(invocation).await else {
        panic!("send_message interrupt parameter should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = err else {
        panic!("expected model-facing parse error");
    };
    assert!(message.starts_with(
        "failed to parse function arguments: unknown field `interrupt`, expected `target` or `message`"
    ));

    let ops = manager.captured_ops();
    let ops_for_agent: Vec<&Op> = ops
        .iter()
        .filter_map(|(id, op)| (*id == agent_id).then_some(op))
        .collect();
    assert!(!ops_for_agent.iter().any(|op| matches!(op, Op::Interrupt)));
    assert!(!ops_for_agent.iter().any(|op| matches!(
        op,
        Op::InterAgentCommunication { communication }
            if communication.author == AgentPath::root()
                && communication.recipient.as_str() == "/root/worker"
                && communication.other_recipients.is_empty()
                && communication.content == "continue"
                && !communication.trigger_turn
    )));
}

#[test]
fn multi_agent_v2_followup_task_completion_notifies_parent_on_every_turn() {
    run_large_stack_async(|| async {
        let (mut session, mut turn) = make_session_and_context().await;
        let manager = thread_manager();
        let root = manager
            .start_thread((*turn.config).clone())
            .await
            .expect("root thread should start");
        session.services.agent_control = manager.agent_control();
        session.conversation_id = root.thread_id;
        let mut config = turn.config.as_ref().clone();
        let _ = config.features.enable(Feature::MultiAgentV2);
        turn.config = Arc::new(config);
        let session = Arc::new(session);
        let turn = Arc::new(turn);

        SpawnAgentHandlerV2::default()
            .handle(invocation(
                session.clone(),
                turn.clone(),
                "spawn_agent",
                function_payload(json!({
                    "message": "boot worker",
                    "task_name": "worker"
                })),
            ))
            .await
            .expect("spawn worker");
        let agent_id = session
            .services
            .agent_control
            .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
            .await
            .expect("worker should resolve");
        let thread = manager
            .get_thread(agent_id)
            .await
            .expect("worker thread should exist");
        let worker_path = AgentPath::try_from("/root/worker").expect("worker path");

        let first_turn = thread.codex.session.new_default_turn().await;
        thread
            .codex
            .session
            .send_event(
                first_turn.as_ref(),
                EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: first_turn.sub_id.clone(),
                    last_agent_message: Some("first done".to_string()),
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            )
            .await;

        FollowupTaskHandlerV2
            .handle(invocation(
                session,
                turn,
                "followup_task",
                function_payload(json!({
                    "target": agent_id.to_string(),
                    "message": "continue",
                })),
            ))
            .await
            .expect("followup_task should succeed");

        let second_turn = thread.codex.session.new_default_turn().await;
        thread
            .codex
            .session
            .send_event(
                second_turn.as_ref(),
                EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: second_turn.sub_id.clone(),
                    last_agent_message: Some("second done".to_string()),
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            )
            .await;

        let first_notification = format_subagent_notification_message(
            &agent_id.to_string(),
            &AgentStatus::Completed(Some("first done".to_string())),
        );
        let second_notification = format_subagent_notification_message(
            &agent_id.to_string(),
            &AgentStatus::Completed(Some("second done".to_string())),
        );

        let notifications = timeout(Duration::from_secs(5), async {
            loop {
                let notifications = manager
                    .captured_ops()
                    .into_iter()
                    .filter_map(|(id, op)| {
                        (id == root.thread_id)
                            .then_some(op)
                            .and_then(|op| match op {
                                Op::InterAgentCommunication { communication }
                                    if communication.author == worker_path
                                        && communication.recipient == AgentPath::root()
                                        && communication.other_recipients.is_empty()
                                        && !communication.trigger_turn =>
                                {
                                    Some(communication.content)
                                }
                                _ => None,
                            })
                    })
                    .collect::<Vec<_>>();
                let first_count = notifications
                    .iter()
                    .filter(|message| **message == first_notification)
                    .count();
                let second_count = notifications
                    .iter()
                    .filter(|message| **message == second_notification)
                    .count();
                if first_count == 1 && second_count == 1 {
                    break notifications;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent should receive one completion notification per child turn");

        assert_eq!(notifications.len(), 2);
    });
}

#[tokio::test]
async fn multi_agent_v2_followup_task_rejects_legacy_items_field() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = turn.config.as_ref().clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "boot worker",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
    let invocation = invocation(
        session,
        turn,
        "followup_task",
        function_payload(json!({
            "target": agent_id.to_string(),
            "items": [{"type": "text", "text": "continue"}],
        })),
    );

    let Err(err) = FollowupTaskHandlerV2.handle(invocation).await else {
        panic!("legacy items field should be rejected in v2");
    };
    let FunctionCallError::RespondToModel(message) = err else {
        panic!("legacy items field should surface as a model-facing error");
    };
    assert!(message.contains("unknown field `items`"));
}

#[tokio::test]
async fn multi_agent_v2_interrupted_turn_does_not_notify_parent() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = turn.config.as_ref().clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "boot worker",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
    let thread = manager
        .get_thread(agent_id)
        .await
        .expect("worker thread should exist");

    let aborted_turn = thread.codex.session.new_default_turn().await;
    thread
        .codex
        .session
        .send_event(
            aborted_turn.as_ref(),
            EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(aborted_turn.sub_id.clone()),
                reason: TurnAbortReason::Interrupted,
                completed_at: None,
                duration_ms: None,
            }),
        )
        .await;

    let notifications = manager
        .captured_ops()
        .into_iter()
        .filter_map(|(id, op)| {
            (id == root.thread_id)
                .then_some(op)
                .and_then(|op| match op {
                    Op::InterAgentCommunication { communication }
                        if communication.author.as_str() == "/root/worker"
                            && communication.recipient == AgentPath::root()
                            && communication.other_recipients.is_empty()
                            && !communication.trigger_turn =>
                    {
                        Some(communication.content)
                    }
                    _ => None,
                })
        })
        .collect::<Vec<_>>();

    assert_eq!(notifications, Vec::<String>::new());
}

#[tokio::test]
async fn multi_agent_v2_spawn_omits_agent_id_when_named() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let output = SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "test_process"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: serde_json::Value =
        serde_json::from_str(&content).expect("spawn_agent result should be json");

    assert!(result.get("agent_id").is_none());
    assert_eq!(result["task_name"], "/root/test_process");
    assert!(result.get("nickname").is_some());
    assert_eq!(success, Some(true));
}

#[tokio::test]
async fn multi_agent_v2_spawn_surfaces_task_name_validation_errors() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({
            "message": "inspect this repo",
            "task_name": "BadName"
        })),
    );
    let Err(err) = SpawnAgentHandlerV2::default().handle(invocation).await else {
        panic!("invalid agent name should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "agent_name must use only lowercase letters, digits, and underscores".to_string()
        )
    );
}

#[tokio::test]
async fn spawn_agent_reapplies_runtime_sandbox_after_role_config() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
        nickname: Option<String>,
    }

    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let expected_sandbox = turn.config.legacy_sandbox_policy();
    #[allow(deprecated)]
    let mut expected_file_system_sandbox_policy =
        FileSystemSandboxPolicy::from_legacy_sandbox_policy_for_cwd(&expected_sandbox, &turn.cwd);
    expected_file_system_sandbox_policy
        .entries
        .push(FileSystemSandboxEntry {
            path: FileSystemPath::GlobPattern {
                pattern: "**/.env".to_string(),
            },
            access: FileSystemAccessMode::Deny,
        });
    let expected_network_sandbox_policy = NetworkSandboxPolicy::from(&expected_sandbox);
    let expected_permission_profile = PermissionProfile::from_runtime_permissions_with_enforcement(
        SandboxEnforcement::from_legacy_sandbox_policy(&expected_sandbox),
        &expected_file_system_sandbox_policy,
        expected_network_sandbox_policy,
    );
    turn.approval_policy
        .set(AskForApproval::OnRequest)
        .expect("approval policy should be set");
    turn.permission_profile = expected_permission_profile.clone();
    assert_ne!(
        expected_permission_profile,
        turn.config.permissions.effective_permission_profile(),
        "test requires a runtime profile override that differs from base config"
    );

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({
            "message": "await this command",
            "agent_type": "explorer"
        })),
    );
    let output = SpawnAgentHandler::default()
        .handle(invocation)
        .await
        .expect("spawn_agent should succeed");
    let (content, _) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn_agent result should be json");
    let agent_id = parse_agent_id(&result.agent_id);
    assert!(
        result
            .nickname
            .as_deref()
            .is_some_and(|nickname| !nickname.is_empty())
    );

    let snapshot = manager
        .get_thread(agent_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;
    assert_eq!(snapshot.sandbox_policy(), expected_sandbox);
    assert_eq!(snapshot.approval_policy, AskForApproval::OnRequest);
    assert_eq!(snapshot.permission_profile, expected_permission_profile);
    let child_thread = manager
        .get_thread(agent_id)
        .await
        .expect("spawned agent thread should exist");
    let child_turn = child_thread.codex.session.new_default_turn().await;
    assert_eq!(
        child_turn.file_system_sandbox_policy(),
        expected_file_system_sandbox_policy
    );
    assert_eq!(
        child_turn.network_sandbox_policy(),
        expected_network_sandbox_policy
    );
    assert_eq!(child_turn.permission_profile(), expected_permission_profile);
}

#[tokio::test]
async fn spawn_agent_rejects_when_depth_limit_exceeded() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();

    let max_depth = turn.config.agent_max_depth;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: session.conversation_id,
        depth: max_depth,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({"message": "hello"})),
    );
    let Err(err) = SpawnAgentHandler::default().handle(invocation).await else {
        panic!("spawn should fail when depth limit exceeded");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Agent depth limit reached. Solve the task yourself.".to_string()
        )
    );
}

#[tokio::test]
async fn spawn_agent_allows_depth_up_to_configured_max_depth() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
        nickname: Option<String>,
    }

    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();

    let mut config = (*turn.config).clone();
    config.agent_max_depth = DEFAULT_AGENT_MAX_DEPTH + 1;
    turn.config = Arc::new(config);
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: session.conversation_id,
        depth: DEFAULT_AGENT_MAX_DEPTH,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({"message": "hello"})),
    );
    let output = SpawnAgentHandler::default()
        .handle(invocation)
        .await
        .expect("spawn should succeed within configured depth");
    let (content, success) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn_agent result should be json");
    assert!(!result.agent_id.is_empty());
    assert!(
        result
            .nickname
            .as_deref()
            .is_some_and(|nickname| !nickname.is_empty())
    );
    assert_eq!(success, Some(true));
}

#[tokio::test]
async fn multi_agent_v2_spawn_agent_rejects_when_depth_limit_exceeded() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let mut config = (*turn.config).clone();
    config.agent_max_depth = 1;
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    let root = manager
        .start_thread(config.clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    turn.config = Arc::new(config);
    let parent_path = AgentPath::try_from("/root/parent").expect("agent path");
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 1,
        agent_path: Some(parent_path),
        agent_nickname: None,
        agent_role: None,
    });

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "spawn_agent",
        function_payload(json!({
            "message": "hello",
            "task_name": "child",
            "fork_turns": "none"
        })),
    );
    let Err(err) = SpawnAgentHandlerV2::default().handle(invocation).await else {
        panic!("multi-agent v2 spawn should fail when depth limit exceeded");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Agent depth limit reached. Solve the task yourself.".to_string()
        )
    );
}

#[tokio::test]
async fn send_input_rejects_empty_message() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "send_input",
        function_payload(json!({"target": ThreadId::new().to_string(), "message": ""})),
    );
    let Err(err) = SendInputHandler.handle(invocation).await else {
        panic!("empty message should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("Empty message can't be sent to an agent".to_string())
    );
}

#[tokio::test]
async fn send_input_rejects_when_message_and_items_are_both_set() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "send_input",
        function_payload(json!({
            "target": ThreadId::new().to_string(),
            "message": "hello",
            "items": [{"type": "mention", "name": "drive", "path": "app://drive"}]
        })),
    );
    let Err(err) = SendInputHandler.handle(invocation).await else {
        panic!("message+items should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Provide either message or items, but not both".to_string()
        )
    );
}

#[tokio::test]
async fn send_input_rejects_invalid_id() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "send_input",
        function_payload(json!({"target": "not-a-uuid", "message": "hi"})),
    );
    let Err(err) = SendInputHandler.handle(invocation).await else {
        panic!("invalid id should be rejected");
    };
    let FunctionCallError::RespondToModel(msg) = err else {
        panic!("expected respond-to-model error");
    };
    assert!(msg.starts_with("invalid agent id not-a-uuid:"));
}

#[tokio::test]
async fn send_input_reports_missing_agent() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let agent_id = ThreadId::new();
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "send_input",
        function_payload(json!({"target": agent_id.to_string(), "message": "hi"})),
    );
    let Err(err) = SendInputHandler.handle(invocation).await else {
        panic!("missing agent should be reported");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(format!("agent with id {agent_id} not found"))
    );
}

#[tokio::test]
async fn send_input_interrupts_before_prompt() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .start_thread(config.clone())
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "send_input",
        function_payload(json!({
            "target": agent_id.to_string(),
            "message": "hi",
            "interrupt": true
        })),
    );
    SendInputHandler
        .handle(invocation)
        .await
        .expect("send_input should succeed");

    let ops = manager.captured_ops();
    let ops_for_agent: Vec<&Op> = ops
        .iter()
        .filter_map(|(id, op)| (*id == agent_id).then_some(op))
        .collect();
    assert_eq!(ops_for_agent.len(), 2);
    assert!(matches!(ops_for_agent[0], Op::Interrupt));
    assert!(matches!(ops_for_agent[1], Op::UserInput { .. }));

    let _ = thread
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");
}

#[tokio::test]
async fn send_input_accepts_structured_items() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .start_thread(config.clone())
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "send_input",
        function_payload(json!({
            "target": agent_id.to_string(),
            "items": [
                {"type": "mention", "name": "drive", "path": "app://google_drive"},
                {"type": "text", "text": "read the folder"}
            ]
        })),
    );
    SendInputHandler
        .handle(invocation)
        .await
        .expect("send_input should succeed");

    let expected = Op::UserInput {
        environments: None,
        items: vec![
            UserInput::Mention {
                name: "drive".to_string(),
                path: "app://google_drive".to_string(),
            },
            UserInput::Text {
                text: "read the folder".to_string(),
                text_elements: Vec::new(),
            },
        ],
        final_output_json_schema: None,
        responsesapi_client_metadata: None,
        additional_context: Default::default(),
        thread_settings: Default::default(),
    };
    let captured = manager
        .captured_ops()
        .into_iter()
        .find(|(id, op)| *id == agent_id && *op == expected);
    assert_eq!(captured, Some((agent_id, expected)));

    let _ = thread
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");
}

#[tokio::test]
async fn send_input_from_subagent_message_uses_inter_agent_communication() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let parent = manager.start_thread(config).await.expect("start parent");
    let parent_thread_id = parent.thread_id;
    session
        .services
        .agent_control
        .register_session_root(parent_thread_id, &SessionSource::Cli);
    let sender_path = AgentPath::try_from("/root/worker").expect("valid sender path");
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(sender_path.clone()),
        agent_nickname: Some("Worker".to_string()),
        agent_role: None,
    });

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "send_input",
        function_payload(json!({
            "target": parent_thread_id.to_string(),
            "message": "ready for review"
        })),
    );
    SendInputHandler
        .handle(invocation)
        .await
        .expect("send_input should succeed");

    let expected = Op::InterAgentCommunication {
        communication: InterAgentCommunication::new(
            sender_path,
            AgentPath::root(),
            Vec::new(),
            "ready for review".to_string(),
            /*trigger_turn*/ true,
        ),
    };
    let captured = manager
        .captured_ops()
        .into_iter()
        .find(|(id, op)| *id == parent_thread_id && *op == expected);
    assert_eq!(captured, Some((parent_thread_id, expected)));

    let _ = parent
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");
}

#[tokio::test]
async fn resume_agent_rejects_invalid_id() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "resume_agent",
        function_payload(json!({"id": "not-a-uuid"})),
    );
    let Err(err) = ResumeAgentHandler.handle(invocation).await else {
        panic!("invalid id should be rejected");
    };
    let FunctionCallError::RespondToModel(msg) = err else {
        panic!("expected respond-to-model error");
    };
    assert!(msg.starts_with("invalid agent id not-a-uuid:"));
}

#[tokio::test]
async fn resume_agent_reports_missing_agent() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let agent_id = ThreadId::new();
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "resume_agent",
        function_payload(json!({"id": agent_id.to_string()})),
    );
    let Err(err) = ResumeAgentHandler.handle(invocation).await else {
        panic!("missing agent should be reported");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(format!("agent with id {agent_id} not found"))
    );
}

#[tokio::test]
async fn resume_agent_noops_for_active_agent() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .start_thread(config.clone())
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let status_before = manager.agent_control().get_status(agent_id).await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "resume_agent",
        function_payload(json!({"id": agent_id.to_string()})),
    );

    let output = ResumeAgentHandler
        .handle(invocation)
        .await
        .expect("resume_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: resume_agent::ResumeAgentResult =
        serde_json::from_str(&content).expect("resume_agent result should be json");
    assert_eq!(result.status, status_before);
    assert_eq!(success, Some(true));

    let thread_ids = manager.list_thread_ids().await;
    assert_eq!(thread_ids, vec![agent_id]);

    let _ = thread
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");
}

#[tokio::test]
async fn resume_agent_restores_closed_agent_and_accepts_send_input() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .resume_thread_with_history(
            config.clone(),
            InitialHistory::Forked(vec![RolloutItem::ResponseItem(ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "materialized".to_string(),
                }],
                phase: None,
            })]),
            AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy")),
            /*persist_extended_history*/ false,
            /*parent_trace*/ None,
        )
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let _ = manager
        .agent_control()
        .shutdown_live_agent(agent_id)
        .await
        .expect("shutdown agent");
    assert_eq!(
        manager.agent_control().get_status(agent_id).await,
        AgentStatus::NotFound
    );
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    let resume_invocation = invocation(
        session.clone(),
        turn.clone(),
        "resume_agent",
        function_payload(json!({"id": agent_id.to_string()})),
    );
    let output = ResumeAgentHandler
        .handle(resume_invocation)
        .await
        .expect("resume_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: resume_agent::ResumeAgentResult =
        serde_json::from_str(&content).expect("resume_agent result should be json");
    assert_ne!(result.status, AgentStatus::NotFound);
    assert_eq!(success, Some(true));

    let send_invocation = invocation(
        session,
        turn,
        "send_input",
        function_payload(json!({"target": agent_id.to_string(), "message": "hello"})),
    );
    let output = SendInputHandler
        .handle(send_invocation)
        .await
        .expect("send_input should succeed after resume");
    let (content, success) = expect_text_output(output);
    let result: serde_json::Value =
        serde_json::from_str(&content).expect("send_input result should be json");
    let submission_id = result
        .get("submission_id")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    assert!(!submission_id.is_empty());
    assert_eq!(success, Some(true));

    let _ = manager
        .agent_control()
        .shutdown_live_agent(agent_id)
        .await
        .expect("shutdown resumed agent");
}

#[tokio::test]
async fn resume_agent_rejects_when_depth_limit_exceeded() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();

    let max_depth = turn.config.agent_max_depth;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: session.conversation_id,
        depth: max_depth,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "resume_agent",
        function_payload(json!({"id": ThreadId::new().to_string()})),
    );
    let Err(err) = ResumeAgentHandler.handle(invocation).await else {
        panic!("resume should fail when depth limit exceeded");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel(
            "Agent depth limit reached. Solve the task yourself.".to_string()
        )
    );
}

#[tokio::test]
async fn wait_agent_rejects_non_positive_timeout() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "wait_agent",
        function_payload(json!({
            "targets": [ThreadId::new().to_string()],
            "timeout_ms": 0
        })),
    );
    let Err(err) = WaitAgentHandler::default().handle(invocation).await else {
        panic!("non-positive timeout should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("timeout_ms must be greater than zero".to_string())
    );
}

#[tokio::test]
async fn wait_agent_rejects_invalid_target() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "wait_agent",
        function_payload(json!({"targets": ["invalid"]})),
    );
    let Err(err) = WaitAgentHandler::default().handle(invocation).await else {
        panic!("invalid id should be rejected");
    };
    let FunctionCallError::RespondToModel(msg) = err else {
        panic!("expected respond-to-model error");
    };
    assert!(msg.starts_with("invalid agent id invalid:"));
}

#[tokio::test]
async fn wait_agent_rejects_empty_targets() {
    let (session, turn) = make_session_and_context().await;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "wait_agent",
        function_payload(json!({"targets": []})),
    );
    let Err(err) = WaitAgentHandler::default().handle(invocation).await else {
        panic!("empty ids should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("agent ids must be non-empty".to_string())
    );
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_accepts_timeout_only_argument() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "boot worker",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
    let worker_path = session
        .services
        .agent_control
        .get_agent_metadata(agent_id)
        .expect("worker metadata")
        .agent_path
        .expect("worker path");

    let wait_task = tokio::spawn({
        let session = session.clone();
        let turn = turn.clone();
        async move {
            WaitAgentHandlerV2::default()
                .handle(invocation(
                    session,
                    turn,
                    "wait_agent",
                    function_payload(json!({"timeout_ms": 10_000})),
                ))
                .await
        }
    });
    tokio::task::yield_now().await;

    session
        .input_queue
        .enqueue_mailbox_communication(InterAgentCommunication::new(
            worker_path,
            AgentPath::root(),
            Vec::new(),
            "hello from worker".to_string(),
            /*trigger_turn*/ false,
        ))
        .await;

    let output = wait_task
        .await
        .expect("wait task should join")
        .expect("timeout-only args should be accepted in v2 mode");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait completed.".to_string(),
            timed_out: false,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_rejects_timeout_below_configured_min() {
    let (session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.multi_agent_v2.min_wait_timeout_ms = 50;
    config.multi_agent_v2.max_wait_timeout_ms = 1_000;
    config.multi_agent_v2.default_wait_timeout_ms = 50;
    turn.config = Arc::new(config);

    let Err(err) = WaitAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait_agent",
            function_payload(json!({"timeout_ms": 1})),
        ))
        .await
    else {
        panic!("timeout below configured minimum should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("timeout_ms must be at least 50".to_string())
    );
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_accepts_explicit_timeout_at_configured_min() {
    let (session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.multi_agent_v2.min_wait_timeout_ms = 1;
    config.multi_agent_v2.max_wait_timeout_ms = 1_000;
    config.multi_agent_v2.default_wait_timeout_ms = 50;
    turn.config = Arc::new(config);

    let output = WaitAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait_agent",
            function_payload(json!({"timeout_ms": 1})),
        ))
        .await
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait timed out.".to_string(),
            timed_out: true,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_uses_configured_default_timeout() {
    let (session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.multi_agent_v2.min_wait_timeout_ms = 1;
    config.multi_agent_v2.max_wait_timeout_ms = 1_000;
    config.multi_agent_v2.default_wait_timeout_ms = 50;
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    let early = timeout(
        Duration::from_millis(/*millis*/ 20),
        WaitAgentHandlerV2::default().handle(invocation(
            session.clone(),
            turn.clone(),
            "wait_agent",
            function_payload(json!({})),
        )),
    )
    .await;
    assert!(
        early.is_err(),
        "wait_agent should not return before the configured default timeout"
    );

    let output = timeout(
        Duration::from_secs(/*secs*/ 1),
        WaitAgentHandlerV2::default().handle(invocation(
            session,
            turn,
            "wait_agent",
            function_payload(json!({})),
        )),
    )
    .await
    .expect("configured default should be shorter than the test timeout")
    .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait timed out.".to_string(),
            timed_out: true,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_allows_zero_configured_timeout() {
    let (session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.multi_agent_v2.min_wait_timeout_ms = 0;
    config.multi_agent_v2.max_wait_timeout_ms = 0;
    config.multi_agent_v2.default_wait_timeout_ms = 0;
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    let output = timeout(
        Duration::from_secs(/*secs*/ 1),
        WaitAgentHandlerV2::default().handle(invocation(
            session,
            turn,
            "wait_agent",
            function_payload(json!({})),
        )),
    )
    .await
    .expect("zero timeout should complete immediately")
    .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait timed out.".to_string(),
            timed_out: true,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_rejects_timeout_above_configured_max() {
    let (session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.multi_agent_v2.min_wait_timeout_ms = 1;
    config.multi_agent_v2.max_wait_timeout_ms = 50;
    config.multi_agent_v2.default_wait_timeout_ms = 1;
    turn.config = Arc::new(config);

    let Err(err) = WaitAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait_agent",
            function_payload(json!({"timeout_ms": 500})),
        ))
        .await
    else {
        panic!("timeout above configured maximum should be rejected");
    };
    assert_eq!(
        err,
        FunctionCallError::RespondToModel("timeout_ms must be at most 50".to_string())
    );
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_accepts_explicit_timeout_at_configured_max() {
    let (session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.multi_agent_v2.min_wait_timeout_ms = 1;
    config.multi_agent_v2.max_wait_timeout_ms = 1;
    config.multi_agent_v2.default_wait_timeout_ms = 1;
    turn.config = Arc::new(config);

    let output = WaitAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait_agent",
            function_payload(json!({"timeout_ms": 1})),
        ))
        .await
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait timed out.".to_string(),
            timed_out: true,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn wait_agent_returns_not_found_for_missing_agents() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let id_a = ThreadId::new();
    let id_b = ThreadId::new();
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "wait_agent",
        function_payload(json!({
            "targets": [id_a.to_string(), id_b.to_string()],
            "timeout_ms": 10_000
        })),
    );
    let output = WaitAgentHandler::default()
        .handle(invocation)
        .await
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        wait::WaitAgentResult {
            status: HashMap::from([
                (id_a.to_string(), AgentStatus::NotFound),
                (id_b.to_string(), AgentStatus::NotFound),
            ]),
            timed_out: false
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn wait_agent_times_out_when_status_is_not_final() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .start_thread(config.clone())
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "wait_agent",
        function_payload(json!({
            "targets": [agent_id.to_string()],
            "timeout_ms": MIN_WAIT_TIMEOUT_MS
        })),
    );
    let output = WaitAgentHandler::default()
        .handle(invocation)
        .await
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        wait::WaitAgentResult {
            status: HashMap::new(),
            timed_out: true
        }
    );
    assert_eq!(success, None);

    let _ = thread
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");
}

#[tokio::test]
async fn wait_agent_clamps_short_timeouts_to_minimum() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .start_thread(config.clone())
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "wait_agent",
        function_payload(json!({
            "targets": [agent_id.to_string()],
            "timeout_ms": 10
        })),
    );

    let early = timeout(
        Duration::from_millis(50),
        WaitAgentHandler::default().handle(invocation),
    )
    .await;
    assert!(
        early.is_err(),
        "wait_agent should not return before the minimum timeout clamp"
    );

    let _ = thread
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");
}

#[tokio::test]
async fn wait_agent_returns_final_status_without_timeout() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .start_thread(config.clone())
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let mut status_rx = manager
        .agent_control()
        .subscribe_status(agent_id)
        .await
        .expect("subscribe should succeed");

    let _ = thread
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");
    let _ = timeout(Duration::from_secs(1), status_rx.changed())
        .await
        .expect("shutdown status should arrive");

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "wait_agent",
        function_payload(json!({
            "targets": [agent_id.to_string()],
            "timeout_ms": 10_000
        })),
    );
    let output = WaitAgentHandler::default()
        .handle(invocation)
        .await
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        wait::WaitAgentResult {
            status: HashMap::from([(agent_id.to_string(), AgentStatus::Shutdown)]),
            timed_out: false
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_returns_summary_for_mailbox_activity() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let session = Arc::new(session);
    let turn = Arc::new(turn);
    let spawn_output = SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "test_process"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");
    let _ = expect_text_output(spawn_output);

    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(
            session.conversation_id,
            &turn.session_source,
            "test_process",
        )
        .await
        .expect("relative path should resolve");
    let worker_path = session
        .services
        .agent_control
        .get_agent_metadata(agent_id)
        .expect("worker metadata")
        .agent_path
        .expect("worker path");
    let wait_task = tokio::spawn({
        let session = session.clone();
        let turn = turn.clone();
        async move {
            WaitAgentHandlerV2::default()
                .handle(invocation(
                    session,
                    turn,
                    "wait_agent",
                    function_payload(json!({"timeout_ms": 10_000})),
                ))
                .await
        }
    });
    tokio::task::yield_now().await;

    session
        .input_queue
        .enqueue_mailbox_communication(InterAgentCommunication::new(
            worker_path,
            AgentPath::root(),
            Vec::new(),
            "completed".to_string(),
            /*trigger_turn*/ false,
        ))
        .await;

    let wait_output = wait_task
        .await
        .expect("wait task should join")
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(wait_output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait completed.".to_string(),
            timed_out: false,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_returns_for_already_queued_mail() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "boot worker",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
    let worker_path = session
        .services
        .agent_control
        .get_agent_metadata(agent_id)
        .expect("worker metadata")
        .agent_path
        .expect("worker path");

    session
        .input_queue
        .enqueue_mailbox_communication(InterAgentCommunication::new(
            worker_path,
            AgentPath::root(),
            Vec::new(),
            "already queued".to_string(),
            /*trigger_turn*/ false,
        ))
        .await;

    let output = timeout(
        Duration::from_millis(500),
        WaitAgentHandlerV2::default().handle(invocation(
            session,
            turn,
            "wait_agent",
            function_payload(json!({"timeout_ms": 10_000})),
        )),
    )
    .await
    .expect("already queued mail should complete wait_agent immediately")
    .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait completed.".to_string(),
            timed_out: false,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_wakes_on_any_mailbox_notification() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    for task_name in ["worker_a", "worker_b"] {
        SpawnAgentHandlerV2::default()
            .handle(invocation(
                session.clone(),
                turn.clone(),
                "spawn_agent",
                function_payload(json!({
                    "message": format!("boot {task_name}"),
                    "task_name": task_name
                })),
            ))
            .await
            .expect("spawn worker");
    }
    let worker_b_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker_b")
        .await
        .expect("worker_b should resolve");
    let worker_b_path = session
        .services
        .agent_control
        .get_agent_metadata(worker_b_id)
        .expect("worker_b metadata")
        .agent_path
        .expect("worker_b path");

    let wait_task = tokio::spawn({
        let session = session.clone();
        let turn = turn.clone();
        async move {
            WaitAgentHandlerV2::default()
                .handle(invocation(
                    session,
                    turn,
                    "wait_agent",
                    function_payload(json!({"timeout_ms": 10_000})),
                ))
                .await
        }
    });
    tokio::task::yield_now().await;

    session
        .input_queue
        .enqueue_mailbox_communication(InterAgentCommunication::new(
            worker_b_path,
            AgentPath::root(),
            Vec::new(),
            "from worker b".to_string(),
            /*trigger_turn*/ false,
        ))
        .await;

    let output = wait_task
        .await
        .expect("wait task should join")
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait completed.".to_string(),
            timed_out: false,
        }
    );
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_wait_agent_does_not_return_completed_content() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "boot worker",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
    let worker_path = session
        .services
        .agent_control
        .get_agent_metadata(agent_id)
        .expect("worker metadata")
        .agent_path
        .expect("worker path");
    let wait_task = tokio::spawn({
        let session = session.clone();
        let turn = turn.clone();
        async move {
            WaitAgentHandlerV2::default()
                .handle(invocation(
                    session,
                    turn,
                    "wait_agent",
                    function_payload(json!({"timeout_ms": 10_000})),
                ))
                .await
        }
    });
    tokio::task::yield_now().await;

    session
        .input_queue
        .enqueue_mailbox_communication(InterAgentCommunication::new(
            worker_path,
            AgentPath::root(),
            Vec::new(),
            "sensitive child output".to_string(),
            /*trigger_turn*/ false,
        ))
        .await;

    let output = wait_task
        .await
        .expect("wait task should join")
        .expect("wait_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult =
        serde_json::from_str(&content).expect("wait_agent result should be json");
    assert_eq!(
        result,
        crate::tools::handlers::multi_agents_v2::wait::WaitAgentResult {
            message: "Wait completed.".to_string(),
            timed_out: false,
        }
    );
    assert!(!content.contains("sensitive child output"));
    assert_eq!(success, None);
}

#[tokio::test]
async fn multi_agent_v2_close_agent_accepts_task_name_target() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let session = Arc::new(session);
    let turn = Arc::new(turn);
    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");

    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker path should resolve");

    let output = CloseAgentHandlerV2
        .handle(invocation(
            session,
            turn,
            "close_agent",
            function_payload(json!({"target": "worker"})),
        ))
        .await
        .expect("close_agent should succeed for v2 task names");
    let (content, success) = expect_text_output(output);
    let result: close_agent::CloseAgentResult =
        serde_json::from_str(&content).expect("close_agent result should be json");
    assert_ne!(result.previous_status, AgentStatus::NotFound);
    assert_eq!(success, Some(true));
    assert_eq!(
        manager.agent_control().get_status(agent_id).await,
        AgentStatus::NotFound
    );
}

#[tokio::test]
async fn multi_agent_v2_close_agent_reaps_stale_task_name_target() {
    let (mut session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config.agent_max_threads = Some(1);
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow sqlite");
    let state_db = init_state_db(&config)
        .await
        .expect("sqlite state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let root = manager
        .start_thread(config.clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    turn.config = Arc::new(config.clone());

    let session = Arc::new(session);
    let turn = Arc::new(turn);
    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "worker"
            })),
        ))
        .await
        .expect("spawn_agent should succeed");

    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "worker")
        .await
        .expect("worker path should resolve");
    let stale_thread = manager
        .remove_thread(&agent_id)
        .await
        .expect("worker thread should be loaded before removal");
    stale_thread
        .submit(Op::Shutdown {})
        .await
        .expect("removed worker thread should still accept shutdown");
    stale_thread.wait_until_terminated().await;

    let output = CloseAgentHandlerV2
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "close_agent",
            function_payload(json!({"target": "worker"})),
        ))
        .await
        .expect("close_agent should reap stale v2 task names");
    let (content, success) = expect_text_output(output);
    let result: close_agent::CloseAgentResult =
        serde_json::from_str(&content).expect("close_agent result should be json");
    assert_eq!(result.previous_status, AgentStatus::NotFound);
    assert_eq!(success, Some(true));

    let open_children = state_db
        .list_thread_spawn_children_with_status(
            root.thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open children should load");
    assert_eq!(open_children, Vec::<ThreadId>::new());
    let closed_children = state_db
        .list_thread_spawn_children_with_status(
            root.thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("closed children should load");
    assert_eq!(closed_children, vec![agent_id]);

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo again",
                "task_name": "replacement"
            })),
        ))
        .await
        .expect("spawn_agent should succeed after stale close releases the slot");
    let replacement_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.conversation_id, &turn.session_source, "replacement")
        .await
        .expect("replacement path should resolve");
    let _ = session
        .services
        .agent_control
        .shutdown_live_agent(replacement_id)
        .await
        .expect("replacement should shut down");
}

#[tokio::test]
async fn multi_agent_v2_close_agent_rejects_root_target_and_id() {
    let (mut session, mut turn) = make_session_and_context().await;
    let manager = thread_manager();
    let root = manager
        .start_thread((*turn.config).clone())
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.conversation_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    turn.config = Arc::new(config);

    let session = Arc::new(session);
    let turn = Arc::new(turn);
    let root_path_error = CloseAgentHandlerV2
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "close_agent",
            function_payload(json!({"target": "/root"})),
        ))
        .await
        .err()
        .expect("close_agent should reject the root path");
    assert_eq!(
        root_path_error,
        FunctionCallError::RespondToModel("root is not a spawned agent".to_string())
    );

    let root_id_error = CloseAgentHandlerV2
        .handle(invocation(
            session,
            turn,
            "close_agent",
            function_payload(json!({"target": root.thread_id.to_string()})),
        ))
        .await
        .err()
        .expect("close_agent should reject the root thread id");
    assert_eq!(
        root_id_error,
        FunctionCallError::RespondToModel("root is not a spawned agent".to_string())
    );
}

#[tokio::test]
async fn close_agent_submits_shutdown_and_returns_previous_status() {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let config = turn.config.as_ref().clone();
    let thread = manager
        .start_thread(config.clone())
        .await
        .expect("start thread");
    let agent_id = thread.thread_id;
    let status_before = manager.agent_control().get_status(agent_id).await;

    let invocation = invocation(
        Arc::new(session),
        Arc::new(turn),
        "close_agent",
        function_payload(json!({"target": agent_id.to_string()})),
    );
    let output = CloseAgentHandler
        .handle(invocation)
        .await
        .expect("close_agent should succeed");
    let (content, success) = expect_text_output(output);
    let result: close_agent::CloseAgentResult =
        serde_json::from_str(&content).expect("close_agent result should be json");
    assert_eq!(result.previous_status, status_before);
    assert_eq!(success, Some(true));

    let ops = manager.captured_ops();
    let submitted_shutdown = ops
        .iter()
        .any(|(id, op)| *id == agent_id && matches!(op, Op::Shutdown));
    assert_eq!(submitted_shutdown, true);

    let status_after = manager.agent_control().get_status(agent_id).await;
    assert_eq!(status_after, AgentStatus::NotFound);
}

#[tokio::test]
async fn tool_handlers_cascade_close_and_resume_and_keep_explicitly_closed_subtrees_closed() {
    let (_session, turn) = make_session_and_context().await;
    let mut config = turn.config.as_ref().clone();
    config.agent_max_depth = 3;
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow sqlite");
    let state_db = init_state_db(&config).await;
    let manager = ThreadManager::new(
        &config,
        AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy")),
        SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        empty_extension_registry(),
        /*analytics_events_client*/ None,
        thread_store_from_config(&config, state_db.clone()),
        state_db.clone(),
        "11111111-1111-4111-8111-111111111111".to_string(),
        /*attestation_provider*/ None,
    );

    let parent = manager
        .start_thread(config.clone())
        .await
        .expect("parent thread should start");
    let parent_thread_id = parent.thread_id;
    let parent_session = parent.thread.codex.session.clone();

    let child_turn = parent_session.new_default_turn().await;
    let child_spawn_output = SpawnAgentHandler::default()
        .handle(invocation(
            parent_session.clone(),
            child_turn,
            "spawn_agent",
            function_payload(json!({"message": "hello child"})),
        ))
        .await
        .expect("child spawn should succeed");
    let (child_content, child_success) = expect_text_output(child_spawn_output);
    let child_result: serde_json::Value =
        serde_json::from_str(&child_content).expect("child spawn result should be json");
    let child_thread_id = parse_agent_id(
        child_result
            .get("agent_id")
            .and_then(serde_json::Value::as_str)
            .expect("child spawn result should include agent_id"),
    );
    assert_eq!(child_success, Some(true));

    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let child_session = child_thread.codex.session.clone();
    let grandchild_spawn_output = SpawnAgentHandler::default()
        .handle(invocation(
            child_session.clone(),
            child_session.new_default_turn().await,
            "spawn_agent",
            function_payload(json!({"message": "hello grandchild"})),
        ))
        .await
        .expect("grandchild spawn should succeed");
    let (grandchild_content, grandchild_success) = expect_text_output(grandchild_spawn_output);
    let grandchild_result: serde_json::Value =
        serde_json::from_str(&grandchild_content).expect("grandchild spawn result should be json");
    let grandchild_thread_id = parse_agent_id(
        grandchild_result
            .get("agent_id")
            .and_then(serde_json::Value::as_str)
            .expect("grandchild spawn result should include agent_id"),
    );
    assert_eq!(grandchild_success, Some(true));

    let close_output = CloseAgentHandler
        .handle(invocation(
            parent_session.clone(),
            parent_session.new_default_turn().await,
            "close_agent",
            function_payload(json!({"target": child_thread_id.to_string()})),
        ))
        .await
        .expect("close_agent should close the child subtree");
    let (close_content, close_success) = expect_text_output(close_output);
    let close_result: close_agent::CloseAgentResult =
        serde_json::from_str(&close_content).expect("close_agent result should be json");
    assert_ne!(close_result.previous_status, AgentStatus::NotFound);
    assert_eq!(close_success, Some(true));
    assert_eq!(
        manager.agent_control().get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        manager
            .agent_control()
            .get_status(grandchild_thread_id)
            .await,
        AgentStatus::NotFound
    );

    let child_resume_output = ResumeAgentHandler
        .handle(invocation(
            parent_session.clone(),
            parent_session.new_default_turn().await,
            "resume_agent",
            function_payload(json!({"id": child_thread_id.to_string()})),
        ))
        .await
        .expect("resume_agent should reopen the child subtree");
    let (child_resume_content, child_resume_success) = expect_text_output(child_resume_output);
    let child_resume_result: resume_agent::ResumeAgentResult =
        serde_json::from_str(&child_resume_content).expect("resume result should be json");
    assert_ne!(child_resume_result.status, AgentStatus::NotFound);
    assert_eq!(child_resume_success, Some(true));
    assert_ne!(
        manager.agent_control().get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_ne!(
        manager
            .agent_control()
            .get_status(grandchild_thread_id)
            .await,
        AgentStatus::NotFound
    );

    let close_again_output = CloseAgentHandler
        .handle(invocation(
            parent_session.clone(),
            parent_session.new_default_turn().await,
            "close_agent",
            function_payload(json!({"target": child_thread_id.to_string()})),
        ))
        .await
        .expect("close_agent should be repeatable for the child subtree");
    let (close_again_content, close_again_success) = expect_text_output(close_again_output);
    let close_again_result: close_agent::CloseAgentResult =
        serde_json::from_str(&close_again_content)
            .expect("second close_agent result should be json");
    assert_ne!(close_again_result.previous_status, AgentStatus::NotFound);
    assert_eq!(close_again_success, Some(true));
    assert_eq!(
        manager.agent_control().get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        manager
            .agent_control()
            .get_status(grandchild_thread_id)
            .await,
        AgentStatus::NotFound
    );

    let operator = manager
        .start_thread(config.clone())
        .await
        .expect("operator thread should start");
    let operator_session = operator.thread.codex.session.clone();
    let _ = manager
        .agent_control()
        .shutdown_live_agent(parent_thread_id)
        .await
        .expect("parent shutdown should succeed");
    assert_eq!(
        manager.agent_control().get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );

    let parent_resume_output = ResumeAgentHandler
        .handle(invocation(
            operator_session,
            operator.thread.codex.session.new_default_turn().await,
            "resume_agent",
            function_payload(json!({"id": parent_thread_id.to_string()})),
        ))
        .await
        .expect("resume_agent should reopen the parent thread");
    let (parent_resume_content, parent_resume_success) = expect_text_output(parent_resume_output);
    let parent_resume_result: resume_agent::ResumeAgentResult =
        serde_json::from_str(&parent_resume_content).expect("parent resume result should be json");
    assert_ne!(parent_resume_result.status, AgentStatus::NotFound);
    assert_eq!(parent_resume_success, Some(true));
    assert_ne!(
        manager.agent_control().get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        manager.agent_control().get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        manager
            .agent_control()
            .get_status(grandchild_thread_id)
            .await,
        AgentStatus::NotFound
    );

    let shutdown_report = manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(shutdown_report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(shutdown_report.timed_out, Vec::<ThreadId>::new());
}

#[tokio::test]
async fn build_agent_spawn_config_uses_turn_context_values() {
    fn pick_allowed_sandbox_policy(
        permissions: &crate::config::Permissions,
        base: SandboxPolicy,
        cwd: &std::path::Path,
    ) -> SandboxPolicy {
        let candidates = [
            SandboxPolicy::new_read_only_policy(),
            SandboxPolicy::new_workspace_write_policy(),
            SandboxPolicy::DangerFullAccess,
        ];
        candidates
            .into_iter()
            .find(|candidate| {
                if *candidate == base {
                    return false;
                }
                permissions
                    .can_set_legacy_sandbox_policy(candidate, cwd)
                    .is_ok()
            })
            .unwrap_or(base)
    }

    let (_session, mut turn) = make_session_and_context().await;
    let base_instructions = BaseInstructions {
        text: "base".to_string(),
    };
    turn.developer_instructions = Some("dev".to_string());
    turn.compact_prompt = Some("compact".to_string());
    turn.shell_environment_policy = ShellEnvironmentPolicy {
        use_profile: true,
        ..ShellEnvironmentPolicy::default()
    };
    let temp_dir = tempfile::tempdir().expect("temp dir");
    #[allow(deprecated)]
    {
        turn.cwd = temp_dir.abs();
    }
    turn.codex_linux_sandbox_exe = Some(PathBuf::from("/bin/echo"));
    #[allow(deprecated)]
    let turn_cwd = turn.cwd.clone();
    let sandbox_policy = pick_allowed_sandbox_policy(
        &turn.config.permissions,
        turn.config.legacy_sandbox_policy(),
        turn_cwd.as_path(),
    );
    let file_system_sandbox_policy =
        FileSystemSandboxPolicy::from_legacy_sandbox_policy_for_cwd(&sandbox_policy, &turn_cwd);
    let network_sandbox_policy = NetworkSandboxPolicy::from(&sandbox_policy);
    let permission_profile = PermissionProfile::from_runtime_permissions_with_enforcement(
        SandboxEnforcement::from_legacy_sandbox_policy(&sandbox_policy),
        &file_system_sandbox_policy,
        network_sandbox_policy,
    );
    turn.permission_profile = permission_profile.clone();
    turn.approval_policy
        .set(AskForApproval::OnRequest)
        .expect("approval policy set");

    let config = build_agent_spawn_config(&base_instructions, &turn).expect("spawn config");
    let mut expected = (*turn.config).clone();
    expected.base_instructions = Some(base_instructions.text);
    expected.model = Some(turn.model_info.slug.clone());
    expected.model_provider = turn.provider.info().clone();
    expected.model_reasoning_effort = turn.reasoning_effort;
    expected.model_reasoning_summary = Some(turn.reasoning_summary);
    expected.developer_instructions = turn.developer_instructions.clone();
    expected.compact_prompt = turn.compact_prompt.clone();
    expected.permissions.shell_environment_policy = turn.shell_environment_policy.clone();
    expected.codex_linux_sandbox_exe = turn.codex_linux_sandbox_exe.clone();
    #[allow(deprecated)]
    {
        expected.cwd = turn.cwd.clone();
    }
    expected
        .permissions
        .approval_policy
        .set(AskForApproval::OnRequest)
        .expect("approval policy set");
    expected
        .permissions
        .set_permission_profile(permission_profile)
        .expect("permission profile set");
    assert_eq!(config, expected);
}

#[tokio::test]
async fn build_agent_spawn_config_preserves_base_user_instructions() {
    let (_session, mut turn) = make_session_and_context().await;
    let mut base_config = (*turn.config).clone();
    base_config.user_instructions = Some("base-user".to_string());
    turn.user_instructions = Some("resolved-user".to_string());
    turn.config = Arc::new(base_config.clone());
    let base_instructions = BaseInstructions {
        text: "base".to_string(),
    };

    let config = build_agent_spawn_config(&base_instructions, &turn).expect("spawn config");

    assert_eq!(config.user_instructions, base_config.user_instructions);
}

#[tokio::test]
async fn build_agent_resume_config_clears_base_instructions() {
    let (_session, mut turn) = make_session_and_context().await;
    let mut base_config = (*turn.config).clone();
    base_config.base_instructions = Some("caller-base".to_string());
    turn.config = Arc::new(base_config);
    turn.approval_policy
        .set(AskForApproval::OnRequest)
        .expect("approval policy set");

    let config = build_agent_resume_config(&turn, /*child_depth*/ 0).expect("resume config");

    let mut expected = (*turn.config).clone();
    expected.base_instructions = None;
    expected.model = Some(turn.model_info.slug.clone());
    expected.model_provider = turn.provider.info().clone();
    expected.model_reasoning_effort = turn.reasoning_effort;
    expected.model_reasoning_summary = Some(turn.reasoning_summary);
    expected.developer_instructions = turn.developer_instructions.clone();
    expected.compact_prompt = turn.compact_prompt.clone();
    expected.permissions.shell_environment_policy = turn.shell_environment_policy.clone();
    expected.codex_linux_sandbox_exe = turn.codex_linux_sandbox_exe.clone();
    #[allow(deprecated)]
    {
        expected.cwd = turn.cwd.clone();
    }
    expected
        .permissions
        .approval_policy
        .set(AskForApproval::OnRequest)
        .expect("approval policy set");
    expected
        .permissions
        .set_permission_profile(turn.permission_profile())
        .expect("permission profile set");
    assert_eq!(config, expected);
}
