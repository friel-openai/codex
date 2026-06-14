use super::*;
use crate::CodexThread;
use crate::StateDbHandle;
use crate::ThreadManager;
use crate::agent::agent_status_from_event;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::config::AgentRoleConfig;
use crate::config::Config;
use crate::config::ConfigBuilder;
use crate::context::ContextualUserFragment;
use crate::context::SubagentNotification;
use crate::init_state_db;
use crate::state::ActiveTurn;
use crate::thread_manager::StartThreadOptions;
use assert_matches::assert_matches;
use codex_config::types::McpServerConfig;
use codex_config::types::McpServerTransportConfig;
use codex_extension_api::ExtensionDataInit;
use codex_extension_api::empty_extension_registry;
use codex_features::Feature;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_models_manager::CustomModelConfig;
use codex_models_manager::bundled_models_response;
use codex_protocol::AgentPath;
use codex_protocol::ResponseItemId;
use codex_protocol::capabilities::CapabilityRootLocation;
use codex_protocol::capabilities::SelectedCapabilityRoot;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::config_types::Settings;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ReasoningItemReasoningSummary;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ModelTokenBudgetConfig;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::CompactedItem;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadGoal;
use codex_protocol::protocol::ThreadGoalStatus;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_rollout::RolloutRecorder;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::ThreadStore;
use codex_utils_path_uri::PathUri;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_failed;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::responses::start_websocket_server;
use core_test_support::responses::strip_response_item_ids;
use futures::FutureExt;
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::ffi::OsStr;
use std::ffi::OsString;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;
use toml::Value as TomlValue;
use wiremock::Mock;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path_regex;

async fn test_config_with_cli_overrides(
    mut cli_overrides: Vec<(String, TomlValue)>,
) -> (TempDir, Config) {
    let home = TempDir::new().expect("create temp dir");
    cli_overrides.push((
        "model".to_string(),
        TomlValue::String("gpt-5.5".to_string()),
    ));
    let config = ConfigBuilder::without_managed_config_for_tests()
        .codex_home(home.path().to_path_buf())
        .cli_overrides(cli_overrides)
        .build()
        .await
        .expect("load default test config");
    (home, config)
}

async fn test_config() -> (TempDir, Config) {
    test_config_with_cli_overrides(Vec::new()).await
}

fn text_input(text: &str) -> Vec<UserInput> {
    vec![UserInput::Text {
        text: text.to_string(),
        text_elements: Vec::new(),
    }]
}

fn adoption_request(
    parent_thread_id: ThreadId,
) -> (
    InterAgentCommunication,
    AgentCommunicationContext,
    SessionSource,
) {
    let agent_path = AgentPath::root()
        .join("adopted_worker")
        .expect("adopted worker path should be valid");
    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        agent_path.clone(),
        Vec::new(),
        "continue the existing thread".to_string(),
        /*trigger_turn*/ true,
    );
    let context = AgentCommunicationContext::new(AgentCommunicationKind::Spawn, parent_thread_id);
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(agent_path),
        agent_nickname: None,
        agent_role: None,
    });
    (communication, context, source)
}

fn assistant_message(text: &str, phase: Option<MessagePhase>) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase,
        internal_chat_message_metadata_passthrough: None,
    }
}

async fn wait_for_turn_complete(thread: &CodexThread) {
    timeout(Duration::from_secs(5), async {
        loop {
            let event = thread
                .next_event()
                .await
                .expect("event channel should stay open");
            if matches!(event.msg, EventMsg::TurnComplete(_)) {
                break;
            }
        }
    })
    .await
    .expect("turn should complete");
}

#[cfg(target_os = "linux")]
fn open_rollout_writer_paths(
    codex_home: &std::path::Path,
) -> std::collections::BTreeSet<std::path::PathBuf> {
    std::fs::read_dir("/proc/self/fd")
        .expect("process descriptors should be readable")
        .filter_map(Result::ok)
        .filter(|entry| {
            let fd = entry.file_name();
            let fdinfo =
                std::fs::read_to_string(std::path::Path::new("/proc/self/fdinfo").join(fd)).ok();
            fdinfo
                .as_deref()
                .and_then(|contents| {
                    contents
                        .lines()
                        .find_map(|line| line.strip_prefix("flags:\t"))
                })
                .and_then(|flags| u32::from_str_radix(flags, 8).ok())
                .is_some_and(|flags| flags & 0o3 != 0)
        })
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .filter(|target| {
            target.starts_with(codex_home)
                && target.to_string_lossy().contains("/sessions/")
                && target.to_string_lossy().contains(".jsonl")
        })
        .collect()
}

fn request_tool_signatures(body: &serde_json::Value) -> std::collections::BTreeSet<String> {
    let mut signatures = std::collections::BTreeSet::new();
    let tools = body["tools"].as_array().expect("tools should be an array");
    for tool in tools {
        let tool_type = tool.get("type").and_then(serde_json::Value::as_str);
        let Some(name) = tool.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if tool_type == Some("namespace") {
            let child_tools = tool
                .get("tools")
                .and_then(serde_json::Value::as_array)
                .expect("namespace tools should have child tools");
            for child_tool in child_tools {
                let child_name = child_tool
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .expect("child tool should have a name");
                signatures.insert(format!("{name}.{child_name}"));
            }
        } else {
            signatures.insert(name.to_string());
        }
    }
    signatures
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
        "spawn should add exactly one child thread"
    );
    spawned_thread_ids
        .pop()
        .expect("spawned thread id should be present")
}

async fn create_active_thread_goal_for_test(
    state_db: &StateDbHandle,
    parent_thread_id: ThreadId,
    parent_session: &std::sync::Arc<crate::session::session::Session>,
    objective: &str,
) -> anyhow::Result<(String, ThreadGoal)> {
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
    state_db.upsert_thread(&parent_metadata).await?;
    let state_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_thread_id,
            objective,
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    let protocol_goal = crate::goal_supervisor::protocol_goal_from_state(state_goal.clone());
    Ok((state_goal.goal_id, protocol_goal))
}

async fn goal_supervisor_continuity_text_for_test(
    session: &std::sync::Arc<crate::session::session::Session>,
    goal_id: &str,
    goal: &ThreadGoal,
) -> String {
    let item =
        crate::goal_supervisor::supervisor_continuity_context_item(session, goal_id, goal, &[])
            .await;
    let RolloutItem::ResponseItem(ResponseItem::Message { content, .. }) = item else {
        panic!("continuity should be a developer message");
    };
    content
        .into_iter()
        .find_map(|item| match item {
            ContentItem::InputText { text } => Some(text),
            _ => None,
        })
        .expect("continuity should contain text")
}

#[test]
fn register_session_root_skips_threads_with_explicit_parent() {
    let control = AgentControl::default();

    control.register_session_root(ThreadId::new(), Some(ThreadId::new()));

    assert_eq!(control.state.agent_id_for_path(&AgentPath::root()), None);
}

#[test]
fn fork_previous_response_id_env_values_parse() {
    for value in ["1", "true", "TRUE", "yes", "on"] {
        assert!(
            fork_previous_response_id_value_enabled(value),
            "{value} should enable previous_response_id inheritance"
        );
    }

    for value in ["", "0", "false", "off", "no", "enabled"] {
        assert!(
            !fork_previous_response_id_value_enabled(value),
            "{value} should not enable previous_response_id inheritance"
        );
    }
}

fn spawn_agent_call(call_id: &str) -> ResponseItem {
    ResponseItem::FunctionCall {
        id: None,
        name: "spawn_agent".to_string(),
        namespace: None,
        arguments: "{}".to_string(),
        call_id: call_id.to_string(),
        encrypted_function_args: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

struct AgentControlHarness {
    _home: TempDir,
    config: Config,
    state_db: Option<StateDbHandle>,
    manager: ThreadManager,
    control: AgentControl,
}

impl AgentControlHarness {
    async fn new() -> Self {
        let (home, config) = test_config().await;
        Self::new_with_config(home, config).await
    }

    async fn new_with_multi_agent_v1() -> Self {
        let (home, mut config) = test_config().await;
        let _ = config.features.disable(Feature::MultiAgentV2);
        Self::new_with_config(home, config).await
    }

    async fn new_with_config(home: TempDir, config: Config) -> Self {
        let state_db = init_state_db(&config).await;
        let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
            CodexAuth::from_api_key("dummy"),
            config.model_provider.clone(),
            config.codex_home.to_path_buf(),
            std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            state_db.clone(),
        );
        let control = manager.agent_control();
        Self {
            _home: home,
            config,
            state_db,
            manager,
            control,
        }
    }

    async fn start_thread(&self) -> (ThreadId, Arc<CodexThread>) {
        let new_thread = self
            .manager
            .start_thread(StartThreadOptions::new(self.config.clone()))
            .await
            .expect("start thread");
        (new_thread.thread_id, new_thread.thread)
    }

    async fn start_paginated_thread(&self) -> (ThreadId, Arc<CodexThread>) {
        let new_thread = self
            .manager
            .start_thread(StartThreadOptions {
                history_mode: Some(ThreadHistoryMode::Paginated),
                environments: Some(Vec::new()),
                ..StartThreadOptions::new(self.config.clone())
            })
            .await
            .expect("start paginated thread");
        (new_thread.thread_id, new_thread.thread)
    }

    async fn spawn_anonymous_child(
        &self,
        parent_thread_id: ThreadId,
        options: SpawnAgentOptions,
    ) -> ThreadId {
        self.control
            .spawn_agent_with_metadata(
                self.config.clone(),
                text_input("child task"),
                Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id,
                    depth: 1,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
                options,
            )
            .await
            .expect("child spawn should succeed")
            .thread_id
    }
}

async fn persisted_originator(thread: &CodexThread) -> String {
    thread.ensure_rollout_materialized().await;
    thread
        .flush_rollout()
        .await
        .expect("thread rollout should flush");
    let stored_thread = thread
        .read_thread(
            /*include_archived*/ true, /*include_history*/ true,
        )
        .await
        .expect("thread should be readable");
    let history = stored_thread.history.expect("history should be loaded");
    history
        .items
        .iter()
        .find_map(|item| match item {
            RolloutItem::SessionMeta(meta_line) => Some(meta_line.meta.originator.clone()),
            RolloutItem::RolloutReference(_)
            | RolloutItem::ResponseItem(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::EventMsg(_)
            | RolloutItem::Compacted(_)
            | RolloutItem::WorldState(_)
            | RolloutItem::TurnContext(_) => None,
        })
        .expect("session metadata should be persisted")
}

fn run_goal_supervisor_test<F, T>(name: &'static str, future: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let test_thread = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build goal supervisor test runtime")
                .block_on(future)
        })
        .expect("spawn goal supervisor test thread");
    match test_thread.join() {
        Ok(result) => result,
        Err(err) => std::panic::resume_unwind(err),
    }
}

struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &OsStr) -> Self {
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

fn has_subagent_notification(history_items: &[ResponseItem]) -> bool {
    history_items.iter().any(|item| {
        let ResponseItem::Message { role, content, .. } = item else {
            return false;
        };
        if role != "user" {
            return false;
        }
        content.iter().any(|content_item| match content_item {
            ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                SubagentNotification::matches_text(text)
            }
            ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
        })
    })
}

/// Returns true when any message item contains `needle` in a text span.
fn history_contains_text(history_items: &[ResponseItem], needle: &str) -> bool {
    history_items.iter().any(|item| {
        let ResponseItem::Message { content, .. } = item else {
            return false;
        };
        content.iter().any(|content_item| match content_item {
            ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                text.contains(needle)
            }
            ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
        })
    })
}

fn history_text_match_count(history_items: &[ResponseItem], needle: &str) -> usize {
    history_items
        .iter()
        .filter(|item| {
            let ResponseItem::Message { content, .. } = item else {
                return false;
            };
            content.iter().any(|content_item| match content_item {
                ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                    text.contains(needle)
                }
                ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
            })
        })
        .count()
}

fn history_contains_assistant_inter_agent_communication(
    history_items: &[ResponseItem],
    expected: &InterAgentCommunication,
) -> bool {
    history_items.iter().any(|item| {
        let ResponseItem::Message { role, content, .. } = item else {
            return false;
        };
        if role != "assistant" {
            return false;
        }
        content.iter().any(|content_item| match content_item {
            ContentItem::OutputText { text } => {
                serde_json::from_str::<InterAgentCommunication>(text)
                    .ok()
                    .as_ref()
                    == Some(expected)
            }
            ContentItem::InputText { .. }
            | ContentItem::InputImage { .. }
            | ContentItem::InputAudio { .. } => false,
        })
    })
}

async fn wait_for_subagent_notification(parent_thread: &Arc<CodexThread>) -> bool {
    let wait = async {
        loop {
            let history_items = parent_thread
                .session
                .clone_history()
                .await
                .raw_items()
                .to_vec();
            if has_subagent_notification(&history_items) {
                return true;
            }
            sleep(Duration::from_millis(25)).await;
        }
    };
    // CI can take several seconds to schedule the detached completion watcher,
    // especially on slower Windows runners.
    timeout(Duration::from_secs(10), wait).await.is_ok()
}

async fn persist_thread_for_tree_resume(thread: &Arc<CodexThread>, message: &str) {
    thread
        .inject_user_message_without_turn(message.to_string())
        .await;
    thread.session.ensure_rollout_materialized().await;
    thread
        .session
        .flush_rollout()
        .await
        .expect("test thread rollout should flush");
}

async fn wait_for_live_thread_spawn_children(
    control: &AgentControl,
    parent_thread_id: ThreadId,
    expected_children: &[ThreadId],
) {
    let mut expected_children = expected_children.to_vec();
    expected_children.sort_by_key(std::string::ToString::to_string);

    timeout(Duration::from_secs(5), async {
        loop {
            let mut child_ids = control
                .open_thread_spawn_children(parent_thread_id)
                .await
                .expect("live child list should load")
                .into_iter()
                .map(|(thread_id, _)| thread_id)
                .collect::<Vec<_>>();
            child_ids.sort_by_key(std::string::ToString::to_string);
            if child_ids == expected_children {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("expected persisted child tree");
}

async fn assert_thread_not_loaded(manager: &ThreadManager, thread_id: ThreadId) {
    match manager.get_thread(thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(id) => assert_eq!(*id, thread_id),
            _ => panic!("expected ThreadNotFound, got {err:?}"),
        },
        Ok(_) => panic!("expected thread not to be loaded"),
    }
}

#[tokio::test]
async fn send_input_errors_when_manager_dropped() {
    let control = AgentControl::default();
    let err = control
        .send_input(
            ThreadId::new(),
            vec![UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            /*parent_turn_id*/ None,
        )
        .await
        .expect_err("send_input should fail without a manager");
    assert_eq!(
        err.to_string(),
        "unsupported operation: thread manager dropped"
    );
}

#[tokio::test]
async fn get_status_returns_not_found_without_manager() {
    let control = AgentControl::default();
    let got = control.get_status(ThreadId::new()).await;
    assert_eq!(got, AgentStatus::NotFound);
}

#[tokio::test]
async fn on_event_updates_status_from_task_started() {
    let status = agent_status_from_event(&EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: "turn-1".to_string(),
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: ModeKind::Default,
    }));
    assert_eq!(status, Some(AgentStatus::Running));
}

#[tokio::test]
async fn on_event_updates_status_from_task_complete() {
    let status = agent_status_from_event(&EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: "turn-1".to_string(),
        started_at: None,
        last_agent_message: Some("done".to_string()),
        error: None,
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
    }));
    let expected = AgentStatus::Completed(Some("done".to_string()));
    assert_eq!(status, Some(expected));
}

#[tokio::test]
async fn on_event_updates_status_from_failed_task_complete() {
    let status = agent_status_from_event(&EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: "turn-1".to_string(),
        started_at: None,
        last_agent_message: None,
        error: Some(ErrorEvent {
            message: "boom".to_string(),
            codex_error_info: None,
        }),
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
    }));
    assert_eq!(status, Some(AgentStatus::Errored("boom".to_string())));
}

#[tokio::test]
async fn on_event_updates_status_from_error() {
    let status = agent_status_from_event(&EventMsg::Error(ErrorEvent {
        message: "boom".to_string(),
        codex_error_info: None,
    }));

    let expected = AgentStatus::Errored("boom".to_string());
    assert_eq!(status, Some(expected));
}

#[tokio::test]
async fn on_event_updates_status_from_turn_aborted() {
    let status = agent_status_from_event(&EventMsg::TurnAborted(TurnAbortedEvent {
        turn_id: Some("turn-1".to_string()),
        started_at: None,
        reason: TurnAbortReason::Interrupted,
        completed_at: None,
        duration_ms: None,
    }));

    let expected = AgentStatus::Interrupted;
    assert_eq!(status, Some(expected));
}

#[tokio::test]
async fn on_event_updates_status_from_shutdown_complete() {
    let status = agent_status_from_event(&EventMsg::ShutdownComplete);
    assert_eq!(status, Some(AgentStatus::Shutdown));
}

#[tokio::test]
async fn spawn_agent_errors_when_manager_dropped() {
    let control = AgentControl::default();
    let (_home, config) = test_config().await;
    let err = control
        .spawn_agent(config, text_input("hello"), /*session_source*/ None)
        .await
        .expect_err("spawn_agent should fail without a manager");
    assert_eq!(
        err.to_string(),
        "unsupported operation: thread manager dropped"
    );
}

#[tokio::test]
async fn resume_agent_errors_when_manager_dropped() {
    let control = AgentControl::default();
    let (_home, config) = test_config().await;
    let err = control
        .resume_agent_from_rollout(config, ThreadId::new(), SessionSource::Exec)
        .await
        .expect_err("resume_agent should fail without a manager");
    assert_eq!(
        err.to_string(),
        "unsupported operation: thread manager dropped"
    );
}

#[tokio::test]
async fn adopt_agent_rejects_parent_adopting_itself() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let (communication, context, source) = adoption_request(parent_thread_id);

    let err = harness
        .control
        .adopt_agent_with_communication(
            harness.config.clone(),
            parent_thread_id,
            communication,
            context,
            source,
            /*parent_turn_id*/ None,
        )
        .await
        .expect_err("a root must not adopt itself");

    assert_matches!(
        err.details(),
        CodexErrorDetails::InvalidRequest(message) if message.contains("cannot adopt itself")
    );
}

#[tokio::test]
async fn adopt_agent_rejects_missing_thread() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let missing_thread_id = ThreadId::new();
    let (communication, context, source) = adoption_request(parent_thread_id);

    let err = harness
        .control
        .adopt_agent_with_communication(
            harness.config.clone(),
            missing_thread_id,
            communication,
            context,
            source,
            /*parent_turn_id*/ None,
        )
        .await
        .expect_err("a missing root must not be adopted");

    assert_matches!(
        err.details(),
        CodexErrorDetails::ThreadNotFound(id) if *id == missing_thread_id
    );
}

#[tokio::test]
async fn adopt_agent_rejects_root_without_a_completed_turn() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let (target_thread_id, target_thread) = harness.start_thread().await;
    let (communication, context, source) = adoption_request(parent_thread_id);

    let err = harness
        .control
        .adopt_agent_with_communication(
            harness.config.clone(),
            target_thread_id,
            communication,
            context,
            source,
            /*parent_turn_id*/ None,
        )
        .await
        .expect_err("a root without a completed turn must not be adopted");

    assert_matches!(
        err.details(),
        CodexErrorDetails::InvalidRequest(message)
            if message.contains("has not completed initialization or a conversation turn")
    );
    assert!(!target_thread.session_source.is_non_root_agent());
    assert!(harness.manager.get_thread(target_thread_id).await.is_ok());
}

#[tokio::test]
async fn adopt_agent_rejects_existing_subagent() {
    Box::pin(adopt_agent_rejects_existing_subagent_inner()).await;
}

async fn adopt_agent_rejects_existing_subagent_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().boxed().await;
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("existing child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(
                    AgentPath::root()
                        .join("existing_worker")
                        .expect("existing worker path should be valid"),
                ),
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .boxed()
        .await
        .expect("existing child should spawn");
    let (communication, context, source) = adoption_request(parent_thread_id);

    let err = harness
        .control
        .adopt_agent_with_communication(
            harness.config.clone(),
            child_thread_id,
            communication,
            context,
            source,
            /*parent_turn_id*/ None,
        )
        .boxed()
        .await
        .expect_err("an existing subagent must not be adopted again");

    assert_matches!(
        err.details(),
        CodexErrorDetails::InvalidRequest(message)
            if message.contains("only an independent root thread can be adopted")
    );
    assert!(harness.manager.get_thread(child_thread_id).await.is_ok());
}

#[tokio::test]
async fn promote_agent_rejects_missing_thread() {
    let harness = AgentControlHarness::new().await;
    let missing_thread_id = ThreadId::new();

    let err = harness
        .control
        .promote_agent(missing_thread_id)
        .await
        .expect_err("a missing subagent must not be promoted");

    assert_matches!(
        err.details(),
        CodexErrorDetails::ThreadNotFound(id) if *id == missing_thread_id
    );
}

#[tokio::test]
async fn promote_agent_rejects_root_thread() {
    let harness = AgentControlHarness::new().await;
    let (root_thread_id, _root_thread) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);

    let err = harness
        .control
        .promote_agent(root_thread_id)
        .await
        .expect_err("a root thread must not be promoted");

    assert_matches!(
        err.details(),
        CodexErrorDetails::InvalidRequest(message) if message.contains("user-visible subagent")
    );
    assert!(harness.manager.get_thread(root_thread_id).await.is_ok());
}

#[tokio::test]
async fn send_input_errors_when_thread_missing() {
    let harness = AgentControlHarness::new().await;
    let thread_id = ThreadId::new();
    let err = harness
        .control
        .send_input(
            thread_id,
            vec![UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            /*parent_turn_id*/ None,
        )
        .await
        .expect_err("send_input should fail for missing thread");
    assert_matches!(
        err.details(),
        CodexErrorDetails::ThreadNotFound(id) if *id == thread_id
    );
}

#[tokio::test]
async fn get_status_returns_not_found_for_missing_thread() {
    let harness = AgentControlHarness::new().await;
    let status = harness.control.get_status(ThreadId::new()).await;
    assert_eq!(status, AgentStatus::NotFound);
}

#[tokio::test]
async fn get_status_returns_pending_init_for_new_thread() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, _) = harness.start_thread().await;
    let status = harness.control.get_status(thread_id).await;
    assert_eq!(status, AgentStatus::PendingInit);
}

#[tokio::test]
async fn subscribe_status_errors_for_missing_thread() {
    let harness = AgentControlHarness::new().await;
    let thread_id = ThreadId::new();
    let err = harness
        .control
        .subscribe_status(thread_id)
        .await
        .expect_err("subscribe_status should fail for missing thread");
    assert_matches!(
        err.details(),
        CodexErrorDetails::ThreadNotFound(id) if *id == thread_id
    );
}

#[tokio::test]
async fn subscribe_status_updates_on_shutdown() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, thread) = harness.start_thread().await;
    let mut status_rx = harness
        .control
        .subscribe_status(thread_id)
        .await
        .expect("subscribe_status should succeed");
    assert_eq!(status_rx.borrow().clone(), AgentStatus::PendingInit);

    let _ = thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");

    let _ = status_rx.changed().await;
    assert_eq!(status_rx.borrow().clone(), AgentStatus::Shutdown);
}

#[tokio::test]
async fn send_input_submits_user_message() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, _thread) = harness.start_thread().await;

    let submission_id = harness
        .control
        .send_input(
            thread_id,
            vec![UserInput::Text {
                text: "hello from tests".to_string(),
                text_elements: Vec::new(),
            }],
            /*parent_turn_id*/ None,
        )
        .await
        .expect("send_input should succeed");
    assert!(!submission_id.is_empty());
    let expected = (
        thread_id,
        Op::UserInput {
            items: vec![UserInput::Text {
                text: "hello from tests".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        },
    );
    let captured = harness
        .manager
        .captured_ops()
        .into_iter()
        .find(|entry| *entry == expected);
    assert_eq!(captured, Some(expected));
}

#[tokio::test]
async fn send_inter_agent_communication_without_turn_queues_message_without_triggering_turn() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, thread) = harness.start_thread().await;
    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "hello from tests".to_string(),
        /*trigger_turn*/ false,
    );

    let submission_id = harness
        .control
        .send_inter_agent_communication(
            thread_id,
            communication.clone(),
            AgentCommunicationContext::new(AgentCommunicationKind::Message, ThreadId::new()),
            /*parent_turn_id*/ None,
        )
        .await
        .expect("send_inter_agent_communication should succeed");
    assert!(!submission_id.is_empty());

    let expected = (
        thread_id,
        Op::InterAgentCommunication {
            communication: communication.clone(),
        },
    );
    let captured = harness
        .manager
        .captured_ops()
        .into_iter()
        .find(|entry| *entry == expected);
    assert_eq!(captured, Some(expected));

    timeout(Duration::from_secs(5), async {
        loop {
            if thread
                .session
                .input_queue
                .has_pending_input(&thread.session.active_turn)
                .await
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("inter-agent communication should stay pending");

    let history_items = thread.session.clone_history().await.raw_items().to_vec();
    assert!(!history_contains_assistant_inter_agent_communication(
        &history_items,
        &communication
    ));
}

#[tokio::test]
async fn ensure_agent_loaded_reloads_registered_unloaded_agent() {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Sqlite);
    config.model = Some("gpt-5.6-sol".to_string());
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, _parent_thread) = harness.start_paginated_thread().await;
    let agent_path = AgentPath::try_from("/root/worker").expect("agent path");
    let mut child_config = harness.config.clone();
    child_config.model = Some("gpt-5.6-luna".to_string());
    let spawned_agent = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(agent_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await
        .expect("spawn_agent should succeed");
    let child_thread = harness
        .manager
        .get_thread(spawned_agent.thread_id)
        .await
        .expect("child thread should exist");
    child_thread
        .inject_response_items(vec![assistant_message(
            "child persisted",
            Some(MessagePhase::FinalAnswer),
        )])
        .await
        .expect("child rollout should persist with v2 metadata");
    child_thread
        .shutdown_and_wait()
        .await
        .expect("child thread should shut down");
    let stored_child = child_thread
        .read_thread(
            /*include_archived*/ true, /*include_history*/ false,
        )
        .await
        .expect("child metadata should be readable");
    assert_eq!(stored_child.history_mode, ThreadHistoryMode::Paginated);

    assert!(
        harness
            .manager
            .remove_thread(&spawned_agent.thread_id)
            .await
            .is_some()
    );
    match harness.manager.get_thread(spawned_agent.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(id) => assert_eq!(*id, spawned_agent.thread_id),
            _ => panic!("expected ThreadNotFound, got {err:?}"),
        },
        Ok(_) => panic!("expected thread to be removed"),
    }

    let mut sender_config = harness.config.clone();
    sender_config.model_provider_id = "ollama".to_string();
    sender_config.model_provider = sender_config
        .model_providers
        .get("ollama")
        .cloned()
        .expect("ollama provider should be configured");

    harness
        .control
        .ensure_v2_agent_loaded(sender_config, spawned_agent.thread_id)
        .await
        .expect("known v2 agent should reload");
    let reloaded_child = harness
        .manager
        .get_thread(spawned_agent.thread_id)
        .await
        .expect("reloaded child thread should exist");
    assert_eq!(
        reloaded_child.config_snapshot().await.model,
        "gpt-5.6-luna",
        "residency reload must preserve the worker model instead of inheriting its parent model",
    );
    assert_eq!(
        (
            reloaded_child.config_snapshot().await.model_provider_id,
            reloaded_child
                .session
                .new_default_turn()
                .await
                .provider
                .info()
                .clone(),
        ),
        (
            stored_child.model_provider,
            harness.config.model_provider.clone()
        ),
        "residency reload must preserve the worker provider instead of inheriting its sender's provider",
    );

    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        agent_path,
        Vec::new(),
        "hello after reload".to_string(),
        /*trigger_turn*/ false,
    );
    harness
        .control
        .send_inter_agent_communication(
            spawned_agent.thread_id,
            communication.clone(),
            AgentCommunicationContext::new(AgentCommunicationKind::Message, ThreadId::new()),
            /*parent_turn_id*/ None,
        )
        .await
        .expect("send_inter_agent_communication should succeed after reload");
    let expected = (
        spawned_agent.thread_id,
        Op::InterAgentCommunication { communication },
    );
    let captured = harness
        .manager
        .captured_ops()
        .into_iter()
        .find(|entry| *entry == expected);
    assert_eq!(captured, Some(expected));
}

#[tokio::test]
async fn restore_v2_agent_metadata_uses_indexed_identity_without_reading_rollout() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("metadata restoration requires state db");
    let indexed_thread_id = ThreadId::new();
    let indexed_path = AgentPath::root()
        .join("indexed_worker")
        .expect("indexed agent path");
    let source_path = AgentPath::root()
        .join("source_worker")
        .expect("source agent path");
    let malformed_rollout = harness.config.codex_home.join("malformed-rollout.jsonl");
    tokio::fs::write(&malformed_rollout, "not a rollout record\n")
        .await
        .expect("malformed rollout should exist");
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(source_path),
        agent_nickname: Some("source-name".to_string()),
        agent_role: Some("source-role".to_string()),
    });
    let mut builder = codex_state::ThreadMetadataBuilder::new(
        indexed_thread_id,
        malformed_rollout.to_path_buf(),
        chrono::Utc::now(),
        source,
    );
    builder.agent_path = Some(indexed_path.to_string());
    builder.agent_nickname = Some("indexed-name".to_string());
    builder.agent_role = Some("indexed-role".to_string());
    state_db
        .upsert_thread(&builder.build("openai"))
        .await
        .expect("indexed metadata should persist");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            indexed_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("indexed spawn edge should persist");

    let anonymous_thread_id = ThreadId::new();
    let anonymous_rollout = harness.config.codex_home.join("anonymous-malformed.jsonl");
    tokio::fs::write(&anonymous_rollout, "not a rollout record\n")
        .await
        .expect("anonymous malformed rollout should exist");
    let anonymous_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: Some("anonymous-name".to_string()),
        agent_role: None,
    });
    let anonymous_metadata = codex_state::ThreadMetadataBuilder::new(
        anonymous_thread_id,
        anonymous_rollout.to_path_buf(),
        chrono::Utc::now(),
        anonymous_source,
    )
    .build("openai");
    state_db
        .upsert_thread(&anonymous_metadata)
        .await
        .expect("anonymous metadata should persist");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            anonymous_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("anonymous spawn edge should persist");

    harness
        .control
        .restore_v2_agent_metadata(&harness.config, parent_thread_id)
        .await;

    let indexed_metadata = harness
        .control
        .state
        .agent_metadata_for_thread(indexed_thread_id)
        .expect("indexed agent should be restored without reading its malformed rollout");
    assert_eq!(indexed_metadata.agent_path, Some(indexed_path));
    assert_eq!(indexed_metadata.agent_role.as_deref(), Some("indexed-role"));
    assert_eq!(
        indexed_metadata.agent_nickname.as_deref(),
        Some("indexed-name")
    );

    let anonymous_metadata = harness
        .control
        .state
        .agent_metadata_for_thread(anonymous_thread_id)
        .expect("missing optional path and role should not require reading the rollout");
    assert_eq!(anonymous_metadata.agent_path, None);
    assert_eq!(anonymous_metadata.agent_role, None);
    assert_eq!(
        anonymous_metadata.agent_nickname.as_deref(),
        Some("anonymous-name")
    );
    assert_thread_not_loaded(&harness.manager, indexed_thread_id).await;
    assert_thread_not_loaded(&harness.manager, anonymous_thread_id).await;
}

#[test]
fn restore_v2_agent_metadata_falls_back_when_indexed_identity_is_invalid() {
    run_goal_supervisor_test(
        "restore_v2_agent_metadata_falls_back_when_indexed_identity_is_invalid",
        restore_v2_agent_metadata_falls_back_when_indexed_identity_is_invalid_inner(),
    );
}

async fn restore_v2_agent_metadata_falls_back_when_indexed_identity_is_invalid_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let child_path = AgentPath::root()
        .join("fallback_worker")
        .expect("fallback agent path");
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("fallback worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(child_path.clone()),
                agent_nickname: Some("fallback-name".to_string()),
                agent_role: Some("fallback-role".to_string()),
            })),
        )
        .await
        .expect("fallback worker should spawn");
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("fallback worker should be loaded");
    persist_thread_for_tree_resume(&child_thread, "fallback worker persisted").await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("metadata restoration requires state db");
    let indexed_metadata = state_db
        .get_thread(child_thread_id)
        .await
        .expect("indexed metadata query should succeed")
        .expect("fallback worker metadata should exist");
    let original_agent_metadata = harness
        .control
        .state
        .agent_metadata_for_thread(child_thread_id)
        .expect("fallback worker should have registered metadata");

    for corrupt_source in [true, false] {
        let mut invalid_metadata = indexed_metadata.clone();
        if corrupt_source {
            invalid_metadata.source = "not valid session source".to_string();
        } else {
            invalid_metadata.agent_path = Some("not a canonical path".to_string());
        }
        state_db
            .upsert_thread(&invalid_metadata)
            .await
            .expect("invalid indexed identity should persist");

        let resumed_manager = ThreadManager::with_models_provider_home_and_state_for_tests(
            CodexAuth::from_api_key("dummy"),
            harness.config.model_provider.clone(),
            harness.config.codex_home.to_path_buf(),
            std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            harness.state_db.clone(),
        );
        let resumed_control = resumed_manager.agent_control();
        resumed_control
            .restore_v2_agent_metadata(&harness.config, parent_thread_id)
            .await;

        let restored_metadata = resumed_control
            .state
            .agent_metadata_for_thread(child_thread_id)
            .expect("invalid indexed identity should fall back to rollout metadata");
        assert_eq!(restored_metadata.agent_path, Some(child_path.clone()));
        assert_eq!(
            restored_metadata.agent_nickname,
            original_agent_metadata.agent_nickname
        );
        assert_eq!(
            restored_metadata.agent_role,
            original_agent_metadata.agent_role
        );
        assert_thread_not_loaded(&resumed_manager, child_thread_id).await;
    }
}

#[tokio::test]
async fn resume_agent_from_rollout_does_not_reopen_v2_descendants() {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Sqlite);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let worker_path = AgentPath::root().join("worker").expect("worker path");
    let worker_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("worker spawn should succeed");
    let reviewer_path = worker_path.join("reviewer").expect("reviewer path");
    let reviewer_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello reviewer"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: Some(reviewer_path.clone()),
                agent_nickname: None,
                agent_role: Some("reviewer".to_string()),
            })),
        )
        .await
        .expect("reviewer spawn should succeed");
    let sibling_thread_id = harness
        .spawn_anonymous_child(parent_thread_id, SpawnAgentOptions::default())
        .await;

    let worker_thread = harness
        .manager
        .get_thread(worker_thread_id)
        .await
        .expect("worker thread should exist");
    let reviewer_thread = harness
        .manager
        .get_thread(reviewer_thread_id)
        .await
        .expect("reviewer thread should exist");
    let sibling_thread = harness
        .manager
        .get_thread(sibling_thread_id)
        .await
        .expect("sibling thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&worker_thread, "worker persisted").await;
    persist_thread_for_tree_resume(&reviewer_thread, "reviewer persisted").await;
    persist_thread_for_tree_resume(&sibling_thread, "sibling persisted").await;
    wait_for_live_thread_spawn_children(
        &harness.control,
        parent_thread_id,
        &[worker_thread_id, sibling_thread_id],
    )
    .await;
    wait_for_live_thread_spawn_children(&harness.control, worker_thread_id, &[reviewer_thread_id])
        .await;

    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());

    let resumed_manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        harness.config.model_provider.clone(),
        harness.config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        harness.state_db.clone(),
    );
    let resumed_control = resumed_manager.agent_control();
    let resumed_parent_thread_id = resumed_control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("v2 root resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        resumed_control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_thread_not_loaded(&resumed_manager, worker_thread_id).await;
    assert_thread_not_loaded(&resumed_manager, reviewer_thread_id).await;
    assert_thread_not_loaded(&resumed_manager, sibling_thread_id).await;
    resumed_control
        .restore_v2_agent_metadata(&harness.config, parent_thread_id)
        .await;
    for thread_id in [worker_thread_id, sibling_thread_id] {
        assert!(resumed_control.ensure_agent_known(thread_id).is_ok());
    }

    resumed_control
        .close_agent(worker_thread_id)
        .await
        .expect("closing a restored sibling should succeed");

    let closed_worker = resumed_control.ensure_agent_known(worker_thread_id);
    let surviving_sibling = resumed_control.ensure_agent_known(sibling_thread_id);
    assert!(closed_worker.is_err());
    assert!(surviving_sibling.is_ok());
    assert_thread_not_loaded(&resumed_manager, sibling_thread_id).await;
}

#[tokio::test]
async fn encrypted_inter_agent_communication_clears_existing_last_task_message() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let agent_path = AgentPath::try_from("/root/worker").expect("agent path");
    let spawned_agent = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("old plaintext task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(agent_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await
        .expect("spawn_agent should succeed");
    assert_eq!(
        harness
            .control
            .state
            .agent_metadata_for_thread(spawned_agent.thread_id)
            .and_then(|metadata| metadata.last_task_message),
        Some("old plaintext task".to_string())
    );

    let communication = InterAgentCommunication::new_encrypted(
        AgentPath::root(),
        agent_path,
        Vec::new(),
        "encrypted-task".to_string(),
        /*trigger_turn*/ true,
    );
    harness
        .control
        .send_inter_agent_communication(
            spawned_agent.thread_id,
            communication,
            AgentCommunicationContext::new(AgentCommunicationKind::Followup, ThreadId::new()),
            /*parent_turn_id*/ None,
        )
        .await
        .expect("send_inter_agent_communication should succeed");

    assert_eq!(
        harness
            .control
            .state
            .agent_metadata_for_thread(spawned_agent.thread_id)
            .and_then(|metadata| metadata.last_task_message),
        None
    );
}

#[tokio::test]
async fn spawn_agent_creates_thread_and_sends_prompt() {
    let harness = AgentControlHarness::new().await;
    let thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("spawned"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");
    let _thread = harness
        .manager
        .get_thread(thread_id)
        .await
        .expect("thread should be registered");
    let expected = (
        thread_id,
        Op::UserInput {
            items: vec![UserInput::Text {
                text: "spawned".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        },
    );
    let captured = harness
        .manager
        .captured_ops()
        .into_iter()
        .find(|entry| *entry == expected);
    assert_eq!(captured, Some(expected));
}

#[tokio::test]
async fn ephemeral_spawn_does_not_persist_agent_graph_edge() {
    let (home, mut config) = test_config().await;
    config.ephemeral = true;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("spawned"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("ephemeral agent spawn should succeed");

    let persisted_children = harness
        .state_db
        .as_ref()
        .expect("manager should retain state db")
        .list_thread_spawn_children(parent_thread_id)
        .await
        .expect("persisted child list should load");
    assert_eq!(persisted_children, Vec::<ThreadId>::new());
    assert!(
        harness.manager.get_thread(child_thread_id).await.is_ok(),
        "ephemeral child should remain live"
    );
}

#[tokio::test]
async fn spawn_agent_fork_from_paginated_parent_persists_reference_backed_model_context() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    parent_thread
        .inject_user_message_without_turn("paginated parent context".to_string())
        .await;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-paginated".to_string();
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[spawn_agent_call(&parent_spawn_call_id)],
        )
        .await;
    parent_thread
        .session
        .persist_rollout_items(&[
            RolloutItem::ResponseItem(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: "id-less inherited context".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }),
            RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                thread_id: parent_thread_id,
                turn_id: "parent-turn".to_string(),
                item: TurnItem::UserMessage(UserMessageItem {
                    id: "parent-user".to_string(),
                    client_id: None,
                    content: Vec::new(),
                }),
                started_at_ms: Some(0),
                completed_at_ms: 1,
            })),
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
                ThreadSettingsAppliedEvent {
                    thread_settings: ThreadSettingsSnapshot {
                        model: "parent-only-model".to_string(),
                        model_provider_id: "parent-only-provider".to_string(),
                        service_tier: None,
                        approval_policy: AskForApproval::Never,
                        approvals_reviewer: ApprovalsReviewer::User,
                        permission_profile: PermissionProfile::workspace_write(),
                        active_permission_profile: None,
                        cwd: harness.config.cwd.clone(),
                        reasoning_effort: None,
                        reasoning_summary: None,
                        personality: None,
                        collaboration_mode: CollaborationMode {
                            mode: ModeKind::Default,
                            settings: Settings {
                                model: "parent-only-model".to_string(),
                                reasoning_effort: None,
                                developer_instructions: None,
                            },
                        },
                    },
                },
            )),
        ])
        .await;

    let child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let child_model_history = child_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .to_vec();
    assert!(
        history_contains_text(&child_model_history, "paginated parent context"),
        "bounded parent context should remain model-visible to the child"
    );
    assert!(
        child_model_history.iter().any(|item| {
            serde_json::to_string(item)
                .expect("serialize response item")
                .contains("id-less inherited context")
        }),
        "model history should contain inherited response item"
    );
    child_thread.ensure_rollout_materialized().await;
    child_thread
        .flush_rollout()
        .await
        .expect("child rollout should flush");
    let rollout_path = child_thread
        .rollout_path()
        .expect("child rollout should exist");
    let lines = std::fs::read_to_string(&rollout_path)
        .expect("read child rollout")
        .lines()
        .map(|line| serde_json::from_str::<RolloutLine>(line).expect("parse rollout line"))
        .collect::<Vec<_>>();
    let RolloutItem::SessionMeta(meta_line) = &lines[0].item else {
        panic!("child rollout should start with session metadata");
    };
    assert_eq!(meta_line.meta.history_mode, ThreadHistoryMode::Paginated);
    assert_eq!(meta_line.meta.parent_thread_id, Some(parent_thread_id));
    assert_eq!(meta_line.meta.forked_from_id, Some(parent_thread_id));
    let prefix_end = meta_line
        .meta
        .subagent_history_start_ordinal
        .expect("paginated child should mark its local history boundary");
    let copied_prefix = lines
        .iter()
        .skip(1)
        .filter(|line| line.ordinal.is_some_and(|ordinal| ordinal < prefix_end))
        .collect::<Vec<_>>();
    let reference_ordinal = copied_prefix
        .iter()
        .find_map(|line| {
            matches!(line.item, RolloutItem::RolloutReference(_)).then_some(line.ordinal)
        })
        .flatten()
        .expect("inherited prefix should contain an ordinaled rollout reference");
    assert_eq!(
        prefix_end,
        reference_ordinal + 1,
        "the first child-local ordinal should immediately follow the inherited reference"
    );
    assert_eq!(
        copied_prefix
            .iter()
            .filter(|line| matches!(line.item, RolloutItem::RolloutReference(_)))
            .count(),
        1,
        "the inherited model context should remain reference-backed"
    );
    assert!(
        !copied_prefix
            .iter()
            .any(|line| matches!(line.item, RolloutItem::SessionMeta(_))),
        "the source session metadata should not be copied into the child rollout"
    );
    assert!(
        !copied_prefix.iter().any(|line| {
            let serialized = serde_json::to_string(&line.item).expect("serialize rollout item");
            serialized.contains("paginated parent context")
                || serialized.contains("id-less inherited context")
        }),
        "the inherited response payload should not be duplicated in the child rollout"
    );
    assert!(
        lines
            .iter()
            .any(|line| { line.ordinal.is_some_and(|ordinal| ordinal >= prefix_end) }),
        "child-local history should start at the recorded boundary"
    );
    let logical_lines = codex_rollout::materialize_recent_rollout_lines_from(
        harness.config.codex_home.as_path(),
        lines.clone(),
    )
    .await
    .expect("materialize child rollout");
    let inherited_idless_context = logical_lines
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::ResponseItem(item)
                if serde_json::to_string(item)
                    .expect("serialize response item")
                    .contains("id-less inherited context") =>
            {
                Some(item)
            }
            _ => None,
        })
        .expect("materialized rollout should contain inherited response item");
    assert_eq!(
        inherited_idless_context.id(),
        None,
        "reference materialization should preserve the parent's response item"
    );
    let inherited_parent_context_count = logical_lines
        .iter()
        .filter(|line| {
            serde_json::to_string(&line.item)
                .expect("serialize rollout item")
                .contains("paginated parent context")
        })
        .count();
    assert_eq!(
        inherited_parent_context_count, 1,
        "the referenced parent context should materialize once"
    );
    assert!(
        !copied_prefix.iter().any(|line| {
            matches!(
                &line.item,
                RolloutItem::EventMsg(
                    EventMsg::ItemCompleted(_) | EventMsg::ThreadSettingsApplied(_)
                )
            )
        }),
        "copied non-structural presentation and metadata records should not enter the child rollout"
    );

    let live_child_history = child_thread.session.clone_history().await;
    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let resumed_child_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            child_thread_id,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            }),
        )
        .await
        .expect("paginated reference-backed child should cold resume");
    let resumed_child_thread = harness
        .manager
        .get_thread(resumed_child_thread_id)
        .await
        .expect("cold-resumed paginated child should be registered");
    let resumed_child_history = resumed_child_thread.session.clone_history().await;
    assert!(
        strip_response_item_ids(resumed_child_history.raw_items())
            .starts_with(&strip_response_item_ids(live_child_history.raw_items())),
        "cold resume should preserve the paginated child's materialized history and startup suffix"
    );
    let _ = harness
        .control
        .shutdown_live_agent(resumed_child_thread_id)
        .await
        .expect("cold-resumed paginated child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_without_fork_from_paginated_parent_stays_fresh_and_paginated() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    parent_thread
        .inject_user_message_without_turn("parent-only context".to_string())
        .await;

    let child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    assert!(
        !history_contains_text(
            child_thread.session.clone_history().await.raw_items(),
            "parent-only context",
        ),
        "fork_turns=none should not copy parent context"
    );
    child_thread.ensure_rollout_materialized().await;
    child_thread
        .flush_rollout()
        .await
        .expect("child rollout should flush");
    let meta = codex_rollout::read_session_meta_line(
        &child_thread
            .rollout_path()
            .expect("child rollout should exist"),
    )
    .await
    .expect("read child session metadata");
    assert_eq!(meta.meta.history_mode, ThreadHistoryMode::Paginated);
    assert_eq!(meta.meta.subagent_history_start_ordinal, None);

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_numeric_fork_from_compacted_paginated_parent_clamps_to_provable_turns() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    let parent_spawn_call_id = "spawn-call-paginated-numeric".to_string();
    parent_thread
        .session
        .persist_rollout_items(&[
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(vec![ResponseItem::Message {
                    id: None,
                    role: "user".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "compacted summary".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                }]),
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
            RolloutItem::ResponseItem(ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "recent parent turn".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }),
            RolloutItem::ResponseItem(spawn_agent_call(&parent_spawn_call_id)),
        ])
        .await;

    let clamped_child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await;
    let clamped_child_thread = harness
        .manager
        .get_thread(clamped_child_thread_id)
        .await
        .expect("clamped child thread should be registered");
    let clamped_history = clamped_child_thread.session.clone_history().await;
    assert!(
        history_contains_text(clamped_history.raw_items(), "recent parent turn"),
        "clamped numeric fork should keep the provable recent turn"
    );
    assert!(
        !history_contains_text(clamped_history.raw_items(), "compacted summary"),
        "clamped numeric fork should not expand into compacted parent context"
    );

    let _ = harness
        .control
        .shutdown_live_agent(clamped_child_thread_id)
        .await
        .expect("clamped child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_fork_rejects_missing_parent_spawn_call_id_for_public_forks() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;

    let err = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await
        .expect_err("forked worker spawns should require the parent spawn call id");

    assert_eq!(
        err.to_string(),
        "Fatal error: spawn_agent fork requires a parent spawn call id"
    );
}

#[tokio::test]
async fn spawn_agent_full_history_fork_uses_compact_reference_and_materializes_parent_items() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    parent_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Parent root guidance.".to_string());
    parent_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Parent subagent guidance.".to_string());
    let mut child_config = harness.config.clone();
    let _ = child_config.features.enable(Feature::MultiAgentV2);
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());
    let _ = child_config.features.enable(Feature::AgentPromptInjection);
    child_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Child root guidance.".to_string());
    child_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Child subagent guidance.".to_string());
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config.clone()))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    parent_thread
        .inject_user_message_without_turn("parent seed context".to_string())
        .await;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-history".to_string();
    let trigger_message = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "parent trigger message".to_string(),
        /*trigger_turn*/ true,
    );
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "Parent root guidance.".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "Parent subagent guidance.".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![
                        ContentItem::InputText {
                            text: "Developer context before.\nParent developer instructions.\nDeveloper context after."
                                .to_string(),
                        },
                        ContentItem::InputText {
                            text: "Preserved developer context.".to_string(),
                        },
                    ],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                assistant_message("parent commentary", Some(MessagePhase::Commentary)),
                assistant_message("parent final answer", Some(MessagePhase::FinalAnswer)),
                assistant_message("parent unknown phase", /*phase*/ None),
                ResponseItem::Reasoning {
                    id: Some(ResponseItemId::with_suffix("rs", "parent-reasoning")),
                    summary: vec![ReasoningItemReasoningSummary::SummaryText {
                        text: "parent reasoning summary".to_string(),
                    }],
                    content: Some(vec![ReasoningItemContent::ReasoningText {
                        text: "parent reasoning content".to_string(),
                    }]),
                    encrypted_content: Some("parent encrypted reasoning".to_string()),
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::FunctionCall {
                    id: Some(ResponseItemId::with_suffix("fc", "parent-tool-call")),
                    name: "parent_tool".to_string(),
                    namespace: None,
                    arguments: r#"{"value":1}"#.to_string(),
                    call_id: "parent-tool-call".to_string(),
                    encrypted_function_args: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::FunctionCallOutput {
                    id: Some(ResponseItemId::with_suffix("fco", "parent-tool-output")),
                    call_id: "parent-tool-call".to_string(),
                    output: FunctionCallOutputPayload::from_text("parent tool output".to_string()),
                    internal_chat_message_metadata_passthrough: None,
                },
                trigger_message.to_response_input_item().into(),
                spawn_agent_call(&parent_spawn_call_id),
            ],
        )
        .await;
    let parent_reference_context_item = turn_context.to_turn_context_item();
    parent_thread
        .session
        .persist_rollout_items(&[RolloutItem::TurnContext(
            parent_reference_context_item.clone(),
        )])
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let parent_history = parent_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .to_vec();
    // Match the live baseline established by the persisted TurnContext in a real completed turn.
    parent_thread
        .session
        .replace_history(
            parent_history.clone(),
            Some(parent_reference_context_item.clone()),
        )
        .await;
    let child_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });
    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config.clone(),
            text_input("child task"),
            Some(child_source.clone()),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                initial_task_message: Some("child task".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should succeed")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    child_thread
        .flush_rollout()
        .await
        .expect("child rollout should flush");
    let child_rollout_path = child_thread.rollout_path().expect("child rollout path");
    let child_physical_items = RolloutRecorder::load_rollout_items(child_rollout_path.as_path())
        .await
        .expect("read child physical rollout")
        .0;
    assert!(matches!(
        child_physical_items.as_slice(),
        [
            RolloutItem::SessionMeta(_),
            RolloutItem::RolloutReference(_),
            ..
        ]
    ));
    let parent_response_items = parent_history
        .iter()
        .map(|item| serde_json::to_value(item).expect("serialize parent response item"))
        .collect::<Vec<_>>();
    assert!(child_physical_items.iter().all(|item| {
        let RolloutItem::ResponseItem(item) = item else {
            return true;
        };
        !parent_response_items
            .contains(&serde_json::to_value(item).expect("serialize child physical response item"))
    }));
    assert_ne!(child_thread_id, parent_thread_id);
    assert_eq!(
        child_thread.config_snapshot().await.history_mode,
        ThreadHistoryMode::Legacy
    );
    assert_eq!(
        child_thread.session.prompt_cache_key(),
        parent_thread.session.prompt_cache_key(),
    );
    let child_mcp_runtime = Arc::clone(&child_thread.session.services.mcp_runtime);
    let parent_mcp_runtime = Arc::clone(&parent_thread.session.services.mcp_runtime);
    assert!(!Arc::ptr_eq(&child_mcp_runtime, &parent_mcp_runtime));
    let mcp_tool_snapshot = child_thread
        .session
        .services
        .mcp_tool_snapshot
        .lock()
        .await
        .clone()
        .expect("forked child should inherit an MCP tool snapshot");
    let parent_binding = parent_mcp_runtime
        .current_binding()
        .await
        .expect("parent should have a published MCP binding");
    assert_eq!(
        serde_json::to_value(&mcp_tool_snapshot.tools).expect("serialize inherited MCP tools"),
        serde_json::to_value(parent_binding.tools()).expect("serialize parent MCP tools"),
    );
    let history = child_thread.session.clone_history().await;
    let subagent_prompt = crate::session::load_subagent_prompt(&harness.config.codex_home).await;
    let mut expected_history = parent_history.clone();
    expected_history.extend([
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "Child developer instructions.".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "Child subagent guidance.".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: subagent_prompt,
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "# Subagent Assignment\n\nYou are `this subagent`. Your direct assignment from your parent agent is:\n\nchild task".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
    ]);
    assert_eq!(
        strip_response_item_ids(history.raw_items()),
        strip_response_item_ids(expected_history.as_slice()),
        "full-history forked child history should preserve the exact parent prefix before adding child-specific context"
    );
    assert_eq!(
        serde_json::to_value(child_thread.session.reference_context_item().await)
            .expect("serialize child reference context item"),
        serde_json::to_value(Some(parent_reference_context_item))
            .expect("serialize expected reference context item"),
        "full-history forked child should preserve the parent diff baseline"
    );

    let mut no_hint_child_config = harness.config.clone();
    let _ = no_hint_child_config.features.enable(Feature::MultiAgentV2);
    no_hint_child_config.developer_instructions = Some(String::new());
    no_hint_child_config
        .multi_agent_v2
        .subagent_developer_instructions = Some(String::new());
    no_hint_child_config.multi_agent_v2.subagent_usage_hint_text = None;
    let no_hint_child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            no_hint_child_config,
            text_input("child task without hints"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should honor an empty subagent usage hint")
        .thread_id;
    let no_hint_child_thread = harness
        .manager
        .get_thread(no_hint_child_thread_id)
        .await
        .expect("no-hint child thread should be registered");
    let no_hint_history = no_hint_child_thread.session.clone_history().await;
    assert!(
        !history_contains_text(no_hint_history.raw_items(), "Child subagent guidance."),
        "full-history forked child should not add empty subagent guidance"
    );
    assert!(
        history_contains_text(
            no_hint_history.raw_items(),
            "Parent developer instructions."
        ),
        "empty child developer instructions should not alter the inherited parent prefix"
    );
    assert!(
        history_contains_text(
            no_hint_history.raw_items(),
            "Developer context before.\nParent developer instructions.\nDeveloper context after."
        ),
        "empty child developer instructions should preserve the exact inherited developer context"
    );
    assert!(
        history_contains_text(no_hint_history.raw_items(), "Preserved developer context."),
        "empty child developer instructions should preserve unrelated developer fragments"
    );

    let expected = (
        child_thread_id,
        Op::UserInput {
            items: vec![UserInput::Text {
                text: "child task".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        },
    );
    let captured = harness
        .manager
        .captured_ops()
        .into_iter()
        .find(|entry| *entry == expected);
    assert_eq!(captured, Some(expected));

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let resumed_child_thread_id = harness
        .control
        .resume_agent_from_rollout(child_config, child_thread_id, child_source)
        .await
        .expect("reference-backed child should cold resume");
    let resumed_child_thread = harness
        .manager
        .get_thread(resumed_child_thread_id)
        .await
        .expect("cold-resumed child should be registered");
    let resumed_history = resumed_child_thread.session.clone_history().await;
    let resumed_history = strip_response_item_ids(resumed_history.raw_items());
    let expected_history = strip_response_item_ids(expected_history.as_slice());
    assert!(
        resumed_history.starts_with(expected_history.as_slice()),
        "cold resume should reconstruct the exact inherited prefix and durable child suffix"
    );
    let _ = harness
        .control
        .shutdown_live_agent(resumed_child_thread_id)
        .await
        .expect("cold-resumed child shutdown should submit");
    let _ = harness
        .control
        .shutdown_live_agent(no_hint_child_thread_id)
        .await
        .expect("no-hint child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[test]
fn goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt() {
    run_goal_supervisor_test(
        "goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt",
        goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt_inner(),
    );
}

async fn goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt_inner() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::AgentPromptInjection);
    let _ = parent_config.features.enable(Feature::Goals);
    let _ = parent_config.features.enable(Feature::GoalSupervisor);
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    parent_thread
        .inject_user_message_without_turn("parent seed context".to_string())
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let before_thread_ids = harness.manager.list_thread_ids().await;
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Ship the active user goal.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-test",
        &goal,
    )
    .await
    .expect("goal supervisor helper should spawn");

    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let helper_thread = harness
        .manager
        .get_thread(helper_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    assert_eq!(
        helper_thread.session.prompt_cache_key(),
        parent_thread.session.prompt_cache_key(),
        "goal supervisor helpers are internal full-history forks and must keep the parent prompt cache key"
    );

    let helper_history = helper_thread.session.clone_history().await;
    assert!(
        helper_history.raw_items().iter().any(|item| matches!(
            item,
            ResponseItem::Message { role, content, .. }
                if role == "user"
                    && content.iter().any(|content_item| matches!(
                        content_item,
                        ContentItem::InputText { text } if text == "parent seed context"
                    ))
        )),
        "goal supervisor helpers must inherit the parent conversation prefix before their supervisor assignment"
    );
    let supervisor_prompt =
        crate::session::load_supervisor_agent_prompt(&harness.config.codex_home).await;
    let supervisor_prompt_count = helper_history
        .raw_items()
        .iter()
        .filter(|item| {
            matches!(
                item,
                ResponseItem::Message { role, content, .. }
                    if role == "developer"
                        && content.iter().any(|content_item| matches!(
                            content_item,
                            ContentItem::InputText { text } if text == &supervisor_prompt
                        ))
            )
        })
        .count();
    assert_eq!(
        supervisor_prompt_count, 1,
        "the supervisor prompt should be injected once as the helper role prompt; duplicating it in the user assignment changes the post-fork context"
    );
    assert!(helper_history.raw_items().iter().any(|item| matches!(
        item,
        ResponseItem::FunctionCall { name, call_id, .. }
            if name == "list_agents" && call_id == "synthetic_supervisor_list_agents"
    )));

    let captured_input = harness
        .manager
        .captured_ops()
        .into_iter()
        .find_map(|(thread_id, op)| {
            if thread_id != helper_thread_id {
                return None;
            }
            match op {
                Op::UserInput { items, .. } => items.into_iter().find_map(|item| match item {
                    UserInput::Text { text, .. } => Some(text),
                    UserInput::Image { .. }
                    | UserInput::LocalImage { .. }
                    | UserInput::Skill { .. }
                    | UserInput::Mention { .. } => None,
                    _ => None,
                }),
                _ => None,
            }
        })
        .expect("supervisor helper assignment should be submitted as user input");
    assert!(captured_input.contains("# Goal Supervisor Assignment"));
    assert!(captured_input.contains("Ship the active user goal."));
    assert!(
        !captured_input.contains("You are also a **goal supervisor**"),
        "the supervisor role prompt must not be copied into the helper assignment"
    );
}

#[test]
fn finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge() {
    run_goal_supervisor_test(
        "finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge",
        finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge_inner(),
    );
}

async fn finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let mut helper_config = harness.config.clone();
    helper_config.ephemeral = true;
    let helper_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            helper_config,
            text_input("supervise"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(supervisor_path),
                agent_nickname: None,
                agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("supervisor helper should spawn")
        .thread_id;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            helper_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("materialized helper edge should persist");

    harness
        .control
        .finish_internal_helper_thread(helper_thread_id)
        .await
        .expect("supervisor helper should finish");

    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    let closed_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("closed child query should succeed");
    assert!(!open_children.contains(&helper_thread_id));
    assert!(closed_children.contains(&helper_thread_id));
}

#[cfg(target_os = "linux")]
#[test]
fn finished_goal_supervisor_retires_session_and_rollout_descriptor() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "finished_goal_supervisor_retires_session_and_rollout_descriptor",
        finished_goal_supervisor_retires_session_and_rollout_descriptor_inner(),
    )
}

#[cfg(target_os = "linux")]
async fn finished_goal_supervisor_retires_session_and_rollout_descriptor_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    Mock::given(method("POST"))
        .and(path_regex(".*/responses$"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(vec![
                    ev_response_created("helper-response"),
                    ev_completed("helper-response"),
                ]))
                .set_delay(Duration::from_millis(250)),
        )
        .mount(&server)
        .await;
    let (home, mut config) = test_config().await;
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let agent_control = parent_thread.session.services.agent_control.clone();
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let mut helper_config = harness.config.clone();
    helper_config.ephemeral = true;
    let helper_thread_id = agent_control
        .spawn_agent_with_metadata(
            helper_config,
            text_input("supervise"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(supervisor_path),
                agent_nickname: None,
                agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await?
        .thread_id;
    let retained_helper = harness.manager.get_thread(helper_thread_id).await?;
    timeout(Duration::from_secs(5), async {
        while !matches!(retained_helper.agent_status().await, AgentStatus::Running) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("supervisor turn should start");
    retained_helper.ensure_rollout_materialized().await;
    let helper_rollout_path = retained_helper
        .session
        .current_rollout_path()
        .await?
        .expect("materialized supervisor should have a live rollout path");
    assert!(
        open_rollout_writer_paths(harness.config.codex_home.as_path())
            .contains(&helper_rollout_path),
        "the active supervisor must hold its rollout descriptor: {}",
        helper_rollout_path.display()
    );

    agent_control
        .finish_internal_helper_thread(helper_thread_id)
        .await?;
    assert_thread_not_loaded(&harness.manager, helper_thread_id).await;
    timeout(
        Duration::from_secs(5),
        retained_helper.wait_until_terminated(),
    )
    .await
    .expect("supervisor session should terminate");
    timeout(Duration::from_secs(2), async {
        loop {
            let event = retained_helper
                .next_event()
                .await
                .expect("retained helper event channel should remain readable");
            if matches!(event.msg, EventMsg::TurnComplete(_)) {
                break;
            }
        }
    })
    .await
    .expect("supervisor turn should complete before shutdown");
    timeout(Duration::from_secs(2), async {
        while open_rollout_writer_paths(harness.config.codex_home.as_path())
            .contains(&helper_rollout_path)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "retired supervisor must release its rollout descriptor: {}",
            helper_rollout_path.display()
        )
    });
    parent_thread.shutdown_and_wait().await?;
    Ok(())
}

#[test]
fn goal_supervisor_spawn_reconciles_stale_persisted_state() {
    run_goal_supervisor_test(
        "goal_supervisor_spawn_reconciles_stale_persisted_state",
        goal_supervisor_spawn_reconciles_stale_persisted_state_inner(),
    );
}

async fn goal_supervisor_spawn_reconciles_stale_persisted_state_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Continue the daily release cycle.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let stale_helper_thread_ids = [ThreadId::new(), ThreadId::new()];
    for stale_helper_thread_id in stale_helper_thread_ids {
        let stale_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth: 1,
            agent_path: Some(supervisor_path.clone()),
            agent_nickname: None,
            agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
        });
        let stale_metadata = codex_state::ThreadMetadataBuilder::new(
            stale_helper_thread_id,
            harness
                .config
                .codex_home
                .join(format!("{stale_helper_thread_id}.jsonl"))
                .to_path_buf(),
            chrono::Utc::now(),
            stale_source,
        )
        .build("openai");
        state_db
            .upsert_thread(&stale_metadata)
            .await
            .expect("stale supervisor metadata should persist");
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                stale_helper_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("stale supervisor edge should persist");
    }
    harness
        .control
        .restore_v2_agent_metadata(&harness.config, parent_thread_id)
        .await;
    assert!(
        harness
            .control
            .state
            .agent_id_for_path(&supervisor_path)
            .is_some_and(|thread_id| stale_helper_thread_ids.contains(&thread_id)),
        "cold restore should reproduce the stale canonical path collision"
    );
    for stale_helper_thread_id in stale_helper_thread_ids {
        assert_eq!(
            harness.control.get_status(stale_helper_thread_id).await,
            AgentStatus::NotFound,
            "restored supervisor metadata must not be mistaken for a live helper"
        );
    }

    let replacement_thread_id =
        crate::goal_supervisor::spawn_supervisor_helper_for_test(&parent_thread.session, &goal)
            .await
            .expect("new supervisor spawn should reconcile stale persisted state");

    assert!(!stale_helper_thread_ids.contains(&replacement_thread_id));
    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    let closed_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("closed child query should succeed");
    for stale_helper_thread_id in stale_helper_thread_ids {
        assert!(!open_children.contains(&stale_helper_thread_id));
        assert!(closed_children.contains(&stale_helper_thread_id));
    }
}

#[test]
fn goal_supervisor_reconciliation_preserves_running_supervisor_and_worker() {
    run_goal_supervisor_test(
        "goal_supervisor_reconciliation_preserves_running_supervisor_and_worker",
        goal_supervisor_reconciliation_preserves_running_supervisor_and_worker_inner(),
    );
}

async fn goal_supervisor_reconciliation_preserves_running_supervisor_and_worker_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let mut helper_config = harness.config.clone();
    helper_config.ephemeral = true;
    let helper_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            helper_config,
            text_input("supervise"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(supervisor_path.clone()),
                agent_nickname: None,
                agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("supervisor helper should spawn")
        .thread_id;
    let worker_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("work"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(AgentPath::root().join("worker").expect("worker path")),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("worker should spawn")
        .thread_id;
    for child_thread_id in [helper_thread_id, worker_thread_id] {
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                child_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("child edge should persist");
    }
    let foreign_supervisor_thread_id = ThreadId::new();
    let foreign_supervisor_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: ThreadId::new(),
        depth: 1,
        agent_path: Some(
            AgentPath::root()
                .join("foreign_goal_supervisor")
                .expect("foreign supervisor path"),
        ),
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let foreign_supervisor_metadata = codex_state::ThreadMetadataBuilder::new(
        foreign_supervisor_thread_id,
        harness
            .config
            .codex_home
            .join(format!("{foreign_supervisor_thread_id}.jsonl"))
            .to_path_buf(),
        chrono::Utc::now(),
        foreign_supervisor_source,
    )
    .build("openai");
    state_db
        .upsert_thread(&foreign_supervisor_metadata)
        .await
        .expect("foreign supervisor metadata should persist");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            foreign_supervisor_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("inaccurate foreign supervisor edge should persist");

    let first_result = harness
        .control
        .reconcile_goal_supervisor_state(parent_thread_id, &supervisor_path)
        .await
        .expect("first reconciliation should succeed");
    let second_result = harness
        .control
        .reconcile_goal_supervisor_state(parent_thread_id, &supervisor_path)
        .await
        .expect("second reconciliation should be idempotent");

    assert_eq!(first_result, Some(helper_thread_id));
    assert_eq!(second_result, Some(helper_thread_id));
    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    assert!(open_children.contains(&helper_thread_id));
    assert!(open_children.contains(&worker_thread_id));
    assert!(
        open_children.contains(&foreign_supervisor_thread_id),
        "reconciliation must not trust an inaccurate edge over the stored supervisor parent"
    );
}

#[test]
fn goal_supervisor_execution_settings_change_restarts_running_helper() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_execution_settings_change_restarts_running_helper",
        goal_supervisor_execution_settings_change_restarts_running_helper_inner(),
    )
}

async fn goal_supervisor_execution_settings_change_restarts_running_helper_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let delayed_response = Duration::from_secs(30);
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("resp-before-settings-change"),
                ev_completed("resp-before-settings-change"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("resp-after-settings-change"),
                ev_completed("resp-after-settings-change"),
            ]))
            .set_delay(delayed_response),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread.flush_rollout().await?;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Restart the supervisor when execution settings change.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent_thread.session, &goal_id, &goal)
        .await?;
    let first_helper_thread_id =
        spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first supervisor request should start");

    let original_model = parent_thread.session.thread_config_snapshot().await.model;
    let next_model = if original_model == "gpt-5.4" {
        "gpt-5.2"
    } else {
        "gpt-5.4"
    };
    let settings_changed_at = Instant::now();
    parent_thread
        .submit(Op::ThreadSettings {
            thread_settings: ThreadSettingsOverrides {
                model: Some(next_model.to_string()),
                effort: Some(Some(ReasoningEffort::High)),
                service_tier: Some(Some(ServiceTier::Fast.request_value().to_string())),
                ..Default::default()
            },
        })
        .await?;
    timeout(Duration::from_secs(5), async {
        while !harness
            .manager
            .captured_ops()
            .into_iter()
            .any(|(thread_id, op)| {
                thread_id == first_helper_thread_id && matches!(op, Op::Shutdown)
            })
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("settings update should stop the running supervisor helper");

    let replacement_started = timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        replacement_started.is_ok(),
        "replacement supervisor request should start before the old response finishes: threads={:?}, ops={:?}",
        harness.manager.list_thread_ids().await,
        harness.manager.captured_ops(),
    );
    assert!(settings_changed_at.elapsed() < delayed_response);

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let replacement_body = requests[1].body_json();
    assert_eq!(replacement_body["model"].as_str(), Some(next_model));
    assert_eq!(
        replacement_body["reasoning"]["effort"].as_str(),
        Some("high")
    );
    assert_eq!(
        replacement_body["service_tier"].as_str(),
        Some(ServiceTier::Fast.request_value())
    );

    let _ = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[test]
fn goal_supervisor_waits_for_parent_turn_to_finish() {
    run_goal_supervisor_test(
        "goal_supervisor_waits_for_parent_turn_to_finish",
        goal_supervisor_waits_for_parent_turn_to_finish_inner(),
    );
}

async fn goal_supervisor_waits_for_parent_turn_to_finish_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Wait for the parent turn, then continue.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let parent_only = harness.manager.list_thread_ids().await;
    *parent_thread.session.active_turn.lock().await = Some(ActiveTurn::default());

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-busy-parent-test",
        &goal,
    )
    .await
    .expect("busy parent should defer the supervisor");

    assert_eq!(harness.manager.list_thread_ids().await, parent_only);

    *parent_thread.session.active_turn.lock().await = None;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-busy-parent-test",
        &goal,
    )
    .await
    .expect("idle parent should start the deferred supervisor");

    let helper_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    assert!(harness.manager.get_thread(helper_thread_id).await.is_ok());
}

#[test]
fn goal_supervisor_finish_serializes_with_the_next_start() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_finish_serializes_with_the_next_start",
        goal_supervisor_finish_serializes_with_the_next_start_inner(),
    )
}

async fn goal_supervisor_finish_serializes_with_the_next_start_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let delayed_response = Duration::from_secs(30);
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("first-supervisor"),
                ev_completed("first-supervisor"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("replacement-supervisor"),
                ev_completed("replacement-supervisor"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("post-followup-supervisor"),
                ev_completed("post-followup-supervisor"),
            ]))
            .set_delay(delayed_response),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread.flush_rollout().await?;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Serialize supervisor retirement and replacement.",
    )
    .await?;
    let parent_only = harness.manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_helper_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first supervisor request should start");

    let transition =
        crate::goal_supervisor::hold_supervisor_transition_for_test(&parent_thread.session).await;
    let finish_session = Arc::clone(&parent_thread.session);
    let finish_task = tokio::spawn(async move {
        crate::goal_supervisor::finish_supervisor_helper(&finish_session, first_helper_thread_id)
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !finish_task.is_finished(),
        "supervisor retirement must wait for the lifecycle transition lock"
    );
    drop(transition);
    assert!(
        finish_task.await.expect("finish task should not panic")?,
        "the active supervisor should retire"
    );

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let replacement_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    assert_ne!(replacement_thread_id, first_helper_thread_id);
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement supervisor request should start");

    crate::goal_supervisor::record_followup_action(
        &parent_thread.session,
        &InterAgentCommunication::new(
            AgentPath::root()
                .join("goal_supervisor")
                .expect("supervisor path"),
            AgentPath::root(),
            Vec::new(),
            "continue".to_string(),
            /*trigger_turn*/ true,
        ),
    )
    .await;
    assert!(
        crate::goal_supervisor::finish_supervisor_helper_after_followup(
            &parent_thread.session,
            replacement_thread_id,
        )
        .await?,
        "the replacement supervisor should retire after its followup"
    );
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 3 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("idle recheck should replace a supervisor after its delivered followup");

    let _ = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[test]
fn successful_supervisor_parent_compaction_is_recorded() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "successful_supervisor_parent_compaction_is_recorded",
        successful_supervisor_parent_compaction_is_recorded_inner(),
    )
}

async fn successful_supervisor_parent_compaction_is_recorded_inner() -> anyhow::Result<()> {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Record parent compaction in supervisor continuity.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let before_thread_ids = harness.manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "parent-compaction-goal",
        &goal,
    )
    .await?;
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let result = harness
        .control
        .compact_parent_for_goal_supervisor_helper(helper_thread_id)
        .await?;
    assert_matches!(
        result,
        SupervisorParentCompactionResult::Submitted {
            parent_thread_id: submitted_parent_thread_id,
            ..
        } if submitted_parent_thread_id == parent_thread_id
    );
    assert!(
        harness
            .manager
            .captured_ops()
            .iter()
            .any(|(thread_id, op)| { *thread_id == parent_thread_id && matches!(op, Op::Compact) })
    );

    let continuity_text = goal_supervisor_continuity_text_for_test(
        &parent_thread.session,
        "parent-compaction-goal",
        &goal,
    )
    .await;
    assert!(continuity_text.contains("\"kind\": \"compact_parent_context\""));
    Ok(())
}

#[test]
fn goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll()
-> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll",
        goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll_inner(),
    )
}

async fn goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll_inner()
-> anyhow::Result<()> {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Continue scheduled work without repeating unchanged checks.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let reason = format!(
        "external API\nreturned\tno new results:\r {}",
        "\u{1F980}".repeat(256)
    );
    assert_eq!(
        harness
            .control
            .snooze_goal_supervisor_helper(
                helper_thread_id,
                /*delay_seconds*/ 60,
                Some(reason.as_str()),
            )
            .await,
        Some(60),
    );

    let expected_reason = reason
        .split_whitespace()
        .flat_map(|word| word.chars().chain(std::iter::once(' ')))
        .filter(|character| !character.is_control())
        .take(120)
        .collect::<String>()
        .trim_end()
        .to_string();
    let expected_snooze = format!("Snooze 60s: {expected_reason}");
    let parent_history = parent_thread.session.clone_history().await;
    let snooze_messages = parent_history
        .raw_items()
        .iter()
        .filter_map(|item| match item {
            ResponseItem::Message { role, content, .. } if role == "assistant" => {
                InterAgentCommunication::from_message_content(content)
            }
            _ => None,
        })
        .filter(|communication| communication.author.as_str() == "/root/goal_supervisor")
        .collect::<Vec<_>>();
    assert_eq!(
        snooze_messages.len(),
        1,
        "one supervisor snooze should produce exactly one model-visible parent message"
    );
    let snooze_message = &snooze_messages[0];
    assert_eq!(snooze_message.recipient, AgentPath::root());
    assert_eq!(snooze_message.content, expected_snooze);
    assert!(
        !snooze_message.content.chars().any(char::is_control),
        "a snooze must remain a compact, single-line history message"
    );
    assert!(
        !snooze_message.trigger_turn,
        "recording a snooze must not wake the parent agent"
    );
    assert!(
        parent_thread.session.active_turn.lock().await.is_none(),
        "recording a snooze must leave the parent agent idle"
    );
    assert!(
        harness
            .manager
            .captured_ops()
            .iter()
            .all(|(thread_id, op)| {
                *thread_id != parent_thread_id
                    || !matches!(
                        op,
                        Op::UserInput { .. } | Op::InterAgentCommunication { .. }
                    )
            }),
        "recording a snooze must not submit a parent user turn or queue parent mail"
    );

    parent_thread.flush_rollout().await?;
    let rollout_path = parent_thread
        .rollout_path()
        .expect("the parent rollout should be materialized");
    let codex_protocol::protocol::InitialHistory::Resumed(persisted_history) =
        RolloutRecorder::get_rollout_history(&rollout_path).await?
    else {
        anyhow::bail!("the parent rollout should reconstruct as resumed history");
    };
    assert!(
        persisted_history.history.iter().any(|item| {
            let RolloutItem::ResponseItem(ResponseItem::Message { role, content, .. }) = item
            else {
                return false;
            };
            role == "assistant"
                && InterAgentCommunication::from_message_content(content).is_some_and(
                    |communication| {
                        communication.author.as_str() == "/root/goal_supervisor"
                            && communication.content == expected_snooze
                            && !communication.trigger_turn
                    },
                )
        }),
        "the compact snooze message must survive rollout persistence"
    );
    assert!(
        !persisted_history.history.iter().any(|item| matches!(
            item,
            RolloutItem::EventMsg(EventMsg::Warning(warning))
                if warning.message.starts_with("Supervisor snoozed for ")
        )),
        "a snooze must not also produce a duplicate warning"
    );

    let continuity_text =
        goal_supervisor_continuity_text_for_test(&parent_thread.session, &goal_id, &goal).await;
    let continuity_json = continuity_text
        .strip_prefix("# Goal Supervisor Continuity\n\n")
        .expect("supervisor continuity should contain a JSON developer message");
    let continuity: serde_json::Value = serde_json::from_str(continuity_json)?;
    assert_eq!(continuity["previous_supervisor_action"]["kind"], "snooze");
    assert_eq!(
        continuity["previous_supervisor_action"]["snoozed_seconds"],
        60
    );
    assert_eq!(
        continuity["goal_timing"]["snooze_count_since_goal_created"],
        1
    );
    assert_eq!(
        continuity["goal_timing"]["snoozed_seconds_since_goal_created"],
        60
    );
    assert_eq!(
        continuity["parent_timing"]["snooze_count_since_last_parent_message"],
        1
    );
    assert_eq!(
        continuity["parent_timing"]["snoozed_seconds_since_last_parent_message"],
        60
    );

    let parent_completion = RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: "unchanged-parent-poll".to_string(),
        last_agent_message: Some("The external API returned no new results.".to_string()),
        error: None,
        started_at: None,
        completed_at: Some(chrono::Utc::now().timestamp().saturating_add(1)),
        duration_ms: None,
        time_to_first_token_ms: None,
    }));
    let continuity_item = crate::goal_supervisor::supervisor_continuity_context_item(
        &parent_thread.session,
        &goal_id,
        &goal,
        &[parent_completion],
    )
    .await;
    let RolloutItem::ResponseItem(ResponseItem::Message { content, .. }) = continuity_item else {
        anyhow::bail!("supervisor continuity should be a developer message");
    };
    let updated_continuity_text = content
        .into_iter()
        .find_map(|item| match item {
            ContentItem::InputText { text } => Some(text),
            _ => None,
        })
        .expect("supervisor continuity should contain text");
    let updated_continuity_json = updated_continuity_text
        .strip_prefix("# Goal Supervisor Continuity\n\n")
        .expect("supervisor continuity should contain a JSON developer message");
    let updated_continuity: serde_json::Value = serde_json::from_str(updated_continuity_json)?;
    assert_eq!(
        updated_continuity["parent_timing"]["snooze_count_since_last_parent_message"],
        0
    );
    assert_eq!(
        updated_continuity["parent_timing"]["snoozed_seconds_since_last_parent_message"],
        0
    );
    assert_eq!(
        updated_continuity["goal_timing"]["snooze_count_since_goal_created"], 1,
        "an unchanged completed parent poll must not reset goal-lifetime backoff"
    );
    assert_eq!(
        updated_continuity["goal_timing"]["snoozed_seconds_since_goal_created"], 60,
        "an unchanged completed parent poll must preserve prior snooze duration"
    );

    let replacement_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_thread_id,
            "Begin a different recurring schedule.",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    let replacement_goal_id = replacement_goal.goal_id.clone();
    let replacement_goal = crate::goal_supervisor::protocol_goal_from_state(replacement_goal);
    let replacement_continuity_text = goal_supervisor_continuity_text_for_test(
        &parent_thread.session,
        &replacement_goal_id,
        &replacement_goal,
    )
    .await;
    let replacement_continuity_json = replacement_continuity_text
        .strip_prefix("# Goal Supervisor Continuity\n\n")
        .expect("replacement goal continuity should contain a JSON developer message");
    let replacement_continuity: serde_json::Value =
        serde_json::from_str(replacement_continuity_json)?;
    assert!(
        replacement_continuity["previous_supervisor_action"].is_null(),
        "a new goal must not inherit a previous goal's supervisor action"
    );
    assert_eq!(
        replacement_continuity["goal_timing"]["snooze_count_since_goal_created"], 0,
        "a new goal must not inherit a previous goal's polling backoff"
    );
    assert_eq!(
        replacement_continuity["goal_timing"]["snoozed_seconds_since_goal_created"], 0,
        "a new goal must not inherit a previous goal's snooze duration"
    );
    assert_eq!(
        replacement_continuity["parent_timing"]["snooze_count_since_last_parent_message"], 0,
        "a new goal must not inherit a previous goal's parent-relative snooze count"
    );
    assert_eq!(
        replacement_continuity["parent_timing"]["snoozed_seconds_since_last_parent_message"], 0,
        "a new goal must not inherit a previous goal's parent-relative snooze duration"
    );

    let _ = parent_thread.submit(Op::Shutdown {}).await;
    Ok(())
}

#[test]
fn goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit() {
    run_goal_supervisor_test(
        "goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit",
        goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit_inner(),
    );
}

async fn goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit_inner() {
    let (home, mut config) = test_config().await;
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow multi-agent v2");
    config
        .features
        .enable(Feature::Goals)
        .expect("test config should allow goals");
    config
        .features
        .enable(Feature::GoalSupervisor)
        .expect("test config should allow goal supervisor");
    config.agent_max_threads = None;
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    assert_eq!(
        (
            config.agent_max_threads,
            config.multi_agent_v2.max_concurrent_threads_per_session,
            config.effective_agent_max_threads(MultiAgentVersion::V2),
        ),
        (None, 2, Some(1))
    );
    let harness = AgentControlHarness::new_with_config(home, config.clone()).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let worker_thread_id = ThreadId::new();
    harness
        .control
        .state
        .reserve_spawn_slot(Some(1))
        .expect("the user-visible worker slot should be available")
        .commit(AgentMetadata {
            agent_id: Some(worker_thread_id),
            ..Default::default()
        });
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let before_thread_ids = harness.manager.list_thread_ids().await;
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Verify supervisor thread accounting.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-limit-test",
        &goal,
    )
    .await
    .expect("goal supervisor should bypass the user-visible agent limit");
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;

    let err = match harness
        .control
        .state
        .reserve_spawn_slot(config.effective_agent_max_threads(MultiAgentVersion::V2))
    {
        Ok(_) => panic!("the goal supervisor must not free the counted worker slot"),
        Err(err) => err,
    };
    let CodexErrorDetails::AgentLimitReached { max_threads } = err.details() else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*max_threads, 1);
    assert!(harness.manager.get_thread(helper_thread_id).await.is_ok());

    harness
        .control
        .state
        .release_spawned_thread(worker_thread_id);
    let _ = harness.control.shutdown_live_agent(helper_thread_id).await;
    let _ = parent_thread.submit(Op::Shutdown {}).await;
}

#[test]
fn goal_supervisor_goal_resume_clears_snooze_and_spawns_helper() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_goal_resume_clears_snooze_and_spawns_helper",
        goal_supervisor_goal_resume_clears_snooze_and_spawns_helper_inner(),
    )
}

async fn goal_supervisor_goal_resume_clears_snooze_and_spawns_helper_inner() -> anyhow::Result<()> {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Resume the paused goal now.",
    )
    .await?;
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            goal_id.as_str(),
            Some(chrono::Utc::now().timestamp_millis() + 60_000),
        )
        .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let before_resume_thread_ids = harness.manager.list_thread_ids().await;
    assert_eq!(
        vec![parent_thread_id],
        before_resume_thread_ids,
        "plain idle continuation should honor the supervisor snooze"
    );

    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await?;

    let child_thread_id =
        spawned_thread_id_after(&harness.manager, &before_resume_thread_ids).await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    let child_config = child_thread.config_snapshot().await;
    assert_matches!(
        child_config.session_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_role: Some(agent_role),
            ..
        }) if agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME
    );
    assert_eq!(
        None,
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
            .await?,
        "manual goal resume should clear the persisted supervisor snooze"
    );
    let _ = parent_thread.submit(Op::Shutdown {}).await;
    Ok(())
}

#[test]
fn failed_goal_supervisor_waits_for_one_persisted_retry() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "failed_goal_supervisor_waits_for_one_persisted_retry",
        failed_goal_supervisor_waits_for_one_persisted_retry_inner(),
    )
}

async fn failed_goal_supervisor_waits_for_one_persisted_retry_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse_failed(
                "supervisor-failure-1",
                "model_not_found",
                "saved model unavailable",
            ),
            sse_failed(
                "supervisor-failure-2",
                "model_not_found",
                "saved model unavailable",
            ),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Keep retrying after transient supervisor failures.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;

    let first_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(deadline_ms) = state_db
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                .await?
                && deadline_ms > chrono::Utc::now().timestamp_millis()
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(request_log.requests().len(), 1);
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    assert!(
        first_deadline_ms - chrono::Utc::now().timestamp_millis() <= 60_000,
        "first failure retry should use the one-minute backoff tier"
    );
    let persisted_goal = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await?
        .expect("active goal should remain persisted");
    assert_eq!(
        persisted_goal.status,
        codex_state::ThreadGoalStatus::Active,
        "supervisor failure must not pause, block, or complete the goal"
    );
    let warning = timeout(Duration::from_secs(5), async {
        loop {
            let event = parent_thread
                .next_event()
                .await
                .expect("parent event channel should stay open");
            if let EventMsg::Warning(warning) = event.msg
                && warning.message.contains("Goal supervisor check-in failed")
            {
                break warning.message;
            }
        }
    })
    .await
    .expect("failed supervisor should warn the user");
    assert!(warning.contains("saved model unavailable"));
    assert!(warning.contains("Retrying in"));

    let scheduled_generation =
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await
        .expect("failure should schedule one retry wakeup");
    for _ in 0..3 {
        crate::goal_supervisor::maybe_start_supervisor_checkin(
            &parent_thread.session,
            goal_id.as_str(),
            &goal,
        )
        .await?;
    }
    assert_eq!(
        request_log.requests().len(),
        1,
        "idle signals before the deadline must not replace the failed helper"
    );
    assert_eq!(
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await,
        Some(scheduled_generation),
        "idle signals for the same deadline must reuse the existing sleeping timer"
    );

    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            goal_id.as_str(),
            /*snoozed_until_ms*/ None,
        )
        .await?;
    crate::goal_supervisor::fire_scheduled_supervisor_wakeup_for_test(&parent_thread.session).await;
    for _ in 0..3 {
        crate::goal_supervisor::maybe_start_supervisor_checkin(
            &parent_thread.session,
            goal_id.as_str(),
            &goal,
        )
        .await?;
    }
    timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second failed supervisor should finish");
    let requests_after_retry = request_log.requests();
    assert_eq!(
        requests_after_retry.len(),
        2,
        "one failure retry should run after the in-memory deadline; loaded threads: {:?}; captured ops: {:?}",
        harness.manager.list_thread_ids().await,
        harness.manager.captured_ops(),
    );
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        request_log.requests().len(),
        2,
        "duplicate idle signals must still produce exactly one retry"
    );
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        2,
        "the second implicit failure should advance the backoff tier"
    );
    Ok(())
}

#[test]
fn goal_resume_clears_supervisor_failure_backoff() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_resume_clears_supervisor_failure_backoff",
        goal_resume_clears_supervisor_failure_backoff_inner(),
    )
}

async fn goal_resume_clears_supervisor_failure_backoff_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse_failed(
                "supervisor-failure-1",
                "model_not_found",
                "saved model unavailable",
            ),
            sse_failed(
                "supervisor-failure-2",
                "model_not_found",
                "saved model unavailable",
            ),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Retry immediately when the user resumes this goal.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(deadline_ms) = state_db
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                .await?
                && deadline_ms > chrono::Utc::now().timestamp_millis()
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    let delivered_parent_message = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        "continue".to_string(),
        /*trigger_turn*/ true,
    );
    crate::goal_supervisor::record_followup_action(
        &parent_thread.session,
        &delivered_parent_message,
    )
    .await;
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        0,
        "a valid supervisor followup action should reset failure backoff"
    );

    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await?;
    let second_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && let Some(deadline_ms) = state_db
                    .thread_goals()
                    .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                    .await?
                && deadline_ms != first_deadline_ms
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let second_delay_ms = second_deadline_ms - chrono::Utc::now().timestamp_millis();
    assert!(
        (0..=60_000).contains(&second_delay_ms),
        "manual resume should reset the failure count to the one-minute tier, got {second_delay_ms}ms"
    );
    assert_eq!(request_log.requests().len(), 2);
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    Ok(())
}

#[test]
fn goal_resume_replaces_a_terminal_owned_supervisor_without_backoff() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_resume_replaces_a_terminal_owned_supervisor_without_backoff",
        goal_resume_replaces_a_terminal_owned_supervisor_without_backoff_inner(),
    )
}

async fn goal_resume_replaces_a_terminal_owned_supervisor_without_backoff_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("supervisor-running"),
                ev_completed("supervisor-running"),
            ]))
            .set_delay(Duration::from_millis(250)),
            sse_response(sse_failed(
                "supervisor-replacement-failure",
                "model_not_found",
                "saved model unavailable",
            )),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Retry immediately when resume races a terminal supervisor.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first supervisor request should start");

    assert!(
        harness
            .manager
            .remove_thread(&helper_thread_id)
            .await
            .is_some(),
        "remove the first helper before its completion watcher retires it"
    );
    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await
        .expect("resume should replace the disappeared active helper");
    timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resume should start one immediate replacement");

    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1,
        "the terminal helper retired by resume must not count as a failed retry"
    );
    Ok(())
}

#[test]
#[serial(fork_env)]
fn goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot",
        goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot_inner(),
    )
}

async fn goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-parent"),
                ev_completed("resp-parent"),
            ]),
            sse(vec![
                ev_response_created("resp-child"),
                ev_completed("resp-child"),
            ]),
        ],
    )
    .await;
    let (_home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::AgentPromptInjection);
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let mcp_server_path = config.codex_home.join("fake_mcp_server.py");
    std::fs::write(
        &mcp_server_path,
        r#"import json
import sys

def read_message():
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    return json.loads(line)

def write_message(message):
    body = json.dumps(message).encode("utf-8")
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    request_id = message.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "fake-mcp", "version": "1.0.0"},
            },
        })
    elif method == "tools/list":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [{
                    "name": "echo",
                    "description": "Echo from fake MCP",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                }],
            },
        })
    else:
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "method not found"},
        })
"#,
    )?;
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "rmcp".to_string(),
            McpServerConfig {
                auth: Default::default(),
                transport: McpServerTransportConfig::Stdio {
                    command: "python3".to_string(),
                    args: vec![mcp_server_path.to_string_lossy().to_string()],
                    env: None,
                    env_vars: Vec::new(),
                    cwd: None,
                },
                environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
                enabled: true,
                required: false,
                supports_parallel_tool_calls: false,
                omit_tools_from: None,
                disabled_reason: None,
                oauth: None,
                startup_timeout_sec: Some(Duration::from_secs(5)),
                tool_timeout_sec: None,
                default_tools_approval_mode: None,
                enabled_tools: None,
                disabled_tools: None,
                scopes: None,
                oauth_resource: None,
                tools: std::collections::HashMap::new(),
            },
        )]))
        .expect("test config should allow MCP servers");

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let parent = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await?;
    let parent_thread_id = parent.thread_id;
    let parent_prompt_cache_key = parent.thread.session.prompt_cache_key();
    let mcp_runtime = Arc::clone(&parent.thread.session.services.mcp_runtime);
    assert!(
        mcp_runtime
            .latest_wait_for_server_ready("rmcp", Duration::from_secs(5))
            .await,
        "parent MCP server should become ready before forking"
    );
    let parent_mcp_tools = mcp_runtime.latest_list_all_tools().await;
    assert!(
        parent_mcp_tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "parent MCP manager should expose live MCP tools before forking: tools={parent_mcp_tools:#?}"
    );
    parent
        .thread
        .submit(text_input("parent seed").into())
        .await?;
    wait_for_turn_complete(parent.thread.as_ref()).await;
    parent.thread.session.ensure_rollout_materialized().await;
    parent.thread.session.flush_rollout().await?;
    let before_thread_ids = manager.list_thread_ids().await;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        &state_db,
        parent_thread_id,
        &parent.thread.session,
        "Supervise the parent with inherited MCP tools.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent.thread.session, &goal_id, &goal)
        .await?;
    let child_thread_id = spawned_thread_id_after(&manager, &before_thread_ids).await;
    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let child_mcp_tool_snapshot = child_thread
        .session
        .services
        .mcp_tool_snapshot
        .lock()
        .await
        .clone()
        .expect("goal supervisor helper should inherit the parent MCP tool snapshot");
    assert!(
        child_mcp_tool_snapshot
            .tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "goal supervisor helper should inherit the parent MCP tool snapshot"
    );

    timeout(Duration::from_secs(5), async {
        loop {
            let event = child_thread
                .next_event()
                .await
                .expect("child event channel should stay open");
            if matches!(event.msg, EventMsg::TurnComplete(_)) {
                break;
            }
        }
    })
    .await
    .expect("child turn should complete");
    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let parent_body = requests[0].body_json();
    let child_body = requests[1].body_json();
    let parent_input = parent_body["input"]
        .as_array()
        .expect("parent input should be an array");
    let child_input = child_body["input"]
        .as_array()
        .expect("child input should be an array");
    let expected_prompt_cache_key = parent_prompt_cache_key.to_string();
    assert_eq!(
        child_body["prompt_cache_key"].as_str(),
        Some(expected_prompt_cache_key.as_str())
    );
    assert_eq!(
        &child_input[..parent_input.len()],
        parent_input,
        "goal supervisor helpers must preserve the exact parent request prefix through the fork point so the fork can reuse the parent prompt cache"
    );
    let child_suffix = &child_input[parent_input.len()..];
    assert!(
        child_suffix.first().is_some_and(|item| {
            item["role"] == "developer"
                && item["content"].as_array().is_some_and(|content| {
                    content.iter().any(|content_item| {
                        content_item["text"]
                            .as_str()
                            .is_some_and(|text| text.contains("You are also a **goal supervisor**"))
                    })
                })
        }),
        "goal supervisor helpers should append the supervisor role prompt immediately after the inherited parent request prefix: suffix={child_suffix:#?}"
    );
    for unexpected_child_context in [
        "# AGENTS.md instructions",
        "<permissions instructions>",
        "<apps_instructions>",
        "<skills_instructions>",
        "<plugins_instructions>",
    ] {
        assert!(
            child_suffix.iter().all(|item| {
                item["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .all(|content_item| {
                        content_item["text"]
                            .as_str()
                            .is_none_or(|text| !text.contains(unexpected_child_context))
                    })
            }),
            "goal supervisor helpers must not append fresh child startup context after forking: found {unexpected_child_context} in suffix={child_suffix:#?}"
        );
    }
    assert_eq!(
        child_body["parallel_tool_calls"], parent_body["parallel_tool_calls"],
        "goal supervisor helpers must keep the same parallel tool-call setting as their parent"
    );
    assert_eq!(
        child_body["tools"], parent_body["tools"],
        "goal supervisor helpers must keep the same serialized tool definitions, order, namespaces, and schemas as their parent"
    );
    let parent_tool_signatures = request_tool_signatures(&parent_body);
    let child_tool_signatures = request_tool_signatures(&child_body);
    assert_eq!(
        child_tool_signatures, parent_tool_signatures,
        "goal supervisor helpers are internal full-history forks and must keep the same eager tool surface as their parent so request prefixes stay cacheable"
    );
    for expected_tool in [
        "collaboration.spawn_agent",
        "collaboration.send_message",
        "collaboration.followup_task",
        "collaboration.wait_agent",
        "collaboration.list_agents",
        "collaboration.interrupt_agent",
        "supervisor.close_self",
        "supervisor.snooze",
        "supervisor.compact_parent_context",
    ] {
        assert!(
            child_tool_signatures.contains(expected_tool),
            "expected forked child request to expose `{expected_tool}`; tools={child_tool_signatures:#?}"
        );
    }
    assert!(
        child_body["tools"].as_array().is_some_and(|tools| {
            tools
                .iter()
                .any(|tool| tool["type"].as_str() == Some("tool_search"))
        }),
        "the inherited MCP snapshot should be discoverable through upstream tool_search: {child_body:#}"
    );

    Ok(())
}

#[test]
fn goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume",
        goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume_inner(),
    )
}

async fn goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume_inner()
-> anyhow::Result<()> {
    const ALIAS: &str = "frontier-local";
    const OLD_BACKING: &str = "gpt-5.2-preview";
    const NEW_BACKING: &str = "gpt-5.4-preview";
    const OLD_GUIDANCE: &str = "Preserve the original alias-backed context.";
    const NEW_GUIDANCE: &str = "Preserve the updated alias-backed context.";

    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-parent"),
                ev_completed("resp-parent"),
            ]),
            sse(vec![
                ev_response_created("resp-supervisor"),
                ev_completed("resp-supervisor"),
            ]),
            sse(vec![
                ev_response_created("resp-resumed"),
                ev_completed("resp-resumed"),
            ]),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let mut model_catalog = bundled_models_response()?;
    for (slug, guidance) in [("gpt-5.2", OLD_GUIDANCE), ("gpt-5.4", NEW_GUIDANCE)] {
        model_catalog
            .models
            .iter_mut()
            .find(|model| model.slug == slug)
            .and_then(|model| model.model_messages.as_mut())
            .expect("backing model should expose model messages")
            .token_budget = Some(ModelTokenBudgetConfig {
            reminder_threshold_tokens: 6_144,
            reminder_message_template: "Alias reminder: {n_remaining} tokens remain.".to_string(),
            guidance_message: guidance.to_string(),
            auto_compact_fallback_prompt: "Save state before rollover.".to_string(),
            auto_compact_fallback_buffer_tokens: 16_384,
        });
    }
    let _ = config.features.enable(Feature::AgentPromptInjection);
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let _ = config.features.enable(Feature::TokenBudget);
    config.model = Some(ALIAS.to_string());
    config.model_catalog = Some(model_catalog);
    config.custom_models.insert(
        ALIAS.to_string(),
        CustomModelConfig {
            model: OLD_BACKING.to_string(),
            model_context_window: Some(128_000),
            model_auto_compact_token_limit: Some(100_000),
        },
    );
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let auth_manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
    let manager = ThreadManager::new(
        &config,
        Arc::clone(&auth_manager),
        crate::thread_manager::build_models_manager(&config, auth_manager),
        crate::CodexAppsToolsCache::default(),
        SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        empty_extension_registry(),
        Arc::new(crate::test_support::EmptyUserInstructionsProvider),
        /*analytics_events_client*/ None,
        crate::thread_manager::thread_store_from_config(&config, Some(state_db.clone())),
        crate::thread_manager::local_agent_graph_store_from_state_db(Some(&state_db)),
        uuid::Uuid::new_v4().to_string(),
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    );
    let control = manager.agent_control();
    let harness = AgentControlHarness {
        _home: home,
        config: config.clone(),
        state_db: Some(state_db),
        manager,
        control,
    };
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let parent_prompt_cache_key = parent_thread.session.prompt_cache_key();
    parent_thread
        .submit(text_input("seed alias-backed guidance").into())
        .await?;
    wait_for_turn_complete(parent_thread.as_ref()).await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (_goal_id, _goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Keep the alias-backed parent progressing.",
    )
    .await?;
    let child_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(
            AgentPath::root()
                .join(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
                .expect("goal supervisor path should be valid"),
        ),
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            config.clone(),
            text_input("inspect the parent goal"),
            Some(child_source.clone()),
            SpawnAgentOptions {
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await?
        .thread_id;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("goal supervisor helper should be registered");
    let child_rollout_path = child_thread
        .rollout_path()
        .expect("segmented goal supervisor rollout should exist");
    wait_for_turn_complete(child_thread.as_ref()).await;
    child_thread.session.flush_rollout().await?;

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let parent_input = requests[0].input();
    let child_input = requests[1].input();
    assert_eq!(requests[0].body_json()["model"], OLD_BACKING);
    assert_eq!(requests[1].body_json()["model"], OLD_BACKING);
    assert_eq!(
        &child_input[..parent_input.len()],
        parent_input,
        "goal supervisor full-history forks should preserve the exact parent request prefix"
    );
    assert_eq!(
        requests[1].body_json()["prompt_cache_key"],
        requests[0].body_json()["prompt_cache_key"]
    );
    assert_eq!(
        child_thread.session.prompt_cache_key(),
        parent_prompt_cache_key
    );
    assert_eq!(
        requests[0]
            .message_input_texts("developer")
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "custom aliases should inherit their backing model's token-budget guidance"
    );
    assert_eq!(
        requests[1]
            .message_input_texts("developer")
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "the supervisor fork should not duplicate inherited guidance"
    );

    let _ = harness.control.shutdown_live_agent(child_thread_id).await?;
    let mut resumed_config = config;
    resumed_config.custom_models.insert(
        ALIAS.to_string(),
        CustomModelConfig {
            model: NEW_BACKING.to_string(),
            model_context_window: Some(128_000),
            model_auto_compact_token_limit: Some(100_000),
        },
    );
    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(resumed_config, child_thread_id, child_source)
        .await?;
    assert_eq!(resumed_thread_id, child_thread_id);
    let resumed_thread = harness
        .manager
        .get_thread(resumed_thread_id)
        .await
        .expect("cold-resumed supervisor should be registered");
    resumed_thread
        .submit(text_input("inspect resumed alias guidance").into())
        .await?;
    wait_for_turn_complete(resumed_thread.as_ref()).await;
    resumed_thread.session.flush_rollout().await?;

    let requests = request_log.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].body_json()["model"], NEW_BACKING);
    let resumed_developer_texts = requests[2].message_input_texts("developer");
    assert_eq!(
        resumed_developer_texts
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "cold resume should retain the original guidance once"
    );
    assert_eq!(
        resumed_developer_texts
            .iter()
            .filter(|text| text.contains(NEW_GUIDANCE))
            .count(),
        1,
        "cold resume should append the updated backing model guidance once"
    );

    let physical_items = RolloutRecorder::load_rollout_items(child_rollout_path.as_path())
        .await?
        .0;
    let reference_and_world_state = physical_items
        .iter()
        .filter(|item| {
            matches!(
                item,
                RolloutItem::RolloutReference(_) | RolloutItem::WorldState(_)
            )
        })
        .collect::<Vec<_>>();
    let [
        RolloutItem::RolloutReference(_),
        RolloutItem::WorldState(usage_hint_tombstone),
        RolloutItem::WorldState(context_guidance),
    ] = reference_and_world_state.as_slice()
    else {
        panic!(
            "expected rollout reference, usage-hint tombstone, and updated context guidance: {reference_and_world_state:#?}"
        );
    };
    assert!(!usage_hint_tombstone.full);
    assert_eq!(
        usage_hint_tombstone.state,
        serde_json::json!({
            "multi_agent_mode": {"usage_hint_hash": null},
            "multi_agent_usage_hint": null,
        })
    );
    assert!(!context_guidance.full);
    assert_eq!(
        context_guidance.state,
        serde_json::json!({"context_window_guidance": NEW_GUIDANCE})
    );

    let _ = parent_thread.submit(Op::Shutdown {}).await?;
    Ok(())
}

#[test]
fn goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id()
-> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id",
        goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id_inner(),
    )
}

async fn goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id_inner()
-> anyhow::Result<()> {
    let server = start_websocket_server(vec![
        vec![
            vec![
                ev_response_created("warm-parent"),
                ev_completed("warm-parent"),
            ],
            vec![
                ev_response_created("resp-parent"),
                ev_assistant_message("msg-parent", "parent done"),
                ev_completed("resp-parent"),
            ],
        ],
        vec![
            vec![
                ev_response_created("warm-supervisor"),
                ev_completed("warm-supervisor"),
            ],
            vec![
                ev_response_created("resp-supervisor"),
                ev_completed("resp-supervisor"),
            ],
        ],
    ])
    .await;
    let (_home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = true;

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let parent = manager
        .start_thread(StartThreadOptions::new(config))
        .await?;
    let parent_thread_id = parent.thread_id;
    let parent_prompt_cache_key = parent.thread.session.prompt_cache_key();
    parent
        .thread
        .submit(text_input("parent seed").into())
        .await?;
    wait_for_turn_complete(parent.thread.as_ref()).await;
    parent.thread.session.ensure_rollout_materialized().await;
    parent.thread.session.flush_rollout().await?;
    let before_thread_ids = manager.list_thread_ids().await;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        &state_db,
        parent_thread_id,
        &parent.thread.session,
        "Supervise the parent with prompt cache inheritance.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent.thread.session, &goal_id, &goal)
        .await?;
    let child_thread_id = spawned_thread_id_after(&manager, &before_thread_ids).await;
    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    wait_for_turn_complete(child_thread.as_ref()).await;

    let connections = server.connections();
    let supervisor_connection = connections
        .get(1)
        .expect("supervisor helper should use its own websocket connection");
    let supervisor_generated_request = supervisor_connection
        .iter()
        .map(core_test_support::responses::WebSocketRequest::body_json)
        .find(|body| body["generate"].as_bool() != Some(false))
        .unwrap_or_else(|| {
            panic!(
                "goal supervisor helper should send a generated websocket request after warmup; supervisor requests={supervisor_connection:#?}"
            )
        });
    assert_ne!(
        supervisor_generated_request["previous_response_id"].as_str(),
        Some("resp-parent"),
        "goal supervisor helpers must not reuse a parent websocket previous_response_id on their own websocket connection"
    );
    assert_eq!(
        supervisor_generated_request["prompt_cache_key"].as_str(),
        Some(parent_prompt_cache_key.to_string().as_str()),
        "goal supervisor helpers must keep the parent prompt cache key across websocket connections"
    );
    assert!(
        request_tool_signatures(&supervisor_generated_request).contains("supervisor.close_self"),
        "goal supervisor helper websocket requests must retain the supervisor tool namespace"
    );

    server.shutdown().await;
    Ok(())
}

#[test]
fn goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt() {
    run_goal_supervisor_test(
        "goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt",
        goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt_inner(),
    );
}

async fn goal_supervisor_helper_uses_full_history_fork_without_duplicate_prompt_inner() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::AgentPromptInjection);
    let _ = parent_config.features.enable(Feature::Goals);
    let _ = parent_config.features.enable(Feature::GoalSupervisor);
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    parent_thread
        .inject_user_message_without_turn("parent seed context".to_string())
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let before_thread_ids = harness.manager.list_thread_ids().await;
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Ship the active user goal.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-test",
        &goal,
    )
    .await
    .expect("goal supervisor helper should spawn");

    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let helper_thread = harness
        .manager
        .get_thread(helper_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    assert_eq!(
        helper_thread.session.prompt_cache_key(),
        parent_thread.session.prompt_cache_key(),
        "goal supervisor helpers are internal full-history forks and must keep the parent prompt cache key"
    );

    let helper_history = helper_thread.session.clone_history().await;
    assert!(
        helper_history.raw_items().iter().any(|item| matches!(
            item,
            ResponseItem::Message { role, content, .. }
                if role == "user"
                    && content.iter().any(|content_item| matches!(
                        content_item,
                        ContentItem::InputText { text } if text == "parent seed context"
                    ))
        )),
        "goal supervisor helpers must inherit the parent conversation prefix before their supervisor assignment"
    );
    let supervisor_prompt =
        crate::session::load_supervisor_agent_prompt(&harness.config.codex_home).await;
    let supervisor_prompt_count = helper_history
        .raw_items()
        .iter()
        .filter(|item| {
            matches!(
                item,
                ResponseItem::Message { role, content, .. }
                    if role == "developer"
                        && content.iter().any(|content_item| matches!(
                            content_item,
                            ContentItem::InputText { text } if text == &supervisor_prompt
                        ))
            )
        })
        .count();
    assert_eq!(
        supervisor_prompt_count, 1,
        "the supervisor prompt should be injected once as the helper role prompt; duplicating it in the user assignment changes the post-fork context"
    );
    assert!(helper_history.raw_items().iter().any(|item| matches!(
        item,
        ResponseItem::FunctionCall { name, call_id, .. }
            if name == "list_agents" && call_id == "synthetic_supervisor_list_agents"
    )));

    let captured_input = harness
        .manager
        .captured_ops()
        .into_iter()
        .find_map(|(thread_id, op)| {
            if thread_id != helper_thread_id {
                return None;
            }
            match op {
                Op::UserInput { items, .. } => items.into_iter().find_map(|item| match item {
                    UserInput::Text { text, .. } => Some(text),
                    UserInput::Image { .. }
                    | UserInput::LocalImage { .. }
                    | UserInput::Skill { .. }
                    | UserInput::Mention { .. } => None,
                    _ => None,
                }),
                _ => None,
            }
        })
        .expect("supervisor helper assignment should be submitted as user input");
    assert!(captured_input.contains("# Goal Supervisor Assignment"));
    assert!(captured_input.contains("Ship the active user goal."));
    assert!(
        !captured_input.contains("You are also a **goal supervisor**"),
        "the supervisor role prompt must not be copied into the helper assignment"
    );
}

#[test]
fn finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge() {
    run_goal_supervisor_test(
        "finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge",
        finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge_inner(),
    );
}

async fn finished_ephemeral_goal_supervisor_closes_persisted_spawn_edge_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let mut helper_config = harness.config.clone();
    helper_config.ephemeral = true;
    let helper_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            helper_config,
            text_input("supervise"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(supervisor_path),
                agent_nickname: None,
                agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("supervisor helper should spawn")
        .thread_id;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            helper_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("materialized helper edge should persist");

    harness
        .control
        .finish_internal_helper_thread(helper_thread_id)
        .await
        .expect("supervisor helper should finish");

    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    let closed_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("closed child query should succeed");
    assert!(!open_children.contains(&helper_thread_id));
    assert!(closed_children.contains(&helper_thread_id));
}

#[test]
fn finished_goal_supervisor_releases_shared_mcp_lease() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "finished_goal_supervisor_releases_shared_mcp_lease",
        finished_goal_supervisor_releases_shared_mcp_lease_inner(),
    )
}

async fn finished_goal_supervisor_releases_shared_mcp_lease_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    Mock::given(method("POST"))
        .and(path_regex(".*/responses$"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(vec![
                    ev_response_created("helper-response"),
                    ev_completed("helper-response"),
                ]))
                .set_delay(Duration::from_millis(250)),
        )
        .mount(&server)
        .await;
    let (home, mut config) = test_config().await;
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let pid_file = home.path().join("shared-mcp.pid");
    let mcp_server_path = home.path().join("shared_mcp_server.py");
    std::fs::write(
        &mcp_server_path,
        r#"import json
import os
import pathlib
import sys

pathlib.Path(os.environ["MCP_TEST_PID_FILE"]).write_text(str(os.getpid()))

for line in sys.stdin:
    message = json.loads(line)
    request_id = message.get("id")
    if request_id is None:
        continue
    method = message.get("method")
    if method == "initialize":
        result = {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "shared-mcp-test", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {
            "tools": [{
                "name": "shared_counter",
                "description": "Return the shared server process ID",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            }],
        }
    elif method == "tools/call":
        result = {
            "content": [{"type": "text", "text": "shared"}],
            "structuredContent": {"pid": os.getpid()},
            "isError": False,
        }
    else:
        result = None
    response = {"jsonrpc": "2.0", "id": request_id}
    if result is None:
        response["error"] = {"code": -32601, "message": "method not found"}
    else:
        response["result"] = result
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()
"#,
    )?;
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "retirement".to_string(),
            McpServerConfig {
                auth: Default::default(),
                transport: McpServerTransportConfig::Stdio {
                    command: "python3".to_string(),
                    args: vec![mcp_server_path.to_string_lossy().into_owned()],
                    env: Some(std::collections::HashMap::from([(
                        "MCP_TEST_PID_FILE".to_string(),
                        pid_file.to_string_lossy().into_owned(),
                    )])),
                    env_vars: Vec::new(),
                    cwd: None,
                },
                environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
                enabled: true,
                required: true,
                supports_parallel_tool_calls: false,
                omit_tools_from: None,
                disabled_reason: None,
                oauth: None,
                startup_timeout_sec: Some(Duration::from_secs(5)),
                tool_timeout_sec: Some(Duration::from_secs(5)),
                default_tools_approval_mode: None,
                enabled_tools: None,
                disabled_tools: None,
                scopes: None,
                oauth_resource: None,
                tools: std::collections::HashMap::new(),
            },
        )]))
        .expect("test MCP config should be valid");
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
    let sibling_thread = harness
        .manager
        .get_thread(sibling_thread_id)
        .await
        .expect("sibling should be loaded");
    wait_for_turn_complete(sibling_thread.as_ref()).await;

    let parent_pid = shared_mcp_process_id(parent_thread.as_ref()).await;
    let sibling_pid = shared_mcp_process_id(sibling_thread.as_ref()).await;
    let pid = wait_for_pid_file(&pid_file).await?;
    let pid = pid.parse::<u64>()?;
    assert_eq!(parent_pid, pid);
    assert_eq!(sibling_pid, pid);
    let iterations = std::env::var("CODEX_TEST_GOAL_SUPERVISOR_RETIREMENT_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    #[cfg(target_os = "linux")]
    let baseline_rollout_descriptors =
        open_rollout_writer_count(harness.config.codex_home.as_path());
    #[cfg(target_os = "linux")]
    let mut max_rollout_descriptors = baseline_rollout_descriptors;

    for iteration in 0..iterations {
        let supervisor_path = AgentPath::root()
            .join("goal_supervisor")
            .expect("supervisor path");
        let mut helper_config = harness.config.clone();
        helper_config.ephemeral = true;
        let helper_thread_id = agent_control
            .spawn_agent_with_metadata(
                helper_config,
                text_input("supervise"),
                Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id,
                    depth: 1,
                    agent_path: Some(supervisor_path),
                    agent_nickname: None,
                    agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
                })),
                SpawnAgentOptions::default(),
            )
            .await?
            .thread_id;
        let retained_helper = harness.manager.get_thread(helper_thread_id).await?;
        timeout(Duration::from_secs(5), async {
            while !matches!(retained_helper.agent_status().await, AgentStatus::Running) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("supervisor turn should start");
        retained_helper
            .session
            .try_ensure_rollout_materialized()
            .await
            .expect("supervisor rollout should materialize for descriptor retirement");
        #[cfg(target_os = "linux")]
        let helper_rollout_path = retained_helper
            .session
            .current_rollout_path()
            .await?
            .expect("materialized supervisor should have a live rollout path");
        #[cfg(target_os = "linux")]
        {
            let active_paths = open_rollout_writer_paths(harness.config.codex_home.as_path());
            assert!(
                active_paths.contains(&helper_rollout_path),
                "the active supervisor must hold its materialized rollout descriptor: helper={}, open={active_paths:?}",
                helper_rollout_path.display()
            );
            let active = active_paths.len();
            max_rollout_descriptors = max_rollout_descriptors.max(active);
        }
        assert_eq!(shared_mcp_process_id(retained_helper.as_ref()).await, pid);
        assert_eq!(retained_helper.agent_status().await, AgentStatus::Running);

        agent_control
            .finish_internal_helper_thread(helper_thread_id)
            .await?;
        assert_thread_not_loaded(&harness.manager, helper_thread_id).await;
        timeout(
            Duration::from_secs(5),
            retained_helper.wait_until_terminated(),
        )
        .await
        .unwrap_or_else(|_| panic!("supervisor session {iteration} should terminate"));
        timeout(Duration::from_secs(2), async {
            loop {
                let event = retained_helper
                    .next_event()
                    .await
                    .expect("retained helper event channel should remain readable");
                if matches!(event.msg, EventMsg::TurnComplete(_)) {
                    break;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("supervisor turn {iteration} should complete before shutdown"));

        #[cfg(target_os = "linux")]
        {
            let open_paths = timeout(Duration::from_secs(2), async {
                loop {
                    let open_paths = open_rollout_writer_paths(harness.config.codex_home.as_path());
                    if !open_paths.contains(&helper_rollout_path) {
                        break open_paths;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "retired supervisor must release its rollout descriptor: helper={}",
                    helper_rollout_path.display()
                )
            });
            let current = open_paths.len();
            max_rollout_descriptors = max_rollout_descriptors.max(current);
            assert!(
                current <= baseline_rollout_descriptors,
                "retired supervisors must not accumulate rollout descriptors: baseline={baseline_rollout_descriptors}, iteration={iteration}, current={current}"
            );
        }
    }

    assert_eq!(shared_mcp_process_id(parent_thread.as_ref()).await, pid);
    assert_eq!(shared_mcp_process_id(sibling_thread.as_ref()).await, pid);
    #[cfg(target_os = "linux")]
    {
        assert!(
            max_rollout_descriptors <= baseline_rollout_descriptors.saturating_add(1),
            "retired supervisors must keep rollout descriptors bounded"
        );
        assert!(
            open_rollout_writer_count(harness.config.codex_home.as_path())
                <= baseline_rollout_descriptors,
            "retired supervisors must release their rollout descriptors"
        );
    }

    sibling_thread.shutdown_and_wait().await?;
    assert!(
        process_is_alive(&pid.to_string())?,
        "the parent lease should keep the shared MCP process alive"
    );
    parent_thread.shutdown_and_wait().await?;
    wait_for_process_exit(&pid.to_string()).await
}

#[test]
fn goal_supervisor_spawn_reconciles_stale_persisted_state() {
    run_goal_supervisor_test(
        "goal_supervisor_spawn_reconciles_stale_persisted_state",
        goal_supervisor_spawn_reconciles_stale_persisted_state_inner(),
    );
}

async fn goal_supervisor_spawn_reconciles_stale_persisted_state_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Continue the daily release cycle.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let stale_helper_thread_ids = [ThreadId::new(), ThreadId::new()];
    for stale_helper_thread_id in stale_helper_thread_ids {
        let stale_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth: 1,
            agent_path: Some(supervisor_path.clone()),
            agent_nickname: None,
            agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
        });
        let stale_metadata = codex_state::ThreadMetadataBuilder::new(
            stale_helper_thread_id,
            harness
                .config
                .codex_home
                .join(format!("{stale_helper_thread_id}.jsonl"))
                .to_path_buf(),
            chrono::Utc::now(),
            stale_source,
        )
        .build("openai");
        state_db
            .upsert_thread(&stale_metadata)
            .await
            .expect("stale supervisor metadata should persist");
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                stale_helper_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("stale supervisor edge should persist");
    }
    harness
        .control
        .restore_v2_agent_metadata(&harness.config, parent_thread_id)
        .await;
    assert!(
        harness
            .control
            .state
            .agent_id_for_path(&supervisor_path)
            .is_some_and(|thread_id| stale_helper_thread_ids.contains(&thread_id)),
        "cold restore should reproduce the stale canonical path collision"
    );
    for stale_helper_thread_id in stale_helper_thread_ids {
        assert_eq!(
            harness.control.get_status(stale_helper_thread_id).await,
            AgentStatus::NotFound,
            "restored supervisor metadata must not be mistaken for a live helper"
        );
    }

    let replacement_thread_id =
        crate::goal_supervisor::spawn_supervisor_helper_for_test(&parent_thread.session, &goal)
            .await
            .expect("new supervisor spawn should reconcile stale persisted state");

    assert!(!stale_helper_thread_ids.contains(&replacement_thread_id));
    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    let closed_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("closed child query should succeed");
    for stale_helper_thread_id in stale_helper_thread_ids {
        assert!(!open_children.contains(&stale_helper_thread_id));
        assert!(closed_children.contains(&stale_helper_thread_id));
    }
}

#[test]
fn goal_supervisor_reconciliation_preserves_running_supervisor_and_worker() {
    run_goal_supervisor_test(
        "goal_supervisor_reconciliation_preserves_running_supervisor_and_worker",
        goal_supervisor_reconciliation_preserves_running_supervisor_and_worker_inner(),
    );
}

async fn goal_supervisor_reconciliation_preserves_running_supervisor_and_worker_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let mut helper_config = harness.config.clone();
    helper_config.ephemeral = true;
    let helper_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            helper_config,
            text_input("supervise"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(supervisor_path.clone()),
                agent_nickname: None,
                agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("supervisor helper should spawn")
        .thread_id;
    let worker_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("work"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(AgentPath::root().join("worker").expect("worker path")),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("worker should spawn")
        .thread_id;
    for child_thread_id in [helper_thread_id, worker_thread_id] {
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                child_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("child edge should persist");
    }
    let foreign_supervisor_thread_id = ThreadId::new();
    let foreign_supervisor_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: ThreadId::new(),
        depth: 1,
        agent_path: Some(
            AgentPath::root()
                .join("foreign_goal_supervisor")
                .expect("foreign supervisor path"),
        ),
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let foreign_supervisor_metadata = codex_state::ThreadMetadataBuilder::new(
        foreign_supervisor_thread_id,
        harness
            .config
            .codex_home
            .join(format!("{foreign_supervisor_thread_id}.jsonl"))
            .to_path_buf(),
        chrono::Utc::now(),
        foreign_supervisor_source,
    )
    .build("openai");
    state_db
        .upsert_thread(&foreign_supervisor_metadata)
        .await
        .expect("foreign supervisor metadata should persist");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            foreign_supervisor_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("inaccurate foreign supervisor edge should persist");

    let first_result = harness
        .control
        .reconcile_goal_supervisor_state(parent_thread_id, &supervisor_path)
        .await
        .expect("first reconciliation should succeed");
    let second_result = harness
        .control
        .reconcile_goal_supervisor_state(parent_thread_id, &supervisor_path)
        .await
        .expect("second reconciliation should be idempotent");

    assert_eq!(first_result, Some(helper_thread_id));
    assert_eq!(second_result, Some(helper_thread_id));
    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    assert!(open_children.contains(&helper_thread_id));
    assert!(open_children.contains(&worker_thread_id));
    assert!(
        open_children.contains(&foreign_supervisor_thread_id),
        "reconciliation must not trust an inaccurate edge over the stored supervisor parent"
    );
}

#[test]
fn goal_supervisor_execution_settings_change_restarts_running_helper() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_execution_settings_change_restarts_running_helper",
        goal_supervisor_execution_settings_change_restarts_running_helper_inner(),
    )
}

async fn goal_supervisor_execution_settings_change_restarts_running_helper_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let delayed_response = Duration::from_secs(30);
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("resp-before-settings-change"),
                ev_completed("resp-before-settings-change"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("resp-after-settings-change"),
                ev_completed("resp-after-settings-change"),
            ]))
            .set_delay(delayed_response),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread.flush_rollout().await?;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Restart the supervisor when execution settings change.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent_thread.session, &goal_id, &goal)
        .await?;
    let first_helper_thread_id =
        spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first supervisor request should start");

    let original_model = parent_thread.session.thread_config_snapshot().await.model;
    let next_model = if original_model == "gpt-5.4" {
        "gpt-5.2"
    } else {
        "gpt-5.4"
    };
    let settings_changed_at = Instant::now();
    parent_thread
        .submit(Op::ThreadSettings {
            thread_settings: ThreadSettingsOverrides {
                model: Some(next_model.to_string()),
                effort: Some(Some(ReasoningEffort::High)),
                service_tier: Some(Some(ServiceTier::Fast.request_value().to_string())),
                ..Default::default()
            },
        })
        .await?;
    timeout(Duration::from_secs(5), async {
        while !harness
            .manager
            .captured_ops()
            .into_iter()
            .any(|(thread_id, op)| {
                thread_id == first_helper_thread_id && matches!(op, Op::Shutdown)
            })
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("settings update should stop the running supervisor helper");

    let replacement_started = timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        replacement_started.is_ok(),
        "replacement supervisor request should start before the old response finishes: threads={:?}, ops={:?}",
        harness.manager.list_thread_ids().await,
        harness.manager.captured_ops(),
    );
    assert!(settings_changed_at.elapsed() < delayed_response);

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let replacement_body = requests[1].body_json();
    assert_eq!(replacement_body["model"].as_str(), Some(next_model));
    assert_eq!(
        replacement_body["reasoning"]["effort"].as_str(),
        Some("high")
    );
    assert_eq!(
        replacement_body["service_tier"].as_str(),
        Some(ServiceTier::Fast.request_value())
    );

    let _ = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[test]
fn goal_supervisor_waits_for_parent_turn_to_finish() {
    run_goal_supervisor_test(
        "goal_supervisor_waits_for_parent_turn_to_finish",
        goal_supervisor_waits_for_parent_turn_to_finish_inner(),
    );
}

async fn goal_supervisor_waits_for_parent_turn_to_finish_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Wait for the parent turn, then continue.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let parent_only = harness.manager.list_thread_ids().await;
    *parent_thread.session.active_turn.lock().await = Some(ActiveTurn::default());

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-busy-parent-test",
        &goal,
    )
    .await
    .expect("busy parent should defer the supervisor");

    assert_eq!(harness.manager.list_thread_ids().await, parent_only);

    *parent_thread.session.active_turn.lock().await = None;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-busy-parent-test",
        &goal,
    )
    .await
    .expect("idle parent should start the deferred supervisor");

    let helper_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    assert!(harness.manager.get_thread(helper_thread_id).await.is_ok());
}

#[test]
fn goal_supervisor_finish_serializes_with_the_next_start() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_finish_serializes_with_the_next_start",
        goal_supervisor_finish_serializes_with_the_next_start_inner(),
    )
}

async fn goal_supervisor_finish_serializes_with_the_next_start_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let delayed_response = Duration::from_secs(30);
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("first-supervisor"),
                ev_completed("first-supervisor"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("replacement-supervisor"),
                ev_completed("replacement-supervisor"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("post-followup-supervisor"),
                ev_completed("post-followup-supervisor"),
            ]))
            .set_delay(delayed_response),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread.flush_rollout().await?;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Serialize supervisor retirement and replacement.",
    )
    .await?;
    let parent_only = harness.manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_helper_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first supervisor request should start");

    let transition =
        crate::goal_supervisor::hold_supervisor_transition_for_test(&parent_thread.session).await;
    let finish_session = Arc::clone(&parent_thread.session);
    let finish_task = tokio::spawn(async move {
        crate::goal_supervisor::finish_supervisor_helper(&finish_session, first_helper_thread_id)
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !finish_task.is_finished(),
        "supervisor retirement must wait for the lifecycle transition lock"
    );
    drop(transition);
    assert!(
        finish_task.await.expect("finish task should not panic")?,
        "the active supervisor should retire"
    );

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let replacement_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    assert_ne!(replacement_thread_id, first_helper_thread_id);
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement supervisor request should start");

    crate::goal_supervisor::record_followup_action(
        &parent_thread.session,
        &InterAgentCommunication::new(
            AgentPath::root()
                .join("goal_supervisor")
                .expect("supervisor path"),
            AgentPath::root(),
            Vec::new(),
            "continue".to_string(),
            /*trigger_turn*/ true,
        ),
    )
    .await;
    assert!(
        crate::goal_supervisor::finish_supervisor_helper_after_followup(
            &parent_thread.session,
            replacement_thread_id,
        )
        .await?,
        "the replacement supervisor should retire after its followup"
    );
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 3 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("idle recheck should replace a supervisor after its delivered followup");

    let _ = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[test]
fn successful_supervisor_parent_compaction_is_recorded() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "successful_supervisor_parent_compaction_is_recorded",
        successful_supervisor_parent_compaction_is_recorded_inner(),
    )
}

async fn successful_supervisor_parent_compaction_is_recorded_inner() -> anyhow::Result<()> {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Record parent compaction in supervisor continuity.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let before_thread_ids = harness.manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "parent-compaction-goal",
        &goal,
    )
    .await?;
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let result = harness
        .control
        .compact_parent_for_goal_supervisor_helper(helper_thread_id)
        .await?;
    assert_matches!(
        result,
        SupervisorParentCompactionResult::Submitted {
            parent_thread_id: submitted_parent_thread_id,
            ..
        } if submitted_parent_thread_id == parent_thread_id
    );
    assert!(
        harness
            .manager
            .captured_ops()
            .iter()
            .any(|(thread_id, op)| { *thread_id == parent_thread_id && matches!(op, Op::Compact) })
    );

    let continuity_text = goal_supervisor_continuity_text_for_test(
        &parent_thread.session,
        "parent-compaction-goal",
        &goal,
    )
    .await;
    assert!(continuity_text.contains("\"kind\": \"compact_parent_context\""));
    Ok(())
}

#[test]
fn goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll()
-> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll",
        goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll_inner(),
    )
}

async fn goal_supervisor_snooze_history_preserves_backoff_after_unchanged_parent_poll_inner()
-> anyhow::Result<()> {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Continue scheduled work without repeating unchanged checks.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let reason = format!(
        "external API\nreturned\tno new results:\r {}",
        "\u{1F980}".repeat(256)
    );
    assert_eq!(
        harness
            .control
            .snooze_goal_supervisor_helper(
                helper_thread_id,
                /*delay_seconds*/ 60,
                Some(reason.as_str()),
            )
            .await,
        Some(60),
    );

    let expected_reason = reason
        .split_whitespace()
        .flat_map(|word| word.chars().chain(std::iter::once(' ')))
        .filter(|character| !character.is_control())
        .take(120)
        .collect::<String>()
        .trim_end()
        .to_string();
    let expected_snooze = format!("Snooze 60s: {expected_reason}");
    let parent_history = parent_thread.session.clone_history().await;
    let snooze_messages = parent_history
        .raw_items()
        .iter()
        .filter_map(|item| match item {
            ResponseItem::Message { role, content, .. } if role == "assistant" => {
                InterAgentCommunication::from_message_content(content)
            }
            _ => None,
        })
        .filter(|communication| communication.author.as_str() == "/root/goal_supervisor")
        .collect::<Vec<_>>();
    assert_eq!(
        snooze_messages.len(),
        1,
        "one supervisor snooze should produce exactly one model-visible parent message"
    );
    let snooze_message = &snooze_messages[0];
    assert_eq!(snooze_message.recipient, AgentPath::root());
    assert_eq!(snooze_message.content, expected_snooze);
    assert!(
        !snooze_message.content.chars().any(char::is_control),
        "a snooze must remain a compact, single-line history message"
    );
    assert!(
        !snooze_message.trigger_turn,
        "recording a snooze must not wake the parent agent"
    );
    assert!(
        parent_thread.session.active_turn.lock().await.is_none(),
        "recording a snooze must leave the parent agent idle"
    );
    assert!(
        harness
            .manager
            .captured_ops()
            .iter()
            .all(|(thread_id, op)| {
                *thread_id != parent_thread_id
                    || !matches!(
                        op,
                        Op::UserInput { .. } | Op::InterAgentCommunication { .. }
                    )
            }),
        "recording a snooze must not submit a parent user turn or queue parent mail"
    );

    parent_thread.flush_rollout().await?;
    let rollout_path = parent_thread
        .rollout_path()
        .expect("the parent rollout should be materialized");
    let codex_protocol::protocol::InitialHistory::Resumed(persisted_history) =
        RolloutRecorder::get_rollout_history(&rollout_path).await?
    else {
        anyhow::bail!("the parent rollout should reconstruct as resumed history");
    };
    assert!(
        persisted_history.history.iter().any(|item| {
            let RolloutItem::ResponseItem(ResponseItem::Message { role, content, .. }) = item
            else {
                return false;
            };
            role == "assistant"
                && InterAgentCommunication::from_message_content(content).is_some_and(
                    |communication| {
                        communication.author.as_str() == "/root/goal_supervisor"
                            && communication.content == expected_snooze
                            && !communication.trigger_turn
                    },
                )
        }),
        "the compact snooze message must survive rollout persistence"
    );
    assert!(
        !persisted_history.history.iter().any(|item| matches!(
            item,
            RolloutItem::EventMsg(EventMsg::Warning(warning))
                if warning.message.starts_with("Supervisor snoozed for ")
        )),
        "a snooze must not also produce a duplicate warning"
    );

    let continuity_text =
        goal_supervisor_continuity_text_for_test(&parent_thread.session, &goal_id, &goal).await;
    let continuity_json = continuity_text
        .strip_prefix("# Goal Supervisor Continuity\n\n")
        .expect("supervisor continuity should contain a JSON developer message");
    let continuity: serde_json::Value = serde_json::from_str(continuity_json)?;
    assert_eq!(continuity["previous_supervisor_action"]["kind"], "snooze");
    assert_eq!(
        continuity["previous_supervisor_action"]["snoozed_seconds"],
        60
    );
    assert_eq!(
        continuity["goal_timing"]["snooze_count_since_goal_created"],
        1
    );
    assert_eq!(
        continuity["goal_timing"]["snoozed_seconds_since_goal_created"],
        60
    );
    assert_eq!(
        continuity["parent_timing"]["snooze_count_since_last_parent_message"],
        1
    );
    assert_eq!(
        continuity["parent_timing"]["snoozed_seconds_since_last_parent_message"],
        60
    );

    let parent_completion = RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: "unchanged-parent-poll".to_string(),
        last_agent_message: Some("The external API returned no new results.".to_string()),
        error: None,
        started_at: None,
        completed_at: Some(chrono::Utc::now().timestamp().saturating_add(1)),
        duration_ms: None,
        time_to_first_token_ms: None,
    }));
    let continuity_item = crate::goal_supervisor::supervisor_continuity_context_item(
        &parent_thread.session,
        &goal_id,
        &goal,
        &[parent_completion],
    )
    .await;
    let RolloutItem::ResponseItem(ResponseItem::Message { content, .. }) = continuity_item else {
        anyhow::bail!("supervisor continuity should be a developer message");
    };
    let updated_continuity_text = content
        .into_iter()
        .find_map(|item| match item {
            ContentItem::InputText { text } => Some(text),
            _ => None,
        })
        .expect("supervisor continuity should contain text");
    let updated_continuity_json = updated_continuity_text
        .strip_prefix("# Goal Supervisor Continuity\n\n")
        .expect("supervisor continuity should contain a JSON developer message");
    let updated_continuity: serde_json::Value = serde_json::from_str(updated_continuity_json)?;
    assert_eq!(
        updated_continuity["parent_timing"]["snooze_count_since_last_parent_message"],
        0
    );
    assert_eq!(
        updated_continuity["parent_timing"]["snoozed_seconds_since_last_parent_message"],
        0
    );
    assert_eq!(
        updated_continuity["goal_timing"]["snooze_count_since_goal_created"], 1,
        "an unchanged completed parent poll must not reset goal-lifetime backoff"
    );
    assert_eq!(
        updated_continuity["goal_timing"]["snoozed_seconds_since_goal_created"], 60,
        "an unchanged completed parent poll must preserve prior snooze duration"
    );

    let replacement_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_thread_id,
            "Begin a different recurring schedule.",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    let replacement_goal_id = replacement_goal.goal_id.clone();
    let replacement_goal = crate::goal_supervisor::protocol_goal_from_state(replacement_goal);
    let replacement_continuity_text = goal_supervisor_continuity_text_for_test(
        &parent_thread.session,
        &replacement_goal_id,
        &replacement_goal,
    )
    .await;
    let replacement_continuity_json = replacement_continuity_text
        .strip_prefix("# Goal Supervisor Continuity\n\n")
        .expect("replacement goal continuity should contain a JSON developer message");
    let replacement_continuity: serde_json::Value =
        serde_json::from_str(replacement_continuity_json)?;
    assert!(
        replacement_continuity["previous_supervisor_action"].is_null(),
        "a new goal must not inherit a previous goal's supervisor action"
    );
    assert_eq!(
        replacement_continuity["goal_timing"]["snooze_count_since_goal_created"], 0,
        "a new goal must not inherit a previous goal's polling backoff"
    );
    assert_eq!(
        replacement_continuity["goal_timing"]["snoozed_seconds_since_goal_created"], 0,
        "a new goal must not inherit a previous goal's snooze duration"
    );
    assert_eq!(
        replacement_continuity["parent_timing"]["snooze_count_since_last_parent_message"], 0,
        "a new goal must not inherit a previous goal's parent-relative snooze count"
    );
    assert_eq!(
        replacement_continuity["parent_timing"]["snoozed_seconds_since_last_parent_message"], 0,
        "a new goal must not inherit a previous goal's parent-relative snooze duration"
    );

    let _ = parent_thread.submit(Op::Shutdown {}).await;
    Ok(())
}

#[test]
fn goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit() {
    run_goal_supervisor_test(
        "goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit",
        goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit_inner(),
    );
}

async fn goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit_inner() {
    let (home, mut config) = test_config().await;
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow multi-agent v2");
    config
        .features
        .enable(Feature::Goals)
        .expect("test config should allow goals");
    config
        .features
        .enable(Feature::GoalSupervisor)
        .expect("test config should allow goal supervisor");
    config.agent_max_threads = None;
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    assert_eq!(
        (
            config.agent_max_threads,
            config.multi_agent_v2.max_concurrent_threads_per_session,
            config.effective_agent_max_threads(MultiAgentVersion::V2),
        ),
        (None, 2, Some(1))
    );
    let harness = AgentControlHarness::new_with_config(home, config.clone()).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let worker_thread_id = ThreadId::new();
    harness
        .control
        .state
        .reserve_spawn_slot(Some(1))
        .expect("the user-visible worker slot should be available")
        .commit(AgentMetadata {
            agent_id: Some(worker_thread_id),
            ..Default::default()
        });
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let before_thread_ids = harness.manager.list_thread_ids().await;
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Verify supervisor thread accounting.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-limit-test",
        &goal,
    )
    .await
    .expect("goal supervisor should bypass the user-visible agent limit");
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;

    let err = match harness
        .control
        .state
        .reserve_spawn_slot(config.effective_agent_max_threads(MultiAgentVersion::V2))
    {
        Ok(_) => panic!("the goal supervisor must not free the counted worker slot"),
        Err(err) => err,
    };
    let CodexErrorDetails::AgentLimitReached { max_threads } = err.details() else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*max_threads, 1);
    assert!(harness.manager.get_thread(helper_thread_id).await.is_ok());

    harness
        .control
        .state
        .release_spawned_thread(worker_thread_id);
    let _ = harness.control.shutdown_live_agent(helper_thread_id).await;
    let _ = parent_thread.submit(Op::Shutdown {}).await;
}

#[test]
fn goal_supervisor_goal_resume_clears_snooze_and_spawns_helper() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_goal_resume_clears_snooze_and_spawns_helper",
        goal_supervisor_goal_resume_clears_snooze_and_spawns_helper_inner(),
    )
}

async fn goal_supervisor_goal_resume_clears_snooze_and_spawns_helper_inner() -> anyhow::Result<()> {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Resume the paused goal now.",
    )
    .await?;
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            goal_id.as_str(),
            Some(chrono::Utc::now().timestamp_millis() + 60_000),
        )
        .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let before_resume_thread_ids = harness.manager.list_thread_ids().await;
    assert_eq!(
        vec![parent_thread_id],
        before_resume_thread_ids,
        "plain idle continuation should honor the supervisor snooze"
    );

    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await?;

    let child_thread_id =
        spawned_thread_id_after(&harness.manager, &before_resume_thread_ids).await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    let child_config = child_thread.config_snapshot().await;
    assert_matches!(
        child_config.session_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_role: Some(agent_role),
            ..
        }) if agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME
    );
    assert_eq!(
        None,
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
            .await?,
        "manual goal resume should clear the persisted supervisor snooze"
    );
    let _ = parent_thread.submit(Op::Shutdown {}).await;
    Ok(())
}

#[test]
fn failed_goal_supervisor_waits_for_one_persisted_retry() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "failed_goal_supervisor_waits_for_one_persisted_retry",
        failed_goal_supervisor_waits_for_one_persisted_retry_inner(),
    )
}

async fn failed_goal_supervisor_waits_for_one_persisted_retry_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse_failed(
                "supervisor-failure-1",
                "model_not_found",
                "saved model unavailable",
            ),
            sse_failed(
                "supervisor-failure-2",
                "model_not_found",
                "saved model unavailable",
            ),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Keep retrying after transient supervisor failures.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;

    let first_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(deadline_ms) = state_db
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                .await?
                && deadline_ms > chrono::Utc::now().timestamp_millis()
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(request_log.requests().len(), 1);
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    assert!(
        first_deadline_ms - chrono::Utc::now().timestamp_millis() <= 60_000,
        "first failure retry should use the one-minute backoff tier"
    );
    let persisted_goal = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await?
        .expect("active goal should remain persisted");
    assert_eq!(
        persisted_goal.status,
        codex_state::ThreadGoalStatus::Active,
        "supervisor failure must not pause, block, or complete the goal"
    );
    let warning = timeout(Duration::from_secs(5), async {
        loop {
            let event = parent_thread
                .next_event()
                .await
                .expect("parent event channel should stay open");
            if let EventMsg::Warning(warning) = event.msg
                && warning.message.contains("Goal supervisor check-in failed")
            {
                break warning.message;
            }
        }
    })
    .await
    .expect("failed supervisor should warn the user");
    assert!(warning.contains("saved model unavailable"));
    assert!(warning.contains("Retrying in"));

    let scheduled_generation =
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await
        .expect("failure should schedule one retry wakeup");
    for _ in 0..3 {
        crate::goal_supervisor::maybe_start_supervisor_checkin(
            &parent_thread.session,
            goal_id.as_str(),
            &goal,
        )
        .await?;
    }
    assert_eq!(
        request_log.requests().len(),
        1,
        "idle signals before the deadline must not replace the failed helper"
    );
    assert_eq!(
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await,
        Some(scheduled_generation),
        "idle signals for the same deadline must reuse the existing sleeping timer"
    );

    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            goal_id.as_str(),
            /*snoozed_until_ms*/ None,
        )
        .await?;
    crate::goal_supervisor::fire_scheduled_supervisor_wakeup_for_test(&parent_thread.session).await;
    for _ in 0..3 {
        crate::goal_supervisor::maybe_start_supervisor_checkin(
            &parent_thread.session,
            goal_id.as_str(),
            &goal,
        )
        .await?;
    }
    timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second failed supervisor should finish");
    let requests_after_retry = request_log.requests();
    assert_eq!(
        requests_after_retry.len(),
        2,
        "one failure retry should run after the in-memory deadline; loaded threads: {:?}; captured ops: {:?}",
        harness.manager.list_thread_ids().await,
        harness.manager.captured_ops(),
    );
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        request_log.requests().len(),
        2,
        "duplicate idle signals must still produce exactly one retry"
    );
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        2,
        "the second implicit failure should advance the backoff tier"
    );
    Ok(())
}

#[test]
fn goal_resume_clears_supervisor_failure_backoff() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_resume_clears_supervisor_failure_backoff",
        goal_resume_clears_supervisor_failure_backoff_inner(),
    )
}

async fn goal_resume_clears_supervisor_failure_backoff_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse_failed(
                "supervisor-failure-1",
                "model_not_found",
                "saved model unavailable",
            ),
            sse_failed(
                "supervisor-failure-2",
                "model_not_found",
                "saved model unavailable",
            ),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Retry immediately when the user resumes this goal.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(deadline_ms) = state_db
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                .await?
                && deadline_ms > chrono::Utc::now().timestamp_millis()
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    let delivered_parent_message = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        "continue".to_string(),
        /*trigger_turn*/ true,
    );
    crate::goal_supervisor::record_followup_action(
        &parent_thread.session,
        &delivered_parent_message,
    )
    .await;
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        0,
        "a valid supervisor followup action should reset failure backoff"
    );

    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await?;
    let second_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && let Some(deadline_ms) = state_db
                    .thread_goals()
                    .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                    .await?
                && deadline_ms != first_deadline_ms
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let second_delay_ms = second_deadline_ms - chrono::Utc::now().timestamp_millis();
    assert!(
        (0..=60_000).contains(&second_delay_ms),
        "manual resume should reset the failure count to the one-minute tier, got {second_delay_ms}ms"
    );
    assert_eq!(request_log.requests().len(), 2);
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    Ok(())
}

#[test]
fn goal_resume_replaces_a_terminal_owned_supervisor_without_backoff() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_resume_replaces_a_terminal_owned_supervisor_without_backoff",
        goal_resume_replaces_a_terminal_owned_supervisor_without_backoff_inner(),
    )
}

async fn goal_resume_replaces_a_terminal_owned_supervisor_without_backoff_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("supervisor-running"),
                ev_completed("supervisor-running"),
            ]))
            .set_delay(Duration::from_millis(250)),
            sse_response(sse_failed(
                "supervisor-replacement-failure",
                "model_not_found",
                "saved model unavailable",
            )),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Retry immediately when resume races a terminal supervisor.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first supervisor request should start");

    assert!(
        harness
            .manager
            .remove_thread(&helper_thread_id)
            .await
            .is_some(),
        "remove the first helper before its completion watcher retires it"
    );
    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await
        .expect("resume should replace the disappeared active helper");
    timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resume should start one immediate replacement");

    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1,
        "the terminal helper retired by resume must not count as a failed retry"
    );
    Ok(())
}

#[test]
#[serial(fork_env)]
fn goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot",
        goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot_inner(),
    )
}

async fn goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-parent"),
                ev_completed("resp-parent"),
            ]),
            sse(vec![
                ev_response_created("resp-child"),
                ev_completed("resp-child"),
            ]),
        ],
    )
    .await;
    let (_home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::AgentPromptInjection);
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let mcp_server_path = config.codex_home.join("fake_mcp_server.py");
    std::fs::write(
        &mcp_server_path,
        r#"import json
import sys

def read_message():
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    return json.loads(line)

def write_message(message):
    body = json.dumps(message).encode("utf-8")
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    request_id = message.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "fake-mcp", "version": "1.0.0"},
            },
        })
    elif method == "tools/list":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [{
                    "name": "echo",
                    "description": "Echo from fake MCP",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                }],
            },
        })
    else:
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "method not found"},
        })
"#,
    )?;
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "rmcp".to_string(),
            McpServerConfig {
                auth: Default::default(),
                transport: McpServerTransportConfig::Stdio {
                    command: "python3".to_string(),
                    args: vec![mcp_server_path.to_string_lossy().to_string()],
                    env: None,
                    env_vars: Vec::new(),
                    cwd: None,
                },
                environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
                enabled: true,
                required: false,
                supports_parallel_tool_calls: false,
                omit_tools_from: None,
                disabled_reason: None,
                oauth: None,
                startup_timeout_sec: Some(Duration::from_secs(5)),
                tool_timeout_sec: None,
                default_tools_approval_mode: None,
                enabled_tools: None,
                disabled_tools: None,
                scopes: None,
                oauth_resource: None,
                tools: std::collections::HashMap::new(),
            },
        )]))
        .expect("test config should allow MCP servers");

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let parent = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await?;
    let parent_thread_id = parent.thread_id;
    let parent_prompt_cache_key = parent.thread.session.prompt_cache_key();
    let mcp_runtime = Arc::clone(&parent.thread.session.services.mcp_runtime);
    assert!(
        mcp_runtime
            .latest_wait_for_server_ready("rmcp", Duration::from_secs(5))
            .await,
        "parent MCP server should become ready before forking"
    );
    let parent_mcp_tools = mcp_runtime.latest_list_all_tools().await;
    assert!(
        parent_mcp_tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "parent MCP manager should expose live MCP tools before forking: tools={parent_mcp_tools:#?}"
    );
    parent
        .thread
        .submit(text_input("parent seed").into())
        .await?;
    wait_for_turn_complete(parent.thread.as_ref()).await;
    parent.thread.session.ensure_rollout_materialized().await;
    parent.thread.session.flush_rollout().await?;
    let before_thread_ids = manager.list_thread_ids().await;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        &state_db,
        parent_thread_id,
        &parent.thread.session,
        "Supervise the parent with inherited MCP tools.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent.thread.session, &goal_id, &goal)
        .await?;
    let child_thread_id = spawned_thread_id_after(&manager, &before_thread_ids).await;
    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let child_mcp_tool_snapshot = child_thread
        .session
        .services
        .mcp_tool_snapshot
        .lock()
        .await
        .clone()
        .expect("goal supervisor helper should inherit the parent MCP tool snapshot");
    assert!(
        child_mcp_tool_snapshot
            .tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "goal supervisor helper should inherit the parent MCP tool snapshot"
    );

    timeout(Duration::from_secs(5), async {
        loop {
            let event = child_thread
                .next_event()
                .await
                .expect("child event channel should stay open");
            if matches!(event.msg, EventMsg::TurnComplete(_)) {
                break;
            }
        }
    })
    .await
    .expect("child turn should complete");
    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let parent_body = requests[0].body_json();
    let child_body = requests[1].body_json();
    let parent_input = parent_body["input"]
        .as_array()
        .expect("parent input should be an array");
    let child_input = child_body["input"]
        .as_array()
        .expect("child input should be an array");
    let expected_prompt_cache_key = parent_prompt_cache_key.to_string();
    assert_eq!(
        child_body["prompt_cache_key"].as_str(),
        Some(expected_prompt_cache_key.as_str())
    );
    assert_eq!(
        &child_input[..parent_input.len()],
        parent_input,
        "goal supervisor helpers must preserve the exact parent request prefix through the fork point so the fork can reuse the parent prompt cache"
    );
    let child_suffix = &child_input[parent_input.len()..];
    assert!(
        child_suffix.first().is_some_and(|item| {
            item["role"] == "developer"
                && item["content"].as_array().is_some_and(|content| {
                    content.iter().any(|content_item| {
                        content_item["text"]
                            .as_str()
                            .is_some_and(|text| text.contains("You are also a **goal supervisor**"))
                    })
                })
        }),
        "goal supervisor helpers should append the supervisor role prompt immediately after the inherited parent request prefix: suffix={child_suffix:#?}"
    );
    for unexpected_child_context in [
        "# AGENTS.md instructions",
        "<permissions instructions>",
        "<apps_instructions>",
        "<skills_instructions>",
        "<plugins_instructions>",
    ] {
        assert!(
            child_suffix.iter().all(|item| {
                item["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .all(|content_item| {
                        content_item["text"]
                            .as_str()
                            .is_none_or(|text| !text.contains(unexpected_child_context))
                    })
            }),
            "goal supervisor helpers must not append fresh child startup context after forking: found {unexpected_child_context} in suffix={child_suffix:#?}"
        );
    }
    assert_eq!(
        child_body["parallel_tool_calls"], parent_body["parallel_tool_calls"],
        "goal supervisor helpers must keep the same parallel tool-call setting as their parent"
    );
    assert_eq!(
        child_body["tools"], parent_body["tools"],
        "goal supervisor helpers must keep the same serialized tool definitions, order, namespaces, and schemas as their parent"
    );
    let parent_tool_signatures = request_tool_signatures(&parent_body);
    let child_tool_signatures = request_tool_signatures(&child_body);
    assert_eq!(
        child_tool_signatures, parent_tool_signatures,
        "goal supervisor helpers are internal full-history forks and must keep the same eager tool surface as their parent so request prefixes stay cacheable"
    );
    for expected_tool in [
        "collaboration.spawn_agent",
        "collaboration.send_message",
        "collaboration.followup_task",
        "collaboration.wait_agent",
        "collaboration.list_agents",
        "collaboration.interrupt_agent",
        "supervisor.close_self",
        "supervisor.snooze",
        "supervisor.compact_parent_context",
    ] {
        assert!(
            child_tool_signatures.contains(expected_tool),
            "expected forked child request to expose `{expected_tool}`; tools={child_tool_signatures:#?}"
        );
    }
    assert!(
        child_body["tools"].as_array().is_some_and(|tools| {
            tools
                .iter()
                .any(|tool| tool["type"].as_str() == Some("tool_search"))
        }),
        "the inherited MCP snapshot should be discoverable through upstream tool_search: {child_body:#}"
    );

    Ok(())
}

#[test]
fn goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume",
        goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume_inner(),
    )
}

async fn goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume_inner()
-> anyhow::Result<()> {
    const ALIAS: &str = "frontier-local";
    const OLD_BACKING: &str = "gpt-5.2-preview";
    const NEW_BACKING: &str = "gpt-5.4-preview";
    const OLD_GUIDANCE: &str = "Preserve the original alias-backed context.";
    const NEW_GUIDANCE: &str = "Preserve the updated alias-backed context.";

    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-parent"),
                ev_completed("resp-parent"),
            ]),
            sse(vec![
                ev_response_created("resp-supervisor"),
                ev_completed("resp-supervisor"),
            ]),
            sse(vec![
                ev_response_created("resp-resumed"),
                ev_completed("resp-resumed"),
            ]),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let mut model_catalog = bundled_models_response()?;
    for (slug, guidance) in [("gpt-5.2", OLD_GUIDANCE), ("gpt-5.4", NEW_GUIDANCE)] {
        model_catalog
            .models
            .iter_mut()
            .find(|model| model.slug == slug)
            .and_then(|model| model.model_messages.as_mut())
            .expect("backing model should expose model messages")
            .token_budget = Some(ModelTokenBudgetConfig {
            reminder_threshold_tokens: 6_144,
            reminder_message_template: "Alias reminder: {n_remaining} tokens remain.".to_string(),
            guidance_message: guidance.to_string(),
            auto_compact_fallback_prompt: "Save state before rollover.".to_string(),
            auto_compact_fallback_buffer_tokens: 16_384,
        });
    }
    let _ = config.features.enable(Feature::AgentPromptInjection);
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let _ = config.features.enable(Feature::TokenBudget);
    config.model = Some(ALIAS.to_string());
    config.model_catalog = Some(model_catalog);
    config.custom_models.insert(
        ALIAS.to_string(),
        CustomModelConfig {
            model: OLD_BACKING.to_string(),
            routing_profile: None,
            model_context_window: Some(128_000),
            model_auto_compact_token_limit: Some(100_000),
        },
    );
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let auth_manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
    let manager = ThreadManager::new(
        &config,
        Arc::clone(&auth_manager),
        crate::thread_manager::build_models_manager(&config, auth_manager),
        crate::CodexAppsToolsCache::default(),
        SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        empty_extension_registry(),
        Arc::new(crate::test_support::EmptyUserInstructionsProvider),
        /*analytics_events_client*/ None,
        crate::thread_manager::thread_store_from_config(&config, Some(state_db.clone())),
        crate::thread_manager::local_agent_graph_store_from_state_db(Some(&state_db)),
        uuid::Uuid::new_v4().to_string(),
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    );
    let control = manager.agent_control();
    let harness = AgentControlHarness {
        _home: home,
        config: config.clone(),
        state_db: Some(state_db),
        manager,
        control,
    };
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let parent_prompt_cache_key = parent_thread.session.prompt_cache_key();
    parent_thread
        .submit(text_input("seed alias-backed guidance").into())
        .await?;
    wait_for_turn_complete(parent_thread.as_ref()).await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread.session.flush_rollout().await?;
    let (_goal_id, _goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Keep the alias-backed parent progressing.",
    )
    .await?;
    let child_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(
            AgentPath::root()
                .join(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
                .expect("goal supervisor path should be valid"),
        ),
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            config.clone(),
            text_input("inspect the parent goal"),
            Some(child_source.clone()),
            SpawnAgentOptions {
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await?
        .thread_id;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("goal supervisor helper should be registered");
    let child_rollout_path = child_thread
        .rollout_path()
        .expect("segmented goal supervisor rollout should exist");
    wait_for_turn_complete(child_thread.as_ref()).await;
    child_thread.session.flush_rollout().await?;

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let parent_input = requests[0].input();
    let child_input = requests[1].input();
    assert_eq!(requests[0].body_json()["model"], OLD_BACKING);
    assert_eq!(requests[1].body_json()["model"], OLD_BACKING);
    assert_eq!(
        &child_input[..parent_input.len()],
        parent_input,
        "goal supervisor full-history forks should preserve the exact parent request prefix"
    );
    assert_eq!(
        requests[1].body_json()["prompt_cache_key"],
        requests[0].body_json()["prompt_cache_key"]
    );
    assert_eq!(
        child_thread.session.prompt_cache_key(),
        parent_prompt_cache_key
    );
    assert_eq!(
        requests[0]
            .message_input_texts("developer")
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "custom aliases should inherit their backing model's token-budget guidance"
    );
    assert_eq!(
        requests[1]
            .message_input_texts("developer")
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "the supervisor fork should not duplicate inherited guidance"
    );

    let _ = harness.control.shutdown_live_agent(child_thread_id).await?;
    let mut resumed_config = config;
    resumed_config.custom_models.insert(
        ALIAS.to_string(),
        CustomModelConfig {
            model: NEW_BACKING.to_string(),
            routing_profile: None,
            model_context_window: Some(128_000),
            model_auto_compact_token_limit: Some(100_000),
        },
    );
    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(resumed_config, child_thread_id, child_source)
        .await?;
    assert_eq!(resumed_thread_id, child_thread_id);
    let resumed_thread = harness
        .manager
        .get_thread(resumed_thread_id)
        .await
        .expect("cold-resumed supervisor should be registered");
    resumed_thread
        .submit(text_input("inspect resumed alias guidance").into())
        .await?;
    wait_for_turn_complete(resumed_thread.as_ref()).await;
    resumed_thread.session.flush_rollout().await?;

    let requests = request_log.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].body_json()["model"], NEW_BACKING);
    let resumed_developer_texts = requests[2].message_input_texts("developer");
    assert_eq!(
        resumed_developer_texts
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "cold resume should retain the original guidance once"
    );
    assert_eq!(
        resumed_developer_texts
            .iter()
            .filter(|text| text.contains(NEW_GUIDANCE))
            .count(),
        1,
        "cold resume should append the updated backing model guidance once"
    );

    let physical_items = RolloutRecorder::load_rollout_items(child_rollout_path.as_path())
        .await?
        .0;
    let reference_and_world_state = physical_items
        .iter()
        .filter(|item| {
            matches!(
                item,
                RolloutItem::RolloutReference(_) | RolloutItem::WorldState(_)
            )
        })
        .collect::<Vec<_>>();
    let [
        RolloutItem::RolloutReference(_),
        RolloutItem::WorldState(usage_hint_tombstone),
        RolloutItem::WorldState(context_guidance),
    ] = reference_and_world_state.as_slice()
    else {
        panic!(
            "expected rollout reference, usage-hint tombstone, and updated context guidance: {reference_and_world_state:#?}"
        );
    };
    assert!(!usage_hint_tombstone.full);
    assert_eq!(
        usage_hint_tombstone.state,
        serde_json::json!({
            "context_window": "/root/goal_supervisor",
            "multi_agent_mode": {"usage_hint_hash": null},
            "multi_agent_usage_hint": null,
        })
    );
    assert!(!context_guidance.full);
    assert_eq!(
        context_guidance.state,
        serde_json::json!({"context_window_guidance": NEW_GUIDANCE})
    );

    let _ = parent_thread.submit(Op::Shutdown {}).await?;
    Ok(())
}

#[test]
fn goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id()
-> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id",
        goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id_inner(),
    )
}

async fn goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id_inner()
-> anyhow::Result<()> {
    let server = start_websocket_server(vec![
        vec![
            vec![
                ev_response_created("warm-parent"),
                ev_completed("warm-parent"),
            ],
            vec![
                ev_response_created("resp-parent"),
                ev_assistant_message("msg-parent", "parent done"),
                ev_completed("resp-parent"),
            ],
        ],
        vec![
            vec![
                ev_response_created("warm-supervisor"),
                ev_completed("warm-supervisor"),
            ],
            vec![
                ev_response_created("resp-supervisor"),
                ev_completed("resp-supervisor"),
            ],
        ],
    ])
    .await;
    let (_home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = true;

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let parent = manager
        .start_thread(StartThreadOptions::new(config))
        .await?;
    let parent_thread_id = parent.thread_id;
    let parent_prompt_cache_key = parent.thread.session.prompt_cache_key();
    parent
        .thread
        .submit(text_input("parent seed").into())
        .await?;
    wait_for_turn_complete(parent.thread.as_ref()).await;
    parent.thread.session.ensure_rollout_materialized().await;
    parent.thread.session.flush_rollout().await?;
    let before_thread_ids = manager.list_thread_ids().await;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        &state_db,
        parent_thread_id,
        &parent.thread.session,
        "Supervise the parent with prompt cache inheritance.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent.thread.session, &goal_id, &goal)
        .await?;
    let child_thread_id = spawned_thread_id_after(&manager, &before_thread_ids).await;
    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    wait_for_turn_complete(child_thread.as_ref()).await;

    let connections = server.connections();
    let supervisor_connection = connections
        .get(1)
        .expect("supervisor helper should use its own websocket connection");
    let supervisor_generated_request = supervisor_connection
        .iter()
        .map(core_test_support::responses::WebSocketRequest::body_json)
        .find(|body| body["generate"].as_bool() != Some(false))
        .unwrap_or_else(|| {
            panic!(
                "goal supervisor helper should send a generated websocket request after warmup; supervisor requests={supervisor_connection:#?}"
            )
        });
    assert_ne!(
        supervisor_generated_request["previous_response_id"].as_str(),
        Some("resp-parent"),
        "goal supervisor helpers must not reuse a parent websocket previous_response_id on their own websocket connection"
    );
    assert_eq!(
        supervisor_generated_request["prompt_cache_key"].as_str(),
        Some(parent_prompt_cache_key.to_string().as_str()),
        "goal supervisor helpers must keep the parent prompt cache key across websocket connections"
    );
    assert!(
        request_tool_signatures(&supervisor_generated_request).contains("supervisor.close_self"),
        "goal supervisor helper websocket requests must retain the supervisor tool namespace"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial(fork_env)]
async fn fork_previous_response_id_env_disabled_keeps_prompt_cache_key_inheritance()
-> anyhow::Result<()> {
    let _previous_response_id_guard = EnvVarGuard::set(
        CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID_ENV,
        OsStr::new("0"),
    );
    let server = start_mock_server().await;
    let child_response_mock = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-1"), ev_completed("resp-1")]),
    )
    .await;
    let (_home, mut config) = test_config().await;
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let mcp_server_path = config.codex_home.join("fake_mcp_server.py");
    std::fs::write(
        &mcp_server_path,
        r#"import json
import sys

def read_message():
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    return json.loads(line)

def write_message(message):
    body = json.dumps(message).encode("utf-8")
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    request_id = message.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "fake-mcp", "version": "1.0.0"},
            },
        })
    elif method == "tools/list":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [{
                    "name": "echo",
                    "description": "Echo from fake MCP",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                }],
            },
        })
    else:
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "method not found"},
        })
"#,
    )?;
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "rmcp".to_string(),
            McpServerConfig {
                auth: Default::default(),
                transport: McpServerTransportConfig::Stdio {
                    command: "python3".to_string(),
                    args: vec![mcp_server_path.to_string_lossy().to_string()],
                    env: None,
                    env_vars: Vec::new(),
                    cwd: None,
                },
                environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
                enabled: true,
                required: false,
                supports_parallel_tool_calls: false,
                omit_tools_from: None,
                disabled_reason: None,
                oauth: None,
                startup_timeout_sec: Some(Duration::from_secs(5)),
                tool_timeout_sec: None,
                default_tools_approval_mode: None,
                enabled_tools: None,
                disabled_tools: None,
                scopes: None,
                oauth_resource: None,
                tools: std::collections::HashMap::new(),
            },
        )]))
        .expect("test config should allow MCP servers");

    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();
    let parent = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await?;
    let parent_thread_id = parent.thread_id;
    let parent_prompt_cache_key = parent.thread.session.prompt_cache_key();
    let mcp_runtime = Arc::clone(&parent.thread.session.services.mcp_runtime);
    assert!(
        mcp_runtime
            .latest_wait_for_server_ready("rmcp", Duration::from_secs(5))
            .await,
        "parent MCP server should become ready before forking"
    );
    let parent_mcp_tools = mcp_runtime.latest_list_all_tools().await;
    assert!(
        parent_mcp_tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "parent MCP manager should expose live MCP tools before forking: tools={parent_mcp_tools:#?}"
    );
    parent
        .thread
        .inject_user_message_without_turn("parent seed".to_string())
        .await;
    parent.thread.session.ensure_rollout_materialized().await;
    parent.thread.session.flush_rollout().await?;

    // The child has no independently configured MCP servers. Its first request can only advertise
    // the parent's tool catalog if full-history fork inheritance applies the snapshot.
    config
        .mcp_servers
        .set(std::collections::HashMap::new())
        .expect("test config should allow clearing MCP servers");

    let child_thread_id = control
        .spawn_agent_with_metadata(
            config,
            text_input("child request boundary"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: Some("worker".to_string()),
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(
                    "spawn-call-previous-response-id-disabled".to_string(),
                ),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await?
        .thread_id;
    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");

    timeout(Duration::from_secs(5), async {
        loop {
            let event = child_thread
                .next_event()
                .await
                .expect("child event channel should stay open");
            if matches!(event.msg, EventMsg::TurnComplete(_)) {
                break;
            }
        }
    })
    .await
    .expect("child turn should complete");
    let child_prompt_cache_key = child_thread.session.prompt_cache_key();
    assert_eq!(child_prompt_cache_key, parent_prompt_cache_key);

    let body = child_response_mock.single_request().body_json();
    let expected_prompt_cache_key = parent_prompt_cache_key.to_string();
    assert_eq!(
        body["prompt_cache_key"].as_str(),
        Some(expected_prompt_cache_key.as_str())
    );
    assert!(
        body["tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().any(|tool| {
                tool["type"] == "tool_search"
                    && tool["description"]
                        .as_str()
                        .is_some_and(|description| description.contains("\n- rmcp\n"))
            })),
        "forked child request should advertise the inherited parent MCP tool catalog: {body:#}"
    );

    Ok(())
}

#[tokio::test]
async fn spawn_agent_full_history_fork_preserves_compacted_parent_prefix() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    parent_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Parent root guidance.".to_string());
    parent_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Parent subagent guidance.".to_string());
    let mut child_config = harness.config.clone();
    let _ = child_config.features.enable(Feature::MultiAgentV2);
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Child root guidance.".to_string());
    child_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Child subagent guidance.".to_string());
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-compacted-usage-hints".to_string();
    let parent_task = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root().join("worker").expect("valid worker path"),
        Vec::new(),
        "compacted parent delegated task".to_string(),
        /*trigger_turn*/ true,
    );
    let supervisor_reply = InterAgentCommunication::new(
        AgentPath::root()
            .join("goal_supervisor")
            .expect("valid supervisor path"),
        AgentPath::root(),
        Vec::new(),
        "pong 7 (42)".to_string(),
        /*trigger_turn*/ true,
    );
    let replacement_history = vec![
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "compacted parent summary".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        parent_task.to_model_input_item(),
        supervisor_reply.to_model_input_item(),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "Parent root guidance.".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![
                ContentItem::InputText {
                    text: "Compacted context before.\nParent developer instructions.\nCompacted context after."
                        .to_string(),
                },
                ContentItem::InputText {
                    text: "Preserved compacted developer context.".to_string(),
                },
            ],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
    ];
    // Match the live baseline established by the persisted TurnContext in a real completed turn.
    parent_thread
        .session
        .replace_history(
            replacement_history.clone(),
            Some(turn_context.to_turn_context_item()),
        )
        .await;
    parent_thread
        .session
        .persist_rollout_items(&[
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(replacement_history),
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
            RolloutItem::TurnContext(turn_context.to_turn_context_item()),
            RolloutItem::ResponseItem(spawn_agent_call(&parent_spawn_call_id)),
        ])
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await
        .expect("full-history fork should preserve compacted history")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "compacted parent summary"),
        "forked child history should retain compacted non-hint content"
    );
    assert!(
        history.raw_items().iter().any(|item| {
            matches!(
                item,
                ResponseItem::AgentMessage {
                    recipient,
                    content,
                    ..
                } if recipient == AgentPath::root().as_str()
                    && content.iter().any(|item| {
                        matches!(item, AgentMessageInputContent::InputText { text } if text == "pong 7 (42)")
                    })
            )
        }),
        "full-history forks should retain supervisor messages addressed to the parent"
    );
    assert!(
        history.raw_items().iter().any(|item| {
            matches!(
                item,
                ResponseItem::AgentMessage {
                    content,
                    ..
                } if content.iter().any(|item| {
                    matches!(item, AgentMessageInputContent::InputText { text } if text == "compacted parent delegated task")
                })
            )
        }),
        "full-history forks should preserve the complete parent prefix, including delegated tasks"
    );
    assert!(
        history_contains_text(history.raw_items(), "Parent root guidance."),
        "full-history forked child history should preserve the compacted parent prefix"
    );
    assert!(
        history_contains_text(history.raw_items(), "Parent developer instructions."),
        "forked child history should preserve parent instructions in the inherited prefix"
    );
    assert!(
        history_contains_text(
            history.raw_items(),
            "Compacted context before.\nParent developer instructions.\nCompacted context after."
        ),
        "forked child history should preserve the exact compacted parent prefix"
    );
    assert!(
        history_contains_text(history.raw_items(), "Child developer instructions."),
        "full-history forked child should append its instructions after the inherited prefix"
    );
    assert!(
        history_contains_text(
            history.raw_items(),
            "Preserved compacted developer context."
        ),
        "forked child history should preserve unrelated compacted developer fragments"
    );
    assert!(
        history_contains_text(history.raw_items(), "Child subagent guidance."),
        "full-history forked child should add the child subagent hint after the inherited prefix"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

/// Full-history forks must restore child instructions when compaction discarded
/// the only matching parent instruction fragment from effective history.
#[tokio::test]
async fn spawn_agent_full_fork_restores_instructions_after_compaction_discards_parent_fragment() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    let mut child_config = parent_config.clone();
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());

    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-compacted-stale-instructions".to_string();
    let replacement_history = vec![
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "compacted parent summary".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "Preserved compacted developer context.".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
    ];

    // Preserve the parent's live baseline while its durable checkpoint omits the
    // developer fragment that appeared in obsolete pre-compaction history.
    parent_thread
        .session
        .replace_history(
            replacement_history.clone(),
            Some(turn_context.to_turn_context_item()),
        )
        .await;
    parent_thread
        .session
        .persist_rollout_items(&[
            RolloutItem::ResponseItem(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: "Parent developer instructions.".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }),
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(replacement_history),
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
            RolloutItem::TurnContext(turn_context.to_turn_context_item()),
            RolloutItem::ResponseItem(spawn_agent_call(&parent_spawn_call_id)),
        ])
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should preserve effective compacted instructions")
        .thread_id;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(
            history.raw_items(),
            "Preserved compacted developer context."
        ),
        "full-history fork should preserve unrelated compacted developer fragments"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent developer instructions."),
        "full-history fork should not restore stale pre-compaction parent instructions"
    );
    assert!(
        history_contains_text(history.raw_items(), "Child developer instructions."),
        "full-history fork should append child instructions absent from effective compacted history"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

/// A legacy compaction clears the child's baseline, so its first turn must
/// rebuild configured developer instructions exactly once.
#[tokio::test]
async fn spawn_agent_full_fork_legacy_compaction_rebuilds_child_instructions_once() {
    for (case, parent_developer_instructions) in [
        ("without parent instructions", None),
        (
            "with parent instructions",
            Some("Parent developer instructions."),
        ),
    ] {
        let harness = AgentControlHarness::new().await;
        let mut parent_config = harness.config.clone();
        let _ = parent_config.features.enable(Feature::MultiAgentV2);
        parent_config.developer_instructions = parent_developer_instructions.map(str::to_string);
        let mut child_config = parent_config.clone();
        child_config.developer_instructions = Some("Child developer instructions.".to_string());
        child_config.multi_agent_v2.subagent_developer_instructions =
            Some("Child developer instructions.".to_string());

        let new_thread = harness
            .manager
            .start_thread(StartThreadOptions::new(parent_config))
            .await
            .expect("start parent thread");
        let parent_thread_id = new_thread.thread_id;
        let parent_thread = new_thread.thread;
        let turn_context = parent_thread.session.new_default_turn().await;
        let parent_spawn_call_id = match parent_developer_instructions {
            Some(_) => "spawn-call-legacy-compact-with-parent",
            None => "spawn-call-legacy-compact-without-parent",
        };
        let parent_user_message = ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "parent task before legacy compaction".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        };

        // A live parent can reestablish its baseline after resuming a rollout
        // whose older compaction record cannot restore that baseline to a child.
        parent_thread
            .session
            .replace_history(
                vec![parent_user_message.clone()],
                Some(turn_context.to_turn_context_item()),
            )
            .await;
        let mut rollout_items = vec![
            RolloutItem::ResponseItem(parent_user_message),
            RolloutItem::Compacted(CompactedItem {
                message: "legacy compacted summary".to_string(),
                replacement_history: None,
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
        ];
        if let Some(instructions) = parent_developer_instructions {
            rollout_items.push(RolloutItem::ResponseItem(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: instructions.to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }));
        }
        rollout_items.push(RolloutItem::TurnContext(
            turn_context.to_turn_context_item(),
        ));
        rollout_items.push(RolloutItem::ResponseItem(spawn_agent_call(
            parent_spawn_call_id,
        )));
        parent_thread
            .session
            .persist_rollout_items(&rollout_items)
            .await;
        parent_thread.session.ensure_rollout_materialized().await;
        parent_thread
            .session
            .flush_rollout()
            .await
            .expect("parent rollout should flush");

        let child_thread_id = harness
            .control
            .spawn_agent_with_metadata(
                child_config,
                text_input("child task"),
                Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id,
                    depth: 1,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
                SpawnAgentOptions {
                    fork_parent_spawn_call_id: Some(parent_spawn_call_id.to_string()),
                    fork_mode: Some(SpawnAgentForkMode::FullHistory),
                    ..Default::default()
                },
            )
            .await
            .expect("forked spawn should preserve legacy compacted history")
            .thread_id;
        let child_thread = harness
            .manager
            .get_thread(child_thread_id)
            .await
            .expect("child thread should be registered");
        while child_thread
            .session
            .reference_context_item()
            .await
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        let history = child_thread.session.clone_history().await;
        let mut instruction_count = 0;
        for item in history.raw_items() {
            let ResponseItem::Message { role, content, .. } = item else {
                continue;
            };
            if role != "developer" {
                continue;
            }
            for content_item in content {
                if let ContentItem::InputText { text } = content_item
                    && text == "Child developer instructions."
                {
                    instruction_count += 1;
                }
            }
        }
        assert_eq!(
            instruction_count, 1,
            "{case}: canonical context reconstruction must not duplicate child developer instructions"
        );

        let _ = harness
            .control
            .shutdown_live_agent(child_thread_id)
            .await
            .expect("child shutdown should submit");
        let _ = parent_thread
            .submit(Op::Shutdown {})
            .await
            .expect("parent shutdown should submit");
    }
}

#[tokio::test]
async fn spawn_agent_fork_flushes_parent_rollout_before_loading_history() {
    let harness = AgentControlHarness::new().await;
    let mut child_config = harness.config.clone();
    let _ = child_config.features.enable(Feature::AgentPromptInjection);
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-unflushed".to_string();
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[
                assistant_message("unflushed final answer", Some(MessagePhase::FinalAnswer)),
                spawn_agent_call(&parent_spawn_call_id),
            ],
        )
        .await;

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                initial_task_message: Some("child task".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should flush parent rollout before loading history")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "unflushed final answer"),
        "forked child history should include unflushed assistant final answers after flushing the parent rollout"
    );
    assert!(
        history_contains_text(history.raw_items(), "# Subagent Assignment"),
        "forked child history should contain an explicit developer assignment"
    );
    assert_eq!(
        history_text_match_count(history.raw_items(), "# You are a Subagent"),
        1,
        "forked child history must contain exactly one subagent prompt"
    );
    assert_eq!(
        history_text_match_count(history.raw_items(), "# Subagent Assignment"),
        1,
        "forked child history must contain exactly one explicit assignment"
    );
    assert!(
        history_contains_text(
            history.raw_items(),
            "Your direct assignment from your parent agent is:\n\nchild task"
        ),
        "forked child history should make the spawned task unambiguous"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_fork_last_n_turns_keeps_only_recent_turns() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let old_parent_context = format!("old parent context {}", "x".repeat(128 * 1_024));
    parent_thread
        .inject_user_message_without_turn(old_parent_context.clone())
        .await;
    let queued_communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "queued message".to_string(),
        /*trigger_turn*/ false,
    );
    let queued_turn_context = parent_thread.session.new_default_turn().await;
    parent_thread
        .session
        .record_conversation_items(
            queued_turn_context.as_ref(),
            &[queued_communication.to_response_input_item().into()],
        )
        .await;

    let triggered_communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "triggered context".to_string(),
        /*trigger_turn*/ true,
    );
    let triggered_turn_context = parent_thread.session.new_default_turn().await;
    parent_thread
        .session
        .record_conversation_items(
            triggered_turn_context.as_ref(),
            &[triggered_communication.to_response_input_item().into()],
        )
        .await;
    parent_thread
        .inject_user_message_without_turn("current parent task".to_string())
        .await;
    let spawn_turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-last-n".to_string();
    parent_thread
        .session
        .record_conversation_items(
            spawn_turn_context.as_ref(),
            &[spawn_agent_call(&parent_spawn_call_id)],
        )
        .await;
    parent_thread
        .session
        .persist_rollout_items(&[RolloutItem::TurnContext(
            spawn_turn_context.to_turn_context_item(),
        )])
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should keep only the last two turns")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    child_thread
        .flush_rollout()
        .await
        .expect("bounded child rollout should flush");
    let child_rollout_bytes = tokio::fs::read(
        child_thread
            .rollout_path()
            .expect("bounded child rollout path"),
    )
    .await
    .expect("read bounded child rollout");
    assert!(
        child_rollout_bytes.len() < old_parent_context.len(),
        "bounded child storage must not include the large excluded parent prefix"
    );
    let history = child_thread.session.clone_history().await;

    assert!(
        !history_contains_text(history.raw_items(), "old parent context"),
        "forked child history should drop parent context outside the requested last-N turn window"
    );
    assert!(
        !history_contains_text(history.raw_items(), "queued message"),
        "forked child history should drop queued inter-agent messages outside the requested last-N turn window"
    );
    assert!(
        !history_contains_text(history.raw_items(), "triggered context"),
        "forked child history should filter assistant inter-agent messages even when they fall inside the requested last-N turn window"
    );
    assert!(
        history_contains_text(history.raw_items(), "current parent task"),
        "forked child history should keep the parent user message from the requested last-N turn window"
    );
    assert!(
        child_thread
            .session
            .reference_context_item()
            .await
            .is_none(),
        "last-N forked child should rebuild context after truncating the cached prefix"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_fork_last_n_turns_drops_parent_startup_prefix_when_under_limit() {
    let harness = AgentControlHarness::new().await;
    let selected_capability_roots = vec![SelectedCapabilityRoot {
        id: "demo@1".to_string(),
        location: CapabilityRootLocation::Environment {
            environment_id: "build".to_string(),
            path: PathUri::parse("file:///plugins/demo").expect("plugin root URI"),
        },
    }];
    let mut thread_extension_init = ExtensionDataInit::new();
    thread_extension_init.insert(selected_capability_roots.clone());
    let parent = harness
        .manager
        .start_thread(StartThreadOptions {
            environments: Some(Vec::new()),
            thread_extension_init,
            ..StartThreadOptions::new(harness.config.clone())
        })
        .await
        .expect("start parent thread");
    let parent_thread_id = parent.thread_id;
    let parent_thread = parent.thread;
    let startup_turn_context = parent_thread.session.new_default_turn().await;
    parent_thread
        .session
        .record_conversation_items(
            startup_turn_context.as_ref(),
            &[ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: "parent startup developer context".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }],
        )
        .await;
    parent_thread
        .inject_user_message_without_turn("current parent task".to_string())
        .await;
    let spawn_turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-last-n-under-limit".to_string();
    parent_thread
        .session
        .record_conversation_items(
            spawn_turn_context.as_ref(),
            &[spawn_agent_call(&parent_spawn_call_id)],
        )
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await
        .expect("bounded forked spawn should drop startup prefix")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "current parent task"),
        "bounded fork should retain the requested recent parent turn"
    );
    assert!(
        !history_contains_text(history.raw_items(), "parent startup developer context"),
        "bounded fork should drop parent startup context even when fewer turns exist than requested"
    );
    assert_eq!(
        &child_thread.session.services.selected_capability_roots,
        &selected_capability_roots
    );
    assert!(
        child_thread
            .session
            .reference_context_item()
            .await
            .is_none(),
        "bounded forked child should still rebuild context after truncating the cached prefix"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_fork_last_n_turns_strips_parent_usage_hints() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    parent_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Parent root guidance.".to_string());
    let mut child_config = harness.config.clone();
    let _ = child_config.features.enable(Feature::MultiAgentV2);
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Child subagent guidance.".to_string());
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    parent_thread
        .inject_user_message_without_turn("parent task".to_string())
        .await;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-last-n-usage-hints".to_string();
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "Parent root guidance.".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![
                        ContentItem::InputText {
                            text: "Parent developer instructions.".to_string(),
                        },
                        ContentItem::InputText {
                            text: "Preserved bounded developer context.".to_string(),
                        },
                    ],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                spawn_agent_call(&parent_spawn_call_id),
            ],
        )
        .await;
    parent_thread.session.ensure_rollout_materialized().await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await
        .expect("bounded forked spawn should sanitize parent usage hints")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "parent task"),
        "bounded fork should retain the requested recent parent turn"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent root guidance."),
        "bounded fork should strip stale parent root hints before the child rebuilds startup context"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent developer instructions."),
        "bounded fork should remove parent instructions before the child rebuilds startup context"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Child developer instructions."),
        "bounded fork should not inject child instructions before its canonical context rebuild"
    );
    assert!(
        history_contains_text(history.raw_items(), "Preserved bounded developer context."),
        "bounded fork should preserve unrelated developer fragments"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_respects_legacy_max_threads_alias() {
    let max_threads = 1usize;
    let (_home, mut config) = test_config_with_cli_overrides(vec![(
        "agents.max_threads".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("legacy max_threads test should disable MultiAgentV2");
    config.agent_max_threads = Some(max_threads);
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let _ = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start thread");

    let first_agent_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");

    let err = control
        .spawn_agent(
            config,
            text_input("hello again"),
            /*session_source*/ None,
        )
        .await
        .expect_err("spawn_agent should respect max threads");
    let CodexErrorDetails::AgentLimitReached {
        max_threads: seen_max_threads,
    } = err.details()
    else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*seen_max_threads, max_threads);

    let _ = control
        .shutdown_live_agent(first_agent_id)
        .await
        .expect("shutdown agent");
}

#[tokio::test]
async fn spawn_agent_releases_slot_after_shutdown() {
    let max_threads = 1usize;
    let (_home, mut config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("legacy max_threads test should disable MultiAgentV2");
    config.agent_max_threads = Some(max_threads);
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let first_agent_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");
    let _ = control
        .shutdown_live_agent(first_agent_id)
        .await
        .expect("shutdown agent");

    let second_agent_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello again"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed after shutdown");
    let _ = control
        .shutdown_live_agent(second_agent_id)
        .await
        .expect("shutdown agent");
}

#[tokio::test]
async fn spawn_agent_limit_shared_across_clones() {
    let max_threads = 1usize;
    let (_home, mut config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("legacy max_threads test should disable MultiAgentV2");
    config.agent_max_threads = Some(max_threads);
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();
    let cloned = control.clone();

    let first_agent_id = cloned
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");

    let err = control
        .spawn_agent(
            config,
            text_input("hello again"),
            /*session_source*/ None,
        )
        .await
        .expect_err("spawn_agent should respect shared guard");
    let CodexErrorDetails::AgentLimitReached { max_threads } = err.details() else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*max_threads, 1);

    let _ = control
        .shutdown_live_agent(first_agent_id)
        .await
        .expect("shutdown agent");
}

#[tokio::test]
async fn resume_agent_respects_max_threads_limit() {
    let max_threads = 1usize;
    let (_home, mut config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("legacy max_threads test should disable MultiAgentV2");
    config.agent_max_threads = Some(max_threads);
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let resumable_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");
    let _ = control
        .shutdown_live_agent(resumable_id)
        .await
        .expect("shutdown resumable thread");

    let active_id = control
        .spawn_agent(
            config.clone(),
            text_input("occupy"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed for active slot");

    let err = control
        .resume_agent_from_rollout(config, resumable_id, SessionSource::Exec)
        .await
        .expect_err("resume should respect max threads");
    let CodexErrorDetails::AgentLimitReached {
        max_threads: seen_max_threads,
    } = err.details()
    else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*seen_max_threads, max_threads);

    let _ = control
        .shutdown_live_agent(active_id)
        .await
        .expect("shutdown active thread");
}

#[tokio::test]
async fn resume_agent_releases_slot_after_resume_failure() {
    let max_threads = 1usize;
    let (_home, mut config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("legacy max_threads test should disable MultiAgentV2");
    config.agent_max_threads = Some(max_threads);
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let _ = control
        .resume_agent_from_rollout(config.clone(), ThreadId::new(), SessionSource::Exec)
        .await
        .expect_err("resume should fail for missing rollout path");

    let resumed_id = control
        .spawn_agent(config, text_input("hello"), /*session_source*/ None)
        .await
        .expect("spawn should succeed after failed resume");
    let _ = control
        .shutdown_live_agent(resumed_id)
        .await
        .expect("shutdown resumed thread");
}

#[tokio::test]
async fn spawn_child_completion_notifies_parent_history() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    child_thread
        .shutdown_and_wait()
        .await
        .expect("child shutdown should complete");

    assert_eq!(wait_for_subagent_notification(&parent_thread).await, true);
}

#[tokio::test]
async fn multi_agent_v2_completion_ignores_dead_direct_parent() {
    let harness = AgentControlHarness::new().await;
    let mut config = harness.config.clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    let root = harness
        .manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("root thread should start");
    let root_thread_id = root.thread_id;
    let root_thread = root.thread;
    let worker_path = AgentPath::root().join("worker_a").expect("worker path");
    let worker_thread_id = harness
        .control
        .spawn_agent(
            config.clone(),
            text_input("hello worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("worker spawn should succeed");
    let tester_path = worker_path.join("tester").expect("tester path");
    let tester_thread_id = harness
        .control
        .spawn_agent(
            config,
            text_input("hello tester"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: Some(tester_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("tester spawn should succeed");
    harness
        .control
        .shutdown_live_agent(worker_thread_id)
        .await
        .expect("worker shutdown should succeed");

    let tester_thread = harness
        .manager
        .get_thread(tester_thread_id)
        .await
        .expect("tester thread should exist");
    let tester_turn = tester_thread.session.new_default_turn().await;
    tester_thread
        .session
        .send_event(
            tester_turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: tester_turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;

    sleep(Duration::from_millis(100)).await;

    assert!(
        !harness
            .manager
            .captured_ops()
            .into_iter()
            .any(|(thread_id, op)| {
                thread_id == worker_thread_id
                    && matches!(
                        op,
                        Op::InterAgentCommunication { communication }
                            if communication.author == tester_path
                                && communication.recipient == worker_path
                                && communication.content == "done"
                    )
            })
    );

    let root_history_items = root_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .to_vec();
    assert!(!history_contains_assistant_inter_agent_communication(
        &root_history_items,
        &InterAgentCommunication::new(
            tester_path,
            AgentPath::root(),
            Vec::new(),
            "done".to_string(),
            /*trigger_turn*/ true,
        )
    ));
    assert!(!has_subagent_notification(&root_history_items));
}

#[tokio::test]
async fn multi_agent_v2_completion_queues_message_for_direct_parent() {
    let harness = AgentControlHarness::new().await;
    let (_root_thread_id, root_thread) = harness.start_thread().await;
    let (worker_thread_id, _worker_thread) = harness.start_thread().await;
    let mut tester_config = harness.config.clone();
    let _ = tester_config.features.enable(Feature::MultiAgentV2);
    let tester_thread_id = harness
        .manager
        .start_thread(StartThreadOptions::new(tester_config.clone()))
        .await
        .expect("tester thread should start")
        .thread_id;
    let tester_thread = harness
        .manager
        .get_thread(tester_thread_id)
        .await
        .expect("tester thread should exist");
    let tester_turn = tester_thread.session.new_default_turn().await;
    assert_eq!(
        tester_thread
            .session
            .resolve_multi_agent_version_for_model(&tester_turn.model_info, &tester_config),
        MultiAgentVersion::V2,
    );
    let worker_path = AgentPath::root().join("worker_a").expect("worker path");
    let tester_path = worker_path.join("tester").expect("tester path");
    harness.control.maybe_start_completion_watcher(
        tester_thread_id,
        Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: worker_thread_id,
            depth: 2,
            agent_path: Some(tester_path.clone()),
            agent_nickname: None,
            agent_role: Some("explorer".to_string()),
        })),
        tester_path.to_string(),
        Some(tester_path.clone()),
    );
    let tester_turn = tester_thread.session.new_default_turn().await;
    tester_thread
        .session
        .send_event(
            tester_turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: tester_turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;

    let expected_message = crate::session_prefix::format_inter_agent_completion_message(
        worker_path.clone(),
        tester_path.clone(),
        &AgentStatus::Completed(Some("done".to_string())),
    )
    .expect("completed status should render");
    let expected = (
        worker_thread_id,
        Op::InterAgentCommunication {
            communication: InterAgentCommunication::new(
                tester_path.clone(),
                worker_path.clone(),
                Vec::new(),
                expected_message.clone(),
                /*trigger_turn*/ false,
            ),
        },
    );

    timeout(Duration::from_secs(5), async {
        loop {
            let captured = harness
                .manager
                .captured_ops()
                .into_iter()
                .find(|entry| *entry == expected);
            if captured == Some(expected.clone()) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completion watcher should queue a direct-parent message");

    let root_history_items = root_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .to_vec();
    assert!(!history_contains_assistant_inter_agent_communication(
        &root_history_items,
        &InterAgentCommunication::new(
            tester_path,
            AgentPath::root(),
            Vec::new(),
            expected_message,
            /*trigger_turn*/ false,
        )
    ));
}

#[tokio::test]
async fn completion_watcher_notifies_parent_when_child_is_missing() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let child_thread_id = ThreadId::new();

    harness.control.maybe_start_completion_watcher(
        child_thread_id,
        Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: Some("explorer".to_string()),
        })),
        child_thread_id.to_string(),
        /*child_agent_path*/ None,
    );

    assert_eq!(wait_for_subagent_notification(&parent_thread).await, true);

    let history_items = parent_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .to_vec();
    assert_eq!(
        history_contains_text(
            &history_items,
            &format!("\"agent_path\":\"{child_thread_id}\"")
        ),
        true
    );
    assert_eq!(
        history_contains_text(&history_items, "\"status\":\"not_found\""),
        true
    );
}

#[tokio::test]
async fn spawn_thread_subagent_gets_random_nickname_in_session_source() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let snapshot = child_thread.config_snapshot().await;

    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: seen_parent_thread_id,
        depth,
        agent_nickname,
        agent_role,
        ..
    }) = snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(seen_parent_thread_id, parent_thread_id);
    assert_eq!(depth, 1);
    assert!(agent_nickname.is_some());
    assert_eq!(agent_role, Some("explorer".to_string()));
}

#[tokio::test]
async fn spawn_thread_subagents_persist_parent_originator_across_new_and_truncated_fork() {
    let harness = AgentControlHarness::new().await;
    let parent = harness
        .manager
        .start_thread(StartThreadOptions {
            metrics_service_name: Some("codex_work_desktop".to_string()),
            environments: Some(Vec::new()),
            ..StartThreadOptions::new(harness.config.clone())
        })
        .await
        .expect("parent thread should start");
    let parent_originator = persisted_originator(&parent.thread).await;
    assert_eq!(parent_originator, "codex_work_desktop");

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: parent.thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let child_originator = persisted_originator(&child_thread).await;
    assert_eq!(child_originator, parent_originator);

    let child = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("hello forked child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: parent.thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some("spawn-call-last-n".to_string()),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(1)),
                ..Default::default()
            },
        )
        .await
        .expect("forked child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child.thread_id)
        .await
        .expect("child thread should be registered");
    let child_originator = persisted_originator(&child_thread).await;
    assert_eq!(child_originator, parent_originator);
}

#[tokio::test]
async fn spawn_thread_subagent_uses_role_specific_nickname_candidates() {
    let mut harness = AgentControlHarness::new().await;
    harness.config.agent_roles.insert(
        "researcher".to_string(),
        AgentRoleConfig {
            description: Some("Research role".to_string()),
            config_file: None,
            nickname_candidates: Some(vec!["Atlas".to_string()]),
        },
    );
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("researcher".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let snapshot = child_thread.config_snapshot().await;

    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_nickname, .. }) =
        snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(agent_nickname, Some("Atlas".to_string()));
}

#[tokio::test]
async fn resume_thread_subagent_restores_stored_metadata() {
    let (home, config) = test_config().await;
    let thread_store = Arc::new(InMemoryThreadStore::default());
    let auth_manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
    let manager = ThreadManager::new(
        &config,
        auth_manager.clone(),
        crate::thread_manager::build_models_manager(&config, auth_manager),
        crate::CodexAppsToolsCache::default(),
        SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        empty_extension_registry(),
        Arc::new(crate::test_support::EmptyUserInstructionsProvider),
        /*analytics_events_client*/ None,
        thread_store.clone(),
        /*agent_graph_store*/ None,
        uuid::Uuid::new_v4().to_string(),
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    );
    let control = manager.agent_control();
    let harness = AgentControlHarness {
        _home: home,
        config,
        state_db: None,
        manager,
        control,
    };
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let agent_path = AgentPath::from_string("/root/explorer".to_string())
        .expect("test agent path should be valid");

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(agent_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    child_thread.session.ensure_rollout_materialized().await;
    child_thread
        .session
        .flush_rollout()
        .await
        .expect("flush child rollout");
    let mut status_rx = harness
        .control
        .subscribe_status(child_thread_id)
        .await
        .expect("status subscription should succeed");
    if matches!(status_rx.borrow().clone(), AgentStatus::PendingInit) {
        timeout(Duration::from_secs(5), async {
            loop {
                status_rx
                    .changed()
                    .await
                    .expect("child status should advance past pending init");
                if !matches!(status_rx.borrow().clone(), AgentStatus::PendingInit) {
                    break;
                }
            }
        })
        .await
        .expect("child should initialize before shutdown");
    }
    let original_snapshot = child_thread.config_snapshot().await;
    let original_nickname = original_snapshot
        .session_source
        .get_nickname()
        .expect("spawned sub-agent should have a nickname");
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(stored_thread) = thread_store
                .read_thread(ReadThreadParams {
                    thread_id: child_thread_id,
                    include_archived: true,
                    include_history: false,
                })
                .await
                && stored_thread.agent_nickname.is_some()
                && stored_thread.agent_role.as_deref() == Some("explorer")
                && stored_thread.agent_path.as_deref() == Some(agent_path.as_str())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child thread metadata should be persisted to sqlite before shutdown");

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");

    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            child_thread_id,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            }),
        )
        .await
        .expect("resume should succeed");
    assert_eq!(resumed_thread_id, child_thread_id);

    let resumed_thread = harness
        .manager
        .get_thread(resumed_thread_id)
        .await
        .expect("resumed child thread should exist");
    assert_eq!(
        resumed_thread.session.prompt_cache_key(),
        resumed_thread_id,
        "resume should keep the resumed thread's own cache key"
    );
    assert_ne!(
        resumed_thread.session.prompt_cache_key(),
        parent_thread.session.prompt_cache_key(),
        "resume must not opportunistically inherit cache state from a live parent"
    );
    let resumed_snapshot = resumed_thread.config_snapshot().await;
    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: resumed_parent_thread_id,
        depth: resumed_depth,
        agent_path: resumed_agent_path,
        agent_nickname: resumed_nickname,
        agent_role: resumed_role,
        ..
    }) = resumed_snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_eq!(resumed_depth, 1);
    assert_eq!(resumed_agent_path, Some(agent_path));
    assert_eq!(resumed_nickname, Some(original_nickname));
    assert_eq!(resumed_role, Some("explorer".to_string()));

    let _ = harness
        .control
        .shutdown_live_agent(resumed_thread_id)
        .await
        .expect("resumed child shutdown should submit");
}

#[tokio::test]
async fn resume_agent_from_rollout_reads_archived_rollout_path() {
    let harness = AgentControlHarness::new().await;
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    persist_thread_for_tree_resume(&child_thread, "persist before archiving").await;
    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should succeed");
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig::from_config(&harness.config),
        harness.state_db.clone(),
    );
    store
        .archive_thread(ArchiveThreadParams {
            thread_id: child_thread_id,
        })
        .await
        .expect("child thread should archive");

    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(harness.config.clone(), child_thread_id, SessionSource::Exec)
        .await
        .expect("resume should find archived rollout");
    assert_eq!(resumed_thread_id, child_thread_id);

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("resumed child shutdown should succeed");
}

#[tokio::test]
async fn resume_agent_from_paginated_rollout_loads_model_context() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    let child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    assert_eq!(
        child_thread.config_snapshot().await.history_mode,
        ThreadHistoryMode::Paginated
    );
    persist_thread_for_tree_resume(&child_thread, "persist before resume").await;
    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should succeed");

    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(harness.config.clone(), child_thread_id, SessionSource::Exec)
        .await
        .expect("resume should load paginated model context");
    assert_eq!(resumed_thread_id, child_thread_id);
    let resumed_thread = harness
        .manager
        .get_thread(resumed_thread_id)
        .await
        .expect("resumed child thread should exist");
    assert!(
        history_contains_text(
            resumed_thread.session.clone_history().await.raw_items(),
            "persist before resume",
        ),
        "resumed child should keep its persisted model context"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("resumed child shutdown should succeed");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn list_agent_subtree_thread_ids_includes_anonymous_and_closed_descendants() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let worker_path = AgentPath::root().join("worker").expect("worker path");
    let reviewer_path = AgentPath::root().join("reviewer").expect("reviewer path");

    let worker_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("worker spawn should succeed");
    let worker_child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello worker child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: Some(
                    worker_path
                        .join("child")
                        .expect("worker child path should be valid"),
                ),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("worker child spawn should succeed");
    let no_path_child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello anonymous child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("no-path child spawn should succeed");
    let no_path_grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello anonymous grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: no_path_child_thread_id,
                depth: 3,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("no-path grandchild spawn should succeed");
    let _reviewer_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello reviewer"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(reviewer_path),
                agent_nickname: None,
                agent_role: Some("reviewer".to_string()),
            })),
        )
        .await
        .expect("reviewer spawn should succeed");

    let _ = harness
        .control
        .shutdown_live_agent(no_path_grandchild_thread_id)
        .await
        .expect("no-path grandchild shutdown should succeed");

    let mut worker_subtree_thread_ids = harness
        .manager
        .list_agent_subtree_thread_ids(worker_thread_id)
        .await
        .expect("worker subtree thread ids should load");
    worker_subtree_thread_ids.sort_by_key(ToString::to_string);
    let mut expected_worker_subtree_thread_ids = vec![
        worker_thread_id,
        worker_child_thread_id,
        no_path_child_thread_id,
        no_path_grandchild_thread_id,
    ];
    expected_worker_subtree_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(
        worker_subtree_thread_ids,
        expected_worker_subtree_thread_ids
    );

    let mut no_path_child_subtree_thread_ids = harness
        .manager
        .list_agent_subtree_thread_ids(no_path_child_thread_id)
        .await
        .expect("no-path subtree thread ids should load");
    no_path_child_subtree_thread_ids.sort_by_key(ToString::to_string);
    let mut expected_no_path_child_subtree_thread_ids =
        vec![no_path_child_thread_id, no_path_grandchild_thread_id];
    expected_no_path_child_subtree_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(
        no_path_child_subtree_thread_ids,
        expected_no_path_child_subtree_thread_ids
    );
}

#[tokio::test]
async fn list_agent_subtree_thread_ids_finds_live_descendants_of_unloaded_root() {
    let (_home, config) = test_config().await;
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        /*state_db*/ None,
    );
    let control = manager.agent_control();
    let parent_thread_id = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("parent should start")
        .thread_id;

    let child_thread_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = control
        .spawn_agent(
            config,
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    manager.remove_thread(&parent_thread_id).await;

    let mut subtree_thread_ids = manager
        .list_agent_subtree_thread_ids(parent_thread_id)
        .await
        .expect("live subtree should load");
    subtree_thread_ids.sort_by_key(ToString::to_string);
    let mut expected_subtree_thread_ids =
        vec![parent_thread_id, child_thread_id, grandchild_thread_id];
    expected_subtree_thread_ids.sort_by_key(ToString::to_string);

    assert_eq!(subtree_thread_ids, expected_subtree_thread_ids);
}

#[tokio::test]
async fn shutdown_agent_tree_closes_live_descendants() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown should succeed");

    assert_eq!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let shutdown_ids = harness
        .manager
        .captured_ops()
        .into_iter()
        .filter_map(|(thread_id, op)| matches!(op, Op::Shutdown).then_some(thread_id))
        .collect::<Vec<_>>();
    let mut expected_shutdown_ids = vec![parent_thread_id, child_thread_id, grandchild_thread_id];
    expected_shutdown_ids.sort_by_key(std::string::ToString::to_string);
    let mut shutdown_ids = shutdown_ids;
    shutdown_ids.sort_by_key(std::string::ToString::to_string);
    assert_eq!(shutdown_ids, expected_shutdown_ids);
}

#[tokio::test]
async fn shutdown_agent_tree_closes_descendants_when_started_at_child() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close should succeed");

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown should succeed");

    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );

    let shutdown_ids = harness
        .manager
        .captured_ops()
        .into_iter()
        .filter_map(|(thread_id, op)| matches!(op, Op::Shutdown).then_some(thread_id))
        .collect::<Vec<_>>();
    let mut expected_shutdown_ids = vec![parent_thread_id, child_thread_id, grandchild_thread_id];
    expected_shutdown_ids.sort_by_key(std::string::ToString::to_string);
    let mut shutdown_ids = shutdown_ids;
    shutdown_ids.sort_by_key(std::string::ToString::to_string);
    assert_eq!(shutdown_ids, expected_shutdown_ids);
}

#[tokio::test]
async fn resume_agent_from_rollout_does_not_reopen_closed_descendants() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close should succeed");
    let _ = harness
        .control
        .shutdown_live_agent(parent_thread_id)
        .await
        .expect("parent shutdown should succeed");

    let resumed_parent_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("single-thread resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after resume should succeed");
}

#[tokio::test]
async fn resume_closed_child_registers_open_descendants_as_cold() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close should succeed");

    let resumed_child_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            child_thread_id,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            }),
        )
        .await
        .expect("child resume should succeed");
    assert_eq!(resumed_child_thread_id, child_thread_id);
    assert_ne!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );
    assert!(
        harness
            .control
            .get_agent_metadata(grandchild_thread_id)
            .is_some()
    );

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close after resume should succeed");
    let _ = harness
        .control
        .shutdown_live_agent(parent_thread_id)
        .await
        .expect("parent shutdown should succeed");
}

#[tokio::test]
async fn resume_agent_from_rollout_registers_open_descendants_as_cold() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());

    let resumed_parent_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("tree resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_some()
    );
    assert!(
        harness
            .control
            .get_agent_metadata(grandchild_thread_id)
            .is_some()
    );

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after subtree resume should succeed");
}

#[tokio::test]
async fn resume_agent_from_rollout_uses_edge_data_when_descendant_metadata_source_is_stale() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let state_db = grandchild_thread
        .state_db()
        .expect("sqlite state db should be available");
    let mut stale_metadata = state_db
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild metadata query should succeed")
        .expect("grandchild metadata should exist");
    stale_metadata.source =
        serde_json::to_string(&SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 99,
            agent_path: None,
            agent_nickname: None,
            agent_role: Some("worker".to_string()),
        }))
        .expect("stale session source should serialize");
    state_db
        .upsert_thread(&stale_metadata)
        .await
        .expect("stale grandchild metadata should persist");

    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());

    let resumed_manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        harness.config.model_provider.clone(),
        harness.config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        harness.state_db.clone(),
    );
    let resumed_control = resumed_manager.agent_control();
    let resumed_parent_thread_id = resumed_control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("tree resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        resumed_control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        resumed_control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        resumed_control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );
    let repaired_grandchild = state_db
        .get_thread(grandchild_thread_id)
        .await
        .expect("repaired grandchild metadata query should succeed")
        .expect("repaired grandchild metadata should exist");
    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: repaired_parent_thread_id,
        depth: repaired_depth,
        agent_role: repaired_role,
        ..
    }) = serde_json::from_str::<SessionSource>(&repaired_grandchild.source)
        .expect("repaired session source should deserialize")
    else {
        panic!("expected repaired thread-spawn sub-agent source");
    };
    assert_eq!(repaired_parent_thread_id, child_thread_id);
    assert_eq!(repaired_depth, 2);
    assert_eq!(repaired_role.as_deref(), Some("worker"));
    assert_eq!(
        repaired_grandchild.thread_source,
        Some(codex_protocol::protocol::ThreadSource::Subagent)
    );

    resumed_control
        .deliver_inter_agent_communication_to_agent(
            harness.config.clone(),
            grandchild_thread_id,
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                "reload grandchild".to_string(),
                /*trigger_turn*/ false,
            ),
            AgentCommunicationContext::new(AgentCommunicationKind::Followup, parent_thread_id),
            AgentInputDelivery::Queue,
            /*parent_turn_id*/ None,
        )
        .await
        .expect("cold grandchild should reload");

    let resumed_grandchild_snapshot = resumed_manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("resumed grandchild thread should exist")
        .config_snapshot()
        .await;
    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: resumed_parent_thread_id,
        depth: resumed_depth,
        ..
    }) = resumed_grandchild_snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(resumed_parent_thread_id, child_thread_id);
    assert_eq!(resumed_depth, 2);

    let _ = resumed_control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after subtree resume should succeed");
}

#[tokio::test]
async fn resume_agent_from_rollout_skips_descendants_when_parent_resume_fails() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let child_rollout_path = child_thread
        .rollout_path()
        .expect("child thread should have rollout path");
    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());
    tokio::fs::remove_file(&child_rollout_path)
        .await
        .expect("child rollout path should be removable");

    let resumed_parent_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("root resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after partial subtree resume should succeed");
}

#[path = "control/legacy_residency_tests.rs"]
mod legacy_residency_tests;
