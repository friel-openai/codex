use crate::agent::AgentStatus;
use crate::agent::RemovedWatchdog;
use crate::agent::WatchdogManager;
use crate::agent::WatchdogRegistration;
use crate::agent::registry::AgentMetadata;
use crate::agent::registry::AgentRegistry;
use crate::agent::registry::is_watchdog_agent_metadata;
use crate::agent::role::DEFAULT_ROLE_NAME;
use crate::agent::role::resolve_role_config;
use crate::agent::status::is_final;
use crate::agent::watchdog::final_message_requests_watchdog_close;
use crate::codex_thread::ThreadConfigSnapshot;
use crate::find_thread_path_by_id_str;
use crate::goal_supervisor::is_goal_supervisor_helper_source;
use crate::inherited_thread_state::InheritedThreadState;
use crate::rollout::RolloutRecorder;
use crate::session::emit_subagent_session_started;
use crate::session_prefix::format_subagent_context_line;
use crate::session_prefix::format_subagent_notification_message;
use crate::shell_snapshot::ShellSnapshot;
use crate::state::McpToolSnapshot;
use crate::thread_manager::ResumeThreadWithHistoryOptions;
use crate::thread_manager::ThreadManagerState;
use crate::thread_rollout_truncation::truncate_rollout_to_last_n_fork_turns;
use crate::tools::handlers::multi_agents_spec::create_compact_parent_context_tool;
use crate::tools::handlers::multi_agents_spec::create_watchdog_close_self_tool;
use crate::tools::handlers::multi_agents_spec::create_watchdog_snooze_tool;
use crate::tools::handlers::multi_agents_spec::create_watchdog_tools_namespace;
use crate::turn_timing::now_unix_timestamp_ms;
use codex_features::Feature;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::CollabCloseEndEvent;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::ForkReferenceItem;
use codex_protocol::protocol::InitialHistory;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ResumedHistory;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::protocol::WarningEvent;
use codex_protocol::user_input::UserInput;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use codex_thread_store::ReadThreadParams;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;
#[cfg(test)]
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::watch;
use tracing::warn;

const AGENT_NAMES: &str = include_str!("agent_names.txt");
const ROOT_LAST_TASK_MESSAGE: &str = "Main thread";
const CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID_ENV: &str =
    "CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID";
const CODEX_EXPERIMENTAL_FORK_PARENT_PROMPT_CACHE_KEY_ENV: &str =
    "CODEX_EXPERIMENTAL_FORK_PARENT_PROMPT_CACHE_KEY";
const CODEX_EXPERIMENTAL_FORK_PROMPT_CACHE_KEY_ENV: &str =
    "CODEX_EXPERIMENTAL_FORK_PROMPT_CACHE_KEY";
const WATCHDOG_BOOT_TOOL_SEARCH_CALL_ID: &str = "synthetic_watchdog_tool_search";
const WATCHDOG_BOOT_LIST_AGENTS_CALL_ID: &str = "synthetic_watchdog_list_agents";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpawnAgentForkMode {
    FullHistory,
    LastNTurns(usize),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SpawnAgentOptions {
    pub(crate) fork_parent_spawn_call_id: Option<String>,
    pub(crate) fork_mode: Option<SpawnAgentForkMode>,
    pub(crate) environments: Option<Vec<TurnEnvironmentSelection>>,
    pub(crate) initial_task_message: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct LiveAgent {
    pub(crate) thread_id: ThreadId,
    pub(crate) metadata: AgentMetadata,
    pub(crate) status: AgentStatus,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct ListedAgent {
    pub(crate) agent_name: String,
    pub(crate) agent_status: AgentStatus,
    pub(crate) last_task_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WatchdogParentCompactionResult {
    NotWatchdogHelper,
    ParentBusy {
        parent_thread_id: ThreadId,
    },
    Submitted {
        parent_thread_id: ThreadId,
        submission_id: String,
    },
}

fn default_agent_nickname_list() -> Vec<&'static str> {
    AGENT_NAMES
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect()
}

fn agent_nickname_candidates(
    config: &crate::config::Config,
    role_name: Option<&str>,
) -> Vec<String> {
    let role_name = role_name.unwrap_or(DEFAULT_ROLE_NAME);
    if let Some(candidates) =
        resolve_role_config(config, role_name).and_then(|role| role.nickname_candidates.clone())
    {
        return candidates;
    }

    default_agent_nickname_list()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
}

fn keep_forked_rollout_item(item: &RolloutItem) -> bool {
    match item {
        RolloutItem::ResponseItem(ResponseItem::Message { role, phase, .. }) => match role.as_str()
        {
            "system" | "developer" | "user" => true,
            "assistant" => *phase == Some(MessagePhase::FinalAnswer),
            _ => false,
        },
        RolloutItem::ResponseItem(
            ResponseItem::Reasoning { .. }
            | ResponseItem::LocalShellCall { .. }
            | ResponseItem::FunctionCall { .. }
            | ResponseItem::ToolSearchCall { .. }
            | ResponseItem::FunctionCallOutput { .. }
            | ResponseItem::CustomToolCall { .. }
            | ResponseItem::CustomToolCallOutput { .. }
            | ResponseItem::ToolSearchOutput { .. }
            | ResponseItem::WebSearchCall { .. }
            | ResponseItem::ImageGenerationCall { .. }
            | ResponseItem::Compaction { .. }
            | ResponseItem::ContextCompaction { .. }
            | ResponseItem::Other,
        ) => false,
        // A forked child gets its own runtime config, including spawned-agent
        // instructions, so it must establish a fresh context diff baseline.
        RolloutItem::TurnContext(_) => false,
        RolloutItem::Compacted(_)
        | RolloutItem::EventMsg(_)
        | RolloutItem::ForkReference(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::SessionMeta(_) => true,
    }
}

fn full_history_fork_reference_items(
    rollout_path: PathBuf,
    source_items: &[RolloutItem],
) -> Vec<RolloutItem> {
    let source_meta = source_items.iter().find_map(|item| match item {
        RolloutItem::SessionMeta(meta) => Some(meta),
        RolloutItem::Compacted(_)
        | RolloutItem::EventMsg(_)
        | RolloutItem::ForkReference(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::ResponseItem(_)
        | RolloutItem::TurnContext(_) => None,
    });
    source_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::SessionMeta(meta) => Some(RolloutItem::SessionMeta(meta.clone())),
            RolloutItem::Compacted(_)
            | RolloutItem::EventMsg(_)
            | RolloutItem::ForkReference(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::ResponseItem(_)
            | RolloutItem::TurnContext(_) => None,
        })
        .into_iter()
        .chain(std::iter::once(RolloutItem::ForkReference(
            ForkReferenceItem {
                rollout_path,
                thread_id: source_meta.map(|meta| meta.meta.id),
                segment_id: source_meta.and_then(|meta| meta.meta.segment_id),
                nth_user_message: usize::MAX,
            },
        )))
        .collect()
}

fn is_watchdog_helper_source(session_source: &SessionSource) -> bool {
    matches!(
        session_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_role: Some(agent_role),
            ..
        }) if agent_role == "watchdog"
    )
}

fn is_internal_supervisor_helper_source(session_source: &SessionSource) -> bool {
    is_watchdog_helper_source(session_source) || is_goal_supervisor_helper_source(session_source)
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn synthetic_watchdog_tool_search_items() -> Vec<RolloutItem> {
    let namespace = create_watchdog_tools_namespace(vec![
        create_compact_parent_context_tool(),
        create_watchdog_close_self_tool(),
        create_watchdog_snooze_tool(),
    ]);
    let Ok(namespace) = serde_json::to_value(namespace) else {
        return Vec::new();
    };

    vec![
        RolloutItem::ResponseItem(ResponseItem::ToolSearchCall {
            id: None,
            call_id: Some(WATCHDOG_BOOT_TOOL_SEARCH_CALL_ID.to_string()),
            status: Some("completed".to_string()),
            execution: "client".to_string(),
            arguments: serde_json::json!({
                "query": "watchdog namespace tools",
            }),
        }),
        RolloutItem::ResponseItem(ResponseItem::ToolSearchOutput {
            call_id: Some(WATCHDOG_BOOT_TOOL_SEARCH_CALL_ID.to_string()),
            status: "completed".to_string(),
            execution: "client".to_string(),
            tools: vec![namespace],
        }),
    ]
}

fn synthetic_watchdog_list_agents_items(
    owner_thread_id: ThreadId,
    agents: Vec<ListedAgent>,
) -> Vec<RolloutItem> {
    let envelope = serde_json::json!({
        "source": "pre_injected_agents_list",
        "generated_at": unix_timestamp_seconds(),
        "owner_thread_id": owner_thread_id.to_string(),
        "agents": agents,
    });
    let mut output = FunctionCallOutputPayload::from_text(envelope.to_string());
    output.success = Some(true);

    vec![
        RolloutItem::ResponseItem(ResponseItem::FunctionCall {
            id: None,
            name: "list_agents".to_string(),
            namespace: None,
            arguments: "{}".to_string(),
            call_id: WATCHDOG_BOOT_LIST_AGENTS_CALL_ID.to_string(),
        }),
        RolloutItem::ResponseItem(ResponseItem::FunctionCallOutput {
            call_id: WATCHDOG_BOOT_LIST_AGENTS_CALL_ID.to_string(),
            output,
        }),
    ]
}

fn role_prompt_item(prompt: String) -> RolloutItem {
    RolloutItem::ResponseItem(ResponseItem::Message {
        id: None,
        role: "developer".to_string(),
        content: vec![ContentItem::InputText { text: prompt }],
        phase: None,
    })
}

fn subagent_assignment_item(session_source: &SessionSource, message: String) -> RolloutItem {
    let agent_path = session_source
        .get_agent_path()
        .map(String::from)
        .unwrap_or_else(|| "this subagent".to_string());
    role_prompt_item(format!(
        "# Subagent Assignment\n\nYou are `{agent_path}`. Your direct assignment from your parent agent is:\n\n{message}"
    ))
}

/// Control-plane handle for multi-agent operations.
/// `AgentControl` is held by each session (via `SessionServices`). It provides capability to
/// spawn new agents and the inter-agent communication layer.
/// An `AgentControl` instance is intended to be created at most once per root thread/session
/// tree. That same `AgentControl` is then shared with every sub-agent spawned from that root,
/// which keeps the registry scoped to that root thread rather than the entire `ThreadManager`.
#[derive(Clone)]
pub(crate) struct AgentControl {
    /// ID shared by the whole agent control session. This means every sub-agents from a common
    /// root share the same session ID.
    session_id: SessionId,
    /// Weak handle back to the global thread registry/state.
    /// This is `Weak` to avoid reference cycles and shadow persistence of the form
    /// `ThreadManagerState -> CodexThread -> Session -> SessionServices -> ThreadManagerState`.
    manager: Weak<ThreadManagerState>,
    state: Arc<AgentRegistry>,
    watchdogs: Option<Arc<WatchdogManager>>,
}

impl Default for AgentControl {
    fn default() -> Self {
        Self {
            session_id: SessionId::default(),
            manager: Weak::new(),
            state: Arc::new(AgentRegistry::default()),
            watchdogs: None,
        }
    }
}

impl AgentControl {
    /// Construct a new `AgentControl` that can spawn/message agents via the given manager state.
    pub(crate) fn new(manager: Weak<ThreadManagerState>) -> Self {
        let state = Arc::new(AgentRegistry::default());
        let watchdogs = WatchdogManager::new(manager.clone(), Arc::clone(&state));
        watchdogs.start();
        Self {
            session_id: SessionId::default(),
            manager,
            state,
            watchdogs: Some(watchdogs),
        }
    }

    pub(crate) fn from_parts(
        manager: Weak<ThreadManagerState>,
        state: Arc<AgentRegistry>,
        watchdogs: Arc<WatchdogManager>,
    ) -> Self {
        Self {
            session_id: SessionId::default(),
            manager,
            state,
            watchdogs: Some(watchdogs),
        }
    }

    pub(crate) fn with_session_id(mut self, session_id: SessionId) -> Self {
        self.session_id = session_id;
        self
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Spawn a new agent thread and submit the initial prompt.
    #[allow(dead_code)]
    pub(crate) async fn spawn_agent(
        &self,
        config: crate::config::Config,
        initial_operation: Op,
        session_source: Option<SessionSource>,
    ) -> CodexResult<ThreadId> {
        let spawned_agent = Box::pin(self.spawn_agent_internal(
            config,
            initial_operation,
            session_source,
            SpawnAgentOptions::default(),
        ))
        .await?;
        Ok(spawned_agent.thread_id)
    }

    /// Spawn an agent thread with some metadata.
    pub(crate) async fn spawn_agent_with_metadata(
        &self,
        config: crate::config::Config,
        initial_operation: Op,
        session_source: Option<SessionSource>,
        options: SpawnAgentOptions, // TODO(jif) drop with new fork.
    ) -> CodexResult<LiveAgent> {
        Box::pin(self.spawn_agent_internal(config, initial_operation, session_source, options))
            .await
    }

    async fn spawn_agent_internal(
        &self,
        config: crate::config::Config,
        initial_operation: Op,
        session_source: Option<SessionSource>,
        options: SpawnAgentOptions,
    ) -> CodexResult<LiveAgent> {
        let state = self.upgrade()?;
        let mut reservation = if session_source
            .as_ref()
            .is_some_and(is_internal_supervisor_helper_source)
        {
            self.state.reserve_uncounted_spawn_slot()
        } else {
            self.state.reserve_spawn_slot(config.agent_max_threads)?
        };
        let inherited_shell_snapshot = self
            .inherited_shell_snapshot_for_source(&state, session_source.as_ref())
            .await;
        let inherited_exec_policy = self
            .inherited_exec_policy_for_source(&state, session_source.as_ref(), &config)
            .await;
        let (session_source, mut agent_metadata) = match session_source {
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_role,
                ..
            })) => {
                let (session_source, agent_metadata) = self.prepare_thread_spawn(
                    &mut reservation,
                    &config,
                    parent_thread_id,
                    depth,
                    agent_path,
                    agent_role,
                    /*preferred_agent_nickname*/ None,
                )?;
                (Some(session_source), agent_metadata)
            }
            other => (other, AgentMetadata::default()),
        };
        let notification_source = session_source.clone();

        // The same `AgentControl` is sent to spawn the thread.
        let new_thread = match (session_source, options.fork_mode.as_ref()) {
            (Some(session_source), Some(_)) => {
                let inherited_thread_state = InheritedThreadState::builder()
                    .prompt_cache_key(
                        parent_prompt_cache_key_for_source(&state, Some(&session_source)).await,
                    )
                    .response_continuation(
                        parent_response_continuation_for_source(&state, Some(&session_source))
                            .await,
                    )
                    .mcp_tool_snapshot(
                        parent_mcp_tool_snapshot_for_source(&state, Some(&session_source)).await,
                    )
                    .build();
                self.spawn_forked_thread(
                    &state,
                    config,
                    session_source,
                    &options,
                    inherited_shell_snapshot,
                    inherited_exec_policy,
                    inherited_thread_state,
                )
                .await?
            }
            (Some(session_source), None) => {
                state
                    .spawn_new_thread_with_source(
                        config.clone(),
                        self.clone(),
                        session_source,
                        /*thread_source*/ Some(ThreadSource::Subagent),
                        /*persist_extended_history*/ false,
                        /*metrics_service_name*/ None,
                        inherited_shell_snapshot,
                        inherited_exec_policy,
                        options.environments.clone(),
                        Default::default(),
                    )
                    .await?
            }
            (None, _) => state.spawn_new_thread(config.clone(), self.clone()).await?,
        };
        agent_metadata.agent_id = Some(new_thread.thread_id);
        reservation.commit(agent_metadata.clone());

        if let Some(SessionSource::SubAgent(
            subagent_source @ SubAgentSource::ThreadSpawn {
                parent_thread_id, ..
            },
        )) = notification_source.as_ref()
        {
            let client_metadata = match state.get_thread(*parent_thread_id).await {
                Ok(parent_thread) => {
                    parent_thread
                        .codex
                        .session
                        .app_server_client_metadata()
                        .await
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        parent_thread_id = %parent_thread_id,
                        "skipping subagent thread analytics: failed to load parent thread metadata"
                    );
                    crate::session::session::AppServerClientMetadata {
                        client_name: None,
                        client_version: None,
                        mcp_elicitations_auto_deny: false,
                    }
                }
            };
            if let Err(error) = new_thread
                .thread
                .set_app_server_client_info(
                    client_metadata.client_name.clone(),
                    client_metadata.client_version.clone(),
                    client_metadata.mcp_elicitations_auto_deny,
                )
                .await
            {
                warn!(
                    error = %error,
                    child_thread_id = %new_thread.thread_id,
                    parent_thread_id = %parent_thread_id,
                    "failed to inherit app-server client metadata for spawned thread"
                );
            }
            let thread_config = new_thread.thread.codex.thread_config_snapshot().await;
            emit_subagent_session_started(
                &new_thread
                    .thread
                    .codex
                    .session
                    .services
                    .analytics_events_client,
                client_metadata,
                new_thread.thread_id,
                /*parent_thread_id*/ None,
                thread_config,
                subagent_source.clone(),
            );
        }

        // Notify a new thread has been created. This notification will be processed by clients
        // to subscribe or drain this newly created thread.
        // TODO(jif) add helper for drain
        state.notify_thread_created(new_thread.thread_id);

        self.persist_thread_spawn_edge_for_source(
            new_thread.thread.as_ref(),
            new_thread.thread_id,
            notification_source.as_ref(),
        )
        .await;

        Box::pin(self.send_input(new_thread.thread_id, initial_operation)).await?;
        let is_watchdog_helper = options.fork_mode.is_some()
            && notification_source
                .as_ref()
                .is_some_and(is_watchdog_helper_source);
        let is_goal_supervisor_helper = options.fork_mode.is_some()
            && notification_source
                .as_ref()
                .is_some_and(is_goal_supervisor_helper_source);
        // Pathless MultiAgentV2 children cannot emit routed inter-agent completion messages.
        // Keep the completion watcher so their parent still receives the fallback notification.
        let pathless_multi_agent_child = agent_metadata.agent_path.is_none();
        if (!is_watchdog_agent_metadata(&agent_metadata)
            && (!new_thread.thread.enabled(Feature::MultiAgentV2) || pathless_multi_agent_child))
            || is_watchdog_helper
            || is_goal_supervisor_helper
        {
            let child_reference = agent_metadata
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| new_thread.thread_id.to_string());
            self.maybe_start_completion_watcher(
                new_thread.thread_id,
                notification_source,
                child_reference,
                agent_metadata.agent_path.clone(),
            );
        }

        Ok(LiveAgent {
            thread_id: new_thread.thread_id,
            metadata: agent_metadata,
            status: self.get_status(new_thread.thread_id).await,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_forked_thread(
        &self,
        state: &Arc<ThreadManagerState>,
        config: crate::config::Config,
        session_source: SessionSource,
        options: &SpawnAgentOptions,
        inherited_shell_snapshot: Option<Arc<ShellSnapshot>>,
        inherited_exec_policy: Option<Arc<crate::exec_policy::ExecPolicyManager>>,
        inherited_thread_state: InheritedThreadState,
    ) -> CodexResult<crate::thread_manager::NewThread> {
        if options.fork_parent_spawn_call_id.is_none()
            && !is_internal_supervisor_helper_source(&session_source)
        {
            return Err(CodexErr::Fatal(
                "spawn_agent fork requires a parent spawn call id".to_string(),
            ));
        }
        let Some(fork_mode) = options.fork_mode.as_ref() else {
            return Err(CodexErr::Fatal(
                "spawn_agent fork requires a fork mode".to_string(),
            ));
        };
        let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        }) = &session_source
        else {
            return Err(CodexErr::Fatal(
                "spawn_agent fork requires a thread-spawn session source".to_string(),
            ));
        };

        let parent_thread_id = *parent_thread_id;
        let parent_thread = state.get_thread(parent_thread_id).await.ok();
        if let Some(parent_thread) = parent_thread.as_ref() {
            // `record_conversation_items` only queues persistence writes asynchronously.
            // Flush before snapshotting store history for a fork.
            parent_thread.ensure_rollout_materialized().await;
            parent_thread.flush_rollout().await?;
        }

        let rollout_path = parent_thread
            .as_ref()
            .and_then(|parent_thread| parent_thread.rollout_path())
            .or(find_thread_path_by_id_str(
                config.codex_home.as_path(),
                &parent_thread_id.to_string(),
                /*state_db_ctx*/ None,
            )
            .await?)
            .ok_or_else(|| {
                CodexErr::Fatal(format!(
                    "parent thread rollout unavailable for fork: {parent_thread_id}"
                ))
            })?;

        let response_continuation = inherited_thread_state.response_continuation();
        let use_response_continuation_baseline =
            response_continuation.is_some() && matches!(fork_mode, SpawnAgentForkMode::FullHistory);
        let source_items = RolloutRecorder::get_rollout_history(&rollout_path)
            .await?
            .get_rollout_items();
        let mut forked_rollout_items = if let (Some(response_continuation), true) =
            (&response_continuation, use_response_continuation_baseline)
        {
            previous_response_fork_rollout_items(
                source_items,
                response_continuation.fork_baseline_input(),
            )
        } else {
            match fork_mode {
                SpawnAgentForkMode::FullHistory => {
                    full_history_fork_reference_items(rollout_path.clone(), &source_items)
                }
                SpawnAgentForkMode::LastNTurns(last_n_turns) => {
                    truncate_rollout_to_last_n_fork_turns(&source_items, *last_n_turns)
                }
            }
        };
        if !use_response_continuation_baseline {
            // MultiAgentV2 root/subagent usage hints are injected as standalone developer
            // messages at thread start. When forking history, drop hints from the parent
            // so the child gets a fresh hint that matches its own session source/config.
            let multi_agent_v2_usage_hint_texts_to_filter: Vec<String> =
                if let Some(parent_thread) = parent_thread.as_ref() {
                    parent_thread
                        .codex
                        .session
                        .configured_multi_agent_v2_usage_hint_texts()
                        .await
                } else if config.features.enabled(Feature::MultiAgentV2) {
                    [
                        config.multi_agent_v2.root_agent_usage_hint_text.clone(),
                        config.multi_agent_v2.subagent_usage_hint_text.clone(),
                    ]
                    .into_iter()
                    .flatten()
                    .collect()
                } else {
                    Vec::new()
                };
            forked_rollout_items.retain(|item| {
                if let RolloutItem::ResponseItem(ResponseItem::Message { role, content, .. }) = item
                    && role == "developer"
                    && let [ContentItem::InputText { text }] = content.as_slice()
                    && multi_agent_v2_usage_hint_texts_to_filter
                        .iter()
                        .any(|usage_hint_text| usage_hint_text == text)
                {
                    return false;
                }

                keep_forked_rollout_item(item)
            });
        }
        if is_watchdog_helper_source(&session_source) {
            if let Some(role_prompt) =
                crate::session::load_agent_role_prompt(&config, &session_source).await
            {
                forked_rollout_items.push(role_prompt_item(role_prompt));
            }
            forked_rollout_items.extend(
                self.watchdog_boot_context_items(state, parent_thread_id)
                    .await,
            );
        } else if is_goal_supervisor_helper_source(&session_source) {
            if let Some(role_prompt) =
                crate::session::load_agent_role_prompt(&config, &session_source).await
            {
                forked_rollout_items.push(role_prompt_item(role_prompt));
            }
            forked_rollout_items.extend(
                self.supervisor_boot_context_items(state, parent_thread_id)
                    .await,
            );
        } else if options.fork_mode.is_some() {
            if let Some(role_prompt) =
                crate::session::load_agent_role_prompt(&config, &session_source).await
            {
                forked_rollout_items.push(role_prompt_item(role_prompt));
            }
            if let Some(initial_task_message) = options.initial_task_message.clone() {
                forked_rollout_items.push(subagent_assignment_item(
                    &session_source,
                    initial_task_message,
                ));
            }
        }

        state
            .fork_thread_with_source(
                config.clone(),
                InitialHistory::Forked(forked_rollout_items),
                self.clone(),
                session_source,
                /*thread_source*/ Some(ThreadSource::Subagent),
                /*persist_extended_history*/ false,
                inherited_shell_snapshot,
                inherited_exec_policy,
                options.environments.clone(),
                inherited_thread_state,
            )
            .await
    }

    /// Resume an existing agent thread from a recorded rollout file.
    pub(crate) async fn resume_agent_from_rollout(
        &self,
        config: crate::config::Config,
        thread_id: ThreadId,
        session_source: SessionSource,
    ) -> CodexResult<ThreadId> {
        let root_depth = thread_spawn_depth(&session_source).unwrap_or(0);
        let resumed_thread_id = Box::pin(self.resume_single_agent_from_rollout(
            config.clone(),
            thread_id,
            session_source,
        ))
        .await?;
        let state = self.upgrade()?;
        let Ok(resumed_thread) = state.get_thread(resumed_thread_id).await else {
            return Ok(resumed_thread_id);
        };
        let Some(state_db_ctx) = resumed_thread.state_db() else {
            return Ok(resumed_thread_id);
        };

        let mut resume_queue = VecDeque::from([(thread_id, root_depth)]);
        while let Some((parent_thread_id, parent_depth)) = resume_queue.pop_front() {
            let child_ids = match state_db_ctx
                .list_thread_spawn_children_with_status(
                    parent_thread_id,
                    DirectionalThreadSpawnEdgeStatus::Open,
                )
                .await
            {
                Ok(child_ids) => child_ids,
                Err(err) => {
                    warn!(
                        "failed to load persisted thread-spawn children for {parent_thread_id}: {err}"
                    );
                    continue;
                }
            };

            for child_thread_id in child_ids {
                let child_depth = parent_depth + 1;
                let child_resumed = if state.get_thread(child_thread_id).await.is_ok() {
                    true
                } else {
                    let child_session_source =
                        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                            parent_thread_id,
                            depth: child_depth,
                            agent_path: None,
                            agent_nickname: None,
                            agent_role: None,
                        });
                    match self
                        .resume_single_agent_from_rollout(
                            config.clone(),
                            child_thread_id,
                            child_session_source,
                        )
                        .await
                    {
                        Ok(_) => true,
                        Err(err) => {
                            warn!("failed to resume descendant thread {child_thread_id}: {err}");
                            false
                        }
                    }
                };
                if child_resumed {
                    resume_queue.push_back((child_thread_id, child_depth));
                }
            }
        }

        Ok(resumed_thread_id)
    }

    async fn resume_single_agent_from_rollout(
        &self,
        mut config: crate::config::Config,
        thread_id: ThreadId,
        session_source: SessionSource,
    ) -> CodexResult<ThreadId> {
        if let SessionSource::SubAgent(SubAgentSource::ThreadSpawn { depth, .. }) = &session_source
            && *depth >= config.agent_max_depth
            && !config.features.enabled(Feature::MultiAgentV2)
        {
            let _ = config.features.disable(Feature::SpawnCsv);
            let _ = config.features.disable(Feature::Collab);
        }
        let state = self.upgrade()?;
        let state_db_ctx = state.state_db();
        let mut reservation = if is_internal_supervisor_helper_source(&session_source) {
            self.state.reserve_uncounted_spawn_slot()
        } else {
            self.state.reserve_spawn_slot(config.agent_max_threads)?
        };
        let (session_source, agent_metadata) = match session_source {
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_role: _,
                agent_nickname: _,
            }) => {
                let (resumed_agent_nickname, resumed_agent_role) =
                    if let Some(state_db_ctx) = state_db_ctx.as_ref() {
                        match state_db_ctx.get_thread(thread_id).await {
                            Ok(Some(metadata)) => (metadata.agent_nickname, metadata.agent_role),
                            Ok(None) | Err(_) => (None, None),
                        }
                    } else {
                        (None, None)
                    };
                self.prepare_thread_spawn(
                    &mut reservation,
                    &config,
                    parent_thread_id,
                    depth,
                    agent_path,
                    resumed_agent_role,
                    resumed_agent_nickname,
                )?
            }
            other => (other, AgentMetadata::default()),
        };
        let notification_source = session_source.clone();
        let inherited_shell_snapshot = self
            .inherited_shell_snapshot_for_source(&state, Some(&session_source))
            .await;
        let inherited_exec_policy = self
            .inherited_exec_policy_for_source(&state, Some(&session_source), &config)
            .await;
        let stored_thread = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: true,
            })
            .await?;
        let history = stored_thread
            .history
            .ok_or_else(|| CodexErr::ThreadNotFound(thread_id))?
            .items;

        let resumed_thread = state
            .resume_thread_with_history_with_source(ResumeThreadWithHistoryOptions {
                config: config.clone(),
                initial_history: InitialHistory::Resumed(ResumedHistory {
                    conversation_id: thread_id,
                    history,
                    rollout_path: stored_thread.rollout_path,
                }),
                agent_control: self.clone(),
                session_source,
                inherited_shell_snapshot,
                inherited_exec_policy,
                inherited_thread_state: Default::default(),
            })
            .await?;
        let mut agent_metadata = agent_metadata;
        agent_metadata.agent_id = Some(resumed_thread.thread_id);
        reservation.commit(agent_metadata.clone());
        // Resumed threads are re-registered in-memory and need the same listener
        // attachment path as freshly spawned threads.
        state.notify_thread_created(resumed_thread.thread_id);
        // Pathless MultiAgentV2 children cannot emit routed inter-agent completion messages.
        // Keep the completion watcher so their parent still receives the fallback notification.
        let pathless_multi_agent_child = agent_metadata.agent_path.is_none();
        if !is_watchdog_agent_metadata(&agent_metadata)
            && (!resumed_thread.thread.enabled(Feature::MultiAgentV2) || pathless_multi_agent_child)
        {
            let child_reference = agent_metadata
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| resumed_thread.thread_id.to_string());
            self.maybe_start_completion_watcher(
                resumed_thread.thread_id,
                Some(notification_source.clone()),
                child_reference,
                agent_metadata.agent_path.clone(),
            );
        }
        self.persist_thread_spawn_edge_for_source(
            resumed_thread.thread.as_ref(),
            resumed_thread.thread_id,
            Some(&notification_source),
        )
        .await;

        Ok(resumed_thread.thread_id)
    }

    /// Send rich user input items to an existing agent thread.
    pub(crate) async fn send_input(
        &self,
        agent_id: ThreadId,
        initial_operation: Op,
    ) -> CodexResult<String> {
        let last_task_message = render_input_preview(&initial_operation);
        let state = self.upgrade()?;
        let result = self
            .handle_thread_request_result(
                agent_id,
                &state,
                state.send_op(agent_id, initial_operation).await,
            )
            .await;
        if result.is_ok() {
            self.state
                .update_last_task_message(agent_id, last_task_message);
        }
        result
    }

    /// Append a prebuilt message to an existing agent thread outside the normal user-input path.
    #[cfg(test)]
    pub(crate) async fn append_message(
        &self,
        agent_id: ThreadId,
        message: ResponseItem,
    ) -> CodexResult<String> {
        let state = self.upgrade()?;
        self.handle_thread_request_result(
            agent_id,
            &state,
            state.append_message(agent_id, message).await,
        )
        .await
    }

    pub(crate) async fn send_inter_agent_communication(
        &self,
        agent_id: ThreadId,
        communication: InterAgentCommunication,
    ) -> CodexResult<String> {
        let last_task_message = communication.content.clone();
        let state = self.upgrade()?;
        let result = self
            .handle_thread_request_result(
                agent_id,
                &state,
                state
                    .send_op(agent_id, Op::InterAgentCommunication { communication })
                    .await,
            )
            .await;
        if result.is_ok() {
            self.state
                .update_last_task_message(agent_id, last_task_message);
        }
        result
    }

    pub(crate) async fn send_watchdog_wakeup(
        &self,
        owner_thread_id: ThreadId,
        message: String,
    ) -> CodexResult<String> {
        let Some(message) = sanitize_watchdog_wakeup_message(message) else {
            return Ok(String::new());
        };
        let watchdog_path = AgentPath::root()
            .join("watchdog")
            .unwrap_or_else(|_| AgentPath::root());
        self.send_inter_agent_communication(
            owner_thread_id,
            InterAgentCommunication::new(
                watchdog_path,
                AgentPath::root(),
                Vec::new(),
                message,
                /*trigger_turn*/ true,
            ),
        )
        .await
    }

    /// Interrupt the current task for an existing agent thread.
    pub(crate) async fn interrupt_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let state = self.upgrade()?;
        state.send_op(agent_id, Op::Interrupt).await
    }

    async fn handle_thread_request_result(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        result: CodexResult<String>,
    ) -> CodexResult<String> {
        if matches!(result, Err(CodexErr::InternalAgentDied)) {
            let _ = state.remove_thread(&agent_id).await;
            self.state.release_spawned_thread(agent_id);
        }
        result
    }

    /// Submit a shutdown request for a live agent without marking it explicitly closed in
    /// persisted spawn-edge state.
    pub(crate) async fn shutdown_live_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let state = self.upgrade()?;
        let result = if let Ok(thread) = state.get_thread(agent_id).await {
            thread.codex.session.ensure_rollout_materialized().await;
            thread.codex.session.flush_rollout().await?;
            let result = if matches!(thread.agent_status().await, AgentStatus::Shutdown) {
                Ok(String::new())
            } else {
                state.send_op(agent_id, Op::Shutdown {}).await
            };
            thread.wait_until_terminated().await;
            result
        } else {
            state.send_op(agent_id, Op::Shutdown {}).await
        };
        let _ = state.remove_thread(&agent_id).await;
        self.state.release_spawned_thread(agent_id);
        result
    }

    pub(crate) async fn register_watchdog(
        &self,
        registration: WatchdogRegistration,
    ) -> CodexResult<Vec<RemovedWatchdog>> {
        self.watchdog_manager()?.register(registration).await
    }

    pub(crate) async fn unregister_watchdogs_for_owner(
        &self,
        owner_thread_id: ThreadId,
    ) -> Vec<RemovedWatchdog> {
        let Some(watchdogs) = self.watchdogs.as_ref() else {
            return Vec::new();
        };
        watchdogs.unregister_for_owner(owner_thread_id).await
    }

    pub(crate) async fn unregister_watchdog_handle(
        &self,
        target_thread_id: ThreadId,
    ) -> Option<RemovedWatchdog> {
        let watchdogs = self.watchdogs.as_ref()?;
        watchdogs.unregister_handle(target_thread_id).await
    }

    pub(crate) async fn is_watchdog_handle(&self, target_thread_id: ThreadId) -> bool {
        let Some(watchdogs) = self.watchdogs.as_ref() else {
            return false;
        };
        watchdogs.is_watchdog_handle(target_thread_id).await
    }

    #[cfg(test)]
    pub(crate) async fn set_watchdog_active_helper_for_tests(
        &self,
        target_thread_id: ThreadId,
        helper_thread_id: ThreadId,
    ) {
        if let Some(watchdogs) = self.watchdogs.as_ref() {
            watchdogs
                .set_active_helper_for_tests(target_thread_id, helper_thread_id)
                .await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn backdate_watchdog_active_helper_for_tests(
        &self,
        target_thread_id: ThreadId,
        helper_thread_id: ThreadId,
        age: Duration,
    ) {
        if let Some(watchdogs) = self.watchdogs.as_ref() {
            watchdogs
                .backdate_active_helper_for_tests(target_thread_id, helper_thread_id, age)
                .await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn watchdog_helper_is_suppressed_for_tests(
        &self,
        helper_thread_id: ThreadId,
    ) -> bool {
        let Some(watchdogs) = self.watchdogs.as_ref() else {
            return false;
        };
        watchdogs
            .helper_is_suppressed_for_tests(helper_thread_id)
            .await
    }

    pub(crate) async fn watchdog_owner_for_active_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> Option<ThreadId> {
        let watchdogs = self.watchdogs.as_ref()?;
        watchdogs.owner_for_active_helper(helper_thread_id).await
    }

    pub(crate) async fn watchdog_target_for_active_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> Option<ThreadId> {
        let watchdogs = self.watchdogs.as_ref()?;
        watchdogs.target_for_active_helper(helper_thread_id).await
    }

    pub(crate) async fn snooze_watchdog_helper(
        &self,
        helper_thread_id: ThreadId,
        delay_seconds: Option<u64>,
    ) -> Option<crate::agent::watchdog::WatchdogSnoozeResult> {
        let watchdogs = self.watchdogs.as_ref()?;
        watchdogs
            .snooze_active_helper(helper_thread_id, delay_seconds)
            .await
    }

    pub(crate) async fn finish_watchdog_helper(&self, helper_thread_id: ThreadId) -> bool {
        let Some(watchdogs) = self.watchdogs.as_ref() else {
            return false;
        };
        watchdogs.finish_active_helper(helper_thread_id).await
    }

    pub(crate) async fn finish_watchdog_helper_thread(
        &self,
        agent_id: ThreadId,
    ) -> CodexResult<()> {
        let state = self.upgrade()?;
        if let Ok(thread) = state.get_thread(agent_id).await {
            if let Some(state_db_ctx) = thread.state_db()
                && let Err(err) = state_db_ctx
                    .set_thread_spawn_edge_status(
                        agent_id,
                        DirectionalThreadSpawnEdgeStatus::Closed,
                    )
                    .await
            {
                warn!("failed to persist thread-spawn edge status for {agent_id}: {err}");
            }
            thread.codex.session.flush_rollout().await?;
        }
        let _ = state.remove_thread(&agent_id).await;
        self.state.release_spawned_thread(agent_id);
        Ok(())
    }

    pub(crate) async fn finalize_watchdog_helper(
        &self,
        helper_thread_id: ThreadId,
        helper_status: AgentStatus,
    ) -> bool {
        let Some(watchdogs) = self.watchdogs.as_ref() else {
            return false;
        };
        let Some(owner_thread_id) = watchdogs.owner_for_active_helper(helper_thread_id).await
        else {
            return false;
        };
        let target_thread_id = watchdogs.target_for_active_helper(helper_thread_id).await;
        let helper_suppressed = watchdogs.take_suppressed_helper(helper_thread_id).await;
        let mut close_watchdog_handle = false;
        if let AgentStatus::Completed(Some(message)) = helper_status
            && !helper_suppressed
        {
            close_watchdog_handle = final_message_requests_watchdog_close(&message);
            if let Err(err) = self.send_watchdog_wakeup(owner_thread_id, message).await {
                warn!(
                    helper_thread_id = %helper_thread_id,
                    owner_thread_id = %owner_thread_id,
                    "watchdog helper forward failed: {err}"
                );
            }
        }

        let _ = self.shutdown_live_agent(helper_thread_id).await;
        if close_watchdog_handle {
            if let Some(target_thread_id) = target_thread_id {
                let _ = self.unregister_watchdog_handle(target_thread_id).await;
                let _ = self.shutdown_live_agent(target_thread_id).await;
            }
        } else {
            let _ = self.finish_watchdog_helper(helper_thread_id).await;
        }
        true
    }

    pub(crate) async fn goal_supervisor_parent_for_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> Option<ThreadId> {
        let state = self.upgrade().ok()?;
        let helper_thread = state.get_thread(helper_thread_id).await.ok()?;
        let snapshot = helper_thread.codex.thread_config_snapshot().await;
        let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            agent_role: Some(agent_role),
            ..
        }) = snapshot.session_source
        else {
            return None;
        };
        (agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
            .then_some(parent_thread_id)
    }

    pub(crate) async fn finish_goal_supervisor_helper(&self, helper_thread_id: ThreadId) -> bool {
        let Some(parent_thread_id) = self
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await
        else {
            return false;
        };
        let Ok(state) = self.upgrade() else {
            return false;
        };
        let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
            return false;
        };
        match crate::goal_supervisor::finish_supervisor_helper(
            &parent_thread.codex.session,
            helper_thread_id,
        )
        .await
        {
            Ok(finished) => {
                if finished
                    && let Err(err) = parent_thread
                        .codex
                        .session
                        .goal_runtime_apply(crate::goals::GoalRuntimeEvent::MaybeContinueIfIdle)
                        .await
                {
                    warn!("failed to reschedule goal supervisor after helper finished: {err}");
                }
                finished
            }
            Err(err) => {
                warn!("failed to finish goal supervisor helper {helper_thread_id}: {err}");
                false
            }
        }
    }

    pub(crate) async fn snooze_goal_supervisor_helper(
        &self,
        helper_thread_id: ThreadId,
        delay_seconds: Option<u64>,
    ) -> Option<u64> {
        let parent_thread_id = self
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await?;
        let state = self.upgrade().ok()?;
        let parent_thread = state.get_thread(parent_thread_id).await.ok()?;
        match crate::goal_supervisor::snooze_supervisor_helper(
            &parent_thread.codex.session,
            helper_thread_id,
            delay_seconds,
        )
        .await
        {
            Ok(delay_seconds) => delay_seconds,
            Err(err) => {
                warn!("failed to snooze goal supervisor helper {helper_thread_id}: {err}");
                None
            }
        }
    }

    fn watchdog_manager(&self) -> CodexResult<&Arc<WatchdogManager>> {
        self.watchdogs.as_ref().ok_or_else(|| {
            CodexErr::UnsupportedOperation("watchdog manager unavailable".to_string())
        })
    }

    /// Mark `agent_id` as explicitly closed in persisted spawn-edge state, then shut down the
    /// agent and any live descendants reached from the in-memory tree.
    pub(crate) async fn close_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let state = self.upgrade()?;
        if let Some(removed_watchdog) = self.unregister_watchdog_handle(agent_id).await
            && let Some(helper_id) = removed_watchdog.active_helper_id
        {
            let _ = self.shutdown_live_agent(helper_id).await;
        }
        if let Ok(thread) = state.get_thread(agent_id).await
            && let Some(state_db_ctx) = thread.state_db()
            && let Err(err) = state_db_ctx
                .set_thread_spawn_edge_status(agent_id, DirectionalThreadSpawnEdgeStatus::Closed)
                .await
        {
            warn!("failed to persist thread-spawn edge status for {agent_id}: {err}");
        }
        Box::pin(self.shutdown_agent_tree(agent_id)).await
    }

    /// Shut down `agent_id` and any live descendants reachable from the in-memory spawn tree.
    async fn shutdown_agent_tree(&self, agent_id: ThreadId) -> CodexResult<String> {
        let descendant_ids = self.live_thread_spawn_descendants(agent_id).await?;
        let result = self.shutdown_live_agent(agent_id).await;
        for descendant_id in descendant_ids {
            match self.shutdown_live_agent(descendant_id).await {
                Ok(_) | Err(CodexErr::ThreadNotFound(_)) | Err(CodexErr::InternalAgentDied) => {}
                Err(err) => return Err(err),
            }
        }
        result
    }

    /// Fetch the last known status for `agent_id`, returning `NotFound` when unavailable.
    pub(crate) async fn get_status(&self, agent_id: ThreadId) -> AgentStatus {
        let Ok(state) = self.upgrade() else {
            // No agent available if upgrade fails.
            return AgentStatus::NotFound;
        };
        let Ok(thread) = state.get_thread(agent_id).await else {
            return AgentStatus::NotFound;
        };
        self.reported_agent_status(agent_id, thread.agent_status().await)
            .await
    }

    pub(crate) async fn has_running_user_visible_descendant(
        &self,
        parent_thread_id: ThreadId,
    ) -> CodexResult<bool> {
        let state = self.upgrade()?;
        let mut children_by_parent = self.live_thread_spawn_children().await?;
        let mut stack = children_by_parent
            .remove(&parent_thread_id)
            .unwrap_or_default()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();

        while let Some((thread_id, metadata)) = stack.pop() {
            if !is_internal_supervisor_role(metadata.agent_role.as_deref()) {
                let thread = state.get_thread(thread_id).await?;
                if matches!(
                    thread.agent_status().await,
                    AgentStatus::PendingInit | AgentStatus::Running | AgentStatus::Interrupted
                ) {
                    return Ok(true);
                }
            }
            if let Some(children) = children_by_parent.remove(&thread_id) {
                stack.extend(children.into_iter().rev());
            }
        }

        Ok(false)
    }

    pub(crate) fn register_session_root(
        &self,
        current_thread_id: ThreadId,
        current_session_source: &SessionSource,
    ) {
        if thread_spawn_parent_thread_id(current_session_source).is_none() {
            self.state.register_root_thread(current_thread_id);
        }
    }

    pub(crate) fn get_agent_metadata(&self, agent_id: ThreadId) -> Option<AgentMetadata> {
        self.state.agent_metadata_for_thread(agent_id)
    }

    pub(crate) async fn list_live_agent_subtree_thread_ids(
        &self,
        agent_id: ThreadId,
    ) -> CodexResult<Vec<ThreadId>> {
        let mut thread_ids = vec![agent_id];
        thread_ids.extend(self.live_thread_spawn_descendants(agent_id).await?);
        Ok(thread_ids)
    }

    pub(crate) async fn get_agent_config_snapshot(
        &self,
        agent_id: ThreadId,
    ) -> Option<ThreadConfigSnapshot> {
        let Ok(state) = self.upgrade() else {
            return None;
        };
        let Ok(thread) = state.get_thread(agent_id).await else {
            return None;
        };
        Some(thread.config_snapshot().await)
    }

    pub(crate) async fn resolve_agent_reference(
        &self,
        _current_thread_id: ThreadId,
        current_session_source: &SessionSource,
        agent_reference: &str,
    ) -> CodexResult<ThreadId> {
        let current_agent_path = current_session_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root);
        let agent_path = current_agent_path
            .resolve(agent_reference)
            .map_err(CodexErr::UnsupportedOperation)?;
        if let Some(thread_id) = self.state.agent_id_for_path(&agent_path) {
            return Ok(thread_id);
        }
        Err(CodexErr::UnsupportedOperation(format!(
            "live agent path `{}` not found",
            agent_path.as_str()
        )))
    }

    /// Subscribe to status updates for `agent_id`, yielding the latest value and changes.
    pub(crate) async fn subscribe_status(
        &self,
        agent_id: ThreadId,
    ) -> CodexResult<watch::Receiver<AgentStatus>> {
        let state = self.upgrade()?;
        let thread = state.get_thread(agent_id).await?;
        Ok(thread.subscribe_status())
    }

    pub(crate) async fn format_environment_context_subagents(
        &self,
        parent_thread_id: ThreadId,
    ) -> String {
        let Ok(agents) = self.open_thread_spawn_children(parent_thread_id).await else {
            return String::new();
        };

        agents
            .into_iter()
            .map(|(thread_id, metadata)| {
                let reference = metadata
                    .agent_path
                    .as_ref()
                    .map(|agent_path| agent_path.name().to_string())
                    .unwrap_or_else(|| thread_id.to_string());
                format_subagent_context_line(reference.as_str(), metadata.agent_nickname.as_deref())
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(crate) async fn list_agents(
        &self,
        current_session_source: &SessionSource,
        path_prefix: Option<&str>,
    ) -> CodexResult<Vec<ListedAgent>> {
        let state = self.upgrade()?;
        let resolved_prefix = path_prefix
            .map(|prefix| {
                current_session_source
                    .get_agent_path()
                    .unwrap_or_else(AgentPath::root)
                    .resolve(prefix)
                    .map_err(CodexErr::UnsupportedOperation)
            })
            .transpose()?;

        let mut live_agents = self.state.live_agents();
        live_agents.sort_by(|left, right| {
            left.agent_path
                .as_deref()
                .unwrap_or_default()
                .cmp(right.agent_path.as_deref().unwrap_or_default())
                .then_with(|| {
                    left.agent_id
                        .map(|id| id.to_string())
                        .unwrap_or_default()
                        .cmp(&right.agent_id.map(|id| id.to_string()).unwrap_or_default())
                })
        });

        let root_path = AgentPath::root();
        let mut agents = Vec::with_capacity(live_agents.len().saturating_add(1));
        if resolved_prefix
            .as_ref()
            .is_none_or(|prefix| agent_matches_prefix(Some(&root_path), prefix))
            && let Some(root_thread_id) = self.state.agent_id_for_path(&root_path)
            && let Ok(root_thread) = state.get_thread(root_thread_id).await
        {
            agents.push(ListedAgent {
                agent_name: root_path.to_string(),
                agent_status: self
                    .reported_agent_status(root_thread_id, root_thread.agent_status().await)
                    .await,
                last_task_message: Some(ROOT_LAST_TASK_MESSAGE.to_string()),
            });
        }

        for metadata in live_agents {
            let Some(thread_id) = metadata.agent_id else {
                continue;
            };
            if let Some(watchdogs) = self.watchdogs.as_ref()
                && watchdogs
                    .target_for_active_helper(thread_id)
                    .await
                    .is_some()
            {
                continue;
            }
            if is_internal_supervisor_role(metadata.agent_role.as_deref()) {
                continue;
            }
            if resolved_prefix
                .as_ref()
                .is_some_and(|prefix| !agent_matches_prefix(metadata.agent_path.as_ref(), prefix))
            {
                continue;
            }

            let Ok(thread) = state.get_thread(thread_id).await else {
                continue;
            };
            let agent_name = metadata
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| thread_id.to_string());
            let last_task_message = metadata.last_task_message.clone();
            agents.push(ListedAgent {
                agent_name,
                agent_status: self
                    .reported_agent_status(thread_id, thread.agent_status().await)
                    .await,
                last_task_message,
            });
        }

        Ok(agents)
    }

    async fn reported_agent_status(&self, agent_id: ThreadId, status: AgentStatus) -> AgentStatus {
        if matches!(status, AgentStatus::PendingInit)
            && let Some(watchdogs) = self.watchdogs.as_ref()
            && watchdogs.is_watchdog_handle(agent_id).await
        {
            return AgentStatus::Running;
        }
        status
    }

    pub(crate) async fn send_watchdog_close_event(
        &self,
        owner_thread_id: ThreadId,
        target_thread_id: ThreadId,
        receiver_agent_nickname: Option<String>,
        receiver_agent_role: Option<String>,
        status: AgentStatus,
    ) -> CodexResult<()> {
        let state = self.upgrade()?;
        let owner_thread = state.get_thread(owner_thread_id).await?;
        owner_thread
            .codex
            .session
            .send_event_raw(Event {
                id: format!("watchdog-close-{target_thread_id}"),
                msg: CollabCloseEndEvent {
                    call_id: format!("watchdog-close-{target_thread_id}"),
                    completed_at_ms: now_unix_timestamp_ms(),
                    sender_thread_id: owner_thread_id,
                    receiver_thread_id: target_thread_id,
                    receiver_agent_nickname,
                    receiver_agent_role,
                    status,
                }
                .into(),
            })
            .await;
        Ok(())
    }

    pub(crate) async fn send_watchdog_snooze_event(
        &self,
        owner_thread_id: ThreadId,
        target_thread_id: ThreadId,
        delay_seconds: u64,
    ) -> CodexResult<()> {
        let state = self.upgrade()?;
        let owner_thread = state.get_thread(owner_thread_id).await?;
        owner_thread
            .codex
            .session
            .send_event_raw(Event {
                id: format!("watchdog-snooze-{target_thread_id}-{}", ThreadId::new()),
                msg: codex_protocol::protocol::EventMsg::Warning(WarningEvent {
                    message: format!(
                        "Watchdog snoozed for {}.",
                        format_watchdog_snooze_duration(delay_seconds)
                    ),
                }),
            })
            .await;
        Ok(())
    }

    pub(crate) async fn compact_parent_for_watchdog_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> CodexResult<WatchdogParentCompactionResult> {
        let Some(watchdogs) = self.watchdogs.as_ref() else {
            return Ok(WatchdogParentCompactionResult::NotWatchdogHelper);
        };
        let Some(parent_thread_id) = watchdogs.owner_for_active_helper(helper_thread_id).await
        else {
            return Ok(WatchdogParentCompactionResult::NotWatchdogHelper);
        };
        let state = self.upgrade()?;
        let parent_thread = state.get_thread(parent_thread_id).await?;
        if parent_thread
            .codex
            .session
            .active_turn
            .lock()
            .await
            .is_some()
        {
            return Ok(WatchdogParentCompactionResult::ParentBusy { parent_thread_id });
        }

        state
            .send_op(parent_thread_id, Op::Compact)
            .await
            .map(|submission_id| WatchdogParentCompactionResult::Submitted {
                parent_thread_id,
                submission_id,
            })
    }

    pub(crate) async fn compact_parent_for_goal_supervisor_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> CodexResult<WatchdogParentCompactionResult> {
        let Some(parent_thread_id) = self
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await
        else {
            return Ok(WatchdogParentCompactionResult::NotWatchdogHelper);
        };
        let state = self.upgrade()?;
        let parent_thread = state.get_thread(parent_thread_id).await?;
        if parent_thread
            .codex
            .session
            .active_turn
            .lock()
            .await
            .is_some()
        {
            return Ok(WatchdogParentCompactionResult::ParentBusy { parent_thread_id });
        }

        state
            .send_op(parent_thread_id, Op::Compact)
            .await
            .map(|submission_id| WatchdogParentCompactionResult::Submitted {
                parent_thread_id,
                submission_id,
            })
    }

    async fn watchdog_boot_context_items(
        &self,
        state: &Arc<ThreadManagerState>,
        owner_thread_id: ThreadId,
    ) -> Vec<RolloutItem> {
        let owner_source = match state.get_thread(owner_thread_id).await {
            Ok(owner_thread) => {
                owner_thread
                    .codex
                    .thread_config_snapshot()
                    .await
                    .session_source
            }
            Err(_) => SessionSource::Cli,
        };
        self.register_session_root(owner_thread_id, &owner_source);
        let agents = self
            .list_agents(&owner_source, /*path_prefix*/ None)
            .await
            .unwrap_or_default();

        synthetic_watchdog_tool_search_items()
            .into_iter()
            .chain(synthetic_watchdog_list_agents_items(
                owner_thread_id,
                agents,
            ))
            .collect()
    }

    async fn supervisor_boot_context_items(
        &self,
        state: &Arc<ThreadManagerState>,
        owner_thread_id: ThreadId,
    ) -> Vec<RolloutItem> {
        let owner_source = match state.get_thread(owner_thread_id).await {
            Ok(owner_thread) => {
                owner_thread
                    .codex
                    .thread_config_snapshot()
                    .await
                    .session_source
            }
            Err(_) => SessionSource::Cli,
        };
        self.register_session_root(owner_thread_id, &owner_source);
        let agents = self
            .list_agents(&owner_source, /*path_prefix*/ None)
            .await
            .unwrap_or_default();

        synthetic_watchdog_list_agents_items(owner_thread_id, agents)
    }

    /// Starts a detached watcher for sub-agents spawned from another thread.
    ///
    /// This is only enabled for `SubAgentSource::ThreadSpawn`, where a parent thread exists and
    /// can receive completion notifications.
    fn maybe_start_completion_watcher(
        &self,
        child_thread_id: ThreadId,
        session_source: Option<SessionSource>,
        _child_reference: String,
        child_agent_path: Option<AgentPath>,
    ) {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return;
        };
        let control = self.clone();
        tokio::spawn(async move {
            let status = match control.subscribe_status(child_thread_id).await {
                Ok(mut status_rx) => {
                    let mut status = status_rx.borrow().clone();
                    while !is_final(&status) {
                        if status_rx.changed().await.is_err() {
                            status = control.get_status(child_thread_id).await;
                            break;
                        }
                        status = status_rx.borrow().clone();
                    }
                    status
                }
                Err(_) => control.get_status(child_thread_id).await,
            };
            if !is_final(&status) {
                return;
            }
            if control
                .finalize_watchdog_helper(child_thread_id, status.clone())
                .await
            {
                return;
            }
            if control.finish_goal_supervisor_helper(child_thread_id).await {
                return;
            }

            let Ok(state) = control.upgrade() else {
                return;
            };
            let child_thread = state.get_thread(child_thread_id).await.ok();
            // The TUI indexes live subagent rows by ThreadId. Use the child ThreadId in this
            // hidden notification so final status updates remove the correct panel row.
            let message =
                format_subagent_notification_message(&child_thread_id.to_string(), &status);
            if let Some(child_agent_path) = child_agent_path.clone()
                && child_thread
                    .as_ref()
                    .map(|thread| thread.enabled(Feature::MultiAgentV2))
                    .unwrap_or(true)
            {
                let Some(parent_agent_path) = child_agent_path
                    .as_str()
                    .rsplit_once('/')
                    .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
                else {
                    return;
                };
                let communication = InterAgentCommunication::new(
                    child_agent_path,
                    parent_agent_path,
                    Vec::new(),
                    message,
                    /*trigger_turn*/ false,
                );
                let _ = control
                    .send_inter_agent_communication(parent_thread_id, communication)
                    .await;
                return;
            }
            let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
                return;
            };
            parent_thread
                .inject_user_message_without_turn(message)
                .await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_thread_spawn(
        &self,
        reservation: &mut crate::agent::registry::SpawnReservation,
        config: &crate::config::Config,
        parent_thread_id: ThreadId,
        depth: i32,
        agent_path: Option<AgentPath>,
        agent_role: Option<String>,
        preferred_agent_nickname: Option<String>,
    ) -> CodexResult<(SessionSource, AgentMetadata)> {
        if depth == 1 {
            self.state.register_root_thread(parent_thread_id);
        }
        if let Some(agent_path) = agent_path.as_ref() {
            reservation.reserve_agent_path(agent_path)?;
        }
        let candidate_names = agent_nickname_candidates(config, agent_role.as_deref());
        let candidate_name_refs: Vec<&str> = candidate_names.iter().map(String::as_str).collect();
        let agent_nickname = Some(reservation.reserve_agent_nickname_with_preference(
            &candidate_name_refs,
            preferred_agent_nickname.as_deref(),
        )?);
        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth,
            agent_path: agent_path.clone(),
            agent_nickname: agent_nickname.clone(),
            agent_role: agent_role.clone(),
        });
        let agent_metadata = AgentMetadata {
            agent_id: None,
            agent_path,
            agent_nickname,
            agent_role,
            last_task_message: None,
        };
        Ok((session_source, agent_metadata))
    }

    fn upgrade(&self) -> CodexResult<Arc<ThreadManagerState>> {
        self.manager
            .upgrade()
            .ok_or_else(|| CodexErr::UnsupportedOperation("thread manager dropped".to_string()))
    }

    pub(crate) fn upgrade_for_tools(&self) -> CodexResult<Arc<ThreadManagerState>> {
        self.upgrade()
    }

    async fn inherited_shell_snapshot_for_source(
        &self,
        state: &Arc<ThreadManagerState>,
        session_source: Option<&SessionSource>,
    ) -> Option<Arc<ShellSnapshot>> {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return None;
        };

        let parent_thread = state.get_thread(*parent_thread_id).await.ok()?;
        parent_thread.codex.session.user_shell().shell_snapshot()
    }

    async fn inherited_exec_policy_for_source(
        &self,
        state: &Arc<ThreadManagerState>,
        session_source: Option<&SessionSource>,
        child_config: &crate::config::Config,
    ) -> Option<Arc<crate::exec_policy::ExecPolicyManager>> {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return None;
        };

        let parent_thread = state.get_thread(*parent_thread_id).await.ok()?;
        let parent_config = parent_thread.codex.session.get_config().await;
        if !crate::exec_policy::child_uses_parent_exec_policy(&parent_config, child_config) {
            return None;
        }

        Some(Arc::clone(
            &parent_thread.codex.session.services.exec_policy,
        ))
    }

    async fn open_thread_spawn_children(
        &self,
        parent_thread_id: ThreadId,
    ) -> CodexResult<Vec<(ThreadId, AgentMetadata)>> {
        let mut children_by_parent = self.live_thread_spawn_children().await?;
        Ok(children_by_parent
            .remove(&parent_thread_id)
            .unwrap_or_default())
    }

    async fn live_thread_spawn_children(
        &self,
    ) -> CodexResult<HashMap<ThreadId, Vec<(ThreadId, AgentMetadata)>>> {
        let state = self.upgrade()?;
        let mut children_by_parent = HashMap::<ThreadId, Vec<(ThreadId, AgentMetadata)>>::new();

        for thread_id in state.list_thread_ids().await {
            let Ok(thread) = state.get_thread(thread_id).await else {
                continue;
            };
            let snapshot = thread.config_snapshot().await;
            let Some(parent_thread_id) = thread_spawn_parent_thread_id(&snapshot.session_source)
            else {
                continue;
            };
            children_by_parent
                .entry(parent_thread_id)
                .or_default()
                .push((
                    thread_id,
                    self.state
                        .agent_metadata_for_thread(thread_id)
                        .unwrap_or(AgentMetadata {
                            agent_id: Some(thread_id),
                            ..Default::default()
                        }),
                ));
        }

        for children in children_by_parent.values_mut() {
            children.sort_by(|left, right| {
                left.1
                    .agent_path
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(right.1.agent_path.as_deref().unwrap_or_default())
                    .then_with(|| left.0.to_string().cmp(&right.0.to_string()))
            });
        }

        Ok(children_by_parent)
    }

    async fn persist_thread_spawn_edge_for_source(
        &self,
        thread: &crate::CodexThread,
        child_thread_id: ThreadId,
        session_source: Option<&SessionSource>,
    ) {
        let Some(parent_thread_id) = session_source.and_then(thread_spawn_parent_thread_id) else {
            return;
        };
        let Some(state_db_ctx) = thread.state_db() else {
            return;
        };
        if let Err(err) = state_db_ctx
            .upsert_thread_spawn_edge(
                parent_thread_id,
                child_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
        {
            warn!("failed to persist thread-spawn edge: {err}");
        }
    }

    async fn live_thread_spawn_descendants(
        &self,
        root_thread_id: ThreadId,
    ) -> CodexResult<Vec<ThreadId>> {
        let mut children_by_parent = self.live_thread_spawn_children().await?;
        let mut descendants = Vec::new();
        let mut stack = children_by_parent
            .remove(&root_thread_id)
            .unwrap_or_default()
            .into_iter()
            .map(|(child_thread_id, _)| child_thread_id)
            .rev()
            .collect::<Vec<_>>();

        while let Some(thread_id) = stack.pop() {
            descendants.push(thread_id);
            if let Some(children) = children_by_parent.remove(&thread_id) {
                for (child_thread_id, _) in children.into_iter().rev() {
                    stack.push(child_thread_id);
                }
            }
        }

        Ok(descendants)
    }
}

fn is_internal_supervisor_role(agent_role: Option<&str>) -> bool {
    matches!(
        agent_role,
        Some("watchdog" | crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
    )
}

async fn parent_prompt_cache_key_for_source(
    state: &Arc<ThreadManagerState>,
    session_source: Option<&SessionSource>,
) -> Option<ThreadId> {
    if !fork_parent_prompt_cache_key_enabled() {
        return None;
    }

    let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id, ..
    })) = session_source
    else {
        return None;
    };

    state
        .get_thread(*parent_thread_id)
        .await
        .ok()
        .map(|parent_thread| parent_thread.codex.session.prompt_cache_key())
}

fn fork_parent_prompt_cache_key_enabled() -> bool {
    let parent_named_value =
        std::env::var(CODEX_EXPERIMENTAL_FORK_PARENT_PROMPT_CACHE_KEY_ENV).ok();
    let legacy_value = std::env::var(CODEX_EXPERIMENTAL_FORK_PROMPT_CACHE_KEY_ENV).ok();
    fork_parent_prompt_cache_key_value_enabled(
        parent_named_value.as_deref(),
        legacy_value.as_deref(),
    )
}

fn fork_parent_prompt_cache_key_value_enabled(
    parent_named_value: Option<&str>,
    legacy_value: Option<&str>,
) -> bool {
    parent_named_value.or(legacy_value).is_none_or(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn previous_response_fork_rollout_items(
    source_items: Vec<RolloutItem>,
    baseline_input: Vec<ResponseItem>,
) -> Vec<RolloutItem> {
    let source_session_meta = source_items.iter().find_map(|item| match item {
        RolloutItem::SessionMeta(meta) => Some(meta.clone()),
        RolloutItem::ForkReference(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::ResponseItem(_)
        | RolloutItem::Compacted(_)
        | RolloutItem::TurnContext(_)
        | RolloutItem::EventMsg(_) => None,
    });
    let latest_turn_context = source_items.iter().rev().find_map(|item| match item {
        RolloutItem::TurnContext(turn_context) => Some(turn_context.clone()),
        RolloutItem::ForkReference(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::ResponseItem(_)
        | RolloutItem::Compacted(_)
        | RolloutItem::SessionMeta(_)
        | RolloutItem::EventMsg(_) => None,
    });

    source_session_meta
        .into_iter()
        .map(RolloutItem::SessionMeta)
        .chain(baseline_input.into_iter().map(RolloutItem::ResponseItem))
        .chain(
            latest_turn_context
                .into_iter()
                .map(RolloutItem::TurnContext),
        )
        .collect()
}

async fn parent_mcp_tool_snapshot_for_source(
    state: &Arc<ThreadManagerState>,
    session_source: Option<&SessionSource>,
) -> Option<McpToolSnapshot> {
    let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id, ..
    })) = session_source
    else {
        return None;
    };

    let parent_thread = state.get_thread(*parent_thread_id).await.ok()?;
    let list_all_tools = {
        let mcp_connection_manager = parent_thread
            .codex
            .session
            .services
            .mcp_connection_manager
            .read()
            .await;
        mcp_connection_manager.list_all_tools_future()
    };
    let tools = list_all_tools.await;
    Some(McpToolSnapshot { tools })
}

fn fork_previous_response_id_enabled() -> bool {
    let value = std::env::var(CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID_ENV).ok();
    fork_previous_response_id_value_enabled(value.as_deref())
}

fn fork_previous_response_id_value_enabled(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

async fn parent_response_continuation_for_source(
    state: &Arc<ThreadManagerState>,
    session_source: Option<&SessionSource>,
) -> Option<crate::client::ResponseContinuation> {
    if !fork_previous_response_id_enabled() {
        return None;
    }
    let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id, ..
    })) = session_source
    else {
        return None;
    };

    state
        .get_thread(*parent_thread_id)
        .await
        .ok()
        .and_then(|parent_thread| parent_thread.codex.session.response_continuation_for_fork())
}

fn thread_spawn_parent_thread_id(session_source: &SessionSource) -> Option<ThreadId> {
    match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        }) => Some(*parent_thread_id),
        _ => None,
    }
}

fn sanitize_watchdog_wakeup_message(message: String) -> Option<String> {
    let Some(stripped_message) = strip_leading_watchdog_prompt_scaffold(&message) else {
        let message = message.trim();
        return (!message.is_empty()).then(|| message.to_string());
    };

    let stripped_message = stripped_message.trim();
    (!stripped_message.is_empty()).then(|| stripped_message.to_string())
}

fn strip_leading_watchdog_prompt_scaffold(message: &str) -> Option<&str> {
    let mut lines = message.split_inclusive('\n').scan(0, |offset, line| {
        let line_start = *offset;
        *offset += line.len();
        Some((line_start, line))
    });

    let mut saw_watchdog_scaffold = false;
    let mut report_start = None;
    for (line_start, line) in &mut lines {
        let trimmed = line.trim();
        if trimmed.contains("watchdog check-in agent")
            || trimmed.starts_with("Read AGENTS.watchdog.md")
            || trimmed.starts_with("Target agent id:")
        {
            saw_watchdog_scaffold = true;
        }
        if trimmed.starts_with("AUTOPLAN_WATCHDOG_REPORT")
            || trimmed.starts_with("Watchdog report:")
        {
            report_start = Some(line_start);
            break;
        }
    }

    saw_watchdog_scaffold.then(|| &message[report_start.unwrap_or(message.len())..])
}

fn agent_matches_prefix(agent_path: Option<&AgentPath>, prefix: &AgentPath) -> bool {
    if prefix.is_root() {
        return true;
    }

    agent_path.is_some_and(|agent_path| {
        agent_path == prefix
            || agent_path
                .as_str()
                .strip_prefix(prefix.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn format_watchdog_snooze_duration(delay_seconds: u64) -> String {
    let minutes = delay_seconds / 60;
    let seconds = delay_seconds % 60;
    match (minutes, seconds) {
        (0, seconds) => format!("{seconds}s"),
        (minutes, 0) => format!("{minutes}m"),
        (minutes, seconds) => format!("{minutes}m {seconds}s"),
    }
}

pub(crate) fn render_input_preview(initial_operation: &Op) -> String {
    match initial_operation {
        Op::UserInput { items, .. } => items
            .iter()
            .map(|item| match item {
                UserInput::Text { text, .. } => text.clone(),
                UserInput::Image { .. } => "[image]".to_string(),
                UserInput::LocalImage { path } => format!("[local_image:{}]", path.display()),
                UserInput::Skill { name, path } => format!("[skill:${name}]({})", path.display()),
                UserInput::Mention { name, path } => format!("[mention:${name}]({path})"),
                _ => "[input]".to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Op::InterAgentCommunication { communication } => communication.content.clone(),
        _ => String::new(),
    }
}

fn thread_spawn_depth(session_source: &SessionSource) -> Option<i32> {
    match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { depth, .. }) => Some(*depth),
        _ => None,
    }
}
#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
