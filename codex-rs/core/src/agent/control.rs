use crate::TurnInputRequest;
use crate::TurnInputSubmission;
use crate::TurnStartOptions;
use crate::agent::AgentStatus;
use crate::agent::role::DEFAULT_ROLE_NAME;
use crate::agent::role::resolve_role_config;
use crate::agent::types::AgentMetadata;
use crate::agent::types::LiveAgent;
use crate::agent_communication::AgentCommunicationContext;
use crate::config::Config;
use crate::config::RolloutBudgetConfig;
use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::inherited_thread_state::InheritedThreadState;
use crate::session::emit_subagent_session_started;
use crate::state::McpToolSnapshot;
use crate::thread_manager::ResumeThreadWithHistoryOptions;
use crate::thread_manager::ThreadIdGenerator;
use crate::thread_manager::ThreadManagerState;
use crate::thread_manager::default_thread_id_generator;
use crate::turn_timing::now_unix_timestamp_ms;
use codex_history::InitialHistory;
use codex_history::ResumedHistory;
use codex_history::RolloutItem;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::items::SubAgentActivityItem;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HasLegacyEvent;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::ItemStartedEvent;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::turn_input::TurnInputMode;
use codex_protocol::user_input::UserInput;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ReadThreadParams;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Weak;
use tracing::warn;
use uuid::Uuid;

pub(crate) use self::runtime::AgentControlInit;
pub(crate) use self::runtime::AgentTreeMembership;
pub(crate) use self::runtime::AgentTreeShutdownState;
pub(crate) use self::runtime::AgentTreeTeardownGuard;
pub(crate) use self::runtime::LocalAgentRuntime;
pub(crate) use self::watch::StatusSubscription;

const ROOT_LAST_TASK_MESSAGE: &str = "Main thread";
const CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID_ENV: &str =
    "CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID";
const SUPERVISOR_BOOT_LIST_AGENTS_CALL_ID: &str = "synthetic_supervisor_list_agents";

mod api;
mod budget;
mod completion;
mod delivery;
mod execution;
mod inspection;
mod interrupt;
mod legacy;
mod mailbox;
mod ownership;
mod ownership_tree;
mod residency;
mod resume;
mod root_handoff;
mod runtime;
mod runtime_context;
mod sender_context;
mod service_tier;
mod spawn;
mod spawn_guard;
mod spawn_telemetry;
mod target;
mod user_authorization;
mod watch;

/// Result of a supervisor request to compact its authenticated direct parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SupervisorParentCompactionResult {
    NotSupervisorHelper,
    ParentBusy {
        parent_thread_id: ThreadId,
    },
    Submitted {
        parent_thread_id: ThreadId,
        submission_id: String,
    },
}

/// Whether delivery first interrupts the current turn or queues input for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AgentInputDelivery {
    Queue,
    Interrupt,
}

/// Per-session controller handle for a local agent tree.
/// Handles retain a session identity and share their tree's `LocalAgentRuntime`.
/// Local startup preserves that state when creating or resuming children.
#[derive(Clone)]
pub(crate) struct LocalAgentControl {
    /// session_id is equal to the root thread's ID.
    session_id: SessionId,
    pub(crate) runtime: LocalAgentRuntime,
}

impl Default for LocalAgentControl {
    fn default() -> Self {
        Self::new(
            Weak::default(),
            default_thread_id_generator(),
            /*rollout_budget*/ None,
        )
    }
}

impl LocalAgentControl {
    /// Construct a new `LocalAgentControl` that can spawn/message agents via the given manager state.
    pub(crate) fn new(
        manager: Weak<ThreadManagerState>,
        thread_id_generator: ThreadIdGenerator,
        rollout_budget: Option<RolloutBudgetConfig>,
    ) -> Self {
        Self {
            session_id: SessionId::default(),
            runtime: LocalAgentRuntime::new(manager, thread_id_generator, rollout_budget),
        }
    }

    pub(crate) fn with_session_id(mut self, session_id: SessionId, max_threads: usize) -> Self {
        self.session_id = session_id;
        self.runtime.agent_execution_limiter.initialize(max_threads);
        self
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Send rich user input items to an existing agent thread.
    pub(crate) async fn send_input(
        &self,
        agent_id: ThreadId,
        input: Vec<UserInput>,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        let state = self.runtime.upgrade()?;
        let thread = state.get_thread(agent_id).await?;
        let result = match thread
            .start_or_steer_turn(TurnInputRequest::user_input(input).on_start(start_options))
            .await
        {
            Ok(TurnInputSubmission::Started { turn_id, .. }) => Ok(turn_id),
            Ok(TurnInputSubmission::Steered { .. }) => {
                // MAv1 exposes an opaque `submission_id` to the model. The legacy
                // `Op::UserInput` path returned a fresh ID for every steer, while the
                // turn-input API returns the active turn ID. Keep the tool-visible ID
                // unique without adding a submission receipt back to Core.
                Ok(Uuid::now_v7().to_string())
            }
            Ok(TurnInputSubmission::NotSubmitted { reason }) => Err(CodexErr::InvalidRequest(
                format!("turn input was not submitted: {reason:?}"),
            )),
            Err(err) => Err(err),
        };
        self.handle_thread_request_result(agent_id, &state, result)
            .await
    }

    async fn send_input_after_capacity_check(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        input: Vec<UserInput>,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        let last_task_message = non_empty_task_message(render_input_preview(&input));
        let thread = state.get_thread(agent_id).await?;
        let result = match thread
            .io
            .submit_turn_input(
                TurnInputRequest::user_input(input).on_start(start_options),
                TurnInputMode::StartOrSteer,
            )
            .await
        {
            Ok(TurnInputSubmission::Started { turn_id, .. }) => Ok(turn_id),
            Ok(TurnInputSubmission::Steered { .. }) => Ok(Uuid::now_v7().to_string()),
            Ok(TurnInputSubmission::NotSubmitted { reason }) => Err(CodexErr::InvalidRequest(
                format!("turn input was not submitted: {reason:?}"),
            )),
            Err(err) => Err(err),
        };
        let result = self
            .handle_thread_request_result(agent_id, state, result)
            .await;
        if result.is_ok() {
            match last_task_message {
                Some(last_task_message) => self
                    .runtime
                    .registry
                    .update_last_task_message(agent_id, last_task_message),
                None => self.runtime.registry.clear_last_task_message(agent_id),
            }
        }
        result
    }

    pub(crate) async fn send_inter_agent_communication(
        &self,
        agent_id: ThreadId,
        communication: InterAgentCommunication,
        agent_communication_context: AgentCommunicationContext,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        let state = self.runtime.upgrade()?;
        if communication.trigger_turn {
            let thread = state.get_thread(agent_id).await?;
            thread
                .ensure_execution_capacity_for_turn_start(self)
                .await?;
        }
        self.send_inter_agent_communication_after_capacity_check(
            agent_id,
            &state,
            communication,
            agent_communication_context,
            start_options,
        )
        .await
    }

    pub(crate) async fn emit_sub_agent_activity(
        &self,
        thread_id: ThreadId,
        turn_id: String,
        item: SubAgentActivityItem,
    ) -> CodexResult<()> {
        let state = self.runtime.upgrade()?;
        let thread = state.get_thread(thread_id).await?;
        let started_at_ms = now_unix_timestamp_ms();
        let item = TurnItem::SubAgentActivity(item);
        thread
            .session
            .send_event_raw(Event {
                id: turn_id.clone(),
                msg: EventMsg::ItemStarted(ItemStartedEvent {
                    thread_id,
                    turn_id: turn_id.clone(),
                    item: item.clone(),
                    started_at_ms,
                }),
            })
            .await;
        let completed_at_ms = now_unix_timestamp_ms();
        let completed = ItemCompletedEvent {
            thread_id,
            turn_id: turn_id.clone(),
            item,
            started_at_ms: Some(started_at_ms),
            completed_at_ms,
        };
        thread
            .session
            .send_event_raw(Event {
                id: turn_id.clone(),
                msg: EventMsg::ItemCompleted(completed.clone()),
            })
            .await;
        for legacy in completed.as_legacy_events(/*show_raw_agent_reasoning*/ false) {
            thread
                .session
                .send_event_raw(Event {
                    id: turn_id.clone(),
                    msg: legacy,
                })
                .await;
        }
        Ok(())
    }

    async fn send_inter_agent_communication_after_capacity_check(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        self.submit_inter_agent_communication(
            agent_id,
            state,
            communication,
            context,
            start_options,
        )
        .await
    }

    async fn submit_inter_agent_communication(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        let last_task_message = last_task_message_from_communication(&communication);
        let communication_for_log =
            crate::agent_communication::logging_enabled().then(|| communication.clone());
        {
            // Keep unloaded delivery atomic with publication of a reloaded session.
            let threads = state.threads.read().await;
            if !communication.trigger_turn && !threads.contains_key(&agent_id) {
                let _membership = self.runtime.admit_start()?;
                self.runtime.ensure_agent_known(agent_id)?;
                let submission_id = uuid::Uuid::now_v7().to_string();
                self.runtime.mailboxes.enqueue(
                    agent_id,
                    Some(submission_id.clone()),
                    vec![communication],
                )?;
                match last_task_message {
                    Some(message) => self
                        .runtime
                        .registry
                        .update_last_task_message(agent_id, message),
                    None => self.runtime.registry.clear_last_task_message(agent_id),
                }
                if let Some(communication) = communication_for_log {
                    crate::agent_communication::emit_agent_communication_send(
                        &submission_id,
                        &context,
                        &communication,
                        agent_id,
                    );
                }
                return Ok(submission_id);
            }
        }
        // Loaded recipients retain submission ordering with follow-ups and interrupts.
        let (parent_turn_id, root_turn_id) = if communication.trigger_turn {
            (
                start_options.parent_turn_id.clone(),
                start_options.root_turn_id.clone(),
            )
        } else {
            (None, None)
        };
        let result = self
            .handle_thread_request_result(
                agent_id,
                state,
                state
                    .send_op(
                        agent_id,
                        Op::InterAgentCommunication {
                            communication,
                            start_options,
                        },
                        parent_turn_id,
                        root_turn_id,
                    )
                    .await,
            )
            .await;
        if let (Some(communication), Ok(communication_id)) =
            (communication_for_log, result.as_ref())
        {
            crate::agent_communication::emit_agent_communication_send(
                communication_id,
                &context,
                &communication,
                agent_id,
            );
        }
        if result.is_ok() {
            match last_task_message {
                Some(last_task_message) => self
                    .runtime
                    .registry
                    .update_last_task_message(agent_id, last_task_message),
                None => self.runtime.registry.clear_last_task_message(agent_id),
            }
        }
        result
    }

    /// Interrupt the current task for an existing agent thread.
    pub(crate) async fn interrupt_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let state = self.runtime.upgrade()?;
        self.handle_thread_request_result(
            agent_id,
            &state,
            state
                .send_op(
                    agent_id,
                    Op::Interrupt,
                    /*parent_turn_id*/ None,
                    /*root_turn_id*/ None,
                )
                .await,
        )
        .await
    }

    async fn handle_thread_request_result(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        result: CodexResult<String>,
    ) -> CodexResult<String> {
        if result
            .as_ref()
            .is_err_and(|err| matches!(err.details(), CodexErrorDetails::InternalAgentDied))
        {
            let _ = state.remove_thread(&agent_id).await;
            self.forget_agent_residency(agent_id);
            self.runtime.registry.release_spawned_thread(agent_id);
        }
        result
    }

    /// Fetch the last known status for `agent_id`, returning `NotFound` when unavailable.
    pub(crate) async fn get_status(&self, agent_id: ThreadId) -> AgentStatus {
        let Ok(state) = self.runtime.upgrade() else {
            // No agent available if upgrade fails.
            return AgentStatus::NotFound;
        };
        let Ok(thread) = state.get_thread(agent_id).await else {
            return AgentStatus::NotFound;
        };
        thread.agent_status().await
    }

    pub(crate) fn get_agent_metadata(&self, agent_id: ThreadId) -> Option<AgentMetadata> {
        self.runtime.registry.agent_metadata_for_thread(agent_id)
    }

    pub(crate) fn register_session_root(
        &self,
        current_thread_id: ThreadId,
        current_parent_thread_id: Option<ThreadId>,
    ) {
        self.runtime
            .register_session_root(current_thread_id, current_parent_thread_id);
    }

    pub(crate) fn ensure_agent_known(&self, agent_id: ThreadId) -> CodexResult<AgentMetadata> {
        self.runtime.ensure_agent_known(agent_id)
    }

    pub(crate) async fn list_agents(
        &self,
        current_session_source: &SessionSource,
        path_prefix: Option<&str>,
    ) -> CodexResult<Vec<LiveAgent>> {
        let state = self.runtime.upgrade()?;
        let resolved_prefix = path_prefix
            .map(|prefix| {
                current_session_source
                    .get_agent_path()
                    .unwrap_or_else(AgentPath::root)
                    .resolve(prefix)
                    .map_err(CodexErr::UnsupportedOperation)
            })
            .transpose()?;

        let mut live_agents = self.runtime.registry.live_agents();
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
            && let Some(root_thread_id) = self.runtime.registry.agent_id_for_path(&root_path)
            && let Ok(root_thread) = state.get_thread(root_thread_id).await
        {
            agents.push(LiveAgent {
                thread_id: root_thread_id,
                metadata: AgentMetadata {
                    agent_id: Some(root_thread_id),
                    agent_path: Some(root_path),
                    last_task_message: Some(ROOT_LAST_TASK_MESSAGE.to_string()),
                    ..Default::default()
                },
                status: root_thread.agent_status().await,
            });
        }

        for metadata in live_agents {
            let Some(thread_id) = metadata.agent_id else {
                continue;
            };
            if resolved_prefix
                .as_ref()
                .is_some_and(|prefix| !agent_matches_prefix(metadata.agent_path.as_ref(), prefix))
            {
                continue;
            }

            let agent_status = match state.get_thread(thread_id).await {
                Ok(thread) => thread.agent_status().await,
                Err(err)
                    if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_))
                        && self
                            .runtime
                            .registry
                            .agent_lifecycle(thread_id)
                            .is_some_and(|lifecycle| lifecycle.is_visible_when_cold()) =>
                {
                    AgentStatus::Completed(None)
                }
                Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {
                    continue;
                }
                Err(err) => return Err(err),
            };
            agents.push(LiveAgent {
                thread_id,
                metadata,
                status: agent_status,
            });
        }

        Ok(agents)
    }

    fn prepare_agent_metadata(
        &self,
        reservation: &mut crate::agent::registry::SpawnReservation,
        config: &Config,
        agent_path: Option<AgentPath>,
        agent_role: Option<String>,
        preferred_agent_nickname: Option<String>,
    ) -> CodexResult<AgentMetadata> {
        if let Some(agent_path) = agent_path.as_ref() {
            reservation.reserve_agent_path(agent_path)?;
        }
        let candidate_names = spawn::agent_nickname_candidates(config, agent_role.as_deref());
        let candidate_name_refs: Vec<&str> = candidate_names.iter().map(String::as_str).collect();
        let agent_nickname = Some(reservation.reserve_agent_nickname_with_preference(
            &candidate_name_refs,
            preferred_agent_nickname.as_deref(),
        )?);
        Ok(AgentMetadata {
            agent_id: None,
            agent_path,
            agent_nickname,
            agent_role,
            last_task_message: None,
            ..Default::default()
        })
    }
    #[allow(clippy::too_many_arguments)]
    fn prepare_thread_spawn(
        &self,
        reservation: &mut crate::agent::registry::SpawnReservation,
        config: &Config,
        parent_thread_id: ThreadId,
        depth: i32,
        agent_path: Option<AgentPath>,
        agent_role: Option<String>,
        preferred_agent_nickname: Option<String>,
    ) -> CodexResult<(SessionSource, AgentMetadata)> {
        if depth == 1 {
            self.runtime.registry.register_root_thread(parent_thread_id);
        }
        let agent_metadata = self.prepare_agent_metadata(
            reservation,
            config,
            agent_path,
            agent_role,
            preferred_agent_nickname,
        )?;
        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth,
            agent_path: agent_metadata.agent_path.clone(),
            agent_nickname: agent_metadata.agent_nickname.clone(),
            agent_role: agent_metadata.agent_role.clone(),
        });
        Ok((session_source, agent_metadata))
    }

    fn upgrade(&self) -> CodexResult<Arc<ThreadManagerState>> {
        self.runtime.upgrade()
    }

    pub(crate) fn upgrade_for_tools(&self) -> CodexResult<Arc<ThreadManagerState>> {
        self.upgrade()
    }

    async fn inherited_environments_for_source(
        &self,
        state: &Arc<ThreadManagerState>,
        session_source: Option<&SessionSource>,
    ) -> Option<TurnEnvironmentSnapshot> {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return None;
        };

        let parent_thread = state.get_thread(*parent_thread_id).await.ok()?;
        Some(
            parent_thread
                .session
                .services
                .turn_environments
                .snapshot()
                .await,
        )
    }

    async fn inherited_exec_policy_for_source(
        &self,
        state: &Arc<ThreadManagerState>,
        session_source: Option<&SessionSource>,
        child_config: &Config,
    ) -> Option<Arc<crate::exec_policy::ExecPolicyManager>> {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return None;
        };

        let parent_thread = state.get_thread(*parent_thread_id).await.ok()?;
        let parent_config = parent_thread.session.get_config().await;
        if !crate::exec_policy::child_uses_parent_exec_policy(&parent_config, child_config) {
            return None;
        }

        Some(Arc::clone(&parent_thread.session.services.exec_policy))
    }

    async fn persist_thread_spawn_edge_for_source(
        &self,
        child_thread: &crate::CodexThread,
        child_thread_id: ThreadId,
        session_source: Option<&SessionSource>,
    ) {
        let Some(parent_thread_id) = session_source.and_then(SessionSource::parent_thread_id)
        else {
            return;
        };
        if child_thread.config_snapshot().await.ephemeral {
            return;
        }
        let Ok(state) = self.runtime.upgrade() else {
            return;
        };
        let Some(agent_graph_store) = state.agent_graph_store() else {
            return;
        };
        if let Err(err) = agent_graph_store
            .upsert_thread_spawn_edge(
                parent_thread_id,
                child_thread_id,
                codex_agent_graph_store::ThreadSpawnEdgeStatus::Open,
            )
            .await
        {
            warn!("failed to persist thread-spawn edge: {err}");
        }
    }
}

fn subagent_assignment_item(session_source: &SessionSource, message: String) -> ResponseItem {
    let agent_path = session_source
        .get_agent_path()
        .map(String::from)
        .unwrap_or_else(|| "this subagent".to_string());
    ResponseItem::Message {
        id: None,
        role: "developer".to_string(),
        content: vec![ContentItem::InputText {
            text: format!(
                "# Subagent Assignment\n\nYou are `{agent_path}`. Your direct assignment from your parent agent is:\n\n{message}"
            ),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
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

pub(crate) fn render_input_preview(input: &[UserInput]) -> String {
    input
        .iter()
        .map(|item| match item {
            UserInput::Text { text, .. } => text.clone(),
            UserInput::Image { .. } => "[image]".to_string(),
            UserInput::LocalImage { path, .. } => {
                format!("[local_image:{}]", path.display())
            }
            UserInput::Audio { .. } => "[audio]".to_string(),
            UserInput::LocalAudio { path } => {
                format!("[local_audio:{}]", path.display())
            }
            UserInput::Skill { name, path, .. } => {
                format!("[skill:${name}]({})", path.display())
            }
            UserInput::Mention { name, path, .. } => format!("[mention:${name}]({path})"),
            _ => "[input]".to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn last_task_message_from_communication(communication: &InterAgentCommunication) -> Option<String> {
    if communication.encrypted_content.is_some() {
        return None;
    }
    non_empty_task_message(communication.content.clone())
}

fn non_empty_task_message(message: String) -> Option<String> {
    (!message.is_empty()).then_some(message)
}

fn synthetic_supervisor_list_agents_items(
    owner_thread_id: ThreadId,
    agents: Vec<LiveAgent>,
) -> Vec<RolloutItem> {
    let agents: Vec<_> = agents
        .into_iter()
        .map(|agent| {
            serde_json::json!({
                "agent_name": agent.metadata.agent_path.map(|path| path.to_string())
                    .unwrap_or_else(|| agent.thread_id.to_string()),
                "agent_status": agent.status,
                "last_task_message": agent.metadata.last_task_message,
            })
        })
        .collect();
    let envelope = serde_json::json!({
        "source": "pre_injected_agents_list",
        "generated_at": chrono::Utc::now().timestamp(),
        "owner_thread_id": owner_thread_id.to_string(),
        "agents": agents,
    });
    let mut output = FunctionCallOutputPayload::from_text(envelope.to_string());
    output.success = Some(true);

    vec![
        RolloutItem::ResponseItem(
            ResponseItem::FunctionCall {
                id: None,
                name: "list_agents".to_string(),
                namespace: None,
                arguments: "{}".to_string(),
                call_id: SUPERVISOR_BOOT_LIST_AGENTS_CALL_ID.to_string(),
                encrypted_function_args: None,
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
        RolloutItem::ResponseItem(
            ResponseItem::FunctionCallOutput {
                id: None,
                call_id: SUPERVISOR_BOOT_LIST_AGENTS_CALL_ID.to_string(),
                output,
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
    ]
}

fn role_prompt_item(prompt: String) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "developer".to_string(),
        content: vec![ContentItem::InputText { text: prompt }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn thread_spawn_depth(session_source: &SessionSource) -> Option<i32> {
    match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { depth, .. }) => Some(*depth),
        _ => None,
    }
}

async fn parent_prompt_cache_key_for_source(
    state: &Arc<ThreadManagerState>,
    session_source: Option<&SessionSource>,
) -> Option<ThreadId> {
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
        .map(|parent_thread| parent_thread.session.prompt_cache_key())
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
    let binding = parent_thread
        .session
        .services
        .mcp_runtime
        .current_binding()
        .await?;
    Some(McpToolSnapshot {
        tools: binding.tools().to_vec(),
    })
}

fn fork_previous_response_id_enabled() -> bool {
    std::env::var(CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID_ENV)
        .is_ok_and(|value| fork_previous_response_id_value_enabled(&value))
}

fn fork_previous_response_id_value_enabled(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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
        .and_then(|parent_thread| parent_thread.session.response_continuation_for_fork())
}
#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
