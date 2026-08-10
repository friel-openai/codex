use crate::agent::AgentStatus;
use crate::agent::registry::AgentMetadata;
use crate::agent::registry::AgentRegistry;
use crate::agent::role::DEFAULT_ROLE_NAME;
use crate::agent::role::resolve_role_config;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::codex_thread::ThreadConfigSnapshot;
use crate::config::Config;
use crate::config::RolloutBudgetConfig;
use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::goal_supervisor::is_goal_supervisor_helper_source;
use crate::inherited_thread_state::InheritedThreadState;
use crate::rollout_budget::RolloutBudget;
use crate::session::emit_subagent_session_started;
use crate::session_prefix::format_subagent_context_line;
use crate::state::McpToolSnapshot;
use crate::thread_manager::ResumeThreadWithHistoryOptions;
use crate::thread_manager::ThreadIdGenerator;
use crate::thread_manager::ThreadManagerState;
use crate::thread_manager::default_thread_id_generator;
use crate::thread_rollout_truncation::truncate_rollout_to_last_n_fork_turns;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InitialHistory;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ResumedHistory;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::user_input::UserInput;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ReadThreadParams;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Weak;
use tokio::sync::watch;
use tracing::warn;

pub(crate) use self::execution::AgentExecutionGuard;
use self::execution::AgentExecutionLimiter;
use self::residency::AgentResidency;

const LIST_AGENTS_DEFAULT_LIMIT: usize = 25;
const LIST_AGENTS_MAX_LIMIT: usize = 25;
const LIST_AGENTS_TASK_PREVIEW_BYTES: usize = 256;
const LIST_AGENTS_MAX_SERIALIZED_BYTES: usize = 12 * 1024;
const ENVIRONMENT_SUBAGENTS_MAX_RECORDS: usize = 25;
// The renderer adds indentation and XML escaping. Keep this source projection smaller than the
// final ContextualUserFragment cap so adversarial nicknames cannot crowd out the omission notice.
const ENVIRONMENT_SUBAGENTS_MAX_SERIALIZED_BYTES: usize = 768;
const CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID_ENV: &str =
    "CODEX_EXPERIMENTAL_FORK_PREVIOUS_RESPONSE_ID";
const SUPERVISOR_BOOT_LIST_AGENTS_CALL_ID: &str = "synthetic_supervisor_list_agents";

mod close;
mod completion;
mod current_membership;
mod execution;
mod legacy;
mod ownership;
mod ownership_tree;
mod residency;
mod resume;
mod spawn;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpawnAgentForkMode {
    FullHistory,
    LastNTurns(usize),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SpawnAgentOptions {
    pub(crate) fork_parent_spawn_call_id: Option<String>,
    pub(crate) fork_mode: Option<SpawnAgentForkMode>,
    pub(crate) parent_thread_id: Option<ThreadId>,
    pub(crate) parent_turn_id: Option<String>,
    pub(crate) environments: Option<Vec<TurnEnvironmentSelection>>,
    pub(crate) initial_task_message: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct LiveAgent {
    pub(crate) thread_id: ThreadId,
    pub(crate) metadata: AgentMetadata,
    pub(crate) status: AgentStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ListedAgent {
    pub(crate) agent_id: ThreadId,
    pub(crate) parent_agent_id: Option<ThreadId>,
    pub(crate) agent_name: String,
    pub(crate) agent_status: AgentStatus,
    pub(crate) last_task_message: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ListedAgentsPage {
    pub(crate) agents: Vec<ListedAgent>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) total_count: usize,
}

/// Counts from a durable, recursive agent-subtree close operation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct CloseAgentSubtreeReport {
    pub(crate) closed_agents: usize,
    pub(crate) closed_edges: usize,
    pub(crate) newly_closed_edges: usize,
    pub(crate) stopped_runtimes: usize,
    pub(crate) paused_goals: usize,
    pub(crate) cleared_queued_items: usize,
    pub(crate) evicted_identities: usize,
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AgentInputDelivery {
    Queue,
    Interrupt,
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
    /// Captured at construction so delegates retain their manager's allocation policy.
    thread_id_generator: ThreadIdGenerator,
    pub(super) state: Arc<AgentRegistry>,
    agent_residency: Arc<AgentResidency>,
    agent_execution_limiter: Arc<AgentExecutionLimiter>,
    /// Session-scoped state shared by the root thread and every cloned sub-agent control handle.
    rollout_budget: Arc<RolloutBudget>,
}

impl Default for AgentControl {
    fn default() -> Self {
        Self::new(
            Weak::default(),
            default_thread_id_generator(),
            /*rollout_budget*/ None,
        )
    }
}

impl AgentControl {
    pub(crate) fn current_membership_root_thread_id(&self) -> ThreadId {
        self.state
            .agent_id_for_path(&AgentPath::root())
            .unwrap_or_else(|| ThreadId::from(self.session_id))
    }

    pub(crate) fn current_membership_subtree_thread_ids(
        &self,
        root_thread_id: ThreadId,
    ) -> Vec<ThreadId> {
        self.state.registered_subtree_thread_ids(root_thread_id)
    }

    pub(crate) fn current_membership_descendant_parents(
        &self,
        root_thread_id: ThreadId,
    ) -> HashMap<ThreadId, ThreadId> {
        self.state
            .registered_subtree_thread_ids(root_thread_id)
            .into_iter()
            .filter(|thread_id| *thread_id != root_thread_id)
            .filter_map(|thread_id| {
                let parent_thread_id = self
                    .state
                    .agent_metadata_for_thread(thread_id)?
                    .parent_thread_id?;
                Some((thread_id, parent_thread_id))
            })
            .collect()
    }

    pub(crate) fn has_current_agent_members(&self) -> bool {
        !self.state.live_agents().is_empty()
    }

    pub(crate) fn shares_current_agent_registry(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    async fn lock_lifecycle_mutation(&self) -> CodexResult<tokio::sync::OwnedMutexGuard<()>> {
        Ok(self.upgrade()?.lock_lifecycle_mutation().await)
    }

    /// Construct a new `AgentControl` that can spawn/message agents via the given manager state.
    pub(crate) fn new(
        manager: Weak<ThreadManagerState>,
        thread_id_generator: ThreadIdGenerator,
        rollout_budget: Option<RolloutBudgetConfig>,
    ) -> Self {
        let control = Self {
            session_id: SessionId::default(),
            manager,
            thread_id_generator,
            state: Arc::default(),
            agent_residency: Arc::default(),
            agent_execution_limiter: Arc::default(),
            mcp_connection_pool: Default::default(),
            rollout_budget: Arc::default(),
        };
        if let Some(rollout_budget) = rollout_budget {
            control.rollout_budget.configure(rollout_budget);
        }
        control
    }

    pub(crate) fn with_session_id(mut self, session_id: SessionId, max_threads: usize) -> Self {
        self.session_id = session_id;
        self.agent_execution_limiter.initialize(max_threads);
        self
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn generate_thread_id(&self) -> ThreadId {
        (self.thread_id_generator)()
    }

    pub(crate) fn rollout_budget(&self) -> &RolloutBudget {
        self.rollout_budget.as_ref()
    }

    /// Send rich user input items to an existing agent thread.
    pub(crate) async fn send_input(
        &self,
        agent_id: ThreadId,
        input: Vec<UserInput>,
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let state = self.upgrade()?;
        self.ensure_execution_capacity_for_turn_start(agent_id, /*starts_turn*/ true)
            .await?;
        self.send_input_after_capacity_check(agent_id, &state, input, parent_turn_id)
            .await
    }

    async fn send_input_after_capacity_check(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        input: Vec<UserInput>,
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let control = self.clone();
        let state = Arc::clone(state);
        tokio::spawn(async move {
            let last_task_message = non_empty_task_message(render_input_preview(&input));
            let submission = match state
                .send_op_admitted(agent_id, input.into(), parent_turn_id)
                .await
            {
                Ok(submission) => submission,
                Err(err) => {
                    return control
                        .handle_thread_request_result(agent_id, &state, Err(err))
                        .await;
                }
            };
            match last_task_message {
                Some(last_task_message) => control
                    .state
                    .update_last_task_message(agent_id, last_task_message),
                None => control.state.clear_last_task_message(agent_id),
            }
            Ok(submission.into_id())
        })
        .await
        .map_err(|_| CodexErr::InternalAgentDied)?
    }

    pub(crate) async fn send_inter_agent_communication(
        &self,
        agent_id: ThreadId,
        communication: InterAgentCommunication,
        agent_communication_context: AgentCommunicationContext,
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let state = self.upgrade()?;
        self.ensure_execution_capacity_for_turn_start(agent_id, communication.trigger_turn)
            .await?;
        self.send_inter_agent_communication_after_capacity_check(
            agent_id,
            &state,
            communication,
            agent_communication_context,
            parent_turn_id,
        )
        .await
    }

    async fn send_inter_agent_communication_after_capacity_check(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let control = self.clone();
        let state = Arc::clone(state);
        tokio::spawn(async move {
            control
                .submit_inter_agent_communication(
                    agent_id,
                    &state,
                    communication,
                    context,
                    parent_turn_id,
                )
                .await
        })
        .await
        .map_err(|_| CodexErr::InternalAgentDied)?
    }

    async fn submit_inter_agent_communication(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let last_task_message = last_task_message_from_communication(&communication);
        let communication_for_log =
            crate::agent_communication::logging_enabled().then(|| communication.clone());
        let parent_turn_id = parent_turn_id.filter(|_| communication.trigger_turn);
        let submission = match state
            .send_op_admitted(
                agent_id,
                Op::InterAgentCommunication { communication },
                parent_turn_id,
            )
            .await
        {
            Ok(submission) => submission,
            Err(err) => {
                return self
                    .handle_thread_request_result(agent_id, state, Err(err))
                    .await;
            }
        };
        if let Some(communication) = communication_for_log {
            crate::agent_communication::emit_agent_communication_send(
                submission.id(),
                &context,
                &communication,
                agent_id,
            );
        }
        match last_task_message {
            Some(last_task_message) => self
                .state
                .update_last_task_message(agent_id, last_task_message),
            None => self.state.clear_last_task_message(agent_id),
        }
        Ok(submission.into_id())
    }

    /// Interrupt the current task for an existing agent thread.
    pub(crate) async fn interrupt_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let state = self.upgrade()?;
        self.handle_thread_request_result(
            agent_id,
            &state,
            state
                .send_op(agent_id, Op::Interrupt, /*parent_turn_id*/ None)
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
            self.state.release_spawned_thread(agent_id);
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
        thread.agent_status().await
    }

    pub(crate) fn register_session_root(
        &self,
        current_thread_id: ThreadId,
        current_parent_thread_id: Option<ThreadId>,
    ) {
        if current_parent_thread_id.is_none() {
            self.state.register_root_thread(current_thread_id);
        }
    }

    pub(crate) fn get_agent_metadata(&self, agent_id: ThreadId) -> Option<AgentMetadata> {
        self.state.agent_metadata_for_thread(agent_id)
    }

    pub(crate) fn ensure_agent_known(&self, agent_id: ThreadId) -> CodexResult<AgentMetadata> {
        self.state
            .agent_metadata_for_thread(agent_id)
            .ok_or_else(|| CodexErr::ThreadNotFound(agent_id))
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
        current_thread_id: ThreadId,
        current_session_source: &SessionSource,
        agent_reference: &str,
    ) -> CodexResult<ThreadId> {
        let current_agent_path = current_session_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root);
        let agent_path = current_agent_path
            .resolve(agent_reference)
            .map_err(CodexErr::UnsupportedOperation)?;
        let metadata = self
            .ensure_open_agent_known_by_path(current_thread_id, &agent_path)
            .await?;
        metadata.agent_id.ok_or_else(|| {
            CodexErr::Fatal(format!(
                "resolved agent path `{agent_path}` without a thread id"
            ))
        })
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
        let Ok(current_members) = self.current_agent_members().await else {
            return String::new();
        };
        let direct_members = current_members
            .into_iter()
            .filter(|member| member.parent_thread_id == parent_thread_id)
            .collect::<Vec<_>>();
        let total_count = direct_members.len();
        if total_count == 0 {
            return String::new();
        }

        let mut rendered = String::new();
        let mut rendered_count = 0_usize;
        let maximum_truncation_line = format!(
            "[{total_count} additional current subagents omitted; use list_agents to inspect them]"
        );
        for member in direct_members
            .iter()
            .take(ENVIRONMENT_SUBAGENTS_MAX_RECORDS)
        {
            let reference = member
                .agent_path
                .as_ref()
                .map(|agent_path| agent_path.name().to_string())
                .unwrap_or_else(|| member.thread_id.to_string());
            let nickname = self
                .state
                .agent_metadata_for_thread(member.thread_id)
                .and_then(|metadata| metadata.agent_nickname);
            let line = format_subagent_context_line(reference.as_str(), nickname.as_deref());
            let next_count = rendered_count.saturating_add(1);
            let remaining_count = total_count.saturating_sub(next_count);
            let separator_bytes = usize::from(!rendered.is_empty());
            let truncation_bytes = if remaining_count == 0 {
                0
            } else {
                1 + maximum_truncation_line.len()
            };
            if rendered
                .len()
                .saturating_add(separator_bytes)
                .saturating_add(line.len())
                .saturating_add(truncation_bytes)
                > ENVIRONMENT_SUBAGENTS_MAX_SERIALIZED_BYTES
            {
                break;
            }
            if !rendered.is_empty() {
                rendered.push('\n');
            }
            rendered.push_str(line.as_str());
            rendered_count = next_count;
        }

        if rendered_count < total_count {
            let truncation_line = format!(
                "[{} additional current subagents omitted; use list_agents to inspect them]",
                total_count - rendered_count
            );
            if !rendered.is_empty() {
                rendered.push('\n');
            }
            rendered.push_str(truncation_line.as_str());
        }
        debug_assert!(rendered.len() <= ENVIRONMENT_SUBAGENTS_MAX_SERIALIZED_BYTES);
        rendered
    }

    pub(crate) async fn list_agents(
        &self,
        current_session_source: &SessionSource,
        path_prefix: Option<&str>,
    ) -> CodexResult<Vec<ListedAgent>> {
        let resolved_prefix = path_prefix
            .map(|prefix| {
                current_session_source
                    .get_agent_path()
                    .unwrap_or_else(AgentPath::root)
                    .resolve(prefix)
                    .map_err(CodexErr::UnsupportedOperation)
            })
            .transpose()?;

        let current_members = self.current_agent_members().await?;

        let mut agents = Vec::with_capacity(current_members.len());

        for member in current_members {
            if resolved_prefix
                .as_ref()
                .is_some_and(|prefix| !agent_matches_prefix(member.agent_path.as_ref(), prefix))
            {
                continue;
            }
            let agent_name = member
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| member.thread_id.to_string());
            agents.push(ListedAgent {
                agent_id: member.thread_id,
                parent_agent_id: Some(member.parent_thread_id),
                agent_name,
                agent_status: member.status,
                last_task_message: member
                    .last_task_message
                    .as_deref()
                    .map(bounded_list_agents_preview),
            });
        }

        Ok(agents)
    }

    pub(crate) async fn list_agents_page(
        &self,
        current_session_source: &SessionSource,
        path_prefix: Option<&str>,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> CodexResult<ListedAgentsPage> {
        let agents = self
            .list_agents(current_session_source, path_prefix)
            .await?;
        let total_count = agents.len();
        let start = match cursor {
            Some(cursor) => agents
                .iter()
                .position(|agent| agent.agent_id.to_string() == cursor)
                .map(|index| index.saturating_add(1))
                .ok_or_else(|| {
                    CodexErr::InvalidRequest("list_agents cursor is no longer valid".to_string())
                })?,
            None => 0,
        };
        let limit = limit
            .unwrap_or(LIST_AGENTS_DEFAULT_LIMIT)
            .clamp(1, LIST_AGENTS_MAX_LIMIT);
        let requested_end = start.saturating_add(limit).min(total_count);
        let mut end = start;
        while end < requested_end {
            let candidate_end = end + 1;
            let candidate = ListedAgentsPage {
                agents: agents[start..candidate_end].to_vec(),
                next_cursor: (candidate_end < total_count)
                    .then(|| agents[candidate_end - 1].agent_id.to_string()),
                total_count,
            };
            let serialized_len = serde_json::to_vec(&candidate)
                .map_err(|err| CodexErr::Fatal(format!("failed to serialize list_agents: {err}")))?
                .len();
            if serialized_len > LIST_AGENTS_MAX_SERIALIZED_BYTES {
                if end == start {
                    return Err(CodexErr::InvalidRequest(format!(
                        "agent {} exceeds the list_agents response byte limit",
                        agents[end].agent_id
                    )));
                }
                break;
            }
            end = candidate_end;
        }
        let next_cursor =
            (end < total_count).then(|| agents[end.saturating_sub(1)].agent_id.to_string());
        Ok(ListedAgentsPage {
            agents: agents[start..end].to_vec(),
            next_cursor,
            total_count,
        })
    }

    pub(super) fn prepare_agent_metadata(
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
            ephemeral: config.ephemeral,
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
        if self.upgrade()?.is_thread_closing(parent_thread_id) {
            return Err(CodexErr::UnsupportedOperation(format!(
                "cannot spawn a child while agent {parent_thread_id} is closing"
            )));
        }
        if depth == 1 {
            self.state.register_root_thread(parent_thread_id);
        }
        let agent_metadata = self.prepare_agent_metadata(
            reservation,
            config,
            agent_path,
            agent_role,
            preferred_agent_nickname,
        )?;
        let mut agent_metadata = agent_metadata;
        agent_metadata.parent_thread_id = Some(parent_thread_id);
        agent_metadata.depth = Some(depth);
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
        self.manager
            .upgrade()
            .ok_or_else(|| CodexErr::UnsupportedOperation("thread manager dropped".to_string()))
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

    #[cfg(test)]
    pub(crate) fn register_current_only_agent_for_test(
        &self,
        agent_id: ThreadId,
        parent_thread_id: ThreadId,
        depth: i32,
    ) {
        self.state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("current-only test identity should reserve a slot")
            .commit(AgentMetadata {
                agent_id: Some(agent_id),
                parent_thread_id: Some(parent_thread_id),
                depth: Some(depth),
                ephemeral: false,
                ..Default::default()
            });
    }

    #[cfg(test)]
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

        for (parent_thread_id, child_thread_id) in state.list_live_thread_spawn_edges().await {
            children_by_parent
                .entry(parent_thread_id)
                .or_default()
                .push((
                    child_thread_id,
                    self.state
                        .agent_metadata_for_thread(child_thread_id)
                        .unwrap_or(AgentMetadata {
                            agent_id: Some(child_thread_id),
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
        child_thread: &crate::CodexThread,
        child_thread_id: ThreadId,
        session_source: Option<&SessionSource>,
    ) -> CodexResult<()> {
        let Some(parent_thread_id) = session_source.and_then(SessionSource::parent_thread_id)
        else {
            return Ok(());
        };
        if child_thread.config_snapshot().await.ephemeral {
            return Ok(());
        }
        let state = self.upgrade()?;
        let Some(agent_graph_store) = state.agent_graph_store() else {
            return Ok(());
        };
        agent_graph_store
            .upsert_thread_spawn_edge(
                parent_thread_id,
                child_thread_id,
                codex_agent_graph_store::ThreadSpawnEdgeStatus::Open,
            )
            .await
            .map_err(|err| CodexErr::Fatal(format!("failed to persist thread-spawn edge: {err}")))
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

fn bounded_list_agents_preview(message: &str) -> String {
    bounded_utf8_with_ellipsis(message, LIST_AGENTS_TASK_PREVIEW_BYTES)
}

fn bounded_utf8_with_ellipsis(message: &str, maximum_bytes: usize) -> String {
    if message.len() <= maximum_bytes {
        return message.to_string();
    }
    let mut end = maximum_bytes.saturating_sub(3);
    while !message.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &message[..end])
}

pub(super) fn is_internal_supervisor_helper_source(session_source: &SessionSource) -> bool {
    is_goal_supervisor_helper_source(session_source)
}

fn synthetic_supervisor_list_agents_items(page: ListedAgentsPage) -> Vec<RolloutItem> {
    let serialized_page = serde_json::to_string(&page).unwrap_or_else(|error| {
        tracing::error!(%error, "failed to serialize supervisor agent listing");
        serde_json::json!({ "error": format!("failed to serialize agent listing: {error}") })
            .to_string()
    });
    let mut output = FunctionCallOutputPayload::from_text(serialized_page);
    output.success = Some(true);

    vec![
        RolloutItem::ResponseItem(ResponseItem::FunctionCall {
            id: None,
            name: "list_agents".to_string(),
            namespace: None,
            arguments: serde_json::json!({ "limit": LIST_AGENTS_MAX_LIMIT }).to_string(),
            call_id: SUPERVISOR_BOOT_LIST_AGENTS_CALL_ID.to_string(),
            encrypted_function_args: None,
            internal_chat_message_metadata_passthrough: None,
        }),
        RolloutItem::ResponseItem(ResponseItem::FunctionCallOutput {
            id: None,
            call_id: SUPERVISOR_BOOT_LIST_AGENTS_CALL_ID.to_string(),
            output,
            internal_chat_message_metadata_passthrough: None,
        }),
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

fn subagent_assignment_item(session_source: &SessionSource, message: String) -> ResponseItem {
    let agent_path = session_source
        .get_agent_path()
        .map(String::from)
        .unwrap_or_else(|| "this subagent".to_string());
    role_prompt_item(format!(
        "# Subagent Assignment\n\nYou are `{agent_path}`. Your direct assignment from your parent agent is:\n\n{message}"
    ))
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
