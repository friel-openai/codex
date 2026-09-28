//! Implements shared controller operations using the existing local runtime helpers.
//! Runtime loading, message delivery and shared state remain in their existing modules.

use super::ListedAgentsPage;
use super::LocalAgentControl;
use super::agent_matches_prefix;
use super::spawn::SpawnInitialInput;
use crate::agent::api::AgentConfigUpdate;
use crate::agent::api::AgentControl;
use crate::agent::api::AgentInfo;
use crate::agent::api::AgentInput;
use crate::agent::api::AgentTarget;
use crate::agent::api::AgentTurnOutcome;
use crate::agent::api::DeliveryReceipt;
use crate::agent::api::SendRequest;
use crate::agent::api::SpawnRequest;
use crate::agent::types::AgentExecutionGuard;
use crate::agent::types::AgentMetadata;
use crate::agent::types::LiveAgent;
use crate::agent::types::MessageDeliveryMode;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::codex_thread::GuardianRootSnapshot;
use crate::codex_thread::ThreadConfigSnapshot;
use crate::rollout_budget::RolloutBudgetReminder;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TokenUsage;
use codex_rollout_trace::ThreadTraceContext;
use futures::future::BoxFuture;
use std::collections::HashSet;

impl AgentControl for LocalAgentControl {
    fn identity(&self) -> SessionId {
        self.session_id()
    }

    fn resolve<'a>(
        &'a self,
        caller: ThreadId,
        parent: Option<ThreadId>,
        source: &'a SessionSource,
        target: &'a str,
    ) -> BoxFuture<'a, Result<ThreadId>> {
        Box::pin(async move {
            self.runtime.register_session_root(caller, parent);
            if let Ok(thread_id) = ThreadId::from_string(target) {
                self.ensure_open_agent_known_by_id(caller, thread_id)
                    .await?;
                return Ok(thread_id);
            }
            self.resolve_agent_reference(caller, source, target).await
        })
    }

    fn spawn(
        &self,
        request: SpawnRequest,
    ) -> BoxFuture<'_, Result<(LiveAgent, ThreadConfigSnapshot)>> {
        Box::pin(async move {
            let SpawnRequest {
                caller,
                config,
                input,
                source,
                options,
            } = request;
            let input = match input {
                AgentInput::UserInput(input) => SpawnInitialInput::UserInput(input),
                AgentInput::Message { message, mode } => {
                    if mode != MessageDeliveryMode::TriggerTurn {
                        return Err(CodexErr::InvalidRequest(
                            "spawn input must start the child turn".to_string(),
                        ));
                    }
                    let recipient = source.get_agent_path().ok_or_else(|| {
                        CodexErr::InvalidRequest(
                            "spawned agent is missing a canonical task name".to_string(),
                        )
                    })?;
                    let author = recipient
                        .as_str()
                        .rsplit_once('/')
                        .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
                        .ok_or_else(|| {
                            CodexErr::InvalidRequest("spawn input needs a child path".to_string())
                        })?;
                    SpawnInitialInput::InterAgentCommunication(
                        message.into_communication(author, recipient, mode),
                        AgentCommunicationContext::new(AgentCommunicationKind::Spawn, caller),
                    )
                }
            };
            Box::pin(self.spawn_agent_internal(config, input, Some(source), options)).await
        })
    }

    fn send(&self, request: SendRequest) -> BoxFuture<'_, Result<DeliveryReceipt>> {
        // Trait callers must use the same lifecycle lock and cold reload as local callers.
        Box::pin(LocalAgentControl::send(self, request))
    }

    fn ensure_child_loaded(&self, parent: ThreadId, child: ThreadId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let parent_thread = self.runtime.upgrade()?.get_thread(parent).await?;
            let parent_snapshot = parent_thread.config_snapshot().await;
            self.runtime
                .register_session_root(parent, parent_snapshot.parent_thread_id);
            self.ensure_open_agent_known_by_id_for_explicit_resume(parent, child)
                .await?;
            let config = parent_thread.session.get_config().await.as_ref().clone();
            self.ensure_v2_agent_loaded(config, child, Some(parent_thread))
                .await
        })
    }

    fn interrupt(
        &self,
        caller: ThreadId,
        target: AgentTarget,
        version: MultiAgentVersion,
    ) -> BoxFuture<'_, Result<AgentInfo>> {
        Box::pin(async move {
            let target = self.resolve_target(caller, &target).await?;
            match version {
                MultiAgentVersion::Disabled | MultiAgentVersion::V1 => {
                    let snapshot = self.inspect_agent(target).await?;
                    self.interrupt_agent(target).await?;
                    Ok(snapshot)
                }
                MultiAgentVersion::V2 => self.interrupt_spawned_agent(caller, target).await,
            }
        })
    }

    fn list<'a>(
        &'a self,
        caller: ThreadId,
        parent: Option<ThreadId>,
        source: &'a SessionSource,
        path_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<LiveAgent>>> {
        Box::pin(async move {
            self.runtime.register_session_root(caller, parent);
            let state = self.runtime.upgrade()?;
            let resolved_prefix = path_prefix
                .map(|prefix| {
                    source
                        .get_agent_path()
                        .unwrap_or_else(AgentPath::root)
                        .resolve(prefix)
                        .map_err(CodexErr::UnsupportedOperation)
                })
                .transpose()?;
            let mut metadata = self.runtime.registry.live_agents();
            let root_path = AgentPath::root();
            if let Some(root_thread_id) = self.runtime.registry.agent_id_for_path(&root_path) {
                metadata.push(AgentMetadata {
                    agent_id: Some(root_thread_id),
                    agent_path: Some(root_path),
                    last_task_message: Some("Main thread".to_string()),
                    ..Default::default()
                });
            }
            metadata.sort_by(|left, right| {
                left.agent_path.cmp(&right.agent_path).then_with(|| {
                    left.agent_id
                        .map(|id| id.to_string())
                        .cmp(&right.agent_id.map(|id| id.to_string()))
                })
            });
            let mut agents = Vec::with_capacity(metadata.len());
            for metadata in metadata {
                let Some(thread_id) = metadata.agent_id else {
                    continue;
                };
                if resolved_prefix.as_ref().is_some_and(|prefix| {
                    !agent_matches_prefix(metadata.agent_path.as_ref(), prefix)
                }) {
                    continue;
                }
                // The upstream list contract returns loaded runtimes; list_page includes cold identities.
                let thread = match state.get_thread(thread_id).await {
                    Ok(thread) => thread,
                    Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                agents.push(LiveAgent {
                    thread_id,
                    metadata,
                    status: thread.agent_status().await,
                });
            }
            Ok(agents)
        })
    }

    fn list_page<'a>(
        &'a self,
        caller: ThreadId,
        parent: Option<ThreadId>,
        source: &'a SessionSource,
        path_prefix: Option<&'a str>,
        cursor: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<ListedAgentsPage>> {
        Box::pin(async move {
            self.runtime.register_session_root(caller, parent);
            self.list_agents_page(source, path_prefix, cursor, limit)
                .await
        })
    }

    fn child_agent_paths(&self, parent: ThreadId) -> BoxFuture<'_, Vec<AgentPath>> {
        Box::pin(async move {
            let Ok(current_members) = self.current_agent_members().await else {
                return Vec::new();
            };
            let mut agent_paths = current_members
                .into_iter()
                .filter(|member| member.parent_thread_id == parent)
                .filter_map(|member| member.agent_path)
                .collect::<Vec<_>>();
            let loaded_paths = self
                .runtime
                .open_thread_spawn_children(parent)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(_, metadata)| metadata.agent_path)
                .collect::<HashSet<_>>();
            agent_paths.sort();
            // Stable sorting preserves alphabetical order within each group.
            agent_paths.sort_by_key(|path| !loaded_paths.contains(path));
            agent_paths
        })
    }

    fn check_turn_admission(
        &self,
        version: MultiAgentVersion,
        source: &SessionSource,
    ) -> Result<()> {
        self.ensure_execution_capacity(version, source)
    }

    fn admit_turn(
        &self,
        version: MultiAgentVersion,
        source: &SessionSource,
    ) -> Option<AgentExecutionGuard> {
        self.execution_guard(version, source)
    }

    fn record_usage(&self, usage: TokenUsage) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.record_rollout_budget_usage(&usage) })
    }

    fn turn_finished<'a>(
        &'a self,
        outcome: AgentTurnOutcome,
        trace: &'a ThreadTraceContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.notify_parent_of_terminal_turn(outcome, trace))
    }

    fn service_tier(&self) -> Option<String> {
        self.root_service_tier()
    }

    fn propagate_config_update(&self, update: AgentConfigUpdate) {
        match update {
            AgentConfigUpdate::ServiceTier(tier) => self.set_root_service_tier(tier),
        }
    }

    fn get_guardian_package(&self, agent: ThreadId) -> BoxFuture<'_, Option<GuardianRootSnapshot>> {
        Box::pin(self.root_user_authorization(agent))
    }

    fn pending_budget_reminder<'a>(
        &'a self,
        agent: ThreadId,
        window: &'a str,
    ) -> BoxFuture<'a, Option<RolloutBudgetReminder>> {
        Box::pin(async move { LocalAgentControl::pending_budget_reminder(self, agent, window) })
    }

    fn mark_budget_reminder_delivered<'a>(
        &'a self,
        agent: ThreadId,
        window: &'a str,
        reminder: RolloutBudgetReminder,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            LocalAgentControl::mark_budget_reminder_delivered(self, agent, window, reminder);
        })
    }
}
