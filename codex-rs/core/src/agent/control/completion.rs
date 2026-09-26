//! Delivers terminal child results and completion activity to the agent tree.
//!
//! Sessions capture terminal state; the controller owns routing and queue-only delivery.
//! Delivery remains best effort, with tracing recorded only after the parent accepts it.

use super::AgentInputDelivery;
use super::LocalAgentControl;
use super::residency;
use crate::TurnStartOptions;
use crate::agent::api::AgentTurnOutcome;
use crate::agent::status::is_final;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::config::Config;
use crate::context::SubagentNotification;
use crate::session_prefix::format_inter_agent_completion_message;
use crate::thread_manager::ThreadManagerState;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::items::SubAgentActivityItem;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentActivityKind;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::user_input::UserInput;
use codex_rollout_trace::AgentResultTracePayload;
use codex_rollout_trace::ThreadTraceContext;
use std::sync::Arc;
use tracing::debug;

impl LocalAgentControl {
    /// Routes a captured terminal outcome without retaining the child's live turn context.
    pub(crate) async fn notify_parent_of_terminal_turn(
        &self,
        outcome: AgentTurnOutcome,
        trace: &ThreadTraceContext,
    ) {
        let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            agent_path: Some(child_agent_path),
            ..
        }) = &outcome.source
        else {
            return;
        };
        let parent_thread_id = *parent_thread_id;
        let status = outcome.status;
        let Some(parent_agent_path) = child_agent_path
            .as_str()
            .rsplit_once('/')
            .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
        else {
            return;
        };

        if matches!(status, AgentStatus::Completed(_))
            && let Some(parent_turn_id) = outcome.parent_turn_id
        {
            let initiating_thread_id = match outcome.initiating_agent_path.as_ref() {
                Some(initiating_agent_path) if initiating_agent_path != &parent_agent_path => {
                    self.resolve_agent_reference(
                        outcome.thread_id,
                        &outcome.source,
                        initiating_agent_path.as_str(),
                    )
                    .await
                    .inspect_err(|err| {
                        debug!(
                            "failed to resolve completed activity initiator {initiating_agent_path}: {err}"
                        );
                    })
                    .ok()
                }
                _ => Some(parent_thread_id),
            };
            if let Some(initiating_thread_id) = initiating_thread_id
                && let Err(err) = self
                    .emit_sub_agent_activity(
                        initiating_thread_id,
                        parent_turn_id,
                        SubAgentActivityItem {
                            id: format!("subagent-completed-{}", outcome.turn_id),
                            kind: SubAgentActivityKind::Completed,
                            agent_thread_id: outcome.thread_id,
                            agent_path: child_agent_path.clone(),
                        },
                    )
                    .await
            {
                debug!(
                    "failed to emit completed activity to initiating thread {initiating_thread_id}: {err}"
                );
            }
        }

        let Some(message) = format_inter_agent_completion_message(
            parent_agent_path.clone(),
            child_agent_path.clone(),
            &status,
        ) else {
            return;
        };
        // `communication` owns the message. Keep a second copy only when the
        // recorder will actually need it after parent delivery succeeds.
        let trace_message = trace.is_enabled().then(|| message.clone());
        let communication = InterAgentCommunication::new(
            child_agent_path.clone(),
            parent_agent_path,
            Vec::new(),
            message,
            /*trigger_turn*/ false,
        );
        let context =
            AgentCommunicationContext::new(AgentCommunicationKind::Result, outcome.thread_id);
        if let Err(err) = self
            .send_inter_agent_communication(
                parent_thread_id,
                communication,
                context,
                TurnStartOptions::default(),
            )
            .await
        {
            debug!("failed to notify parent thread {parent_thread_id}: {err}");
            return;
        }
        if let Some(message) = trace_message {
            trace.record_agent_result_interaction(
                outcome.turn_id.as_str(),
                parent_thread_id,
                &AgentResultTracePayload {
                    child_agent_path: child_agent_path.as_str(),
                    message: &message,
                    status: &status,
                },
            );
        }
    }
}

/// Input whose submission remains paired with any required communication telemetry context.
enum AgentDeliveryInput {
    UserInput(Vec<UserInput>),
    InterAgentCommunication {
        communication: Box<InterAgentCommunication>,
        context: AgentCommunicationContext,
    },
}

impl AgentDeliveryInput {
    fn starts_turn(&self) -> bool {
        match self {
            Self::UserInput(_) => true,
            Self::InterAgentCommunication { communication, .. } => communication.trigger_turn,
        }
    }
}

impl LocalAgentControl {
    /// Deliver work to an addressable agent while serializing cold reload with completion cleanup.
    pub(crate) async fn deliver_input_to_agent(
        &self,
        config: Config,
        agent_id: ThreadId,
        input: Vec<UserInput>,
        delivery: AgentInputDelivery,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        self.deliver_agent_input(
            config,
            agent_id,
            AgentDeliveryInput::UserInput(input),
            delivery,
            start_options,
        )
        .await
    }

    /// Deliver a context-bearing communication to an addressable agent, reloading it if needed.
    pub(crate) async fn deliver_inter_agent_communication_to_agent(
        &self,
        config: Config,
        agent_id: ThreadId,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        delivery: AgentInputDelivery,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        self.deliver_agent_input(
            config,
            agent_id,
            AgentDeliveryInput::InterAgentCommunication {
                communication: Box::new(communication),
                context,
            },
            delivery,
            start_options,
        )
        .await
    }

    async fn deliver_agent_input(
        &self,
        config: Config,
        agent_id: ThreadId,
        input: AgentDeliveryInput,
        delivery: AgentInputDelivery,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        let metadata = self.ensure_agent_known(agent_id)?;
        let lifecycle = self
            .state
            .agent_lifecycle(agent_id)
            .ok_or(CodexErr::ThreadNotFound(agent_id))?;
        loop {
            let transition = lifecycle.lock_transition().await;
            let state = self.upgrade()?;
            let completion_transition_pending = if lifecycle.completion_watcher_active() {
                match state.get_thread(agent_id).await {
                    Ok(thread) => completion_watcher_status_is_terminal_for_agent(
                        &thread.agent_status().await,
                        thread.multi_agent_version() == Some(MultiAgentVersion::V2)
                            && metadata.agent_path.is_some(),
                        crate::goal_supervisor::is_goal_supervisor_helper_source(
                            &thread.session_source,
                        ),
                    ),
                    Err(_) => true,
                }
            } else {
                false
            };
            if completion_transition_pending {
                drop(transition);
                lifecycle.wait_for_completion_watcher().await;
                continue;
            }

            let multi_agent_version =
                Box::pin(self.ensure_agent_loaded_locked(&state, config.clone(), agent_id)).await?;
            let thread = state.get_thread(agent_id).await?;
            if input.starts_turn() {
                self.ensure_execution_capacity_for_turn_start(&thread)
                    .await?;
            }
            if multi_agent_version != MultiAgentVersion::V2
                || crate::goal_supervisor::is_goal_supervisor_helper_source(&thread.session_source)
            {
                self.maybe_start_completion_watcher_for_loaded_agent(&state, agent_id)
                    .await;
            }
            if delivery == AgentInputDelivery::Interrupt {
                self.interrupt_agent(agent_id).await?;
            }
            return match &input {
                AgentDeliveryInput::UserInput(input) => {
                    self.send_input_after_capacity_check(
                        agent_id,
                        &state,
                        input.clone(),
                        start_options.clone(),
                    )
                    .await
                }
                AgentDeliveryInput::InterAgentCommunication {
                    communication,
                    context,
                } => {
                    self.send_inter_agent_communication_after_capacity_check(
                        agent_id,
                        &state,
                        communication.as_ref().clone(),
                        context.clone(),
                        start_options.clone(),
                    )
                    .await
                }
            };
        }
    }

    /// Tracks legacy completion cleanup and goal-supervisor retry state.
    ///
    /// This is only enabled for `SubAgentSource::ThreadSpawn`, where a parent thread exists and
    /// can receive completion notifications.
    pub(super) fn maybe_start_completion_watcher(
        &self,
        child_thread_id: ThreadId,
        session_source: Option<SessionSource>,
        child_reference: String,
        child_agent_path: Option<AgentPath>,
    ) -> bool {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            agent_role,
            ..
        })) = session_source
        else {
            return false;
        };
        let is_goal_supervisor_helper =
            agent_role.as_deref() == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME);
        let lifecycle = self
            .state
            .agent_lifecycle(child_thread_id)
            .unwrap_or_default();
        let Some(watcher_registration) = lifecycle.try_start_completion_watcher() else {
            return false;
        };
        let control = self.clone();
        tokio::spawn(async move {
            let _watcher_registration = watcher_registration;
            let mut status_rx = control.subscribe_status(child_thread_id).await.ok();
            let child_uses_multi_agent_v2 = match control.upgrade() {
                Ok(state) => state
                    .get_thread(child_thread_id)
                    .await
                    .ok()
                    .is_none_or(|thread| {
                        thread.multi_agent_version() == Some(MultiAgentVersion::V2)
                    }),
                Err(_) => true,
            };
            let mut status = status_rx
                .as_ref()
                .map(|status_rx| status_rx.borrow().clone())
                .unwrap_or_else(|| AgentStatus::NotFound);
            let uses_inter_agent_completion =
                child_uses_multi_agent_v2 && child_agent_path.is_some();
            if uses_inter_agent_completion && !is_goal_supervisor_helper {
                // V2 sessions deliver captured terminal outcomes directly; a status watcher
                // would race that delivery and report the same terminal turn twice.
                return;
            }

            loop {
                while !completion_watcher_status_is_terminal_for_agent(
                    &status,
                    uses_inter_agent_completion,
                    is_goal_supervisor_helper,
                ) {
                    let Some(receiver) = status_rx.as_mut() else {
                        status = control.get_status(child_thread_id).await;
                        break;
                    };
                    if receiver.changed().await.is_err() {
                        status = control.get_status(child_thread_id).await;
                        break;
                    }
                    status = receiver.borrow().clone();
                }
                if !completion_watcher_status_is_terminal_for_agent(
                    &status,
                    uses_inter_agent_completion,
                    is_goal_supervisor_helper,
                ) {
                    return;
                }
                if is_goal_supervisor_helper {
                    let _ = control
                        .defer_failed_goal_supervisor_helper(
                            parent_thread_id,
                            child_thread_id,
                            status.clone(),
                        )
                        .await;
                    return;
                }

                let Ok(state) = control.upgrade() else {
                    return;
                };
                let _transition = lifecycle.lock_transition().await;
                let child_thread = state.get_thread(child_thread_id).await.ok();
                let status_advanced = status_rx
                    .as_ref()
                    .is_some_and(|receiver| receiver.has_changed().unwrap_or(false));
                let completion_quiescent = if status_advanced {
                    false
                } else if let Some(child_thread) = child_thread.as_ref() {
                    residency::is_unloadable(child_thread.as_ref()).await
                } else {
                    true
                };
                if let Ok(parent_thread) = state.get_thread(parent_thread_id).await {
                    parent_thread
                        .inject_fragment_without_turn(SubagentNotification::new(
                            child_reference.as_str(),
                            status.clone(),
                        ))
                        .await;
                }
                if child_uses_multi_agent_v2 {
                    return;
                }
                if completion_quiescent {
                    return;
                }
                drop(_transition);

                let Some(receiver) = status_rx.as_mut() else {
                    return;
                };
                if receiver.changed().await.is_err() {
                    return;
                }
                status = receiver.borrow().clone();
            }
        });
        true
    }

    async fn maybe_start_completion_watcher_for_loaded_agent(
        &self,
        state: &Arc<ThreadManagerState>,
        child_thread_id: ThreadId,
    ) {
        let Ok(child_thread) = state.get_thread(child_thread_id).await else {
            return;
        };
        let thread_config = child_thread.config_snapshot().await;
        let metadata = self.get_agent_metadata(child_thread_id).unwrap_or_default();
        let child_reference = metadata
            .agent_path
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| child_thread_id.to_string());
        self.maybe_start_completion_watcher(
            child_thread_id,
            Some(thread_config.session_source),
            child_reference,
            metadata.agent_path,
        );
    }
}

fn is_legacy_completion_status(status: &AgentStatus) -> bool {
    is_final(status) || matches!(status, AgentStatus::Interrupted)
}

fn completion_watcher_status_is_terminal(
    status: &AgentStatus,
    uses_inter_agent_completion: bool,
) -> bool {
    if uses_inter_agent_completion {
        is_final(status)
    } else {
        is_legacy_completion_status(status)
    }
}

fn completion_watcher_status_is_terminal_for_agent(
    status: &AgentStatus,
    uses_inter_agent_completion: bool,
    is_goal_supervisor_helper: bool,
) -> bool {
    (is_goal_supervisor_helper && matches!(status, AgentStatus::Interrupted))
        || completion_watcher_status_is_terminal(status, uses_inter_agent_completion)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupted_goal_supervisor_is_terminal_for_completion_watcher() {
        assert!(completion_watcher_status_is_terminal_for_agent(
            &AgentStatus::Interrupted,
            /*uses_inter_agent_completion*/ true,
            /*is_goal_supervisor_helper*/ true,
        ));
        assert!(!completion_watcher_status_is_terminal_for_agent(
            &AgentStatus::Interrupted,
            /*uses_inter_agent_completion*/ true,
            /*is_goal_supervisor_helper*/ false,
        ));
    }
}
