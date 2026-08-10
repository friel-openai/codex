use super::*;
use crate::agent::registry::CompletionStatusClaim;
use crate::agent::registry::CompletionWatcherRegistration;
use crate::agent::registry::CompletionWatcherWaitOutcome;
use crate::agent::status::is_final;
use crate::codex_thread::CodexThread;
use crate::session_prefix::format_inter_agent_completion_message;
use crate::session_prefix::format_subagent_notification_message;

/// Input whose submission remains paired with any required communication telemetry context.
enum AgentDeliveryInput {
    UserInput(Vec<UserInput>),
    InterAgentCommunication {
        communication: InterAgentCommunication,
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

/// Selects whether a completion watcher owns the status already visible when it starts.
enum CompletionWatcherStart {
    /// Fresh spawn and explicit resume watchers must process a terminal status already present.
    ObserveCurrent,
    /// Follow-up delivery has already observed the current status and must wait for a later event.
    AwaitNext,
}

/// A follow-up watcher registered before its submission can publish a terminal status.
struct LoadedAgentCompletionWatcher {
    session_source: SessionSource,
    child_reference: String,
    child_agent_path: Option<AgentPath>,
    lifecycle: Arc<crate::agent::registry::AgentLifecycle>,
    registration: CompletionWatcherRegistration,
}

/// A terminal remains claimed until its canonical parent accepts the notification.
struct PendingCompletionStatus {
    status: AgentStatus,
    claim: Option<CompletionStatusClaim>,
}

impl PendingCompletionStatus {
    fn from_claim(claim: CompletionStatusClaim) -> Self {
        Self {
            status: claim.status().clone(),
            claim: Some(claim),
        }
    }

    fn observed(status: AgentStatus) -> Self {
        Self {
            status,
            claim: None,
        }
    }
}

/// A registered parent either remains addressable or has left this agent tree.
enum CompletionParent {
    Loaded {
        thread: Arc<CodexThread>,
        _transition: tokio::sync::OwnedMutexGuard<()>,
    },
    Gone,
}

impl AgentControl {
    /// Deliver work to an addressable agent while serializing cold reload with completion cleanup.
    pub(crate) async fn deliver_input_to_agent(
        &self,
        config: Config,
        agent_id: ThreadId,
        input: Vec<UserInput>,
        delivery: AgentInputDelivery,
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        self.deliver_agent_input(
            config,
            agent_id,
            AgentDeliveryInput::UserInput(input),
            delivery,
            parent_turn_id,
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
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        self.deliver_agent_input(
            config,
            agent_id,
            AgentDeliveryInput::InterAgentCommunication {
                communication,
                context,
            },
            delivery,
            parent_turn_id,
        )
        .await
    }

    async fn deliver_agent_input(
        &self,
        config: Config,
        agent_id: ThreadId,
        input: AgentDeliveryInput,
        delivery: AgentInputDelivery,
        parent_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let metadata = self.ensure_agent_known(agent_id)?;
        let lifecycle = Arc::clone(&metadata.lifecycle);
        loop {
            let transition = lifecycle.lock_transition().await;
            let state = self.upgrade()?;
            if lifecycle.completion_watcher_active() {
                let completion_targets_calling_parent =
                    match (metadata.parent_thread_id, parent_turn_id.as_deref()) {
                        (Some(parent_thread_id), Some(parent_turn_id)) => {
                            match state.get_thread(parent_thread_id).await {
                                Ok(parent_thread) => parent_thread
                                    .session
                                    .active_turn
                                    .lock()
                                    .await
                                    .as_ref()
                                    .and_then(|active_turn| active_turn.task.as_ref())
                                    .is_some_and(|task| task.turn_context.sub_id == parent_turn_id),
                                Err(_) => false,
                            }
                        }
                        (None, _) | (_, None) => false,
                    };
                // Waiting here would deadlock when the watcher has queued its receipt into this
                // parent turn: the tool call blocks the history boundary that resolves the receipt.
                if completion_targets_calling_parent {
                    if lifecycle
                        .wait_for_completion_watcher_or_pending_terminal(transition)
                        .await
                        == CompletionWatcherWaitOutcome::PendingTerminal
                    {
                        return Err(CodexErr::UnsupportedOperation(format!(
                            "agent {agent_id} completed during this parent turn; process its completion notification before sending more input"
                        )));
                    }
                } else {
                    drop(transition);
                    lifecycle.wait_for_completion_watcher().await;
                }
                continue;
            }

            let multi_agent_version = Box::pin(self.ensure_agent_loaded_locked(
                &state,
                config.clone(),
                agent_id,
                super::resume::AgentLoadProtection::TargetOnly,
            ))
            .await?;
            self.ensure_execution_capacity_for_turn_start(agent_id, input.starts_turn())
                .await?;
            let completion_watcher = if (multi_agent_version != MultiAgentVersion::V2
                || metadata.agent_path.is_none()
                || metadata.agent_role.as_deref()
                    == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME))
                && (input.starts_turn() || delivery == AgentInputDelivery::Interrupt)
            {
                self.prepare_completion_watcher_for_loaded_agent(&state, agent_id)
                    .await
            } else {
                None
            };
            let control = self.clone();
            let delivery_owner = tokio::spawn(async move {
                let _transition = transition;
                let interrupt_result = if delivery == AgentInputDelivery::Interrupt {
                    control.interrupt_agent(agent_id).await.map(drop)
                } else {
                    Ok(())
                };
                let interrupt_succeeded = interrupt_result.is_ok();
                let result = match interrupt_result {
                    Err(err) => Err(err),
                    Ok(()) => match input {
                        AgentDeliveryInput::UserInput(input) => {
                            control
                                .send_input_after_capacity_check(
                                    agent_id,
                                    &state,
                                    input,
                                    parent_turn_id,
                                )
                                .await
                        }
                        AgentDeliveryInput::InterAgentCommunication {
                            communication,
                            context,
                        } => {
                            control
                                .send_inter_agent_communication_after_capacity_check(
                                    agent_id,
                                    &state,
                                    communication,
                                    context,
                                    parent_turn_id,
                                )
                                .await
                        }
                    },
                };
                if let Some(mut completion_watcher) = completion_watcher {
                    let should_spawn = result.is_ok()
                        || (delivery == AgentInputDelivery::Interrupt && interrupt_succeeded)
                        || !completion_watcher.registration.try_retire();
                    if should_spawn {
                        control.spawn_completion_watcher(
                            agent_id,
                            Some(completion_watcher.session_source),
                            completion_watcher.child_reference,
                            completion_watcher.child_agent_path,
                            completion_watcher.lifecycle,
                            completion_watcher.registration,
                            CompletionWatcherStart::AwaitNext,
                        );
                    }
                }
                result
            });
            return delivery_owner
                .await
                .map_err(|_| CodexErr::InternalAgentDied)?;
        }
    }

    /// Publishes a definitive child terminal event and ensures its parent has a watcher.
    pub(crate) fn publish_completion_status<F>(
        &self,
        child_thread_id: ThreadId,
        event_id: String,
        status: AgentStatus,
        multi_agent_version: Option<MultiAgentVersion>,
        publish_status: F,
    ) where
        F: FnOnce(),
    {
        let Some(metadata) = self.get_agent_metadata(child_thread_id) else {
            publish_status();
            return;
        };
        let is_goal_supervisor_helper = metadata.agent_role.as_deref()
            == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME);
        // Pathful MultiAgentV2 completion is owned by `Session::forward_child_completion_to_parent`.
        // Keeping it out of this legacy queue preserves the mailbox contract and prevents a
        // compatibility watcher from claiming an in-memory-only delivery as durable.
        if multi_agent_version == Some(MultiAgentVersion::V2)
            && metadata.agent_path.is_some()
            && !is_goal_supervisor_helper
        {
            publish_status();
            return;
        }
        let requires_registered_watcher = matches!(status, AgentStatus::Shutdown);
        if requires_registered_watcher && !metadata.lifecycle.completion_watcher_registered() {
            publish_status();
            return;
        }
        let parent_thread_id = metadata.parent_thread_id;
        let publication = if parent_thread_id.is_some() && !requires_registered_watcher {
            metadata
                .lifecycle
                .publish_completion_status(event_id, status, publish_status)
        } else {
            metadata
                .lifecycle
                .publish_completion_status_for_registered_watcher(event_id, status, publish_status)
        };
        if !publication.recorded {
            return;
        }
        let Some(parent_thread_id) = parent_thread_id else {
            return;
        };
        let child_reference = metadata
            .agent_path
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| child_thread_id.to_string());
        let Some(registration) = publication.registration else {
            return;
        };
        self.spawn_completion_watcher(
            child_thread_id,
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: metadata.depth.unwrap_or(1),
                agent_path: metadata.agent_path.clone(),
                agent_nickname: metadata.agent_nickname,
                agent_role: metadata.agent_role,
            })),
            child_reference,
            metadata.agent_path,
            metadata.lifecycle,
            registration,
            CompletionWatcherStart::ObserveCurrent,
        );
    }

    /// Starts a detached watcher for sub-agents spawned from another thread.
    ///
    /// This is only enabled for `SubAgentSource::ThreadSpawn`, where a parent thread exists and
    /// can receive completion notifications.
    pub(super) fn maybe_start_completion_watcher(
        &self,
        child_thread_id: ThreadId,
        session_source: Option<SessionSource>,
        child_reference: String,
        child_agent_path: Option<AgentPath>,
        multi_agent_version: MultiAgentVersion,
    ) -> bool {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_role, .. })) =
            session_source.as_ref()
        else {
            return false;
        };
        let is_goal_supervisor_helper =
            agent_role.as_deref() == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME);
        if multi_agent_version == MultiAgentVersion::V2
            && child_agent_path.is_some()
            && !is_goal_supervisor_helper
        {
            return false;
        }
        let lifecycle = self
            .get_agent_metadata(child_thread_id)
            .map(|metadata| metadata.lifecycle)
            .unwrap_or_default();
        let Some(watcher_registration) = lifecycle.try_start_completion_watcher() else {
            return false;
        };
        lifecycle.begin_completion_transition();
        self.spawn_completion_watcher(
            child_thread_id,
            session_source,
            child_reference,
            child_agent_path,
            lifecycle,
            watcher_registration,
            CompletionWatcherStart::ObserveCurrent,
        );
        true
    }

    /// Reloads a registered cold parent while excluding concurrent residency eviction.
    async fn load_completion_parent(
        &self,
        state: &Arc<ThreadManagerState>,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
    ) -> CodexResult<CompletionParent> {
        let Some(parent_metadata) = self.get_agent_metadata(parent_thread_id) else {
            return Ok(CompletionParent::Gone);
        };
        if state.is_thread_closing(parent_thread_id) {
            return Ok(CompletionParent::Gone);
        }
        let parent_lifecycle = Arc::clone(&parent_metadata.lifecycle);
        let transition = parent_lifecycle.lock_transition().await;
        let Some(current_parent_metadata) = self.get_agent_metadata(parent_thread_id) else {
            return Ok(CompletionParent::Gone);
        };
        if !Arc::ptr_eq(&parent_lifecycle, &current_parent_metadata.lifecycle)
            || state.is_thread_closing(parent_thread_id)
        {
            return Ok(CompletionParent::Gone);
        }
        if let Ok(thread) = state.get_thread(parent_thread_id).await {
            return Ok(CompletionParent::Loaded {
                thread,
                _transition: transition,
            });
        }
        // Root sessions are not residency-managed. A missing root runtime is therefore dead,
        // while a registered subagent parent can be reconstructed from its persisted rollout.
        if current_parent_metadata.parent_thread_id.is_none() {
            return Ok(CompletionParent::Gone);
        }

        let config = match state.get_thread(child_thread_id).await {
            Ok(child_thread) => child_thread.config().await.as_ref().clone(),
            Err(_) => {
                let root_thread_id = self.current_membership_root_thread_id();
                state
                    .get_thread(root_thread_id)
                    .await?
                    .config()
                    .await
                    .as_ref()
                    .clone()
            }
        };
        Box::pin(self.ensure_agent_loaded_locked(
            state,
            config,
            parent_thread_id,
            super::resume::AgentLoadProtection::TargetAndCompletionChild(child_thread_id),
        ))
        .await?;
        let thread = state.get_thread(parent_thread_id).await?;
        Ok(CompletionParent::Loaded {
            thread,
            _transition: transition,
        })
    }

    /// Delivers the canonical MultiAgentV2 completion mailbox item to a registered parent.
    ///
    /// A cold subagent parent is reloaded while the completing child remains protected from
    /// residency eviction. An unregistered parent remains a dead address and is not rerouted.
    pub(crate) fn deliver_completion_to_registered_parent(
        &self,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
    ) -> futures::future::BoxFuture<'_, CodexResult<bool>> {
        Box::pin(async move {
            let state = self.upgrade()?;
            match self
                .load_completion_parent(&state, child_thread_id, parent_thread_id)
                .await?
            {
                CompletionParent::Loaded {
                    thread: _,
                    _transition,
                } => {
                    let _parent_transition = _transition;
                    self.ensure_execution_capacity_for_turn_start(
                        parent_thread_id,
                        communication.trigger_turn,
                    )
                    .await?;
                    self.send_inter_agent_communication_after_capacity_check(
                        parent_thread_id,
                        &state,
                        communication,
                        context,
                        /*parent_turn_id*/ None,
                    )
                    .await?;
                    Ok(true)
                }
                CompletionParent::Gone => Ok(false),
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_completion_watcher(
        &self,
        child_thread_id: ThreadId,
        session_source: Option<SessionSource>,
        child_reference: String,
        child_agent_path: Option<AgentPath>,
        lifecycle: Arc<crate::agent::registry::AgentLifecycle>,
        watcher_registration: CompletionWatcherRegistration,
        start: CompletionWatcherStart,
    ) {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            agent_role,
            ..
        })) = session_source
        else {
            return;
        };
        let is_goal_supervisor_helper =
            agent_role.as_deref() == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME);
        let control = self.clone();
        tokio::spawn(async move {
            let mut watcher_registration = watcher_registration;
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
            let mut pending_status = match start {
                CompletionWatcherStart::ObserveCurrent => {
                    let current = control.get_status(child_thread_id).await;
                    match lifecycle.try_claim_completion_status() {
                        Some(claim) => PendingCompletionStatus::from_claim(claim),
                        // `EventMsg::Error` updates the latest status before task teardown emits
                        // the definitive TurnComplete or TurnAborted event. Only that definitive
                        // event enters the lossless queue and can notify the parent.
                        None if matches!(current, AgentStatus::Errored(_)) => {
                            PendingCompletionStatus::from_claim(
                                lifecycle.wait_for_completion_status_claim().await,
                            )
                        }
                        None => PendingCompletionStatus::observed(current),
                    }
                }
                CompletionWatcherStart::AwaitNext => PendingCompletionStatus::from_claim(
                    lifecycle.wait_for_completion_status_claim().await,
                ),
            };
            let uses_inter_agent_completion =
                child_uses_multi_agent_v2 && child_agent_path.is_some();

            loop {
                while !completion_watcher_status_is_terminal_for_agent(
                    &pending_status.status,
                    uses_inter_agent_completion,
                    is_goal_supervisor_helper,
                ) {
                    pending_status = PendingCompletionStatus::from_claim(
                        lifecycle.wait_for_completion_status_claim().await,
                    );
                }
                if !completion_watcher_status_is_terminal_for_agent(
                    &pending_status.status,
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
                            pending_status.status.clone(),
                        )
                        .await;
                    if let Some(claim) = pending_status.claim.as_ref() {
                        let _ = lifecycle.acknowledge_completion_status(claim);
                    }
                    drop(watcher_registration);
                    return;
                }

                let Ok(state) = control.upgrade() else {
                    return;
                };
                if child_agent_path.is_some() && child_uses_multi_agent_v2 {
                    let Some(child_agent_path) = child_agent_path.clone() else {
                        return;
                    };
                    let Some(parent_agent_path) = child_agent_path
                        .as_str()
                        .rsplit_once('/')
                        .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
                    else {
                        return;
                    };
                    let Some(message) = format_inter_agent_completion_message(
                        parent_agent_path.clone(),
                        child_agent_path.clone(),
                        &pending_status.status,
                    ) else {
                        return;
                    };
                    let communication = InterAgentCommunication::new(
                        child_agent_path,
                        parent_agent_path,
                        Vec::new(),
                        message,
                        /*trigger_turn*/ false,
                    );
                    let context = AgentCommunicationContext::new(
                        AgentCommunicationKind::Result,
                        child_thread_id,
                    );
                    let _ = control
                        .send_inter_agent_communication(
                            parent_thread_id,
                            communication,
                            context,
                            /*parent_turn_id*/ None,
                        )
                        .await;
                } else {
                    let message = format_subagent_notification_message(
                        child_reference.as_str(),
                        &pending_status.status,
                    );
                    match control
                        .load_completion_parent(&state, child_thread_id, parent_thread_id)
                        .await
                    {
                        Ok(CompletionParent::Loaded {
                            thread,
                            _transition,
                        }) => {
                            let _parent_transition = _transition;
                            let delivery_result = match pending_status.claim.as_ref() {
                                Some(claim) => {
                                    let item_id = completion_notification_item_id(
                                        child_thread_id,
                                        claim.event_id(),
                                    );
                                    thread
                                        .record_subagent_completion_notification(item_id, message)
                                        .await
                                }
                                None => thread.inject_user_message_without_turn(message).await,
                            };
                            if let Err(err) = delivery_result {
                                warn!(
                                    "failed to persist completion in registered parent {parent_thread_id}: {err}"
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                        }
                        Ok(CompletionParent::Gone) => {
                            if let Some(claim) = pending_status.claim.as_ref() {
                                let _ = lifecycle.acknowledge_completion_status(claim);
                            }
                            drop(watcher_registration);
                            return;
                        }
                        Err(err) => {
                            warn!(
                                "failed to reload registered completion parent {parent_thread_id}: {err}"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            continue;
                        }
                    }
                }
                if let Some(claim) = pending_status.claim.as_ref()
                    && !lifecycle.acknowledge_completion_status(claim)
                {
                    warn!(
                        "completion queue changed before parent {parent_thread_id} acknowledged child {child_thread_id}"
                    );
                    return;
                }

                let _transition = lifecycle.lock_transition().await;
                let child_thread = state.get_thread(child_thread_id).await.ok();
                let completion_quiescent = if lifecycle.has_pending_completion_status() {
                    false
                } else if let Some(child_thread) = child_thread.as_ref() {
                    residency::is_unloadable(child_thread.as_ref()).await
                } else {
                    true
                };
                if completion_quiescent && let Some(child_thread) = child_thread.as_ref() {
                    let config = child_thread.session.get_config().await;
                    let multi_agent_version = child_thread
                        .multi_agent_version()
                        .unwrap_or_else(|| config.multi_agent_version_from_features());
                    control.schedule_agent_residency_trim(
                        config.as_ref(),
                        multi_agent_version,
                        &child_thread.session_source,
                    );
                }
                if completion_quiescent && watcher_registration.try_retire() {
                    return;
                }
                drop(_transition);
                pending_status = PendingCompletionStatus::from_claim(
                    lifecycle.wait_for_completion_status_claim().await,
                );
            }
        });
    }

    async fn prepare_completion_watcher_for_loaded_agent(
        &self,
        state: &Arc<ThreadManagerState>,
        child_thread_id: ThreadId,
    ) -> Option<LoadedAgentCompletionWatcher> {
        let Ok(child_thread) = state.get_thread(child_thread_id).await else {
            return None;
        };
        let thread_config = child_thread.config_snapshot().await;
        let metadata = self.get_agent_metadata(child_thread_id)?;
        let lifecycle = Arc::clone(&metadata.lifecycle);
        let registration = lifecycle.try_start_completion_watcher()?;
        let child_reference = metadata
            .agent_path
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| child_thread_id.to_string());
        Some(LoadedAgentCompletionWatcher {
            session_source: thread_config.session_source,
            child_reference,
            child_agent_path: metadata.agent_path,
            lifecycle,
            registration,
        })
    }
}

fn completion_notification_item_id(
    child_thread_id: ThreadId,
    terminal_event_id: &str,
) -> codex_protocol::ResponseItemId {
    let namespace = uuid::Uuid::from_u128(0x778e_2eb0_817e_4fb7_b8ef_52f3_3304_aaf4);
    let identity = format!("{child_thread_id}:{terminal_event_id}");
    codex_protocol::ResponseItemId::with_suffix(
        "msg",
        uuid::Uuid::new_v5(&namespace, identity.as_bytes()),
    )
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
