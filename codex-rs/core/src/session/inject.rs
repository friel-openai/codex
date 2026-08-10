use super::input_queue::TurnInput;
use super::session::Session;
use super::turn_context::TurnContext;
use crate::codex_thread::TryStartTurnIfIdleError;
use crate::codex_thread::TryStartTurnIfIdleRejectionReason;
use crate::state::ActiveTurn;
use crate::state::TurnState;
use crate::tasks::MailboxParentProvenance;
use crate::tasks::RegularTask;
use crate::tasks::TaskStartOutcome;
use codex_protocol::ResponseItemId;
use codex_protocol::config_types::ModeKind;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::RolloutItem;
use codex_thread_store::LoadThreadHistoryParams;
use futures::future::BoxFuture;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn response_item_with_id(items: &[ResponseItem], item_id: &ResponseItemId) -> Option<ResponseItem> {
    items
        .iter()
        .find(|item| item.id() == Some(item_id))
        .cloned()
}

fn persisted_response_item_with_id(
    items: &[RolloutItem],
    item_id: &ResponseItemId,
) -> Option<ResponseItem> {
    items.iter().rev().find_map(|item| match item {
        RolloutItem::ResponseItem(item) => (item.id() == Some(item_id)).then(|| item.clone()),
        RolloutItem::Compacted(compacted) => compacted
            .replacement_history
            .as_deref()
            .and_then(|items| response_item_with_id(items, item_id)),
        RolloutItem::SessionMeta(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::InterAgentCommunication(_)
        | RolloutItem::InterAgentCommunicationMetadata { .. }
        | RolloutItem::TurnContext(_)
        | RolloutItem::WorldState(_)
        | RolloutItem::EventMsg(_) => None,
    })
}

impl Session {
    async fn load_persisted_subagent_completion_item(
        &self,
        rollout_path: &Option<PathBuf>,
        item_id: &ResponseItemId,
    ) -> CodexResult<Option<ResponseItem>> {
        let persisted = self
            .services
            .thread_store
            .load_latest_model_context(LoadThreadHistoryParams {
                thread_id: self.thread_id(),
                rollout_path: rollout_path.clone(),
                include_archived: true,
            })
            .await
            .map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to read the parent rollout for a subagent completion notification: {err}"
                ))
            })?
            .items;
        Ok(persisted_response_item_with_id(&persisted, item_id))
    }

    /// Injects raw response items as one operation relative to rollback replay and publication.
    ///
    /// The lock spans active-turn injection or, when idle, initial context establishment, history
    /// mutation, rollout persistence, and the final flush. Rollback therefore observes the entire
    /// injection before its replay snapshot or waits until it can publish the new active segment.
    pub(crate) async fn inject_response_items(
        self: &Arc<Self>,
        items: Vec<ResponseItem>,
    ) -> CodexResult<()> {
        let operation_session = Arc::clone(self);
        let operation = tokio::spawn(async move {
            operation_session
                .with_checkpoint_admission("inject response items", || async {
                    let _history_rewrite = Arc::clone(&operation_session.history_rewrite_lock)
                        .lock_owned()
                        .await;
                    let active_task_kind = operation_session
                        .active_turn
                        .lock()
                        .await
                        .as_ref()
                        .map(|active_turn| active_turn.task.as_ref().map(|task| task.kind));
                    if matches!(active_task_kind, Some(Some(kind)) if kind != crate::state::TaskKind::Regular)
                        || matches!(active_task_kind, Some(None))
                    {
                        return Err(CodexErr::InvalidRequest(
                            "response items cannot be injected into a non-steerable turn"
                                .to_string(),
                        ));
                    }

                    let items = match active_task_kind {
                        Some(Some(crate::state::TaskKind::Regular)) => {
                            let Err(items) = operation_session.inject_if_running(items).await
                            else {
                                operation_session.flush_rollout().await?;
                                return Ok(());
                            };
                            items
                        }
                        Some(Some(_)) | Some(None) => unreachable!(
                            "non-steerable active tasks were rejected before response injection"
                        ),
                        None => items,
                    };
                    let turn_context = operation_session
                        .new_default_turn_with_sub_id(operation_session.next_internal_sub_id())
                        .await?;
                    if operation_session.reference_context_item().await.is_none() {
                        let step_context = operation_session
                            .capture_step_context(
                                Arc::clone(&turn_context),
                                &CancellationToken::new(),
                            )
                            .await?;
                        operation_session
                            .record_context_updates_and_set_reference_context_item(
                                step_context.as_ref(),
                            )
                            .await?;
                    }
                    operation_session
                        .inject_no_new_turn_locked(items, Some(turn_context.as_ref()))
                        .await?;
                    operation_session.flush_rollout().await?;
                    Ok(())
                })
                .await
                .map_err(|_| CodexErr::TurnAborted)?
        });
        let supervisor_session = Arc::clone(self);
        tokio::spawn(async move {
            match operation.await {
                Ok(result) => result,
                Err(_) => {
                    supervisor_session.require_restart_after_indeterminate_persistence();
                    if let Some(live_thread) = supervisor_session.live_thread() {
                        let _ = live_thread.require_restart_and_discard().await;
                    }
                    Err(CodexErr::InternalAgentDied)
                }
            }
        })
        .await
        .map_err(|_| CodexErr::InternalAgentDied)?
    }

    /// Queues a legacy subagent completion at an active turn boundary or records it while idle.
    ///
    /// Active turns preserve response causality by recording this item only after their in-flight
    /// model output. The receipt resolves after that boundary makes the stable item ID durable.
    pub(crate) async fn record_subagent_completion_notification(
        &self,
        item: ResponseItem,
    ) -> CodexResult<()> {
        if item.id().is_none() {
            return Err(CodexErr::InvalidRequest(
                "subagent completion notification requires a stable response item id".to_string(),
            ));
        }
        match self
            .input_queue
            .enqueue_durable_response_item_for_active_turn(&self.active_turn, item)
            .await
        {
            Ok(receipt) => receipt
                .await
                .map_err(|_| CodexErr::InternalAgentDied)?
                .map_err(CodexErr::Fatal),
            Err(item) => {
                let turn_context = self
                    .new_default_turn_with_sub_id(self.next_internal_sub_id())
                    .await?;
                self.record_subagent_completion_notification_at_history_boundary(turn_context, item)
                    .await
            }
        }
    }

    /// Persists one queued completion at a model-history boundary before sampling can continue.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "parent history mutation must remain ordered after durable completion persistence"
    )]
    pub(crate) fn record_subagent_completion_notification_at_history_boundary(
        &self,
        turn_context: Arc<TurnContext>,
        item: ResponseItem,
    ) -> BoxFuture<'_, CodexResult<()>> {
        Box::pin(async move {
            let Some(item_id) = item.id().cloned() else {
                return Err(CodexErr::InvalidRequest(
                    "subagent completion notification requires a stable response item id"
                        .to_string(),
                ));
            };
            self.with_checkpoint_admission("record subagent completion notification", || async {
            let _history_rewrite = self.history_rewrite_lock.lock().await;
            if self.persistence_restart_required() {
                return Err(CodexErr::TurnAborted);
            }
            let Some(live_thread) = self.live_thread() else {
                return Err(CodexErr::Fatal(format!(
                    "thread {} has no durable writer for a subagent completion notification",
                    self.thread_id()
                )));
            };

            live_thread.persist().await.map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to materialize the parent rollout for a subagent completion notification: {err}"
                ))
            })?;
            live_thread.flush().await.map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to reconcile subagent completion notification: {err}"
                ))
            })?;
            let rollout_path = self.current_rollout_path().await.map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to resolve the parent rollout for a subagent completion notification: {err}"
                ))
            })?;
            let notification_items = [item];
            let (prepared_items, image_preparations) = self
                .prepare_conversation_items_for_history(
                    turn_context.as_ref(),
                    &notification_items,
                );
            debug_assert!(image_preparations.is_empty());
            let prepared_item = prepared_items
                .into_owned()
                .into_iter()
                .next()
                .ok_or_else(|| {
                    CodexErr::Fatal(
                        "completion notification preparation returned no response item".to_string(),
                    )
                })?;
            let mut state = self.state.lock().await;
            if self.persistence_restart_required() {
                return Err(CodexErr::TurnAborted);
            }
            let recorded_item = response_item_with_id(
                state.clone_history().raw_items(),
                &item_id,
            );
            let mut persisted_item = self
                .load_persisted_subagent_completion_item(&rollout_path, &item_id)
                .await?;
            let item_to_persist = recorded_item.clone().unwrap_or(prepared_item);
            let mut append_required = persisted_item.is_none();
            let mut append_result_was_error = false;
            while persisted_item.is_none() {
                if append_required {
                    append_result_was_error = live_thread
                        .append_items(&[RolloutItem::ResponseItem(item_to_persist.clone())])
                        .await
                        .inspect_err(|err| {
                            tracing::warn!(
                                "subagent completion append needs reconciliation before retry: {err}"
                            );
                        })
                        .is_err();
                    append_required = false;
                }
                if let Err(err) = live_thread.flush().await {
                    tracing::warn!(
                        "subagent completion flush needs reconciliation before retry: {err}"
                    );
                }
                match self
                    .load_persisted_subagent_completion_item(&rollout_path, &item_id)
                    .await
                {
                    Ok(found_item) => {
                        persisted_item = found_item;
                        if persisted_item.is_none() && append_result_was_error {
                            append_required = true;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            "subagent completion readback needs reconciliation before retry: {err}"
                        );
                    }
                }
                if persisted_item.is_none() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            let Some(persisted_item) = persisted_item else {
                return Err(CodexErr::Fatal(
                    "completion notification readback ended without a persisted item".to_string(),
                ));
            };
            if recorded_item.is_none() {
                let items = [persisted_item];
                state.current_time_reminder.note_recorded_items(&items);
                state.record_items(
                    items.iter(),
                    turn_context.model_info.truncation_policy.into(),
                );
                drop(state);
                let event = codex_protocol::protocol::Event {
                    id: turn_context.sub_id.clone(),
                    msg: codex_protocol::protocol::EventMsg::RawResponseItem(
                        codex_protocol::protocol::RawResponseItemEvent {
                            item: items[0].clone(),
                        },
                    ),
                };
                self.services
                    .rollout_thread_trace
                    .record_protocol_event(&event.msg);
                if let Err(err) = self.tx_event.send(event).await {
                    tracing::debug!("failed to send raw completion response item: {err}");
                }
            }
            Ok(())
            })
            .await
            .map_err(|_| CodexErr::TurnAborted)?
        })
    }

    /// Returns the input if there is no active turn to inject into.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub async fn inject_if_running(
        &self,
        input: Vec<ResponseItem>,
    ) -> Result<(), Vec<ResponseItem>> {
        let mut active = self.active_turn.lock().await;
        match active.as_mut() {
            Some(active_turn)
                if active_turn
                    .task
                    .as_ref()
                    .is_some_and(|task| task.kind == crate::state::TaskKind::Regular) =>
            {
                let state = self.state.lock().await;
                if self.persistence_restart_required() {
                    return Err(input);
                }
                self.input_queue
                    .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                        active_turn.turn_state.as_ref(),
                        input.into_iter().map(TurnInput::ResponseItem).collect(),
                    )
                    .await;
                drop(state);
                Ok(())
            }
            Some(_) | None => Err(input),
        }
    }

    /// Starts a regular turn with the provided input only if automatic idle work
    /// is allowed for the current session state.
    ///
    /// This is the shared gate for extension-initiated idle work. It refuses to
    /// start a turn when user/client-triggered work is queued or any task is
    /// still active. Work without user input is also rejected in Plan mode.
    /// Active Review tasks are covered by the active-task check because Review
    /// turns are not steerable.
    pub(crate) async fn try_start_turn_if_idle(
        self: &Arc<Self>,
        input: Vec<TurnInput>,
    ) -> Result<(), TryStartTurnIfIdleError> {
        if input.is_empty() {
            return Ok(());
        }
        let input_on_admission_failure = input.clone();
        let input_on_persistence_failure = input.clone();
        let input_persisted_receiver = self
            .with_checkpoint_admission("start an idle extension turn", || async {
                let has_user_input = input.iter().any(|item| {
                    matches!(item, TurnInput::UserInput { content, .. } if !content.is_empty())
                });
                if self.has_pending_turn_start_work().await {
                    return Err(TryStartTurnIfIdleError::new(
                        TryStartTurnIfIdleRejectionReason::PendingTriggerTurn,
                        input,
                    ));
                }
                if !has_user_input && self.collaboration_mode().await.mode == ModeKind::Plan {
                    return Err(TryStartTurnIfIdleError::new(
                        TryStartTurnIfIdleRejectionReason::PlanMode,
                        input,
                    ));
                }

                let turn_state = {
                    let mut active_turn = self.active_turn.lock().await;
                    if active_turn.is_some() {
                        return Err(TryStartTurnIfIdleError::new(
                            TryStartTurnIfIdleRejectionReason::Busy,
                            input,
                        ));
                    }
                    let active_turn = active_turn.get_or_insert_with(ActiveTurn::default);
                    Arc::clone(&active_turn.turn_state)
                };

                if self.has_pending_turn_start_work().await {
                    self.clear_reserved_idle_turn(&turn_state).await;
                    self.maybe_start_turn_for_pending_work().await;
                    return Err(TryStartTurnIfIdleError::new(
                        TryStartTurnIfIdleRejectionReason::PendingTriggerTurn,
                        input,
                    ));
                }

                let turn_context = match self
                    .new_default_turn_with_sub_id(uuid::Uuid::new_v4().to_string())
                    .await
                {
                    Ok(turn_context) => turn_context,
                    Err(_) => {
                        self.clear_reserved_idle_turn(&turn_state).await;
                        return Err(TryStartTurnIfIdleError::new(
                            TryStartTurnIfIdleRejectionReason::PersistenceFailed,
                            input,
                        ));
                    }
                };
                if !has_user_input && turn_context.mode == ModeKind::Plan {
                    self.clear_reserved_idle_turn(&turn_state).await;
                    self.maybe_start_turn_for_pending_work().await;
                    return Err(TryStartTurnIfIdleError::new(
                        TryStartTurnIfIdleRejectionReason::PlanMode,
                        input,
                    ));
                }
                self.maybe_emit_model_warnings_for_turn(turn_context.as_ref())
                    .await;
                if self.has_pending_turn_start_work().await {
                    self.clear_reserved_idle_turn(&turn_state).await;
                    self.maybe_start_turn_for_pending_work().await;
                    return Err(TryStartTurnIfIdleError::new(
                        TryStartTurnIfIdleRejectionReason::PendingTriggerTurn,
                        input,
                    ));
                }
                let still_reserved = {
                    let active_turn = self.active_turn.lock().await;
                    active_turn.as_ref().is_some_and(|active_turn| {
                        active_turn.task.is_none()
                            && Arc::ptr_eq(&active_turn.turn_state, &turn_state)
                    })
                };
                if !still_reserved {
                    self.clear_reserved_idle_turn(&turn_state).await;
                    return Err(TryStartTurnIfIdleError::new(
                        TryStartTurnIfIdleRejectionReason::Busy,
                        input,
                    ));
                }

                let (input_persisted_sender, input_persisted_receiver) =
                    has_user_input.then(tokio::sync::oneshot::channel).unzip();
                let original_input = input.clone();
                let task_input = if has_user_input {
                    self.clear_connector_selection().await;
                    for item in &input {
                        if let TurnInput::UserInput { content, .. } = item {
                            turn_context.session_telemetry.user_prompt(content);
                        }
                    }
                    input
                } else {
                    self.input_queue
                        .extend_pending_input_for_turn_state(turn_state.as_ref(), input)
                        .await;
                    Vec::new()
                };
                let task_start = self
                    .start_task(
                        turn_context,
                        task_input,
                        RegularTask::new(),
                        input_persisted_sender,
                        MailboxParentProvenance::Ignore,
                    )
                    .await;
                if !matches!(task_start, TaskStartOutcome::Started) {
                    self.clear_reserved_idle_turn(&turn_state).await;
                    return Err(TryStartTurnIfIdleError::new(
                        if matches!(task_start, TaskStartOutcome::RestartRequired) {
                            TryStartTurnIfIdleRejectionReason::PersistenceFailed
                        } else {
                            TryStartTurnIfIdleRejectionReason::Busy
                        },
                        original_input,
                    ));
                }
                Ok(input_persisted_receiver)
            })
            .await
            .unwrap_or_else(|_| {
                Err(TryStartTurnIfIdleError::new(
                    TryStartTurnIfIdleRejectionReason::PersistenceFailed,
                    input_on_admission_failure,
                ))
            })?;
        if let Some(receiver) = input_persisted_receiver {
            return receiver
                .await
                .unwrap_or(Err(
                    TryStartTurnIfIdleRejectionReason::TaskEndedBeforePersistence,
                ))
                .map_err(|reason| {
                    TryStartTurnIfIdleError::new(reason, input_on_persistence_failure)
                });
        }
        Ok(())
    }

    async fn clear_reserved_idle_turn(&self, turn_state: &Arc<tokio::sync::Mutex<TurnState>>) {
        let mut active_turn_guard = self.active_turn.lock().await;
        if let Some(active_turn) = active_turn_guard.as_ref()
            && active_turn.task.is_none()
            && Arc::ptr_eq(&active_turn.turn_state, turn_state)
        {
            *active_turn_guard = None;
        }
    }

    /// Injects items into active work, or records them without starting a turn.
    pub(crate) async fn inject_no_new_turn(
        &self,
        items: Vec<ResponseItem>,
        current_turn_context: Option<&TurnContext>,
    ) -> CodexResult<()> {
        self.with_checkpoint_admission("inject response items without a turn", || async {
            let _history_rewrite = Arc::clone(&self.history_rewrite_lock).lock_owned().await;
            self.inject_no_new_turn_locked(items, current_turn_context)
                .await
        })
        .await
        .map_err(|_| CodexErr::TurnAborted)?
    }

    async fn inject_no_new_turn_locked(
        &self,
        items: Vec<ResponseItem>,
        current_turn_context: Option<&TurnContext>,
    ) -> CodexResult<()> {
        if self.persistence_restart_required() {
            return Err(CodexErr::TurnAborted);
        }
        let Err(items) = self.inject_if_running(items).await else {
            return Ok(());
        };
        let default_turn_context;
        let turn_context = match current_turn_context {
            Some(turn_context) => turn_context,
            None => {
                default_turn_context = self
                    .new_default_turn_with_sub_id(self.next_internal_sub_id())
                    .await?;
                default_turn_context.as_ref()
            }
        };
        self.try_record_conversation_items(turn_context, &items)
            .await
    }
}
