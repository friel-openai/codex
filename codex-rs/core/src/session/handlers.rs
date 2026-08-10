use crate::realtime_conversation::handle_audio as handle_realtime_conversation_audio;
use crate::realtime_conversation::handle_close as handle_realtime_conversation_close;
use crate::realtime_conversation::handle_speech as handle_realtime_conversation_speech;
use crate::realtime_conversation::handle_start as handle_realtime_conversation_start;
use crate::realtime_conversation::handle_text as handle_realtime_conversation_text;
use async_channel::Receiver;
use codex_otel::set_parent_from_w3c_trace_context;
use codex_protocol::protocol::Submission;
use tracing::Instrument;
use tracing::debug_span;
use tracing::info_span;

use crate::session::SteerInputError;
use crate::session::TurnInput;
use crate::session::session::Session;
use crate::session::session::SessionSettingsUpdate;

use crate::config::Config;
use crate::review_prompts::resolve_review_request;
use crate::session::spawn_review_thread;
use crate::tasks::CompactTask;
use crate::tasks::TaskStartOutcome;
use crate::tasks::UserShellCommandMode;
use crate::tasks::UserShellCommandTask;
use crate::tasks::execute_user_shell_command;
use crate::user_message_admission::UserMessageAdmission;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::GuardianAssessmentEvent;
use codex_protocol::protocol::GuardianAssessmentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::RealtimeConversationListVoicesResponseEvent;
use codex_protocol::protocol::RealtimeVoicesList;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::protocol::ReviewRequest;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::WarningEvent;
use codex_protocol::request_permissions::RequestPermissionsResponse;
use codex_protocol::request_user_input::RequestUserInputResponse;

use crate::context_manager::is_user_turn_boundary;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::mcp::RequestId as ProtocolRequestId;
use codex_rmcp_client::ElicitationAction;
use codex_rmcp_client::ElicitationResponse;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;
use tracing::debug;
use tracing::info;
use tracing::warn;

pub async fn interrupt(sess: &Arc<Session>) {
    sess.interrupt_task().await;
}

pub async fn clean_background_terminals(sess: &Arc<Session>) {
    sess.close_unified_exec_processes().await;
}

pub async fn realtime_conversation_list_voices(sess: &Session, sub_id: String) {
    sess.send_event_raw(Event {
        id: sub_id,
        msg: EventMsg::RealtimeConversationListVoicesResponse(
            RealtimeConversationListVoicesResponseEvent {
                voices: RealtimeVoicesList::builtin(),
            },
        ),
    })
    .await;
}

pub async fn user_input_or_turn(
    sess: &Arc<Session>,
    sub_id: String,
    op: Op,
    client_user_message_id: Option<String>,
    parent_turn_id: Option<String>,
) {
    let admission = user_input_or_turn_inner(
        sess,
        sub_id.clone(),
        op,
        client_user_message_id,
        parent_turn_id,
    )
    .await;
    sess.pending_user_message_admissions
        .complete(&sub_id, admission);
}

pub async fn update_thread_settings(
    sess: &Arc<Session>,
    sub_id: String,
    thread_settings: ThreadSettingsOverrides,
) {
    let event_id = sub_id.clone();
    let result = sess
        .with_checkpoint_admission("update thread settings", || async {
            let previous_execution_settings = execution_settings(sess).await;
            let updates = thread_settings_update(sess, thread_settings).await;
            match sess.update_settings(updates).await {
                Ok(()) => {
                    if execution_settings(sess).await != previous_execution_settings {
                        if let Some(active_turn) = sess.active_turn.lock().await.as_mut() {
                            active_turn.execution_settings_refresh_requested = true;
                        }
                        crate::goal_supervisor::restart_active_helper_for_execution_settings_change(
                            sess,
                        )
                        .await;
                    }
                    sess.send_event_raw_without_materializing_rollout(Event {
                        id: event_id.clone(),
                        msg: thread_settings_applied_event(sess).await,
                    })
                    .await;
                }
                Err(err) => {
                    sess.send_event_raw(Event {
                        id: event_id,
                        msg: EventMsg::Error(ErrorEvent {
                            message: format!("invalid thread settings override: {err}"),
                            codex_error_info: Some(CodexErrorInfo::BadRequest),
                        }),
                    })
                    .await;
                }
            }
        })
        .await;
    if result.is_err() {
        sess.deliver_event_raw(Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                message: "Thread persistence is in an indeterminate state. Restart this thread before changing its settings."
                    .to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        })
        .await;
    }
}

async fn execution_settings(sess: &Session) -> (String, Option<ReasoningEffort>, Option<String>) {
    let state = sess.state.lock().await;
    let snapshot = state.session_configuration.thread_config_snapshot();
    (
        snapshot.model,
        snapshot.reasoning_effort,
        snapshot.service_tier,
    )
}

async fn thread_settings_update(
    sess: &Session,
    thread_settings: ThreadSettingsOverrides,
) -> SessionSettingsUpdate {
    let ThreadSettingsOverrides {
        environments,
        profile_workspace_roots,
        approval_policy,
        approvals_reviewer,
        sandbox_policy,
        permission_profile,
        active_permission_profile,
        windows_sandbox_level,
        model,
        effort,
        summary,
        service_tier,
        collaboration_mode,
        personality,
    } = thread_settings;
    let collaboration_mode = match collaboration_mode {
        Some(collaboration_mode) => collaboration_mode,
        None => {
            let state = sess.state.lock().await;
            // Model and reasoning effort live in CollaborationMode settings today, so
            // partial thread-settings updates refresh those fields on the active mode.
            state
                .session_configuration
                .collaboration_mode
                .with_updates(model, effort, /*developer_instructions*/ None)
        }
    };
    SessionSettingsUpdate {
        environments,
        profile_workspace_roots,
        approval_policy,
        approvals_reviewer,
        sandbox_policy,
        permission_profile,
        active_permission_profile,
        windows_sandbox_level,
        collaboration_mode: Some(collaboration_mode),
        reasoning_summary: summary,
        service_tier,
        personality,
        ..Default::default()
    }
}

pub(crate) async fn thread_settings_applied_event(sess: &Session) -> EventMsg {
    let snapshot = {
        let state = sess.state.lock().await;
        state.session_configuration.thread_config_snapshot()
    };
    EventMsg::ThreadSettingsApplied(ThreadSettingsAppliedEvent {
        thread_settings: snapshot.into_thread_settings_snapshot(),
    })
}

pub(super) async fn user_input_or_turn_inner(
    sess: &Arc<Session>,
    sub_id: String,
    op: Op,
    client_user_message_id: Option<String>,
    parent_turn_id: Option<String>,
) -> CodexResult<UserMessageAdmission> {
    let Op::UserInput {
        items,
        final_output_json_schema,
        responsesapi_client_metadata,
        additional_context,
        thread_settings,
    } = op
    else {
        unreachable!();
    };
    if sess.active_turn.lock().await.is_none() {
        sess.reload_user_config_layer().await;
    }
    let emit_thread_settings_applied = thread_settings != ThreadSettingsOverrides::default();
    let mut updates = if emit_thread_settings_applied {
        thread_settings_update(sess, thread_settings).await
    } else {
        SessionSettingsUpdate::default()
    };
    updates.final_output_json_schema = Some(final_output_json_schema);

    // new_turn_with_sub_id already emits an error event when settings are invalid.
    let current_context = sess.new_turn_with_sub_id(sub_id.clone(), updates).await?;
    if emit_thread_settings_applied {
        sess.send_event_raw_without_materializing_rollout(Event {
            id: sub_id.clone(),
            msg: thread_settings_applied_event(sess).await,
        })
        .await;
    }
    sess.maybe_emit_model_warnings_for_turn(current_context.as_ref())
        .await;
    match sess
        .steer_input(
            items.clone(),
            additional_context.clone(),
            /*expected_turn_id*/ None,
            client_user_message_id.clone(),
            responsesapi_client_metadata.clone(),
        )
        .await
    {
        Ok(turn_id) => {
            current_context.session_telemetry.user_prompt(&items);
            Ok(UserMessageAdmission::Steered { turn_id })
        }
        Err(SteerInputError::NoActiveTurn(items)) => {
            if let Some(id) = parent_turn_id {
                current_context.turn_metadata_state.set_parent_turn_id(id);
            }
            if let Some(responsesapi_client_metadata) = responsesapi_client_metadata {
                current_context
                    .turn_metadata_state
                    .set_responsesapi_client_metadata(responsesapi_client_metadata);
            }
            current_context.session_telemetry.user_prompt(&items);
            let additional_context_input = {
                let mut state = sess.state.lock().await;
                if sess.persistence_restart_required() {
                    drop(state);
                    sess.send_event_raw(Event {
                        id: sub_id,
                        msg: EventMsg::Error(ErrorEvent {
                            message: "Thread persistence is in an indeterminate state. Restart this thread before starting another turn."
                                .to_string(),
                            codex_error_info: Some(CodexErrorInfo::Other),
                        }),
                    })
                    .await;
                    return Err(CodexErr::TurnAborted);
                }
                state.additional_context.merge(additional_context)
            };
            let mut task_input = additional_context_input
                .into_iter()
                .map(ResponseItem::from)
                .map(TurnInput::ResponseItem)
                .collect::<Vec<_>>();
            if !items.is_empty() {
                task_input.push(TurnInput::UserInput {
                    content: items,
                    client_id: client_user_message_id,
                });
            }
            let task_start = sess
                .spawn_task(
                    Arc::clone(&current_context),
                    task_input,
                    crate::tasks::RegularTask::new(),
                )
                .await;
            if matches!(task_start, TaskStartOutcome::RestartRequired) {
                return Err(CodexErr::TurnAborted);
            }
            Ok(UserMessageAdmission::Started { turn_id: sub_id })
        }
        Err(err) => {
            sess.send_event_raw(Event {
                id: sub_id.clone(),
                msg: EventMsg::Error(err.to_error_event()),
            })
            .await;
            Err(CodexErr::InvalidRequest(format!(
                "failed to admit user message: {err:?}"
            )))
        }
    }
}

/// Queues an inter-agent message, then lets the shared pending-work scheduler
/// decide whether an idle session should start a regular turn.
pub async fn inter_agent_communication(
    sess: &Arc<Session>,
    sub_id: String,
    communication: InterAgentCommunication,
    parent_turn_id: Option<String>,
) {
    let state = Arc::clone(&sess.state).lock_owned().await;
    if sess.persistence_restart_required() {
        return;
    }
    let trigger_turn = communication.trigger_turn;
    sess.input_queue
        .enqueue_mailbox_communication(communication, parent_turn_id.filter(|_| trigger_turn))
        .await;
    drop(state);
    crate::agent_communication::emit_agent_communication_receive(&sub_id);
    if trigger_turn || sess.has_outstanding_durable_sleep() {
        sess.maybe_start_turn_for_pending_work_with_sub_id(sub_id)
            .await;
    }
}

pub async fn run_user_shell_command(sess: &Arc<Session>, sub_id: String, command: String) {
    if let Some((turn_context, cancellation_token)) =
        sess.active_turn_context_and_cancellation_token().await
    {
        let session = Arc::clone(sess);
        tokio::spawn(async move {
            execute_user_shell_command(
                session,
                turn_context,
                command,
                cancellation_token,
                UserShellCommandMode::ActiveTurnAuxiliary,
            )
            .await;
        });
        return;
    }

    let Ok(turn_context) = sess.new_default_turn_with_sub_id(sub_id).await else {
        return;
    };
    sess.spawn_task(
        Arc::clone(&turn_context),
        Vec::new(),
        UserShellCommandTask::new(command),
    )
    .await;
}

pub async fn resolve_elicitation(
    sess: &Arc<Session>,
    server_name: String,
    request_id: ProtocolRequestId,
    decision: codex_protocol::approvals::ElicitationAction,
    content: Option<Value>,
    meta: Option<Value>,
) {
    let action = match decision {
        codex_protocol::approvals::ElicitationAction::Accept => ElicitationAction::Accept,
        codex_protocol::approvals::ElicitationAction::Decline => ElicitationAction::Decline,
        codex_protocol::approvals::ElicitationAction::Cancel => ElicitationAction::Cancel,
    };
    let content = match action {
        // Preserve the legacy fallback for clients that only send an action.
        ElicitationAction::Accept => Some(content.unwrap_or_else(|| serde_json::json!({}))),
        ElicitationAction::Decline | ElicitationAction::Cancel => None,
        _ => None,
    };
    let response = ElicitationResponse {
        action,
        content,
        meta,
    };
    let request_id = match request_id {
        ProtocolRequestId::String(value) => {
            rmcp::model::NumberOrString::String(std::sync::Arc::from(value))
        }
        ProtocolRequestId::Integer(value) => rmcp::model::NumberOrString::Number(value),
    };
    if let Err(err) = sess
        .resolve_elicitation(server_name, request_id, response)
        .await
    {
        warn!(
            error = %err,
            "failed to resolve elicitation request in session"
        );
    }
}

/// Propagate a user's exec approval decision to the session.
/// Also optionally applies an execpolicy amendment.
pub async fn exec_approval(
    sess: &Arc<Session>,
    approval_id: String,
    turn_id: Option<String>,
    decision: ReviewDecision,
) {
    let event_turn_id = turn_id.unwrap_or_else(|| approval_id.clone());
    if let ReviewDecision::ApprovedExecpolicyAmendment {
        proposed_execpolicy_amendment,
    } = &decision
        && let Err(err) = sess
            .persist_execpolicy_amendment(proposed_execpolicy_amendment)
            .await
    {
        let message = format!("Failed to apply execpolicy amendment: {err}");
        tracing::warn!("{message}");
        let warning = EventMsg::Warning(WarningEvent { message });
        sess.send_event_raw(Event {
            id: event_turn_id.clone(),
            msg: warning,
        })
        .await;
    }
    match decision {
        ReviewDecision::Abort => {
            sess.interrupt_task().await;
        }
        other => sess.notify_approval(&approval_id, other).await,
    }
}

pub async fn patch_approval(sess: &Arc<Session>, id: String, decision: ReviewDecision) {
    match decision {
        ReviewDecision::Abort => {
            sess.interrupt_task().await;
        }
        other => sess.notify_approval(&id, other).await,
    }
}

pub async fn request_user_input_response(
    sess: &Arc<Session>,
    id: String,
    response: RequestUserInputResponse,
) {
    sess.notify_user_input_response(&id, response).await;
}

pub async fn request_permissions_response(
    sess: &Arc<Session>,
    id: String,
    response: RequestPermissionsResponse,
) {
    sess.notify_request_permissions_response(&id, response)
        .await;
}

pub async fn dynamic_tool_response(sess: &Arc<Session>, id: String, response: DynamicToolResponse) {
    sess.notify_dynamic_tool_response(&id, response).await;
}

pub async fn refresh_mcp_servers(sess: &Session) {
    let Ok(_checkpoint_admission) = sess.lock_checkpoint_admission("refresh MCP servers").await
    else {
        return;
    };
    sess.services.mcp_runtime.reconnect_on_next_refresh();
    sess.request_mcp_runtime_refresh();
}

pub async fn reload_user_config(sess: &Arc<Session>) {
    sess.reload_user_config_layer().await;
}

pub async fn compact(sess: &Arc<Session>, sub_id: String) {
    let Ok(turn_context) = sess.new_default_turn_with_sub_id(sub_id).await else {
        return;
    };

    sess.spawn_task(Arc::clone(&turn_context), Vec::new(), CompactTask)
        .await;
}

#[cfg(test)]
pub async fn thread_rollback(sess: &Arc<Session>, sub_id: String, num_turns: u32) {
    let checkpoint_admission = Arc::clone(&sess.checkpoint_admission_lock)
        .lock_owned()
        .await;
    thread_rollback_with_admission(sess, sub_id, num_turns, checkpoint_admission).await;
}

#[expect(
    clippy::await_holding_invalid_type,
    reason = "rollback keeps active-turn admission closed until the replacement turn is installed"
)]
async fn thread_rollback_with_admission(
    sess: &Arc<Session>,
    sub_id: String,
    num_turns: u32,
    checkpoint_admission: OwnedMutexGuard<()>,
) {
    if num_turns == 0 {
        sess.send_event_raw(Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                message: "num_turns must be >= 1".to_string(),
                codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
            }),
        })
        .await;
        return;
    }

    let history_rewrite = Arc::clone(&sess.history_rewrite_lock).lock_owned().await;
    let active_turn = sess.active_turn.lock().await;
    if active_turn.is_some() {
        sess.send_event_raw(Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                message: "Cannot rollback while a turn is in progress.".to_string(),
                codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
            }),
        })
        .await;
        return;
    }

    let Ok(turn_context) = sess.new_default_turn_with_sub_id(sub_id).await else {
        return;
    };
    let state = Arc::clone(&sess.state).lock_owned().await;
    if sess.persistence_restart_required() {
        drop(state);
        sess.deliver_event_raw(Event {
            id: turn_context.sub_id.clone(),
            msg: EventMsg::Error(ErrorEvent {
                message: "Thread persistence is in an indeterminate state. Restart this thread before rolling it back."
                    .to_string(),
                codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
            }),
        })
        .await;
        return;
    }
    let live_thread = match sess.live_thread_for_persistence("rollback thread") {
        Ok(live_thread) => live_thread,
        Err(_) => {
            sess.send_event_raw(Event {
                id: turn_context.sub_id.clone(),
                msg: EventMsg::Error(ErrorEvent {
                    message: "thread rollback requires persisted thread history".to_string(),
                    codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
                }),
            })
            .await;
            return;
        }
    };
    if let Err(err) = live_thread.flush().await {
        sess.send_event_raw(Event {
            id: turn_context.sub_id.clone(),
            msg: EventMsg::Error(ErrorEvent {
                message: format!("failed to flush thread persistence for rollback replay: {err}"),
                codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
            }),
        })
        .await;
        return;
    }

    let stored_history = match live_thread.load_history(/*include_archived*/ false).await {
        Ok(history) => history,
        Err(err) => {
            sess.send_event_raw(Event {
                id: turn_context.sub_id.clone(),
                msg: EventMsg::Error(ErrorEvent {
                    message: format!("failed to load thread history for rollback replay: {err}"),
                    codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
                }),
            })
            .await;
            return;
        }
    };

    let rollback_event = ThreadRolledBackEvent { num_turns };
    let rollback_msg = EventMsg::ThreadRolledBack(rollback_event.clone());
    let replay_items = stored_history
        .items
        .into_iter()
        .chain(std::iter::once(RolloutItem::EventMsg(rollback_msg.clone())))
        .collect::<Vec<_>>();
    let state = super::CheckpointMutationGuard::new(checkpoint_admission, state, Arc::clone(sess));
    sess.complete_thread_rollback(
        turn_context,
        replay_items,
        rollback_event,
        state,
        history_rewrite,
    )
    .await;
    drop(active_turn);
}

#[expect(
    clippy::await_holding_invalid_type,
    reason = "memory-mode persistence must remain ordered with checkpoint persistence"
)]
pub(super) async fn persist_thread_memory_mode_update(
    sess: &Arc<Session>,
    mode: ThreadMemoryMode,
) -> anyhow::Result<()> {
    let state = sess
        .lock_state_for_persistence_mutation("update thread memory mode")
        .await?;
    let live_thread = sess.live_thread_for_mutation("update thread memory mode")?;
    live_thread.persist().await?;
    live_thread.flush().await?;
    live_thread
        .update_memory_mode(mode, /*include_archived*/ false)
        .await?;
    live_thread.flush().await?;
    drop(state);
    Ok(())
}

/// Persists thread-level memory mode metadata for the active session.
///
/// This does not involve the model and only affects whether the thread is
/// eligible for future memory generation.
pub async fn set_thread_memory_mode(sess: &Arc<Session>, sub_id: String, mode: ThreadMemoryMode) {
    if let Err(err) = persist_thread_memory_mode_update(sess, mode).await {
        warn!("Failed to persist thread memory mode update to rollout: {err}");
        let event = Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                message: err.to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        };
        sess.send_event_raw(event).await;
    }
}

async fn shutdown_session_runtime(sess: &Arc<Session>) {
    if let Some(startup_prewarm) = sess.take_session_startup_prewarm().await {
        startup_prewarm.abort().await;
    }
    let _ = sess.conversation.shutdown().await;
    sess.abort_all_tasks(TurnAbortReason::Interrupted).await;
    sess.hooks().shutdown().await;
    sess.async_hook_results.close();
    while sess.async_hook_results.try_recv().is_ok() {}
    sess.services
        .unified_exec_manager
        .terminate_all_processes()
        .await;
    if let Err(err) = sess.services.code_mode_service.shutdown().await {
        warn!("failed to shutdown code mode session: {err}");
    }
    sess.stop_mcp_prewarm_worker().await;
    {
        let _refresh = sess.mcp_refresh.acquire().await;
        sess.mcp_refresh.close();
        sess.services.mcp_runtime.shutdown().await;
    }
    sess.guardian_review_session.shutdown().await;

    crate::hook_runtime::run_session_end_hooks(sess).await;
}

async fn emit_thread_stop_lifecycle(sess: &Session) {
    for contributor in sess.services.extensions.thread_lifecycle_contributors() {
        contributor
            .on_thread_stop(codex_extension_api::ThreadStopInput {
                session_store: &sess.services.session_extension_data,
                thread_store: &sess.services.thread_extension_data,
            })
            .await;
    }
}

pub async fn shutdown(sess: &Arc<Session>, sub_id: String) -> bool {
    shutdown_session_runtime(sess).await;
    info!("Shutting down Codex instance");
    let history = sess.clone_history().await;
    let turn_count = history
        .raw_items()
        .iter()
        .filter(|item| is_user_turn_boundary(item))
        .count();
    sess.services.session_telemetry.counter(
        "codex.conversation.turn.count",
        i64::try_from(turn_count).unwrap_or(0),
        &[],
    );

    emit_thread_stop_lifecycle(sess.as_ref()).await;

    // Gracefully flush and shutdown thread persistence on session end so tests
    // that inspect durable state do not race with the background writer.
    if let Some(live_thread) = sess.live_thread()
        && let Err(e) = live_thread.shutdown().await
    {
        warn!("failed to shutdown thread persistence: {e}");
        let event = Event {
            id: sub_id.clone(),
            msg: EventMsg::Error(ErrorEvent {
                message: "Failed to shutdown thread persistence".to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        };
        sess.send_event_raw(event).await;
    }

    let event = Event {
        id: sub_id,
        msg: EventMsg::ShutdownComplete,
    };
    sess.services
        .rollout_thread_trace
        .record_protocol_event(&event.msg);
    sess.deliver_event_raw(event).await;
    sess.services
        .rollout_thread_trace
        .record_ended(codex_rollout_trace::RolloutStatus::Completed);
    true
}

pub async fn review(
    sess: &Arc<Session>,
    config: &Arc<Config>,
    sub_id: String,
    review_request: ReviewRequest,
) {
    let Ok(turn_context) = sess.new_default_turn_with_sub_id(sub_id.clone()).await else {
        return;
    };
    sess.maybe_emit_model_warnings_for_turn(turn_context.as_ref())
        .await;
    #[allow(deprecated)]
    match resolve_review_request(review_request, &turn_context.cwd) {
        Ok(resolved) => {
            spawn_review_thread(
                Arc::clone(sess),
                Arc::clone(config),
                turn_context.clone(),
                sub_id,
                resolved,
            )
            .await;
        }
        Err(err) => {
            let event = Event {
                id: sub_id,
                msg: EventMsg::Error(ErrorEvent {
                    message: err.to_string(),
                    codex_error_info: Some(CodexErrorInfo::Other),
                }),
            };
            sess.send_event(&turn_context, event.msg).await;
        }
    }
}

#[cfg(test)]
pub(super) async fn submission_loop(
    sess: Arc<Session>,
    config: Arc<Config>,
    rx_sub: Receiver<Submission>,
) {
    let (tx_control_sub, rx_control_sub) = async_channel::bounded(1);
    drop(tx_control_sub);
    submission_loop_with_control(sess, config, rx_sub, rx_control_sub).await;
}

pub(super) async fn submission_loop_with_control(
    sess: Arc<Session>,
    config: Arc<Config>,
    rx_sub: Receiver<Submission>,
    rx_control_sub: Receiver<Submission>,
) {
    struct PendingCheckpointAcknowledgements<'a>(&'a Session);

    impl Drop for PendingCheckpointAcknowledgements<'_> {
        fn drop(&mut self) {
            self.0.clear_checkpoint_submission_acknowledgements();
        }
    }

    let _pending_checkpoint_acknowledgements = PendingCheckpointAcknowledgements(sess.as_ref());
    // To break out of this loop, send Op::Shutdown.
    let mut shutdown_received = false;
    let mut ordinary_open = true;
    let mut control_open = true;
    let mut pending_ordinary = None;
    while ordinary_open || control_open || pending_ordinary.is_some() {
        if pending_ordinary.is_none() {
            tokio::select! {
                biased;
                control = rx_control_sub.recv(), if control_open => match control {
                    Ok(sub) => {
                        if matches!(sub.op, Op::Shutdown) {
                            shutdown_received = shutdown_while_serving_control(
                                &sess,
                                &config,
                                sub,
                                &rx_control_sub,
                            )
                            .await;
                            break;
                        }
                        dispatch_submission(&sess, &config, sub, None).await;
                    }
                    Err(_) => control_open = false,
                },
                ordinary = rx_sub.recv(), if ordinary_open => match ordinary {
                    Ok(sub) => pending_ordinary = Some(sub),
                    Err(_) => ordinary_open = false,
                },
            }
            continue;
        }

        let checkpoint_admission = Arc::clone(&sess.checkpoint_admission_lock).lock_owned();
        tokio::pin!(checkpoint_admission);
        loop {
            tokio::select! {
                biased;
                control = rx_control_sub.recv(), if control_open => match control {
                    Ok(sub) => {
                        if matches!(sub.op, Op::Shutdown) {
                            shutdown_received = shutdown_while_serving_control(
                                &sess,
                                &config,
                                sub,
                                &rx_control_sub,
                            )
                            .await;
                            break;
                        }
                        dispatch_submission(&sess, &config, sub, None).await;
                    }
                    Err(_) => control_open = false,
                },
                admission = &mut checkpoint_admission => {
                    let Some(sub) = pending_ordinary.take() else {
                        break;
                    };
                    if matches!(sub.op, Op::Shutdown) {
                        shutdown_received = dispatch_submission(
                            &sess,
                            &config,
                            sub,
                            Some(admission),
                        )
                        .await;
                    } else {
                        dispatch_submission(&sess, &config, sub, Some(admission)).await;
                    }
                    break;
                }
            }
            if shutdown_received {
                break;
            }
        }
        if shutdown_received {
            break;
        }
    }
    // If the submission loop exits because the channels closed without an
    // explicit shutdown op, still run session teardown.
    if !shutdown_received {
        shutdown_session_runtime(&sess).await;
        emit_thread_stop_lifecycle(sess.as_ref()).await;
        if let Some(live_thread) = sess.live_thread()
            && let Err(err) = live_thread.shutdown().await
        {
            warn!("failed to shutdown thread persistence after submission channel closed: {err}");
        }
    }
    debug!("Agent loop exited");
}

async fn shutdown_while_serving_control(
    sess: &Arc<Session>,
    config: &Arc<Config>,
    shutdown_sub: Submission,
    rx_control_sub: &Receiver<Submission>,
) -> bool {
    let shutdown = dispatch_submission(sess, config, shutdown_sub, None);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            result = &mut shutdown => return result,
            control = rx_control_sub.recv() => match control {
                Ok(sub) if matches!(sub.op, Op::ResolveElicitation { .. }) => {
                    dispatch_submission(sess, config, sub, None).await;
                }
                Ok(sub) => {
                    warn!(op = ?sub.op, "ignoring control submission after shutdown started");
                }
                Err(_) => return shutdown.await,
            }
        }
    }
}

async fn dispatch_submission(
    sess: &Arc<Session>,
    config: &Arc<Config>,
    sub: Submission,
    mut checkpoint_admission: Option<OwnedMutexGuard<()>>,
) -> bool {
    let checkpoint_submission_acknowledgement =
        sess.take_checkpoint_submission_acknowledgement(&sub.id);
    debug!(?sub, "Submission");
    if sess.persistence_restart_required() && !matches!(&sub.op, Op::Shutdown) {
        if matches!(&sub.op, Op::UserInput { .. }) {
            sess.pending_user_message_admissions
                .complete(&sub.id, Err(CodexErr::TurnAborted));
        }
        if let Some(acknowledgement) = checkpoint_submission_acknowledgement {
            let _ = acknowledgement.send(Err(CodexErr::TurnAborted));
        }
        sess.deliver_event_raw(Event {
            id: sub.id.clone(),
            msg: EventMsg::Error(ErrorEvent {
                message: "Thread persistence is in an indeterminate state. Restart this thread before continuing."
                    .to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        })
        .await;
        return false;
    }
    let dispatch_span = submission_dispatch_span(&sub);
    let thread_rollback_admission = if matches!(&sub.op, Op::ThreadRollback { .. }) {
        checkpoint_admission.take()
    } else {
        None
    };
    let dispatch = async {
        match sub.op.clone() {
            Op::Interrupt => {
                interrupt(sess).await;
                false
            }
            Op::CleanBackgroundTerminals => {
                clean_background_terminals(sess).await;
                false
            }
            Op::RealtimeConversationStart(params) => {
                if let Err(err) = Box::pin(handle_realtime_conversation_start(
                    sess,
                    sub.id.clone(),
                    params,
                ))
                .await
                {
                    sess.send_event_raw(Event {
                        id: sub.id.clone(),
                        msg: EventMsg::Error(ErrorEvent {
                            message: err.to_string(),
                            codex_error_info: Some(CodexErrorInfo::Other),
                        }),
                    })
                    .await;
                }
                false
            }
            Op::RealtimeConversationAudio(params) => {
                handle_realtime_conversation_audio(sess, sub.id.clone(), params).await;
                false
            }
            Op::RealtimeConversationText(params) => {
                handle_realtime_conversation_text(sess, sub.id.clone(), params).await;
                false
            }
            Op::RealtimeConversationSpeech(params) => {
                handle_realtime_conversation_speech(sess, sub.id.clone(), params).await;
                false
            }
            Op::RealtimeConversationClose => {
                handle_realtime_conversation_close(sess, sub.id.clone()).await;
                false
            }
            Op::RealtimeConversationListVoices => {
                realtime_conversation_list_voices(sess, sub.id.clone()).await;
                false
            }
            Op::UserInput { .. } => {
                user_input_or_turn(
                    sess,
                    sub.id.clone(),
                    sub.op,
                    sub.client_user_message_id,
                    sub.parent_turn_id,
                )
                .await;
                false
            }
            Op::ThreadSettings { thread_settings } => {
                update_thread_settings(sess, sub.id.clone(), thread_settings).await;
                false
            }
            Op::InterAgentCommunication { communication } => {
                inter_agent_communication(sess, sub.id.clone(), communication, sub.parent_turn_id)
                    .await;
                false
            }
            Op::ExecApproval {
                id: approval_id,
                turn_id,
                decision,
            } => {
                exec_approval(sess, approval_id, turn_id, decision).await;
                false
            }
            Op::PatchApproval { id, decision } => {
                patch_approval(sess, id, decision).await;
                false
            }
            Op::UserInputAnswer { id, response } => {
                request_user_input_response(sess, id, response).await;
                false
            }
            Op::RequestPermissionsResponse { id, response } => {
                request_permissions_response(sess, id, response).await;
                false
            }
            Op::DynamicToolResponse { id, response } => {
                dynamic_tool_response(sess, id, response).await;
                false
            }
            Op::RefreshMcpServers => {
                refresh_mcp_servers(sess).await;
                false
            }
            Op::ReloadUserConfig => {
                reload_user_config(sess).await;
                false
            }
            Op::Compact => {
                compact(sess, sub.id.clone()).await;
                false
            }
            Op::ThreadRollback { num_turns } => {
                let Some(checkpoint_admission) = thread_rollback_admission else {
                    sess.deliver_event_raw(Event {
                        id: sub.id.clone(),
                        msg: EventMsg::Error(ErrorEvent {
                            message: "thread rollback admission was not acquired".to_string(),
                            codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
                        }),
                    })
                    .await;
                    return false;
                };
                thread_rollback_with_admission(
                    sess,
                    sub.id.clone(),
                    num_turns,
                    checkpoint_admission,
                )
                .await;
                false
            }
            Op::SetThreadMemoryMode { mode } => {
                set_thread_memory_mode(sess, sub.id.clone(), mode).await;
                false
            }
            Op::RunUserShellCommand { command } => {
                run_user_shell_command(sess, sub.id.clone(), command).await;
                false
            }
            Op::ResolveElicitation {
                server_name,
                request_id,
                decision,
                content,
                meta,
            } => {
                resolve_elicitation(sess, server_name, request_id, decision, content, meta).await;
                false
            }
            Op::Shutdown => shutdown(sess, sub.id.clone()).await,
            Op::Review { review_request } => {
                review(sess, config, sub.id.clone(), review_request).await;
                false
            }
            Op::ApproveGuardianDeniedAction { event } => {
                approve_guardian_denied_action(sess, event).await;
                false
            }
            _ => false, // Ignore unknown ops; enum is non_exhaustive to allow extensions.
        }
    }
    .instrument(dispatch_span);
    let should_exit = if checkpoint_admission.is_some() {
        sess.with_inherited_checkpoint_admission(dispatch).await
    } else {
        dispatch.await
    };
    if let Some(acknowledgement) = checkpoint_submission_acknowledgement {
        match checkpoint_admission.take() {
            Some(admission) => {
                let _ = acknowledgement.send(Ok(admission));
            }
            None => {
                let session = Arc::clone(sess);
                tokio::spawn(async move {
                    let admission = Arc::clone(&session.checkpoint_admission_lock)
                        .lock_owned()
                        .await;
                    let admission_result = if session.persistence_restart_required() {
                        Err(CodexErr::TurnAborted)
                    } else {
                        Ok(admission)
                    };
                    let _ = acknowledgement.send(admission_result);
                });
            }
        }
    }
    drop(checkpoint_admission);
    should_exit
}

async fn approve_guardian_denied_action(sess: &Arc<Session>, event: GuardianAssessmentEvent) {
    if event.status != GuardianAssessmentStatus::Denied {
        warn!(
            review_id = event.id.as_str(),
            "ignoring approval for non-denied Guardian assessment"
        );
        return;
    }

    let approved_action = serde_json::json!({
        "action": &event.action,
        "outcome": "allowed",
    });
    let approved_action_json = match serde_json::to_string_pretty(&approved_action) {
        Ok(approved_action_json) => approved_action_json,
        Err(error) => {
            warn!(%error, review_id = event.id.as_str(), "failed to serialize approved Guardian action");
            return;
        }
    };
    let approval_prefix = crate::guardian::AUTO_REVIEW_DENIED_ACTION_APPROVAL_DEVELOPER_PREFIX;
    let text = format!(
        r#"{approval_prefix}

Treat this as approval to perform that exact action in the same context in which it was originally requested.
Do not assume this also authorizes similar operations with different payloads.

Approved action:
{approved_action_json}"#,
    );
    let items = vec![ResponseItem::from(ResponseInputItem::Message {
        role: "developer".to_string(),
        content: vec![ContentItem::InputText { text }],
        phase: None,
    })];

    let _ = sess
        .inject_no_new_turn(items, /*current_turn_context*/ None)
        .await;
}

pub(super) fn submission_dispatch_span(sub: &Submission) -> tracing::Span {
    let op_name = sub.op.kind();
    let span_name = format!("op.dispatch.{op_name}");
    let dispatch_span = match &sub.op {
        Op::RealtimeConversationAudio(_) => {
            debug_span!(
                "submission_dispatch",
                otel.name = span_name.as_str(),
                submission.id = sub.id.as_str(),
                codex.op = op_name
            )
        }
        _ => info_span!(
            "submission_dispatch",
            otel.name = span_name.as_str(),
            submission.id = sub.id.as_str(),
            codex.op = op_name
        ),
    };
    if let Some(trace) = sub.trace.as_ref()
        && !set_parent_from_w3c_trace_context(&dispatch_span, trace)
    {
        warn!(
            submission.id = sub.id.as_str(),
            "ignoring invalid submission trace carrier"
        );
    }
    dispatch_span
}
