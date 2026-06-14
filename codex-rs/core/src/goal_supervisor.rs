use crate::agent::control::SpawnAgentForkMode;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::session::session::Session;
use chrono::Utc;
use codex_features::Feature;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadGoal;
use codex_protocol::protocol::ThreadGoalStatus;
use codex_protocol::protocol::ThreadGoalUpdatedEvent;
use codex_protocol::user_input::UserInput;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::warn;

pub(crate) const GOAL_SUPERVISOR_ROLE_NAME: &str = "goal_supervisor";
const MIN_SUPERVISOR_SNOOZE_SECONDS: u64 = 1;

pub(crate) struct GoalSupervisorRuntimeState {
    active_helper_id: Mutex<Option<ThreadId>>,
    active_goal_id: Mutex<Option<String>>,
    snoozed_until: Mutex<Option<Instant>>,
    scheduled_wakeup: Mutex<bool>,
    last_action: Mutex<Option<SupervisorActionRecord>>,
    snooze_records: Mutex<Vec<SupervisorSnoozeRecord>>,
    pending_post_compaction_activation: Mutex<Option<PostCompactionActivation>>,
}

impl GoalSupervisorRuntimeState {
    pub(crate) fn new() -> Self {
        Self {
            active_helper_id: Mutex::new(None),
            active_goal_id: Mutex::new(None),
            snoozed_until: Mutex::new(None),
            scheduled_wakeup: Mutex::new(false),
            last_action: Mutex::new(None),
            snooze_records: Mutex::new(Vec::new()),
            pending_post_compaction_activation: Mutex::new(None),
        }
    }
}

/// One-use context for a supervisor check-in started after mid-turn compaction.
#[derive(Debug)]
struct PostCompactionActivation {
    parent_turn_id: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum SupervisorActionKind {
    CompactParentContext,
    FollowupTask,
    Snooze,
}

#[derive(Clone, Debug)]
struct SupervisorActionRecord {
    kind: SupervisorActionKind,
    sent_at: chrono::DateTime<Utc>,
    delivered_parent_message: Option<InterAgentCommunication>,
    snoozed_seconds: Option<u64>,
}

#[derive(Clone, Debug)]
struct SupervisorSnoozeRecord {
    sent_at: chrono::DateTime<Utc>,
    snoozed_seconds: u64,
}

pub(crate) fn is_goal_supervisor_helper_source(session_source: &SessionSource) -> bool {
    matches!(
        session_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_role: Some(agent_role),
            ..
        }) if agent_role == GOAL_SUPERVISOR_ROLE_NAME
    )
}

pub(crate) async fn mark_post_compaction_activation_if_supervised_goal_active(
    session: &Arc<Session>,
    parent_turn_id: &str,
) -> bool {
    if !session.enabled(Feature::Goals) || !session.enabled(Feature::GoalSupervisor) {
        return false;
    }
    let Some(state_db) = session.services.state_db.as_ref() else {
        return false;
    };
    let goal = match state_db
        .thread_goals()
        .get_thread_goal(session.thread_id)
        .await
    {
        Ok(goal) => goal,
        Err(err) => {
            warn!(
                thread_id = %session.thread_id,
                "failed to read active goal after mid-turn compaction: {err}"
            );
            return false;
        }
    };
    if !matches!(
        goal,
        Some(goal) if goal.status == codex_state::ThreadGoalStatus::Active
    ) {
        return false;
    }
    *session
        .goal_supervisor_runtime
        .pending_post_compaction_activation
        .lock()
        .await = Some(PostCompactionActivation {
        parent_turn_id: parent_turn_id.to_string(),
    });
    true
}

pub(crate) async fn clear_post_compaction_activation_for_turn_start(session: &Arc<Session>) {
    *session
        .goal_supervisor_runtime
        .pending_post_compaction_activation
        .lock()
        .await = None;
}

pub(crate) async fn maybe_start_supervisor_checkin(
    session: &Arc<Session>,
    goal_id: &str,
    goal: &ThreadGoal,
) -> anyhow::Result<()> {
    if session.active_turn.lock().await.is_some()
        || session.input_queue.has_trigger_turn_mailbox_items().await
    {
        return Ok(());
    }

    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    if let Some(helper_id) = active_helper_id {
        let active_goal_id = session
            .goal_supervisor_runtime
            .active_goal_id
            .lock()
            .await
            .clone();
        let status = session.services.agent_control.get_status(helper_id).await;
        if matches!(status, AgentStatus::PendingInit | AgentStatus::Running)
            && active_goal_id.as_deref() == Some(goal_id)
        {
            return Ok(());
        }
        finish_supervisor_helper(session, helper_id).await?;
    }

    let now = Instant::now();
    let snoozed_until = *session.goal_supervisor_runtime.snoozed_until.lock().await;
    if let Some(snoozed_until) = snoozed_until
        && now < snoozed_until
    {
        schedule_supervisor_wakeup(session, snoozed_until.duration_since(now)).await;
        return Ok(());
    }
    if let Some(delay) = persisted_snooze_delay(session, goal_id).await? {
        schedule_supervisor_wakeup(session, delay).await;
        return Ok(());
    }

    let helper_id = spawn_supervisor_helper(session, goal).await?;
    *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await = Some(helper_id);
    *session.goal_supervisor_runtime.active_goal_id.lock().await = Some(goal_id.to_string());
    Ok(())
}

pub(crate) async fn clear_supervisor_snooze_for_goal(
    session: &Arc<Session>,
    goal_id: &str,
) -> anyhow::Result<()> {
    *session.goal_supervisor_runtime.snoozed_until.lock().await = None;
    if let Some(state_db) = session.services.state_db.as_ref() {
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                goal_id,
                /*snoozed_until_ms*/ None,
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn finish_supervisor_helper(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let cleared_active_helper = {
        let mut active_helper_id = session
            .goal_supervisor_runtime
            .active_helper_id
            .lock()
            .await;
        if *active_helper_id != Some(helper_thread_id) {
            false
        } else {
            *active_helper_id = None;
            true
        }
    };
    if !cleared_active_helper {
        return Ok(false);
    }
    *session.goal_supervisor_runtime.active_goal_id.lock().await = None;
    session
        .services
        .agent_control
        .finish_internal_helper_thread(helper_thread_id)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(true)
}

pub(crate) async fn snooze_supervisor_helper(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
    delay_seconds: u64,
) -> anyhow::Result<Option<u64>> {
    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    if active_helper_id != Some(helper_thread_id) {
        return Ok(None);
    }
    let active_goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    let Some(active_goal_id) = active_goal_id else {
        let _ = finish_supervisor_helper(session, helper_thread_id).await?;
        return Ok(None);
    };
    let delay_seconds = delay_seconds.max(MIN_SUPERVISOR_SNOOZE_SECONDS);
    if let Some(state_db) = session.services.state_db.as_ref()
        && let Some(goal) = state_db
            .thread_goals()
            .get_thread_goal(session.thread_id)
            .await?
    {
        if goal.goal_id != active_goal_id {
            let _ = finish_supervisor_helper(session, helper_thread_id).await?;
            return Ok(None);
        }
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                &active_goal_id,
                Some(Utc::now().timestamp_millis() + (delay_seconds as i64 * 1000)),
            )
            .await?;
    }
    *session.goal_supervisor_runtime.snoozed_until.lock().await =
        Some(Instant::now() + Duration::from_secs(delay_seconds));
    record_snooze_action(session, delay_seconds).await;
    let _ = finish_supervisor_helper(session, helper_thread_id).await?;
    schedule_supervisor_wakeup(session, Duration::from_secs(delay_seconds)).await;
    Ok(Some(delay_seconds))
}

pub(crate) async fn record_followup_action(
    session: &Arc<Session>,
    delivered_parent_message: &InterAgentCommunication,
) {
    record_action(
        session,
        SupervisorActionRecord {
            kind: SupervisorActionKind::FollowupTask,
            sent_at: Utc::now(),
            delivered_parent_message: Some(delivered_parent_message.clone()),
            snoozed_seconds: None,
        },
    )
    .await;
}

pub(crate) async fn record_compact_parent_context_action(session: &Arc<Session>) {
    record_action(
        session,
        SupervisorActionRecord {
            kind: SupervisorActionKind::CompactParentContext,
            sent_at: Utc::now(),
            delivered_parent_message: None,
            snoozed_seconds: None,
        },
    )
    .await;
}

async fn record_snooze_action(session: &Arc<Session>, snoozed_seconds: u64) {
    let sent_at = Utc::now();
    session
        .goal_supervisor_runtime
        .snooze_records
        .lock()
        .await
        .push(SupervisorSnoozeRecord {
            sent_at,
            snoozed_seconds,
        });
    record_action(
        session,
        SupervisorActionRecord {
            kind: SupervisorActionKind::Snooze,
            sent_at,
            delivered_parent_message: None,
            snoozed_seconds: Some(snoozed_seconds),
        },
    )
    .await;
}

async fn record_action(session: &Arc<Session>, action: SupervisorActionRecord) {
    *session.goal_supervisor_runtime.last_action.lock().await = Some(action);
}

pub(crate) async fn complete_supervised_goal(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    if active_helper_id != Some(helper_thread_id) {
        return Ok(None);
    }
    let active_goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    let Some(active_goal_id) = active_goal_id else {
        let _ = finish_supervisor_helper(session, helper_thread_id).await?;
        return Ok(None);
    };
    let Some(state_db) = session.services.state_db.as_ref() else {
        return Ok(None);
    };
    let updated = state_db
        .thread_goals()
        .update_thread_goal(
            session.thread_id,
            codex_state::GoalUpdate {
                objective: None,
                status: Some(codex_state::ThreadGoalStatus::Complete),
                token_budget: None,
                expected_goal_id: Some(active_goal_id.clone()),
            },
        )
        .await?
        .map(protocol_goal_from_state);
    if let Some(goal) = updated.as_ref() {
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                &active_goal_id,
                /*snoozed_until_ms*/ None,
            )
            .await?;
        session
            .send_event_raw(Event {
                id: format!("goal-supervisor-complete-{}", session.thread_id),
                msg: EventMsg::ThreadGoalUpdated(ThreadGoalUpdatedEvent {
                    thread_id: session.thread_id,
                    turn_id: None,
                    goal: goal.clone(),
                }),
            })
            .await;
    }
    let _ = finish_supervisor_helper(session, helper_thread_id).await?;
    Ok(updated)
}

async fn spawn_supervisor_helper(
    session: &Arc<Session>,
    goal: &ThreadGoal,
) -> anyhow::Result<ThreadId> {
    let mut helper_config = (*session.get_config().await).clone();
    helper_config.ephemeral = true;
    let parent_source = session
        .services
        .agent_control
        .get_agent_config_snapshot(session.thread_id)
        .await
        .map(|snapshot| snapshot.session_source)
        .unwrap_or(SessionSource::Cli);
    let depth = next_thread_spawn_depth(&parent_source);
    let supervisor_path = parent_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root)
        .join("goal_supervisor")
        .map_err(anyhow::Error::msg)?;
    let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: session.thread_id,
        depth,
        agent_path: Some(supervisor_path),
        agent_nickname: None,
        agent_role: Some(GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let prompt = supervisor_helper_prompt(session, goal);
    let helper = session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            helper_config,
            Op::UserInput {
                items: vec![UserInput::Text {
                    text: prompt,
                    text_elements: Vec::new(),
                }],
                final_output_json_schema: None,
                responsesapi_client_metadata: None,
                additional_context: Default::default(),
                thread_settings: Default::default(),
            },
            Some(session_source),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: None,
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                parent_thread_id: Some(session.thread_id),
                environments: None,
                initial_task_message: None,
            },
        )
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(helper.thread_id)
}

fn supervisor_helper_prompt(session: &Arc<Session>, goal: &ThreadGoal) -> String {
    format!(
        "# Goal Supervisor Assignment\n\nParent agent id: {}\n\nActive goal objective:\n\n{}\n\nEvaluate whether the parent should continue now, snooze, compact, or mark the goal complete.",
        session.thread_id, goal.objective
    )
}

pub(crate) async fn supervisor_continuity_context_item(
    session: &Arc<Session>,
    goal: &ThreadGoal,
    source_items: &[RolloutItem],
) -> RolloutItem {
    let previous_supervisor_action = session
        .goal_supervisor_runtime
        .last_action
        .lock()
        .await
        .clone();
    let last_parent_message_at = source_items.iter().rev().find_map(|item| match item {
        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => event.completed_at,
        _ => None,
    });
    let post_compaction_parent_turn_id = session
        .goal_supervisor_runtime
        .pending_post_compaction_activation
        .lock()
        .await
        .take()
        .map(|activation| activation.parent_turn_id);
    let snooze_records = session
        .goal_supervisor_runtime
        .snooze_records
        .lock()
        .await
        .clone();
    let snooze_records = snooze_records.into_iter().filter(|record| {
        last_parent_message_at.is_none_or(|completed_at| record.sent_at.timestamp() >= completed_at)
    });
    let (snooze_count_since_last_parent_message, snoozed_seconds_since_last_parent_message) =
        snooze_records.fold((0_u64, 0_u64), |(count, seconds), record| {
            (
                count.saturating_add(1),
                seconds.saturating_add(record.snoozed_seconds),
            )
        });
    let continuity = serde_json::json!({
        "supervisor_identity": "/root/goal_supervisor",
        "activation_reason": if post_compaction_parent_turn_id.is_some() { "post_compaction" } else { "thread_idle" },
        "compaction": post_compaction_parent_turn_id.as_ref().map(|parent_turn_id| serde_json::json!({
            "phase": "mid_turn",
            "reason": "context_limit",
            "parent_turn_id": parent_turn_id,
            "requested_by": "automatic",
        })),
        "previous_supervisor_action": previous_supervisor_action.as_ref().map(|action| serde_json::json!({
            "kind": action.kind,
            "sent_at_utc": action.sent_at.to_rfc3339(),
            "delivered_parent_message": action.delivered_parent_message,
            "snoozed_seconds": action.snoozed_seconds,
        })),
        "goal_timing": {
            "goal_created_at_utc": chrono::DateTime::<Utc>::from_timestamp(goal.created_at, 0).map(|created_at| created_at.to_rfc3339()),
            "seconds_since_goal_created": Utc::now().timestamp().saturating_sub(goal.created_at),
        },
        "parent_timing": {
            "last_parent_message_at_utc": last_parent_message_at.and_then(|completed_at| chrono::DateTime::<Utc>::from_timestamp(completed_at, 0)).map(|completed_at| completed_at.to_rfc3339()),
            "snooze_count_since_last_parent_message": snooze_count_since_last_parent_message,
            "snoozed_seconds_since_last_parent_message": snoozed_seconds_since_last_parent_message,
        },
    });
    let continuity = match serde_json::to_string_pretty(&continuity) {
        Ok(continuity) => continuity,
        Err(err) => format!("failed to serialize goal supervisor continuity: {err}"),
    };
    RolloutItem::ResponseItem(ResponseItem::Message {
        id: None,
        role: "developer".to_string(),
        content: vec![ContentItem::InputText {
            text: format!("# Goal Supervisor Continuity\n\n{continuity}"),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    })
}

async fn persisted_snooze_delay(
    session: &Arc<Session>,
    goal_id: &str,
) -> anyhow::Result<Option<Duration>> {
    let Some(state_db) = session.services.state_db.as_ref() else {
        return Ok(None);
    };
    let Some(snoozed_until_ms) = state_db
        .thread_goals()
        .get_thread_goal_supervisor_snoozed_until_ms(session.thread_id, goal_id)
        .await?
    else {
        return Ok(None);
    };
    let now_ms = Utc::now().timestamp_millis();
    if snoozed_until_ms <= now_ms {
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                goal_id,
                /*snoozed_until_ms*/ None,
            )
            .await?;
        return Ok(None);
    }
    Ok(Some(Duration::from_millis(
        (snoozed_until_ms - now_ms) as u64,
    )))
}

async fn schedule_supervisor_wakeup(session: &Arc<Session>, delay: Duration) {
    let mut scheduled = session
        .goal_supervisor_runtime
        .scheduled_wakeup
        .lock()
        .await;
    if *scheduled {
        return;
    }
    *scheduled = true;
    let session = Arc::clone(session);
    tokio::spawn(async move {
        if let Err(err) = session
            .services
            .time_provider
            .sleep(session.thread_id, delay)
            .await
        {
            warn!(
                thread_id = %session.thread_id,
                "failed to wait for goal supervisor wakeup: {err}"
            );
            *session
                .goal_supervisor_runtime
                .scheduled_wakeup
                .lock()
                .await = false;
            return;
        }
        *session
            .goal_supervisor_runtime
            .scheduled_wakeup
            .lock()
            .await = false;
        session.emit_thread_idle_lifecycle_if_idle().await;
    });
}

pub(crate) fn protocol_goal_from_state(goal: codex_state::ThreadGoal) -> ThreadGoal {
    ThreadGoal {
        thread_id: goal.thread_id,
        objective: goal.objective,
        status: protocol_status_from_state(goal.status),
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
        created_at: goal.created_at.timestamp(),
        updated_at: goal.updated_at.timestamp(),
    }
}

fn protocol_status_from_state(status: codex_state::ThreadGoalStatus) -> ThreadGoalStatus {
    match status {
        codex_state::ThreadGoalStatus::Active => ThreadGoalStatus::Active,
        codex_state::ThreadGoalStatus::Paused => ThreadGoalStatus::Paused,
        codex_state::ThreadGoalStatus::Blocked => ThreadGoalStatus::Blocked,
        codex_state::ThreadGoalStatus::UsageLimited => ThreadGoalStatus::UsageLimited,
        codex_state::ThreadGoalStatus::BudgetLimited => ThreadGoalStatus::BudgetLimited,
        codex_state::ThreadGoalStatus::Complete => ThreadGoalStatus::Complete,
    }
}
