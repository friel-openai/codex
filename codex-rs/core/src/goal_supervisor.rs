use crate::agent::control::SpawnAgentForkMode;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::session::session::Session;
use chrono::Utc;
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
use codex_protocol::protocol::ThreadGoalUpdatedEvent;
use codex_protocol::user_input::UserInput;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;

pub(crate) const GOAL_SUPERVISOR_ROLE_NAME: &str = "goal_supervisor";
const MIN_SUPERVISOR_SNOOZE_SECONDS: u64 = 1;

pub(crate) struct GoalSupervisorRuntimeState {
    active_helper_id: Mutex<Option<ThreadId>>,
    active_goal_id: Mutex<Option<String>>,
    snoozed_until: Mutex<Option<Instant>>,
    scheduled_wakeup: Mutex<bool>,
    last_action: Mutex<Option<SupervisorActionRecord>>,
    snooze_records: Mutex<Vec<SupervisorSnoozeRecord>>,
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
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum SupervisorActionKind {
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

pub(crate) async fn maybe_start_supervisor_checkin(
    session: &Arc<Session>,
    goal_id: &str,
    goal: &ThreadGoal,
) -> anyhow::Result<()> {
    if let Some(helper_id) = *session
        .goal_runtime
        .supervisor
        .active_helper_id
        .lock()
        .await
    {
        let active_goal_id = session
            .goal_runtime
            .supervisor
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
    if let Some(snoozed_until) = *session.goal_runtime.supervisor.snoozed_until.lock().await
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
        .goal_runtime
        .supervisor
        .active_helper_id
        .lock()
        .await = Some(helper_id);
    *session.goal_runtime.supervisor.active_goal_id.lock().await = Some(goal_id.to_string());
    Ok(())
}

pub(crate) async fn finish_supervisor_helper(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let mut active_helper_id = session
        .goal_runtime
        .supervisor
        .active_helper_id
        .lock()
        .await;
    if *active_helper_id != Some(helper_thread_id) {
        return Ok(false);
    }
    *active_helper_id = None;
    drop(active_helper_id);
    *session.goal_runtime.supervisor.active_goal_id.lock().await = None;
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
        .goal_runtime
        .supervisor
        .active_helper_id
        .lock()
        .await;
    if active_helper_id != Some(helper_thread_id) {
        return Ok(None);
    }
    let active_goal_id = session
        .goal_runtime
        .supervisor
        .active_goal_id
        .lock()
        .await
        .clone();
    let Some(active_goal_id) = active_goal_id else {
        let _ = finish_supervisor_helper(session, helper_thread_id).await?;
        return Ok(None);
    };
    let delay_seconds = delay_seconds.max(MIN_SUPERVISOR_SNOOZE_SECONDS);
    if let Some(state_db) = session.state_db_for_thread_goals().await?
        && let Some(goal) = state_db.get_thread_goal(session.conversation_id).await?
    {
        if goal.goal_id != active_goal_id {
            let _ = finish_supervisor_helper(session, helper_thread_id).await?;
            return Ok(None);
        }
        state_db
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.conversation_id,
                &active_goal_id,
                Some(Utc::now().timestamp_millis() + (delay_seconds as i64 * 1000)),
            )
            .await?;
    }
    *session.goal_runtime.supervisor.snoozed_until.lock().await =
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

async fn record_snooze_action(session: &Arc<Session>, snoozed_seconds: u64) {
    let sent_at = Utc::now();
    session
        .goal_runtime
        .supervisor
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
    *session.goal_runtime.supervisor.last_action.lock().await = Some(action);
}

pub(crate) async fn complete_supervised_goal(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    let active_helper_id = *session
        .goal_runtime
        .supervisor
        .active_helper_id
        .lock()
        .await;
    if active_helper_id != Some(helper_thread_id) {
        return Ok(None);
    }
    let active_goal_id = session
        .goal_runtime
        .supervisor
        .active_goal_id
        .lock()
        .await
        .clone();
    let Some(active_goal_id) = active_goal_id else {
        let _ = finish_supervisor_helper(session, helper_thread_id).await?;
        return Ok(None);
    };
    let Some(state_db) = session.state_db_for_thread_goals().await? else {
        return Ok(None);
    };
    let updated = state_db
        .update_thread_goal(
            session.conversation_id,
            codex_state::ThreadGoalUpdate {
                objective: None,
                status: Some(codex_state::ThreadGoalStatus::Complete),
                token_budget: None,
                expected_goal_id: Some(active_goal_id.clone()),
            },
        )
        .await?
        .map(crate::goals::protocol_goal_from_state);
    if let Some(goal) = updated.as_ref() {
        state_db
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.conversation_id,
                &active_goal_id,
                None,
            )
            .await?;
        session
            .send_event_raw(Event {
                id: format!("goal-supervisor-complete-{}", session.conversation_id),
                msg: EventMsg::ThreadGoalUpdated(ThreadGoalUpdatedEvent {
                    thread_id: session.conversation_id,
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
        .get_agent_config_snapshot(session.conversation_id)
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
        parent_thread_id: session.conversation_id,
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
                environments: None,
                items: vec![UserInput::Text {
                    text: prompt,
                    text_elements: Vec::new(),
                }],
                final_output_json_schema: None,
                responsesapi_client_metadata: None,
            },
            Some(session_source),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: None,
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
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
        session.conversation_id, goal.objective
    )
}

pub(crate) async fn supervisor_continuity_context_item(
    session: &Arc<Session>,
    goal: &ThreadGoal,
    source_items: &[RolloutItem],
) -> RolloutItem {
    let previous_supervisor_action = session
        .goal_runtime
        .supervisor
        .last_action
        .lock()
        .await
        .clone();
    let last_parent_message_at = source_items.iter().rev().find_map(|item| match item {
        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => event.completed_at,
        _ => None,
    });
    let snooze_records = session
        .goal_runtime
        .supervisor
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
    })
}

async fn persisted_snooze_delay(
    session: &Arc<Session>,
    goal_id: &str,
) -> anyhow::Result<Option<Duration>> {
    let Some(state_db) = session.state_db_for_thread_goals().await? else {
        return Ok(None);
    };
    let Some(snoozed_until_ms) = state_db
        .get_thread_goal_supervisor_snoozed_until_ms(session.conversation_id, goal_id)
        .await?
    else {
        return Ok(None);
    };
    let now_ms = Utc::now().timestamp_millis();
    if snoozed_until_ms <= now_ms {
        state_db
            .set_thread_goal_supervisor_snoozed_until_ms(session.conversation_id, goal_id, None)
            .await?;
        return Ok(None);
    }
    Ok(Some(Duration::from_millis(
        (snoozed_until_ms - now_ms) as u64,
    )))
}

async fn schedule_supervisor_wakeup(session: &Arc<Session>, delay: Duration) {
    let mut scheduled = session
        .goal_runtime
        .supervisor
        .scheduled_wakeup
        .lock()
        .await;
    if *scheduled {
        return;
    }
    *scheduled = true;
    let session = Arc::clone(session);
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        *session
            .goal_runtime
            .supervisor
            .scheduled_wakeup
            .lock()
            .await = false;
        if let Err(err) = session
            .goal_runtime_apply(crate::goals::GoalRuntimeEvent::MaybeContinueIfIdle)
            .await
        {
            tracing::warn!("failed to run scheduled goal supervisor check-in: {err}");
        }
    });
}
