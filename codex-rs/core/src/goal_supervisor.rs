use crate::agent::control::SpawnAgentForkMode;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::session::session::Session;
use chrono::Utc;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadGoal;
use codex_protocol::protocol::ThreadGoalUpdatedEvent;
use codex_protocol::user_input::UserInput;
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
}

impl GoalSupervisorRuntimeState {
    pub(crate) fn new() -> Self {
        Self {
            active_helper_id: Mutex::new(None),
            active_goal_id: Mutex::new(None),
            snoozed_until: Mutex::new(None),
            scheduled_wakeup: Mutex::new(false),
        }
    }
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

    if session
        .services
        .agent_control
        .has_running_user_visible_descendant(session.conversation_id)
        .await
        .unwrap_or(false)
    {
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
        && let Some(goal) = state_db
            .thread_goals()
            .get_thread_goal(session.conversation_id)
            .await?
    {
        if goal.goal_id != active_goal_id {
            let _ = finish_supervisor_helper(session, helper_thread_id).await?;
            return Ok(None);
        }
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.conversation_id,
                &active_goal_id,
                Some(Utc::now().timestamp_millis() + (delay_seconds as i64 * 1000)),
            )
            .await?;
    }
    *session.goal_runtime.supervisor.snoozed_until.lock().await =
        Some(Instant::now() + Duration::from_secs(delay_seconds));
    let _ = finish_supervisor_helper(session, helper_thread_id).await?;
    schedule_supervisor_wakeup(session, Duration::from_secs(delay_seconds)).await;
    Ok(Some(delay_seconds))
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
        .thread_goals()
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
            .thread_goals()
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

async fn persisted_snooze_delay(
    session: &Arc<Session>,
    goal_id: &str,
) -> anyhow::Result<Option<Duration>> {
    let Some(state_db) = session.state_db_for_thread_goals().await? else {
        return Ok(None);
    };
    let Some(snoozed_until_ms) = state_db
        .thread_goals()
        .get_thread_goal_supervisor_snoozed_until_ms(session.conversation_id, goal_id)
        .await?
    else {
        return Ok(None);
    };
    let now_ms = Utc::now().timestamp_millis();
    if snoozed_until_ms <= now_ms {
        state_db
            .thread_goals()
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
