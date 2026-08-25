use super::*;
use crate::session::session::Session;
use pretty_assertions::assert_eq;
use serde_json::Value;

async fn continuity(
    session: &Arc<Session>,
    goal_id: &str,
    goal: &ThreadGoal,
    source_items: &[RolloutItem],
) -> Value {
    let item = crate::goal_supervisor::supervisor_continuity_context_item(
        session,
        goal_id,
        goal,
        source_items,
    )
    .await;
    let RolloutItem::ResponseItem(envelope) = item else {
        panic!("continuity response item");
    };
    let ResponseItem::Message { content, .. } = envelope.item else {
        panic!("continuity developer message");
    };
    content
        .iter()
        .find_map(|part| match part {
            ContentItem::InputText { text } => text
                .strip_prefix("# Goal Supervisor Continuity\n\n")
                .map(|json| serde_json::from_str(json).expect("continuity JSON")),
            _ => None,
        })
        .expect("continuity text")
}

fn snooze_communication(item: &ResponseItem) -> Option<InterAgentCommunication> {
    let ResponseItem::Message { role, content, .. } = item else {
        return None;
    };
    (role == "assistant")
        .then(|| InterAgentCommunication::from_message_content(content))
        .flatten()
        .filter(|message| message.author.as_str() == "/root/goal_supervisor")
}

#[test]
fn supervisor_actions_preserve_snooze_history_and_goal_lifetime_counters() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "supervisor_actions_preserve_snooze_history_and_goal_lifetime_counters",
        supervisor_actions_preserve_snooze_history_and_goal_lifetime_counters_inner(),
    )
}

async fn supervisor_actions_preserve_snooze_history_and_goal_lifetime_counters_inner()
-> anyhow::Result<()> {
    let harness = AgentControlHarness::new().await;
    let state_db = harness.state_db.as_ref().expect("persisted goals");
    let (parent_id, parent) = harness.start_thread().await;
    parent
        .session
        .try_ensure_rollout_materialized(PersistContext::Standard)
        .await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_id,
        &parent.session,
        "Preserve scheduled work across unchanged polls.",
    )
    .await?;
    let before = harness.manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent.session, &goal_id, &goal)
        .await?;
    let helper_id = spawned_thread_id_after(&harness.manager, &before).await;

    let reason = format!("external\nAPI\treturned\rno results: {}", "x".repeat(256));
    let prefix = "external API returned no results: ";
    let expected = format!("Snooze 60s: {prefix}{}", "x".repeat(120 - prefix.len()));
    assert_eq!(
        parent
            .session
            .services
            .agent_control
            .snooze_goal_supervisor_helper(
                helper_id,
                /*delay_seconds*/ 60,
                Some(reason.as_str())
            )
            .await,
        Some(60)
    );
    let history = parent.session.clone_history().await;
    let live_messages = history
        .raw_items()
        .filter_map(snooze_communication)
        .collect::<Vec<_>>();
    assert_eq!(
        live_messages.len(),
        1,
        "one snooze must produce one model-visible message"
    );
    assert_eq!(live_messages[0].content, expected);
    assert_eq!(live_messages[0].recipient, AgentPath::root());
    assert!(!live_messages[0].trigger_turn);
    assert!(!live_messages[0].content.chars().any(char::is_control));
    assert!(parent.session.active_turn.lock().await.is_none());
    assert!(
        !parent
            .session
            .input_queue
            .has_pending_input(&parent.session.active_turn)
            .await
    );
    assert!(
        harness.manager.captured_ops().iter().all(|(id, op)| {
            *id != parent_id || !matches!(op, Op::InterAgentCommunication { .. })
        }),
        "recording snooze must not start or queue a parent turn"
    );

    parent.flush_rollout().await?;
    let stored = parent
        .read_thread(
            /*include_archived*/ true, /*include_history*/ true,
        )
        .await?;
    let stored = stored.history.expect("persisted parent history");
    let persisted_messages = stored
        .items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::ResponseItem(envelope) => snooze_communication(&envelope.item),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        persisted_messages, live_messages,
        "snooze persistence must not drop or duplicate messages"
    );
    assert!(!stored.items.iter().any(|item| matches!(item,
        RolloutItem::EventMsg(EventMsg::Warning(warning))
            if warning.message.starts_with("Supervisor snoozed for ")
    )));
    let before_poll = continuity(&parent.session, &goal_id, &goal, &[]).await;
    assert_eq!(before_poll["previous_supervisor_action"]["kind"], "snooze");
    assert_eq!(
        before_poll["previous_supervisor_action"]["snoozed_seconds"],
        60
    );
    for (section, count, seconds) in [
        (
            "goal_timing",
            "snooze_count_since_goal_created",
            "snoozed_seconds_since_goal_created",
        ),
        (
            "parent_timing",
            "snooze_count_since_last_parent_message",
            "snoozed_seconds_since_last_parent_message",
        ),
    ] {
        assert_eq!(before_poll[section][count], 1);
        assert_eq!(before_poll[section][seconds], 60);
    }
    let completed_poll = RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: "unchanged-parent-poll".to_string(),
        last_agent_message: Some("No new results.".to_string()),
        error: None,
        started_at: None,
        completed_at: Some(chrono::Utc::now().timestamp().saturating_add(1)),
        duration_ms: None,
        time_to_first_token_ms: None,
    }));
    let after_poll = continuity(&parent.session, &goal_id, &goal, &[completed_poll]).await;
    assert_eq!(
        after_poll["parent_timing"]["snooze_count_since_last_parent_message"],
        0
    );
    assert_eq!(
        after_poll["parent_timing"]["snoozed_seconds_since_last_parent_message"],
        0
    );
    assert_eq!(
        after_poll["goal_timing"]["snooze_count_since_goal_created"],
        1
    );
    assert_eq!(
        after_poll["goal_timing"]["snoozed_seconds_since_goal_created"],
        60
    );

    let replacement = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_id,
            "A different schedule.",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    let replacement_id = replacement.goal_id.clone();
    let replacement = crate::goal_supervisor::protocol_goal_from_state(replacement);
    let new_goal = continuity(&parent.session, &replacement_id, &replacement, &[]).await;
    assert!(new_goal["previous_supervisor_action"].is_null());
    for (section, count, seconds) in [
        (
            "goal_timing",
            "snooze_count_since_goal_created",
            "snoozed_seconds_since_goal_created",
        ),
        (
            "parent_timing",
            "snooze_count_since_last_parent_message",
            "snoozed_seconds_since_last_parent_message",
        ),
    ] {
        assert_eq!(new_goal[section][count], 0);
        assert_eq!(new_goal[section][seconds], 0);
    }
    let before = harness.manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent.session,
        &replacement_id,
        &replacement,
    )
    .await?;
    let helper_id = spawned_thread_id_after(&harness.manager, &before).await;
    let compacted = parent
        .session
        .services
        .agent_control
        .compact_parent_for_goal_supervisor_helper(helper_id)
        .await?;
    assert_matches!(compacted, SupervisorParentCompactionResult::Submitted {
        parent_thread_id, ..
    } if parent_thread_id == parent_id);
    assert!(
        harness
            .manager
            .captured_ops()
            .iter()
            .any(|(id, op)| { *id == parent_id && matches!(op, Op::Compact) })
    );
    let after_compact = continuity(&parent.session, &replacement_id, &replacement, &[]).await;
    assert_eq!(
        after_compact["previous_supervisor_action"]["kind"],
        "compact_parent_context"
    );
    parent.shutdown_and_wait().await?;
    Ok(())
}
