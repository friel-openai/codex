use super::*;
use crate::session_prefix::format_subagent_notification_message;
use assert_matches::assert_matches;
use codex_protocol::error::CodexErrorDetails;
use pretty_assertions::assert_eq;

async fn apply_legacy_lru_pressure(
    harness: &AgentControlHarness,
    parent_thread_id: ThreadId,
    parent_thread: &Arc<CodexThread>,
    first_child_index: usize,
    notifications_before_pressure: usize,
) {
    for offset in 0..LEGACY_TEST_MAX_THREADS {
        spawn_completed_legacy_child(
            harness,
            parent_thread_id,
            first_child_index + offset,
            4 * 1024,
        )
        .await;
    }
    wait_for_notification_count(
        parent_thread,
        notifications_before_pressure + LEGACY_TEST_MAX_THREADS,
    )
    .await;
}

async fn wait_for_legacy_completion_watcher(
    harness: &AgentControlHarness,
    child_thread_id: ThreadId,
) {
    harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("legacy child metadata")
        .lifecycle
        .wait_for_completion_watcher()
        .await;
}

#[tokio::test]
async fn legacy_subagent_completion_becomes_lru_evictable_after_notification() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let child_thread_id = spawn_completed_legacy_child(
        &harness,
        parent_thread_id,
        /*child_index*/ 0,
        32 * 1024,
    )
    .await;
    wait_for_notification_count(&parent_thread, /*expected*/ 1).await;

    assert!(harness.manager.get_thread(child_thread_id).await.is_ok());

    apply_legacy_lru_pressure(
        &harness,
        parent_thread_id,
        &parent_thread,
        /*first_child_index*/ 1,
        /*notifications_before_pressure*/ 1,
    )
    .await;
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
    assert!(
        wait_for_loaded_child_count_at_most(
            &harness,
            parent_thread_id,
            LEGACY_TEST_MAX_THREADS,
            Duration::from_secs(5),
        )
        .await
    );
}

#[tokio::test]
async fn legacy_subagent_followup_reloads_history_and_becomes_evictable_again() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let child_thread_id = spawn_completed_legacy_child(
        &harness,
        parent_thread_id,
        /*child_index*/ 0,
        32 * 1024,
    )
    .await;
    wait_for_notification_count(&parent_thread, /*expected*/ 1).await;
    apply_legacy_lru_pressure(
        &harness,
        parent_thread_id,
        &parent_thread,
        /*first_child_index*/ 1,
        /*notifications_before_pressure*/ 1,
    )
    .await;
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());

    harness
        .control
        .deliver_inter_agent_communication_to_agent(
            harness.config.clone(),
            child_thread_id,
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                "follow-up".to_string(),
                /*trigger_turn*/ false,
            ),
            AgentCommunicationContext::new(AgentCommunicationKind::Followup, parent_thread_id),
            AgentInputDelivery::Queue,
            /*parent_turn_id*/ None,
        )
        .await
        .expect("completed legacy child should accept a follow-up");
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("follow-up should reload the child");
    assert!(history_contains_text(
        child_thread.session.clone_history().await.raw_items(),
        "child-00000000-"
    ));
    timeout(Duration::from_secs(5), async {
        while !child_thread
            .session
            .input_queue
            .has_pending_mailbox_items()
            .await
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("follow-up should reach the reloaded child mailbox");
    let (followup_items, parent_turn_id) = child_thread
        .session
        .input_queue
        .drain_mailbox_input_items()
        .await;
    assert_eq!(parent_turn_id, None);
    assert_eq!(
        followup_items,
        vec![crate::session::TurnInput::InterAgentCommunication(
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                "follow-up".to_string(),
                /*trigger_turn*/ false,
            )
        )]
    );
    let turn = child_thread.session.new_default_turn().await;
    child_thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn.sub_id.clone(),
                last_agent_message: Some("follow-up complete".to_string()),
                error: None,
                started_at: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
    wait_for_notification_count(&parent_thread, /*expected*/ 4).await;
    apply_legacy_lru_pressure(
        &harness,
        parent_thread_id,
        &parent_thread,
        /*first_child_index*/ 3,
        /*notifications_before_pressure*/ 4,
    )
    .await;
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
    assert_eq!(
        subagent_notification_count(parent_thread.session.clone_history().await.raw_items()),
        6
    );
}

#[tokio::test]
async fn legacy_terminal_statuses_notify_once_and_become_lru_evictable() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let terminal_statuses = [
        LegacyTerminalStatus::Completed,
        LegacyTerminalStatus::Errored,
        LegacyTerminalStatus::Interrupted,
        LegacyTerminalStatus::Shutdown,
    ];
    let mut terminal_child_ids = Vec::with_capacity(terminal_statuses.len());

    for (child_index, terminal_status) in terminal_statuses.into_iter().enumerate() {
        let child_thread_id = spawn_terminal_legacy_child(
            &harness,
            parent_thread_id,
            child_index,
            4 * 1024,
            terminal_status,
        )
        .await;
        terminal_child_ids.push(child_thread_id);
        wait_for_notification_count(&parent_thread, child_index + 1).await;
        let expected_notification = format_subagent_notification_message(
            &child_thread_id.to_string(),
            &terminal_status.agent_status(child_index),
        );
        assert!(history_contains_text(
            parent_thread.session.clone_history().await.raw_items(),
            &expected_notification,
        ));
    }

    apply_legacy_lru_pressure(
        &harness,
        parent_thread_id,
        &parent_thread,
        terminal_statuses.len(),
        terminal_statuses.len(),
    )
    .await;
    for child_thread_id in terminal_child_ids {
        assert!(harness.manager.get_thread(child_thread_id).await.is_err());
    }

    assert_eq!(
        subagent_notification_count(parent_thread.session.clone_history().await.raw_items()),
        terminal_statuses.len() + LEGACY_TEST_MAX_THREADS
    );
}

#[tokio::test]
async fn explicitly_closed_cold_legacy_subagent_cannot_reload() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let child_thread_id =
        spawn_completed_legacy_child(&harness, parent_thread_id, /*child_index*/ 0, 4 * 1024).await;
    wait_for_notification_count(&parent_thread, /*expected*/ 1).await;
    apply_legacy_lru_pressure(
        &harness,
        parent_thread_id,
        &parent_thread,
        /*first_child_index*/ 1,
        /*notifications_before_pressure*/ 1,
    )
    .await;
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());

    harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("cold legacy child should close");
    let state_db = harness.state_db.as_ref().expect("state database");
    assert!(
        !state_db
            .list_thread_spawn_children_with_status(
                parent_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("list open edges")
            .contains(&child_thread_id)
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                parent_thread_id,
                DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("list closed edges"),
        vec![child_thread_id]
    );
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_none()
    );
    let err = harness
        .control
        .deliver_input_to_agent(
            harness.config.clone(),
            child_thread_id,
            text_input("must stay closed"),
            AgentInputDelivery::Queue,
            /*parent_turn_id*/ None,
        )
        .await
        .expect_err("closed child should not accept input");
    assert_matches!(
        err.details(),
        CodexErrorDetails::ThreadNotFound(thread_id) if *thread_id == child_thread_id
    );
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
}

#[tokio::test]
async fn legacy_completion_and_followup_are_serialized_without_loss() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let (child_thread_id, child_thread, first_turn) =
        spawn_quiescent_legacy_child(&harness, parent_thread_id, /*child_index*/ 0, 4 * 1024).await;
    let lifecycle = harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("legacy child metadata")
        .lifecycle;
    let transition = lifecycle.lock_transition().await;
    let control = harness.control.clone();
    let config = harness.config.clone();
    let (delivery_started_tx, delivery_started_rx) = tokio::sync::oneshot::channel();
    let delivery = tokio::spawn(async move {
        let _ = delivery_started_tx.send(());
        control
            .deliver_inter_agent_communication_to_agent(
                config,
                child_thread_id,
                InterAgentCommunication::new(
                    AgentPath::root(),
                    AgentPath::root(),
                    Vec::new(),
                    "racing follow-up".to_string(),
                    /*trigger_turn*/ false,
                ),
                AgentCommunicationContext::new(AgentCommunicationKind::Followup, parent_thread_id),
                AgentInputDelivery::Queue,
                /*parent_turn_id*/ None,
            )
            .await
    });
    delivery_started_rx
        .await
        .expect("delivery task should reach the transition");
    tokio::task::yield_now().await;
    child_thread
        .session
        .send_event(
            first_turn.as_ref(),
            LegacyTerminalStatus::Completed.event(&first_turn.sub_id, /*child_index*/ 0),
        )
        .await;
    drop(transition);

    delivery
        .await
        .expect("delivery task should not panic")
        .expect("racing follow-up should be delivered once");
    wait_for_notification_count(&parent_thread, /*expected*/ 1).await;
    let reloaded_child = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("racing follow-up should reload the child");
    timeout(Duration::from_secs(5), async {
        while !reloaded_child
            .session
            .input_queue
            .has_pending_mailbox_items()
            .await
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("racing follow-up should reach the mailbox");
    let (followup_items, parent_turn_id) = reloaded_child
        .session
        .input_queue
        .drain_mailbox_input_items()
        .await;
    assert_eq!(parent_turn_id, None);
    assert_eq!(followup_items.len(), 1);
    let second_turn = reloaded_child.session.new_default_turn().await;
    reloaded_child
        .session
        .send_event(
            second_turn.as_ref(),
            LegacyTerminalStatus::Completed.event(&second_turn.sub_id, /*child_index*/ 1),
        )
        .await;
    wait_for_notification_count(&parent_thread, /*expected*/ 2).await;
    apply_legacy_lru_pressure(
        &harness,
        parent_thread_id,
        &parent_thread,
        /*first_child_index*/ 2,
        /*notifications_before_pressure*/ 2,
    )
    .await;
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
    assert_eq!(
        subagent_notification_count(parent_thread.session.clone_history().await.raw_items()),
        4
    );
}

#[tokio::test]
async fn legacy_child_with_missing_parent_is_lru_evictable() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let missing_parent_thread_id = ThreadId::new();
    let child_thread_id = spawn_completed_legacy_child(
        &harness,
        missing_parent_thread_id,
        /*child_index*/ 0,
        4 * 1024,
    )
    .await;
    wait_for_legacy_completion_watcher(&harness, child_thread_id).await;

    for child_index in 1..=LEGACY_TEST_MAX_THREADS {
        let pressure_child_id =
            spawn_completed_legacy_child(&harness, missing_parent_thread_id, child_index, 4 * 1024)
                .await;
        wait_for_legacy_completion_watcher(&harness, pressure_child_id).await;
    }
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
}

#[tokio::test]
async fn legacy_child_with_dead_parent_is_lru_evictable() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let (child_thread_id, child_thread, turn) =
        spawn_quiescent_legacy_child(&harness, parent_thread_id, /*child_index*/ 0, 4 * 1024).await;
    parent_thread
        .shutdown_and_wait()
        .await
        .expect("parent should shut down");
    assert!(
        harness
            .manager
            .remove_thread(&parent_thread_id)
            .await
            .is_some()
    );

    child_thread
        .session
        .send_event(
            turn.as_ref(),
            LegacyTerminalStatus::Completed.event(&turn.sub_id, /*child_index*/ 0),
        )
        .await;
    wait_for_legacy_completion_watcher(&harness, child_thread_id).await;
    for child_index in 1..=LEGACY_TEST_MAX_THREADS {
        let pressure_child_id =
            spawn_completed_legacy_child(&harness, parent_thread_id, child_index, 4 * 1024).await;
        wait_for_legacy_completion_watcher(&harness, pressure_child_id).await;
    }
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
}

#[tokio::test]
async fn queued_followup_keeps_legacy_child_loaded_until_next_completion() {
    let harness = legacy_harness(/*network_proxy_enabled*/ false).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let (child_thread_id, child_thread, first_turn) =
        spawn_quiescent_legacy_child(&harness, parent_thread_id, /*child_index*/ 0, 4 * 1024).await;
    child_thread
        .session
        .input_queue
        .enqueue_mailbox_communication(
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                "already queued".to_string(),
                /*trigger_turn*/ false,
            ),
            /*parent_turn_id*/ None,
        )
        .await;
    child_thread
        .session
        .send_event(
            first_turn.as_ref(),
            LegacyTerminalStatus::Completed.event(&first_turn.sub_id, /*child_index*/ 0),
        )
        .await;
    wait_for_notification_count(&parent_thread, /*expected*/ 1).await;
    assert!(harness.manager.get_thread(child_thread_id).await.is_ok());

    let second_turn = child_thread.session.new_default_turn().await;
    child_thread
        .session
        .send_event(
            second_turn.as_ref(),
            EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: second_turn.sub_id.clone(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: ModeKind::Default,
            }),
        )
        .await;
    let (queued, parent_turn_id) = child_thread
        .session
        .input_queue
        .drain_mailbox_input_items()
        .await;
    assert_eq!(parent_turn_id, None);
    assert_eq!(queued.len(), 1);
    child_thread
        .session
        .send_event(
            second_turn.as_ref(),
            LegacyTerminalStatus::Completed.event(&second_turn.sub_id, /*child_index*/ 1),
        )
        .await;
    wait_for_notification_count(&parent_thread, /*expected*/ 2).await;
    apply_legacy_lru_pressure(
        &harness,
        parent_thread_id,
        &parent_thread,
        /*first_child_index*/ 2,
        /*notifications_before_pressure*/ 2,
    )
    .await;
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
    assert_eq!(
        subagent_notification_count(parent_thread.session.clone_history().await.raw_items()),
        4
    );
}
