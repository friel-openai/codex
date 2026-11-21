use super::*;
use pretty_assertions::assert_eq;

#[test]
fn goal_resume_clears_supervisor_failure_backoff() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_resume_clears_supervisor_failure_backoff",
        goal_resume_clears_supervisor_failure_backoff_inner(),
    )
}

async fn goal_resume_clears_supervisor_failure_backoff_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse_failed(
                "supervisor-failure-1",
                "model_not_found",
                "saved model unavailable",
            ),
            sse_failed(
                "supervisor-failure-2",
                "model_not_found",
                "saved model unavailable",
            ),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Retry immediately when the user resumes this goal.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(deadline_ms) = state_db
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                .await?
                && deadline_ms > chrono::Utc::now().timestamp_millis()
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    let delivered_parent_message = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        "continue".to_string(),
        /*trigger_turn*/ true,
    );
    crate::goal_supervisor::record_followup_action(
        &parent_thread.session,
        &delivered_parent_message,
    )
    .await;
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        0,
        "a valid supervisor followup action should reset failure backoff"
    );

    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await?;
    let second_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && let Some(deadline_ms) = state_db
                    .thread_goals()
                    .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                    .await?
                && deadline_ms != first_deadline_ms
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let second_delay_ms = second_deadline_ms - chrono::Utc::now().timestamp_millis();
    assert!(
        (0..=60_000).contains(&second_delay_ms),
        "manual resume should reset the failure count to the one-minute tier, got {second_delay_ms}ms"
    );
    assert_eq!(request_log.requests().len(), 2);
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    Ok(())
}

#[test]
fn goal_resume_replaces_a_terminal_owned_supervisor_without_backoff() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_resume_replaces_a_terminal_owned_supervisor_without_backoff",
        goal_resume_replaces_a_terminal_owned_supervisor_without_backoff_inner(),
    )
}

async fn goal_resume_replaces_a_terminal_owned_supervisor_without_backoff_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("supervisor-running"),
                ev_completed("supervisor-running"),
            ]))
            .set_delay(Duration::from_millis(250)),
            sse_response(sse_failed(
                "supervisor-replacement-failure",
                "model_not_found",
                "saved model unavailable",
            )),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Retry immediately when resume races a terminal supervisor.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first supervisor request should start");

    assert!(
        harness
            .manager
            .remove_thread(&helper_thread_id)
            .await
            .is_some(),
        "remove the first helper before its completion watcher retires it"
    );
    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await
        .expect("resume should replace the disappeared active helper");
    timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
                // Removing the helper precedes the persisted retry update.
                && state_db.thread_goals()
                    .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                    .await?
                    .is_some_and(|deadline| deadline > chrono::Utc::now().timestamp_millis())
            {
                break Ok::<(), anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resume should start one immediate replacement")?;

    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1,
        "the terminal helper retired by resume must not count as a failed retry"
    );
    Ok(())
}
