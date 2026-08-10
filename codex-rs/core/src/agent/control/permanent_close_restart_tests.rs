use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn permanent_close_retry_recovers_ephemeral_leaf_tombstone() {
    let (harness, blocking_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let ephemeral = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("ephemeral runtime should start");
    let ephemeral_thread_id = ephemeral.thread_id;
    harness
        .control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("ephemeral registry slot")
        .commit(AgentMetadata {
            agent_id: Some(ephemeral_thread_id),
            parent_thread_id: Some(root_thread_id),
            depth: Some(1),
            ephemeral: true,
            ..Default::default()
        });
    assert!(
        harness
            .manager
            .get_thread(ephemeral_thread_id)
            .await
            .is_ok()
    );

    let sibling_thread_id = ThreadId::new();
    let state_db = harness.state_db.as_ref().expect("state db").clone();
    state_db
        .upsert_thread_spawn_edge(
            root_thread_id,
            sibling_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("unrelated sibling edge");
    state_db
        .thread_goals()
        .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
            thread_id: ephemeral_thread_id,
            goal_id: "ephemeral-leaf-goal".to_string(),
            objective: "restart retry must pause this Goal".to_string(),
            status: codex_state::ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("ephemeral Goal");
    state_db
        .thread_queue()
        .enqueue(ephemeral_thread_id, r#"{"message":"ephemeral"}"#)
        .await
        .expect("ephemeral queue");

    *blocking_store
        .blocked_target_thread_id
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ephemeral_thread_id);
    blocking_store
        .commit_before_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first_close = tokio::spawn({
        let control = harness.control.clone();
        async move {
            control
                .close_agent_subtree(root_thread_id, ephemeral_thread_id)
                .await
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        blocking_store.close_entered.notified(),
    )
    .await
    .expect("ephemeral close should commit before cleanup");
    first_close.abort();
    assert!(
        first_close
            .await
            .expect_err("first close should be cancelled")
            .is_cancelled()
    );

    assert_eq!(
        state_db
            .get_permanently_closed_thread_spawn_subtree(root_thread_id, ephemeral_thread_id,)
            .await
            .expect("restart lookup should succeed")
            .expect("restart lookup should find the ephemeral tombstone"),
        codex_state::ClosedThreadSpawnSubtree {
            members: vec![codex_state::ClosedThreadSpawnSubtreeMember {
                thread_id: ephemeral_thread_id,
                depth: 0,
            }],
            newly_closed_edge_count: 0,
        }
    );
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(ephemeral_thread_id)
            .await
            .expect("Goal should load")
            .expect("Goal should remain before retry")
            .status,
        codex_state::ThreadGoalStatus::Active
    );
    assert_eq!(
        state_db
            .thread_queue()
            .list_page(ephemeral_thread_id, 0, 10)
            .await
            .expect("queue should load")
            .len(),
        1
    );

    drop(ephemeral);
    let AgentControlHarness {
        _home,
        config,
        state_db: restarted_state_db,
        manager: original_manager,
        control: original_control,
    } = harness;
    drop(original_control);
    drop(original_manager);
    let restarted_manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        restarted_state_db,
    );
    let restarted_control = restarted_manager.agent_control();
    restarted_control.register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    assert!(
        restarted_manager
            .get_thread(ephemeral_thread_id)
            .await
            .is_err()
    );
    assert!(
        restarted_control
            .get_agent_metadata(ephemeral_thread_id)
            .is_none()
    );

    let report = restarted_control
        .close_agent_subtree(root_thread_id, ephemeral_thread_id)
        .await
        .expect("restart retry should finish ephemeral cleanup from the tombstone");
    assert_eq!(report.closed_agents, 1);
    assert_eq!(report.closed_edges, 1);
    assert_eq!(report.newly_closed_edges, 0);
    assert_eq!(report.stopped_runtimes, 0);
    assert_eq!(report.paused_goals, 1);
    assert_eq!(report.cleared_queued_items, 1);
    assert_eq!(report.evicted_identities, 0);
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(ephemeral_thread_id)
            .await
            .expect("Goal should load")
            .expect("Goal remains auditable")
            .status,
        codex_state::ThreadGoalStatus::Paused
    );
    assert!(
        state_db
            .thread_queue()
            .list_page(ephemeral_thread_id, 0, 10)
            .await
            .expect("queue should load")
            .is_empty()
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("unrelated sibling should remain Open"),
        vec![sibling_thread_id]
    );
    assert!(
        state_db
            .upsert_thread_spawn_edge(
                sibling_thread_id,
                ephemeral_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect_err("ownership transfer must not reopen the ephemeral tombstone")
            .to_string()
            .contains("permanently closed")
    );
}

#[tokio::test]
async fn permanent_close_retry_recovers_materialized_current_only_descendants() {
    let (harness, blocking_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let sibling_thread_id = ThreadId::new();
    let promoted_thread_id = ThreadId::new();
    let promoted_child_thread_id = ThreadId::new();
    let current_only_parent = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only parent runtime should start");
    let current_only_parent_thread_id = current_only_parent.thread_id;
    let current_only_child = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only child runtime should start");
    let current_only_child_thread_id = current_only_child.thread_id;
    for (thread_id, parent_thread_id, depth) in [
        (current_only_parent_thread_id, worker_thread_id, 2),
        (
            current_only_child_thread_id,
            current_only_parent_thread_id,
            3,
        ),
    ] {
        harness
            .control
            .state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("current-only registry slot")
            .commit(AgentMetadata {
                agent_id: Some(thread_id),
                parent_thread_id: Some(parent_thread_id),
                depth: Some(depth),
                ephemeral: false,
                ..Default::default()
            });
    }
    let state_db = harness.state_db.as_ref().expect("state db").clone();
    for (parent_thread_id, child_thread_id, status) in [
        (
            root_thread_id,
            sibling_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            worker_thread_id,
            promoted_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        ),
        (
            promoted_thread_id,
            promoted_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            current_only_parent_thread_id,
            current_only_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
    ] {
        state_db
            .upsert_thread_spawn_edge(parent_thread_id, child_thread_id, status)
            .await
            .expect("seed durable ownership boundary");
    }
    for (thread_id, suffix) in [
        (current_only_parent_thread_id, "parent"),
        (current_only_child_thread_id, "child"),
    ] {
        state_db
            .thread_goals()
            .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
                thread_id,
                goal_id: format!("current-only-{suffix}-goal"),
                objective: "restart retry must pause this Goal".to_string(),
                status: codex_state::ThreadGoalStatus::Active,
                token_budget: None,
                tokens_used: 0,
                time_used_seconds: 0,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .expect("current-only Goal");
        let queued_item = format!(r#"{{"message":"{suffix}"}}"#);
        state_db
            .thread_queue()
            .enqueue(thread_id, &queued_item)
            .await
            .expect("current-only queue");
    }

    *blocking_store
        .blocked_target_thread_id
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker_thread_id);
    blocking_store
        .commit_before_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first_close = tokio::spawn({
        let control = harness.control.clone();
        async move {
            control
                .close_agent_subtree(root_thread_id, worker_thread_id)
                .await
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        blocking_store.close_entered.notified(),
    )
    .await
    .expect("first close should commit before cleanup");
    first_close.abort();
    assert!(
        first_close
            .await
            .expect_err("first close should be cancelled")
            .is_cancelled()
    );

    let committed_members = vec![
        codex_state::ClosedThreadSpawnSubtreeMember {
            thread_id: current_only_child_thread_id,
            depth: 2,
        },
        codex_state::ClosedThreadSpawnSubtreeMember {
            thread_id: current_only_parent_thread_id,
            depth: 1,
        },
        codex_state::ClosedThreadSpawnSubtreeMember {
            thread_id: worker_thread_id,
            depth: 0,
        },
    ];
    assert_eq!(
        state_db
            .get_permanently_closed_thread_spawn_subtree(root_thread_id, worker_thread_id)
            .await
            .expect("restart lookup should succeed")
            .expect("restart lookup should find the complete subtree"),
        codex_state::ClosedThreadSpawnSubtree {
            members: committed_members,
            newly_closed_edge_count: 0,
        }
    );

    // A new manager has neither the original registry nor an in-memory close record. Its retry
    // can discover the current-only descendants only through the materialized PClosed edges.
    drop(current_only_parent);
    drop(current_only_child);
    let AgentControlHarness {
        _home,
        config,
        state_db: restarted_state_db,
        manager: original_manager,
        control: original_control,
    } = harness;
    drop(original_control);
    drop(original_manager);
    let restarted_manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        restarted_state_db,
    );
    let restarted_control = restarted_manager.agent_control();
    restarted_control.register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    for thread_id in [
        worker_thread_id,
        current_only_parent_thread_id,
        current_only_child_thread_id,
    ] {
        assert!(restarted_manager.get_thread(thread_id).await.is_err());
        assert!(restarted_control.get_agent_metadata(thread_id).is_none());
    }

    let report = restarted_control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("restart-style retry should finish cleanup from durable state");
    assert_eq!(report.closed_agents, 3);
    assert_eq!(report.closed_edges, 3);
    assert_eq!(report.newly_closed_edges, 0);
    assert_eq!(report.stopped_runtimes, 0);
    assert_eq!(report.paused_goals, 2);
    assert_eq!(report.cleared_queued_items, 2);
    assert_eq!(report.evicted_identities, 0);
    for thread_id in [
        worker_thread_id,
        current_only_parent_thread_id,
        current_only_child_thread_id,
    ] {
        assert!(restarted_manager.get_thread(thread_id).await.is_err());
        assert!(restarted_control.get_agent_metadata(thread_id).is_none());
    }
    for thread_id in [current_only_parent_thread_id, current_only_child_thread_id] {
        assert_eq!(
            state_db
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("current-only Goal should load")
                .expect("current-only Goal remains auditable")
                .status,
            codex_state::ThreadGoalStatus::Paused
        );
        assert!(
            state_db
                .thread_queue()
                .list_page(thread_id, 0, 10)
                .await
                .expect("current-only queue should load")
                .is_empty()
        );
    }
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                worker_thread_id,
                DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("promotion boundary should remain Closed"),
        vec![promoted_thread_id]
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                promoted_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("promoted branch should remain Open"),
        vec![promoted_child_thread_id]
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("unrelated sibling should remain Open"),
        vec![sibling_thread_id]
    );
    assert!(
        state_db
            .upsert_thread_spawn_edge(
                sibling_thread_id,
                current_only_child_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect_err("ownership transfer must not reopen the durable close")
            .to_string()
            .contains("permanently closed")
    );
}
