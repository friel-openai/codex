use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn close_fence_transition_preserves_overlapping_refcounts() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let state = harness.control.upgrade().expect("manager state");
    let retained_thread_id = ThreadId::new();
    let released_thread_id = ThreadId::new();
    let added_thread_id = ThreadId::new();

    state.mark_threads_closing([retained_thread_id, released_thread_id]);
    // Model an overlapping ancestor close that independently owns the retained fence.
    state.mark_threads_closing([retained_thread_id]);
    state.replace_threads_closing(
        [retained_thread_id, released_thread_id],
        [retained_thread_id, added_thread_id],
    );

    assert!(state.is_thread_closing(retained_thread_id));
    assert!(!state.is_thread_closing(released_thread_id));
    assert!(state.is_thread_closing(added_thread_id));

    state.unmark_threads_closing([retained_thread_id, added_thread_id]);
    assert!(state.is_thread_closing(retained_thread_id));
    assert!(!state.is_thread_closing(added_thread_id));
    state.unmark_threads_closing([retained_thread_id]);
    assert!(!state.is_thread_closing(retained_thread_id));
}

#[tokio::test]
async fn permanent_close_fence_wins_against_queued_child_spawn() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    let worker_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("worker should spawn");

    let mutation_guard = harness
        .control
        .lock_lifecycle_mutation()
        .await
        .expect("manager lifecycle lock");
    let close_control = harness.control.clone();
    let close_task = tokio::spawn(async move {
        close_control
            .close_agent_subtree(root_thread_id, worker_thread_id)
            .await
    });
    tokio::task::yield_now().await;
    let spawn_control = harness.control.clone();
    let spawn_config = harness.config.clone();
    let spawn_task = tokio::spawn(async move {
        spawn_control
            .spawn_agent(
                spawn_config,
                text_input("late child"),
                Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: worker_thread_id,
                    depth: 2,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
            )
            .await
    });
    tokio::task::yield_now().await;
    drop(mutation_guard);

    close_task
        .await
        .expect("close task should join")
        .expect("worker close should succeed");
    let spawn_err = spawn_task
        .await
        .expect("spawn task should join")
        .expect_err("spawn queued behind a permanent close must fail");
    assert_matches!(spawn_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("closing"));
    assert!(
        harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id)
    );
    assert_eq!(
        harness
            .state_db
            .as_ref()
            .expect("state db")
            .list_thread_spawn_children_with_status(
                worker_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("open child query"),
        Vec::<ThreadId>::new()
    );
}

#[tokio::test]
async fn permanent_close_fence_wins_against_queued_ownership_transfer() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    let worker_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("worker should spawn");
    let independent = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("independent root should start");
    persist_thread_for_tree_resume(&independent.thread, "independent persisted").await;
    let independent_thread_id = independent.thread_id;
    harness
        .control
        .shutdown_live_agent(independent_thread_id)
        .await
        .expect("independent root should become cold");
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);

    let mutation_guard = harness
        .control
        .lock_lifecycle_mutation()
        .await
        .expect("manager lifecycle lock");
    let close_control = harness.control.clone();
    let close_task = tokio::spawn(async move {
        close_control
            .close_agent_subtree(root_thread_id, worker_thread_id)
            .await
    });
    tokio::task::yield_now().await;
    let transfer_control = harness.manager.agent_control();
    transfer_control.register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let transfer_config = harness.config.clone();
    let transfer_task = tokio::spawn(async move {
        transfer_control
            .resume_agent_from_rollout_with_ownership(
                transfer_config,
                independent_thread_id,
                SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: worker_thread_id,
                    depth: 2,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                }),
                super::ownership::ResumedThreadOwnership::Transfer,
            )
            .await
    });
    tokio::task::yield_now().await;
    drop(mutation_guard);

    close_task
        .await
        .expect("close task should join")
        .expect("worker close should succeed");
    let transfer_err = transfer_task
        .await
        .expect("transfer task should join")
        .expect_err("ownership transfer queued behind close must fail");
    assert_matches!(transfer_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("closing"));
    assert!(
        harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id)
    );
    assert_eq!(
        harness
            .state_db
            .as_ref()
            .expect("state db")
            .list_thread_spawn_children_with_status(
                worker_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("open child query"),
        Vec::<ThreadId>::new()
    );
}

#[tokio::test]
async fn permanent_close_fence_rejects_lazy_registration_of_a_cold_descendant() {
    let (harness, graph_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_path = AgentPath::root().join("worker").expect("worker path");
    let worker_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("worker should spawn");
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: Some(worker_path.join("child").expect("child path")),
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("child should spawn");
    harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child should become cold");

    let close_control = harness.control.clone();
    let close_task = tokio::spawn(async move {
        close_control
            .close_agent_subtree(root_thread_id, worker_thread_id)
            .await
    });
    timeout(Duration::from_secs(5), graph_store.close_entered.notified())
        .await
        .expect("close should reach the blocked graph transaction");
    let lazy_err = timeout(
        Duration::from_secs(5),
        harness
            .control
            .ensure_open_agent_known_by_id(root_thread_id, child_thread_id),
    )
    .await
    .expect("lazy selection should reject without waiting for graph closure")
    .expect_err("a closing ancestor must fence its cold descendant");
    assert_matches!(lazy_err.details(), CodexErrorDetails::ThreadNotFound(id) if *id == child_thread_id);
    graph_store.release_close.notify_one();
    close_task
        .await
        .expect("close task should join")
        .expect("worker subtree should close");
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_none()
    );
}

#[tokio::test]
async fn concurrent_permanent_close_rejects_an_in_progress_same_target_retry() {
    let (harness, graph_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;

    let first_control = harness.control.clone();
    let first_close = tokio::spawn(async move {
        first_control
            .close_agent_subtree(root_thread_id, worker_thread_id)
            .await
    });
    timeout(Duration::from_secs(5), graph_store.close_entered.notified())
        .await
        .expect("first close should reach the blocked graph transaction");
    let retry_err = timeout(
        Duration::from_secs(5),
        harness
            .control
            .close_agent_subtree(root_thread_id, worker_thread_id),
    )
    .await
    .expect("same-target retry should reject without waiting for the first close")
    .expect_err("an in-progress close is not a completed repair record");
    assert_matches!(retry_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("already being permanently closed"));
    graph_store.release_close.notify_one();
    first_close
        .await
        .expect("first close task should join")
        .expect("first close should complete");
}

#[tokio::test]
async fn overlapping_ancestor_close_retains_descendant_fence_after_descendant_rollback() {
    let (harness, graph_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let child_thread_id = harness
        .spawn_anonymous_child(
            worker_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(worker_thread_id),
                ..Default::default()
            },
        )
        .await;
    *graph_store
        .blocked_target_thread_id
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(child_thread_id);

    let child_close_control = harness.control.clone();
    let child_close = tokio::spawn(async move {
        child_close_control
            .close_agent_subtree(root_thread_id, child_thread_id)
            .await
    });
    timeout(Duration::from_secs(5), graph_store.close_entered.notified())
        .await
        .expect("descendant close should block before graph mutation");

    harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("ancestor close should win while descendant close is blocked");
    graph_store.release_close.notify_one();
    child_close
        .await
        .expect("descendant close task should join")
        .expect_err("the descendant edge was already closed by its ancestor");

    let state = harness.control.upgrade().expect("manager state");
    assert!(state.is_thread_closing(child_thread_id));
    let spawn_err = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("late child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 3,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect_err("ancestor permanent close must retain the descendant fence");
    assert_matches!(spawn_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("closing"));
    assert_eq!(
        harness
            .state_db
            .as_ref()
            .expect("state db")
            .list_thread_spawn_children_with_status(
                worker_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("permanently closed child edge"),
        vec![child_thread_id]
    );
}

#[tokio::test]
async fn cancelled_close_after_commit_retains_fence_and_retry_finishes_cleanup() {
    let (harness, graph_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let sibling_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    graph_store
        .commit_before_release
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let close_control = harness.control.clone();
    let close_task = tokio::spawn(async move {
        close_control
            .close_agent_subtree(root_thread_id, worker_thread_id)
            .await
    });
    timeout(Duration::from_secs(5), graph_store.close_entered.notified())
        .await
        .expect("close should commit before acknowledgement");
    close_task.abort();
    assert!(
        close_task
            .await
            .expect_err("close task should be cancelled")
            .is_cancelled()
    );
    assert!(
        harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id)
    );
    let sibling_err = harness
        .control
        .close_agent_subtree(sibling_thread_id, worker_thread_id)
        .await
        .expect_err("a sibling must not finish another caller's abandoned close");
    assert_matches!(sibling_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("owned by another caller"));

    let retry = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("durable close retry should finish cleanup");
    assert_eq!(retry.closed_agents, 1);
    assert!(
        harness
            .control
            .get_agent_metadata(worker_thread_id)
            .is_none()
    );
    assert_eq!(
        harness
            .state_db
            .as_ref()
            .expect("state db")
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("permanent close edge"),
        vec![worker_thread_id]
    );
}

#[tokio::test]
async fn helper_completion_cannot_downgrade_a_concurrent_permanent_close() {
    let (harness, graph_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let helper_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;

    let close_control = harness.control.clone();
    let close_task = tokio::spawn(async move {
        close_control
            .close_agent_subtree(root_thread_id, helper_thread_id)
            .await
    });
    timeout(Duration::from_secs(5), graph_store.close_entered.notified())
        .await
        .expect("close should reach the blocked graph transaction");
    harness
        .control
        .finish_internal_helper_thread(helper_thread_id)
        .await
        .expect("helper completion should retire its runtime");
    graph_store.release_close.notify_one();
    close_task
        .await
        .expect("close task should join")
        .expect("permanent close should succeed");

    assert_eq!(
        harness
            .state_db
            .as_ref()
            .expect("state db")
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("permanent close edge"),
        vec![helper_thread_id]
    );
    let retry = harness
        .control
        .close_agent_subtree(root_thread_id, helper_thread_id)
        .await
        .expect("permanent close should remain retryable after helper completion");
    assert_eq!(retry.newly_closed_edges, 0);
}

#[tokio::test]
async fn failed_permanent_close_rolls_back_only_its_in_progress_fences() {
    let (harness, graph_store) = AgentControlHarness::new_with_blocking_close_store().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    graph_store
        .fail_close
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let close_control = harness.control.clone();
    let close_task = tokio::spawn(async move {
        close_control
            .close_agent_subtree(root_thread_id, worker_thread_id)
            .await
    });
    timeout(Duration::from_secs(5), graph_store.close_entered.notified())
        .await
        .expect("close should reach the forced graph failure");
    graph_store.release_close.notify_one();
    let close_err = close_task
        .await
        .expect("close task should join")
        .expect_err("forced graph failure must fail the close");
    assert!(close_err.to_string().contains("forced close failure"));
    assert!(
        harness
            .control
            .get_agent_metadata(worker_thread_id)
            .is_some()
    );
    assert!(
        !harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id)
    );
    assert_eq!(
        harness
            .state_db
            .as_ref()
            .expect("state db")
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("failed close must retain the open ownership edge"),
        vec![worker_thread_id]
    );
}
