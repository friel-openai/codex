use super::*;
use pretty_assertions::assert_eq;
#[tokio::test]
async fn manager_membership_snapshot_resolves_a_cold_nested_scope_to_its_root() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let grandchild_thread_id = harness
        .spawn_anonymous_child(
            worker_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(worker_thread_id),
                ..Default::default()
            },
        )
        .await;
    let grandchild_metadata = harness
        .control
        .get_agent_metadata(grandchild_thread_id)
        .expect("grandchild metadata");
    grandchild_metadata
        .lifecycle
        .remember_cold_terminal_status(AgentStatus::Completed(None), true);
    let state = harness.control.upgrade().expect("manager state");
    harness
        .control
        .unload_agent_thread(&state, grandchild_thread_id)
        .await
        .expect("grandchild should become cold");

    let snapshot = harness
        .manager
        .current_agent_membership_snapshot(grandchild_thread_id)
        .await
        .expect("cold nested identity should resolve through its loaded root registry");
    assert_eq!(snapshot.registry_root_thread_id, root_thread_id);
    assert!(snapshot.members.is_empty());
}
#[tokio::test]
async fn manager_membership_snapshot_scopes_by_registered_parent_topology() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
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
    let hidden_thread_id = harness
        .spawn_anonymous_child(
            worker_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(worker_thread_id),
                ..Default::default()
            },
        )
        .await;
    let leaf_path = worker_path.join("leaf").expect("leaf path");
    let leaf_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("leaf"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: hidden_thread_id,
                depth: 3,
                agent_path: Some(leaf_path),
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("visible leaf should spawn beneath a pathless intermediate");
    let sibling_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;

    let snapshot = harness
        .manager
        .current_agent_membership_snapshot(worker_thread_id)
        .await
        .expect("nested scope should use the owning root registry");
    assert_eq!(snapshot.registry_root_thread_id, root_thread_id);
    assert_eq!(
        snapshot
            .members
            .iter()
            .map(|member| member.thread_id)
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([hidden_thread_id, leaf_thread_id])
    );
    assert!(!snapshot.members.iter().any(|member| {
        member.thread_id == worker_thread_id || member.thread_id == sibling_thread_id
    }));
}

#[tokio::test]
async fn archive_membership_handle_fences_mutation_without_holding_shutdown_lock() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
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
    let handle = harness
        .manager
        .prepare_current_agent_membership_eviction(worker_thread_id)
        .await
        .expect("archive should retain the owning current registry");
    assert!(
        harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id)
    );

    let spawn_control = harness.control.clone();
    let spawn_config = harness.config.clone();
    let blocked_spawn = tokio::spawn(async move {
        spawn_control
            .spawn_agent(
                spawn_config,
                text_input("must not survive archive"),
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
    let evicted = timeout(
        Duration::from_secs(5),
        handle.evict_exact(&[worker_thread_id]),
    )
    .await
    .expect("runtime eviction must not deadlock with a fenced lifecycle mutation")
    .expect("worker eviction should succeed");
    assert_eq!(evicted, 1);
    let spawn_err = timeout(Duration::from_secs(5), blocked_spawn)
        .await
        .expect("fenced spawn should finish")
        .expect("fenced spawn task should join")
        .expect_err("fenced spawn must not resurrect the archived subtree");
    assert_matches!(spawn_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("closing"));
    assert!(
        harness
            .control
            .get_agent_metadata(worker_thread_id)
            .is_none()
    );
    assert!(
        harness
            .control
            .get_agent_metadata(sibling_thread_id)
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
            .expect("archive eviction must not change ownership edges")
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([worker_thread_id, sibling_thread_id])
    );
}

#[tokio::test]
async fn overlapping_archive_membership_handles_retain_each_others_fences() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let first = harness
        .manager
        .prepare_current_agent_membership_eviction(worker_thread_id)
        .await
        .expect("first archive handle");
    let second = harness
        .manager
        .prepare_current_agent_membership_eviction(worker_thread_id)
        .await
        .expect("second archive handle");
    let close_err = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect_err("permanent close must not overlap archive or delete");
    assert_matches!(close_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("archived or deleted"));
    drop(first);
    assert!(
        harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id),
        "dropping one overlapping handle must retain the other handle's fence"
    );
    drop(second);
    assert!(
        !harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id)
    );
}

#[tokio::test]
async fn archive_membership_handle_uses_persisted_edges_to_classify_current_only_descendants() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, root_thread) = harness.start_thread().await;
    let control = root_thread.session.services.agent_control.clone();
    control.register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = ThreadId::new();
    let current_only_thread_id = ThreadId::new();
    let current_only_child_thread_id = ThreadId::new();
    let closed_thread_id = ThreadId::new();
    let permanently_closed_thread_id = ThreadId::new();
    let mismatched_thread_id = ThreadId::new();
    let mismatched_child_thread_id = ThreadId::new();
    for (thread_id, parent_thread_id, depth) in [
        (worker_thread_id, root_thread_id, 1),
        (current_only_thread_id, worker_thread_id, 2),
        (current_only_child_thread_id, current_only_thread_id, 3),
        (closed_thread_id, worker_thread_id, 2),
        (permanently_closed_thread_id, worker_thread_id, 2),
        (mismatched_thread_id, worker_thread_id, 2),
        (mismatched_child_thread_id, mismatched_thread_id, 3),
    ] {
        control
            .state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("spawn slot")
            .commit(AgentMetadata {
                agent_id: Some(thread_id),
                parent_thread_id: Some(parent_thread_id),
                depth: Some(depth),
                ephemeral: false,
                ..Default::default()
            });
    }
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .upsert_thread_spawn_edge(
            root_thread_id,
            worker_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("persist worker edge");
    state_db
        .upsert_thread_spawn_edge(
            current_only_thread_id,
            current_only_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("persist child below current-only parent");
    state_db
        .upsert_thread_spawn_edge(
            worker_thread_id,
            closed_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("persist closed boundary");
    state_db
        .upsert_thread_spawn_edge(
            worker_thread_id,
            permanently_closed_thread_id,
            DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await
        .expect("persist permanently closed boundary");
    state_db
        .upsert_thread_spawn_edge(
            root_thread_id,
            mismatched_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("persist mismatched parent edge");

    assert_eq!(
        harness
            .manager
            .list_open_agent_subtree_thread_ids(root_thread_id)
            .await
            .expect("open subtree with current-only descendants")
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([
            root_thread_id,
            worker_thread_id,
            current_only_thread_id,
            current_only_child_thread_id,
            mismatched_thread_id,
        ])
    );

    let handle = harness
        .manager
        .prepare_current_agent_membership_eviction(root_thread_id)
        .await
        .expect("archive handle");
    assert_eq!(
        handle
            .candidate_thread_ids()
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([root_thread_id, worker_thread_id, mismatched_thread_id,])
    );
    assert_eq!(
        handle
            .current_ids_with_current_only_descendants(&[worker_thread_id, mismatched_thread_id,])
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([
            worker_thread_id,
            current_only_thread_id,
            current_only_child_thread_id,
            mismatched_thread_id,
        ])
    );
    assert!(
        !handle
            .current_ids_with_current_only_descendants(&[worker_thread_id])
            .contains(&closed_thread_id)
    );
    assert!(
        !handle
            .current_ids_with_current_only_descendants(&[worker_thread_id])
            .contains(&permanently_closed_thread_id)
    );
    assert!(
        !handle
            .current_ids_with_current_only_descendants(&[worker_thread_id])
            .contains(&mismatched_child_thread_id)
    );
}

#[tokio::test]
async fn archive_runtime_unload_preserves_failed_identity_until_exact_evict() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, root_thread) = harness.start_thread().await;
    let control = root_thread.session.services.agent_control.clone();
    control.register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("worker task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions::default(),
        )
        .await;
    let worker_thread_id = worker_thread_id.expect("worker should spawn").thread_id;
    let child_thread_id = control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                parent_thread_id: Some(worker_thread_id),
                ..Default::default()
            },
        )
        .await
        .expect("child should spawn")
        .thread_id;
    let handle = harness
        .manager
        .prepare_current_agent_membership_eviction(root_thread_id)
        .await
        .expect("archive should fence the loaded registry");

    assert!(
        handle
            .unload_candidate_runtime_preserving_identity(child_thread_id)
            .await
            .expect("child runtime should unload")
    );
    assert!(
        handle
            .unload_candidate_runtime_preserving_identity(worker_thread_id)
            .await
            .expect("worker runtime should unload")
    );
    assert!(
        handle
            .unload_candidate_runtime_preserving_identity(root_thread_id)
            .await
            .expect("root runtime should unload")
    );
    assert_thread_not_loaded(&harness.manager, root_thread_id).await;
    assert_thread_not_loaded(&harness.manager, worker_thread_id).await;
    assert_thread_not_loaded(&harness.manager, child_thread_id).await;
    assert!(control.get_agent_metadata(worker_thread_id).is_some());
    assert!(control.get_agent_metadata(child_thread_id).is_some());
    assert_eq!(
        handle
            .evict_exact(&[root_thread_id])
            .await
            .expect("successful root archive should evict only the root identity"),
        1
    );
    for thread_id in [worker_thread_id, child_thread_id] {
        let metadata = control
            .get_agent_metadata(thread_id)
            .expect("failed candidate identity should remain registered");
        assert!(metadata.lifecycle.is_visible_when_cold());
        assert_eq!(
            metadata.lifecycle.cold_terminal_status(),
            Some(AgentStatus::Interrupted)
        );
        assert!(
            !control
                .upgrade()
                .expect("manager state")
                .is_thread_closing(thread_id)
        );
    }
    assert_eq!(
        control
            .current_agent_members()
            .await
            .expect("owning registry projection should load")
            .into_iter()
            .map(|member| member.thread_id)
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([worker_thread_id, child_thread_id])
    );
    assert_eq!(
        harness
            .manager
            .current_agent_members(root_thread_id)
            .await
            .expect("failed archive candidates should remain current")
            .into_iter()
            .map(|member| { (member.thread_id, (member.parent_thread_id, member.status),) })
            .collect::<std::collections::HashMap<_, _>>(),
        std::collections::HashMap::from([
            (worker_thread_id, (root_thread_id, AgentStatus::Interrupted),),
            (
                child_thread_id,
                (worker_thread_id, AgentStatus::Interrupted),
            ),
        ])
    );

    let handle = harness
        .manager
        .prepare_current_agent_membership_eviction(worker_thread_id)
        .await
        .expect("retry should fence the preserved cold branch");
    assert_eq!(
        handle
            .evict_exact(&[child_thread_id])
            .await
            .expect("exact successful archive should evict only the child"),
        1
    );
    assert!(control.get_agent_metadata(child_thread_id).is_none());
    assert!(control.get_agent_metadata(worker_thread_id).is_some());
    assert_eq!(
        harness
            .manager
            .current_agent_members(root_thread_id)
            .await
            .expect("failed worker should remain current")
            .into_iter()
            .map(|member| member.thread_id)
            .collect::<Vec<_>>(),
        vec![worker_thread_id]
    );

    let handle = harness
        .manager
        .prepare_current_agent_membership_eviction(worker_thread_id)
        .await
        .expect("final retry should resolve the retained registry");
    assert_eq!(
        handle
            .evict_exact(&[worker_thread_id])
            .await
            .expect("final retry should evict the worker"),
        1
    );
    assert_matches!(
        harness.manager.current_agent_members(root_thread_id).await,
        Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(id) if *id == root_thread_id)
    );
}

#[tokio::test]
async fn archive_membership_handle_evicts_identity_after_rollout_flush_error() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let worker = harness
        .manager
        .get_thread(worker_thread_id)
        .await
        .expect("worker should be loaded");
    worker
        .session
        .live_thread()
        .expect("worker should have durable persistence")
        .shutdown()
        .await
        .expect("test should stop the persistence writer");
    let handle = harness
        .manager
        .prepare_current_agent_membership_eviction(worker_thread_id)
        .await
        .expect("archive handle");

    let err = timeout(
        Duration::from_secs(5),
        handle.evict_exact(&[worker_thread_id]),
    )
    .await
    .expect("eviction should stop the runtime after a flush error")
    .expect_err("the stopped persistence writer must surface a flush error");
    assert!(
        err.to_string()
            .contains("failed to evict current agent identities")
    );
    assert!(
        harness
            .control
            .get_agent_metadata(worker_thread_id)
            .is_none()
    );
    assert_thread_not_loaded(&harness.manager, worker_thread_id).await;
    assert!(
        !harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(worker_thread_id)
    );
}
