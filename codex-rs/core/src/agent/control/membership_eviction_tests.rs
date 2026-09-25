use super::*;
use pretty_assertions::assert_eq;
use tokio::time::Duration;
use tokio::time::timeout;

fn child_source(parent_thread_id: ThreadId) -> SessionSource {
    SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    })
}

#[tokio::test]
async fn eviction_keeps_current_only_descendants_of_validated_closed_owners() {
    let (home, mut config) = test_config().await;
    config
        .features
        .enable(Feature::Sqlite)
        .expect("enable indexed ownership");
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state = harness.control.upgrade().expect("manager state");
    let graph = state.agent_graph_store().expect("indexed ownership graph");
    let root_thread_id = ThreadId::new();
    let owner_thread_id = ThreadId::new();
    let current_child_id = ThreadId::new();
    let current_leaf_id = ThreadId::new();
    let closed_child_id = ThreadId::new();
    let closed_leaf_id = ThreadId::new();
    let mismatched_child_id = ThreadId::new();
    let mismatched_leaf_id = ThreadId::new();
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    for (thread_id, parent_thread_id, depth) in [
        (owner_thread_id, root_thread_id, 1),
        (current_child_id, owner_thread_id, 2),
        (current_leaf_id, current_child_id, 3),
        (closed_child_id, owner_thread_id, 2),
        (closed_leaf_id, closed_child_id, 3),
        (mismatched_child_id, owner_thread_id, 2),
        (mismatched_leaf_id, mismatched_child_id, 3),
    ] {
        harness
            .control
            .state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("register current identity")
            .commit(crate::agent::types::AgentMetadata {
                agent_id: Some(thread_id),
                parent_thread_id: Some(parent_thread_id),
                depth: Some(depth),
                ..Default::default()
            });
    }
    for (parent_thread_id, child_thread_id, status) in [
        (
            root_thread_id,
            owner_thread_id,
            codex_agent_graph_store::ThreadSpawnEdgeStatus::Closed,
        ),
        (
            owner_thread_id,
            closed_child_id,
            codex_agent_graph_store::ThreadSpawnEdgeStatus::Closed,
        ),
        (
            ThreadId::new(),
            mismatched_child_id,
            codex_agent_graph_store::ThreadSpawnEdgeStatus::Open,
        ),
    ] {
        graph
            .upsert_thread_spawn_edge(parent_thread_id, child_thread_id, status)
            .await
            .expect("persist ownership boundary");
    }
    // The eviction caller has validated the stopped owner's persisted SessionSource. The two
    // other incoming edges are not validated ownership and must exclude their entire subtrees.
    let persisted_owned_ids = std::collections::HashSet::from([root_thread_id, owner_thread_id]);
    let expected = std::collections::HashMap::from([
        (current_child_id, owner_thread_id),
        (current_leaf_id, current_child_id),
    ]);
    for owner_registered in [true, false] {
        if !owner_registered {
            harness
                .control
                .state
                .release_spawned_thread(owner_thread_id);
            assert!(
                harness
                    .control
                    .get_agent_metadata(owner_thread_id)
                    .is_none()
            );
        }
        assert_eq!(
            harness
                .control
                .current_only_descendant_parents_within(
                    root_thread_id,
                    &persisted_owned_ids,
                    Some(graph.as_ref()),
                )
                .await
                .expect("ordinary current membership"),
            std::collections::HashMap::new(),
            "ordinary membership must not cross a closed owner"
        );
        assert_eq!(
            harness
                .control
                .current_only_descendant_parents_for_eviction(
                    root_thread_id,
                    &persisted_owned_ids,
                    Some(graph.as_ref()),
                )
                .await
                .expect("eviction current membership"),
            expected,
            "eviction must retain children of a validated owner even after its registry entry is removed"
        );
    }
}

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
    let grandchild_lifecycle = harness
        .control
        .state
        .agent_lifecycle(grandchild_thread_id)
        .expect("registered grandchild lifecycle");
    grandchild_lifecycle.remember_cold_terminal_status(
        AgentStatus::Completed(None),
        /*visible_when_cold*/ true,
    );
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
async fn archive_or_delete_capture_fences_a_concurrent_spawn_before_thread_creation() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let state = harness.control.upgrade().expect("manager state");
    let lifecycle_mutation = state.lock_lifecycle_mutation().await;
    let control = harness.control.clone();
    let config = harness.config.clone();
    let mut spawn = tokio::spawn(async move {
        control
            .spawn_agent(
                config,
                text_input("late child"),
                Some(child_source(root_thread_id)),
            )
            .await
    });

    assert!(
        timeout(Duration::from_millis(50), &mut spawn)
            .await
            .is_err(),
        "spawn must wait while archive or delete captures current membership"
    );
    state.mark_threads_for_membership_eviction([root_thread_id]);
    drop(lifecycle_mutation);

    let error = spawn
        .await
        .expect("spawn task should not panic")
        .expect_err("spawn must fail after its parent is fenced");
    assert!(error.to_string().contains("being archived or deleted"));
    assert!(
        harness
            .manager
            .current_agent_members(root_thread_id)
            .await
            .expect("membership should remain readable")
            .is_empty()
    );
    state.unmark_threads_for_membership_eviction([root_thread_id]);
}

#[tokio::test]
async fn archive_or_delete_capture_fences_concurrent_lazy_identity_registration() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("persisted child"),
            Some(child_source(root_thread_id)),
        )
        .await
        .expect("child should spawn before eviction capture");
    harness
        .control
        .evict_current_agent_ids(&[child_thread_id])
        .await
        .expect("test child should become a cold persisted identity");
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_none()
    );

    let state = harness.control.upgrade().expect("manager state");
    let lifecycle_mutation = state.lock_lifecycle_mutation().await;
    let control = harness.control.clone();
    let mut registration = tokio::spawn(async move {
        control
            .ensure_open_agent_known_by_id(root_thread_id, child_thread_id)
            .await
    });

    assert!(
        timeout(Duration::from_millis(50), &mut registration)
            .await
            .is_err(),
        "lazy registration must wait while archive or delete captures current membership"
    );
    state.mark_threads_for_membership_eviction([root_thread_id, child_thread_id]);
    drop(lifecycle_mutation);

    let error = registration
        .await
        .expect("registration task should not panic")
        .expect_err("lazy registration must fail after its ownership chain is fenced");
    assert!(error.to_string().contains("being archived or deleted"));
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_none()
    );
    state.unmark_threads_for_membership_eviction([root_thread_id, child_thread_id]);
}
