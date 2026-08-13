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
