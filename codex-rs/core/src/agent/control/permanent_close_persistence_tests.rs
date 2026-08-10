use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn permanent_close_cleans_registered_current_only_descendant_without_persisted_edge() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let current_only = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only runtime should start");
    let current_only_thread_id = current_only.thread_id;
    harness
        .control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("current-only registry slot")
        .commit(AgentMetadata {
            agent_id: Some(current_only_thread_id),
            parent_thread_id: Some(worker_thread_id),
            depth: Some(2),
            ephemeral: false,
            ..Default::default()
        });
    current_only
        .thread
        .update_thread_metadata(
            ThreadMetadataPatch {
                source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: worker_thread_id,
                    depth: 2,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
                thread_source: Some(Some(codex_protocol::protocol::ThreadSource::Subagent)),
                ..Default::default()
            },
            /*include_archived*/ true,
        )
        .await
        .expect("persist the same metadata shape produced by descendant adoption");
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .thread_goals()
        .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
            thread_id: current_only_thread_id,
            goal_id: "current-only-goal".to_string(),
            objective: "must be paused".to_string(),
            status: codex_state::ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("current-only Goal");
    state_db
        .thread_queue()
        .enqueue(current_only_thread_id, r#"{"message":"current-only"}"#)
        .await
        .expect("current-only queue");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("worker close should include its current-only descendant");
    assert_eq!(report.closed_agents, 2);
    assert_eq!(report.stopped_runtimes, 2);
    assert_eq!(report.paused_goals, 1);
    assert_eq!(report.cleared_queued_items, 1);
    assert_eq!(report.evicted_identities, 2);
    assert!(
        harness
            .manager
            .get_thread(current_only_thread_id)
            .await
            .is_err()
    );
    assert!(
        harness
            .control
            .get_agent_metadata(current_only_thread_id)
            .is_none()
    );
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(current_only_thread_id)
            .await
            .expect("current-only Goal should load")
            .expect("current-only Goal remains auditable")
            .status,
        codex_state::ThreadGoalStatus::Paused
    );
    assert!(
        state_db
            .thread_queue()
            .list_page(current_only_thread_id, 0, 10)
            .await
            .expect("current-only queue should load")
            .is_empty()
    );
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(current_only_thread_id)
            .await
            .expect("current-only Goal should load")
            .expect("current-only Goal remains auditable")
            .status,
        codex_state::ThreadGoalStatus::Paused
    );
    assert!(
        state_db
            .thread_queue()
            .list_page(current_only_thread_id, 0, 10)
            .await
            .expect("current-only queue should load")
            .is_empty()
    );
}

#[tokio::test]
async fn permanent_close_materializes_nested_current_only_ownership_for_restart_retry() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let current_only_parent = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only parent runtime should start");
    let current_only_parent_thread_id = current_only_parent.thread_id;
    let current_only_target = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only target runtime should start");
    let current_only_target_thread_id = current_only_target.thread_id;
    for (thread_id, parent_thread_id, depth) in [
        (current_only_parent_thread_id, root_thread_id, 1),
        (
            current_only_target_thread_id,
            current_only_parent_thread_id,
            2,
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
    for (thread, parent_thread_id, depth) in [
        (&current_only_parent.thread, root_thread_id, 1),
        (
            &current_only_target.thread,
            current_only_parent_thread_id,
            2,
        ),
    ] {
        thread
            .update_thread_metadata(
                ThreadMetadataPatch {
                    source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                        parent_thread_id,
                        depth,
                        agent_path: None,
                        agent_nickname: None,
                        agent_role: None,
                    })),
                    thread_source: Some(Some(codex_protocol::protocol::ThreadSource::Subagent)),
                    ..Default::default()
                },
                /*include_archived*/ true,
            )
            .await
            .expect("persist adopted current-only metadata");
    }
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .delete_threads_strict(&[current_only_parent_thread_id, current_only_target_thread_id])
        .await
        .expect("simulate adopted descendants whose metadata exists only in rollout storage");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, current_only_target_thread_id)
        .await
        .expect("nested current-only target should close directly");
    assert_eq!(report.closed_agents, 1);
    assert_eq!(report.closed_edges, 1);
    assert_eq!(report.newly_closed_edges, 1);
    assert!(
        harness
            .manager
            .get_thread(current_only_target_thread_id)
            .await
            .is_err()
    );
    assert!(
        harness
            .manager
            .get_thread(current_only_parent_thread_id)
            .await
            .is_ok()
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("materialized current-only ancestor"),
        vec![current_only_parent_thread_id]
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                current_only_parent_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("materialized target closure"),
        vec![current_only_target_thread_id]
    );
    assert_eq!(
        state_db
            .get_permanently_closed_thread_spawn_subtree(
                root_thread_id,
                current_only_target_thread_id,
            )
            .await
            .expect("restart retry lookup")
            .expect("materialized Open ancestor chain should authorize retry")
            .members,
        vec![codex_state::ClosedThreadSpawnSubtreeMember {
            thread_id: current_only_target_thread_id,
            depth: 0,
        }]
    );
}

#[tokio::test]
async fn permanent_close_accepts_registered_current_only_agent_as_direct_target() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let current_only = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only runtime should start");
    let current_only_thread_id = current_only.thread_id;
    harness
        .control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("current-only registry slot")
        .commit(AgentMetadata {
            agent_id: Some(current_only_thread_id),
            parent_thread_id: Some(worker_thread_id),
            depth: Some(2),
            ephemeral: false,
            ..Default::default()
        });
    current_only
        .thread
        .update_thread_metadata(
            ThreadMetadataPatch {
                source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: worker_thread_id,
                    depth: 2,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
                thread_source: Some(Some(codex_protocol::protocol::ThreadSource::Subagent)),
                ..Default::default()
            },
            /*include_archived*/ true,
        )
        .await
        .expect("persist the same metadata shape produced by descendant adoption");
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .delete_thread(current_only_thread_id)
        .await
        .expect("simulate adopted current-only rollout metadata without an ownership edge");
    let persisted_child_thread_id = ThreadId::new();
    state_db
        .upsert_thread_spawn_edge(
            current_only_thread_id,
            persisted_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("persisted child below current-only target");
    state_db
        .thread_goals()
        .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
            thread_id: current_only_thread_id,
            goal_id: "direct-current-only-goal".to_string(),
            objective: "must be paused".to_string(),
            status: codex_state::ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("current-only Goal");
    state_db
        .thread_queue()
        .enqueue(current_only_thread_id, r#"{"message":"current-only"}"#)
        .await
        .expect("current-only queue");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, current_only_thread_id)
        .await
        .expect("registered current-only target should close directly");
    assert_eq!(report.closed_agents, 2);
    assert_eq!(report.closed_edges, 2);
    assert_eq!(report.newly_closed_edges, 2);
    assert_eq!(report.stopped_runtimes, 1);
    assert_eq!(report.paused_goals, 1);
    assert_eq!(report.cleared_queued_items, 1);
    assert_eq!(report.evicted_identities, 1);
    assert!(
        harness
            .manager
            .get_thread(current_only_thread_id)
            .await
            .is_err()
    );
    assert!(
        harness
            .control
            .get_agent_metadata(current_only_thread_id)
            .is_none()
    );
    assert!(harness.manager.get_thread(worker_thread_id).await.is_ok());
    assert!(
        harness
            .control
            .get_agent_metadata(worker_thread_id)
            .is_some()
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                worker_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("materialized target edge"),
        vec![current_only_thread_id]
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                current_only_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("persisted child should close"),
        vec![persisted_child_thread_id]
    );
    assert!(
        state_db
            .upsert_thread_spawn_edge(
                worker_thread_id,
                current_only_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect_err("adoption must not reopen the current-only target after restart")
            .to_string()
            .contains("permanently closed")
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("unrelated open worker edge"),
        vec![worker_thread_id]
    );
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(current_only_thread_id)
            .await
            .expect("current-only Goal should load")
            .expect("current-only Goal remains auditable")
            .status,
        codex_state::ThreadGoalStatus::Paused
    );
    assert!(
        state_db
            .thread_queue()
            .list_page(current_only_thread_id, 0, 10)
            .await
            .expect("current-only queue should load")
            .is_empty()
    );
}

#[tokio::test]
async fn ephemeral_current_only_target_with_persisted_child_gets_a_durable_close() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
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
    let persisted_child_thread_id = ThreadId::new();
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
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .upsert_thread_spawn_edge(
            ephemeral_thread_id,
            persisted_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("persisted descendant below ephemeral target");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, ephemeral_thread_id)
        .await
        .expect("persisted descendant requires a durable target anchor");
    assert_eq!(report.closed_agents, 2);
    assert_eq!(report.closed_edges, 2);
    assert_eq!(report.newly_closed_edges, 2);
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("durable ephemeral target anchor"),
        vec![ephemeral_thread_id]
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                ephemeral_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("durable descendant closure"),
        vec![persisted_child_thread_id]
    );
}

#[tokio::test]
async fn ancestor_close_finishes_cleanup_for_permanently_closed_descendant() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
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
    let state_db = harness.state_db.as_ref().expect("state db");
    let child_close = state_db
        .close_open_thread_spawn_subtree(worker_thread_id, child_thread_id)
        .await
        .expect("child edge close")
        .expect("child should be owned");
    assert_eq!(child_close.newly_closed_edge_count, 1);
    let current_only = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only grandchild runtime should start");
    let current_only_thread_id = current_only.thread_id;
    harness
        .control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("current-only grandchild registry slot")
        .commit(AgentMetadata {
            agent_id: Some(current_only_thread_id),
            parent_thread_id: Some(child_thread_id),
            depth: Some(3),
            ephemeral: false,
            ..Default::default()
        });
    state_db
        .thread_goals()
        .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
            thread_id: current_only_thread_id,
            goal_id: "unfinished-current-only-goal".to_string(),
            objective: "ancestor close must pause the current-only grandchild".to_string(),
            status: codex_state::ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("unfinished current-only Goal");
    state_db
        .thread_queue()
        .enqueue(current_only_thread_id, r#"{"message":"unfinished"}"#)
        .await
        .expect("unfinished current-only queue");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("ancestor close should finish the permanently closed child cleanup");
    assert_eq!(report.closed_agents, 3);
    assert_eq!(report.closed_edges, 3);
    assert_eq!(report.newly_closed_edges, 2);
    assert_eq!(report.stopped_runtimes, 3);
    assert_eq!(report.paused_goals, 1);
    assert_eq!(report.cleared_queued_items, 1);
    assert!(harness.manager.get_thread(child_thread_id).await.is_err());
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_none()
    );
    assert!(
        harness
            .manager
            .get_thread(current_only_thread_id)
            .await
            .is_err()
    );
    assert!(
        harness
            .control
            .get_agent_metadata(current_only_thread_id)
            .is_none()
    );
}

#[tokio::test]
async fn permanent_close_retry_materializes_a_current_only_descendant() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .close_open_thread_spawn_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("initial durable close")
        .expect("worker remains owned");
    let current_only = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only runtime should start");
    let current_only_thread_id = current_only.thread_id;
    harness
        .control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("current-only registry slot")
        .commit(AgentMetadata {
            agent_id: Some(current_only_thread_id),
            parent_thread_id: Some(worker_thread_id),
            depth: Some(2),
            ephemeral: false,
            ..Default::default()
        });

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("retry should extend its durable cleanup set");
    assert_eq!(report.closed_agents, 2);
    assert_eq!(report.closed_edges, 2);
    assert_eq!(report.newly_closed_edges, 1);
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                worker_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("materialized current-only descendant"),
        vec![current_only_thread_id]
    );
}

#[tokio::test]
async fn permanent_close_retry_closes_an_existing_open_descendant() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let interrupted_child_thread_id = ThreadId::new();
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .upsert_thread_spawn_edge(
            worker_thread_id,
            interrupted_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("simulate an Open descendant below the durable target");
    state_db
        .upsert_thread_spawn_edge(
            root_thread_id,
            worker_thread_id,
            DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await
        .expect("simulate an interrupted close after only the target edge committed");
    state_db
        .thread_goals()
        .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
            thread_id: interrupted_child_thread_id,
            goal_id: "interrupted-child-goal".to_string(),
            objective: "durable retry must pause this Goal".to_string(),
            status: codex_state::ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("interrupted child Goal");
    state_db
        .thread_queue()
        .enqueue(interrupted_child_thread_id, r#"{"message":"interrupted"}"#)
        .await
        .expect("interrupted child queue");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("retry should close and clean the existing Open descendant");
    assert_eq!(report.closed_agents, 2);
    assert_eq!(report.closed_edges, 2);
    assert_eq!(report.newly_closed_edges, 1);
    assert_eq!(report.paused_goals, 1);
    assert_eq!(report.cleared_queued_items, 1);
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                worker_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("repaired child edge"),
        vec![interrupted_child_thread_id]
    );
    assert!(
        state_db
            .upsert_thread_spawn_edge(
                root_thread_id,
                interrupted_child_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect_err("ownership mutation must not reopen the repaired child")
            .to_string()
            .contains("permanently closed")
    );
}

#[tokio::test]
async fn permanent_close_clears_goal_and_queue_written_after_edge_fence() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("child should spawn");
    let transition_guard = harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("child metadata")
        .lifecycle
        .lock_transition()
        .await;
    let close_control = harness.control.clone();
    let close_task = tokio::spawn(async move {
        close_control
            .close_agent_subtree(root_thread_id, child_thread_id)
            .await
    });
    let state_db = harness.state_db.as_ref().expect("state db");
    timeout(Duration::from_secs(5), async {
        loop {
            let closed = state_db
                .list_thread_spawn_children_with_status(
                    root_thread_id,
                    DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
                )
                .await
                .expect("closed child query");
            if closed == vec![child_thread_id] {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("close should persist its edge fence before runtime shutdown");

    state_db
        .thread_goals()
        .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
            thread_id: child_thread_id,
            goal_id: "in-flight-goal".to_string(),
            objective: "in-flight goal".to_string(),
            status: codex_state::ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("in-flight Goal should persist");
    state_db
        .thread_queue()
        .enqueue(child_thread_id, r#"{"message":"in-flight"}"#)
        .await
        .expect("in-flight queued item should persist");
    drop(transition_guard);

    let report = close_task
        .await
        .expect("close task should join")
        .expect("close should finish after runtime release");
    assert_eq!(report.paused_goals, 1);
    assert_eq!(report.cleared_queued_items, 1);
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(child_thread_id)
            .await
            .expect("Goal should load")
            .expect("Goal remains auditable")
            .status,
        codex_state::ThreadGoalStatus::Paused
    );
    assert!(
        state_db
            .thread_queue()
            .list_page(child_thread_id, 0, 10)
            .await
            .expect("queue should load")
            .is_empty()
    );
}

#[tokio::test]
async fn permanent_close_fails_closed_without_store_for_non_ephemeral_identity() {
    let manager = ThreadManager::with_models_provider_for_tests(
        CodexAuth::from_api_key("dummy"),
        codex_model_provider_info::built_in_model_providers(
            /* openai_base_url */ /*openai_base_url*/ None,
        )["openai"]
            .clone(),
    );
    let control = manager.agent_control();
    let root_thread_id = ThreadId::new();
    let child_thread_id = ThreadId::new();
    control.register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("child registry slot")
        .commit(AgentMetadata {
            agent_id: Some(child_thread_id),
            parent_thread_id: Some(root_thread_id),
            ephemeral: false,
            ..Default::default()
        });

    let err = control
        .close_agent_subtree(root_thread_id, child_thread_id)
        .await
        .expect_err("non-ephemeral close without an ownership store must fail closed");
    assert_matches!(err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("ownership store"));
    assert!(control.get_agent_metadata(child_thread_id).is_some());
    assert!(
        !control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(child_thread_id)
    );

    let root_err = control
        .close_agent_subtree(root_thread_id, root_thread_id)
        .await
        .expect_err("root cannot be closed");
    assert_matches!(root_err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("root"));
}

#[tokio::test]
async fn permanent_close_rejects_cold_unregistered_goal_supervisor_uuid() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let supervisor_thread_id = ThreadId::new();
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("goal supervisor path");
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root_thread_id,
        depth: 1,
        agent_path: Some(supervisor_path),
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let rollout_path = harness
        .config
        .codex_home
        .join(format!("{supervisor_thread_id}.jsonl"));
    tokio::fs::write(&rollout_path, "")
        .await
        .expect("goal supervisor rollout path");
    let mut builder = codex_state::ThreadMetadataBuilder::new(
        supervisor_thread_id,
        rollout_path.to_path_buf(),
        chrono::Utc::now(),
        source,
    );
    builder.agent_role = Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string());
    let state_db = harness.state_db.as_ref().expect("state db");
    state_db
        .upsert_thread(&builder.build("openai"))
        .await
        .expect("goal supervisor metadata");
    state_db
        .upsert_thread_spawn_edge(
            root_thread_id,
            supervisor_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("goal supervisor edge");
    assert!(
        harness
            .control
            .get_agent_metadata(supervisor_thread_id)
            .is_none()
    );

    let err = harness
        .control
        .close_agent_subtree(root_thread_id, supervisor_thread_id)
        .await
        .expect_err("cold Goal Supervisor UUID must remain protected");
    assert_matches!(err.details(), CodexErrorDetails::UnsupportedOperation(message) if message == "goal supervisor agents cannot be closed with close_agent");
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("open Goal Supervisor edge"),
        vec![supervisor_thread_id]
    );
}

#[tokio::test]
async fn permanent_close_repairs_a_legacy_closed_edge_and_its_open_descendants() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let helper_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let child_thread_id = harness
        .spawn_anonymous_child(
            helper_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(helper_thread_id),
                ..Default::default()
            },
        )
        .await;
    harness
        .control
        .finish_internal_helper_thread(helper_thread_id)
        .await
        .expect("legacy helper completion should ordinary-close its incoming edge");
    let state_db = harness.state_db.as_ref().expect("state db");
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("legacy closed edge"),
        vec![helper_thread_id]
    );

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, helper_thread_id)
        .await
        .expect("close_agent should repair the pre-fix ordinary close");
    assert_eq!(report.closed_agents, 2);
    assert_eq!(report.newly_closed_edges, 2);
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("repaired helper edge"),
        vec![helper_thread_id]
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                helper_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("repaired descendant edge"),
        vec![child_thread_id]
    );
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_none()
    );
}

#[tokio::test]
async fn legacy_close_repair_materializes_a_current_only_descendant() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let helper_thread_id = harness
        .spawn_anonymous_child(root_thread_id, SpawnAgentOptions::default())
        .await;
    let helper_thread = harness
        .manager
        .get_thread(helper_thread_id)
        .await
        .expect("helper runtime should exist");
    persist_thread_for_tree_resume(&helper_thread, "persist legacy helper ownership").await;
    helper_thread
        .update_thread_metadata(
            ThreadMetadataPatch {
                source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: root_thread_id,
                    depth: 1,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
                thread_source: Some(Some(codex_protocol::protocol::ThreadSource::Subagent)),
                ..Default::default()
            },
            /*include_archived*/ true,
        )
        .await
        .expect("persist helper ownership metadata before legacy closure");
    harness
        .control
        .finish_internal_helper_thread(helper_thread_id)
        .await
        .expect("legacy helper completion");
    let current_only = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("current-only runtime should start");
    let current_only_thread_id = current_only.thread_id;
    harness
        .control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("current-only registry slot")
        .commit(AgentMetadata {
            agent_id: Some(current_only_thread_id),
            parent_thread_id: Some(helper_thread_id),
            depth: Some(2),
            ephemeral: false,
            ..Default::default()
        });
    let state_db = harness.state_db.as_ref().expect("state db");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, helper_thread_id)
        .await
        .expect("legacy repair should include current-only descendants");
    assert_eq!(report.closed_agents, 2);
    assert_eq!(report.closed_edges, 2);
    assert_eq!(report.newly_closed_edges, 2);
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                helper_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("materialized current-only descendant"),
        vec![current_only_thread_id]
    );
}

#[tokio::test]
async fn permanent_close_rejects_former_owner_after_incoming_edge_is_closed() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let promoted_thread_id = ThreadId::new();
    let descendant_thread_id = ThreadId::new();
    for (thread_id, parent_thread_id, depth) in [
        (promoted_thread_id, root_thread_id, 1),
        (descendant_thread_id, promoted_thread_id, 2),
    ] {
        harness
            .control
            .state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("registry slot")
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
            promoted_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("closed former incoming edge");
    state_db
        .upsert_thread_spawn_edge(
            promoted_thread_id,
            descendant_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("promoted descendant edge");

    let err = harness
        .control
        .close_agent_subtree(root_thread_id, promoted_thread_id)
        .await
        .expect_err("a former owner must not close a promoted independent root");
    assert_matches!(err.details(), CodexErrorDetails::UnsupportedOperation(message) if message.contains("not an owned descendant"));
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                promoted_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("promoted descendant edge must remain open"),
        vec![descendant_thread_id]
    );
    assert!(
        harness
            .control
            .get_agent_metadata(promoted_thread_id)
            .is_some()
    );
    assert!(
        !harness
            .control
            .upgrade()
            .expect("manager state")
            .is_thread_closing(promoted_thread_id)
    );
}

#[tokio::test]
async fn permanent_close_stops_at_promoted_descendant_ownership_boundary() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(root_thread_id, /*current_parent_thread_id*/ None);
    let worker_thread_id = ThreadId::new();
    let promoted_thread_id = ThreadId::new();
    let promoted_child_thread_id = ThreadId::new();
    for (thread_id, parent_thread_id, depth) in [
        (worker_thread_id, root_thread_id, 1),
        (promoted_thread_id, worker_thread_id, 2),
        (promoted_child_thread_id, promoted_thread_id, 3),
    ] {
        let metadata = AgentMetadata {
            agent_id: Some(thread_id),
            parent_thread_id: Some(parent_thread_id),
            depth: Some(depth),
            ephemeral: false,
            ..Default::default()
        };
        metadata
            .lifecycle
            .remember_cold_terminal_status(AgentStatus::Completed(None), true);
        harness
            .control
            .state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("registry slot")
            .commit(metadata);
    }
    let state_db = harness.state_db.as_ref().expect("state db");
    for (parent_thread_id, child_thread_id, status) in [
        (
            root_thread_id,
            worker_thread_id,
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
    ] {
        state_db
            .upsert_thread_spawn_edge(parent_thread_id, child_thread_id, status)
            .await
            .expect("ownership edge");
    }
    state_db
        .thread_goals()
        .replace_thread_goal_snapshot(&codex_state::ThreadGoal {
            thread_id: promoted_thread_id,
            goal_id: "promoted-goal".to_string(),
            objective: "remain independent".to_string(),
            status: codex_state::ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("promoted Goal");
    state_db
        .thread_queue()
        .enqueue(promoted_child_thread_id, r#"{"message":"independent"}"#)
        .await
        .expect("promoted queue");

    let report = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("worker close should stop at the promoted boundary");
    assert_eq!(report.closed_agents, 1);
    assert_eq!(report.closed_edges, 1);
    let retry = harness
        .control
        .close_agent_subtree(root_thread_id, worker_thread_id)
        .await
        .expect("same-process worker close retry should remain bounded to its first subtree");
    assert_eq!(retry.closed_agents, 1);
    assert_eq!(retry.newly_closed_edges, 0);
    assert!(
        harness
            .control
            .get_agent_metadata(promoted_thread_id)
            .is_some()
    );
    assert!(
        harness
            .control
            .get_agent_metadata(promoted_child_thread_id)
            .is_some()
    );
    assert_eq!(
        state_db
            .list_thread_spawn_children_with_status(
                promoted_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("promoted open descendants"),
        vec![promoted_child_thread_id]
    );
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(promoted_thread_id)
            .await
            .expect("promoted Goal query")
            .expect("promoted Goal remains")
            .status,
        codex_state::ThreadGoalStatus::Active
    );
    assert_eq!(
        state_db
            .thread_queue()
            .list_page(promoted_child_thread_id, 0, 10)
            .await
            .expect("promoted queue query")
            .len(),
        1
    );
}
