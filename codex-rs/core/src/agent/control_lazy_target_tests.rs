use super::*;
use pretty_assertions::assert_eq;

#[test_case::test_case("id")]
#[test_case::test_case("relative_reference")]
#[tokio::test]
async fn inspect_restores_indexed_identity_without_loading_rollout(target_kind: &str) {
    let harness = AgentControlHarness::new().await;
    let (parent_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(parent_id, /*current_parent_thread_id*/ None);
    let child_id = ThreadId::new();
    let child_path = AgentPath::root().join("worker").expect("child path");
    let rollout_path = harness.config.codex_home.join("unreadable-child.jsonl");
    tokio::fs::write(&rollout_path, "not a rollout record\n")
        .await
        .expect("write malformed rollout");
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: parent_id,
        depth: 1,
        agent_path: Some(child_path.clone()),
        agent_nickname: Some("Worker".to_string()),
        agent_role: Some("worker".to_string()),
    });
    let mut builder = codex_state::ThreadMetadataBuilder::new(
        child_id,
        rollout_path.to_path_buf(),
        chrono::Utc::now(),
        source,
    );
    builder.agent_path = Some(child_path.to_string());
    builder.agent_nickname = Some("Worker".to_string());
    builder.agent_role = Some("worker".to_string());
    let state_db = harness.state_db.as_ref().expect("state database");
    state_db
        .upsert_thread(&builder.build("openai"))
        .await
        .expect("persist indexed identity");
    state_db
        .upsert_thread_spawn_edge(parent_id, child_id, DirectionalThreadSpawnEdgeStatus::Open)
        .await
        .expect("persist ownership");

    assert!(harness.control.get_agent_metadata(child_id).is_none());
    assert_thread_not_loaded(&harness.manager, child_id).await;
    let target = match target_kind {
        "id" => AgentTarget::Id(child_id),
        "relative_reference" => AgentTarget::Reference("worker".to_string()),
        _ => unreachable!("unknown test target"),
    };
    let info = harness
        .control
        .inspect(parent_id, target)
        .await
        .expect("inspect indexed identity without reading malformed rollout");
    assert_matches!(&info, AgentInfo::Unloaded(_));
    let metadata = info.metadata();
    assert_eq!(
        (
            metadata.agent_id,
            metadata.parent_thread_id,
            metadata.agent_path.as_ref(),
            metadata.agent_nickname.as_deref(),
            metadata.agent_role.as_deref(),
            info.status(),
        ),
        (
            Some(child_id),
            Some(parent_id),
            Some(&child_path),
            Some("Worker"),
            Some("worker"),
            None,
        )
    );
    assert_thread_not_loaded(&harness.manager, child_id).await;
}

#[tokio::test]
async fn queue_only_delivery_restores_cold_indexed_recipient_without_starting_turn() {
    let (home, mut config) = test_config().await;
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("enable multi-agent v2");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("enable SQLite");
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_id, parent) = harness.start_thread().await;
    let child = spawn_v2_reload_test_child(
        &parent.session.services.agent_control,
        harness.config.clone(),
        &parent,
        "worker",
    )
    .await;
    let thread = harness
        .manager
        .get_thread(child.thread_id)
        .await
        .expect("child runtime");
    persist_thread_for_tree_resume(&thread, "persisted child history").await;
    thread.shutdown_and_wait().await.expect("stop child");
    assert!(
        harness
            .manager
            .remove_thread(&child.thread_id)
            .await
            .is_some()
    );
    drop(thread);
    harness
        .state_db
        .as_ref()
        .expect("state database")
        .upsert_thread_spawn_edge(
            parent_id,
            child.thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("persist open ownership");

    let control = harness.manager.agent_control();
    control.register_session_root(parent_id, /*current_parent_thread_id*/ None);
    assert!(control.get_agent_metadata(child.thread_id).is_none());
    assert_thread_not_loaded(&harness.manager, child.thread_id).await;
    let receipt = control
        .send(crate::SendRequest {
            caller: parent_id,
            target: AgentTarget::Id(child.thread_id),
            resume_config: harness.config.clone(),
            input: crate::AgentInput::Message {
                message: AgentMessage::Plaintext("queued after restart".to_string()),
                mode: MessageDeliveryMode::QueueOnly,
            },
            start_options: TurnStartOptions {
                parent_turn_id: Some("must-not-wake-child".to_string()),
                ..Default::default()
            },
        })
        .await
        .expect("restore indexed recipient and queue message");
    assert_eq!(receipt.thread_id, child.thread_id);
    assert!(!receipt.submission_id.is_empty());
    let resumed = harness
        .manager
        .get_thread(child.thread_id)
        .await
        .expect("recipient loaded on demand");
    timeout(Duration::from_secs(5), async {
        loop {
            if resumed
                .session
                .input_queue
                .has_pending_input(&resumed.session.active_turn)
                .await
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("queue-only message should remain pending");
    assert!(resumed.session.active_turn.lock().await.is_none());
    let communication = AgentMessage::Plaintext("queued after restart".to_string())
        .into_communication(
            AgentPath::root(),
            child.metadata.agent_path.expect("recipient path"),
            MessageDeliveryMode::QueueOnly,
        );
    assert!(harness.manager.captured_ops().into_iter().any(|(id, op)| {
        matches!(op, Op::InterAgentCommunication { communication: actual, start_options }
            if id == child.thread_id && actual == communication && start_options.parent_turn_id.is_none())
    }));
    resumed
        .shutdown_and_wait()
        .await
        .expect("stop resumed child");
}

#[tokio::test]
async fn loaded_unregistered_id_allows_inspection_and_user_input_but_not_agent_messages() {
    let harness = AgentControlHarness::new().await;
    let (caller_id, _) = harness.start_thread().await;
    let (target_id, target) = harness.start_thread().await;
    harness
        .control
        .register_session_root(caller_id, /*current_parent_thread_id*/ None);
    assert!(harness.control.get_agent_metadata(target_id).is_none());

    let info = harness
        .control
        .inspect(caller_id, AgentTarget::Id(target_id))
        .await
        .expect("loaded thread remains inspectable by ID");
    assert_matches!(info, AgentInfo::Loaded { agent, .. } if agent.thread_id == target_id);
    let err = harness
        .control
        .send(crate::SendRequest {
            caller: caller_id,
            target: AgentTarget::Id(target_id),
            resume_config: harness.config.clone(),
            input: crate::AgentInput::Message {
                message: AgentMessage::Plaintext("not a member".to_string()),
                mode: MessageDeliveryMode::QueueOnly,
            },
            start_options: Default::default(),
        })
        .await
        .err()
        .expect("ordinary messages require membership");
    assert_matches!(err.details(), CodexErrorDetails::ThreadNotFound(id) if *id == target_id);

    let receipt = harness
        .control
        .send(crate::SendRequest {
            caller: caller_id,
            target: AgentTarget::Id(target_id),
            resume_config: harness.config.clone(),
            input: crate::AgentInput::UserInput(text_input("direct user input")),
            start_options: Default::default(),
        })
        .await
        .expect("direct user input does not require agent registration");
    assert_eq!(receipt.thread_id, target_id);
    assert!(!receipt.submission_id.is_empty());
    wait_for_recorded_user_message(&target, "direct user input").await;
    assert!(harness.control.get_agent_metadata(target_id).is_none());
    target.shutdown_and_wait().await.expect("stop target");
}

#[tokio::test]
async fn unloaded_agent_info_does_not_expose_cached_terminal_status() {
    let harness = AgentControlHarness::new().await;
    let child_id = ThreadId::new();
    harness
        .control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("reserve child identity")
        .commit(AgentMetadata {
            agent_id: Some(child_id),
            ..Default::default()
        });
    let expected = AgentStatus::Completed(Some("completed before unload".to_string()));
    harness
        .control
        .state
        .agent_lifecycle(child_id)
        .expect("registered lifecycle")
        .remember_cold_terminal_status(expected.clone(), /*visible_when_cold*/ true);
    let info = harness
        .control
        .inspect(ThreadId::new(), AgentTarget::Id(child_id))
        .await
        .expect("registered dormant agent remains inspectable");
    assert_matches!(&info, AgentInfo::Unloaded(metadata) if metadata.agent_id == Some(child_id));
    assert_eq!(info.status(), None);
    assert_eq!(harness.control.get_status(child_id).await, expected);
    assert_eq!(
        harness
            .control
            .subscribe_status(child_id)
            .await
            .expect("compatibility subscription retains cached terminal status")
            .borrow()
            .clone(),
        expected
    );
    assert_thread_not_loaded(&harness.manager, child_id).await;
}

#[tokio::test]
async fn inspect_distinguishes_missing_id_and_unresolved_reference() {
    let harness = AgentControlHarness::new().await;
    let (caller_id, _) = harness.start_thread().await;
    harness
        .control
        .register_session_root(caller_id, /*current_parent_thread_id*/ None);
    let missing_id = ThreadId::new();
    let err = harness
        .control
        .inspect(caller_id, AgentTarget::Id(missing_id))
        .await
        .expect_err("missing ID must remain an error");
    assert_matches!(err.details(), CodexErrorDetails::ThreadNotFound(id) if *id == missing_id);
    let err = harness
        .control
        .inspect(caller_id, AgentTarget::Reference("missing".to_string()))
        .await
        .expect_err("unresolved reference must remain an error");
    assert_eq!(
        err.to_string(),
        "unsupported operation: open owned agent path `/root/missing` not found"
    );
}

#[tokio::test]
async fn inspect_preserves_dropped_manager_error() {
    let control = LocalAgentControl::default();
    let err = control
        .inspect(ThreadId::new(), AgentTarget::Id(ThreadId::new()))
        .await
        .expect_err("manager loss is not an unloaded or missing agent");
    assert_eq!(
        err.to_string(),
        "unsupported operation: thread manager dropped"
    );
}
