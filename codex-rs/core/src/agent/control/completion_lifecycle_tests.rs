use super::*;
use crate::session_prefix::format_inter_agent_completion_message;
use pretty_assertions::assert_eq;

fn subagent_notification_count(items: &[ResponseItem]) -> usize {
    items
        .iter()
        .filter(|item| {
            let ResponseItem::Message { role, content, .. } = item else {
                return false;
            };
            role == "user"
                && content.iter().any(|content| match content {
                    ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                        SubagentNotification::matches_text(text)
                    }
                    ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
                })
        })
        .count()
}

fn rollout_response_items(items: &[RolloutItem]) -> Vec<ResponseItem> {
    items
        .iter()
        .flat_map(|item| match item {
            RolloutItem::ResponseItem(item) => vec![item.clone()],
            RolloutItem::Compacted(compacted) => {
                compacted.replacement_history.clone().unwrap_or_default()
            }
            RolloutItem::SessionMeta(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::TurnContext(_)
            | RolloutItem::WorldState(_)
            | RolloutItem::EventMsg(_) => Vec::new(),
        })
        .collect()
}

async fn loaded_current_subagent_count(
    harness: &AgentControlHarness,
    root_thread_id: ThreadId,
) -> usize {
    let mut count = 0;
    for thread_id in harness
        .control
        .current_membership_subtree_thread_ids(root_thread_id)
    {
        if thread_id != root_thread_id && harness.manager.get_thread(thread_id).await.is_ok() {
            count += 1;
        }
    }
    count
}

async fn complete_agent(thread: &Arc<CodexThread>, message: &str) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some(message.to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
}

async fn wait_for_completion_delivery(lifecycle: &Arc<crate::agent::registry::AgentLifecycle>) {
    timeout(Duration::from_secs(10), async {
        while lifecycle.completion_watcher_registered() || lifecycle.has_pending_completion_status()
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completion watcher should finish delivery");
}

#[tokio::test]
async fn promotion_internal_shutdown_does_not_notify_former_parent() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, root_thread) = harness.start_thread().await;
    let (child_thread_id, child_thread) = harness
        .spawn_quiescent_pathless_v1_child(root_thread_id, "promotion child")
        .await;
    let child_lifecycle = harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("child metadata")
        .lifecycle;
    complete_agent(&child_thread, "promotion completion").await;
    wait_for_completion_delivery(&child_lifecycle).await;
    assert_eq!(
        subagent_notification_count(root_thread.session.clone_history().await.raw_items()),
        1
    );

    let promoted_thread_id = harness
        .control
        .promote_agent(child_thread_id)
        .await
        .expect("completed pathless child should promote");
    assert_eq!(promoted_thread_id, child_thread_id);
    sleep(Duration::from_millis(100)).await;

    assert_eq!(
        subagent_notification_count(root_thread.session.clone_history().await.raw_items()),
        1,
        "promotion unload must not synthesize a shutdown completion"
    );
    assert_eq!(
        history_text_match_count(
            root_thread.session.clone_history().await.raw_items(),
            "promotion completion"
        ),
        1
    );
    assert!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .is_none()
    );
    let promoted_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("promoted root should remain loaded");
    assert!(!promoted_thread.session_source.is_non_root_agent());
    assert_eq!(
        harness
            .control
            .current_membership_subtree_thread_ids(root_thread_id),
        vec![root_thread_id]
    );
}

#[tokio::test]
async fn adoption_internal_shutdown_does_not_notify_transferred_parent() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (destination_root_id, _destination_root) = harness.start_thread().await;
    let adoptable = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone()))
        .await
        .expect("start independent adoptable root");
    let original_control = adoptable.thread.session.services.agent_control.clone();
    let descendant_thread_id = original_control
        .spawn_agent_with_communication(
            harness.config.clone(),
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                "adoption descendant".to_string(),
                /*trigger_turn*/ false,
            ),
            AgentCommunicationContext::new(AgentCommunicationKind::Spawn, adoptable.thread_id),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: adoptable.thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("spawn adoptable descendant")
        .thread_id;
    let descendant = harness
        .manager
        .get_thread(descendant_thread_id)
        .await
        .expect("adoptable descendant should load");
    let _ = descendant
        .session
        .input_queue
        .drain_mailbox_input_items()
        .await;
    let descendant_lifecycle = original_control
        .get_agent_metadata(descendant_thread_id)
        .expect("descendant metadata")
        .lifecycle;

    complete_agent(&descendant, "adoption descendant completion").await;
    wait_for_completion_delivery(&descendant_lifecycle).await;
    assert_eq!(
        subagent_notification_count(adoptable.thread.session.clone_history().await.raw_items()),
        1
    );
    complete_agent(&adoptable.thread, "adoptable root ready").await;
    adoptable.thread.ensure_rollout_materialized().await;
    adoptable
        .thread
        .flush_rollout()
        .await
        .expect("flush adoptable tree");

    let (communication, context, source) = adoption_request(destination_root_id);
    harness
        .control
        .adopt_agent_with_communication(
            harness.config.clone(),
            adoptable.thread_id,
            communication,
            context,
            source,
            /*parent_turn_id*/ None,
        )
        .await
        .expect("completed root with pathless descendant should transfer");
    sleep(Duration::from_millis(100)).await;

    let adopted = harness
        .manager
        .get_thread(adoptable.thread_id)
        .await
        .expect("adopted root should load");
    assert_eq!(
        subagent_notification_count(adopted.session.clone_history().await.raw_items()),
        1,
        "descendant unload must not synthesize a shutdown completion"
    );
    assert_eq!(
        history_text_match_count(
            adopted.session.clone_history().await.raw_items(),
            "adoption descendant completion"
        ),
        1
    );
    assert!(
        original_control
            .get_agent_metadata(descendant_thread_id)
            .is_none()
    );
    assert_eq!(
        harness
            .control
            .get_agent_metadata(descendant_thread_id)
            .expect("transferred descendant metadata")
            .parent_thread_id,
        Some(adoptable.thread_id)
    );
    let mut current_tree = harness
        .control
        .current_membership_subtree_thread_ids(destination_root_id);
    current_tree.sort_by_key(ToString::to_string);
    let mut expected_tree = vec![
        destination_root_id,
        adoptable.thread_id,
        descendant_thread_id,
    ];
    expected_tree.sort_by_key(ToString::to_string);
    assert_eq!(current_tree, expected_tree);
}

#[tokio::test]
async fn cold_registered_parent_persists_child_completion_exactly_once() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (root_thread_id, root_thread) = harness.start_thread().await;
    let (parent_thread_id, parent_thread) = harness
        .spawn_quiescent_pathless_v1_child(root_thread_id, "cold parent")
        .await;
    let (child_thread_id, child_thread) = harness
        .spawn_quiescent_pathless_v1_child(parent_thread_id, "cold child")
        .await;
    let parent_lifecycle = harness
        .control
        .get_agent_metadata(parent_thread_id)
        .expect("parent metadata")
        .lifecycle;
    let child_lifecycle = harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("child metadata")
        .lifecycle;

    complete_agent(&parent_thread, "parent ready for eviction").await;
    wait_for_completion_delivery(&parent_lifecycle).await;
    assert_eq!(
        subagent_notification_count(root_thread.session.clone_history().await.raw_items()),
        1
    );
    let state = harness
        .control
        .upgrade()
        .expect("manager should remain live");
    harness
        .control
        .unload_agent_thread(&state, parent_thread_id)
        .await
        .expect("completed parent should become cold");
    assert_thread_not_loaded(&harness.manager, parent_thread_id).await;

    complete_agent(&child_thread, "cold child completion").await;
    timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(reloaded_parent) = harness.manager.get_thread(parent_thread_id).await
                && history_text_match_count(
                    reloaded_parent.session.clone_history().await.raw_items(),
                    "cold child completion",
                ) == 1
                && !child_lifecycle.has_pending_completion_status()
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child completion should reload and update its registered parent");
    wait_for_completion_delivery(&child_lifecycle).await;

    let reloaded_parent = harness
        .manager
        .get_thread(parent_thread_id)
        .await
        .expect("completion delivery should reload parent");
    let persisted_parent = reloaded_parent
        .read_thread(
            /*include_archived*/ true, /*include_history*/ true,
        )
        .await
        .expect("completion notification should already be readable from the rollout");
    let persisted_response_items = rollout_response_items(
        &persisted_parent
            .history
            .expect("parent history should be loaded")
            .items,
    );
    assert_eq!(
        history_text_match_count(&persisted_response_items, "cold child completion"),
        1,
        "the child terminal must remain claimed until its parent rollout is durable"
    );
    harness
        .control
        .unload_agent_thread(&state, parent_thread_id)
        .await
        .expect("parent should become cold again");
    harness
        .control
        .ensure_agent_loaded(harness.config.clone(), parent_thread_id)
        .await
        .expect("registered parent should reload from its rollout");
    let restarted_parent = harness
        .manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent should be loaded after restart");
    assert_eq!(
        history_text_match_count(
            restarted_parent.session.clone_history().await.raw_items(),
            "cold child completion"
        ),
        1
    );
    assert_eq!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .expect("child membership should remain registered")
            .parent_thread_id,
        Some(parent_thread_id)
    );
    let mut current_tree = harness
        .control
        .current_membership_subtree_thread_ids(root_thread_id);
    current_tree.sort_by_key(ToString::to_string);
    let mut expected_tree = vec![root_thread_id, parent_thread_id, child_thread_id];
    expected_tree.sort_by_key(ToString::to_string);
    assert_eq!(current_tree, expected_tree);
}

#[tokio::test]
async fn pathful_v2_completion_reloads_cold_parent_before_mailbox_delivery() {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 4;
    let harness = AgentControlHarness::new_with_config(home, config.clone()).await;
    let (root_thread_id, _root_thread) = harness.start_thread().await;
    let parent_path = AgentPath::try_from("/root/parent").expect("parent path");
    let child_path = AgentPath::try_from("/root/parent/child").expect("child path");
    let parent_thread_id = harness
        .control
        .spawn_agent_with_communication(
            config.clone(),
            InterAgentCommunication::new(
                AgentPath::root(),
                parent_path.clone(),
                Vec::new(),
                "start parent".to_string(),
                /*trigger_turn*/ false,
            ),
            AgentCommunicationContext::new(AgentCommunicationKind::Spawn, root_thread_id),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: Some(parent_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("pathful parent should start")
        .thread_id;
    let child_thread_id = harness
        .control
        .spawn_agent_with_communication(
            config,
            InterAgentCommunication::new(
                parent_path.clone(),
                child_path.clone(),
                Vec::new(),
                "start child".to_string(),
                /*trigger_turn*/ false,
            ),
            AgentCommunicationContext::new(AgentCommunicationKind::Spawn, parent_thread_id),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 2,
                agent_path: Some(child_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("pathful child should start")
        .thread_id;
    let parent_thread = harness
        .manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent should load");
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child should load");
    let _ = parent_thread
        .session
        .input_queue
        .drain_mailbox_input_items()
        .await;
    let _ = child_thread
        .session
        .input_queue
        .drain_mailbox_input_items()
        .await;

    complete_agent(&parent_thread, "parent ready for cold delivery").await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread
        .flush_rollout()
        .await
        .expect("flush parent before unload");
    let state = harness
        .control
        .upgrade()
        .expect("manager should remain live");
    harness
        .control
        .unload_agent_thread(&state, parent_thread_id)
        .await
        .expect("completed pathful parent should become cold");
    assert_thread_not_loaded(&harness.manager, parent_thread_id).await;

    complete_agent(&child_thread, "pathful child complete").await;
    let reloaded_parent = timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(parent) = harness.manager.get_thread(parent_thread_id).await
                && parent.session.input_queue.has_pending_mailbox_items().await
            {
                break parent;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("canonical completion should reload its registered cold parent");
    let expected_message = format_inter_agent_completion_message(
        parent_path.clone(),
        child_path.clone(),
        &AgentStatus::Completed(Some("pathful child complete".to_string())),
    )
    .expect("completion should render");
    assert_eq!(
        reloaded_parent
            .session
            .input_queue
            .drain_mailbox_input_items()
            .await
            .0,
        vec![crate::session::TurnInput::InterAgentCommunication(
            InterAgentCommunication::new(
                child_path,
                parent_path,
                Vec::new(),
                expected_message,
                /*trigger_turn*/ false,
            )
        )]
    );
}

#[tokio::test]
async fn cold_parent_completion_delivery_does_not_deadlock_at_residency_capacity() {
    let (home, mut config) = test_config().await;
    let _ = config.features.disable(Feature::MultiAgentV2);
    config.agent_max_threads = Some(9);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (root_thread_id, root_thread) = harness.start_thread().await;
    let (parent_thread_id, parent_thread) = harness
        .spawn_quiescent_pathless_v1_child(root_thread_id, "capacity parent")
        .await;
    let (child_thread_id, child_thread) = harness
        .spawn_quiescent_pathless_v1_child(parent_thread_id, "capacity child")
        .await;
    let parent_lifecycle = harness
        .control
        .get_agent_metadata(parent_thread_id)
        .expect("parent metadata")
        .lifecycle;
    let child_lifecycle = harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("child metadata")
        .lifecycle;

    complete_agent(&parent_thread, "capacity parent ready").await;
    wait_for_completion_delivery(&parent_lifecycle).await;
    let state = harness
        .control
        .upgrade()
        .expect("manager should remain live");
    harness
        .control
        .unload_agent_thread(&state, parent_thread_id)
        .await
        .expect("completed parent should become cold");
    assert_thread_not_loaded(&harness.manager, parent_thread_id).await;

    let mut filler_thread_ids = Vec::new();
    for index in 0..7 {
        let (thread_id, _thread) = harness
            .spawn_quiescent_pathless_v1_child(root_thread_id, &format!("resident filler {index}"))
            .await;
        filler_thread_ids.push(thread_id);
    }
    assert_eq!(
        loaded_current_subagent_count(&harness, root_thread_id).await,
        8
    );

    complete_agent(&child_thread, "capacity child completion").await;
    wait_for_completion_delivery(&child_lifecycle).await;
    harness
        .control
        .ensure_agent_loaded(harness.config.clone(), parent_thread_id)
        .await
        .expect("parent should remain reloadable after capacity-pressure delivery");
    let reloaded_parent = harness
        .manager
        .get_thread(parent_thread_id)
        .await
        .expect("parent should be loaded after completion delivery");
    assert_eq!(
        history_text_match_count(
            reloaded_parent.session.clone_history().await.raw_items(),
            "capacity child completion"
        ),
        1
    );
    assert_eq!(
        history_text_match_count(
            root_thread.session.clone_history().await.raw_items(),
            "capacity parent ready"
        ),
        1
    );
    timeout(Duration::from_secs(10), async {
        while loaded_current_subagent_count(&harness, root_thread_id).await > 8 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completion delivery should restore the configured residency capacity");
    assert_eq!(
        harness
            .control
            .get_agent_metadata(child_thread_id)
            .expect("child membership should remain registered")
            .parent_thread_id,
        Some(parent_thread_id)
    );
    assert_eq!(filler_thread_ids.len(), 7);
}
