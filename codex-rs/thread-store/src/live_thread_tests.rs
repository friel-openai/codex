use super::*;
use crate::InMemoryThreadStore;
use crate::ThreadPersistenceMetadata;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn retains_debounced_and_failed_root_recency_touches() {
    let store = Arc::new(InMemoryThreadStore::default());
    store
        .queue_root_recency_touch_results([Ok(Some(Duration::from_secs(60 * 60)))])
        .await;
    let (blocked_touch_started, blocked_touch_release) = store
        .queue_blocked_root_recency_touch(Ok(Some(Duration::from_secs(60 * 60))))
        .await;
    store
        .queue_root_recency_touch_results([
            Err(ThreadStoreError::Internal {
                message: "injected root recency failure".to_string(),
            }),
            Ok(Some(Duration::from_secs(60 * 60))),
        ])
        .await;
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
        .await
        .expect("live thread should be created");
    let activity = RolloutItem::ResponseItem(
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "descendant activity".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    );

    live_thread
        .append_items(std::slice::from_ref(&activity))
        .await
        .expect("first activity should persist and touch root recency");
    live_thread
        .append_items(std::slice::from_ref(&activity))
        .await
        .expect("second activity should remain pending during debounce");
    assert_eq!(store.calls().await.touch_root_thread_recency, 1);
    assert!(
        live_thread
            .root_recency_touch_state
            .lock()
            .await
            .has_pending_activity()
    );

    live_thread
        .root_recency_touch_state
        .lock()
        .await
        .next_attempt = Some(Instant::now());
    let cancelled_flush = tokio::spawn({
        let live_thread = live_thread.clone();
        async move { live_thread.flush().await }
    });
    blocked_touch_started.notified().await;
    cancelled_flush.abort();
    assert!(
        cancelled_flush
            .await
            .expect_err("flush should be cancelled")
            .is_cancelled()
    );
    drop(blocked_touch_release);
    assert_eq!(store.calls().await.touch_root_thread_recency, 2);
    assert!(
        live_thread
            .root_recency_touch_state
            .lock()
            .await
            .has_pending_activity()
    );

    live_thread
        .flush()
        .await
        .expect("failed root recency touch should remain best effort");
    assert_eq!(store.calls().await.touch_root_thread_recency, 3);
    assert!(
        live_thread
            .root_recency_touch_state
            .lock()
            .await
            .has_pending_activity()
    );

    live_thread
        .shutdown()
        .await
        .expect("shutdown should retry the failed root recency touch");
    assert_eq!(store.calls().await.touch_root_thread_recency, 4);
    assert!(
        !live_thread
            .root_recency_touch_state
            .lock()
            .await
            .has_pending_activity()
    );
}

fn create_thread_params(thread_id: ThreadId) -> CreateThreadParams {
    CreateThreadParams {
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "test_originator".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: ThreadHistoryMode::Legacy,
        history_base: None,
        subagent_history_start_ordinal: None,
        persistence_mode: ThreadPersistenceMode::Durable,
        initial_rollout_ordinal: 0,
        initial_window_id: uuid::Uuid::now_v7().to_string(),
        runtime_workspace_roots: None,
        metadata: ThreadPersistenceMetadata {
            cwd: None,
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}
