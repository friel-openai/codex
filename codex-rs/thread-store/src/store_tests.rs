use super::*;
use crate::InMemoryThreadStore;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::WorldStateItem;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

struct HistoryOnlyThreadStore {
    history: StoredThreadHistory,
    load_history_calls: AtomicUsize,
}

fn unsupported<T>() -> ThreadStoreFuture<'static, T> {
    Box::pin(async { Err(ThreadStoreError::Unsupported { operation: "test" }) })
}

impl ThreadStore for HistoryOnlyThreadStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_thread(&self, _params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn resume_thread(&self, _params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn append_items(&self, _params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn persist_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn flush_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn shutdown_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn discard_thread(&self, _thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        self.load_history_calls.fetch_add(1, Ordering::SeqCst);
        let history = self.history.clone();
        Box::pin(async move {
            if params.thread_id != history.thread_id {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            Ok(history)
        })
    }

    fn read_thread(&self, _params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        unsupported()
    }

    fn read_thread_by_rollout_path(
        &self,
        _params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        unsupported()
    }

    fn list_threads(&self, _params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        unsupported()
    }

    fn update_thread_metadata(
        &self,
        _params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        unsupported()
    }

    fn archive_thread(&self, _params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }

    fn unarchive_thread(
        &self,
        _params: ArchiveThreadParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        unsupported()
    }

    fn delete_thread(&self, _params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        unsupported()
    }
}

#[tokio::test]
async fn load_latest_model_context_defaults_to_full_history() {
    let thread_id = ThreadId::default();
    let items = vec![RolloutItem::WorldState(WorldStateItem::full(json!({
        "checkpoint": "history"
    })))];
    let store = HistoryOnlyThreadStore {
        history: StoredThreadHistory {
            thread_id,
            items: items.clone(),
        },
        load_history_calls: AtomicUsize::new(0),
    };

    let actual = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            rollout_path: None,
            include_archived: true,
        })
        .await
        .expect("default latest-model-context load should use full history");

    assert_eq!(
        serde_json::to_value(actual).expect("serialize actual model context"),
        serde_json::to_value(StoredModelContext { thread_id, items })
            .expect("serialize expected model context"),
    );
    assert_eq!(store.load_history_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn segment_checkpoint_default_fails_before_mutating_custom_store() {
    let thread_id = ThreadId::default();
    let store = HistoryOnlyThreadStore {
        history: StoredThreadHistory {
            thread_id,
            items: Vec::new(),
        },
        load_history_calls: AtomicUsize::new(0),
    };

    let outcome = store
        .persist_segment_checkpoint(
            thread_id,
            FreezeRolloutSegmentParams::rotate(vec![RolloutItem::WorldState(
                WorldStateItem::full(json!({"checkpoint": "must not be appended"})),
            )]),
        )
        .await;

    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::NotCommitted {
            error: ThreadStoreError::Unsupported {
                operation: "persist_segment_checkpoint"
            }
        }
    ));
    assert_eq!(store.load_history_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn in_memory_checkpoint_rejects_snapshot_without_mutation() {
    let store = InMemoryThreadStore::default();

    let outcome = store
        .persist_segment_checkpoint(ThreadId::new(), FreezeRolloutSegmentParams::snapshot())
        .await;

    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::NotCommitted {
            error: ThreadStoreError::InvalidRequest { .. }
        }
    ));
    assert_eq!(store.calls().await.append_items, 0);
}
