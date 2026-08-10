use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tempfile::TempDir;

use super::CheckpointPersistenceFailurePoint;
use super::immutable_segment_path;
use super::inject_checkpoint_persistence_failure;
use super::install_committed_replace_pause;
use super::pending_immutable_marker_path;
use super::prepare_rotation_immutable_install;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::FreezeRolloutSegmentParams;
use crate::LiveThread;
use crate::LocalThreadStore;
use crate::ResumeThreadParams;
use crate::SegmentCheckpointPersistenceOutcome;
use crate::ThreadPersistenceMetadata;
use crate::ThreadPersistenceMode;
use crate::ThreadStore;
use crate::live_thread::inject_next_checkpoint_metadata_failure;
use crate::local::test_support::test_config;

#[tokio::test]
async fn checkpoint_persistence_requires_the_store_that_owns_the_live_writer() {
    let home = TempDir::new().expect("temp dir");
    let owner = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let competing_store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    create_durable_thread(&owner, thread_id).await;
    append(&owner, thread_id, "writer-owned history").await;
    let active_path = owner
        .live_rollout_path(thread_id)
        .await
        .expect("active path");
    let before = tokio::fs::read(active_path.as_path())
        .await
        .expect("read owner rollout");

    let outcome = ThreadStore::persist_segment_checkpoint(
        &competing_store,
        thread_id,
        FreezeRolloutSegmentParams::rotate(vec![message("competing checkpoint")]),
    )
    .await;

    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::NotCommitted { .. }
    ));
    assert_eq!(
        tokio::fs::read(active_path.as_path())
            .await
            .expect("read unchanged owner rollout"),
        before
    );
}

#[tokio::test]
async fn immutable_segment_cannot_be_resumed_as_a_live_writer() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    let create = create_thread_params(thread_id);
    let metadata = create.metadata.clone();
    store.create_thread(create).await.expect("create thread");
    append(&store, thread_id, "immutable source").await;
    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze immutable source");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("release source writer");
    let immutable_path = frozen.reference.rollout_path;
    let before = tokio::fs::read(immutable_path.as_path())
        .await
        .expect("read immutable source");
    let resumed = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let error = resumed
        .resume_thread(ResumeThreadParams {
            thread_id,
            rollout_path: Some(immutable_path.clone()),
            history: None,
            include_archived: true,
            metadata,
        })
        .await
        .expect_err("immutable segment must not become a live writer");

    assert!(error.to_string().contains("immutable rollout segment"));
    assert_eq!(
        tokio::fs::read(immutable_path.as_path())
            .await
            .expect("read preserved immutable source"),
        before
    );

    let other_thread_id = ThreadId::new();
    let error = resumed
        .resume_thread(ResumeThreadParams {
            thread_id: other_thread_id,
            rollout_path: Some(immutable_path.clone()),
            history: Some(Arc::new(Vec::new())),
            include_archived: true,
            metadata: create_thread_params(other_thread_id).metadata,
        })
        .await
        .expect_err("an immutable segment must not be resumed under another thread id");

    assert!(error.to_string().contains("rollout belongs to"));
    assert_eq!(
        tokio::fs::read(immutable_path.as_path())
            .await
            .expect("read preserved cross-thread immutable source"),
        before
    );
}

#[tokio::test]
async fn partial_staged_checkpoint_failure_is_proven_not_committed() {
    assert_atomic_fallback_failure(
        CheckpointPersistenceFailurePoint::AfterFirstStagedRecord,
        vec![message("checkpoint first"), message("checkpoint second")],
    )
    .await;
}

#[tokio::test]
async fn complete_staged_rollback_batch_failure_is_proven_not_committed() {
    assert_atomic_fallback_failure(
        CheckpointPersistenceFailurePoint::AfterCompleteStagedBatch,
        vec![
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            message("post rollback checkpoint"),
        ],
    )
    .await;
}

#[tokio::test]
async fn failed_deferred_checkpoint_keeps_live_and_local_persistence_modes_aligned() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state runtime");
    let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
    let thread_id = ThreadId::new();
    let mut create = create_thread_params(thread_id);
    create.persistence_mode = ThreadPersistenceMode::Deferred;
    let live_thread = LiveThread::create(store, create)
        .await
        .expect("create deferred live thread");
    assert!(live_thread.is_persistence_deferred().await);
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterImmutableInstall,
    );
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterFirstStagedRecord,
    );

    let outcome = live_thread
        .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![message(
            "rejected deferred checkpoint",
        )]))
        .await;

    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::NotCommitted { .. }
    ));
    assert!(!live_thread.is_persistence_deferred().await);
    live_thread
        .append_items(&[message("metadata after durable transition")])
        .await
        .expect("append after durable transition");
    let metadata = runtime
        .get_thread(thread_id)
        .await
        .expect("read thread metadata")
        .expect("persisted thread metadata");
    assert_eq!(
        metadata.preview.as_deref(),
        Some("metadata after durable transition")
    );
}

#[tokio::test]
async fn failed_rotation_cleans_orphan_and_later_rotation_succeeds() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    create_durable_thread(&store, thread_id).await;
    append(&store, thread_id, "before fallback").await;
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("active path");
    let source_meta = codex_rollout::read_session_meta_line(active_path.as_path())
        .await
        .expect("source metadata");
    let orphan_path = immutable_segment_path(
        home.path(),
        thread_id,
        source_meta.meta.segment_id,
        active_path.as_path(),
    )
    .expect("orphan path");
    let orphan_marker = pending_immutable_marker_path(orphan_path.as_path());
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterImmutableInstall,
    );

    let outcome = ThreadStore::persist_segment_checkpoint(
        &store,
        thread_id,
        FreezeRolloutSegmentParams::rotate(vec![message("fallback checkpoint")]),
    )
    .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Committed
    ));
    assert!(!orphan_path.exists());
    assert!(!orphan_marker.exists());
    let fallback_meta = codex_rollout::read_session_meta_line(active_path.as_path())
        .await
        .expect("fallback metadata");
    assert_eq!(fallback_meta.meta.segment_id, source_meta.meta.segment_id);

    append(&store, thread_id, "after fallback").await;
    let outcome = ThreadStore::persist_segment_checkpoint(
        &store,
        thread_id,
        FreezeRolloutSegmentParams::rotate(vec![message("second checkpoint")]),
    )
    .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Committed
    ));
    let logical = codex_rollout::materialize_rollout_items(home.path(), active_path.as_path())
        .await
        .expect("materialize recovered rotations");
    for expected in [
        "before fallback",
        "fallback checkpoint",
        "after fallback",
        "second checkpoint",
    ] {
        assert_eq!(message_count(logical.as_slice(), expected), 1, "{expected}");
    }
}

#[tokio::test]
async fn metadata_failure_after_checkpoint_replace_remains_committed() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::default();
    let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
        .await
        .expect("create live thread");
    live_thread.persist().await.expect("persist live thread");
    live_thread
        .append_items(&[message("before metadata failure")])
        .await
        .expect("append before metadata failure");
    inject_next_checkpoint_metadata_failure(thread_id);

    let outcome = live_thread
        .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![message(
            "committed checkpoint",
        )]))
        .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Committed
    ));
    live_thread
        .append_items(&[message("after metadata recovery")])
        .await
        .expect("append after metadata recovery");
    let active_path = live_thread
        .local_rollout_path()
        .await
        .expect("read rollout path")
        .expect("local rollout path");
    let logical = codex_rollout::materialize_rollout_items(home.path(), active_path.as_path())
        .await
        .expect("materialize committed checkpoint");
    assert_eq!(message_count(logical.as_slice(), "committed checkpoint"), 1);
    assert_eq!(
        message_count(logical.as_slice(), "after metadata recovery"),
        1
    );
}

#[tokio::test]
async fn task_failure_after_atomic_replace_is_indeterminate() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    create_durable_thread(&store, thread_id).await;
    append(&store, thread_id, "before indeterminate checkpoint").await;
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("active path");
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterImmutableInstall,
    );
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::PanicAfterAtomicReplace,
    );

    let outcome = ThreadStore::persist_segment_checkpoint(
        &store,
        thread_id,
        FreezeRolloutSegmentParams::rotate(vec![message("indeterminate checkpoint")]),
    )
    .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Indeterminate { .. }
    ));
    let immediate_restart = RolloutRecorder::load_rollout_items(active_path.as_path())
        .await
        .expect("read committed indeterminate checkpoint")
        .0;
    assert_eq!(
        message_count(immediate_restart.as_slice(), "indeterminate checkpoint"),
        1
    );
}

#[tokio::test]
async fn cancelled_live_checkpoint_owner_still_fences_unknown_nominal_publication() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
        .await
        .expect("create live thread");
    live_thread.persist().await.expect("persist live thread");
    live_thread
        .append_items(&[message("before unknown nominal publication")])
        .await
        .expect("append source history");
    let active_path = live_thread
        .local_rollout_path()
        .await
        .expect("read rollout path")
        .expect("local rollout path");
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::NominalDurabilityUnknown,
    );
    let pause = install_committed_replace_pause(thread_id);
    let checkpoint_owner = {
        let live_thread = live_thread.clone();
        tokio::spawn(async move {
            live_thread
                .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![message(
                    "unknown nominal checkpoint",
                )]))
                .await
        })
    };
    pause.reached.notified().await;
    checkpoint_owner.abort();
    let queued_append = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            store
                .append_items(AppendThreadItemsParams {
                    thread_id,
                    items: vec![message("queued behind unknown publication")],
                })
                .await
        })
    };
    let queued_metadata = {
        let live_thread = live_thread.clone();
        tokio::spawn(async move {
            live_thread
                .update_memory_mode(ThreadMemoryMode::Disabled, /*include_archived*/ true)
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!queued_append.is_finished());
    assert!(!queued_metadata.is_finished());
    pause.release.notify_one();

    queued_append
        .await
        .expect("queued append task")
        .expect_err("cancelled owner must not discard the indeterminate fence");
    let persisted = RolloutRecorder::load_rollout_items(active_path.as_path())
        .await
        .expect("read rollout after queued append rejection")
        .0;
    assert_eq!(
        message_count(persisted.as_slice(), "queued behind unknown publication"),
        0
    );
    queued_metadata
        .await
        .expect("queued metadata task")
        .expect_err("cancelled checkpoint ownership must also fence metadata mutation");
}

#[tokio::test]
async fn unknown_fallback_publication_fences_later_live_mutation() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(store, create_thread_params(thread_id))
        .await
        .expect("create live thread");
    live_thread.persist().await.expect("persist live thread");
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterImmutableInstall,
    );
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::FallbackDurabilityUnknown,
    );

    let outcome = live_thread
        .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![message(
            "unknown fallback checkpoint",
        )]))
        .await;

    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Indeterminate { .. }
    ));
    live_thread
        .update_memory_mode(ThreadMemoryMode::Disabled, /*include_archived*/ true)
        .await
        .expect_err("metadata mutation must be fenced after an unknown fallback publication");
}

#[tokio::test]
async fn unknown_publication_retains_compressed_source_and_fresh_plain_reference() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    create_durable_thread(&store, thread_id).await;
    append(&store, thread_id, "history in compressed source").await;
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("active path");
    let source_meta = codex_rollout::read_session_meta_line(active_path.as_path())
        .await
        .expect("source metadata");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("release source writer");
    let compressed_path = active_path.with_extension("jsonl.zst");
    let source_bytes = tokio::fs::read(active_path.as_path())
        .await
        .expect("read source rollout");
    let compressed =
        zstd::stream::encode_all(source_bytes.as_slice(), 1).expect("compress source rollout");
    tokio::fs::write(compressed_path.as_path(), compressed)
        .await
        .expect("write compressed source");
    tokio::fs::remove_file(active_path.as_path())
        .await
        .expect("remove plain source");
    let abandoned_path = immutable_segment_path(
        home.path(),
        thread_id,
        source_meta.meta.segment_id,
        compressed_path.as_path(),
    )
    .expect("abandoned compressed immutable path");
    let abandoned_marker = pending_immutable_marker_path(abandoned_path.as_path());
    tokio::fs::create_dir_all(abandoned_marker.parent().expect("marker parent"))
        .await
        .expect("create marker parent");
    tokio::fs::write(
        abandoned_marker.as_path(),
        b"unfinished compressed rotation\n",
    )
    .await
    .expect("write abandoned marker");
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::NominalDurabilityUnknown,
    );

    store
        .freeze_thread_segment(
            thread_id,
            FreezeRolloutSegmentParams::rotate(vec![message("checkpoint from compressed source")]),
        )
        .await
        .expect_err("unknown publication must require restart");

    assert!(compressed_path.exists());
    assert!(active_path.exists());
    assert!(abandoned_marker.exists());
    let active_lines = RolloutRecorder::load_rollout_lines(active_path.as_path())
        .await
        .expect("read replacement active rollout")
        .0;
    let reference = active_lines
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::RolloutReference(reference) => Some(reference),
            _ => None,
        })
        .expect("replacement reference");
    assert_ne!(reference.rollout_path, abandoned_path);
    assert!(
        reference
            .rollout_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".jsonl"))
    );
    let logical = codex_rollout::materialize_rollout_items(home.path(), active_path.as_path())
        .await
        .expect("materialize compressed-source rotation");
    assert_eq!(
        message_count(logical.as_slice(), "history in compressed source"),
        1
    );
    assert_eq!(
        message_count(logical.as_slice(), "checkpoint from compressed source"),
        1
    );
}

#[tokio::test]
async fn retry_preserves_a_crash_marked_immutable_and_uses_a_fresh_identity() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    create_durable_thread(&store, thread_id).await;
    append(&store, thread_id, "source before crash").await;
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("active path");
    let source_meta = codex_rollout::read_session_meta_line(active_path.as_path())
        .await
        .expect("source metadata");
    let orphan_path = immutable_segment_path(
        home.path(),
        thread_id,
        source_meta.meta.segment_id,
        active_path.as_path(),
    )
    .expect("orphan path");
    tokio::fs::create_dir_all(orphan_path.parent().expect("orphan parent"))
        .await
        .expect("create orphan parent");
    tokio::fs::copy(active_path.as_path(), orphan_path.as_path())
        .await
        .expect("copy crash-marked immutable");
    let orphan_bytes = tokio::fs::read(orphan_path.as_path())
        .await
        .expect("read crash-marked immutable");
    let marker = pending_immutable_marker_path(orphan_path.as_path());
    tokio::fs::write(marker.as_path(), b"unfinished rotation\n")
        .await
        .expect("write orphan marker");
    let child_thread_id = ThreadId::new();
    create_durable_thread(&store, child_thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_thread_id,
            items: vec![RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_path: orphan_path.clone(),
                thread_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: source_meta.meta.segment_id,
                max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            })],
        })
        .await
        .expect("persist cross-thread reference to crash-marked immutable");
    let child_active_path = store
        .live_rollout_path(child_thread_id)
        .await
        .expect("child active path");

    let outcome = ThreadStore::persist_segment_checkpoint(
        &store,
        thread_id,
        FreezeRolloutSegmentParams::rotate(vec![message("checkpoint after restart")]),
    )
    .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Committed
    ));
    assert!(marker.exists());
    assert_eq!(
        tokio::fs::read(orphan_path.as_path())
            .await
            .expect("read preserved crash-marked immutable"),
        orphan_bytes
    );
    let active_lines = RolloutRecorder::load_rollout_lines(active_path.as_path())
        .await
        .expect("read active lines")
        .0;
    let reference_path = active_lines
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::RolloutReference(reference) => Some(reference.rollout_path.clone()),
            _ => None,
        })
        .expect("published immutable reference");
    assert_ne!(reference_path, orphan_path);
    let child_history =
        codex_rollout::materialize_rollout_items(home.path(), child_active_path.as_path())
            .await
            .expect("materialize preserved cross-thread reference");
    assert_eq!(
        message_count(child_history.as_slice(), "source before crash"),
        1
    );
    let referenced_bytes = tokio::fs::read(reference_path.as_path())
        .await
        .expect("read referenced immutable");
    let referenced_marker = pending_immutable_marker_path(reference_path.as_path());
    tokio::fs::write(referenced_marker.as_path(), b"stale marker\n")
        .await
        .expect("write referenced marker");
    let error = prepare_rotation_immutable_install(
        home.path(),
        reference_path.as_path(),
        active_lines.as_slice(),
    )
    .await
    .expect_err("referenced immutable must never be removed");
    assert!(error.to_string().contains("already references"));
    assert_eq!(
        tokio::fs::read(reference_path.as_path())
            .await
            .expect("read preserved referenced immutable"),
        referenced_bytes
    );
}

#[tokio::test]
async fn retry_preserves_an_unmarked_conflicting_immutable_and_uses_a_fresh_identity() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    create_durable_thread(&store, thread_id).await;
    append(&store, thread_id, "history in abandoned immutable").await;
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("active path");
    let source_meta = codex_rollout::read_session_meta_line(active_path.as_path())
        .await
        .expect("source metadata");
    let abandoned_path = immutable_segment_path(
        home.path(),
        thread_id,
        source_meta.meta.segment_id,
        active_path.as_path(),
    )
    .expect("abandoned immutable path");
    tokio::fs::create_dir_all(abandoned_path.parent().expect("abandoned parent"))
        .await
        .expect("create abandoned parent");
    tokio::fs::copy(active_path.as_path(), abandoned_path.as_path())
        .await
        .expect("copy abandoned immutable");
    let abandoned_bytes = tokio::fs::read(abandoned_path.as_path())
        .await
        .expect("read abandoned immutable");
    append(
        &store,
        thread_id,
        "history appended after abandoned install",
    )
    .await;

    let outcome = ThreadStore::persist_segment_checkpoint(
        &store,
        thread_id,
        FreezeRolloutSegmentParams::rotate(vec![message("checkpoint after upgrade")]),
    )
    .await;

    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Committed
    ));
    assert_eq!(
        tokio::fs::read(abandoned_path.as_path())
            .await
            .expect("read preserved abandoned immutable"),
        abandoned_bytes
    );
    let active_lines = RolloutRecorder::load_rollout_lines(active_path.as_path())
        .await
        .expect("read rotated active lines")
        .0;
    let reference_path = active_lines
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::RolloutReference(reference) => Some(reference.rollout_path.clone()),
            _ => None,
        })
        .expect("fresh immutable reference");
    assert_ne!(reference_path, abandoned_path);
    let logical = codex_rollout::materialize_rollout_items(home.path(), active_path.as_path())
        .await
        .expect("materialize rotation after upgrade");
    for expected in [
        "history in abandoned immutable",
        "history appended after abandoned install",
        "checkpoint after upgrade",
    ] {
        assert_eq!(message_count(logical.as_slice(), expected), 1, "{expected}");
    }
}

async fn assert_atomic_fallback_failure(
    failure_point: CheckpointPersistenceFailurePoint,
    checkpoint_items: Vec<RolloutItem>,
) {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    create_durable_thread(&store, thread_id).await;
    append(&store, thread_id, "authoritative history").await;
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("active path");
    let before = tokio::fs::read(active_path.as_path())
        .await
        .expect("read active before failure");
    inject_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterImmutableInstall,
    );
    inject_checkpoint_persistence_failure(thread_id, failure_point);

    let outcome = ThreadStore::persist_segment_checkpoint(
        &store,
        thread_id,
        FreezeRolloutSegmentParams::rotate(checkpoint_items.clone()),
    )
    .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::NotCommitted { .. }
    ));
    assert_eq!(
        tokio::fs::read(active_path.as_path())
            .await
            .expect("read active after failure"),
        before
    );
    let immediate_restart = RolloutRecorder::load_rollout_items(active_path.as_path())
        .await
        .expect("read active after immediate restart")
        .0;
    assert_eq!(
        message_count(immediate_restart.as_slice(), "authoritative history"),
        1
    );
    assert_checkpoint_items_absent(immediate_restart.as_slice(), checkpoint_items.as_slice());

    append(&store, thread_id, "continuation after recovery").await;
    let after_continuation = RolloutRecorder::load_rollout_items(active_path.as_path())
        .await
        .expect("read active after continuation")
        .0;
    assert_eq!(
        message_count(after_continuation.as_slice(), "continuation after recovery"),
        1
    );
    assert_checkpoint_items_absent(after_continuation.as_slice(), checkpoint_items.as_slice());
}

async fn create_durable_thread(store: &LocalThreadStore, thread_id: ThreadId) {
    store
        .create_thread(create_thread_params(thread_id))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");
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
        originator: "checkpoint-persistence-test".to_string(),
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
        metadata: ThreadPersistenceMetadata {
            cwd: Some(std::env::current_dir().expect("cwd")),
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}

async fn append(store: &LocalThreadStore, thread_id: ThreadId, text: &str) {
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message(text)],
        })
        .await
        .expect("append message");
}

fn message(text: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: text.to_string(),
        images: None,
        local_images: Vec::new(),
        text_elements: Vec::new(),
        ..Default::default()
    }))
}

fn message_count(items: &[RolloutItem], expected: &str) -> usize {
    items
        .iter()
        .filter(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event))
                    if event.message == expected
            )
        })
        .count()
}

fn assert_checkpoint_items_absent(actual: &[RolloutItem], rejected: &[RolloutItem]) {
    for item in rejected {
        match item {
            RolloutItem::EventMsg(EventMsg::UserMessage(event)) => {
                assert_eq!(message_count(actual, event.message.as_str()), 0);
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_)) => {
                assert!(!actual.iter().any(|item| matches!(
                    item,
                    RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_))
                )));
            }
            _ => panic!("unsupported checkpoint persistence test item"),
        }
    }
}
