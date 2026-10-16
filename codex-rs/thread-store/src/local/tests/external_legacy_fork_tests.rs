//! External Legacy capture preserves source persistence and writer ownership.

use codex_protocol::ThreadId;
use codex_rollout::RolloutRecorder;
use tempfile::TempDir;

use super::LocalThreadStore;
use super::assert_rollout_contains_message;
use super::create_thread_params;
use super::test_config;
use super::thread_metadata;
use super::user_message_item;
use crate::AppendThreadItemsParams;
use crate::FreezeRolloutSegmentParams;
use crate::PersistContext;
use crate::ResumeThreadParams;
use crate::ThreadStore;
use crate::ThreadStoreError;

#[tokio::test]
async fn external_writer_snapshot_rejects_the_writer_before_parsing_malformed_source() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let primary = LocalThreadStore::new(config.clone(), /*state_db*/ None);
    let secondary = LocalThreadStore::new(config, /*state_db*/ None);
    let thread_id = ThreadId::new();
    primary
        .create_thread(create_thread_params(thread_id))
        .await
        .expect("create Legacy source");
    primary
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist source");
    primary.flush_thread(thread_id).await.expect("flush source");
    let rollout_path = primary
        .live_rollout_path(thread_id)
        .await
        .expect("source rollout path");
    let malformed = b"not session metadata\n";
    tokio::fs::write(&rollout_path, malformed)
        .await
        .expect("replace flushed source bytes with malformed fixture");
    assert!(
        codex_rollout::read_session_meta_line(&rollout_path)
            .await
            .is_err()
    );

    let error = secondary
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect_err("active writer must be rejected before reading malformed source");

    assert!(matches!(
        error,
        ThreadStoreError::Conflict { message }
            if message == format!("thread {thread_id} already has an active writer")
    ));
    assert_eq!(
        tokio::fs::read(&rollout_path)
            .await
            .expect("source bytes after rejected snapshot"),
        malformed
    );
    primary
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown source writer");
}

#[tokio::test]
async fn snapshot_writer_availability_check_releases_its_advisory_lease() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let primary = LocalThreadStore::new(config.clone(), /*state_db*/ None);
    let secondary = LocalThreadStore::new(config, /*state_db*/ None);
    let thread_id = ThreadId::new();
    secondary
        .check_snapshot_writer_available(thread_id)
        .await
        .expect("a free thread ID permits a snapshot writer check");
    let primary_lease = primary
        .acquire_writer_lock(thread_id)
        .expect("successful advisory check must release its lease");

    let error = secondary
        .check_snapshot_writer_available(thread_id)
        .await
        .expect_err("advisory check must detect the primary writer's lease");

    assert!(matches!(
        error,
        ThreadStoreError::Conflict { message }
            if message == format!("thread {thread_id} already has an active writer")
    ));
    drop(primary_lease);
    secondary
        .check_snapshot_writer_available(thread_id)
        .await
        .expect("releasing the primary lease makes the thread available again");
}

#[tokio::test]
async fn external_legacy_capture_does_not_repair_metadata_or_take_source_writer() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state db should initialize");
    // The writer is intentionally detached from SQLite so ordinary lookup would seed its row.
    let primary = LocalThreadStore::new(config.clone(), /*state_db*/ None);
    let secondary = LocalThreadStore::new(config.clone(), Some(runtime.clone()));
    let competing = LocalThreadStore::new(config, /*state_db*/ None);
    let thread_id = ThreadId::new();
    primary
        .create_thread(create_thread_params(thread_id))
        .await
        .expect("create Legacy source");
    primary
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![user_message_item("captured user message")],
        })
        .await
        .expect("append source item");
    primary
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist source");
    primary.flush_thread(thread_id).await.expect("flush source");
    let rollout_path = primary
        .live_rollout_path(thread_id)
        .await
        .expect("source rollout path");
    let rollout_id = codex_rollout::rollout_id_from_path(&rollout_path)
        .expect("source rollout has a canonical filename");
    let bytes_before = tokio::fs::read(&rollout_path)
        .await
        .expect("source bytes before capture");
    let (items_before, _, parse_errors) = RolloutRecorder::load_rollout_items(&rollout_path)
        .await
        .expect("source items before capture");
    assert_eq!(parse_errors, 0);
    let metadata_before = runtime
        .get_thread(thread_id)
        .await
        .expect("source metadata before capture");
    assert_eq!(metadata_before, None);

    let error = secondary
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect_err("ordinary snapshot must reject the external writer");
    assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    assert_eq!(
        runtime
            .get_thread(thread_id)
            .await
            .expect("source metadata after rejected snapshot"),
        metadata_before
    );

    let prepared = secondary
        .capture_external_legacy_fork(thread_id, Some(rollout_id))
        .await
        .expect("capture source owned by another writer");

    assert_eq!(
        tokio::fs::read(&rollout_path)
            .await
            .expect("source bytes after capture"),
        bytes_before
    );
    assert_eq!(
        runtime
            .get_thread(thread_id)
            .await
            .expect("source metadata after capture"),
        metadata_before
    );
    assert!(prepared.frozen_segment.is_none());
    assert!(prepared.history_base.is_none());
    let copied = prepared
        .copied_history
        .as_ref()
        .expect("external fork owns copied history");
    let expected_history = serde_json::to_value(&items_before).expect("serialize source history");
    assert_eq!(
        serde_json::to_value(copied.as_ref()).expect("serialize copied history"),
        expected_history
    );
    assert_eq!(
        serde_json::to_value(prepared.response_history.as_ref())
            .expect("serialize response history"),
        expected_history
    );

    let resume_params = ResumeThreadParams {
        history_revision: None,
        thread_id,
        rollout_path: Some(rollout_path.clone()),
        history: None,
        include_archived: false,
        metadata: thread_metadata(),
    };
    let error = competing
        .resume_thread(resume_params.clone())
        .await
        .expect_err("capture must leave the primary writer's lease intact");
    assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    primary
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![user_message_item("primary write after capture")],
        })
        .await
        .expect("primary writer remains usable");
    primary
        .flush_thread(thread_id)
        .await
        .expect("flush primary write after capture");
    assert_rollout_contains_message(&rollout_path, "primary write after capture").await;
    assert_eq!(
        serde_json::to_value(copied.as_ref()).expect("serialize copied history"),
        expected_history
    );

    // Prove this missing-row fixture would detect the ordinary finder's read repair.
    let found = codex_rollout::find_thread_path_by_id_str(
        home.path(),
        &thread_id.to_string(),
        Some(runtime.as_ref()),
    )
    .await
    .expect("ordinary source lookup");
    assert_eq!(found.as_ref(), Some(&rollout_path));
    let repaired = runtime
        .get_thread(thread_id)
        .await
        .expect("metadata after ordinary lookup")
        .expect("ordinary lookup seeds the missing metadata row");
    assert_eq!(repaired.rollout_path, rollout_path);

    drop(prepared);
    primary
        .shutdown_thread(thread_id)
        .await
        .expect("primary shutdown releases writer ownership");
    competing
        .resume_thread(resume_params)
        .await
        .expect("capture must not retain another source writer lease");
    competing
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown competing writer");
}
