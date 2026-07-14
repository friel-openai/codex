use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::protocol::WorldStateItem;
use codex_protocol::user_input::UserInput;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::install_immutable_segment;
use super::snapshot_segment_id;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::FreezeRolloutSegmentParams;
use crate::LiveThread;
use crate::ThreadPersistenceMetadata;
use crate::ThreadPersistenceMode;
use crate::ThreadStore;

#[tokio::test]
async fn deferred_live_thread_stays_pathless_until_freeze_materializes_its_canonical_journal() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let sqlite = config.sqlite.clone();
    let runtime =
        codex_state::StateRuntime::init(sqlite.clone(), config.default_model_provider_id.clone())
            .await
            .expect("initialize state db");
    let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
    let thread_id = ThreadId::default();
    let mut params = create_params(thread_id, ThreadHistoryMode::Legacy);
    params.persistence_mode = ThreadPersistenceMode::Deferred;
    let live_thread = LiveThread::create(store.clone(), params)
        .await
        .expect("create deferred live thread");
    let clone = live_thread.clone();
    let stable_path = live_thread
        .local_rollout_path()
        .await
        .expect("read rollout path")
        .expect("local rollout path");

    live_thread
        .append_items(&[user_message_item("ephemeral prefix")])
        .await
        .expect("append deferred item");
    clone.flush().await.expect("flush deferred thread");
    assert!(
        !tokio::fs::try_exists(stable_path.as_path())
            .await
            .expect("check deferred rollout path"),
        "append and flush must leave a deferred rollout pathless"
    );
    assert_eq!(
        runtime
            .get_thread(thread_id)
            .await
            .expect("read deferred metadata"),
        None,
        "deferred append and flush must not write thread metadata"
    );
    let history_pool = codex_state::open_thread_history_db(&sqlite)
        .await
        .expect("open thread history db");
    let projection_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_one(&history_pool)
    .await
    .expect("count projection state");
    assert_eq!(projection_count, 0);

    let frozen = clone
        .freeze_local_segment(FreezeRolloutSegmentParams::rotate(vec![user_message_item(
            "replacement suffix",
        )]))
        .await
        .expect("freeze deferred thread")
        .expect("local frozen segment");
    assert!(
        tokio::fs::try_exists(stable_path.as_path())
            .await
            .expect("check stable rollout")
    );
    assert!(
        runtime
            .get_thread(thread_id)
            .await
            .expect("read materialized metadata")
            .is_some(),
        "freeze must make deferred metadata durable"
    );
    let immutable_items =
        RolloutRecorder::load_rollout_items(frozen.reference.rollout_path.as_path())
            .await
            .expect("load immutable segment")
            .0;
    assert!(matches!(
        immutable_items.first(),
        Some(RolloutItem::SessionMeta(_))
    ));
    assert!(has_message(&immutable_items, "ephemeral prefix"));
    let mut segment_entries = tokio::fs::read_dir(
        frozen
            .reference
            .rollout_path
            .parent()
            .expect("segment directory"),
    )
    .await
    .expect("read segment directory");
    let mut segment_file_count = 0;
    while segment_entries
        .next_entry()
        .await
        .expect("read segment entry")
        .is_some()
    {
        segment_file_count += 1;
    }
    assert_eq!(segment_file_count, 1);

    let replacement_items = RolloutRecorder::load_rollout_items(stable_path.as_path())
        .await
        .expect("load replacement rollout")
        .0;
    assert!(matches!(
        replacement_items.as_slice(),
        [
            RolloutItem::SessionMeta(_),
            RolloutItem::RolloutReference(_),
            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent { message, .. }))
        ] if message == "replacement suffix"
    ));

    let logical_items =
        codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
            .await
            .expect("materialize reference-backed rollout");
    assert!(has_message(&logical_items, "ephemeral prefix"));
    assert!(has_message(&logical_items, "replacement suffix"));

    live_thread
        .append_items(&[user_message_item("durable append")])
        .await
        .expect("append after freeze");
    live_thread.flush().await.expect("flush after freeze");
    let logical_items =
        codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
            .await
            .expect("materialize durable continuation");
    assert!(has_message(&logical_items, "durable append"));
}

#[tokio::test]
async fn live_freeze_installs_immutable_prefix_and_isolates_later_appends() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");
    append_message(&store, thread_id, "before freeze").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let source_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read source prefix");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze live segment");
    assert_eq!(frozen.history_mode, ThreadHistoryMode::Legacy);
    assert_eq!(frozen.next_rollout_ordinal, None);
    assert!(
        tokio::fs::try_exists(frozen.reference.rollout_path.as_path())
            .await
            .expect("check immutable segment")
    );
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable prefix"),
        source_bytes
    );

    append_message(&store, thread_id, "after freeze").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let immutable_items =
        RolloutRecorder::load_rollout_items(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable segment")
            .0;
    assert!(has_message(&immutable_items, "before freeze"));
    assert!(!has_message(&immutable_items, "after freeze"));

    let live_path = store.live_rollout_path(thread_id).await.expect("live path");
    assert_eq!(live_path, stable_path);
    let replacement_meta = codex_rollout::read_session_meta_line(live_path.as_path())
        .await
        .expect("read replacement metadata");
    assert_eq!(replacement_meta.meta.id, thread_id);
    assert_ne!(
        replacement_meta.meta.segment_id,
        frozen.source_session_meta.meta.segment_id
    );
    let logical_items = codex_rollout::materialize_rollout_items(home.path(), live_path.as_path())
        .await
        .expect("materialize logical history");
    assert!(has_message(&logical_items, "before freeze"));
    assert!(has_message(&logical_items, "after freeze"));
}

#[tokio::test]
async fn paginated_rotation_installs_the_exact_source_bytes() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create paginated thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated metadata");
    append_message(&store, thread_id, "paginated prefix").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let source_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read paginated source");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("rotate paginated segment");

    assert_eq!(frozen.history_mode, ThreadHistoryMode::Paginated);
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path)
            .await
            .expect("read immutable paginated segment"),
        source_bytes
    );
}

#[tokio::test]
async fn repeated_freeze_without_local_items_reuses_existing_reference() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "shared prefix").await;

    let first = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze first segment");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let stable_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read stable rollout");

    let second = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("reuse frozen segment");

    assert_eq!(second.reference.rollout_path, first.reference.rollout_path);
    assert_eq!(second.reference.segment_id, first.reference.segment_id);
    assert_eq!(
        tokio::fs::read(stable_path)
            .await
            .expect("reread stable rollout"),
        stable_bytes
    );
}

#[tokio::test]
async fn snapshot_stabilizes_nested_references_to_legacy_mutable_rollouts() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let parent_id = ThreadId::default();
    store
        .create_thread(create_params(parent_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create parent");
    store
        .persist_thread(parent_id)
        .await
        .expect("persist parent");
    append_message(&store, parent_id, "stable inherited prefix").await;
    store.flush_thread(parent_id).await.expect("flush parent");
    let parent_path = store
        .live_rollout_path(parent_id)
        .await
        .expect("parent rollout path");
    let parent_meta = codex_rollout::read_session_meta_line(parent_path.as_path())
        .await
        .expect("read parent metadata");
    let parent_reference = RolloutReferenceItem {
        rollout_path: parent_path,
        thread_id: Some(parent_id),
        rollout_timestamp: None,
        segment_id: parent_meta.meta.segment_id,
        max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    let child_id = ThreadId::default();
    store
        .create_thread(create_params(child_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create child");
    store.persist_thread(child_id).await.expect("persist child");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![RolloutItem::RolloutReference(parent_reference)],
        })
        .await
        .expect("append legacy child reference");
    store.flush_thread(child_id).await.expect("flush child");
    let child_path = store
        .live_rollout_path(child_id)
        .await
        .expect("child rollout path");
    let child_meta = codex_rollout::read_session_meta_line(child_path.as_path())
        .await
        .expect("read child metadata");

    let grandchild_id = ThreadId::default();
    store
        .create_thread(create_params(grandchild_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create grandchild");
    store
        .persist_thread(grandchild_id)
        .await
        .expect("persist grandchild");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: grandchild_id,
            items: vec![RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_path: child_path,
                thread_id: Some(child_id),
                rollout_timestamp: None,
                segment_id: child_meta.meta.segment_id,
                max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            })],
        })
        .await
        .expect("append legacy grandchild reference");
    store
        .flush_thread(grandchild_id)
        .await
        .expect("flush grandchild");

    let frozen = store
        .freeze_thread_segment(grandchild_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze legacy reference graph");
    append_message(&store, parent_id, "later mutable parent append").await;
    store.flush_thread(parent_id).await.expect("flush append");

    let frozen_lines = RolloutRecorder::load_rollout_lines(frozen.reference.rollout_path.as_path())
        .await
        .expect("load frozen child segment")
        .0;
    let RolloutItem::RolloutReference(stabilized_parent) = &frozen_lines[1].item else {
        panic!("frozen child segment must retain its parent reference");
    };
    assert!(
        stabilized_parent.rollout_path.starts_with(
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        )
    );
    let logical_items = codex_rollout::materialize_rollout_items(
        home.path(),
        frozen.reference.rollout_path.as_path(),
    )
    .await
    .expect("materialize stabilized graph");
    assert!(has_message(&logical_items, "stable inherited prefix"));
    assert!(!has_message(&logical_items, "later mutable parent append"));
}

#[tokio::test]
async fn snapshots_after_append_get_new_identity_and_unchanged_snapshots_reuse_it() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "first snapshot").await;

    let first = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze first snapshot");
    append_message(&store, thread_id, "second snapshot").await;
    store.flush_thread(thread_id).await.expect("flush append");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let stable_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read stable rollout");

    let second = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze changed snapshot");
    let third = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("reuse unchanged snapshot");

    assert_ne!(first.reference.segment_id, second.reference.segment_id);
    assert_ne!(first.reference.rollout_path, second.reference.rollout_path);
    assert_eq!(third.reference.segment_id, second.reference.segment_id);
    assert_eq!(third.reference.rollout_path, second.reference.rollout_path);
    let installed_lines =
        RolloutRecorder::load_rollout_lines(second.reference.rollout_path.as_path())
            .await
            .expect("load installed snapshot")
            .0;
    assert_eq!(
        snapshot_segment_id(installed_lines.as_slice()).expect("rehash installed snapshot"),
        second.reference.segment_id.expect("snapshot segment ID")
    );
    assert_eq!(
        tokio::fs::read(stable_path)
            .await
            .expect("reread stable rollout"),
        stable_bytes
    );
}

#[tokio::test]
async fn snapshot_identity_ignores_source_segment_id_and_object_insertion_order() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let mut first = RolloutRecorder::load_rollout_lines(stable_path.as_path())
        .await
        .expect("load first source")
        .0;
    let mut second = first.clone();
    let RolloutItem::SessionMeta(first_meta) = &mut first[0].item else {
        panic!("first source must start with session metadata");
    };
    first_meta.meta.segment_id = Some(SegmentId::new());
    let RolloutItem::SessionMeta(second_meta) = &mut second[0].item else {
        panic!("second source must start with session metadata");
    };
    second_meta.meta.segment_id = Some(SegmentId::new());

    let mut first_state = serde_json::Map::new();
    first_state.insert("z".to_string(), serde_json::json!(1));
    first_state.insert("a".to_string(), serde_json::json!(2));
    let mut second_state = serde_json::Map::new();
    second_state.insert("a".to_string(), serde_json::json!(2));
    second_state.insert("z".to_string(), serde_json::json!(1));
    first.push(RolloutLine {
        timestamp: "2026-07-14T00:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::WorldState(WorldStateItem::full(first_state.into())),
    });
    second.push(RolloutLine {
        timestamp: "2026-07-14T00:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::WorldState(WorldStateItem::full(second_state.into())),
    });

    assert_eq!(
        snapshot_segment_id(first.as_slice()).expect("hash first source"),
        snapshot_segment_id(second.as_slice()).expect("hash second source")
    );
}

#[tokio::test]
async fn full_history_child_storage_does_not_scale_with_parent_history() {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let (small_parent_bytes, small_children_bytes) =
            stored_parent_and_children_bytes(history_mode, /*parent_message_count*/ 1).await;
        let (large_parent_bytes, large_children_bytes) =
            stored_parent_and_children_bytes(history_mode, /*parent_message_count*/ 128).await;

        assert!(
            large_parent_bytes > small_parent_bytes.saturating_add(32 * 1_024),
            "large parent fixture must be substantially larger for {history_mode:?}: \
             small={small_parent_bytes}, large={large_parent_bytes}"
        );
        assert!(
            large_children_bytes.abs_diff(small_children_bytes) < 1_024,
            "child rollout storage must not scale with parent history for {history_mode:?}: \
             small={small_children_bytes}, large={large_children_bytes}"
        );
    }
}

#[tokio::test]
async fn segmentless_legacy_history_freezes_under_initial() {
    assert_segmentless_source_freezes(ThreadHistoryMode::Legacy).await;
}

#[tokio::test]
async fn segmentless_paginated_history_freezes_under_initial() {
    assert_segmentless_source_freezes(ThreadHistoryMode::Paginated).await;
}

#[tokio::test]
async fn immutable_install_copies_source_and_rejects_different_existing_contents() {
    let home = TempDir::new().expect("temp dir");
    let source = home.path().join("source.jsonl");
    let destination = home.path().join("segments").join("segment.jsonl");
    tokio::fs::write(source.as_path(), b"frozen prefix")
        .await
        .expect("write source");

    install_immutable_segment(source.as_path(), destination.as_path())
        .await
        .expect("install immutable copy");
    tokio::fs::write(source.as_path(), b"mutated source")
        .await
        .expect("mutate source");
    assert_eq!(
        tokio::fs::read(destination.as_path())
            .await
            .expect("read immutable copy"),
        b"frozen prefix"
    );

    tokio::fs::write(source.as_path(), b"frozen prefix")
        .await
        .expect("restore identical source");
    install_immutable_segment(source.as_path(), destination.as_path())
        .await
        .expect("accept identical immutable copy");

    tokio::fs::write(source.as_path(), b"different prefix")
        .await
        .expect("write conflicting source");
    let err = install_immutable_segment(source.as_path(), destination.as_path())
        .await
        .expect_err("reject conflicting immutable copy");
    assert!(
        err.to_string().contains("different contents"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn unloaded_freeze_replaces_stable_rollout_without_installing_a_writer() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "closed prefix").await;
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown thread");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze unloaded segment");
    assert!(
        store.live_rollout_path(thread_id).await.is_err(),
        "unloaded freeze must not install a live writer"
    );
    assert!(
        tokio::fs::try_exists(stable_path.as_path())
            .await
            .expect("check stable path")
    );
    let logical_items =
        codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
            .await
            .expect("materialize unloaded history");
    assert!(has_message(&logical_items, "closed prefix"));
    assert_eq!(frozen.reference.thread_id, Some(thread_id));
    assert_eq!(
        frozen.reference.segment_id,
        frozen.source_session_meta.meta.segment_id
    );
}

#[tokio::test]
async fn paginated_freeze_continues_ordinals_and_resets_only_projection_offset() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let config = test_config(home.path());
    let sqlite = config.sqlite.clone();
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze paginated segment");
    assert_eq!(frozen.next_rollout_ordinal, Some(1));
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let (lines, _, parse_errors) = RolloutRecorder::load_rollout_lines(stable_path.as_path())
        .await
        .expect("read replacement rollout");
    assert_eq!(parse_errors, 0);
    assert_eq!(
        lines.iter().map(|line| line.ordinal).collect::<Vec<_>>(),
        vec![Some(1), Some(2)]
    );

    let pool = codex_state::open_thread_history_db(&sqlite)
        .await
        .expect("open history db");
    let projection_state = sqlx::query_as::<_, (i64, i64)>(
        "SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read projection state");
    let replacement_len = i64::try_from(
        tokio::fs::metadata(stable_path)
            .await
            .expect("replacement metadata")
            .len(),
    )
    .expect("replacement length");
    assert_eq!(projection_state, (replacement_len, 3));
}

#[tokio::test]
async fn paginated_child_projection_contains_only_child_local_rows() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let config = test_config(home.path());
    let sqlite = config.sqlite.clone();
    let parent_id = ThreadId::default();
    store
        .create_thread(create_params(parent_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create parent");
    store
        .persist_thread(parent_id)
        .await
        .expect("persist parent");
    append_turn(
        &store,
        parent_id,
        "parent-turn",
        "parent-item",
        "parent content",
    )
    .await;
    let frozen = store
        .freeze_thread_segment(parent_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze parent");

    let child_id = ThreadId::default();
    let mut child_params = create_params(child_id, ThreadHistoryMode::Paginated);
    child_params.forked_from_id = Some(parent_id);
    child_params.initial_rollout_ordinal = frozen
        .next_rollout_ordinal
        .expect("paginated continuation ordinal");
    store
        .create_thread(child_params)
        .await
        .expect("create child");
    store.persist_thread(child_id).await.expect("persist child");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![RolloutItem::RolloutReference(frozen.reference)],
        })
        .await
        .expect("append inherited reference");
    append_turn(
        &store,
        child_id,
        "child-turn",
        "child-item",
        "child content",
    )
    .await;

    let pool = codex_state::open_thread_history_db(&sqlite)
        .await
        .expect("open history db");
    let projected_turn_ids = sqlx::query_scalar::<_, String>(
        "SELECT turn_id FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(child_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read child turns");
    let projected_item_ids = sqlx::query_scalar::<_, String>(
        "SELECT item_id FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(child_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read child items");
    assert_eq!(projected_turn_ids, vec!["child-turn"]);
    assert_eq!(projected_item_ids, vec!["child-item"]);
}

#[tokio::test]
async fn missing_immutable_segment_fails_strict_history_read() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "before freeze").await;
    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze segment");
    tokio::fs::remove_file(frozen.reference.rollout_path)
        .await
        .expect("remove immutable segment");

    let err = store
        .load_history(crate::LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect_err("missing reference must fail");
    assert!(
        err.to_string().contains("could not be resolved"),
        "unexpected error: {err}"
    );
}

async fn assert_segmentless_source_freezes(history_mode: ThreadHistoryMode) {
    let home = TempDir::new().expect("temp dir");
    let store = if history_mode == ThreadHistoryMode::Paginated {
        state_backed_store(home.path()).await
    } else {
        LocalThreadStore::new(test_config(home.path()), /*state_db*/ None)
    };
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, history_mode))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "legacy segment prefix").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown thread");
    remove_segment_id(stable_path.as_path()).await;
    let source_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read segmentless source");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze segmentless source");
    assert_eq!(frozen.source_session_meta.meta.segment_id, None);
    assert_eq!(frozen.reference.segment_id, None);
    assert_eq!(
        frozen
            .reference
            .rollout_path
            .parent()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str()),
        Some("initial")
    );
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable segment"),
        source_bytes
    );
    let replacement_meta = codex_rollout::read_session_meta_line(stable_path.as_path())
        .await
        .expect("read replacement metadata");
    assert!(replacement_meta.meta.segment_id.is_some());
    assert_eq!(replacement_meta.meta.history_mode, history_mode);
}

async fn state_backed_store(codex_home: &Path) -> LocalThreadStore {
    let config = test_config(codex_home);
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state db");
    LocalThreadStore::new(config, Some(state_db))
}

async fn stored_parent_and_children_bytes(
    history_mode: ThreadHistoryMode,
    parent_message_count: usize,
) -> (u64, u64) {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent_id = ThreadId::default();
    store
        .create_thread(create_params(parent_id, history_mode))
        .await
        .expect("create parent");
    store
        .persist_thread(parent_id)
        .await
        .expect("persist parent");
    for index in 0..parent_message_count {
        let turn_id = format!("parent-turn-{index}");
        let item_id = format!("parent-item-{index}");
        let content = format!("parent-{index}-{}", "x".repeat(1_024));
        append_turn(
            &store,
            parent_id,
            turn_id.as_str(),
            item_id.as_str(),
            content.as_str(),
        )
        .await;
    }
    let frozen = store
        .freeze_thread_segment(parent_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze parent");
    let parent_bytes = tokio::fs::metadata(frozen.reference.rollout_path.as_path())
        .await
        .expect("read frozen parent metadata")
        .len();

    let mut children_bytes = 0;
    for _ in 0..3 {
        let child_id = ThreadId::default();
        let mut child_params = create_params(child_id, history_mode);
        child_params.forked_from_id = Some(parent_id);
        child_params.initial_rollout_ordinal = frozen.next_rollout_ordinal.unwrap_or_default();
        store
            .create_thread(child_params)
            .await
            .expect("create child");
        store.persist_thread(child_id).await.expect("persist child");
        store
            .append_items(AppendThreadItemsParams {
                thread_id: child_id,
                items: vec![RolloutItem::RolloutReference(frozen.reference.clone())],
            })
            .await
            .expect("append child reference");
        store.flush_thread(child_id).await.expect("flush child");
        let child_path = store
            .live_rollout_path(child_id)
            .await
            .expect("child rollout path");
        let child_items = RolloutRecorder::load_rollout_items(child_path.as_path())
            .await
            .expect("read child rollout")
            .0;
        assert!(matches!(
            child_items.as_slice(),
            [
                RolloutItem::SessionMeta(_),
                RolloutItem::RolloutReference(_)
            ]
        ));
        children_bytes += tokio::fs::metadata(child_path)
            .await
            .expect("read child metadata")
            .len();
    }

    (parent_bytes, children_bytes)
}

async fn remove_segment_id(path: &std::path::Path) {
    let contents = tokio::fs::read_to_string(path).await.expect("read rollout");
    let mut lines = contents.lines();
    let first_line = lines.next().expect("session metadata line");
    let mut first_value =
        serde_json::from_str::<serde_json::Value>(first_line).expect("parse session metadata line");
    first_value
        .get_mut("payload")
        .and_then(serde_json::Value::as_object_mut)
        .expect("session metadata payload")
        .remove("segment_id")
        .expect("segment id");
    let mut rewritten = serde_json::to_string(&first_value).expect("serialize session metadata");
    rewritten.push('\n');
    for line in lines {
        rewritten.push_str(line);
        rewritten.push('\n');
    }
    tokio::fs::write(path, rewritten)
        .await
        .expect("write segmentless rollout");
}

async fn append_message(store: &LocalThreadStore, thread_id: ThreadId, message: &str) {
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::EventMsg(EventMsg::UserMessage(
                UserMessageEvent {
                    message: message.to_string(),
                    images: None,
                    local_images: Vec::new(),
                    text_elements: Vec::new(),
                    ..Default::default()
                },
            ))],
        })
        .await
        .expect("append message");
}

fn user_message_item(message: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: message.to_string(),
        images: None,
        local_images: Vec::new(),
        text_elements: Vec::new(),
        ..Default::default()
    }))
}

async fn append_turn(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    content: &str,
) {
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: turn_id.to_string(),
                    trace_id: None,
                    started_at: Some(10),
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                })),
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    thread_id,
                    turn_id: turn_id.to_string(),
                    item: TurnItem::UserMessage(UserMessageItem {
                        id: item_id.to_string(),
                        client_id: None,
                        content: vec![UserInput::Text {
                            text: content.to_string(),
                            text_elements: Vec::new(),
                        }],
                    }),
                    started_at_ms: None,
                    completed_at_ms: 1,
                })),
                RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: turn_id.to_string(),
                    last_agent_message: None,
                    error: None,
                    started_at: Some(10),
                    completed_at: Some(20),
                    duration_ms: Some(10_000),
                    time_to_first_token_ms: None,
                })),
            ],
        })
        .await
        .expect("append turn");
}

fn has_message(items: &[RolloutItem], message: &str) -> bool {
    items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == message
        )
    })
}

fn create_params(thread_id: ThreadId, history_mode: ThreadHistoryMode) -> CreateThreadParams {
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
        history_mode,
        history_base: None,
        subagent_history_start_ordinal: None,
        persistence_mode: crate::ThreadPersistenceMode::Durable,
        initial_rollout_ordinal: 0,
        initial_window_id: uuid::Uuid::now_v7().to_string(),
        metadata: ThreadPersistenceMetadata {
            cwd: Some(std::env::current_dir().expect("cwd")),
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}
