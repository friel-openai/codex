use std::fs;
use std::io::Write;
use std::path::Path;

use chrono::Utc;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::RolloutLineageSegment;
use super::compressed_history_position_matches;
use crate::ForkBoundary;
use crate::LoadThreadHistoryParams;
use crate::PrepareForkParams;
use crate::ThreadStore;
use super::resolve_path;

#[cfg(unix)]
#[tokio::test]
async fn rejects_reference_lineage_escaping_symlinked_sessions_root() {
    let home = TempDir::new().expect("temp dir");
    let external = TempDir::new().expect("external temp dir");
    let external_sessions = external.path().join("sessions");
    fs::create_dir_all(external_sessions.as_path()).expect("create external sessions");
    std::os::unix::fs::symlink(external_sessions.as_path(), home.path().join("sessions"))
        .expect("symlink sessions");
    let escaped = TempDir::new().expect("escaped temp dir");
    let escaped_rollouts = escaped.path().join("rollouts");
    fs::create_dir_all(escaped_rollouts.as_path()).expect("create escaped rollouts");
    std::os::unix::fs::symlink(escaped_rollouts.as_path(), external_sessions.join("2026"))
        .expect("symlink nested sessions directory");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    write_rollout(
        home.path(),
        thread_id,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );

    let error = match store
        .resolve_rollout_lineage_for_reference(thread_id, None)
        .await
    {
        Ok(_) => panic!("escaping reference lineage should be rejected"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("must be in Codex home"));
}

#[tokio::test]
async fn resolves_nested_lineage_with_empty_intermediate_segments() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let middle = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let root_end = history_position(root_path.as_path(), root, /*end_ordinal_exclusive*/ 4);
    let middle_path = write_rollout(home.path(), middle, Some(root_end), /*next_ordinal*/ 1);
    let middle_end = history_position(
        middle_path.as_path(),
        middle,
        /*end_ordinal_exclusive*/ 5,
    );
    let child_path = write_rollout(
        home.path(),
        child,
        Some(middle_end),
        /*next_ordinal*/ 3,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve nested lineage");
    let child_len = fs::metadata(&child_path).expect("child metadata").len();

    assert_eq!(
        lineage.segments,
        vec![
            RolloutLineageSegment {
                thread_id: root,
                rollout_id: root,
                segment_id: None,
                rollout_path: root_path.clone(),
                start_ordinal: 1,
                physical_start_ordinal: 1,
                history_page_visible: true,
                history_page_start: None,
                end_ordinal_exclusive: Some(root_end.end_ordinal_exclusive),
                jsonl_end_byte_offset: Some(root_end.end_byte_offset),
                end_byte_offset: Some(root_end.end_byte_offset),
                byte_boundary_recorded: true,
                physical_size: Some(fs::metadata(&root_path).unwrap().len()),
                filter_texts: Vec::new(),
            },
            RolloutLineageSegment {
                thread_id: middle,
                rollout_id: middle,
                segment_id: None,
                rollout_path: middle_path.clone(),
                start_ordinal: 5,
                physical_start_ordinal: 5,
                history_page_visible: true,
                history_page_start: None,
                end_ordinal_exclusive: Some(middle_end.end_ordinal_exclusive),
                jsonl_end_byte_offset: Some(middle_end.end_byte_offset),
                end_byte_offset: Some(middle_end.end_byte_offset),
                byte_boundary_recorded: true,
                physical_size: Some(fs::metadata(&middle_path).unwrap().len()),
                filter_texts: Vec::new(),
            },
            RolloutLineageSegment {
                thread_id: child,
                rollout_id: child,
                segment_id: None,
                rollout_path: child_path,
                start_ordinal: 6,
                physical_start_ordinal: 6,
                history_page_visible: true,
                history_page_start: None,
                end_ordinal_exclusive: None,
                jsonl_end_byte_offset: Some(child_len),
                end_byte_offset: Some(child_len),
                byte_boundary_recorded: false,
                physical_size: Some(child_len),
                filter_texts: Vec::new(),
            },
        ]
    );
}

#[tokio::test]
async fn copied_fork_reads_compressed_ordinal_only_ancestor_at_selected_cutoff() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let root = ThreadId::new();
    let middle = ThreadId::new();
    let root_path = write_rollout(home.path(), root, None, 3);
    let root_end = history_position(&root_path, root, 3);
    let middle_path = write_rollout(home.path(), middle, Some(root_end), 4);
    let original = fs::read_to_string(&middle_path).unwrap();
    let (header, remainder) = original.split_once('\n').unwrap();
    let unordinaled = serde_json::to_string(&RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(), ordinal: None,
        item: RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
    }).unwrap();
    let contents = format!("{header}\n{unordinaled}\n{remainder}");
    let compressed = middle_path.with_extension("jsonl.zst");
    fs::write(&compressed, zstd::encode_all(contents.as_bytes(), 0).unwrap()).unwrap();
    fs::remove_file(&middle_path).unwrap();
    for (reference_ordinal, expected_count) in [(6, 5), (7, 6)] {
        let child = ThreadId::new();
        let child_path = write_referenced_rollout(home.path(), child, middle, compressed.clone(), reference_ordinal, reference_ordinal + 2);
        let end = HistoryPosition { thread_id: child, end_ordinal_exclusive: reference_ordinal + 2,
            end_byte_offset: fs::metadata(child_path).unwrap().len() };
        let lineage = store.resolve_rollout_lineage(child).await.expect("resolve compressed lineage");
        assert!(lineage.segments.iter().any(|segment| segment.thread_id == middle && segment.end_byte_offset.is_none()));
        let items = super::super::model_context::load_full_for_fork(lineage, Some(end)).await.expect("load selected prefix");
        assert_eq!(items.iter().filter(|item| matches!(item, RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete))).count(), expected_count);
    }
}

#[tokio::test]
async fn copied_fork_keeps_leading_unordinaled_records_in_nonzero_segment() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let root = ThreadId::new();
    let child = ThreadId::new();
    let root_path = write_rollout(home.path(), root, None, 3);
    let root_end = history_position(&root_path, root, 3);
    let child_path = write_rollout(home.path(), child, Some(root_end), 3);
    let original = fs::read_to_string(&child_path).unwrap();
    let (header, remainder) = original.split_once('\n').expect("session header");
    let unordinaled = serde_json::to_string(&RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(), ordinal: None,
        item: RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
    }).unwrap();
    fs::write(&child_path, format!("{header}\n{unordinaled}\n{remainder}")).unwrap();
    let end = HistoryPosition { thread_id: child, end_ordinal_exclusive: root_end.end_ordinal_exclusive + 3,
        end_byte_offset: fs::metadata(&child_path).unwrap().len() };
    let lineage = store.resolve_rollout_lineage(child).await.expect("resolve lineage");
    let items = super::super::model_context::load_full_for_fork(lineage, Some(end)).await.expect("load bounded history");
    assert_eq!(items.iter().filter(|item| matches!(item, RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete))).count(), 5);
}

#[tokio::test]
async fn copied_fork_respects_trimmed_start_and_unordinaled_byte_cutoff() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let root = ThreadId::new();
    let child = ThreadId::new();
    let root_path = write_rollout(home.path(), root, None, 3);
    let root_end = history_position(&root_path, root, 3);
    let child_path = write_rollout(home.path(), child, Some(root_end), 4);
    let original = fs::read_to_string(&child_path).unwrap();
    let lines = original.lines().collect::<Vec<_>>();
    let unordinaled = serde_json::to_string(&RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(),
        ordinal: None,
        item: RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
    }).unwrap();
    // Records before ordinal 5 are outside the selected start. The last unordinaled
    // record is included only when the recorded byte boundary includes it.
    let prefix = format!("{}\n{}\n{}\n{}\n{}\n", lines[0], unordinaled, lines[1], unordinaled, lines[2]);
    let with_unordinaled = format!("{prefix}{unordinaled}\n");
    fs::write(&child_path, format!("{with_unordinaled}{}\n", lines[3])).unwrap();

    for (offset, expected_count) in [(prefix.len(), 3), (with_unordinaled.len(), 4)] {
        let mut lineage = store.resolve_rollout_lineage(child).await.expect("resolve lineage");
        let segment = lineage.segments.last_mut().expect("child segment");
        assert_eq!(segment.physical_start_ordinal, 4);
        segment.start_ordinal = 5;
        let end = HistoryPosition { thread_id: child, end_ordinal_exclusive: 6, end_byte_offset: offset as u64 };
        let items = super::super::model_context::load_full_for_fork(lineage, Some(end)).await.expect("load bounded history");
        assert_eq!(items.iter().filter(|item| matches!(item, RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete))).count(), expected_count);
    }
}

#[tokio::test]
async fn history_page_ignores_changes_to_a_hidden_parent() {
    use std::io::Write;
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let parent = ThreadId::new();
    let child = ThreadId::new();
    let parent_path = write_rollout(home.path(), parent, None, 3);
    let child_path = write_rollout(home.path(), child, None, 3);
    let mut hidden = expected_segment(parent, parent_path.clone(), 1, None);
    hidden.history_page_visible = false;
    let visible = expected_segment(child, child_path.clone(), 1, None);
    let lineage = super::RolloutLineage::from_segments(child, child, child_path, vec![hidden, visible]);
    let mut file = fs::OpenOptions::new().append(true).open(parent_path).unwrap();
    writeln!(file, "{}", rollout_line(3, RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete))).unwrap();
    let page = lineage.for_history_page(&store).await.expect("hidden parent is not read");
    assert_eq!(page.segments.len(), 1);
    assert_eq!(page.segments[0].thread_id, child);
}

#[tokio::test]
async fn history_page_rejects_leaf_append_after_lineage_resolution() {
    use std::io::Write;
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, None, 3);
    let lineage = store.resolve_rollout_lineage(thread_id).await.expect("resolve lineage");
    let mut file = fs::OpenOptions::new().append(true).open(path).expect("open rollout");
    writeln!(file, "{}", rollout_line(3, RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete))).expect("append rollout");
    let error = lineage.for_history_page(&store).await.expect_err("new records must not enter the frozen lineage");
    assert!(error.to_string().contains("lineage changed"));
}

#[tokio::test]
async fn compressed_position_checks_byte_and_ordinal_boundaries() {
    let home = TempDir::new().expect("temp dir");
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, None, 4);
    let lines = fs::read(&path).expect("read rollout");
    let good = history_position(&path, thread_id, 2);
    let wrong = history_position(&path, thread_id, 1);
    let zst = path.with_extension("jsonl.zst");
    fs::write(&zst, zstd::encode_all(lines.as_slice(), 0).expect("compress rollout")).expect("write compressed rollout");
    let check = |position| {
        let reader = codex_rollout::open_rollout_seekable_reader_with_exact_path(&zst, || false, true).expect("open exact compressed copy");
        compressed_history_position_matches(reader, position).expect("check position")
    };
    assert!(check(good));
    assert!(!check(HistoryPosition { end_byte_offset: wrong.end_byte_offset, ..good }));
    assert!(!check(HistoryPosition { end_byte_offset: lines.len() as u64, ..good }));
    assert!(!check(HistoryPosition { end_byte_offset: lines.len() as u64 + 1, ..good }));
    let plain = String::from_utf8(lines).expect("UTF-8 rollout");
    let boundary = good.end_byte_offset as usize;
    let unordinaled = serde_json::to_string(&RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(), ordinal: None,
        item: RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
    }).expect("serialize legacy record") + "\n";
    let amended = format!("{}{}{}", &plain[..boundary], unordinaled, &plain[boundary..]);
    fs::write(&zst, zstd::encode_all(amended.as_bytes(), 0).expect("compress amended rollout")).expect("write compressed rollout");
    assert!(check(good));
}

#[tokio::test]
async fn resolves_archived_ancestors() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout_under(
        home.path().join("archived_sessions"),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    write_rollout(
        home.path(),
        child,
        Some(history_position(
            root_path.as_path(),
            root,
            /*end_ordinal_exclusive*/ 3,
        )),
        /*next_ordinal*/ 2,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve archived ancestor");

    assert_eq!(lineage.segments[0].rollout_path, root_path);
}

#[tokio::test]
async fn truncated_lineage_shares_retry_failure_and_progress() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let root = ThreadId::new();
    let child = ThreadId::new();
    let root_path = write_rollout(home.path(), root, None, 3);
    let root_end = history_position(&root_path, root, 3);
    let child_path = write_rollout(home.path(), child, Some(root_end), 2);
    let child_end = HistoryPosition {
        thread_id: child,
        end_ordinal_exclusive: 5,
        end_byte_offset: fs::metadata(child_path).unwrap().len(),
    };
    let lineage = store.resolve_rollout_lineage(child).await.expect("resolve lineage");
    let truncated = lineage.clone().truncate_at(child_end).await.expect("truncate lineage");
    truncated.reads.record_failure(&root_path);
    let error = || crate::ThreadStoreError::Internal { message: "synthetic ancestor read failure".to_string() };
    assert!(lineage.retry_exclusion(error()).is_some());
    truncated.reads.prevent_retry();
    assert!(lineage.retry_exclusion(error()).is_none());
}

#[tokio::test]
async fn successful_ancestor_projection_prevents_a_later_retry() {
    let temp = TempDir::new().expect("temp dir");
    let home = temp.path().canonicalize().expect("canonical home");
    let store = store_with_state(&home).await;
    let root = ThreadId::new();
    let child = ThreadId::new();
    let root_path = write_rollout(&home, root, None, 4);
    let root_end = history_position(&root_path, root, 3);
    write_rollout(&home, child, Some(root_end), 2);
    let lineage = store.resolve_rollout_lineage(child).await.expect("resolve lineage");
    super::super::thread_history_materialization::materialize_to_sqlite_with_reads(
        &store, root, &root_path, &lineage.reads,
    ).await.expect("project ancestor");
    lineage.reads.record_failure(&root_path);
    let error = crate::ThreadStoreError::Internal { message: "later read failure".to_string() };
    assert!(lineage.retry_exclusion(error).is_none());
}

#[tokio::test]
async fn history_repair_rejects_unbound_distinct_path_retry() {
    let temp = TempDir::new().expect("temp dir");
    let home = temp.path().canonicalize().expect("canonical home");
    let store = store_with_state(&home).await;
    let root = ThreadId::new();
    let child = ThreadId::new();
    let active = write_rollout(&home, root, None, 4);
    let child_path = write_rollout(&home, child, Some(history_position(&active, root, 3)), 2);
    let archived = home.join("archived_sessions").join(active.file_name().unwrap());
    fs::create_dir_all(archived.parent().unwrap()).unwrap();
    fs::write(&archived, b"different archived bytes").unwrap();
    let before_active = fs::read(&active).unwrap();
    let before_archived = fs::read(&archived).unwrap();
    let lineage = store.resolve_rollout_lineage(child).await.expect("resolve original");
    lineage.reads.record_failure(&active);
    let exclusion = lineage.retry_exclusion(crate::ThreadStoreError::Internal { message: "synthetic failure".to_string() }).expect("tracked ancestor");
    let result = super::super::goal_supervisor_runtime_repair::repair_compatibility_history_before_retry(
        &store, child, &child_path, &lineage, &[exclusion],
    ).await;
    assert!(matches!(result, Err(crate::ThreadStoreError::Conflict { .. })));
    assert_eq!(fs::read(&active).unwrap(), before_active);
    assert_eq!(fs::read(&archived).unwrap(), before_archived);
}

#[cfg(unix)]
#[tokio::test]
async fn fork_retry_rejects_same_path_source_replacement() {
    let temp = TempDir::new().expect("temp dir");
    let home = temp.path().canonicalize().expect("canonical home");
    let store = store_with_state(&home).await;
    let thread_id = ThreadId::new();
    let source = write_rollout(&home, thread_id, None, 3);
    let (original, reservation, _) = store
        .resolve_rollout_lineage_for_reference(thread_id, None)
        .await.expect("reserve source");
    drop(reservation);
    let replacement = source.with_extension("replacement");
    fs::copy(&source, &replacement).expect("copy source");
    fs::rename(&replacement, &source).expect("replace source at the same path");
    let result = store.resolve_rollout_lineage_for_reference_retry(thread_id, None, &[], Some(&original)).await;
    assert!(matches!(result, Err(crate::ThreadStoreError::Conflict { .. })));
}

#[tokio::test]
async fn fork_retry_source_stamp_hashes_same_length_rewrites() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("source.jsonl");
    fs::write(&path, b"first source").unwrap();
    let before = super::RetrySourceStamp::read(&path).await.unwrap();
    fs::write(&path, b"other source").unwrap();
    let after = super::RetrySourceStamp::read(&path).await.unwrap();
    assert_eq!(before.len, after.len);
    assert_ne!(before.sha256, after.sha256);
}

#[tokio::test]
async fn fork_reservation_pins_materialized_compressed_source() {
    let temp = TempDir::new().expect("temp dir");
    let home = temp.path().canonicalize().expect("canonical home");
    let store = store_with_state(&home).await;
    let thread_id = ThreadId::new();
    let source = write_rollout(&home, thread_id, None, 3);
    let compressed = source.with_extension("jsonl.zst");
    fs::write(&compressed, zstd::encode_all(fs::read(&source).unwrap().as_slice(), 0).unwrap()).unwrap();
    fs::remove_file(&source).unwrap();
    let (original, reservation, _) = store.resolve_rollout_lineage_for_reference(thread_id, None)
        .await.expect("materialize compressed source");
    assert_eq!(original.leaf_path, source);
    drop(reservation);
    store.resolve_rollout_lineage_for_reference_retry(thread_id, None, &[], Some(&original))
        .await.expect("reacquire materialized source");
}

#[tokio::test]
async fn fallback_rejects_an_existing_unattributed_projection() {
    let temp = TempDir::new().expect("temp dir");
    let home = temp.path().canonicalize().expect("canonical home");
    let store = store_with_state(&home).await;
    let root = ThreadId::new();
    let child = ThreadId::new();
    let active = write_rollout(&home, root, None, 5);
    let end = history_position(&active, root, 3);
    write_rollout(&home, child, Some(end), 2);
    let archived = home.join("archived_sessions").join(active.file_name().unwrap());
    fs::create_dir_all(archived.parent().unwrap()).unwrap();
    fs::copy(&active, &archived).unwrap();
    super::super::thread_history_materialization::materialize_to_sqlite(&store, root, &active)
        .await.expect("project original ancestor");
    let original = fs::read_to_string(&active).unwrap();
    let header = original.split_inclusive('\n').next().unwrap();
    fs::write(&active, header).unwrap();
    let error = store.prepare_fork(PrepareForkParams { thread_id: child, boundary: ForkBoundary::Latest })
        .await.expect_err("fallback must not resume an unattributed SQLite prefix");
    assert!(matches!(error, crate::ThreadStoreError::Conflict { message } if message.contains("alternate fork lineage") || message.contains("verified prefix equality")));
}

#[tokio::test]
async fn unreadable_ancestor_recovers_from_another_copy_of_the_same_rollout() {
    let temp = TempDir::new().expect("temp dir");
    let home = temp.path().canonicalize().expect("canonical home");
    let store = store_with_state(&home).await;
    let owner = ThreadId::new();
    let root = ThreadId::new();
    let child = ThreadId::new();
    let original = write_rollout(
        &home, owner, /*history_base*/ None, /*next_ordinal*/ 5,
    );
    // Reverted rollout filenames contain the owner's stable thread ID and a distinct rollout ID.
    let name = format!("rollout-2026-07-16T00-00-00-{owner}_{root}.jsonl");
    let active = original.with_file_name(&name);
    fs::rename(original, &active).expect("name reverted rollout");
    let end = history_position(&active, root, /*end_ordinal_exclusive*/ 3);
    let child_path = write_rollout(&home, child, Some(end), /*next_ordinal*/ 2);
    let archived_dir = home.join("archived_sessions");
    fs::create_dir_all(&archived_dir).expect("create archive");
    let archived = archived_dir.join(name);
    let contents = fs::read_to_string(&active).expect("read active rollout");
    let load_context = || {
        store.load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child,
            include_archived: false,
        })
    };
    let expected = load_context().await.expect("load intact context").items;
    let expected = serde_json::to_value(expected).expect("serialize context");
    let compressed = active.with_extension("jsonl.zst");
    let header = contents
        .split_inclusive('\n')
        .next()
        .expect("session header");
    write_unfinished_frame(&compressed, header);
    fs::remove_file(&active).expect("remove plain rollout");

    let assert_original_error = async || {
        let err = load_context().await.expect_err("no usable copy");
        assert!(
            err.to_string()
                .ends_with(&format!("{}: incomplete frame", compressed.display())),
            "{err}"
        );
    };
    assert_original_error().await;
    // History pages read projected rows and only need the ancestor header for its bounds.
    let page = store
        .resolve_rollout_lineage_for_history_page(child)
        .await
        .expect("page with only the short active copy");
    assert_eq!(page.segments[0].rollout_path, compressed);
    let child_contents = fs::read_to_string(&child_path).expect("read child");
    let checkpoint = serde_json::json!({
        "timestamp": "2026-07-16T00:00:00.000Z", "ordinal": 5, "type": "compacted",
        "payload": {"message": "child checkpoint", "replacement_history": [], "window_number": 1}
    });
    fs::write(&child_path, format!("{child_contents}{checkpoint}\n")).expect("append checkpoint");
    let context = load_context()
        .await
        .expect("resume at the newer checkpoint");
    assert!(matches!(
        context.items.as_slice(),
        [RolloutItem::SessionMeta(_), RolloutItem::Compacted(item)] if item.message == "child checkpoint"
    ));
    fs::write(&child_path, child_contents).expect("restore child");
    fs::write(
        &archived,
        contents.replace(&owner.to_string(), &root.to_string()),
    )
    .expect("stage another owner's history");
    assert_original_error().await;

    let archive = contents.split_inclusive('\n').take(4).collect::<String>();
    fs::write(&archived, archive).expect("restore matching archived copy");
    let context = load_context().await.expect("resume from archive").items;
    assert_eq!(
        serde_json::to_value(context).expect("serialize resumed context"),
        expected
    );
    let fork = store
        .prepare_fork(PrepareForkParams {
            thread_id: child,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect("fork from archive");
    assert!(matches!(&fork.history, crate::PreparedForkHistory::Copied {
        forked_from_ordinal_exclusive: Some(_), response_history, ..
    } if !response_history.is_empty()));
    assert_eq!(
        serde_json::to_value(fork.model_context.as_ref()).expect("serialize fork context"),
        expected
    );

    let assert_archive_projected = async |expected_offset| {
        let err = store
            .prepare_fork(PrepareForkParams {
                thread_id: child,
                boundary: ForkBoundary::ThroughTurn("missing".to_string()),
            })
            .await
            .expect_err("turn is missing after projection");
        assert!(
            matches!(&err, crate::ThreadStoreError::InvalidRequest { message } if message == "turn not found: missing"),
            "{err}"
        );
        let projection = super::super::thread_history::projection_state(&store, root)
            .await
            .expect("read checkpoint")
            .expect("ancestor projected");
        assert_eq!(projection.next_byte_offset, expected_offset);
    };
    assert_archive_projected(end.end_byte_offset).await;

    // The ancestor's own projection can advance beyond the child's inherited prefix.
    super::super::thread_history_materialization::materialize_to_sqlite(&store, root, &archived)
        .await
        .expect("project the rest of the archive");
    let projected = fs::metadata(&archived).expect("archive size").len();
    let prefix = &contents.as_bytes()[..end.end_byte_offset as usize];
    let compressed_prefix = zstd::encode_all(prefix, /*level*/ 0).expect("compress prefix");
    fs::write(&compressed, compressed_prefix).expect("replace active copy with a shorter stream");
    fs::write(&archived, prefix).expect("shorten archive to the inherited prefix");
    assert_archive_projected(projected).await;
}

async fn store_with_state(home: &Path) -> LocalThreadStore {
    let config = test_config(home);
    let state = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize sqlite");
    LocalThreadStore::new(config, Some(state))
}

fn write_unfinished_frame(path: &Path, prefix: &str) {
    let mut encoder = zstd::Encoder::new(Vec::new(), /*level*/ 0).expect("create encoder");
    encoder
        .set_pledged_src_size(Some(prefix.len() as u64))
        .expect("set frame size");
    encoder.write_all(prefix.as_bytes()).expect("write prefix");
    encoder.flush().expect("flush unfinished frame");
    fs::write(path, encoder.get_ref()).expect("write unfinished frame");
}

#[tokio::test]
async fn indexed_ancestor_must_be_managed_and_owned_by_the_indexed_thread() {
    let temp = TempDir::new().expect("temp dir");
    let home = temp.path().canonicalize().expect("canonical home");
    let external_home = TempDir::new().expect("external home");
    let store = store_with_state(&home).await;
    let state = store.state_db().await.expect("SQLite store");
    let root = ThreadId::new();
    let child = ThreadId::new();
    let active = write_rollout(
        &home, root, /*history_base*/ None, /*next_ordinal*/ 3,
    );
    let archived = write_rollout_under(
        home.join("archived_sessions"),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 4,
    );
    let external = write_rollout(
        external_home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    let end = history_position(&active, root, /*end_ordinal_exclusive*/ 3);
    write_rollout(&home, child, Some(end), /*next_ordinal*/ 2);
    let assert_lookup = async |indexed: &Path, expected: &Path| {
        let metadata = codex_state::ThreadMetadataBuilder::new(
            root,
            indexed.to_owned(),
            chrono::Utc::now(),
            codex_protocol::protocol::SessionSource::Cli,
        )
        .with_history_mode(ThreadHistoryMode::Paginated);
        state
            .upsert_thread(&metadata.build("test-provider"))
            .await
            .expect("index ancestor");
        let existing = store
            .resolve_rollout_lineage(child)
            .await
            .expect("resolve ancestor");
        let reference = store
            .resolve_rollout_lineage_for_reference(child, /*expected_rollout_id*/ None)
            .await
            .expect("resolve reference")
            .0;
        for lineage in [existing, reference] {
            assert_eq!(lineage.segments[0].rollout_path, expected);
        }
    };
    assert_lookup(&archived, &archived).await;
    assert_lookup(&external, &active).await;
    let contents = fs::read_to_string(&archived).expect("read archive");
    let (header, events) = contents.split_once('\n').expect("header and events");
    let legacy = header.replace("\"paginated\"", "\"legacy\"");
    fs::write(&archived, header).expect("shorten indexed archive");
    assert_lookup(&archived, &archived).await;
    fs::write(&archived, format!("{legacy}\n{events}")).expect("write legacy archive");
    assert_lookup(&archived, &active).await;
    let compressed = archived.with_extension("jsonl.zst");
    fs::write(
        &compressed,
        zstd::encode_all(contents.as_bytes(), /*level*/ 0).expect("compress archive"),
    )
    .expect("write complete compressed copy");
    fs::remove_file(&archived).expect("remove plain archive");
    assert_lookup(&compressed, &compressed).await;
    let load_context = || {
        store.load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child,
            include_archived: false,
        })
    };
    let items = async || {
        serde_json::to_value(load_context().await.expect("load context").items)
            .expect("serialize context")
    };
    let expected = items().await;
    for prefix in [contents.as_str(), &contents[..header.len() + 1]] {
        write_unfinished_frame(&compressed, prefix);
        assert_lookup(&compressed, &compressed).await;
        assert_eq!(items().await, expected);
        let fork = store
            .prepare_fork(PrepareForkParams {
                thread_id: child,
                boundary: ForkBoundary::Latest,
            })
            .await
            .expect("fork using the intact active copy");
        assert_eq!(
            serde_json::to_value(fork.model_context.as_ref()).expect("serialize fork"),
            expected
        );
        let page = store
            .resolve_rollout_lineage_for_history_page(child)
            .await
            .expect("resolve page");
        assert_eq!(page.segments[0].rollout_path, compressed);
    }
    let wrong_owner = contents.replace(&root.to_string(), &ThreadId::new().to_string());
    fs::write(&active, &wrong_owner).expect("substitute another thread's history");
    let error = load_context()
        .await
        .expect_err("reject wrong owner on retry");
    assert!(
        error
            .to_string()
            .ends_with(&format!("{}: incomplete frame", compressed.display())),
        "{error}"
    );
    fs::remove_file(&compressed).expect("remove truncated archive");
    fs::write(&archived, &contents).expect("restore archive");
    assert_lookup(&active, &archived).await;
    fs::write(&active, []).expect("empty indexed rollout");
    assert_lookup(&active, &archived).await;
    fs::remove_file(&archived).expect("remove fallback");
    assert_invalid_lineage(
        &store,
        child,
        &format!("rollout at {} is empty", active.display()),
    )
    .await;

    #[cfg(unix)]
    {
        fs::copy(&external, &active).expect("restore active rollout");
        fs::write(
            &compressed,
            zstd::encode_all(contents.as_bytes(), /*level*/ 0).expect("compress archive"),
        )
        .expect("restore compressed archive");
        std::os::unix::fs::symlink(&external, &archived).expect("link plain sibling outside home");
        let alias = home
            .join("sessions")
            .join(compressed.file_name().expect("filename"));
        std::os::unix::fs::symlink(&compressed, &alias).expect("link indexed compressed rollout");
        assert_lookup(&alias, &active).await;
    }
}

#[tokio::test]
async fn preserves_history_position_byte_boundary_for_compressed_ancestor() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let parent_end = history_position(
        parent_path.as_path(),
        parent,
        /*end_ordinal_exclusive*/ 4,
    );
    let compressed_parent = compress_rollout(parent_path.as_path());
    write_rollout(
        home.path(),
        child,
        Some(parent_end),
        /*next_ordinal*/ 2,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve compressed ancestor");
    let parent_segment = lineage.segments.first().expect("parent segment");

    assert_eq!(parent_segment.rollout_path(), compressed_parent);
    assert_eq!(parent_segment.end_byte_offset(), None);
    assert_eq!(
        parent_segment.jsonl_end_byte_offset(),
        Some(parent_end.end_byte_offset)
    );
}

#[tokio::test]
async fn unbounded_compressed_segment_has_no_decoded_byte_boundary() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    let rollout_path = write_rollout(
        home.path(),
        thread_id,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    let compressed_path = compress_rollout(rollout_path.as_path());

    let lineage = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve compressed rollout");
    let segment = lineage.segments.first().expect("root segment");

    assert_eq!(segment.rollout_path(), compressed_path);
    assert_eq!(segment.jsonl_end_byte_offset(), None);
}

#[tokio::test]
async fn nested_reference_filters_compose_oldest_first() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let oldest = ThreadId::new();
    let middle = ThreadId::new();
    let root = ThreadId::new();
    let oldest_path = write_rollout(
        home.path(),
        oldest,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    let middle_path = write_referenced_rollout(
        home.path(),
        middle,
        oldest,
        oldest_path,
        /*reference_ordinal*/ 3,
        /*next_ordinal*/ 5,
    );
    set_leading_reference_filters(
        middle_path.as_path(),
        vec!["inner-b".to_string(), "outer-a".to_string()],
    );
    let root_path = write_referenced_rollout(
        home.path(),
        root,
        middle,
        middle_path,
        /*reference_ordinal*/ 6,
        /*next_ordinal*/ 8,
    );
    set_leading_reference_filters(root_path.as_path(), vec!["outer-a".to_string()]);

    let lineage = store
        .resolve_rollout_lineage(root)
        .await
        .expect("resolve filtered nested lineage");
    assert_eq!(
        lineage
            .segments
            .iter()
            .map(|segment| (segment.thread_id, segment.filter_texts.clone()))
            .collect::<Vec<_>>(),
        vec![
            (oldest, vec!["outer-a".to_string(), "inner-b".to_string()]),
            (middle, vec!["outer-a".to_string()]),
            (root, Vec::new()),
        ]
    );
}

#[tokio::test]
async fn resolves_lineage_at_explicit_history_position() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let root_end = history_position(root_path.as_path(), root, /*end_ordinal_exclusive*/ 4);
    let child_path = write_rollout(home.path(), child, Some(root_end), /*next_ordinal*/ 4);
    let end = history_position(
        child_path.as_path(),
        child,
        /*end_ordinal_exclusive*/ 6,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve child lineage")
        .truncate_at(end)
        .await
        .expect("resolve explicit position");

    assert_eq!(
        lineage.segments,
        vec![
            RolloutLineageSegment {
                thread_id: root,
                rollout_id: root,
                segment_id: None,
                rollout_path: root_path.clone(),
                start_ordinal: 1,
                physical_start_ordinal: 1,
                history_page_visible: true,
                history_page_start: None,
                end_ordinal_exclusive: Some(root_end.end_ordinal_exclusive),
                jsonl_end_byte_offset: Some(root_end.end_byte_offset),
                end_byte_offset: Some(root_end.end_byte_offset),
                byte_boundary_recorded: true,
                physical_size: Some(fs::metadata(&root_path).unwrap().len()),
                filter_texts: Vec::new(),
            },
            RolloutLineageSegment {
                thread_id: child,
                rollout_id: child,
                segment_id: None,
                rollout_path: child_path.clone(),
                start_ordinal: 5,
                physical_start_ordinal: 5,
                history_page_visible: true,
                history_page_start: None,
                end_ordinal_exclusive: Some(end.end_ordinal_exclusive),
                jsonl_end_byte_offset: Some(end.end_byte_offset),
                end_byte_offset: Some(end.end_byte_offset),
                byte_boundary_recorded: true,
                physical_size: Some(fs::metadata(&child_path).unwrap().len()),
                filter_texts: Vec::new(),
            },
        ]
    );
}

#[tokio::test]
async fn fork_lineage_preserves_validated_unordinaled_ancestor_cutoff() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    let store = LocalThreadStore::new(config.clone(), Some(state_db.clone()));
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let mutable_parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    let segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent.to_string())
        .join(segment_id.to_string())
        .join(
            mutable_parent_path
                .file_name()
                .expect("parent rollout file name"),
        );
    let mut lines = fs::read_to_string(mutable_parent_path.as_path())
        .expect("read parent rollout")
        .lines()
        .map(codex_rollout::parse_rollout_line)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse parent rollout");
    let Some(RolloutItem::SessionMeta(session_meta)) = lines.first_mut().map(|line| &mut line.item)
    else {
        panic!("parent rollout must start with session metadata");
    };
    session_meta.meta.segment_id = Some(segment_id);
    let records = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .expect("serialize parent rollout");
    fs::create_dir_all(parent_path.parent().expect("parent segment directory"))
        .expect("create immutable parent directory");
    fs::write(parent_path.as_path(), format!("{}\n", records.join("\n")))
        .expect("write immutable parent rollout");
    let parent_end = history_position(
        parent_path.as_path(),
        parent,
        /*end_ordinal_exclusive*/ 2,
    );
    lines.insert(
        2,
        RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: None,
            item: RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        },
    );
    let records = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .expect("serialize parent rollout");
    fs::write(parent_path.as_path(), format!("{}\n", records.join("\n")))
        .expect("write parent rollout");
    fs::remove_file(mutable_parent_path.as_path()).expect("remove mutable parent rollout");
    let mut parent_metadata = codex_state::ThreadMetadataBuilder::new(
        parent,
        parent_path.clone(),
        Utc::now(),
        SessionSource::Cli,
    );
    parent_metadata.history_mode = ThreadHistoryMode::Paginated;
    state_db
        .upsert_thread(&parent_metadata.build(config.default_model_provider_id.as_str()))
        .await
        .expect("register immutable parent rollout");
    write_rollout(
        home.path(),
        child,
        Some(parent_end),
        /*next_ordinal*/ 2,
    );

    let expected = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve child lineage with explicit byte cutoff");
    assert_eq!(
        expected.segments[0].end_byte_offset,
        Some(parent_end.end_byte_offset),
    );

    let (prepared, _writer_reservation, _projection_was_missing) = store
        .resolve_rollout_lineage_for_reference(child, /*expected_rollout_id*/ None)
        .await
        .expect("prepare child lineage for a fork reference");

    assert_eq!(prepared.segments, expected.segments);
}

#[tokio::test]
async fn normalizes_rollout_references_and_same_thread_rotations() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 4,
    );
    let child_path = write_referenced_rollout(
        home.path(),
        child,
        parent,
        parent_path.clone(),
        /*reference_ordinal*/ 4,
        /*next_ordinal*/ 6,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve referenced child");
    assert_eq!(
        lineage.segments,
        vec![
            expected_segment(
                parent,
                parent_path.clone(),
                /*start_ordinal*/ 1,
                Some(history_position(
                    parent_path.as_path(),
                    parent,
                    /*end_ordinal_exclusive*/ 3,
                )),
            ),
            expected_segment(
                child, child_path, /*start_ordinal*/ 4, /*end*/ None,
            ),
        ]
    );

    let stable_path = write_referenced_rollout(
        home.path(),
        parent,
        parent,
        parent_path.clone(),
        /*reference_ordinal*/ 4,
        /*next_ordinal*/ 6,
    );
    let rotated = store.resolve_rollout_lineage_from_path(parent, &stable_path)
        .await.expect("resolve same-thread rotation");
    assert_eq!(rotated.segments.len(), 2);
    assert_eq!(rotated.segments[0].thread_id, parent);
    assert_eq!(rotated.segments[0].end_ordinal_exclusive, Some(3));
    assert_eq!(rotated.segments[1].thread_id, parent);
    assert_eq!(rotated.segments[1].start_ordinal, 4);
    assert_eq!(rotated.segments[1].rollout_path, stable_path);
}

#[tokio::test]
async fn history_base_cutoff_survives_parent_rotation() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let parent_end = history_position(
        parent_path.as_path(),
        parent,
        /*end_ordinal_exclusive*/ 4,
    );
    let immutable_directory = home
        .path()
        .join("rotated_rollout_segments")
        .join(parent.to_string())
        .join("initial");
    fs::create_dir_all(&immutable_directory).expect("create immutable parent directory");
    let immutable_path =
        immutable_directory.join(parent_path.file_name().expect("parent rollout file name"));
    fs::copy(parent_path.as_path(), immutable_path.as_path()).expect("copy immutable parent");
    write_referenced_rollout_at(
        parent_path.as_path(),
        parent,
        parent,
        immutable_path.clone(),
        /*reference_ordinal*/ 6,
        /*next_ordinal*/ 8,
    );
    let child_path = write_rollout(
        home.path(),
        child,
        Some(parent_end),
        /*next_ordinal*/ 3,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve child after parent rotation");

    assert_eq!(
        lineage.segments,
        vec![
            expected_segment(
                parent,
                immutable_path,
                /*start_ordinal*/ 1,
                Some(parent_end),
            ),
            expected_segment(
                child, child_path, /*start_ordinal*/ 5, /*end*/ None,
            ),
        ]
    );
}

fn expected_segment(
    thread_id: ThreadId,
    rollout_path: std::path::PathBuf,
    start_ordinal: u64,
    end: Option<HistoryPosition>,
) -> RolloutLineageSegment {
    RolloutLineageSegment {
        thread_id,
        rollout_id: thread_id,
        segment_id: None,
        end_ordinal_exclusive: end.map(|position| position.end_ordinal_exclusive),
        jsonl_end_byte_offset: end.map(|position| position.end_byte_offset).or_else(|| {
            fs::metadata(rollout_path.as_path())
                .ok()
                .map(|metadata| metadata.len())
        }),
        end_byte_offset: end.map(|position| position.end_byte_offset).or_else(|| {
            fs::metadata(rollout_path.as_path())
                .ok()
                .map(|metadata| metadata.len())
        }),
        byte_boundary_recorded: end.is_some(),
        physical_size: fs::metadata(&rollout_path).ok().map(|metadata| metadata.len()),
        rollout_path,
        start_ordinal,
        physical_start_ordinal: start_ordinal,
        history_page_visible: true,
        history_page_start: None,
        filter_texts: Vec::new(),
    }
}

#[tokio::test]
async fn rejects_missing_cycles_and_out_of_bounds_offsets() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let missing_parent = ThreadId::default();
    let missing_child = ThreadId::default();
    write_rollout(
        home.path(),
        missing_child,
        Some(unchecked_history_position(
            missing_parent,
            /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(&store, missing_child, "missing source rollout").await;

    let cycle_a = ThreadId::default();
    let cycle_b = ThreadId::default();
    write_rollout(
        home.path(),
        cycle_a,
        Some(unchecked_history_position(
            cycle_b, /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    write_rollout(
        home.path(),
        cycle_b,
        Some(unchecked_history_position(
            cycle_a, /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(&store, cycle_a, "cycle detected").await;

    let root = ThreadId::default();
    let invalid_child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    write_rollout(
        home.path(),
        invalid_child,
        Some(HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 2,
            end_byte_offset: fs::metadata(root_path).expect("root metadata").len() + 1,
        }),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(
        &store,
        invalid_child,
        "cutoff byte offset is past the source rollout",
    )
    .await;

    let mismatch_root = ThreadId::default();
    let mismatch_root_path = write_rollout(
        home.path(),
        mismatch_root,
        /*history_base*/ None,
        /*next_ordinal*/ 4,
    );
    let earlier_offset_child = ThreadId::default();
    write_rollout(
        home.path(),
        earlier_offset_child,
        Some(HistoryPosition {
            thread_id: mismatch_root,
            end_ordinal_exclusive: 3,
            end_byte_offset: rollout_end_byte_offset(
                mismatch_root_path.as_path(),
                /*end_ordinal_exclusive*/ 2,
            ),
        }),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(
        &store,
        earlier_offset_child,
        "cutoff byte offset does not match its ordinal boundary",
    )
    .await;

    let later_offset_child = ThreadId::default();
    write_rollout(
        home.path(),
        later_offset_child,
        Some(HistoryPosition {
            thread_id: mismatch_root,
            end_ordinal_exclusive: 2,
            end_byte_offset: rollout_end_byte_offset(
                mismatch_root_path.as_path(),
                /*end_ordinal_exclusive*/ 3,
            ),
        }),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(
        &store,
        later_offset_child,
        "cutoff byte offset does not match its ordinal boundary",
    )
    .await;
}

#[tokio::test]
async fn rejects_reference_graphs_past_the_global_depth_limit() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    let child_path = write_referenced_rollout(
        home.path(),
        child,
        parent,
        parent_path.clone(),
        /*reference_ordinal*/ 2,
        /*next_ordinal*/ 3,
    );

    let mut active_paths = std::collections::HashSet::new();
    resolve_path(
        &store,
        child,
        child,
        child_path.clone(),
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH - 1,
        super::LineageResolutionMode::ModelContext,
        &mut active_paths,
        &[],
        &mut Default::default(),
        &mut None,
    )
    .await
    .expect("the final permitted reference edge should resolve");

    let mut active_paths = std::collections::HashSet::new();
    let err = resolve_path(
        &store,
        child,
        child,
        child_path,
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        super::LineageResolutionMode::ModelContext,
        &mut active_paths,
        &[],
        &mut Default::default(),
        &mut None,
    )
    .await
    .expect_err("a reference edge beyond the global limit must fail");
    assert!(
        err.to_string().contains(&format!(
            "exceeds maximum depth of {}",
            codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
        )),
        "{err}"
    );

    let history_child = ThreadId::default();
    let history_child_path = write_rollout(
        home.path(),
        history_child,
        Some(history_position(
            parent_path.as_path(),
            parent,
            /*end_ordinal_exclusive*/ 2,
        )),
        /*next_ordinal*/ 3,
    );
    let mut active_paths = std::collections::HashSet::new();
    let err = resolve_path(
        &store,
        history_child,
        history_child,
        history_child_path,
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        super::LineageResolutionMode::ModelContext,
        &mut active_paths,
        &[],
        &mut Default::default(),
        &mut None,
    )
    .await
    .expect_err("a history_base edge beyond the global limit must fail");
    assert!(
        err.to_string().contains(&format!(
            "exceeds maximum depth of {}",
            codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
        )),
        "{err}"
    );
}

#[tokio::test]
async fn resolves_512_same_thread_lineage_segments() {
    const SEGMENT_COUNT: usize = 512;

    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    let segment_ids = (0..SEGMENT_COUNT)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let paths = (0..SEGMENT_COUNT)
        .map(|index| {
            home.path()
                .join("rotated_rollout_segments")
                .join(thread_id.to_string())
                .join(format!("lineage-segment-{index}"))
                .join(format!("rollout-2026-07-16T00-00-00-{thread_id}.jsonl"))
        })
        .collect::<Vec<_>>();

    for index in 0..SEGMENT_COUNT {
        let base_ordinal = u64::try_from(index).expect("fixture ordinal") * 3;
        let mut lines = vec![rollout_line(
            base_ordinal,
            RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    segment_id: Some(segment_ids[index]),
                    history_mode: ThreadHistoryMode::Paginated,
                    ..SessionMeta::default()
                },
                git: None,
            })),
        )];
        if let Some(previous_index) = index.checked_sub(1) {
            lines.push(rollout_line(
                base_ordinal + 1,
                RolloutItem::RolloutReference(RolloutReferenceItem {
                    rollout_id: Some(thread_id),
                    rollout_path: paths[previous_index].clone(),
                    thread_id: Some(thread_id),
                    rollout_timestamp: None,
                    segment_id: Some(segment_ids[previous_index]),
                    max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                    nth_user_message: None,
                    compacted_replacement_history_filter_texts: None,
                }),
            ));
        }
        lines.push(rollout_line(
            base_ordinal + 2,
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        ));
        fs::create_dir_all(paths[index].parent().expect("lineage segment directory"))
            .expect("create lineage segment directory");
        fs::write(paths[index].as_path(), format!("{}\n", lines.join("\n")))
            .expect("write segment fixture");
    }

    let mut active_paths = std::collections::HashSet::new();
    let segments = resolve_path(
        &store,
        thread_id,
        thread_id,
        paths.last().expect("latest segment").clone(),
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        /*graph_depth*/ 0,
        super::LineageResolutionMode::ModelContext,
        &mut active_paths,
        &[],
        &mut Default::default(),
        &mut None,
    )
    .await
    .expect("ordinary same-thread rotation must not exhaust fork depth");

    assert_eq!(segments.len(), SEGMENT_COUNT);
    assert!(active_paths.is_empty());

    let overflow_path = home
        .path()
        .join("rotated_rollout_segments")
        .join(thread_id.to_string())
        .join("lineage-segment-overflow")
        .join(format!("rollout-2026-07-16T00-00-00-{thread_id}.jsonl"));
    let overflow_ordinal = u64::try_from(SEGMENT_COUNT).expect("fixture ordinal") * 3;
    let overflow_lines = [
        rollout_line(
            overflow_ordinal,
            RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    segment_id: Some(SegmentId::new()),
                    history_mode: ThreadHistoryMode::Paginated,
                    ..SessionMeta::default()
                },
                git: None,
            })),
        ),
        rollout_line(
            overflow_ordinal + 1,
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(thread_id),
                rollout_path: paths[SEGMENT_COUNT - 1].clone(),
                thread_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: Some(segment_ids[SEGMENT_COUNT - 1]),
                max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
        ),
    ];
    fs::create_dir_all(
        overflow_path
            .parent()
            .expect("overflow lineage segment directory"),
    )
    .expect("create overflow lineage segment directory");
    fs::write(
        overflow_path.as_path(),
        format!("{}\n", overflow_lines.join("\n")),
    )
    .expect("write overflowing lineage fixture");
    let segments = resolve_path(
        &store,
        thread_id,
        thread_id,
        overflow_path,
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        /*graph_depth*/ 0,
        super::LineageResolutionMode::ModelContext,
        &mut active_paths,
        &[],
        &mut Default::default(),
        &mut None,
    )
    .await
    .expect("same-thread lineage must remain readable beyond 512 segments");
    assert_eq!(segments.len(), SEGMENT_COUNT + 1);
    assert!(active_paths.is_empty());
}

async fn assert_invalid_lineage(store: &LocalThreadStore, thread_id: ThreadId, detail: &str) {
    let err = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect_err("lineage should be invalid");
    assert!(err.to_string().contains(detail), "{err}");
}

fn write_rollout(
    home: &Path,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) -> std::path::PathBuf {
    write_rollout_under(
        home.join("sessions/2026/07/16"),
        thread_id,
        history_base,
        next_ordinal,
    )
}

fn write_rollout_under(
    directory: std::path::PathBuf,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) -> std::path::PathBuf {
    fs::create_dir_all(directory.as_path()).expect("create rollout directory");
    let path = directory.join(format!("rollout-2026-07-16T00-00-00-{thread_id}.jsonl"));
    let initial_ordinal = history_base.map_or(0, |base| base.end_ordinal_exclusive);
    let mut lines = vec![rollout_line(
        initial_ordinal,
        RolloutItem::SessionMeta(Box::new(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        })),
    )];
    for offset in 1..next_ordinal {
        let ordinal = initial_ordinal
            .checked_add(offset)
            .expect("fixture ordinal");
        lines.push(rollout_line(
            ordinal,
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        ));
    }
    fs::write(path.as_path(), format!("{}\n", lines.join("\n"))).expect("write rollout");
    path
}

fn write_referenced_rollout(
    home: &Path,
    thread_id: ThreadId,
    referenced_thread_id: ThreadId,
    referenced_path: std::path::PathBuf,
    reference_ordinal: u64,
    next_ordinal: u64,
) -> std::path::PathBuf {
    let directory = home.join("sessions/2026/07/16");
    fs::create_dir_all(directory.as_path()).expect("create rollout directory");
    let path = directory.join(format!("rollout-2026-07-17T00-00-00-{thread_id}.jsonl"));
    write_referenced_rollout_at(
        path.as_path(),
        thread_id,
        referenced_thread_id,
        referenced_path,
        reference_ordinal,
        next_ordinal,
    );
    path
}

fn write_referenced_rollout_at(
    path: &Path,
    thread_id: ThreadId,
    referenced_thread_id: ThreadId,
    referenced_path: std::path::PathBuf,
    reference_ordinal: u64,
    next_ordinal: u64,
) {
    let mut lines = vec![
        rollout_line(
            /*ordinal*/ 0,
            RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    history_mode: ThreadHistoryMode::Paginated,
                    ..SessionMeta::default()
                },
                git: None,
            })),
        ),
        rollout_line(
            reference_ordinal,
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(referenced_thread_id),
                rollout_path: referenced_path,
                thread_id: Some(referenced_thread_id),
                rollout_timestamp: None,
                segment_id: None,
                max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
        ),
    ];
    for ordinal in reference_ordinal + 1..next_ordinal {
        lines.push(rollout_line(
            ordinal,
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        ));
    }
    fs::write(path, format!("{}\n", lines.join("\n"))).expect("write rollout");
}

fn set_leading_reference_filters(path: &Path, filter_texts: Vec<String>) {
    let mut lines = fs::read_to_string(path)
        .expect("read referenced rollout")
        .lines()
        .map(codex_rollout::parse_rollout_line)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse referenced rollout");
    let reference = lines
        .iter_mut()
        .find_map(|line| match &mut line.item {
            RolloutItem::RolloutReference(reference) => Some(reference),
            _ => None,
        })
        .expect("leading reference");
    reference.compacted_replacement_history_filter_texts = Some(filter_texts);
    fs::write(
        path,
        format!(
            "{}\n",
            lines
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()
                .expect("serialize referenced rollout")
                .join("\n")
        ),
    )
    .expect("rewrite referenced rollout");
}

fn rollout_line(ordinal: u64, item: RolloutItem) -> String {
    serde_json::to_string(&RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(),
        ordinal: Some(ordinal),
        item,
    })
    .expect("serialize rollout line")
}

fn history_position(
    path: &Path,
    thread_id: ThreadId,
    end_ordinal_exclusive: u64,
) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: rollout_end_byte_offset(path, end_ordinal_exclusive),
    }
}

fn rollout_end_byte_offset(path: &Path, end_ordinal_exclusive: u64) -> u64 {
    let bytes = fs::read(path).expect("read rollout");
    let end_byte_offset = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .take_while(|line| {
            codex_rollout::parse_rollout_line_bytes(line)
                .expect("parse rollout fixture")
                .ordinal
                .expect("paginated rollout ordinal")
                < end_ordinal_exclusive
        })
        .map(<[u8]>::len)
        .sum::<usize>();
    u64::try_from(end_byte_offset).expect("rollout byte offset fits u64")
}

fn compress_rollout(path: &Path) -> std::path::PathBuf {
    let compressed_path = path.with_extension("jsonl.zst");
    let source = fs::read(path).expect("read rollout for compression");
    let compressed =
        zstd::stream::encode_all(source.as_slice(), /*level*/ 0).expect("compress rollout");
    fs::write(compressed_path.as_path(), compressed).expect("write compressed rollout");
    fs::remove_file(path).expect("remove plain rollout");
    compressed_path
}

fn unchecked_history_position(thread_id: ThreadId, end_ordinal_exclusive: u64) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: 0,
    }
}
