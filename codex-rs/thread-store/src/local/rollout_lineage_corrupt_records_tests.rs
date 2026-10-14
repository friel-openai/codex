use std::fs;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::RolloutLineage;
use super::RolloutLineageSegment;
use super::parse_rollout_bytes;
use crate::FreezeRolloutSegmentParams;

fn metadata(
    thread_id: ThreadId,
    ordinal: u64,
    history_base: Option<HistoryPosition>,
) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-09-01T00:00:00Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        }),
    }
}

fn message(ordinal: u64, text: &str) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-09-01T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            message: text.to_string(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        })),
    }
}

fn record(line: &RolloutLine) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(line).expect("serialize rollout record");
    bytes.push(b'\n');
    bytes
}

fn interrupted(line: &RolloutLine) -> Vec<u8> {
    let complete = record(line);
    let mut bytes = complete[..complete.len() - 5].to_vec();
    bytes.push(b'\n');
    bytes
}

fn rollout_path(home: &Path, rollout_id: ThreadId) -> PathBuf {
    home.join("sessions/2026/09/01")
        .join(format!("rollout-2026-09-01T00-00-00-{rollout_id}.jsonl"))
}

fn write(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().expect("rollout directory"))
        .expect("create rollout directory");
    fs::write(path, bytes).expect("write rollout");
}

fn segment(
    thread_id: ThreadId,
    rollout_id: ThreadId,
    path: &Path,
    start_ordinal: u64,
    end: Option<HistoryPosition>,
) -> RolloutLineageSegment {
    RolloutLineageSegment {
        thread_id,
        rollout_id,
        rollout_path: path.to_path_buf(),
        start_ordinal,
        end_ordinal_exclusive: end.map(|end| end.end_ordinal_exclusive),
        end_byte_offset: end.map(|end| end.end_byte_offset),
        jsonl_end_byte_offset: end.map(|end| end.end_byte_offset),
        filter_texts: Vec::new(),
        goal_supervisor_provenance: Default::default(),
        uses_history_base: false,
        uses_fork_boundary: false,
    }
}

async fn assert_native_snapshot_retains_source_and_ordinals(retry_is_complete: bool) {
    let home = TempDir::new().expect("temp dir");
    let thread_id = ThreadId::new();
    let path = rollout_path(home.path(), thread_id);
    let meta = metadata(thread_id, /*ordinal*/ 0, /*history_base*/ None);
    let first = message(/*ordinal*/ 1, "first complete local record");
    let retry = message(/*ordinal*/ 2, "interrupted ordinary record");
    let last = message(/*ordinal*/ 3, "later complete record");
    let mut bytes = record(&meta);
    bytes.extend_from_slice(&record(&first));
    bytes.extend_from_slice(&interrupted(&retry));
    let mut expected = vec![meta, first];
    if retry_is_complete {
        bytes.extend_from_slice(&record(&retry));
        expected.push(retry);
    }
    bytes.extend_from_slice(&record(&last));
    expected.push(last);
    write(&path, &bytes);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("snapshot accepts interior damage without modifying the source");
    let snapshot_id = frozen.reference.rollout_id.expect("snapshot physical ID");
    let snapshot_path = &frozen.reference.rollout_path;
    let base = frozen.history_base.expect("snapshot history_base");
    assert_ne!(snapshot_id, thread_id);
    assert_ne!(snapshot_path, &path);
    assert_eq!(base.thread_id, snapshot_id);
    assert_eq!(base.end_ordinal_exclusive, 4);
    assert_eq!(
        base.end_byte_offset,
        fs::metadata(snapshot_path)
            .expect("snapshot metadata")
            .len()
    );
    assert_eq!(frozen.next_rollout_ordinal, Some(4));
    assert_eq!(fs::read(&path).expect("source bytes after snapshot"), bytes);
    let (actual, owner, errors) = RolloutRecorder::load_rollout_lines(snapshot_path)
        .await
        .expect("read newly published immutable snapshot");
    assert_eq!(owner, Some(thread_id));
    assert_eq!(errors, 0);
    assert_eq!(
        serde_json::to_value(actual).expect("serialize snapshot envelopes"),
        serde_json::to_value(&expected).expect("serialize expected snapshot envelopes")
    );
    assert_eq!(
        codex_rollout::resolve_rollout_reference_path(home.path(), &frozen.reference)
            .await
            .expect("new physical reference resolves"),
        *snapshot_path
    );

    // Follow the new HistoryPosition through the real resolver, including a missing ordinary
    // ordinal. Native cutoffs use the retained ordinal and new file's bytes, not record count.
    let child_id = ThreadId::new();
    let child_path = rollout_path(home.path(), child_id);
    let mut child_bytes = record(&metadata(child_id, 4, Some(base)));
    child_bytes.extend_from_slice(&record(&message(5, "child local record")));
    write(&child_path, &child_bytes);
    let child_lineage = store
        .resolve_rollout_lineage(child_id)
        .await
        .expect("real resolver authenticates preserved snapshot ordinal cutoff");
    let inherited = child_lineage
        .segments
        .iter()
        .find(|segment| segment.rollout_id == snapshot_id)
        .expect("new immutable snapshot is in the resolved lineage");
    assert_eq!(inherited.end_ordinal_exclusive, Some(4));
    assert_eq!(inherited.end_byte_offset, Some(base.end_byte_offset));

    let reservation = store
        .reserve_rollout_writers(&[thread_id])
        .await
        .expect("reserve native source for a bounded prefix snapshot");
    let prefix_snapshot = super::super::segment::freeze_paginated_prefix_reserved(
        &store,
        thread_id,
        &path,
        thread_id,
        thread_id,
        &path,
        /*end_ordinal_exclusive*/ 4,
        bytes.len() as u64,
        &reservation,
    )
    .await
    .expect("bounded snapshot uses the same interior corruption policy");
    let prefix_base = prefix_snapshot
        .history_base
        .expect("prefix snapshot history_base");
    assert_ne!(prefix_base.thread_id, thread_id);
    assert_eq!(prefix_base.end_ordinal_exclusive, 4);
    assert_eq!(
        prefix_base.end_byte_offset,
        fs::metadata(&prefix_snapshot.reference.rollout_path)
            .expect("prefix snapshot metadata")
            .len()
    );
    let (prefix_lines, _, prefix_errors) =
        RolloutRecorder::load_rollout_lines(&prefix_snapshot.reference.rollout_path)
            .await
            .expect("read prefix snapshot");
    assert_eq!(prefix_errors, 0);
    assert_eq!(
        serde_json::to_value(prefix_lines).expect("serialize prefix snapshot envelopes"),
        serde_json::to_value(expected).expect("serialize expected prefix envelopes")
    );
    let error = super::super::segment::freeze_paginated_prefix_reserved(
        &store,
        thread_id,
        &path,
        thread_id,
        thread_id,
        &path,
        /*end_ordinal_exclusive*/ 5,
        bytes.len() as u64,
        &reservation,
    )
    .await
    .expect_err("accepted records cannot authenticate an invented prefix ordinal");
    assert!(error.to_string().contains("ended at ordinal 4, expected 5"));
    drop(reservation);

    let error = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect_err("rotation still rejects a damaged native source");
    assert!(error.to_string().contains("contains 1 invalid record(s)"));
    assert_eq!(
        fs::read(&path).expect("source bytes after rejected rotation"),
        bytes
    );
}

#[tokio::test]
async fn full_history_snapshot_keeps_native_retry_ordinals_and_source_bytes() {
    assert_native_snapshot_retains_source_and_ordinals(/*retry_is_complete*/ true).await;
}

#[tokio::test]
async fn full_history_snapshot_keeps_native_ordinal_gaps_and_new_cutoff() {
    assert_native_snapshot_retains_source_and_ordinals(/*retry_is_complete*/ false).await;
}

#[tokio::test]
async fn full_history_snapshot_keeps_damaged_first_local_record_structural() {
    let home = TempDir::new().expect("temp dir");
    let thread_id = ThreadId::new();
    let path = rollout_path(home.path(), thread_id);
    let first = message(1, "first local record");
    let mut bytes = record(&metadata(thread_id, 0, None));
    bytes.extend_from_slice(&interrupted(&first));
    bytes.extend_from_slice(&record(&first));
    write(&path, &bytes);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let error = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect_err("snapshot must authenticate a possibly leading lineage reference");
    assert!(
        error
            .to_string()
            .contains("failed to read paginated rollout line")
    );
    assert_eq!(fs::read(&path).expect("source remains unchanged"), bytes);
}

#[tokio::test]
async fn full_history_skips_interrupted_retry_without_changing_ordinals_or_offsets() {
    let home = TempDir::new().expect("temp dir");
    let thread_id = ThreadId::new();
    let path = rollout_path(home.path(), thread_id);
    let meta = metadata(thread_id, /*ordinal*/ 0, /*history_base*/ None);
    let first = message(/*ordinal*/ 1, "first complete local record");
    let retry = message(/*ordinal*/ 2, "complete retry");
    let last = message(/*ordinal*/ 3, "later complete record");
    let meta_bytes = record(&meta);
    let first_bytes = record(&first);
    let damaged = interrupted(&retry);
    let mut bytes = meta_bytes.clone();
    bytes.extend_from_slice(&first_bytes);
    bytes.extend_from_slice(&damaged);
    bytes.extend_from_slice(&record(&retry));
    let last_offset = bytes.len() as u64;
    bytes.extend_from_slice(&record(&last));
    write(&path, &bytes);

    let parsed = parse_rollout_bytes(&path, &bytes, thread_id).expect("load FullHistory bytes");
    let (file_lines, file_thread_id, errors) = RolloutRecorder::load_rollout_lines(&path)
        .await
        .expect("normal compatibility loading");

    assert_eq!(
        serde_json::to_value(parsed).expect("serialize parsed envelopes and offsets"),
        serde_json::to_value(vec![
            (0, meta.clone()),
            (meta_bytes.len() as u64, first.clone()),
            (
                (meta_bytes.len() + first_bytes.len() + damaged.len()) as u64,
                retry.clone()
            ),
            (last_offset, last.clone()),
        ])
        .expect("serialize expected envelopes and offsets")
    );
    assert_eq!(
        serde_json::to_value((file_lines, file_thread_id, errors))
            .expect("serialize file load result"),
        serde_json::to_value((
            vec![meta.clone(), first.clone(), retry.clone(), last.clone()],
            Some(thread_id),
            1
        ))
        .expect("serialize expected file load result")
    );

    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let reservation = store
        .reserve_rollout_writers(&[thread_id])
        .await
        .expect("reserve source");
    let lineage = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("real resolver admits damaged records after the trusted rollout head");
    let (lineage, _, full_history, _) = store
        .materialize_clean_fork_lineage(
            thread_id,
            lineage,
            &reservation,
            /*include_full_history*/ true,
        )
        .await
        .expect("prepare FullHistory source")
        .expect("clean fork preparation remains available");
    assert_eq!(
        serde_json::to_value(full_history).expect("serialize FullHistory items"),
        serde_json::to_value(Some(vec![meta.item, first.item, retry.item, last.item]))
            .expect("serialize expected FullHistory items")
    );
    assert_eq!(lineage.segments[0].end_ordinal_exclusive, Some(4));
    assert_eq!(
        lineage.segments[0].end_byte_offset,
        Some(bytes.len() as u64)
    );
    assert_eq!(fs::read(path).expect("source remains intact"), bytes);
}

#[tokio::test]
async fn full_history_resolver_keeps_a_damaged_first_local_record_structural() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    let path = rollout_path(home.path(), thread_id);
    let first = message(/*ordinal*/ 1, "complete first-local retry");
    let mut bytes = record(&metadata(
        thread_id, /*ordinal*/ 0, /*history_base*/ None,
    ));
    bytes.extend_from_slice(&interrupted(&first));
    bytes.extend_from_slice(&record(&first));
    write(&path, &bytes);

    // The first local record may be a leading lineage reference. Recovery begins only after
    // read_rollout_head has decoded that record without changing reference interpretation.
    let error = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect_err("the canonical rollout head must remain trustworthy");

    assert!(
        error
            .to_string()
            .contains("failed to read paginated rollout line")
    );
    assert_eq!(fs::read(path).expect("source remains intact"), bytes);
}

#[tokio::test]
async fn full_history_reads_damaged_rotated_ancestor_without_rewriting_history_base() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    let ancestor_id = ThreadId::new();
    let ancestor = home
        .path()
        .join("sessions/rollout_segments")
        .join(thread_id.to_string())
        .join(format!("rollout-2026-09-01T00-00-00-{ancestor_id}.jsonl"));
    let first = message(/*ordinal*/ 1, "immutable ancestor first");
    let retry = message(/*ordinal*/ 2, "immutable ancestor retry");
    let mut ancestor_bytes = record(&metadata(
        thread_id, /*ordinal*/ 0, /*history_base*/ None,
    ));
    ancestor_bytes.extend_from_slice(&record(&first));
    ancestor_bytes.extend_from_slice(&interrupted(&retry));
    ancestor_bytes.extend_from_slice(&record(&retry));
    write(&ancestor, &ancestor_bytes);
    let end = HistoryPosition {
        thread_id: ancestor_id,
        end_ordinal_exclusive: 3,
        end_byte_offset: ancestor_bytes.len() as u64,
    };
    let meta = metadata(thread_id, /*ordinal*/ 3, Some(end));
    let latest = message(/*ordinal*/ 4, "active segment record");
    let path = rollout_path(home.path(), thread_id);
    let mut bytes = record(&meta);
    bytes.extend_from_slice(&record(&latest));
    write(&path, &bytes);
    let reservation = store
        .reserve_rollout_writers(&[thread_id])
        .await
        .expect("reserve source");
    let lineage = RolloutLineage {
        root_rollout_id: thread_id,
        segments: vec![
            segment(
                thread_id,
                ancestor_id,
                &ancestor,
                /*start_ordinal*/ 1,
                Some(end),
            ),
            segment(thread_id, thread_id, &path, /*start_ordinal*/ 4, None),
        ],
    };

    let (lineage, _, full_history, session_meta) = store
        .materialize_clean_fork_lineage(
            thread_id,
            lineage,
            &reservation,
            /*include_full_history*/ true,
        )
        .await
        .expect("load damaged ancestor")
        .expect("prepare complete fork source");

    assert_eq!(
        serde_json::to_value(full_history).expect("serialize FullHistory items"),
        serde_json::to_value(Some(vec![
            meta.item.clone(),
            first.item,
            retry.item,
            latest.item
        ]))
        .expect("serialize expected FullHistory items")
    );
    assert_eq!(session_meta.meta.history_base, Some(end));
    assert_eq!(
        lineage.segments[0].end_byte_offset,
        Some(end.end_byte_offset)
    );
    assert_eq!(
        fs::read(ancestor).expect("ancestor remains intact"),
        ancestor_bytes
    );
    assert_eq!(
        fs::read(path).expect("active segment remains intact"),
        bytes
    );
}

#[tokio::test]
async fn full_history_damaged_tail_keeps_last_complete_ordinal_and_cutoff_checks() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    let path = rollout_path(home.path(), thread_id);
    let meta = metadata(thread_id, /*ordinal*/ 0, /*history_base*/ None);
    let complete = message(/*ordinal*/ 1, "last complete record");
    let mut bytes = record(&meta);
    bytes.extend_from_slice(&record(&complete));
    bytes.extend_from_slice(&interrupted(&message(
        /*ordinal*/ 2,
        "incomplete tail",
    )));
    write(&path, &bytes);
    let reservation = store
        .reserve_rollout_writers(&[thread_id])
        .await
        .expect("reserve source");
    let lineage = RolloutLineage {
        root_rollout_id: thread_id,
        segments: vec![segment(
            thread_id, thread_id, &path, /*start_ordinal*/ 1, None,
        )],
    };
    let (lineage, _, full_history, _) = store
        .materialize_clean_fork_lineage(
            thread_id,
            lineage,
            &reservation,
            /*include_full_history*/ true,
        )
        .await
        .expect("load complete prefix")
        .expect("prepare latest fork source");
    assert_eq!(
        serde_json::to_value(full_history).expect("serialize FullHistory items"),
        serde_json::to_value(Some(vec![meta.item, complete.item]))
            .expect("serialize expected FullHistory items")
    );
    assert_eq!(lineage.segments[0].end_ordinal_exclusive, Some(2));

    let claimed_end = HistoryPosition {
        thread_id,
        end_ordinal_exclusive: 3,
        end_byte_offset: bytes.len() as u64,
    };
    let claimed_lineage = RolloutLineage {
        root_rollout_id: thread_id,
        segments: vec![segment(
            thread_id,
            thread_id,
            &path,
            /*start_ordinal*/ 1,
            Some(claimed_end),
        )],
    };
    assert!(
        store
            .materialize_clean_fork_lineage(
                thread_id,
                claimed_lineage,
                &reservation,
                /*include_full_history*/ true
            )
            .await
            .expect("inspect claimed cutoff")
            .is_none(),
        "damaged tail cannot satisfy a missing ordinal boundary"
    );
    assert_eq!(fs::read(path).expect("source remains intact"), bytes);
}

#[test]
fn full_history_shared_loader_preserves_arbitrary_precision_token_count() {
    let thread_id = ThreadId::new();
    let path = Path::new("native-token-count.jsonl");
    let mut bytes = record(&metadata(
        thread_id, /*ordinal*/ 0, /*history_base*/ None,
    ));
    let token_count = serde_json::json!({
        "timestamp": "2026-09-01T00:00:01Z", "ordinal": 1, "type": "event_msg",
        "payload": { "type": "token_count", "info": null,
            "rate_limits": { "primary": { "used_percent": 12.5, "window_minutes": 60, "resets_at": 42 } }
        }
    });
    let token_count_offset = bytes.len() as u64;
    let token_count_bytes = serde_json::to_vec(&token_count).expect("serialize token count");
    bytes.extend_from_slice(&token_count_bytes);
    bytes.push(b'\n');
    let expected = RolloutRecorder::parse_rollout_line_value(token_count)
        .expect("compatible token count")
        .expect("ordinary record");

    let parsed =
        parse_rollout_bytes(path, &bytes, thread_id).expect("parse arbitrary precision event");

    assert_eq!(
        serde_json::to_value(parsed.last()).expect("serialize token count envelope and offset"),
        serde_json::to_value(Some(&(token_count_offset, expected)))
            .expect("serialize expected token count envelope and offset")
    );
    assert_eq!(parsed.len(), 2);
}

#[test]
fn full_history_shared_loader_keeps_reference_errors_fatal_and_native_suffixes_unsplit() {
    let thread_id = ThreadId::new();
    let path = Path::new("native-record-policy.jsonl");
    let meta = record(&metadata(
        thread_id, /*ordinal*/ 0, /*history_base*/ None,
    ));
    let ordinary = record(&message(
        /*ordinal*/ 1,
        "valid suffix must not invent a native boundary",
    ));
    let mut bytes = meta.clone();
    bytes.extend_from_slice(b"{broken");
    bytes.extend_from_slice(&ordinary);
    let (parsed, parsed_thread_id, errors) =
        RolloutRecorder::load_rollout_lines_from_bytes(path, &bytes)
            .expect("skip malformed native physical line");
    assert_eq!(
        (parsed.len(), parsed_thread_id, errors),
        (1, Some(thread_id), 1)
    );

    let mut bytes = meta;
    bytes.extend_from_slice(br#"{"timestamp":"2026-09-01T00:00:01Z","ordinal":1,"type":"rollout_reference","payload":{}}"#);
    bytes.push(b'\n');
    let error = parse_rollout_bytes(path, &bytes, thread_id)
        .err()
        .expect("reference topology remains validated");
    assert!(
        error
            .to_string()
            .contains("invalid rollout reference record")
    );
}
