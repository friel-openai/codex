use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use codex_history::CodexHarnessMetadata;
use codex_history::CompactedItem;
use codex_history::GuardianHistoryCheckpoint;
use codex_history::ResponseItemEnvelope;
use codex_history::RetainedContextReplay;
use codex_history::ReviewInputRecord;
use codex_history::ReviewTranscriptApplicability;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use super::load_review_input_prefix;
use super::resolve_review_input;
use super::review_input_segment_path;
use crate::RolloutItem;
use crate::RolloutLine;

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn review_records() -> Vec<ReviewInputRecord> {
    let boundary = user_message("Remove this instruction.");
    vec![
        ReviewInputRecord::Baseline {
            applicability: ReviewTranscriptApplicability::Both,
            history: GuardianHistoryCheckpoint(vec![user_message("Keep this instruction.").into()]),
            root_retains_legacy_transcript: None,
        },
        ReviewInputRecord::ResponseItem {
            response: ResponseItemEnvelope::new(boundary.clone()),
        },
        ReviewInputRecord::Rollback {
            boundary,
            boundary_metadata: None,
        },
    ]
}

fn segment_lines(rollout_id: ThreadId, records: &[ReviewInputRecord]) -> Vec<Value> {
    let header = RolloutLine {
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        ordinal: Some(0),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: rollout_id,
                session_id: rollout_id.into(),
                segment_id: Some(
                    SegmentId::from_string(&rollout_id.to_string()).expect("segment ID"),
                ),
                history_mode: ThreadHistoryMode::Paginated,
                ..Default::default()
            },
            git: None,
        }),
    };
    let mut lines = vec![serde_json::to_value(header).expect("serialize segment header")];
    lines.extend(records.iter().enumerate().map(|(index, record)| {
        json!({
            "ordinal": index + 1,
            "record": record,
        })
    }));
    lines
}

fn write_segment(home: &Path, rollout_id: ThreadId, lines: &[Value]) -> HistoryPosition {
    let path = review_input_segment_path(home, rollout_id);
    fs::create_dir_all(path.parent().expect("segment directory")).expect("create directory");
    let mut file = fs::File::create(path).expect("create review input segment");
    for line in lines {
        serde_json::to_writer(&mut file, line).expect("serialize segment line");
        file.write_all(b"\n").expect("terminate segment line");
    }
    HistoryPosition {
        thread_id: rollout_id,
        end_ordinal_exclusive: u64::try_from(lines.len()).expect("line count"),
        end_byte_offset: file.metadata().expect("segment metadata").len(),
    }
}

fn checkpoint(review_input: Option<HistoryPosition>) -> CompactedItem {
    let mut compacted: CompactedItem = serde_json::from_value(json!({
        "message": "",
        "replacement_history": [],
    }))
    .expect("compaction fixture");
    compacted.retained_context_replay = Some(RetainedContextReplay {
        legacy: Default::default(),
        thread_owned_worker: Default::default(),
        thread_owned_root: Default::default(),
        review_input,
        resolved_review_input: None,
    });
    compacted
}

#[tokio::test]
async fn finite_review_prefix_ignores_corrupt_appended_tail() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let records = review_records();
    let position = write_segment(
        home.path(),
        rollout_id,
        &segment_lines(rollout_id, &records),
    );
    fs::OpenOptions::new()
        .append(true)
        .open(review_input_segment_path(home.path(), rollout_id))
        .expect("open segment tail")
        .write_all(b"not a JSON record\n")
        .expect("append corrupt tail");

    assert_eq!(
        load_review_input_prefix(home.path(), position)
            .await
            .expect("read finite prefix"),
        records,
    );
}

#[tokio::test]
async fn review_prefix_rejects_header_identity_mismatch() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    for field in ["id", "segment_id"] {
        let mut lines = segment_lines(rollout_id, &review_records());
        lines[0]["payload"][field] = json!(ThreadId::new());
        let position = write_segment(home.path(), rollout_id, &lines);

        let error = load_review_input_prefix(home.path(), position)
            .await
            .expect_err("reject mismatched segment identity");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("session metadata"),
            "{field}: {error}"
        );
    }
}

#[tokio::test]
async fn review_prefix_rejects_noncontiguous_ordinals() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    for ordinal in [1, 3] {
        let mut lines = segment_lines(rollout_id, &review_records());
        lines[2]["ordinal"] = json!(ordinal);
        let position = write_segment(home.path(), rollout_id, &lines);

        let error = load_review_input_prefix(home.path(), position)
            .await
            .expect_err("reject duplicate or missing ordinal");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not contiguous"), "{error}");
    }
}

#[tokio::test]
async fn review_prefix_rejects_incomplete_byte_boundary() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let position = write_segment(
        home.path(),
        rollout_id,
        &segment_lines(rollout_id, &review_records()),
    );
    for end_byte_offset in [position.end_byte_offset - 1, position.end_byte_offset + 1] {
        let error = load_review_input_prefix(
            home.path(),
            HistoryPosition {
                end_byte_offset,
                ..position
            },
        )
        .await
        .expect_err("reject partial record or endpoint beyond EOF");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("record boundary"), "{error}");
    }
}

#[tokio::test]
async fn review_prefix_rejects_wrong_ordinal_endpoint() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let mut position = write_segment(
        home.path(),
        rollout_id,
        &segment_lines(rollout_id, &review_records()),
    );
    position.end_ordinal_exclusive += 1;

    let error = load_review_input_prefix(home.path(), position)
        .await
        .expect_err("reject ordinal endpoint inconsistent with bytes");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("ordinal endpoint"), "{error}");
}

#[tokio::test]
async fn review_prefix_rejects_blank_physical_records() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let mut position = write_segment(
        home.path(),
        rollout_id,
        &segment_lines(rollout_id, &review_records()),
    );
    fs::OpenOptions::new()
        .append(true)
        .open(review_input_segment_path(home.path(), rollout_id))
        .expect("open segment tail")
        .write_all(b"\n")
        .expect("append blank record");
    position.end_byte_offset += 1;

    load_review_input_prefix(home.path(), position)
        .await
        .expect_err("a blank physical record cannot be ignored inside the finite prefix");
}

#[tokio::test]
async fn review_prefix_requires_exactly_one_initial_baseline() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let records = review_records();
    for invalid in [
        vec![records[0].clone(), records[0].clone(), records[2].clone()],
        records[1..].to_vec(),
    ] {
        let position = write_segment(
            home.path(),
            rollout_id,
            &segment_lines(rollout_id, &invalid),
        );
        let error = load_review_input_prefix(home.path(), position)
            .await
            .expect_err("reject duplicate or missing baseline");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("initial baseline"), "{error}");
    }
}

#[tokio::test]
async fn review_prefix_requires_rollback_endpoint() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let records = review_records();
    let position = write_segment(
        home.path(),
        rollout_id,
        &segment_lines(rollout_id, &records[..2]),
    );

    let error = load_review_input_prefix(home.path(), position)
        .await
        .expect_err("reject endpoint after a response item");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("follow a rollback"), "{error}");
}

#[tokio::test]
async fn review_prefix_preserves_decimal_response_payloads_and_harness_metadata() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let item: ResponseItem = serde_json::from_value(json!({
        "type": "tool_search_call",
        "execution": "client",
        "arguments": { "score": 0.75 },
        "internal_chat_message_metadata_passthrough": { "create_time": 1.25 },
    }))
    .expect("floating-point response item");
    let mut records = review_records();
    records.insert(
        2,
        ReviewInputRecord::ResponseItem {
            response: ResponseItemEnvelope {
                item,
                metadata: Some(CodexHarnessMetadata {
                    history_truncation_token_limit: Some(128),
                    user_input_order: Some(7),
                    ..Default::default()
                }),
            },
        },
    );
    let position = write_segment(
        home.path(),
        rollout_id,
        &segment_lines(rollout_id, &records),
    );

    let loaded = load_review_input_prefix(home.path(), position)
        .await
        .expect("decode floating-point payload through canonical JSON values");
    assert_eq!(loaded, records);
    let loaded = serde_json::to_value(loaded).expect("serialize loaded records");
    assert_eq!(
        loaded[2]["response"]["item"]["arguments"]["score"].as_f64(),
        Some(0.75)
    );
    assert_eq!(
        loaded[2]["response"]["item"]["internal_chat_message_metadata_passthrough"]["create_time"]
            .as_f64(),
        Some(1.25),
    );
}

#[tokio::test]
async fn resolving_review_input_only_populates_the_derived_cache() {
    let home = tempfile::tempdir().expect("Codex home");
    let rollout_id = ThreadId::new();
    let records = review_records();
    let position = write_segment(
        home.path(),
        rollout_id,
        &segment_lines(rollout_id, &records),
    );
    let mut compacted = checkpoint(Some(position));
    let original = compacted.clone();

    resolve_review_input(home.path(), &mut compacted)
        .await
        .expect("resolve review inputs");
    assert_eq!(compacted, original);
    assert_eq!(
        serde_json::to_value(&compacted).expect("serialize hydrated checkpoint"),
        serde_json::to_value(&original).expect("serialize unresolved checkpoint"),
    );
    let cached = Arc::clone(
        compacted
            .retained_context_replay
            .as_ref()
            .expect("replay metadata")
            .resolved_review_input
            .as_ref()
            .expect("resolved inputs"),
    );
    assert_eq!(cached.as_ref(), &records);

    resolve_review_input(&home.path().join("missing-home"), &mut compacted)
        .await
        .expect("reuse cache without filesystem access");
    assert!(Arc::ptr_eq(
        &cached,
        compacted
            .retained_context_replay
            .as_ref()
            .expect("replay metadata")
            .resolved_review_input
            .as_ref()
            .expect("cached inputs"),
    ));
}

#[tokio::test]
async fn resolving_without_review_reference_does_not_require_a_home() {
    let home = tempfile::tempdir().expect("Codex home");
    let missing_home = home.path().join("missing-home");
    let mut compacted = checkpoint(None);
    resolve_review_input(&missing_home, &mut compacted)
        .await
        .expect("no review reference needs no files");
    assert!(
        compacted
            .retained_context_replay
            .as_ref()
            .expect("replay metadata")
            .resolved_review_input
            .is_none(),
    );
    compacted.retained_context_replay = None;
    resolve_review_input(&missing_home, &mut compacted)
        .await
        .expect("ordinary checkpoint needs no files");
}
