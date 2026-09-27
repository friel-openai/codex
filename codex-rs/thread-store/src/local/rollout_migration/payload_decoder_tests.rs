use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use codex_protocol::items::TurnItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ReviewCodeLocation;
use codex_protocol::protocol::ReviewFinding;
use codex_protocol::protocol::ReviewLineRange;
use codex_protocol::protocol::ReviewOutputEvent;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use super::decode;
use crate::local::rollout_migration::line_parser;

fn assert_same_result(
    expected: Result<Option<RolloutLine>, String>,
    actual: Result<Option<RolloutLine>, String>,
) {
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => assert_eq!(
            expected.map(|line| serde_json::to_value(line).expect("reference JSON")),
            actual.map(|line| serde_json::to_value(line).expect("candidate JSON")),
        ),
        (Err(_), Err(_)) => {}
        (expected, actual) => panic!(
            "decoder acceptance differs: reference error {:?}, candidate error {:?}",
            expected.err(),
            actual.err()
        ),
    }
}

fn reference(bytes: &[u8], normalize: bool) -> Result<Option<RolloutLine>, String> {
    let value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let value = if normalize {
        line_parser::normalize_legacy_rollout_value(value)?
    } else {
        Some(value)
    };
    value
        .map(|value| codex_rollout::decode_rollout_line(value).map_err(|error| error.to_string()))
        .transpose()
}

fn candidate(bytes: &[u8], normalize: bool) -> Result<Option<RolloutLine>, String> {
    if normalize {
        line_parser::parse_legacy_rollout_line(bytes)
    } else {
        line_parser::parse_paginated_rollout_line(bytes).map(Some)
    }
}

#[test]
fn guarded_payload_decoder_preserves_numeric_and_malformed_acceptance() {
    for number in [
        "0",
        "-1",
        "1.2300",
        "-0.0",
        "1e2",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775809",
        "0.123456789012345678901",
        "1e400",
    ] {
        let number: Value = serde_json::from_str(number).expect("numeric JSON");
        for value in [
            json!({"timestamp":"now","type":"event_msg","payload":{
                "type":"token_count","info":null,"rate_limits":{"primary":{
                    "used_percent":number,"window_minutes":300,"resets_at":1800000000}}}}),
            json!({"timestamp":"now","type":"compacted","payload":{"message":"checkpoint"},"future":number}),
            json!({"timestamp":"now","type":"compacted","payload":{"message":"checkpoint","future":[number]}}),
        ] {
            let bytes = serde_json::to_vec(&value).expect("fixture JSON");
            for normalize in [false, true] {
                assert_same_result(reference(&bytes, normalize), candidate(&bytes, normalize));
            }
        }
    }
    for encoded in [
        r#"{"timestamp":"old","timestamp":"new","type":"event_msg","payload":{"type":"warning","message":"old","message":"new"}}"#,
        r#"{"timestamp":"now","type":"response_item","payload":{"type":"message","role":"developer","content":[]},"metadata":{"client_authored":true}}"#,
        r#"{"timestamp":"now","type":"response_item","payload":{"type":"message","role":"user","content":[]},"metadata":null}"#,
        r#"{"timestamp":"now","type":"response_item","payload":{"type":"message","role":"user","content":[]},"metadata":false}"#,
        r#"{"timestamp":"now","type":"compacted","payload":{"message":"checkpoint","replacement_history":[],"replacement_history_metadata":[{}]}}"#,
        r#"{"timestamp":"now","type":"compacted","payload":{"message":"checkpoint","replacement_history":[{"type":"message","role":"user","content":[]}],"replacement_history_metadata":[{"client_authored":true}]}}"#,
        r#"{"timestamp":"now","type":"compacted","payload":null}"#,
        r#"{"timestamp":"now","type":"compacted"}"#,
        r#"{"timestamp":"now","ordinal":-1,"type":"event_msg","payload":{"type":"warning","message":"x"}}"#,
        r#"{"timestamp":"now","type":"event_msg","payload":{"type":"unknown"}}"#,
        r#"{"timestamp":"now","type":"fork_reference","payload":{}}"#,
        r#"{"timestamp":"now","type":"unknown","payload":{}}"#,
        r#"{"timestamp":"now","type":"compacted","payload":{"message":"checkpoint"},"future":18446744073709551616}"#,
        r#"{"timestamp":"now","type":"event_msg","payload":{"type":"guardian_assessment"}}"#,
    ] {
        for normalize in [false, true] {
            assert_same_result(
                reference(encoded.as_bytes(), normalize),
                candidate(encoded.as_bytes(), normalize),
            );
        }
    }
    for number in ["18446744073709551616", "-9223372036854775809"] {
        let number: Value = serde_json::from_str(number).expect("oversized integer");
        for oversized in [
            json!({"timestamp":"now","type":"compacted","payload":{"message":"checkpoint"},"future":number}),
            json!({"timestamp":"now","type":"compacted","payload":{"message":"checkpoint","future":[number]}}),
        ] {
            assert!(codex_rollout::decode_rollout_line(oversized.clone()).is_err());
            assert!(decode(oversized).is_err());
        }
    }
}

#[test]
fn guarded_payload_decoder_preserves_decimal_review_outputs() {
    let thread_id = codex_protocol::ThreadId::new();
    let expected = ReviewOutputEvent {
        findings: vec![ReviewFinding {
            title: "Review finding".to_string(),
            body: "Finding evidence".to_string(),
            confidence_score: 0.75,
            priority: 2,
            code_location: ReviewCodeLocation {
                absolute_file_path: PathBuf::from("/tmp/review.rs"),
                line_range: ReviewLineRange { start: 3, end: 5 },
            },
        }],
        overall_correctness: "patch is incorrect".to_string(),
        overall_explanation: "Review evidence".to_string(),
        overall_confidence_score: 0.75,
    };
    for score in [
        "0.75",
        "7.5e-1",
        "75e-2",
        "0.75000000000000000000000000000001",
    ] {
        let review_output: Value = serde_json::from_str(&format!(
            r#"{{"findings":[{{"title":"Review finding","body":"Finding evidence","confidence_score":{score},"priority":2,"code_location":{{"absolute_file_path":"/tmp/review.rs","line_range":{{"start":3,"end":5}}}}}}],"overall_correctness":"patch is incorrect","overall_explanation":"Review evidence","overall_confidence_score":{score}}}"#
        ))
        .expect("raw decimal review output");
        for payload in [
            json!({
                "type": "exited_review_mode", "turn_id": "review-turn", "item_id": "review-1",
                "review_output": review_output,
            }),
            json!({
                "type": "item_completed", "thread_id": thread_id, "turn_id": "review-turn",
                "started_at_ms": 1, "completed_at_ms": 2,
                "item": {"type": "ExitedReviewMode", "id": "review-1", "review_output": review_output},
            }),
        ] {
            let value = json!({
                "timestamp": "2026-09-23T00:00:00Z", "ordinal": 7,
                "type": "event_msg", "payload": payload,
            });
            assert!(!super::has_only_content_safe_numbers(&value));
            let bytes = serde_json::to_vec(&value).expect("serialize review rollout");
            for normalize in [false, true] {
                let canonical = reference(&bytes, normalize)
                    .expect("canonical decoder accepts decimal review")
                    .expect("canonical decoder retains review");
                let decoded = candidate(&bytes, normalize)
                    .expect("migration decoder accepts decimal review")
                    .expect("migration decoder retains review");
                assert_eq!(
                    serde_json::to_value(&decoded).expect("migration review JSON"),
                    serde_json::to_value(canonical).expect("canonical review JSON")
                );
                let review_output = match decoded.item {
                    RolloutItem::EventMsg(EventMsg::ExitedReviewMode(event)) => {
                        assert_eq!(event.turn_id.as_deref(), Some("review-turn"));
                        assert_eq!(event.item_id.as_deref(), Some("review-1"));
                        event.review_output
                    }
                    RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => {
                        assert_eq!(event.thread_id, thread_id);
                        assert_eq!(event.turn_id, "review-turn");
                        assert_eq!(event.started_at_ms, Some(1));
                        assert_eq!(event.completed_at_ms, 2);
                        let TurnItem::ExitedReviewMode(item) = event.item else {
                            panic!("expected completed review item");
                        };
                        assert_eq!(item.id, "review-1");
                        item.review_output
                    }
                    _ => panic!("expected persisted review output"),
                };
                assert_eq!(review_output.as_ref(), Some(&expected));
            }
        }
    }
}

#[test]
fn guarded_payload_decoder_preserves_response_metadata_and_file_id_images() {
    let value = json!({
        "timestamp": "now",
        "ordinal": 7,
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [
                {"type": "input_text", "text": "Compare these images."},
                {"type": "input_image", "file_id": "file-inherited", "detail": "high"},
                {"type": "input_image", "image_url": "https://example.com/image.png", "detail": "original"}
            ]
        },
        "metadata": {
            "client_authored": true,
            "fallback_token_limit_override": 20_000,
            "delivered_assistant_message": "Delivered assistant text.",
            "harness_authored_configuration": true,
            "compaction_model_hash": "producer-model",
            "user_input_order": 9,
            "inherited_user_message": true,
            "sender_user_messages": {
                "receiver_turn_id": "receiver-turn",
                "receiver_message_id": "receiver-message",
                "text": "Host-rendered sender context."
            }
        }
    });
    for canonical_fallback in [false, true] {
        let mut value = value.clone();
        if canonical_fallback {
            value["future"] = json!(0.5);
        }
        assert_eq!(
            super::has_only_content_safe_numbers(&value),
            !canonical_fallback
        );
        let bytes = serde_json::to_vec(&value).expect("serialize response envelope");
        for normalize in [false, true] {
            for decoded in [reference(&bytes, normalize), candidate(&bytes, normalize)] {
                let decoded = decoded
                    .expect("accept response envelope")
                    .expect("retain response envelope");
                assert_eq!(decoded.ordinal, Some(7));
                let RolloutItem::ResponseItem(envelope) = decoded.item else {
                    panic!("expected response envelope");
                };
                let metadata = envelope.metadata.expect("retain harness metadata");
                assert_eq!(metadata.history_truncation_token_limit, Some(20_000));
                assert_eq!(
                    metadata.delivered_assistant_message.as_deref(),
                    Some("Delivered assistant text.")
                );
                assert!(metadata.inherited_user_message);
                assert_eq!(metadata.user_input_order, Some(9));
                let sender = metadata
                    .sender_user_messages
                    .as_deref()
                    .expect("retain sender");
                assert_eq!(sender.receiver_turn_id, "receiver-turn");
                assert_eq!(sender.receiver_message_id, "receiver-message");
                assert_eq!(sender.text, "Host-rendered sender context.");
                assert_eq!(
                    serde_json::to_value(metadata).expect("serialize retained metadata"),
                    value["metadata"]
                );
                assert_eq!(
                    serde_json::to_value(envelope.item).expect("serialize retained response")["content"],
                    value["payload"]["content"]
                );
            }
        }
    }
}

/// Private incident rollouts stay outside the repository; compare both unmodified and normalized
/// inputs because source authentication and Legacy canonicalization use different acceptance.
#[tokio::test]
#[ignore = "set CODEX_ROLLOUT_DECODER_CORPUS to copied rollout files"]
async fn benchmark_guarded_payload_decoder_against_existing_decoder() {
    let mut directories = vec![PathBuf::from(
        std::env::var_os("CODEX_ROLLOUT_DECODER_CORPUS").expect("corpus directory"),
    )];
    let mut records = 0_u64;
    let mut existing = Duration::ZERO;
    let mut guarded = Duration::ZERO;
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).expect("corpus directory") {
            let entry = entry.expect("corpus entry");
            let path = entry.path();
            if entry.file_type().expect("file type").is_dir() {
                directories.push(path);
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("rollout-")
                || !(name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
            {
                continue;
            }
            let mut reader = codex_rollout::open_rollout_line_reader(&path)
                .await
                .expect("source reader");
            while let Some(raw) = reader.next_line().await.expect("source record") {
                for normalize in [false, true] {
                    let started = Instant::now();
                    let expected = reference(raw.as_bytes(), normalize);
                    existing += started.elapsed();
                    let started = Instant::now();
                    let actual = candidate(raw.as_bytes(), normalize);
                    guarded += started.elapsed();
                    assert_same_result(expected, actual);
                }
                records += 1;
            }
        }
    }
    assert!(records > 0);
    eprintln!(
        "records={records} raw_and_normalized_existing_ms={} guarded_ms={}",
        existing.as_millis(),
        guarded.as_millis()
    );
}
