use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_rollout::ResponseItemEnvelope;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::parse_legacy_rollout_line;
use super::parse_legacy_rollout_lines;
use super::parse_paginated_rollout_line;

fn assert_decoder_equivalence(value: serde_json::Value) -> bool {
    let canonical = codex_rollout::decode_rollout_line(value.clone());
    let optimized = super::super::payload_decoder::decode(value);
    match (canonical, optimized) {
        (Ok(canonical), Ok(optimized)) => {
            let canonical =
                serde_json::to_vec(&canonical).expect("serialize canonical decoded record");
            let optimized =
                serde_json::to_vec(&optimized).expect("serialize optimized decoded record");
            if canonical == optimized {
                return true;
            }
            // Some protocol payloads contain HashMaps. Independent decodes can serialize those
            // keys in different orders even when both use the same decoder.
            assert!(
                serde_json::from_slice::<serde_json::Value>(&canonical).expect("canonical JSON")
                    == serde_json::from_slice::<serde_json::Value>(&optimized)
                        .expect("optimized JSON"),
                "decoded rollout contents differ"
            );
            false
        }
        (Err(_), Err(_)) => true,
        (canonical, optimized) => panic!(
            "decoder acceptance differs: canonical error {:?}, optimized error {:?}",
            canonical.err(),
            optimized.err()
        ),
    }
}

#[test]
fn value_decoders_preserve_acceptance_and_canonical_bytes() {
    for encoded in [
        r#"{"timestamp":"old","timestamp":"new","ordinal":null,"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"primary":{"used_percent":0.1234567890123456789,"window_minutes":300,"resets_at":1800000000}}}}"#,
        r#"{"payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"hello"}]},"metadata":{"client_authored":true},"type":"response_item","ordinal":18446744073709551615,"timestamp":"now"}"#,
        r#"{"timestamp":"now","ordinal":3,"type":"event_msg","metadata":"ignored","payload":{"type":"warning","message":"old","message":"new"}}"#,
        r#"{"timestamp":"now","ordinal":-1,"type":"event_msg","payload":{"type":"warning","message":"bad ordinal"}}"#,
        r#"{"timestamp":"now","type":"event_msg","payload":{"type":"unknown"}}"#,
    ] {
        let value = serde_json::from_str(encoded).expect("parse fixture JSON");
        assert_decoder_equivalence(value);
    }
}

#[test]
fn compacted_replacement_history_preserves_aligned_metadata_and_image_references() {
    let replacement_history = json!([
        {
            "type": "message",
            "role": "user",
            "content": [
                { "type": "input_text", "text": "Compare these images." },
                {
                    "type": "input_image",
                    "image_url": "data:image/png;base64,aW5saW5l",
                    "detail": "high"
                },
                {
                    "type": "input_image",
                    "file_id": "file-reference-image",
                    "detail": "original"
                }
            ]
        },
        {
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "The images differ." }],
            "phase": "final_answer"
        }
    ]);
    let replacement_history_metadata = json!([
        {
            "client_authored": true,
            "fallback_token_limit_override": 8192,
            "delivered_assistant_message": "First delivered message.",
            "harness_authored_configuration": true,
            "compaction_model_hash": "first-model-hash",
            "user_input_order": 73,
            "inherited_user_message": true,
            "sender_user_messages": {
                "receiver_turn_id": "first-receiver-turn",
                "receiver_message_id": "first-receiver-message",
                "text": "First sender context."
            }
        },
        {
            "client_authored": false,
            "fallback_token_limit_override": 4096,
            "delivered_assistant_message": "Second delivered message.",
            "user_input_order": 74,
            "sender_user_messages": {
                "receiver_turn_id": "second-receiver-turn",
                "receiver_message_id": "second-receiver-message",
                "text": "Second sender context."
            }
        }
    ]);
    let fixture = json!({
        "timestamp": "2026-09-23T00:00:00Z",
        "ordinal": 17,
        "type": "compacted",
        "payload": {
            "message": "Compacted image comparison.",
            "replacement_history": replacement_history,
            "replacement_history_metadata": replacement_history_metadata
        }
    });
    let bytes = serde_json::to_vec(&fixture).expect("serialize compacted metadata fixture");
    for (route, decoded) in [
        (
            "canonical value",
            codex_rollout::decode_rollout_line(fixture.clone())
                .expect("canonical decoder accepts compacted metadata"),
        ),
        (
            "optimized value",
            super::super::payload_decoder::decode(fixture.clone())
                .expect("optimized decoder accepts compacted metadata"),
        ),
        (
            "paginated bytes",
            parse_paginated_rollout_line(&bytes)
                .expect("paginated parser accepts compacted metadata"),
        ),
        (
            "legacy bytes",
            parse_legacy_rollout_line(&bytes)
                .expect("legacy parser accepts compacted metadata")
                .expect("legacy parser retains compacted metadata"),
        ),
    ] {
        assert_eq!(decoded.timestamp, "2026-09-23T00:00:00Z", "{route}");
        assert_eq!(decoded.ordinal, Some(17), "{route}");
        let RolloutItem::Compacted(compacted) = decoded.item else {
            panic!("{route} changed the compacted variant");
        };
        assert_eq!(compacted.message, "Compacted image comparison.", "{route}");
        let entries = compacted
            .replacement_history
            .expect("preserve both replacement history entries");
        assert_eq!(entries.len(), 2, "{route}");
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(
                serde_json::to_value(&entry.item).expect("serialize replacement item"),
                replacement_history[index],
                "{route}: replacement item {index}"
            );
            let metadata = entry.metadata.as_ref().expect("preserve entry metadata");
            assert_eq!(
                serde_json::to_value(metadata).expect("serialize replacement metadata"),
                replacement_history_metadata[index],
                "{route}: replacement metadata {index}"
            );
            assert_eq!(
                metadata.history_truncation_token_limit,
                Some([8192, 4096][index]),
                "{route}: restore fallback_token_limit_override"
            );
            assert_eq!(metadata.inherited_user_message, index == 0, "{route}");
        }
    }

    for metadata_count in [1, 3] {
        let mut invalid = fixture.clone();
        invalid["payload"]["replacement_history_metadata"] = serde_json::Value::Array(
            (0..metadata_count)
                .map(|index| replacement_history_metadata[index % 2].clone())
                .collect(),
        );
        let bytes = serde_json::to_vec(&invalid).expect("serialize misaligned metadata");
        assert!(codex_rollout::decode_rollout_line(invalid.clone()).is_err());
        assert!(super::super::payload_decoder::decode(invalid).is_err());
        assert!(parse_paginated_rollout_line(&bytes).is_err());
        assert!(parse_legacy_rollout_line(&bytes).is_err());
    }
}

/// Optional differential admission over private incident files without checking them into Git.
#[tokio::test]
#[ignore = "set CODEX_ROLLOUT_DECODER_CORPUS to a directory of copied rollouts"]
async fn value_decoders_match_supplied_rollout_corpus() {
    let mut directories = vec![std::path::PathBuf::from(
        std::env::var_os("CODEX_ROLLOUT_DECODER_CORPUS").expect("rollout corpus directory"),
    )];
    let mut records = 0_u64;
    let mut reordered = 0_u64;
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).expect("read corpus directory") {
            let entry = entry.expect("read corpus entry");
            let path = entry.path();
            if entry.file_type().expect("read corpus file type").is_dir() {
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
                .expect("open corpus rollout");
            while let Some(raw) = reader.next_line().await.expect("read corpus record") {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
                    continue;
                };
                reordered += u64::from(!assert_decoder_equivalence(value.clone()));
                if let Ok(Some(normalized)) = super::normalize_legacy_rollout_value(value) {
                    reordered += u64::from(!assert_decoder_equivalence(normalized));
                }
                records += 1;
            }
        }
    }
    assert!(records > 0, "corpus contains no rollout records");
    eprintln!(
        "compared {records} raw and normalized rollout records; {reordered} object-key reorderings"
    );
}

fn line(payload_type: &str, payload: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "timestamp": "2025-01-03T12:00:00Z",
        "type": payload_type,
        "payload": payload,
    }))
    .expect("serialize fixture")
}

#[test]
fn recovers_observed_response_items_after_truncated_legacy_prefixes() {
    let fixtures = [
        (
            r#"{"timestamp":"broken","type":"response_item","payload":{"type":"message"#,
            r#"{"timestamp":"2026-06-12T19:05:57Z","type":"response_item","payload":{"type":"custom_tool_call","call_id":"call-1","name":"tool","input":"{}"}}"#,
            "custom_tool_call",
        ),
        (
            r#"{"timestamp":"broken","type":"response_item","payload":{"type":"reasoning","encrypted_content":"truncated"#,
            r#"{"timestamp":"2026-06-12T19:05:57Z","type":"response_item","payload":{"type":"reasoning","summary":[],"encrypted_content":"ciphertext"}}"#,
            "reasoning",
        ),
    ];

    for (prefix, complete, expected) in fixtures {
        let bytes = format!("{prefix}{complete}");
        let lines = parse_legacy_rollout_lines(bytes.as_bytes()).expect("recover complete suffix");
        assert_eq!(lines.len(), 1);
        let RolloutItem::ResponseItem(ResponseItemEnvelope { item, .. }) = &lines[0].item else {
            panic!("expected response item");
        };
        assert!(matches!(
            (expected, item),
            ("custom_tool_call", ResponseItem::CustomToolCall { .. })
                | ("reasoning", ResponseItem::Reasoning { .. })
        ));
    }
}

#[test]
fn parses_legacy_numeric_event_payloads_through_value() {
    let bytes = line(
        "event_msg",
        json!({
            "type": "token_count",
            "info": null,
            "rate_limits": {
                "primary": {
                    "used_percent": 0.0,
                    "window_minutes": 300,
                    "resets_at": 1_770_414_841,
                },
                "secondary": {
                    "used_percent": 12.5,
                    "window_minutes": 10_080,
                    "resets_at": 1_770_698_702,
                },
                "credits": {
                    "has_credits": false,
                    "unlimited": false,
                    "balance": null,
                },
                "plan_type": null,
            },
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy token count")
        .expect("keep legacy token count");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::TokenCount(_))
    ));
}

#[test]
fn parses_paginated_numeric_event_payloads_through_value() {
    let bytes = serde_json::to_vec(&json!({
        "timestamp": "2026-08-18T21:03:49.690Z",
        "ordinal": 11938,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": 319590193,
                    "cached_input_tokens": 312717039,
                    "cache_write_input_tokens": 6711808,
                    "output_tokens": 364803,
                    "reasoning_output_tokens": 57778,
                    "total_tokens": 319954996
                },
                "last_token_usage": {
                    "input_tokens": 203881,
                    "cached_input_tokens": 0,
                    "cache_write_input_tokens": 203740,
                    "output_tokens": 280,
                    "reasoning_output_tokens": 184,
                    "total_tokens": 204161
                },
                "model_context_window": 258400
            },
            "rate_limits": {
                "limit_id": "codex",
                "limit_name": null,
                "primary": {
                    "used_percent": 0.0,
                    "window_minutes": 1,
                    "resets_at": 1787087041
                },
                "secondary": {
                    "used_percent": 0.0,
                    "window_minutes": 300,
                    "resets_at": 1787102386
                },
                "credits": {
                    "has_credits": true,
                    "unlimited": true,
                    "balance": null
                },
                "individual_limit": null,
                "spend_control_reached": null,
                "plan_type": "business",
                "rate_limit_reached_type": null
            }
        }
    }))
    .expect("serialize Paginated token count");

    let parsed = parse_paginated_rollout_line(&bytes).expect("parse Paginated token count");
    assert_eq!(
        serde_json::to_value(
            codex_rollout::parse_rollout_line_bytes(&bytes)
                .expect("canonical rollout decoder accepts numeric token count")
        )
        .expect("serialize canonical decode"),
        serde_json::to_value(&parsed).expect("serialize canonical decode")
    );
    assert_eq!(parsed.ordinal, Some(11938));
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::TokenCount(_))
    ));
}

#[test]
fn normalizes_legacy_rate_limit_reset_timestamps() {
    let bytes = line(
        "event_msg",
        json!({
            "type": "token_count",
            "info": null,
            "rate_limits": {
                "primary": {
                    "used_percent": 2,
                    "window_minutes": 300,
                    "resets_at": "2025-10-19T08:51:37.876641+00:00",
                },
                "secondary": null,
                "credits": null,
                "plan_type": null,
            },
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy reset timestamp")
        .expect("keep legacy token count");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::TokenCount(_))
    ));
}

#[test]
fn normalizes_legacy_turn_context_collaboration_mode() {
    let cwd = std::env::temp_dir().to_string_lossy().into_owned();
    let bytes = line(
        "turn_context",
        json!({
            "cwd": cwd,
            "approval_policy": "never",
            "sandbox_policy": {"type": "danger-full-access"},
            "model": "gpt-test",
            "personality": null,
            "collaboration_mode": {
                "mode": "plan",
                "model": "gpt-test",
                "reasoning_effort": null,
                "developer_instructions": null,
            },
            "effort": null,
            "summary": "auto",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy turn context")
        .expect("keep legacy turn context");
    let RolloutItem::TurnContext(context) = parsed.item else {
        panic!("expected turn context");
    };
    assert_eq!(
        context
            .collaboration_mode
            .expect("collaboration mode")
            .model(),
        "gpt-test"
    );
}

#[test]
fn normalizes_legacy_turn_context_sandbox_policy() {
    let cwd = std::env::temp_dir().to_string_lossy().into_owned();
    let bytes = line(
        "turn_context",
        json!({
            "cwd": cwd,
            "approval_policy": "never",
            "sandbox_policy": {"mode": "danger-full-access"},
            "model": "gpt-test",
            "personality": null,
            "effort": null,
            "summary": "auto",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy sandbox policy")
        .expect("keep legacy turn context");
    assert!(matches!(parsed.item, RolloutItem::TurnContext(_)));
}

#[test]
fn normalizes_legacy_review_entry_prompt() {
    let bytes = line(
        "event_msg",
        json!({
            "type": "entered_review_mode",
            "prompt": "review these changes",
            "user_facing_hint": "Review requested.",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy review entry")
        .expect("keep legacy review entry");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::EnteredReviewMode(_))
    ));
}

#[test]
fn normalizes_legacy_plain_command_cwd() {
    let cwd = std::env::temp_dir().to_string_lossy().into_owned();
    let bytes = line(
        "event_msg",
        json!({
            "type": "exec_command_end",
            "call_id": "call-1",
            "turn_id": "turn-1",
            "command": ["echo", "ok"],
            "cwd": cwd,
            "parsed_cmd": [],
            "source": "agent",
            "stdout": "",
            "stderr": "",
            "aggregated_output": "",
            "exit_code": 0,
            "duration": {"secs": 0, "nanos": 0},
            "formatted_output": "",
            "status": "completed",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy command")
        .expect("keep legacy command");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::ExecCommandEnd(_))
    ));
}

#[test]
fn skips_only_known_retired_events() {
    for event_type in [
        "guardian_assessment",
        "thread_name_updated",
        "undo_completed",
    ] {
        let bytes = line("event_msg", json!({"type": event_type}));
        assert!(
            parse_legacy_rollout_line(&bytes)
                .expect("inspect retired event")
                .is_none()
        );
    }

    let unknown = line("event_msg", json!({"type": "unknown_legacy_event"}));
    assert!(parse_legacy_rollout_line(&unknown).is_err());
}

#[test]
fn skips_legacy_ghost_snapshots() {
    let ghost_snapshot = line(
        "response_item",
        json!({
            "type": "ghost_snapshot",
            "ghost_commit": {"id": "legacy"},
        }),
    );

    assert!(
        parse_legacy_rollout_line(&ghost_snapshot)
            .expect("inspect ghost snapshot")
            .is_none()
    );
}
