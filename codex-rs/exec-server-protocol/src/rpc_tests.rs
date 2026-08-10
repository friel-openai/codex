use std::path::PathBuf;

use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ExitedReviewModeEvent;
use codex_protocol::protocol::RateLimitSnapshot;
use codex_protocol::protocol::RateLimitWindow;
use codex_protocol::protocol::ReviewCodeLocation;
use codex_protocol::protocol::ReviewFinding;
use codex_protocol::protocol::ReviewLineRange;
use codex_protocol::protocol::ReviewOutputEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::TokenCountEvent;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use super::JSONRPCError;
use super::JSONRPCErrorError;
use super::JSONRPCMessage;
use super::JSONRPCNotification;
use super::JSONRPCRequest;
use super::JSONRPCResponse;
use super::MAX_JSONRPC_VALUE_NODES;
use super::RequestId;
use super::SERDE_JSON_RAW_VALUE_TOKEN;

#[test]
fn round_trips_every_jsonrpc_message_variant() -> serde_json::Result<()> {
    let messages = [
        JSONRPCMessage::Request(JSONRPCRequest {
            id: RequestId::Integer(1),
            method: "request".to_string(),
            params: Some(json!({"items": [1, 2, 3]})),
            trace: None,
        }),
        JSONRPCMessage::Notification(JSONRPCNotification {
            method: "notification".to_string(),
            params: Some(json!({"enabled": true})),
        }),
        JSONRPCMessage::Response(JSONRPCResponse {
            id: RequestId::String("response".to_string()),
            result: json!({"value": "ok"}),
        }),
        JSONRPCMessage::Error(JSONRPCError {
            error: JSONRPCErrorError {
                code: -32603,
                data: Some(json!({"retryable": false})),
                message: "failed".to_string(),
            },
            id: RequestId::Integer(2),
        }),
    ];

    for expected in messages {
        let encoded = serde_json::to_string(&expected)?;
        let actual = serde_json::from_str::<JSONRPCMessage>(&encoded)?;
        assert_eq!(actual, expected);
    }

    Ok(())
}

#[test]
fn round_trips_arbitrary_precision_numbers() -> serde_json::Result<()> {
    let encoded = r#"{"method":"numbers","params":{"decimal":1.5,"exponent":1e100,"largeInteger":18446744073709551616}}"#;
    let expected = serde_json::from_str::<Value>(encoded)?;

    let message = serde_json::from_str::<JSONRPCMessage>(encoded)?;
    let actual = serde_json::to_value(message)?;

    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn rollout_line_round_trips_rate_limit_windows_with_arbitrary_precision() -> serde_json::Result<()>
{
    let expected = RateLimitSnapshot {
        limit_id: Some("codex".to_string()),
        limit_name: Some("Codex".to_string()),
        primary: Some(RateLimitWindow {
            used_percent: 25.0,
            window_minutes: Some(300),
            resets_at: Some(1_700),
        }),
        secondary: Some(RateLimitWindow {
            used_percent: 12.5,
            window_minutes: Some(10_080),
            resets_at: Some(2_300),
        }),
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    };
    let line = RolloutLine {
        timestamp: "2026-08-09T00:00:00.000Z".to_string(),
        ordinal: Some(1),
        item: RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: Some(expected.clone()),
        })),
    };

    let encoded = serde_json::to_string(&line)?;
    let decoded = serde_json::from_str::<RolloutLine>(&encoded)?;
    let RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
        rate_limits: Some(actual),
        ..
    })) = decoded.item
    else {
        panic!("expected token-count rollout item");
    };
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn rollout_line_round_trips_review_confidence_with_arbitrary_precision() -> serde_json::Result<()> {
    let expected = ReviewOutputEvent {
        findings: vec![ReviewFinding {
            title: "Finding".to_string(),
            body: "Body".to_string(),
            confidence_score: 0.625,
            priority: 1,
            code_location: ReviewCodeLocation {
                absolute_file_path: PathBuf::from("/tmp/example.rs"),
                line_range: ReviewLineRange { start: 10, end: 12 },
            },
        }],
        overall_correctness: "correct".to_string(),
        overall_explanation: "Explanation".to_string(),
        overall_confidence_score: 0.875,
    };
    let line = RolloutLine {
        timestamp: "2026-08-09T00:00:00.000Z".to_string(),
        ordinal: Some(2),
        item: RolloutItem::EventMsg(EventMsg::ExitedReviewMode(ExitedReviewModeEvent {
            turn_id: Some("turn-1".to_string()),
            item_id: Some("review-1".to_string()),
            review_output: Some(expected.clone()),
        })),
    };

    let encoded = serde_json::to_string(&line)?;
    let decoded = serde_json::from_str::<RolloutLine>(&encoded)?;
    let RolloutItem::EventMsg(EventMsg::ExitedReviewMode(ExitedReviewModeEvent {
        review_output: Some(actual),
        ..
    })) = decoded.item
    else {
        panic!("expected review-output rollout item");
    };
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn applies_value_limit_to_raw_value_wrapper() -> serde_json::Result<()> {
    let encoded =
        format!(r#"{{"method":"raw","params":{{"{SERDE_JSON_RAW_VALUE_TOKEN}":"[0,1]"}}}}"#);
    let actual = serde_json::from_str::<JSONRPCMessage>(&encoded)?;
    let expected = JSONRPCMessage::Notification(JSONRPCNotification {
        method: "raw".to_string(),
        params: Some(json!([0, 1])),
    });
    assert_eq!(actual, expected);

    let mut wrapped = String::with_capacity(2 * MAX_JSONRPC_VALUE_NODES + 1);
    wrapped.push('[');
    for index in 0..MAX_JSONRPC_VALUE_NODES {
        if index != 0 {
            wrapped.push(',');
        }
        wrapped.push('0');
    }
    wrapped.push(']');
    let encoded =
        format!(r#"{{"method":"raw","params":{{"{SERDE_JSON_RAW_VALUE_TOKEN}":"{wrapped}"}}}}"#);

    let error = serde_json::from_str::<JSONRPCMessage>(&encoded)
        .expect_err("raw value wrapper should not bypass the JSON value limit");
    let expected_error = format!("exceeds the limit of {MAX_JSONRPC_VALUE_NODES} JSON values");
    assert!(
        error.to_string().contains(&expected_error),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn accepts_large_scalar_payload() -> serde_json::Result<()> {
    let expected = JSONRPCMessage::Notification(JSONRPCNotification {
        method: "large".to_string(),
        params: Some(json!({"data": "x".repeat(MAX_JSONRPC_VALUE_NODES + 1)})),
    });

    let encoded = serde_json::to_string(&expected)?;
    let actual = serde_json::from_str::<JSONRPCMessage>(&encoded)?;

    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn rejects_duplicate_object_keys() {
    let error = serde_json::from_str::<JSONRPCMessage>(r#"{"method":"safe","method":"dangerous"}"#)
        .expect_err("duplicate JSON object keys should be rejected");

    assert!(
        error
            .to_string()
            .contains("duplicate JSON object key `method`"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_compact_array_heap_amplification() {
    const REPRO_VALUE_COUNT: usize = 2_097_137;
    const REPRO_MESSAGE_BYTES: usize = 4_194_303;

    let mut encoded = String::with_capacity(REPRO_MESSAGE_BYTES);
    encoded.push_str(r#"{"method":"probe","params":["#);
    for index in 0..REPRO_VALUE_COUNT {
        if index != 0 {
            encoded.push(',');
        }
        encoded.push('0');
    }
    encoded.push_str("]}");
    assert_eq!(encoded.len(), REPRO_MESSAGE_BYTES);

    let error = serde_json::from_str::<JSONRPCMessage>(&encoded)
        .expect_err("amplification payload should exceed the JSON value limit");
    let expected_error = format!("exceeds the limit of {MAX_JSONRPC_VALUE_NODES} JSON values");
    assert!(
        error.to_string().contains(&expected_error),
        "unexpected error: {error}"
    );
}
