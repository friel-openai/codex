//! Covers failed-response classification and retry-delay parsing.

use super::*;
use assert_matches::assert_matches;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn usage_limit_type_preserves_plan_and_reset_details() {
    for code in [
        None,
        Some("cyber_policy"),
        Some("invalid_prompt"),
        Some("server_is_overloaded"),
        Some("rate_limit_exceeded"),
        Some("model_not_found"),
    ] {
        for resets_at in [4_102_444_800_i64, i64::MAX] {
            let response = json!({
                "error": {
                    "type": "usage_limit_reached",
                    "code": code,
                    "param": "model",
                    "plan_type": "pro",
                    "resets_at": resets_at,
                },
            });

            assert_matches!(
                parse_failed_response(Some(response)),
                ApiError::UsageLimitReached(UsageLimitReachedError {
                    plan_type: Some(plan_type),
                    resets_at: actual_resets_at,
                    limit_window_minutes: None,
                    rate_limits: None,
                    promo_message: None,
                    rate_limit_reached_type: None,
                }) if plan_type == PlanType::from_raw_value("pro")
                    && actual_resets_at == chrono::DateTime::from_timestamp(resets_at, 0)
            );
        }
    }
}

#[test]
fn context_and_quota_codes_take_precedence_over_usage_limit_type() {
    for code in [
        "context_length_exceeded",
        "insufficient_quota",
        "credit_balance_exhausted",
        "organization_spend_limit_exceeded",
        "project_spend_limit_exceeded",
        "usage_not_included",
    ] {
        let response = json!({
            "error": { "type": "usage_limit_reached", "code": code },
        });
        let error = parse_failed_response(Some(response));

        match code {
            "context_length_exceeded" => assert_matches!(error, ApiError::ContextWindowExceeded),
            "usage_not_included" => assert_matches!(error, ApiError::UsageNotIncluded),
            _ => assert_matches!(error, ApiError::QuotaExceeded),
        }
    }
}

#[test]
fn test_try_parse_retry_delay() {
    let err = Error {
        r#type: None,
        message: Some("Rate limit reached for gpt-5.1 in organization org- on tokens per min (TPM): Limit 1, Used 1, Requested 19304. Please try again in 28ms. Visit https://platform.openai.com/account/rate-limits to learn more.".to_string()),
        code: Some("rate_limit_exceeded".to_string()),
        param: None,
        plan_type: None,
        resets_at: None,
        misalignment: None,
    };

    let delay = try_parse_retry_delay(&err);
    assert_eq!(delay, Some(Duration::from_millis(28)));
}

#[test]
fn test_try_parse_retry_delay_no_delay() {
    let err = Error {
        r#type: None,
        message: Some("Rate limit reached for gpt-5.1 in organization <ORG> on tokens per min (TPM): Limit 30000, Used 6899, Requested 24050. Please try again in 1.898s. Visit https://platform.openai.com/account/rate-limits to learn more.".to_string()),
        code: Some("rate_limit_exceeded".to_string()),
        param: None,
        plan_type: None,
        resets_at: None,
        misalignment: None,
    };
    let delay = try_parse_retry_delay(&err);
    assert_eq!(delay, Some(Duration::from_secs_f64(1.898)));
}

#[test]
fn test_try_parse_retry_delay_azure() {
    let err = Error {
        r#type: None,
        message: Some("Rate limit exceeded. Try again in 35 seconds.".to_string()),
        code: Some("rate_limit_exceeded".to_string()),
        param: None,
        plan_type: None,
        resets_at: None,
        misalignment: None,
    };
    let delay = try_parse_retry_delay(&err);
    assert_eq!(delay, Some(Duration::from_secs(35)));
}
