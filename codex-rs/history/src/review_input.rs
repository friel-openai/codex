use codex_protocol::models::ResponseItem;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::CodexHarnessMetadata;
use crate::GuardianHistoryCheckpoint;
use crate::ResponseItemEnvelope;

/// Reviewers that retained the original checkpoint transcript before suffix replay.
/// Session installation still checks compatibility with the current reviewer model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReviewTranscriptApplicability {
    Legacy,
    ThreadOwned,
    Both,
}

/// Transcript-only inputs stored once in an immutable migration segment.
/// A finite prefix ends after a rollback; none of these records enters model or UI history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReviewInputRecord {
    Baseline {
        applicability: ReviewTranscriptApplicability,
        history: GuardianHistoryCheckpoint,
    },
    ResponseItem {
        #[serde(with = "response_envelope")]
        #[schemars(with = "ReviewResponseItemWire")]
        response: ResponseItemEnvelope,
    },
    Rollback {
        /// Original instruction boundary, including anonymous historical identity.
        boundary: ResponseItem,
    },
}

#[derive(Deserialize, JsonSchema)]
struct ReviewResponseItemWire {
    item: ResponseItem,
    #[serde(default)]
    metadata: Option<CodexHarnessMetadata>,
}

mod response_envelope {
    use super::*;

    pub(super) fn serialize<S>(
        response: &ResponseItemEnvelope,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct BorrowedResponse<'a> {
            item: &'a ResponseItem,
            #[serde(skip_serializing_if = "Option::is_none")]
            metadata: Option<&'a CodexHarnessMetadata>,
        }
        BorrowedResponse {
            item: &response.item,
            metadata: response.metadata.as_ref(),
        }
        .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<ResponseItemEnvelope, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let response = ReviewResponseItemWire::deserialize(deserializer)?;
        Ok(ResponseItemEnvelope {
            item: response.item,
            metadata: response.metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn review_input_records_round_trip_without_losing_harness_metadata() {
        let boundary: ResponseItem = serde_json::from_value(json!({
            "type": "message", "role": "user",
            "content": [{"type": "input_text", "text": "anonymous instruction"}],
        }))
        .unwrap();
        let output: ResponseItem = serde_json::from_value(json!({
            "type": "function_call_output", "call_id": "call-1", "output": "original output",
        }))
        .unwrap();
        let records = vec![
            ReviewInputRecord::Baseline {
                applicability: ReviewTranscriptApplicability::Both,
                history: GuardianHistoryCheckpoint(vec![boundary.clone()]),
            },
            ReviewInputRecord::ResponseItem {
                response: ResponseItemEnvelope {
                    item: output,
                    metadata: Some(CodexHarnessMetadata {
                        history_truncation_token_limit: Some(128),
                        user_input_order: Some(7),
                        ..Default::default()
                    }),
                },
            },
            ReviewInputRecord::Rollback { boundary },
        ];
        let serialized = serde_json::to_value(&records).unwrap();
        assert_eq!(
            serialized[1]["response"]["metadata"]["fallback_token_limit_override"],
            json!(128)
        );
        assert_eq!(
            serde_json::from_value::<Vec<ReviewInputRecord>>(serialized).unwrap(),
            records,
        );
    }
}
