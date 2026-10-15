//! Tests for cumulative MCP attribution restoration and checkpoint acknowledgements.

use super::*;
use codex_history::CodexHarnessMetadata;
use codex_history::CompactedItem;
use codex_history::ResponseItemEnvelope;
use codex_protocol::models::ResponseItem;
use pretty_assertions::assert_eq;

fn source(tool_name: &str, first_turn_id: &str) -> McpAttributionSource {
    McpAttributionSource {
        connector_id: None,
        plugin_id: None,
        server_name: "example".to_string(),
        tool_name: tool_name.to_string(),
        first_turn_id: first_turn_id.to_string(),
    }
}

fn envelope(attribution: Option<McpAttribution>) -> ResponseItemEnvelope {
    ResponseItemEnvelope {
        item: ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: Vec::new(),
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        metadata: attribution.map(|mcp_attribution| CodexHarnessMetadata {
            mcp_attribution: Some(mcp_attribution),
            ..Default::default()
        }),
    }
}

#[test]
fn records_the_first_turn_for_each_unique_source() {
    let recorder = McpAttributionRecorder::default();
    recorder.record(source("search", "turn_1"));
    recorder.record(source("search", "turn_2"));
    recorder.record(source("fetch", "turn_2"));

    assert_eq!(
        recorder.snapshot(),
        McpAttribution {
            status: McpAttributionStatus::Complete,
            error_reason: None,
            sources: vec![source("search", "turn_1"), source("fetch", "turn_2")],
        }
    );
}

#[test]
fn restores_cumulative_item_and_compaction_checkpoints() {
    let initial = McpAttribution {
        status: McpAttributionStatus::Complete,
        error_reason: None,
        sources: vec![source("search", "turn_1")],
    };
    let cumulative = McpAttribution {
        status: McpAttributionStatus::Complete,
        error_reason: None,
        sources: vec![source("search", "turn_1"), source("fetch", "turn_2")],
    };
    let history = InitialHistory::Forked(vec![
        RolloutItem::ResponseItem(envelope(Some(initial))),
        RolloutItem::Compacted(CompactedItem {
            message: "summary".to_string(),
            replacement_history: Some(vec![envelope(Some(cumulative.clone()))]),
            guardian_history: None,
            retained_context: None,
            retained_context_replay: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
    ]);

    assert_eq!(McpAttributionRecorder::new(&history).snapshot(), cumulative);

    let recorder = McpAttributionRecorder::new(&InitialHistory::Forked(Vec::new()));
    let shared = recorder.clone();
    recorder.restore_from_rollout_items(history.get_rollout_items());
    assert_eq!(shared.snapshot(), cumulative);
}

#[test]
fn startup_restoration_replaces_provisional_errors_and_keeps_source_identity() {
    for invalid_source in [false, true] {
        let expected = McpAttribution {
            status: if invalid_source {
                McpAttributionStatus::AttributionError
            } else {
                McpAttributionStatus::Complete
            },
            error_reason: invalid_source.then_some(McpAttributionErrorReason::SourceInvalid),
            sources: vec![McpAttributionSource {
                connector_id: Some("connector".to_string()),
                plugin_id: Some("messages@openai-bundled".to_string()),
                server_name: "messages".to_string(),
                tool_name: "search".to_string(),
                first_turn_id: "original-turn".to_string(),
            }],
        };
        let recorder = McpAttributionRecorder::new(&InitialHistory::Forked(Vec::new()));
        let shared = recorder.clone();
        assert_eq!(
            recorder.snapshot().error_reason,
            Some(McpAttributionErrorReason::HistoryMissingCheckpoint)
        );
        recorder.restore_from_response_items(&[envelope(Some(expected.clone()))]);
        assert_eq!(shared.snapshot(), expected);

        recorder.restore_from_rollout_items(&[]);
        recorder.restore_from_snapshot(&expected);
        assert_eq!(shared.snapshot(), expected);
        let (checkpoint, revision) = shared
            .checkpoint(/*force*/ false)
            .expect("restored startup state still needs persistence");
        assert_eq!(checkpoint, expected);
        shared.mark_persisted(revision);
        assert_eq!(recorder.checkpoint(/*force*/ false), None);
    }
}

#[test]
fn startup_restoration_distinguishes_explicit_empty_and_missing_checkpoints() {
    let recorder = McpAttributionRecorder::default();
    recorder.record(source("search", "old-turn"));
    let missing = McpAttribution {
        status: McpAttributionStatus::AttributionError,
        error_reason: Some(McpAttributionErrorReason::HistoryMissingCheckpoint),
        sources: Vec::new(),
    };

    recorder.restore_from_response_items(&[envelope(Some(McpAttribution::default()))]);
    assert_eq!(recorder.snapshot(), McpAttribution::default());
    recorder.restore_from_response_items(&[envelope(None)]);
    assert_eq!(recorder.snapshot(), missing);

    recorder.restore_from_rollout_items(&[RolloutItem::ResponseItem(envelope(Some(
        McpAttribution::default(),
    )))]);
    assert_eq!(recorder.snapshot(), McpAttribution::default());
    recorder.restore_from_rollout_items(&[RolloutItem::ResponseItem(envelope(None))]);
    assert_eq!(recorder.snapshot(), missing);

    recorder.restore_from_snapshot(&McpAttribution::default());
    assert_eq!(recorder.snapshot(), McpAttribution::default());
    recorder.restore_from_response_items(&[]);
    assert_eq!(recorder.snapshot(), missing);
}

#[test]
fn startup_checkpoint_restoration_keeps_validation_and_first_error() {
    let recorder = McpAttributionRecorder::default();
    recorder.restore_from_response_items(&[
        envelope(Some(McpAttribution {
            status: McpAttributionStatus::Complete,
            error_reason: None,
            sources: vec![source("search", "first-turn")],
        })),
        envelope(Some(McpAttribution {
            status: McpAttributionStatus::Complete,
            error_reason: None,
            sources: vec![source("search", "conflicting-turn")],
        })),
        envelope(Some(McpAttribution {
            status: McpAttributionStatus::AttributionError,
            error_reason: Some(McpAttributionErrorReason::SourceInvalid),
            sources: Vec::new(),
        })),
    ]);
    assert_eq!(
        recorder.snapshot(),
        McpAttribution {
            status: McpAttributionStatus::AttributionError,
            error_reason: Some(McpAttributionErrorReason::CheckpointSourceConflict),
            sources: vec![source("search", "first-turn")],
        }
    );
}

#[test]
fn startup_restoration_preserves_shared_execution_trackers() {
    let mut features = codex_features::Features::default();
    features.enable(codex_features::Feature::ExecutedToolCallMetadata);
    let recorder =
        super::super::ExecutedToolCalls::new(&features, &InitialHistory::Forked(Vec::new()));
    let shared = recorder.clone();
    let recording = {
        let mut state = recorder.lock_state();
        let state = state.as_mut().expect("execution metadata enabled");
        state
            .pending_wrapper_origins
            .insert("pending-call".to_string());
        Arc::clone(&state.recording)
    };
    recorder
        .retained_direct_metadata_bytes
        .store(17, std::sync::atomic::Ordering::Relaxed);
    let expected = McpAttribution {
        status: McpAttributionStatus::Complete,
        error_reason: None,
        sources: vec![source("search", "original-turn")],
    };

    for restore in 0..3 {
        match restore {
            0 => recorder.restore_mcp_attribution_from_snapshot(&expected),
            1 => recorder.restore_mcp_attribution_from_rollout_items(&[RolloutItem::ResponseItem(
                envelope(Some(expected.clone())),
            )]),
            _ => recorder
                .restore_mcp_attribution_from_response_items(&[envelope(Some(expected.clone()))]),
        }
        assert_eq!(shared.mcp_attribution_snapshot(), expected);
        assert_eq!(
            shared
                .retained_direct_metadata_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            17
        );
        let state = shared.lock_state();
        let state = state.as_ref().expect("execution metadata retained");
        assert!(Arc::ptr_eq(&state.recording, &recording));
        assert!(state.pending_wrapper_origins.contains("pending-call"));
    }
}

#[test]
fn legacy_or_conflicting_history_is_not_complete() {
    let legacy = InitialHistory::Forked(vec![RolloutItem::ResponseItem(envelope(
        /*attribution*/ None,
    ))]);
    assert_eq!(
        McpAttributionRecorder::new(&legacy).snapshot(),
        McpAttribution {
            status: McpAttributionStatus::AttributionError,
            error_reason: Some(McpAttributionErrorReason::HistoryMissingCheckpoint),
            sources: Vec::new(),
        }
    );

    let history = InitialHistory::Forked(
        ["turn_1", "turn_2"]
            .map(|turn_id| {
                RolloutItem::ResponseItem(envelope(Some(McpAttribution {
                    status: McpAttributionStatus::Complete,
                    error_reason: None,
                    sources: vec![source("search", turn_id)],
                })))
            })
            .to_vec(),
    );
    assert_eq!(
        McpAttributionRecorder::new(&history).snapshot(),
        McpAttribution {
            status: McpAttributionStatus::AttributionError,
            error_reason: Some(McpAttributionErrorReason::CheckpointSourceConflict),
            sources: vec![source("search", "turn_1")],
        }
    );
}

#[test]
fn first_error_reason_survives_checkpoints_and_later_sources() {
    let recorder = McpAttributionRecorder::default();
    recorder.record(source("", "turn_1"));
    recorder.record(source("search", "turn_2"));
    let (snapshot, _) = recorder
        .checkpoint(/*force*/ true)
        .expect("error checkpoint");
    let checkpoint = envelope(Some(snapshot));
    let mut compacted: CompactedItem =
        serde_json::from_value(serde_json::json!({"message": "summary"}))
            .expect("compaction checkpoint");
    compacted.replacement_history = Some(vec![checkpoint.clone()]);
    for item in [
        RolloutItem::ResponseItem(checkpoint),
        RolloutItem::Compacted(compacted),
    ] {
        let restored = McpAttributionRecorder::new(&InitialHistory::Forked(vec![item]));
        restored.record(source("", "turn_3"));

        assert_eq!(
            restored.snapshot(),
            McpAttribution {
                status: McpAttributionStatus::AttributionError,
                error_reason: Some(McpAttributionErrorReason::SourceInvalid),
                sources: vec![source("search", "turn_2")],
            },
        );
    }
}

#[test]
fn poisoned_recorder_reports_a_bounded_reason() {
    let recorder = McpAttributionRecorder::default();
    let poisoned = recorder.clone();
    assert!(
        std::thread::spawn(move || {
            let _state = poisoned.0.lock().expect("unpoisoned recorder");
            panic!("poison recorder");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        recorder.snapshot(),
        McpAttribution {
            status: McpAttributionStatus::AttributionError,
            error_reason: Some(McpAttributionErrorReason::RecorderPoisoned),
            sources: Vec::new(),
        }
    );
}

#[test]
fn restored_diagnostics_do_not_change_attribution_state() {
    let checkpoint = envelope(Some(McpAttribution {
        status: McpAttributionStatus::Complete,
        error_reason: Some(McpAttributionErrorReason::SourceInvalid),
        sources: vec![source("search", "turn_1")],
    }));
    assert_eq!(
        McpAttributionRecorder::new(&InitialHistory::Forked(vec![RolloutItem::ResponseItem(
            checkpoint
        ),]))
        .snapshot(),
        McpAttribution {
            status: McpAttributionStatus::Complete,
            error_reason: None,
            sources: vec![source("search", "turn_1")],
        }
    );

    for (attribution, expected_reason) in [
        (
            McpAttribution {
                status: McpAttributionStatus::AttributionError,
                error_reason: None,
                sources: Vec::new(),
            },
            McpAttributionErrorReason::RestoredErrorUnknown,
        ),
        (
            McpAttribution {
                status: McpAttributionStatus::Complete,
                error_reason: None,
                sources: Vec::new(),
            },
            McpAttributionErrorReason::CheckpointInvalid,
        ),
    ] {
        assert_eq!(
            McpAttributionRecorder::new(&InitialHistory::Forked(vec![RolloutItem::ResponseItem(
                envelope(Some(attribution))
            ),]))
            .snapshot(),
            McpAttribution {
                status: McpAttributionStatus::AttributionError,
                error_reason: Some(expected_reason),
                sources: Vec::new(),
            }
        );
    }
}

#[test]
fn acknowledging_an_older_checkpoint_does_not_clear_newer_changes() {
    let recorder = McpAttributionRecorder::default();
    let (_, initial_revision) = recorder
        .checkpoint(/*force*/ false)
        .expect("initial checkpoint");
    recorder.record(source("search", "turn_1"));
    recorder.mark_persisted(initial_revision);

    let (_, latest_revision) = recorder
        .checkpoint(/*force*/ false)
        .expect("dirty checkpoint");
    recorder.mark_persisted(latest_revision);
    assert_eq!(recorder.checkpoint(/*force*/ false), None);
    assert!(recorder.checkpoint(/*force*/ true).is_some());
}
