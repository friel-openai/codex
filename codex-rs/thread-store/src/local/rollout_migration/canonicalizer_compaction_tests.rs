use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_protocol::protocol::SegmentStateCheckpoint;
use codex_protocol::protocol::SegmentStateCheckpointDisposition;
use codex_rollout::CompactedItem;
use codex_rollout::CompactionResumeMetadata;
use pretty_assertions::assert_eq;

use super::*;

fn checkpoint() -> CompactedItem {
    CompactedItem {
        message: "Visible compaction".to_string(),
        replacement_history: Some(Vec::new()),
        retained_context: None,
        retained_context_replay: None,
        guardian_history: None,
        mcp_resource_origins: None,
        window_number: Some(1),
        first_window_id: None,
        previous_window_id: None,
        window_id: None,
        compaction_response_id: None,
        latest_token_usage_record: None,
        resume_metadata: Some(CompactionResumeMetadata {
            multi_agent_version: None,
            last_started_turn_id: Some("parent".to_string()),
            turn_attribution: None,
            previous_turn_settings: None,
        }),
        segment_state_checkpoint: None,
    }
}

fn started() -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: "parent".to_string(),
        root_turn_id: None,
        turn_attribution: None,
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn completed() -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: "parent".to_string(),
        root_turn_id: None,
        last_agent_message: None,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
    }))
}

async fn canonicalize(items: &[RolloutItem]) -> Vec<RolloutItem> {
    let mut writer = Vec::new();
    let mut canonicalizer = LegacyRolloutCanonicalizer::new(ThreadId::new());
    let timestamp = "2025-01-03T12:00:00Z";
    for item in items {
        canonicalizer
            .process_line(
                RolloutLine {
                    timestamp: timestamp.to_string(),
                    ordinal: None,
                    item: item.clone(),
                },
                &mut writer,
            )
            .await
            .expect("canonicalize source record");
    }
    canonicalizer
        .finish(&mut writer, timestamp)
        .await
        .expect("finish canonical history");
    writer
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            codex_rollout::decode_rollout_line(serde_json::from_slice(line).expect("JSON record"))
                .expect("canonical record")
                .item
        })
        .collect()
}

#[tokio::test]
async fn visible_modern_compaction_preserves_post_completion_desktop_turn() {
    let checkpoint = checkpoint();
    let source = vec![
        started(),
        completed(),
        RolloutItem::Compacted(checkpoint.clone()),
    ];
    let output = canonicalize(&source).await;
    let mut native = ThreadHistoryBuilder::new();
    for item in &output {
        native.handle_paginated_rollout_item(item);
    }
    assert_eq!(native.finish(), build_turns_from_rollout_items(&source));
    assert!(output.iter().any(|item| matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(event)) if event.turn_id == "rollout-2")));
    assert_eq!(
        output.iter().find_map(|item| match item {
            RolloutItem::Compacted(item) => Some(item),
            _ => None,
        }),
        Some(&checkpoint),
    );
}

#[tokio::test]
async fn visible_modern_compaction_in_turn_does_not_add_a_turn() {
    let source = vec![started(), RolloutItem::Compacted(checkpoint()), completed()];
    let output = canonicalize(&source).await;
    assert_eq!(
        serde_json::to_value(output).expect("canonical items"),
        serde_json::to_value(source).expect("original items")
    );
}

#[tokio::test]
async fn state_only_and_unproven_checkpoints_do_not_synthesize_lifecycle_records() {
    let mut state_only = checkpoint();
    state_only.message.clear();
    let mut legacy = checkpoint();
    legacy.resume_metadata = None;
    let mut invalid_certificate = checkpoint();
    invalid_certificate.segment_state_checkpoint = Some(SegmentStateCheckpoint {
        version: u32::MAX,
        previous_turn_settings: None,
        world_state: SegmentStateCheckpointDisposition::Cleared,
        reference_context: SegmentStateCheckpointDisposition::Cleared,
    });
    let mut certified_state_only = invalid_certificate.clone();
    certified_state_only.message.clear();
    for checkpoint in [
        state_only,
        legacy,
        invalid_certificate,
        certified_state_only,
    ] {
        let source = vec![RolloutItem::Compacted(checkpoint)];
        assert_eq!(
            serde_json::to_value(canonicalize(&source).await).expect("canonical items"),
            serde_json::to_value(source).expect("original items")
        );
    }
}
