use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::models::ContentItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::CompactedItem;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::SegmentStateCheckpointDisposition;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::WorldStateItem;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::CertifiedSegmentStateCheckpoint;
use crate::ModelContextScan;
use crate::ModelContextScanProgress;

fn compacted() -> CompactedItem {
    CompactedItem {
        message: "checkpoint".to_string(),
        replacement_history: Some(vec![ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "replacement".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }]),
        window_number: Some(3),
        first_window_id: Some("019b3f6e-0000-7000-8000-000000000001".to_string()),
        previous_window_id: Some("019b3f6e-0000-7000-8000-000000000002".to_string()),
        window_id: Some("019b3f6e-0000-7000-8000-000000000003".to_string()),
        segment_state_checkpoint: None,
    }
}

fn thread_settings() -> ThreadSettingsAppliedEvent {
    ThreadSettingsAppliedEvent {
        thread_settings: ThreadSettingsSnapshot {
            model: "gpt-test".to_string(),
            model_provider_id: "test-provider".to_string(),
            service_tier: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: ApprovalsReviewer::User,
            permission_profile: PermissionProfile::workspace_write(),
            active_permission_profile: None,
            cwd: serde_json::from_value(json!("/tmp")).expect("absolute test cwd"),
            environments: Some(TurnEnvironmentSelections::new(
                serde_json::from_value(json!("/tmp")).expect("absolute test cwd"),
                Vec::new(),
            )),
            workspace_roots: Some(Vec::new()),
            profile_workspace_roots: Some(Vec::new()),
            windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
            reasoning_effort: None,
            reasoning_summary: None,
            personality: None,
            collaboration_mode: CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: "gpt-test".to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            },
        },
    }
}

fn token_count() -> TokenCountEvent {
    TokenCountEvent {
        info: None,
        rate_limits: None,
    }
}

#[test]
fn cleared_checkpoint_is_immediately_bounded() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        Some(SegmentPreviousTurnSettings {
            model: "gpt-test".to_string(),
            comp_hash: Some("hash".to_string()),
            realtime_active: Some(false),
        }),
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let [
        RolloutItem::Compacted(compacted),
        RolloutItem::EventMsg(_),
        RolloutItem::EventMsg(_),
    ] = checkpoint.items()
    else {
        panic!("cleared checkpoint should contain compaction and current-state records");
    };
    let descriptor = compacted
        .segment_state_checkpoint
        .as_ref()
        .expect("checkpoint descriptor");
    assert_eq!(
        (
            descriptor.world_state,
            descriptor.reference_context,
            descriptor.previous_turn_settings.clone(),
        ),
        (
            SegmentStateCheckpointDisposition::Cleared,
            SegmentStateCheckpointDisposition::Cleared,
            Some(SegmentPreviousTurnSettings {
                model: "gpt-test".to_string(),
                comp_hash: Some("hash".to_string()),
                realtime_active: Some(false),
            }),
        )
    );

    let mut scan = ModelContextScan::default();
    let progress = checkpoint
        .items()
        .iter()
        .rev()
        .cloned()
        .map(|item| scan.push(item))
        .last()
        .expect("checkpoint items");
    assert_eq!(progress, ModelContextScanProgress::Complete);
    assert!(scan.completed_at_segment_checkpoint());
}

#[test]
fn full_world_state_must_be_adjacent_to_checkpoint() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        Some(WorldStateItem::full(
            json!({"environment": {"cwd": "/tmp"}}),
        )),
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let items = checkpoint.into_items();

    let mut complete_scan = ModelContextScan::default();
    let progress = items
        .iter()
        .rev()
        .cloned()
        .map(|item| complete_scan.push(item))
        .last()
        .expect("checkpoint items");
    assert_eq!(progress, ModelContextScanProgress::Complete);
    assert!(complete_scan.completed_at_segment_checkpoint());

    let mut incomplete_scan = ModelContextScan::default();
    let progress = items
        .iter()
        .filter(|item| !matches!(item, RolloutItem::WorldState(_)))
        .rev()
        .cloned()
        .map(|item| incomplete_scan.push(item))
        .last()
        .expect("checkpoint items without world state");
    assert_eq!(progress, ModelContextScanProgress::Continue);
    assert!(!incomplete_scan.completed_at_segment_checkpoint());
}

#[test]
fn checkpoint_requires_thread_settings_and_token_count() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint")
    .into_items();

    for missing_index in [1, 2] {
        let mut incomplete = checkpoint.clone();
        incomplete.remove(missing_index);
        let mut scan = ModelContextScan::default();
        let progress = incomplete
            .into_iter()
            .rev()
            .map(|item| scan.push(item))
            .last()
            .expect("incomplete checkpoint items");
        assert_eq!(progress, ModelContextScanProgress::Continue);
        assert!(!scan.completed_at_segment_checkpoint());
    }

    let mut incomplete_settings = Vec::new();
    let mut missing_environments = thread_settings();
    missing_environments.thread_settings.environments = None;
    incomplete_settings.push(missing_environments);
    let mut missing_workspace_roots = thread_settings();
    missing_workspace_roots.thread_settings.workspace_roots = None;
    incomplete_settings.push(missing_workspace_roots);
    let mut missing_profile_workspace_roots = thread_settings();
    missing_profile_workspace_roots
        .thread_settings
        .profile_workspace_roots = None;
    incomplete_settings.push(missing_profile_workspace_roots);
    let mut missing_windows_sandbox_level = thread_settings();
    missing_windows_sandbox_level
        .thread_settings
        .windows_sandbox_level = None;
    incomplete_settings.push(missing_windows_sandbox_level);
    for incomplete_settings in incomplete_settings {
        assert!(
            CertifiedSegmentStateCheckpoint::new(
                compacted(),
                /*previous_turn_settings*/ None,
                /*world_state*/ None,
                /*reference_context*/ None,
                incomplete_settings,
                token_count(),
            )
            .is_err()
        );
    }

    let mut invalid_reader_items = checkpoint;
    let RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ThreadSettingsApplied(event)) =
        &mut invalid_reader_items[1]
    else {
        panic!("checkpoint settings event");
    };
    event.thread_settings.environments = None;
    let mut scan = ModelContextScan::default();
    let progress = invalid_reader_items
        .into_iter()
        .rev()
        .map(|item| scan.push(item))
        .last()
        .expect("invalid checkpoint items");
    assert_eq!(progress, ModelContextScanProgress::Continue);
    assert!(!scan.completed_at_segment_checkpoint());
}

#[test]
fn non_object_full_world_state_falls_back_to_older_history() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        Some(WorldStateItem::full(
            json!({"environment": {"cwd": "/tmp"}}),
        )),
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let mut items = checkpoint.into_items();
    let RolloutItem::WorldState(world_state) = &mut items[1] else {
        panic!("checkpoint full world state");
    };
    world_state.state = json!(["not", "a", "snapshot"]);

    let mut scan = ModelContextScan::default();
    let progress = items
        .into_iter()
        .rev()
        .map(|item| scan.push(item))
        .last()
        .expect("checkpoint items");
    assert_eq!(progress, ModelContextScanProgress::Continue);
    assert!(!scan.completed_at_segment_checkpoint());
}

#[test]
fn unsupported_checkpoint_version_falls_back_to_older_history() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let mut items = checkpoint.into_items();
    let RolloutItem::Compacted(compacted) = &mut items[0] else {
        panic!("checkpoint compaction");
    };
    compacted
        .segment_state_checkpoint
        .as_mut()
        .expect("checkpoint descriptor")
        .version = 999;

    let mut scan = ModelContextScan::default();
    let progress = items
        .into_iter()
        .rev()
        .map(|item| scan.push(item))
        .last()
        .expect("checkpoint items");
    assert_eq!(progress, ModelContextScanProgress::Continue);
}

#[test]
fn non_v7_window_identifier_falls_back_to_older_history() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let mut items = checkpoint.into_items();
    let RolloutItem::Compacted(compacted) = &mut items[0] else {
        panic!("checkpoint compaction");
    };
    compacted.window_id = Some("550e8400-e29b-41d4-a716-446655440000".to_string());

    let mut scan = ModelContextScan::default();
    let progress = items
        .into_iter()
        .rev()
        .map(|item| scan.push(item))
        .last()
        .expect("checkpoint items");
    assert_eq!(progress, ModelContextScanProgress::Continue);
    assert!(!scan.completed_at_segment_checkpoint());
}
