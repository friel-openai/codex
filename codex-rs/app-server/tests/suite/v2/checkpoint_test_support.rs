use app_test_support::test_thread_settings_snapshot;
use codex_protocol::protocol::CompactedItem;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::protocol::WorldStateItem;
use codex_rollout::CertifiedSegmentStateCheckpoint;
use codex_rollout::SegmentStateCheckpointError;

pub(super) fn test_segment_state_checkpoint(
    compacted: CompactedItem,
    previous_turn_settings: Option<SegmentPreviousTurnSettings>,
    world_state: Option<WorldStateItem>,
    reference_context: Option<TurnContextItem>,
) -> Result<CertifiedSegmentStateCheckpoint, SegmentStateCheckpointError> {
    test_segment_state_checkpoint_with_current_state(
        compacted,
        previous_turn_settings,
        world_state,
        reference_context,
        ThreadSettingsAppliedEvent {
            thread_settings: test_thread_settings_snapshot(),
        },
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
}

pub(super) fn test_segment_state_checkpoint_with_current_state(
    compacted: CompactedItem,
    previous_turn_settings: Option<SegmentPreviousTurnSettings>,
    world_state: Option<WorldStateItem>,
    reference_context: Option<TurnContextItem>,
    thread_settings: ThreadSettingsAppliedEvent,
    token_count: TokenCountEvent,
) -> Result<CertifiedSegmentStateCheckpoint, SegmentStateCheckpointError> {
    CertifiedSegmentStateCheckpoint::new(
        compacted,
        previous_turn_settings,
        world_state,
        reference_context,
        thread_settings,
        token_count,
    )
}
