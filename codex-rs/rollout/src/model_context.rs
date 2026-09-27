//! Collects persisted rollout items needed to reconstruct the most recent context window.
//!
//! Storage readers feed items newest-to-oldest. Modern compactions persist resume metadata;
//! older compactions also need a surviving turn-context baseline. Items are returned in
//! chronological order, and readers prepend their canonical session metadata.

use crate::ResponseItemEnvelope;
use crate::RolloutItem;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;

/// Whether a reverse model-context scan needs more rollout items.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelContextScanProgress {
    /// The reader should provide the next older rollout item.
    Continue,
    /// The scan has collected a safe bounded suffix.
    Complete,
}

/// Finds a bounded suffix for reconstructing the most recent context window.
///
/// Replacement history and a window number bound model history. Explicit resume metadata also
/// bounds previous-turn settings; legacy compactions require a surviving turn-context baseline.
/// A rollback or compaction missing replacement history or a window number requires full replay.
#[derive(Debug, Default)]
pub struct ModelContextScan {
    items_newest_first: Vec<RolloutItem>,
    saw_compaction: bool,
    saw_resume_metadata: bool,
    saw_completed_turn_context: bool,
    must_scan_to_start: bool,
    active_segment: ActiveTurnSegment,
}

impl ModelContextScan {
    /// Adds the next newest-to-oldest rollout item and reports whether the reader can stop.
    pub fn push(&mut self, item: RolloutItem) -> ModelContextScanProgress {
        let progress = self.observe(&item);
        self.items_newest_first.push(item);
        progress
    }

    /// Returns the collected items in chronological order.
    ///
    /// Call this after the reader reaches the beginning of its source or after [`Self::push`]
    /// returns [`ModelContextScanProgress::Complete`].
    pub fn finish(mut self) -> Vec<RolloutItem> {
        self.items_newest_first.reverse();
        self.items_newest_first
    }

    fn observe(&mut self, item: &RolloutItem) -> ModelContextScanProgress {
        if self.must_scan_to_start {
            return ModelContextScanProgress::Continue;
        }

        match item {
            RolloutItem::Compacted(compacted)
                if compacted.replacement_history.is_none() || compacted.window_number.is_none() =>
            {
                self.must_scan_to_start = true;
            }
            RolloutItem::Compacted(compacted) => {
                if !self.saw_compaction {
                    self.saw_resume_metadata = compacted.resume_metadata.is_some();
                }
                self.saw_compaction = true;
                self.active_segment.saw_compaction = true;
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_)) => {
                // Rollback markers only occur in legacy history. Keep old rollouts correct rather than
                // duplicating rollback survival semantics in this bounded selector.
                self.must_scan_to_start = true;
            }
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => {
                if self.active_segment.turn_id.is_none() {
                    self.active_segment.turn_id = Some(event.turn_id.clone());
                }
                if turn_ids_are_compatible(
                    self.active_segment.turn_id.as_deref(),
                    Some(event.turn_id.as_str()),
                ) {
                    self.active_segment.has_user_turn |=
                        matches!(&event.item, TurnItem::UserMessage(_));
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                self.active_segment
                    .turn_id
                    .get_or_insert_with(|| event.turn_id.clone());
            }
            RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                if let Some(turn_id) = &event.turn_id {
                    self.active_segment
                        .turn_id
                        .get_or_insert_with(|| turn_id.clone());
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                if turn_ids_are_compatible(
                    self.active_segment.turn_id.as_deref(),
                    Some(event.turn_id.as_str()),
                ) {
                    self.finalize_active_segment();
                }
            }
            RolloutItem::TurnContext(context) => {
                if self.active_segment.turn_id.is_none() {
                    self.active_segment.turn_id = context.turn_id.clone();
                }
                if turn_ids_are_compatible(
                    self.active_segment.turn_id.as_deref(),
                    context.turn_id.as_deref(),
                ) {
                    self.active_segment.has_turn_context = true;
                }
            }
            RolloutItem::ResponseItem(response_item) => {
                self.active_segment.has_user_turn |=
                    response_item_counts_as_user_turn(response_item);
            }
            RolloutItem::InterAgentCommunication(_) => {
                self.active_segment.has_user_turn = true;
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                self.active_segment.has_user_turn = true;
            }
            RolloutItem::WorldState(state) => {
                self.active_segment.has_full_world_state |=
                    state.full && !self.active_segment.saw_compaction;
            }
            RolloutItem::EventMsg(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::SessionMeta(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::RealtimeItem(_)
            | RolloutItem::RetainedContext(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::TokenUsageRecord(_) => {}
        }

        if self.has_bounded_cutoff() {
            ModelContextScanProgress::Complete
        } else {
            ModelContextScanProgress::Continue
        }
    }

    fn finalize_active_segment(&mut self) {
        if self.active_segment.has_turn_context
            && (self.active_segment.has_user_turn || self.active_segment.has_full_world_state)
        {
            self.saw_completed_turn_context = true;
        }
        self.active_segment = ActiveTurnSegment::default();
    }

    fn has_bounded_cutoff(&self) -> bool {
        !self.must_scan_to_start
            && self.saw_compaction
            && (self.saw_resume_metadata || self.saw_completed_turn_context)
    }
}

/// Resume baselines accumulated within one surviving turn during reverse scanning.
#[derive(Debug, Default)]
struct ActiveTurnSegment {
    turn_id: Option<String>,
    has_user_turn: bool,
    has_turn_context: bool,
    has_full_world_state: bool,
    saw_compaction: bool,
}

fn turn_ids_are_compatible(active_turn_id: Option<&str>, item_turn_id: Option<&str>) -> bool {
    active_turn_id
        .is_none_or(|turn_id| item_turn_id.is_none_or(|item_turn_id| item_turn_id == turn_id))
}

fn response_item_counts_as_user_turn(response_item: &ResponseItemEnvelope) -> bool {
    match &response_item.item {
        ResponseItem::AgentMessage { .. } => true,
        ResponseItem::Message { role, content, .. } => {
            role == "assistant" && InterAgentCommunication::is_message_content(content)
        }
        _ => false,
    }
}
