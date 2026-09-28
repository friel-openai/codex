//! Preserves an authoritative modern checkpoint when migration removes rollback records.

use std::collections::HashMap;
use std::sync::Arc;

use codex_guardian_context::SectionHistory;
use codex_guardian_context::TranscriptHistory;
use codex_prompts::SUMMARY_PREFIX;
use codex_protocol::ResponseItemId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TokenUsageRecord;
use codex_rollout::CompactedItem;
use codex_rollout::CompactionCheckpoint;
use codex_rollout::CompactionResumeMetadata;
use codex_rollout::GuardianHistoryCheckpoint;
use codex_rollout::MAX_RETAINED_USER_MESSAGE_TOKENS;
use codex_rollout::ReconciledRetainedContext;
use codex_rollout::ResponseItemEnvelope;
use codex_rollout::RetainedContext;
use codex_rollout::RetainedContextReplay;
use codex_rollout::RetainedInputSource;
use codex_rollout::RetainedMessageSource;
use codex_rollout::RetainedUserMessage;
use codex_rollout::ReviewInputRecord;
use codex_rollout::ReviewTranscriptApplicability;
use codex_rollout::RolloutItem;
use codex_rollout::UserInputOrigin;
use codex_rollout::checkpoint_requires_parent_context;
use codex_rollout::is_api_message;
use codex_rollout::is_user_authorization_message;
use codex_rollout::record_retained_message;
use codex_rollout::truncate_text;
use codex_rollout::user_message_input;

use super::rollback;

/// The frozen migration equivalent of core's Guardian transcript exclusion predicate.
fn is_guardian_context_message(item: &ResponseItem) -> bool {
    let ResponseItem::Message {
        role,
        content,
        internal_chat_message_metadata_passthrough,
        ..
    } = item
    else {
        return false;
    };
    if role != "user" {
        return false;
    }
    let kinds = internal_chat_message_metadata_passthrough
        .as_ref()
        .and_then(|metadata| metadata.content_item_kinds.as_deref());
    if let Some([kind]) = kinds
        && matches!(kind.0.as_str(), "user.goal" | "user.goal.omitted")
        && matches!(content.as_slice(), [ContentItem::InputText { .. }])
    {
        return false;
    }
    rollback::is_pre_turn_context_update(item)
        || kinds.is_some_and(|kinds| {
            !kinds.is_empty()
                && kinds.len() == content.len()
                && kinds
                    .iter()
                    .all(|kind| kind.0 == "guardian.retained_instructions")
        })
}

/// Match core's legacy instruction candidates without interpreting summary text as authorization.
fn legacy_user_message(item: &ResponseItem) -> Option<RetainedUserMessage> {
    if !is_user_authorization_message(item) || rollback::is_pre_turn_context_update(item) {
        return None;
    }
    let ResponseItem::Message { content, .. } = item else {
        return None;
    };
    let text = UserMessageItem::new(&user_message_input(content)).message();
    if text
        .strip_prefix(SUMMARY_PREFIX)
        .is_some_and(|suffix| suffix.starts_with('\n'))
        || text.trim_start().starts_with("<user_action>")
        || rollback::is_known_contextual_user_text(&text)
    {
        return None;
    }
    Some(RetainedUserMessage {
        phase: None,
        origin: UserInputOrigin::from_message(item),
        turn_id: item.turn_id().unwrap_or_default().to_owned(),
        message_id: item.id().map(|id| id.as_str().to_owned()),
        text: truncate_text(&text, MAX_RETAINED_USER_MESSAGE_TOKENS),
        complete: false,
    })
}

/// Checkpoint contents to publish at one removed rollback record.
pub(super) struct ModernRollbackSnapshot {
    /// Removals from the original replacement history, in chronological order.
    pub(super) rollback_turns: Vec<u32>,
    /// Prefix of the base checkpoint's retained-context edits at this rollback.
    pub(super) retained_context_edit_count: usize,
    resume_metadata: CompactionResumeMetadata,
    latest_token_usage_record: Option<TokenUsageRecord>,
    /// ThreadOwned state after replaying the removed source records and rollback.
    pub(super) retained_context: RetainedContext,
    /// Resume may select another review mode or promote a worker to a root.
    pub(super) retained_context_replay: RetainedContextReplay,
    /// Exclusive count of newly captured review inputs through this rollback.
    pub(super) review_input_end: Option<usize>,
}

/// One final compaction whose post-rollback state can be represented without replaying model items.
pub(super) struct ModernRollbackPlan {
    pub(super) checkpoint_index: usize,
    pub(super) base: Arc<CompactedItem>,
    pub(super) snapshots: HashMap<usize, ModernRollbackSnapshot>,
    /// A prior immutable prefix is copied once when a migrated checkpoint is migrated again.
    pub(super) review_input_base: Option<HistoryPosition>,
    /// Transcript-only records are staged once; snapshots store finite endpoints, not copies.
    pub(super) review_inputs: Vec<ReviewInputRecord>,
}

impl ModernRollbackPlan {
    /// Copies the base payload once per emitted record; the caller applies deferred history edits.
    pub(super) fn compacted_at(&self, record_index: usize) -> Option<CompactedItem> {
        let snapshot = self.snapshots.get(&record_index)?;
        let mut compacted = self.base.as_ref().clone();
        compacted.message.clear();
        compacted.compaction_response_id = None;
        compacted.resume_metadata = Some(snapshot.resume_metadata.clone());
        compacted.latest_token_usage_record = snapshot.latest_token_usage_record.clone();
        compacted.retained_context = Some(snapshot.retained_context.clone());
        compacted.retained_context_replay = Some(snapshot.retained_context_replay.clone());
        Some(compacted)
    }
}

/// State after the most recent compaction; another compaction always replaces this candidate.
struct ModernRollbackCandidate {
    plan: ModernRollbackPlan,
    resume_metadata: CompactionResumeMetadata,
    /// Parsed source-record positions, without retaining response payloads.
    model_records: Vec<usize>,
    user_boundaries: Vec<usize>,
    /// Contexts and UI instruction boundaries must be removed before publishing a snapshot.
    owned_records: Vec<usize>,
    rollback_turns: Vec<u32>,
    eligible: bool,
    retained_replay: RetainedReplay,
}

/// Source identity is sufficient for rollback after bounded retained admission has run.
/// Anonymous records keep their missing identity; lifecycle events cannot supply it.
struct RetainedReplayItem {
    /// Whether this response starts an instruction turn counted by rollback.
    boundary: bool,
    /// Context updates immediately before a removed instruction are removed with it.
    pre_turn_context: bool,
    turn_id: Option<String>,
    message_id: Option<ResponseItemId>,
    input_source: RetainedInputSource,
    /// Assistant messages and function calls also roll back by acceptance order.
    assistant_source: bool,
    /// Verified answers and confirmed assistant deliveries refer to the originating call.
    answer_call_id: Option<String>,
    /// Transcript rollback compares the original instruction, including anonymous payloads.
    boundary_item: Option<ResponseItem>,
}

impl From<&ResponseItemEnvelope> for RetainedReplayItem {
    fn from(response: &ResponseItemEnvelope) -> Self {
        Self {
            boundary: rollback::counts_as_boundary(&response.item),
            pre_turn_context: rollback::is_pre_turn_context_update(&response.item),
            turn_id: response.item.turn_id().map(str::to_owned),
            message_id: response.item.id().cloned(),
            input_source: response.metadata.as_ref().into(),
            assistant_source: matches!(&response.item, ResponseItem::Message { role, .. } if role == "assistant")
                || matches!(&response.item, ResponseItem::FunctionCall { .. }),
            answer_call_id: match &response.item {
                ResponseItem::FunctionCall { call_id, .. } => Some(call_id.clone()),
                _ => None,
            },
            boundary_item: rollback::counts_as_boundary(&response.item)
                .then(|| response.item.clone()),
        }
    }
}

/// Bounded retained state for each replay policy, plus response identities for rollback.
/// Raw instructions can advance counters or evict evidence even when their model items disappear.
struct RetainedReplay {
    states: RetainedContextReplay,
    items: Vec<RetainedReplayItem>,
    /// A prior reference supplies the baseline on remigration; otherwise the first record does.
    review_inputs: Option<Vec<ReviewInputRecord>>,
}

impl RetainedReplay {
    fn new(compacted: &CompactedItem) -> Self {
        let items = compacted.replacement_history.as_deref().unwrap_or_default();
        let replay = compacted.retained_context_replay.as_ref();
        let surviving_items = if replay.is_some() { &[][..] } else { items };
        let mut thread_owned_root = RetainedContext::default();
        thread_owned_root.restore(
            replay
                .map(|states| &states.thread_owned_root)
                .or(compacted.retained_context.as_ref()),
            surviving_items,
        );
        let mut thread_owned_worker = RetainedContext::default();
        thread_owned_worker.restore(
            replay
                .map(|states| &states.thread_owned_worker)
                .or(compacted.retained_context.as_ref()),
            surviving_items,
        );
        let mut legacy = RetainedContext::default();
        legacy.restore(
            replay
                .map(|states| &states.legacy)
                .or(compacted.retained_context.as_ref()),
            surviving_items,
        );
        let mut retained = Self {
            states: RetainedContextReplay {
                legacy,
                thread_owned_worker,
                thread_owned_root,
                review_input: replay.and_then(|replay| replay.review_input),
                resolved_review_input: None,
            },
            items: items.iter().map(RetainedReplayItem::from).collect(),
            review_inputs: replay
                .and_then(|replay| replay.review_input)
                .map(|_| Vec::new()),
        };
        if replay.is_none() {
            // Match ordinary checkpoint restoration before adopting inherited messages.
            // Already-migrated states must not recapture evidence removed by rollback.
            for response in items.iter().filter(|response| {
                response
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.delivered_assistant_message.is_some())
            }) {
                retained.capture_message(response, RetainedMessageSource::Checkpoint);
            }
        }
        if replay.is_none()
            && !retained
                .states
                .thread_owned_root
                .has_inherited_user_messages()
        {
            for response in items.iter().filter(|response| {
                response
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.inherited_user_message)
            }) {
                retained.capture_message(response, RetainedMessageSource::Checkpoint);
            }
        }
        if replay.is_none() {
            for context in [
                &mut retained.states.thread_owned_root,
                &mut retained.states.thread_owned_worker,
            ] {
                context.recover_user_message_excerpts(|id| {
                    let original = compacted
                        .guardian_history
                        .iter()
                        .flat_map(|history| &history.0)
                        .chain(items.iter().map(|response| &response.item))
                        .find(|item| item.id().is_some_and(|item_id| item_id.as_str() == id))?;
                    let ResponseItem::Message { role, content, .. } = original else {
                        return None;
                    };
                    if role != "user" || is_guardian_context_message(original) {
                        return None;
                    }
                    let original = UserMessageItem::new(&user_message_input(content));
                    Some(truncate_text(
                        &original.message(),
                        MAX_RETAINED_USER_MESSAGE_TOKENS,
                    ))
                });
            }
            let thread_owned_fallback = !checkpoint_requires_parent_context(
                compacted.retained_context.as_ref(),
                compacted.guardian_history.as_ref(),
                items,
                is_guardian_context_message,
            ) && CompactionCheckpoint::latest(items)
                .is_some_and(|checkpoint| !checkpoint.is_compatible_with(None));
            let root_may_retain_legacy = retained
                .states
                .thread_owned_root
                .has_missing_user_messages()
                && (compacted.guardian_history.is_some()
                    || CompactionCheckpoint::latest(items).is_none());
            if compacted.guardian_history.is_some()
                || thread_owned_fallback
                || root_may_retain_legacy
            {
                let mut history = TranscriptHistory::new(0);
                if let Some(checkpoint) = &compacted.guardian_history {
                    history.reset(checkpoint.0.iter());
                } else {
                    history.reset(
                        items
                            .iter()
                            .map(|response| &response.item)
                            .filter(|item| !is_guardian_context_message(item)),
                    );
                }
                // Record the original decision before suffix eviction or rollback can change it.
                let root_retains_legacy_transcript = root_may_retain_legacy
                    && ReconciledRetainedContext::new(
                        Some(&retained.states.thread_owned_root),
                        std::iter::empty(),
                    )
                    .unmatched_user_messages(history.items().filter_map(legacy_user_message))
                    .next()
                    .is_some();
                let applicability = match (
                    compacted.guardian_history.is_some() || root_retains_legacy_transcript,
                    thread_owned_fallback,
                ) {
                    (true, true) => Some(ReviewTranscriptApplicability::Both),
                    (true, false) => Some(ReviewTranscriptApplicability::Legacy),
                    (false, true) => Some(ReviewTranscriptApplicability::ThreadOwned),
                    (false, false) => None,
                };
                retained.review_inputs = applicability.map(|applicability| {
                    vec![ReviewInputRecord::Baseline {
                        applicability,
                        history: GuardianHistoryCheckpoint(history.items().cloned().collect()),
                        root_retains_legacy_transcript: Some(root_retains_legacy_transcript),
                    }]
                });
            }
        }
        retained
    }

    fn observe(&mut self, response: &ResponseItemEnvelope) {
        if !is_api_message(&response.item, response.metadata.as_ref()) {
            return;
        }
        if let Some(review_inputs) = &mut self.review_inputs
            && !is_guardian_context_message(&response.item)
        {
            review_inputs.push(ReviewInputRecord::ResponseItem {
                response: response.clone(),
            });
        }
        self.items.push(RetainedReplayItem::from(response));
        if let Some(metadata) = &response.metadata {
            self.states
                .thread_owned_root
                .record_sender_user_messages(metadata);
            self.states
                .thread_owned_worker
                .record_sender_user_messages(metadata);
        }
        if is_user_authorization_message(&response.item) {
            self.states.legacy.mark_user_messages_incomplete();
        }
        self.capture_message(response, RetainedMessageSource::Original);
    }

    fn capture_message(&mut self, response: &ResponseItemEnvelope, source: RetainedMessageSource) {
        let metadata = response.metadata.as_ref();
        let delivery = metadata
            .and_then(|metadata| metadata.delivered_assistant_message.as_ref())
            .and_then(|text| {
                let ResponseItem::FunctionCallOutput {
                    call_id: Some(call_id),
                    ..
                } = &response.item
                else {
                    return None;
                };
                Some((call_id, text))
            });
        let delivered_call = if let Some((call_id, text)) = delivery {
            let Some(call) = self
                .items
                .iter()
                .rev()
                .find(|item| item.answer_call_id.as_ref() == Some(call_id))
            else {
                return;
            };
            Some((call, text))
        } else {
            None
        };
        let input_source = delivered_call
            .map(|(call, _)| call.input_source)
            .unwrap_or_else(|| metadata.into());
        for (context, retain_inherited) in [
            (&mut self.states.thread_owned_root, true),
            (&mut self.states.thread_owned_worker, false),
        ] {
            if input_source == RetainedInputSource::Inherited && !retain_inherited {
                continue;
            }
            if let Some((call, text)) = delivered_call {
                // Completion can follow a queued user message; keep the confirmed
                // assistant text at the call's original identity and acceptance order.
                context.record_assistant_message(
                    RetainedUserMessage {
                        turn_id: call.turn_id.clone().unwrap_or_default(),
                        message_id: call.message_id.as_ref().map(|id| id.as_str().to_owned()),
                        text: text.clone(),
                        complete: true,
                        phase: None,
                        origin: UserInputOrigin::User,
                    },
                    input_source,
                );
                continue;
            }
            let captured = record_retained_message(context, &response.item, metadata, source);
            if let Some(saved) = metadata.and_then(|metadata| metadata.retained_source.as_ref())
                && captured.as_ref().is_some_and(|captured| {
                    captured.id == saved.id && captured.complete == saved.complete
                })
            {
                context.restore_source_revision(saved);
            }
        }
    }

    fn rollback(&mut self, num_turns: u32) {
        let boundaries = self
            .items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| item.boundary.then_some(index))
            .collect::<Vec<_>>();
        let Some(&first_boundary) = boundaries.first() else {
            return;
        };
        let count = usize::try_from(num_turns).unwrap_or(usize::MAX);
        let Some(&boundary) = boundaries.get(boundaries.len().saturating_sub(count)) else {
            return;
        };
        let first_removed = &self.items[boundary];
        let acceptance_boundary = first_removed.input_source.acceptance_order();
        if let Some(review_inputs) = &mut self.review_inputs
            && let Some(boundary) = &first_removed.boundary_item
        {
            review_inputs.push(ReviewInputRecord::Rollback {
                boundary: boundary.clone(),
            });
        }
        let mut cut = boundary;
        while cut > first_boundary && self.items[cut - 1].pre_turn_context {
            cut -= 1;
        }
        let removed_turns = self.items[cut..]
            .iter()
            .filter_map(|item| item.turn_id.as_deref())
            .collect::<Vec<_>>();
        for context in [
            &mut self.states.thread_owned_root,
            &mut self.states.thread_owned_worker,
        ] {
            context.rollback(
                &removed_turns,
                first_removed
                    .message_id
                    .as_ref()
                    .map(ResponseItemId::as_str),
                first_removed.input_source,
            );
        }
        self.states.legacy.retain_answers(|answer| {
            if let Some(source) = self.items.iter().rposition(|item| {
                item.turn_id.as_deref() == Some(answer.turn_id.as_str())
                    && item.answer_call_id.as_deref() == Some(answer.call_id.as_str())
            }) {
                return source < cut;
            }
            !removed_turns.contains(&answer.turn_id.as_str())
        });
        self.items.truncate(cut);
        if let Some(boundary) = acceptance_boundary {
            // A removed call must not supply identity or order to a later delivery.
            self.items.retain(|item| {
                !item.assistant_source
                    || item
                        .input_source
                        .acceptance_order()
                        .is_none_or(|order| order < boundary)
            });
        }
    }
}

/// Tracks only final, uncertified modern checkpoints with a directly representable rollback suffix.
#[derive(Default)]
pub(super) struct ModernRollbackPlanner {
    candidate: Option<ModernRollbackCandidate>,
    /// A compaction ends token-usage reconstruction even when its usage value is absent.
    latest_token_usage_record: Option<TokenUsageRecord>,
    /// Earlier certified state cannot be represented by an uncertified synthetic checkpoint.
    saw_certified_checkpoint: bool,
    /// Workers exclude inherited instructions; standalone roots adopt their surviving prefix.
    source_is_non_root_agent: bool,
    /// The indexed source can reflect promotion after the immutable JSONL header was written.
    source_override: Option<bool>,
}

impl ModernRollbackPlanner {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn set_source(&mut self, source: &SessionSource) {
        let is_non_root_agent = source.is_non_root_agent();
        self.source_override = Some(is_non_root_agent);
        self.source_is_non_root_agent = is_non_root_agent;
    }

    pub(super) fn checkpoint_index(&self) -> Option<usize> {
        self.candidate
            .as_ref()
            .map(|candidate| candidate.plan.checkpoint_index)
    }

    pub(super) fn observe(&mut self, record_index: usize, item: &RolloutItem) {
        match item {
            RolloutItem::SessionMeta(meta) => {
                self.source_is_non_root_agent = self
                    .source_override
                    .unwrap_or_else(|| meta.meta.source.is_non_root_agent());
            }
            RolloutItem::TokenUsageRecord(record) => {
                self.latest_token_usage_record = Some(record.clone());
            }
            RolloutItem::Compacted(compacted) => {
                self.latest_token_usage_record = compacted.latest_token_usage_record.clone();
                self.saw_certified_checkpoint |= compacted.segment_state_checkpoint.is_some();
                // An incomplete or certified newer compaction must not expose an older candidate.
                self.candidate = compacted
                    .resume_metadata
                    .as_ref()
                    .filter(|_| {
                        compacted.replacement_history.is_some()
                            && compacted.window_number.is_some()
                            // A copied MCP checkpoint would hide later surviving tool origins.
                            && compacted.mcp_resource_origins.is_none()
                            && !self.saw_certified_checkpoint
                    })
                    .map(|resume_metadata| ModernRollbackCandidate {
                        plan: ModernRollbackPlan {
                            checkpoint_index: record_index,
                            base: Arc::new(compacted.clone()),
                            snapshots: HashMap::new(),
                            review_input_base: compacted
                                .retained_context_replay
                                .as_ref()
                                .and_then(|replay| replay.review_input),
                            review_inputs: Vec::new(),
                        },
                        resume_metadata: resume_metadata.clone(),
                        model_records: Vec::new(),
                        user_boundaries: Vec::new(),
                        owned_records: Vec::new(),
                        rollback_turns: Vec::new(),
                        eligible: true,
                        retained_replay: RetainedReplay::new(compacted),
                    });
                return;
            }
            _ => {}
        }
        let Some(candidate) = self
            .candidate
            .as_mut()
            .filter(|candidate| candidate.eligible)
        else {
            return;
        };
        match item {
            RolloutItem::ResponseItem(response) => {
                candidate.retained_replay.observe(response);
                candidate.model_records.push(record_index);
                if rollback::counts_as_boundary(&response.item) {
                    candidate.user_boundaries.push(record_index);
                }
            }
            RolloutItem::TurnContext(context) => {
                candidate.owned_records.push(record_index);
                if let Some(version) = context.multi_agent_version {
                    candidate.resume_metadata.multi_agent_version = Some(version);
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                candidate.owned_records.push(record_index);
                candidate.resume_metadata.last_started_turn_id = Some(event.turn_id.clone());
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                candidate.owned_records.push(record_index);
            }
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                if matches!(&event.item, TurnItem::UserMessage(_)) =>
            {
                candidate.owned_records.push(record_index);
            }
            RolloutItem::WorldState(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::RetainedContext(_)
            | RolloutItem::RealtimeItem(_) => {
                // These records change reconstructed state that this bounded planner does not copy.
                candidate.eligible = false;
            }
            RolloutItem::SessionMeta(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::TokenUsageRecord(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::EventMsg(_) => {}
            RolloutItem::Compacted(_) => unreachable!("compactions replace the candidate above"),
        }
    }

    pub(super) fn observe_rollback(
        &mut self,
        record_index: usize,
        num_turns: u32,
        retained_context_edit_count: usize,
        is_retained: impl Fn(usize) -> bool,
    ) {
        let Some(candidate) = self
            .candidate
            .as_mut()
            .filter(|candidate| candidate.eligible)
        else {
            return;
        };
        if num_turns == 0 {
            return;
        }
        candidate.retained_replay.rollback(num_turns);
        let num_turns = usize::try_from(num_turns).unwrap_or(usize::MAX);
        let suffix_turns = candidate.user_boundaries.len();
        if num_turns <= suffix_turns {
            let remaining_turns = suffix_turns - num_turns;
            let first_removed_record = candidate.user_boundaries[remaining_turns];
            let retained_records = candidate
                .model_records
                .partition_point(|index| *index < first_removed_record);
            candidate.model_records.truncate(retained_records);
            candidate.user_boundaries.truncate(remaining_turns);
        } else {
            // Removing a base-history instruction also removes every later raw response item.
            candidate.model_records.clear();
            candidate.user_boundaries.clear();
            candidate
                .rollback_turns
                .push(u32::try_from(num_turns - suffix_turns).unwrap_or(u32::MAX));
        }
        if !candidate.model_records.is_empty()
            || candidate.owned_records.iter().copied().any(is_retained)
        {
            // Publishing only the reduced base would discard a surviving response or context.
            candidate.eligible = false;
            return;
        }
        candidate.plan.snapshots.insert(
            record_index,
            ModernRollbackSnapshot {
                rollback_turns: candidate.rollback_turns.clone(),
                retained_context_edit_count,
                resume_metadata: candidate.resume_metadata.clone(),
                latest_token_usage_record: self.latest_token_usage_record.clone(),
                retained_context: if self.source_is_non_root_agent {
                    candidate.retained_replay.states.thread_owned_worker.clone()
                } else {
                    candidate.retained_replay.states.thread_owned_root.clone()
                },
                retained_context_replay: candidate.retained_replay.states.clone(),
                review_input_end: candidate
                    .retained_replay
                    .review_inputs
                    .as_ref()
                    .map(Vec::len),
            },
        );
    }

    pub(super) fn finish(self) -> Option<ModernRollbackPlan> {
        self.candidate
            .filter(|candidate| candidate.eligible && !candidate.plan.snapshots.is_empty())
            .map(|mut candidate| {
                candidate.plan.review_inputs =
                    candidate.retained_replay.review_inputs.unwrap_or_default();
                let end = candidate
                    .plan
                    .snapshots
                    .values()
                    .filter_map(|snapshot| snapshot.review_input_end)
                    .max()
                    .unwrap_or_default();
                candidate.plan.review_inputs.truncate(end);
                candidate.plan
            })
    }
}

#[cfg(test)]
#[path = "modern_retained_tests.rs"]
mod retained_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::MultiAgentVersion;
    use codex_protocol::protocol::TokenUsage;
    use codex_rollout::RolloutRecorder;
    use serde_json::Value;
    use serde_json::json;

    fn record(kind: &str, payload: Value) -> RolloutItem {
        RolloutRecorder::parse_rollout_line_value(json!({
            "timestamp": "2026-09-25T00:00:00Z",
            "type": kind,
            "payload": payload,
        }))
        .expect("compatible rollout fixture")
        .expect("retained rollout fixture")
        .item
    }

    fn user_payload(turn: &str) -> Value {
        json!({
            "type": "message", "role": "user",
            "content": [{"type": "input_text", "text": turn}],
        })
    }

    fn checkpoint() -> CompactedItem {
        let RolloutItem::Compacted(compacted) = record(
            "compacted",
            json!({
                "message": "original compaction",
                "compaction_response_id": "original-response",
                "replacement_history": [user_payload("A"), user_payload("B")],
                "window_number": 1,
                "resume_metadata": {
                    "multi_agent_version": "v2",
                    "last_started_turn_id": "B",
                    "previous_turn_settings": {
                        "model": "checkpoint-model", "comp_hash": "checkpoint-hash",
                        "realtime_active": true,
                    },
                },
            }),
        ) else {
            panic!("compacted fixture");
        };
        compacted
    }

    fn context(turn: &str) -> RolloutItem {
        record(
            "turn_context",
            json!({
                "turn_id": turn, "cwd": "/tmp", "approval_policy": "never",
                "sandbox_policy": {"type": "danger-full-access"},
                "model": "later-model", "multi_agent_version": "v1", "summary": "auto",
            }),
        )
    }

    fn start(turn: &str) -> RolloutItem {
        record(
            "event_msg",
            json!({"type": "task_started", "turn_id": turn}),
        )
    }

    fn user_event(turn: &str) -> RolloutItem {
        record(
            "event_msg",
            json!({"type": "user_message", "message": turn}),
        )
    }

    fn native_user(turn: &str) -> RolloutItem {
        record(
            "event_msg",
            json!({
                "type": "item_completed",
                "thread_id": "00000000-0000-7000-8000-000000000001", "turn_id": turn,
                "item": {"type": "UserMessage", "id": turn, "content": []},
            }),
        )
    }

    fn usage(turn: &str) -> RolloutItem {
        record(
            "token_usage_record",
            json!({
                "thread_id": "00000000-0000-7000-8000-000000000001",
                "session_id": "00000000-0000-7000-8000-000000000002",
                "turn_id": turn, "root_turn_id": turn, "response_id": turn,
                "usage": TokenUsage::default(),
                "turn_token_usage": TokenUsage::default(),
                "thread_token_usage": TokenUsage::default(),
            }),
        )
    }

    fn certificate() -> Value {
        json!({"version": 1, "world_state": "cleared", "reference_context": "cleared"})
    }

    #[test]
    fn snapshots_preserve_sticky_metadata_without_repeating_base_rollback() {
        let base = checkpoint();
        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &usage("older"));
        planner.observe(1, &RolloutItem::Compacted(base.clone()));
        assert_eq!(planner.checkpoint_index(), Some(1));
        planner.observe_rollback(2, 1, 1, |_| false);
        planner.observe(3, &start("C"));
        planner.observe(4, &record("response_item", user_payload("C")));
        planner.observe(5, &user_event("C"));
        planner.observe(6, &native_user("C"));
        planner.observe(7, &context("C"));
        planner.observe(8, &usage("C"));
        planner.observe_rollback(9, 1, 2, |_| false);

        let plan = planner.finish().expect("both rollbacks are representable");
        assert_eq!(plan.base.as_ref(), &base);
        assert_eq!(plan.snapshots[&2].rollback_turns, vec![1]);
        assert_eq!(plan.snapshots[&9].rollback_turns, vec![1]);
        assert_eq!(plan.snapshots[&2].retained_context_edit_count, 1);
        assert_eq!(plan.snapshots[&9].retained_context_edit_count, 2);
        assert!(plan.compacted_at(1).is_none());
        let first = plan.compacted_at(2).expect("first rollback snapshot");
        assert_eq!(first.resume_metadata, base.resume_metadata);
        assert_eq!(first.latest_token_usage_record, None);
        let last = plan.compacted_at(9).expect("second rollback snapshot");
        let metadata = last.resume_metadata.expect("authoritative metadata");
        assert_eq!(metadata.multi_agent_version, Some(MultiAgentVersion::V1));
        assert_eq!(metadata.last_started_turn_id.as_deref(), Some("C"));
        assert_eq!(
            metadata.previous_turn_settings,
            base.resume_metadata.unwrap().previous_turn_settings
        );
        assert_eq!(last.latest_token_usage_record.unwrap().turn_id, "C");
        assert!(last.message.is_empty());
        assert_eq!(last.compaction_response_id, None);
    }

    #[test]
    fn retained_context_snapshots_preserve_anonymous_admission_across_remigration() {
        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &RolloutItem::Compacted(checkpoint()));
        planner.observe(1, &record("response_item", user_payload("C")));
        planner.observe_rollback(2, 1, 0, |_| false);
        let compacted = planner.finish().unwrap().compacted_at(2).unwrap();
        assert_eq!(
            serde_json::to_value(&compacted.retained_context).unwrap(),
            json!({
                "verified_answers": [], "incomplete": false,
                "user_messages": [{
                    "order": 0, "turn_id": "", "message_id": null,
                    "text": "C", "complete": false,
                }],
                "user_messages_incomplete": true, "next_order": 1,
                "assistant_messages": [], "assistant_messages_incomplete": false,
            })
        );
        let legacy = json!({
            "verified_answers": [], "incomplete": false, "user_messages": [],
            "user_messages_incomplete": true, "next_order": 0,
            "assistant_messages": [], "assistant_messages_incomplete": false,
        });
        assert_eq!(
            serde_json::to_value(&compacted.retained_context_replay.as_ref().unwrap().legacy)
                .unwrap(),
            legacy
        );

        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &RolloutItem::Compacted(compacted));
        planner.observe(1, &record("response_item", user_payload("D")));
        planner.observe_rollback(2, 1, 0, |_| false);
        let compacted = planner.finish().unwrap().compacted_at(2).unwrap();
        assert_eq!(
            serde_json::to_value(&compacted.retained_context).unwrap(),
            json!({
                "verified_answers": [], "incomplete": false,
                "user_messages": [
                    {"order": 0, "turn_id": "", "message_id": null, "text": "C", "complete": false},
                    {"order": 1, "turn_id": "", "message_id": null, "text": "D", "complete": false},
                ],
                "user_messages_incomplete": true, "next_order": 2,
                "assistant_messages": [], "assistant_messages_incomplete": false,
            })
        );
        assert_eq!(
            serde_json::to_value(&compacted.retained_context_replay.as_ref().unwrap().legacy)
                .unwrap(),
            legacy
        );
    }

    #[test]
    fn retained_context_snapshots_keep_eviction_and_counter_after_ordered_rollback() {
        let users = (0..8)
            .map(|order| {
                json!({
                    "order": order, "turn_id": format!("turn-{order}"),
                    "message_id": format!("message-{order}"), "text": format!("user-{order}"),
                    "complete": true,
                })
            })
            .collect::<Vec<_>>();
        let mut base = checkpoint();
        base.retained_context = Some(
            serde_json::from_value(json!({
                "verified_answers": [], "incomplete": false, "user_messages": users,
                "user_messages_incomplete": false, "next_order": 8,
            }))
            .unwrap(),
        );
        let mut response = record("response_item", user_payload("C"));
        let RolloutItem::ResponseItem(envelope) = &mut response else {
            panic!("response fixture");
        };
        envelope.metadata = Some(codex_history::CodexHarnessMetadata {
            user_input_order: Some(8),
            ..Default::default()
        });
        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &RolloutItem::Compacted(base));
        planner.observe(1, &response);
        planner.observe_rollback(2, 1, 0, |_| false);
        let compacted = planner.finish().unwrap().compacted_at(2).unwrap();
        assert_eq!(
            serde_json::to_value(&compacted.retained_context).unwrap(),
            json!({
                "verified_answers": [], "incomplete": false, "user_messages": users[1..],
                "user_messages_incomplete": true, "next_order": 9,
                "assistant_messages": [], "assistant_messages_incomplete": false,
            })
        );
        assert_eq!(
            serde_json::to_value(&compacted.retained_context_replay.as_ref().unwrap().legacy)
                .unwrap(),
            json!({
                "verified_answers": [], "incomplete": false, "user_messages": users,
                "user_messages_incomplete": true, "next_order": 8,
                "assistant_messages": [], "assistant_messages_incomplete": false,
            })
        );
    }

    #[test]
    fn anonymous_sender_delivery_preserves_its_independent_recording_order() {
        let mut response = record("response_item", user_payload("C"));
        let RolloutItem::ResponseItem(envelope) = &mut response else {
            panic!("response fixture");
        };
        envelope.metadata = Some(codex_history::CodexHarnessMetadata {
            sender_user_messages: Some(Box::new(codex_history::SenderUserMessages {
                receiver_turn_id: "C".to_string(),
                receiver_message_id: "delivery-C".to_string(),
                text: "sender restriction".to_string(),
            })),
            ..Default::default()
        });
        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &RolloutItem::Compacted(checkpoint()));
        planner.observe(1, &response);
        planner.observe_rollback(2, 1, 0, |_| false);
        let compacted = planner.finish().unwrap().compacted_at(2).unwrap();
        assert_eq!(
            serde_json::to_value(&compacted.retained_context).unwrap(),
            json!({
                "verified_answers": [], "incomplete": false,
                "user_messages": [{
                    "order": 1, "turn_id": "", "message_id": null, "text": "C", "complete": false,
                }],
                "user_messages_incomplete": true, "next_order": 2,
                "assistant_messages": [], "assistant_messages_incomplete": false,
                "sender_deliveries": [{
                    "order": 0, "receiver_turn_id": "C", "receiver_message_id": "delivery-C",
                    "text": "sender restriction",
                }],
            })
        );
        assert_eq!(
            serde_json::to_value(&compacted.retained_context_replay.as_ref().unwrap().legacy)
                .unwrap(),
            json!({
                "verified_answers": [], "incomplete": false, "user_messages": [],
                "user_messages_incomplete": true, "next_order": 0,
                "assistant_messages": [], "assistant_messages_incomplete": false,
            })
        );
    }

    #[test]
    fn retained_context_restores_cleared_excerpts_before_suffix_admission() {
        let mut base = checkpoint();
        base.retained_context = Some(
            serde_json::from_value(json!({
                "verified_answers": [], "incomplete": false,
                "user_messages": [{
                    "order": 0, "turn_id": "A", "message_id": "A", "text": "", "complete": false,
                }],
                "user_messages_incomplete": false, "next_order": 1,
            }))
            .unwrap(),
        );
        let original = serde_json::from_value(json!({
            "type": "message", "role": "user", "id": "A",
            "content": [
                {"type": "input_text", "text": "first"},
                {"type": "output_text", "text": "not user input"},
                {"type": "input_text", "text": "second"},
            ],
        }))
        .unwrap();
        base.guardian_history = Some(codex_history::GuardianHistoryCheckpoint(vec![original]));
        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &RolloutItem::Compacted(base));
        planner.observe(1, &record("response_item", user_payload("C")));
        planner.observe_rollback(2, 1, 0, |_| false);
        let compacted = planner.finish().unwrap().compacted_at(2).unwrap();
        assert_eq!(
            serde_json::to_value(&compacted.retained_context).unwrap(),
            json!({
                "verified_answers": [], "incomplete": false,
                "user_messages": [
                    {"order": 0, "turn_id": "A", "message_id": "A", "text": "firstsecond", "complete": false},
                    {"order": 1, "turn_id": "", "message_id": null, "text": "C", "complete": false},
                ],
                "user_messages_incomplete": false, "next_order": 2,
                "assistant_messages": [], "assistant_messages_incomplete": false,
            })
        );
    }

    #[test]
    fn contextual_developer_turn_ids_remove_legacy_verified_answers() {
        for text in [
            "<persistent_mode>mode</persistent_mode>",
            "<multi_agent_role>role</multi_agent_role>",
            "Approved command prefix saved: cargo test",
        ] {
            let mut base = checkpoint();
            base.replacement_history = Some(
                [
                    user_payload("A"),
                    json!({
                        "type": "message", "role": "developer",
                        "content": [{"type": "input_text", "text": text}],
                        "internal_chat_message_metadata_passthrough": {"turn_id": "X"},
                    }),
                    user_payload("B"),
                ]
                .into_iter()
                .map(|item| ResponseItemEnvelope::new(serde_json::from_value(item).unwrap()))
                .collect(),
            );
            base.retained_context = Some(
                serde_json::from_value(json!({
                    "verified_answers": [{
                        "order": 0, "turn_id": "X", "call_id": "ask-X",
                        "questions": [{"question": "Continue?", "answer": "No"}],
                    }],
                    "incomplete": false, "user_messages": [],
                    "user_messages_incomplete": false, "next_order": 1,
                }))
                .unwrap(),
            );
            let mut planner = ModernRollbackPlanner::new();
            planner.observe(0, &RolloutItem::Compacted(base));
            planner.observe_rollback(1, 1, 0, |_| false);
            let compacted = planner.finish().unwrap().compacted_at(1).unwrap();
            let expected = json!({
                "verified_answers": [], "incomplete": false, "user_messages": [],
                "user_messages_incomplete": false, "next_order": 1,
                "assistant_messages": [], "assistant_messages_incomplete": false,
            });
            assert_eq!(
                serde_json::to_value(&compacted.retained_context).unwrap(),
                expected,
                "{text}"
            );
            assert_eq!(
                serde_json::to_value(&compacted.retained_context_replay.as_ref().unwrap().legacy)
                    .unwrap(),
                expected,
                "{text}"
            );
        }
    }

    #[test]
    fn prefix_before_rollback_has_no_snapshot_and_later_start_does_not_change_prior_snapshot() {
        let base = checkpoint();
        let mut prefix = ModernRollbackPlanner::new();
        prefix.observe(0, &RolloutItem::Compacted(base.clone()));
        assert!(prefix.finish().is_none());

        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &RolloutItem::Compacted(base.clone()));
        planner.observe_rollback(1, 1, 1, |_| false);
        planner.observe(2, &start("D"));
        planner.observe(3, &record("response_item", user_payload("D")));
        planner.observe(4, &context("D"));
        let plan = planner
            .finish()
            .expect("later records replay after the snapshot");
        assert_eq!(plan.snapshots.len(), 1);
        assert_eq!(plan.base.as_ref(), &base);
        assert_eq!(
            plan.compacted_at(1).unwrap().resume_metadata,
            base.resume_metadata
        );
        assert!(plan.compacted_at(4).is_none());
    }

    #[test]
    fn empty_modern_metadata_does_not_restore_older_values() {
        let mut base = checkpoint();
        base.resume_metadata = Some(CompactionResumeMetadata {
            multi_agent_version: None,
            last_started_turn_id: None,
            previous_turn_settings: None,
        });
        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &context("older"));
        planner.observe(1, &start("older"));
        planner.observe(2, &RolloutItem::Compacted(base.clone()));
        planner.observe_rollback(3, 1, 0, |_| false);
        assert_eq!(
            planner
                .finish()
                .unwrap()
                .compacted_at(3)
                .unwrap()
                .resume_metadata,
            base.resume_metadata,
        );
    }

    #[test]
    fn surviving_response_suffix_disables_all_snapshots() {
        for surviving in [
            user_payload("D"),
            json!({"type": "function_call_output", "call_id": "D", "output": "surviving output"}),
        ] {
            let mut planner = ModernRollbackPlanner::new();
            planner.observe(0, &RolloutItem::Compacted(checkpoint()));
            planner.observe_rollback(1, 1, 0, |_| false);
            planner.observe(2, &record("response_item", surviving));
            planner.observe(3, &record("response_item", user_payload("C")));
            planner.observe_rollback(4, 1, 0, |_| false);
            assert!(planner.finish().is_none());
        }
    }

    #[test]
    fn surviving_context_or_ui_boundary_disables_snapshot() {
        for surviving in [context("D"), start("D"), user_event("D"), native_user("D")] {
            let mut planner = ModernRollbackPlanner::new();
            planner.observe(0, &RolloutItem::Compacted(checkpoint()));
            planner.observe(1, &surviving);
            planner.observe(2, &record("response_item", user_payload("C")));
            planner.observe_rollback(3, 1, 0, |index| index == 1);
            assert!(planner.finish().is_none());
        }
    }

    #[test]
    fn later_incomplete_or_certified_compaction_disables_older_candidate() {
        for field in [
            "resume_metadata",
            "replacement_history",
            "window_number",
            "segment_state_checkpoint",
        ] {
            let mut later = serde_json::to_value(checkpoint()).unwrap();
            later[field] = if field == "segment_state_checkpoint" {
                certificate()
            } else {
                Value::Null
            };
            let mut planner = ModernRollbackPlanner::new();
            planner.observe(0, &RolloutItem::Compacted(checkpoint()));
            planner.observe_rollback(1, 1, 0, |_| false);
            planner.observe(2, &record("compacted", later));
            assert_eq!(planner.checkpoint_index(), None, "{field}");
            planner.observe_rollback(3, 1, 0, |_| false);
            assert!(planner.finish().is_none(), "{field}");
        }
    }

    #[test]
    fn earlier_certificate_disables_later_uncertified_candidate() {
        let mut certified = serde_json::to_value(checkpoint()).unwrap();
        certified["segment_state_checkpoint"] = certificate();
        let mut planner = ModernRollbackPlanner::new();
        planner.observe(0, &record("compacted", certified));
        planner.observe(1, &RolloutItem::Compacted(checkpoint()));
        planner.observe_rollback(2, 1, 0, |_| false);
        assert!(planner.finish().is_none());
    }

    #[test]
    fn mcp_origin_checkpoint_disables_candidate_even_when_empty() {
        for origins in [
            json!([]),
            json!([{
                "call_id": "call-A", "turn_id": "A", "tool": "read",
                "connector_id": "connector", "uri": "resource://A",
            }]),
        ] {
            let mut compacted = serde_json::to_value(checkpoint()).unwrap();
            compacted["mcp_resource_origins"] = json!({
                "origins": origins, "turns": ["A"], "current_turn_id": "A",
            });
            let mut planner = ModernRollbackPlanner::new();
            planner.observe(0, &record("compacted", compacted));
            assert_eq!(planner.checkpoint_index(), None);
            planner.observe_rollback(1, 1, 0, |_| false);
            assert!(planner.finish().is_none());
        }
    }

    #[test]
    fn unsupported_post_checkpoint_state_disables_snapshot() {
        for unsupported in [
            record("world_state", json!({"full": true, "state": {}})),
            record(
                "inter_agent_communication",
                json!({
                    "author": "/root/worker", "recipient": "/root",
                    "content": "result", "trigger_turn": true,
                }),
            ),
            RolloutItem::InterAgentCommunicationMetadata { trigger_turn: true },
            record(
                "retained_context",
                json!({
                    "type": "verified_answer", "turn_id": "C", "call_id": "question-C",
                    "questions": [{"question": "Continue?", "answer": "Yes"}],
                }),
            ),
        ] {
            let mut planner = ModernRollbackPlanner::new();
            planner.observe(0, &RolloutItem::Compacted(checkpoint()));
            planner.observe(1, &unsupported);
            planner.observe_rollback(2, 1, 0, |_| false);
            assert!(planner.finish().is_none());
        }
    }
}
