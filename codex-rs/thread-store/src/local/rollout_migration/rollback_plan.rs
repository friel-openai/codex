//! Decides which legacy records remain visible after historical rollback.
//!
//! Legacy rollback removes logical instruction turns, not a physical suffix of the rollout file.
//! Most records happen to be ordered that way, but late completion events can target an older
//! surviving turn after a newer turn has started. This planner keeps compact per-record ownership
//! metadata for SQLite visibility, then combines it with `rollback_replay`'s cold-resume answer
//! before the writer makes its second streaming pass. Retained checkpoints are reduced when
//! rollback is encountered, never by reapplying old rollbacks to later checkpoints.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use sha2::Digest;
use sha2::Sha256;

use codex_protocol::ResponseItemId;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ImageReference;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::protocol::UserMessageImageKind;
use codex_rollout::CompactedItem;
use codex_rollout::RetainedContextEntry;
use codex_rollout::RetainedInputSource;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;

use super::migration_error;
use super::rollback;
use super::rollback_replay::ModelReplayPlanner;
use crate::ThreadStoreResult;

/// Deferred edits to one compaction. The replay pass owns the large replacement history, so the
/// planner retains only the rollback counts rather than cloning every model-context checkpoint.
struct CompactionFrame {
    record_index: usize,
    boundary_depth: usize,
    owner: Option<usize>,
    has_replacement_history: bool,
    /// Retained-context edits in the order historical rollbacks occurred.
    retained_context_edits: Vec<Arc<RetainedContextEdit>>,
    /// User-turn removals in their original replay order.
    rollback_turns: Vec<u32>,
}

/// A bounded description of one historical rollback's effect on an earlier checkpoint.
struct RetainedContextEdit {
    removed_turn_ids: Vec<String>,
    first_removed_message_id: Option<ResponseItemId>,
    /// Original instruction position, not the later rollback record's position.
    source_record_index: usize,
    /// Exact serialized item identity for legacy instructions without message IDs.
    first_removed_fingerprint: Option<[u8; 32]>,
    input_source: RetainedInputSource,
    force_ordered_rollback: bool,
    known_answer_sources: HashSet<(String, String)>,
    removed_answer_sources: HashSet<(String, String)>,
}

impl RetainedContextEdit {
    fn apply(&self, compacted: &mut CompactedItem) {
        let Some(context) = compacted.retained_context.as_mut() else {
            return;
        };
        if self.force_ordered_rollback
            || context
                .ordered_entries()
                .any(|(_, entry)| matches!(entry, RetainedContextEntry::UserMessage(_)))
        {
            let removed_turn_ids = self
                .removed_turn_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            context.rollback(
                &removed_turn_ids,
                self.first_removed_message_id
                    .as_ref()
                    .map(ResponseItemId::as_str),
                self.input_source,
            );
        } else {
            context.retain_answers(|answer| {
                let source = (answer.turn_id.clone(), answer.call_id.clone());
                if self.removed_answer_sources.contains(&source) {
                    return false;
                }
                self.known_answer_sources.contains(&source)
                    || !self.removed_turn_ids.contains(&answer.turn_id)
            });
        }
    }
}

#[derive(Clone)]
struct PendingUserResponse {
    boundary: usize,
    content: Vec<ContentItem>,
}

struct InstructionBoundary {
    record_index: usize,
    message_id: Option<ResponseItemId>,
    /// Bounded identity metadata; never retain the original user/image payload here.
    fingerprint: Option<[u8; 32]>,
    input_source: RetainedInputSource,
    alive: bool,
}

struct RetainedFactSource {
    record_index: usize,
    turn_id: String,
    acceptance_order: Option<u64>,
}

/// Compact plan keyed by parsed source-record index.
pub(super) struct RollbackPlan {
    record_boundaries: Vec<Option<usize>>,
    boundary_alive: Vec<bool>,
    /// A removed compaction whose empty checkpoint must still stop reverse model replay.
    empty_replacement_history_compaction: Option<usize>,
    /// Deferred user-turn removals keyed by parsed source-record index.
    compacted_rollbacks: HashMap<usize, Vec<u32>>,
    /// Deferred retained-context edits keyed by parsed source-record index.
    retained_context_edits: HashMap<usize, Vec<Arc<RetainedContextEdit>>>,
    /// Explicit turn IDs whose last instruction boundary was removed.
    removed_turn_ids: HashSet<String>,
}

impl RollbackPlan {
    pub(super) fn removed_turn_ids(&self) -> &HashSet<String> {
        &self.removed_turn_ids
    }
    pub(super) fn record_count(&self) -> usize {
        self.record_boundaries.len()
    }

    pub(super) fn apply(
        &self,
        record_index: usize,
        mut line: RolloutLine,
    ) -> ThreadStoreResult<Option<RolloutLine>> {
        let boundary = self
            .record_boundaries
            .get(record_index)
            .ok_or_else(|| migration_error("rollback plan is shorter than source replay"))?;
        if matches!(
            &line.item,
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_))
        ) {
            return Ok(None);
        }
        let is_replay_anchor = self.empty_replacement_history_compaction == Some(record_index);
        let retained_context_edits = self.retained_context_edits.get(&record_index);
        let rollbacks = self.compacted_rollbacks.get(&record_index);
        if is_replay_anchor || retained_context_edits.is_some() || rollbacks.is_some() {
            let RolloutItem::Compacted(compacted) = &mut line.item else {
                return Err(migration_error(
                    "rollback compaction changed during source replay",
                ));
            };
            // Resolve every removed instruction against the original Guardian transcript before
            // truncating it. Repeated rollbacks can otherwise mistake an already removed boundary
            // for evicted evidence and discard the surviving prefix.
            let mut guardian_cut = None;
            for edit in retained_context_edits.into_iter().flatten() {
                if (edit.first_removed_message_id.is_some()
                    || edit.first_removed_fingerprint.is_some())
                    && let Some(history) = compacted.guardian_history.as_ref()
                {
                    let mut edit_cut = None;
                    for (index, item) in history.0.iter().enumerate() {
                        let matches = item
                            .id()
                            .zip(edit.first_removed_message_id.as_ref())
                            .is_some_and(|(left, right)| left == right)
                            || (edit.first_removed_fingerprint.is_some()
                                && rollback::counts_as_boundary(item)
                                && Some(instruction_fingerprint(item)?)
                                    == edit.first_removed_fingerprint);
                        if matches {
                            edit_cut = Some(index);
                            break;
                        }
                    }
                    if edit_cut.is_none() && record_index >= edit.source_record_index {
                        // Match TranscriptHistory::truncate_before when retention evicted the
                        // removed boundary. Acceptance-order edits can also reach older checkpoints;
                        // those must keep their transcript when the boundary is absent.
                        edit_cut = Some(0);
                    }
                    if let Some(cut) = edit_cut {
                        guardian_cut =
                            Some(guardian_cut.map_or(cut, |previous: usize| previous.min(cut)));
                    }
                }
            }
            if let Some(edits) = retained_context_edits {
                for edit in edits {
                    edit.apply(compacted);
                }
            }
            if is_replay_anchor {
                compacted.replacement_history = Some(Vec::new());
                compacted.mcp_resource_origins = None;
                if let Some(history) = compacted.guardian_history.as_mut() {
                    history.0.truncate(guardian_cut.unwrap_or(0));
                }
                return Ok(Some(line));
            }
            if let Some(rollbacks) = rollbacks {
                let replacement_history =
                    compacted.replacement_history.as_mut().ok_or_else(|| {
                        migration_error(
                            "legacy rollback crosses a compaction without replacement history",
                        )
                    })?;
                compacted.mcp_resource_origins = None;
                for &num_turns in rollbacks {
                    if let Some(history) = compacted.guardian_history.as_ref()
                        && let Some(boundary) = replacement_history
                            .iter()
                            .rev()
                            .filter(|item| rollback::counts_as_boundary(&item.item))
                            .take(usize::try_from(num_turns).unwrap_or(usize::MAX))
                            .last()
                    {
                        let cut = history
                            .0
                            .iter()
                            .position(|item| {
                                item.id()
                                    .zip(boundary.item.id())
                                    .is_some_and(|(left, right)| left == right)
                                    || item == &boundary.item
                            })
                            .unwrap_or(0);
                        guardian_cut = Some(guardian_cut.map_or(cut, |previous| previous.min(cut)));
                    }
                    rollback::drop_last_n_user_turns(replacement_history, num_turns);
                }
            }
            if let Some(cut) = guardian_cut
                && let Some(history) = compacted.guardian_history.as_mut()
            {
                history.0.truncate(cut);
            }
        }
        if boundary.is_some_and(|boundary| !self.boundary_alive[boundary]) {
            return Ok(None);
        }
        Ok(Some(line))
    }
}

/// Streaming builder for RollbackPlan.
pub(super) struct RollbackPlanner {
    record_boundaries: Vec<Option<usize>>,
    boundaries: Vec<InstructionBoundary>,
    boundary_stack: Vec<usize>,
    active_turn_id: Option<String>,
    pending_turn_records: Vec<usize>,
    pending_context_records: Vec<usize>,
    pending_user_response: Option<PendingUserResponse>,
    pending_delivery_boundary: Option<usize>,
    turn_boundaries: HashMap<String, usize>,
    call_boundaries: HashMap<(String, String), Option<usize>>,
    retained_fact_sources: Vec<RetainedFactSource>,
    /// Canonical user snapshots can be repeated after the matching model response.
    native_user_boundaries: HashMap<(String, String), usize>,
    compactions: Vec<CompactionFrame>,
    model_replay: ModelReplayPlanner,
}

impl RollbackPlanner {
    pub(super) fn new() -> Self {
        Self {
            record_boundaries: Vec::new(),
            boundaries: Vec::new(),
            boundary_stack: Vec::new(),
            active_turn_id: None,
            pending_turn_records: Vec::new(),
            pending_context_records: Vec::new(),
            pending_user_response: None,
            pending_delivery_boundary: None,
            turn_boundaries: HashMap::new(),
            call_boundaries: HashMap::new(),
            retained_fact_sources: Vec::new(),
            native_user_boundaries: HashMap::new(),
            compactions: Vec::new(),
            model_replay: ModelReplayPlanner::new(),
        }
    }

    pub(super) fn observe(&mut self, line: &RolloutLine) -> ThreadStoreResult<()> {
        self.observe_inner(line, /*native*/ false)
    }

    pub(super) fn observe_paginated(&mut self, line: &RolloutLine) -> ThreadStoreResult<()> {
        self.observe_inner(line, /*native*/ true)
    }

    fn observe_inner(&mut self, line: &RolloutLine, native: bool) -> ThreadStoreResult<()> {
        if matches!(line.item, RolloutItem::RolloutReference(_)) {
            self.model_replay
                .observe(self.record_boundaries.len(), &line.item);
            self.record_boundaries.push(None);
            return Ok(());
        }
        let index = self.record_boundaries.len();
        if native {
            self.model_replay.observe_paginated(index, &line.item);
        } else {
            self.model_replay.observe(index, &line.item);
        }
        self.record_boundaries
            .push(self.boundary_stack.last().copied());
        let paired_user_boundary = match (&self.pending_user_response, &line.item) {
            (Some(pending), RolloutItem::EventMsg(EventMsg::UserMessage(event)))
                if user_response_matches_event(&pending.content, event) =>
            {
                Some(pending.boundary)
            }
            (Some(pending), RolloutItem::EventMsg(EventMsg::ItemCompleted(event))) => {
                match &event.item {
                    TurnItem::UserMessage(user) => match user.as_legacy_event() {
                        EventMsg::UserMessage(event)
                            if user_response_matches_event(&pending.content, &event) =>
                        {
                            Some(pending.boundary)
                        }
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        };
        let paired_delivery_boundary = match (&self.pending_delivery_boundary, &line.item) {
            (Some(boundary), RolloutItem::ResponseItem(response))
                if matches!(&response.item, ResponseItem::AgentMessage { .. }) =>
            {
                Some(*boundary)
            }
            _ => None,
        };
        self.pending_user_response = None;
        self.pending_delivery_boundary = None;

        match &line.item {
            RolloutItem::SessionMeta(_) => self.record_boundaries[index] = None,
            RolloutItem::RolloutReference(_) => {}
            RolloutItem::ResponseItem(response) => {
                if let Some(boundary) = paired_delivery_boundary {
                    self.record_boundaries[index] = Some(boundary);
                    self.boundaries[boundary].message_id = response.id().cloned();
                    if response.id().is_none() {
                        self.boundaries[boundary].fingerprint =
                            Some(instruction_fingerprint(&response.item)?);
                    }
                } else if rollback::counts_as_boundary(&response.item) {
                    let boundary = self.start_boundary(index);
                    self.boundaries[boundary].message_id = response.id().cloned();
                    if response.id().is_none() {
                        self.boundaries[boundary].fingerprint =
                            Some(instruction_fingerprint(&response.item)?);
                    }
                    self.boundaries[boundary].input_source = response.metadata.as_ref().into();
                    if let ResponseItem::Message { role, content, .. } = &response.item
                        && role == "user"
                    {
                        self.pending_user_response = Some(PendingUserResponse {
                            boundary,
                            content: content.clone(),
                        });
                    }
                } else if rollback::is_pre_turn_context_update(&response.item) {
                    // Until another user boundary arrives, this is trailing context for the
                    // previous turn. Keep that fallback owner so rollback drops it when there is
                    // no later turn to attach it to.
                    self.pending_context_records.push(index);
                }
                if let ResponseItem::FunctionCall { call_id, .. } = &response.item
                    && let Some(turn_id) = response.turn_id().or(self.active_turn_id.as_deref())
                {
                    self.call_boundaries.insert(
                        (turn_id.to_owned(), call_id.clone()),
                        self.record_boundaries[index],
                    );
                }
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                self.apply_rollback(rollback.num_turns)?;
            }
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                self.active_turn_id = Some(event.turn_id.clone());
                self.pending_turn_records.clear();
                self.pending_turn_records.push(index);
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                self.assign_targeted_record(index, Some(event.turn_id.as_str()));
                if self.active_turn_id.as_deref() == Some(event.turn_id.as_str()) {
                    self.active_turn_id = None;
                    self.pending_turn_records.clear();
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                self.assign_targeted_record(index, event.turn_id.as_deref());
                if event
                    .turn_id
                    .as_deref()
                    .is_some_and(|turn_id| self.active_turn_id.as_deref() == Some(turn_id))
                {
                    self.active_turn_id = None;
                    self.pending_turn_records.clear();
                }
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                let boundary = paired_user_boundary.unwrap_or_else(|| self.start_boundary(index));
                self.record_boundaries[index] = Some(boundary);
            }
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => {
                if let TurnItem::UserMessage(user) = &event.item
                    && native
                {
                    let key = (event.turn_id.clone(), user.id.clone());
                    let boundary = self
                        .native_user_boundaries
                        .get(&key)
                        .copied()
                        .or(paired_user_boundary)
                        .unwrap_or_else(|| self.start_boundary(index));
                    self.native_user_boundaries.insert(key, boundary);
                    self.turn_boundaries.insert(event.turn_id.clone(), boundary);
                    self.record_boundaries[index] = Some(boundary);
                    if self.boundaries[boundary].message_id.is_none() {
                        self.boundaries[boundary].message_id =
                            Some(ResponseItemId::from_server(user.id.clone()));
                    }
                } else {
                    self.assign_targeted_record(index, Some(event.turn_id.as_str()));
                }
            }
            RolloutItem::EventMsg(event) => {
                self.assign_targeted_record(index, explicit_event_turn_id(event));
            }
            RolloutItem::InterAgentCommunication(_) => {
                self.start_boundary(index);
            }
            RolloutItem::InterAgentCommunicationMetadata { .. } => {
                let boundary = self.start_boundary(index);
                self.pending_delivery_boundary = Some(boundary);
            }
            RolloutItem::Compacted(item) => {
                let owner = self
                    .active_turn_id
                    .as_deref()
                    .and_then(|turn_id| self.turn_boundaries.get(turn_id).copied());
                self.record_boundaries[index] = owner;
                self.compactions.push(CompactionFrame {
                    record_index: index,
                    boundary_depth: self.boundary_stack.len(),
                    owner,
                    has_replacement_history: item.replacement_history.is_some(),
                    retained_context_edits: Vec::new(),
                    rollback_turns: Vec::new(),
                });
            }
            RolloutItem::TurnContext(_) => {
                if self.active_turn_id.is_some()
                    && self
                        .active_turn_id
                        .as_deref()
                        .is_none_or(|turn_id| !self.turn_boundaries.contains_key(turn_id))
                {
                    self.pending_turn_records.push(index);
                }
            }
            RolloutItem::TokenUsageRecord(record) => {
                self.assign_targeted_record(index, Some(record.turn_id.as_str()));
            }
            RolloutItem::WorldState(_) | RolloutItem::RealtimeItem(_) => {}
            RolloutItem::RetainedContext(codex_rollout::RetainedContextEvent::VerifiedAnswer {
                answer,
                acceptance_order,
            }) => {
                let source = (answer.turn_id.clone(), answer.call_id.clone());
                // A late answer still belongs to its call's instruction boundary, even
                // if the same running turn has since received another user steer.
                if let Some(boundary) = self.call_boundaries.get(&source) {
                    self.record_boundaries[index] = *boundary;
                } else {
                    self.assign_targeted_record(index, Some(&answer.turn_id));
                }
                self.call_boundaries
                    .insert(source, self.record_boundaries[index]);
                self.retained_fact_sources.push(RetainedFactSource {
                    record_index: index,
                    turn_id: answer.turn_id.clone(),
                    acceptance_order: *acceptance_order,
                });
            }
            RolloutItem::SecurityRiskScore(_) => self.record_boundaries[index] = None,
        }

        Ok(())
    }

    pub(super) fn finish(self) -> RollbackPlan {
        let RollbackPlanner {
            record_boundaries,
            boundaries,
            compactions,
            model_replay,
            turn_boundaries,
            ..
        } = self;
        let replay_anchor = model_replay.finish().empty_replacement_history_compaction;
        let boundary_alive = boundaries
            .into_iter()
            .map(|boundary| boundary.alive)
            .collect::<Vec<_>>();
        let mut compacted_rollbacks = HashMap::new();
        let mut retained_context_edits = HashMap::new();
        for frame in compactions {
            let is_replay_anchor = Some(frame.record_index) == replay_anchor;
            if !is_replay_anchor
                && frame
                    .owner
                    .is_some_and(|boundary| !boundary_alive[boundary])
            {
                continue;
            }
            if !frame.retained_context_edits.is_empty() {
                retained_context_edits.insert(frame.record_index, frame.retained_context_edits);
            }
            if !frame.rollback_turns.is_empty() {
                compacted_rollbacks.insert(frame.record_index, frame.rollback_turns);
            }
        }
        let removed_turn_ids = turn_boundaries
            .into_iter()
            .filter_map(|(turn_id, boundary)| (!boundary_alive[boundary]).then_some(turn_id))
            .collect();
        RollbackPlan {
            record_boundaries,
            boundary_alive,
            empty_replacement_history_compaction: replay_anchor,
            compacted_rollbacks,
            retained_context_edits,
            removed_turn_ids,
        }
    }

    fn start_boundary(&mut self, index: usize) -> usize {
        let boundary = self.boundaries.len();
        self.boundaries.push(InstructionBoundary {
            record_index: index,
            message_id: None,
            fingerprint: None,
            input_source: RetainedInputSource::Local(None),
            alive: true,
        });
        let had_prior_boundary = !self.boundary_stack.is_empty();
        if had_prior_boundary {
            for pending_index in self.pending_context_records.drain(..) {
                self.record_boundaries[pending_index] = Some(boundary);
            }
        } else {
            self.pending_context_records.clear();
        }
        for pending_index in self.pending_turn_records.drain(..) {
            self.record_boundaries[pending_index] = Some(boundary);
        }
        self.record_boundaries[index] = Some(boundary);
        self.boundary_stack.push(boundary);
        self.bind_active_turn(boundary);
        boundary
    }

    fn bind_active_turn(&mut self, boundary: usize) {
        if let Some(turn_id) = self.active_turn_id.as_ref() {
            self.turn_boundaries.insert(turn_id.clone(), boundary);
        }
    }

    fn assign_targeted_record(&mut self, index: usize, turn_id: Option<&str>) {
        if let Some(boundary) = turn_id.and_then(|turn_id| self.turn_boundaries.get(turn_id)) {
            self.record_boundaries[index] = Some(*boundary);
        } else if self.active_turn_id.is_some()
            && self
                .active_turn_id
                .as_deref()
                .is_none_or(|turn_id| !self.turn_boundaries.contains_key(turn_id))
        {
            self.pending_turn_records.push(index);
        }
    }

    fn apply_rollback(&mut self, num_turns: u32) -> ThreadStoreResult<()> {
        let count = usize::try_from(num_turns).unwrap_or(usize::MAX);
        if count == 0 {
            return Ok(());
        }
        let depth_before = self.boundary_stack.len();
        let mut removed_boundaries = HashSet::new();
        let mut first_removed_boundary = None;
        for _ in 0..count {
            let Some(boundary) = self.boundary_stack.pop() else {
                break;
            };
            self.boundaries[boundary].alive = false;
            removed_boundaries.insert(boundary);
            first_removed_boundary = Some(boundary);
        }
        if let Some(boundary) = first_removed_boundary {
            let source_record_index = self.boundaries[boundary].record_index;
            let first_removed_message_id = self.boundaries[boundary].message_id.clone();
            let input_source = self.boundaries[boundary].input_source;
            let acceptance_order = input_source.acceptance_order();
            if let Some(order) = acceptance_order {
                // An answer may have been persisted before the queued instruction
                // accepted ahead of it. Both are removed at that acceptance boundary.
                for fact in &self.retained_fact_sources {
                    if fact
                        .acceptance_order
                        .is_some_and(|accepted| accepted >= order)
                    {
                        self.record_boundaries[fact.record_index] = Some(boundary);
                    }
                }
            }
            let removed_turn_ids = self
                .turn_boundaries
                .iter()
                .filter(|(_, boundary)| removed_boundaries.contains(*boundary))
                .map(|(turn_id, _)| turn_id.clone())
                .chain(
                    self.retained_fact_sources
                        .iter()
                        .filter(|fact| {
                            self.record_boundaries[fact.record_index]
                                .is_some_and(|boundary| removed_boundaries.contains(&boundary))
                        })
                        .map(|fact| fact.turn_id.clone()),
                )
                .collect::<Vec<_>>();
            // Accepted answers can precede a queued instruction in the rollout,
            // including in checkpoints. Legacy evidence uses the recorded boundary.
            // Later checkpoints have not been observed yet and already reflect this rollback.
            let known_answer_sources = self.call_boundaries.keys().cloned().collect();
            let removed_answer_sources = self
                .call_boundaries
                .iter()
                .filter(|&(_, boundary)| {
                    boundary
                        .as_ref()
                        .is_some_and(|boundary| !self.boundaries[*boundary].alive)
                })
                .map(|(source, _)| source.clone())
                .collect();
            let edit = Arc::new(RetainedContextEdit {
                removed_turn_ids,
                first_removed_message_id,
                source_record_index,
                first_removed_fingerprint: self.boundaries[boundary].fingerprint,
                input_source,
                force_ordered_rollback: acceptance_order.is_some(),
                known_answer_sources,
                removed_answer_sources,
            });
            for frame in self.compactions.iter_mut().rev().take_while(|frame| {
                acceptance_order.is_some() || frame.record_index >= source_record_index
            }) {
                frame.retained_context_edits.push(Arc::clone(&edit));
            }
        }
        let compaction_index = self.compactions.iter().rposition(|frame| {
            frame
                .owner
                .is_none_or(|boundary| self.boundaries[boundary].alive)
        });
        if let Some(compaction_index) = compaction_index {
            let frame = &mut self.compactions[compaction_index];
            let post_compaction_turns = depth_before.saturating_sub(frame.boundary_depth);
            let remaining = count.saturating_sub(post_compaction_turns);
            if remaining > 0 {
                if !frame.has_replacement_history {
                    return Err(migration_error(
                        "legacy rollback crosses a compaction without replacement history",
                    ));
                }
                frame
                    .rollback_turns
                    .push(u32::try_from(remaining).unwrap_or(u32::MAX));
            }
        }
        self.active_turn_id = None;
        self.pending_turn_records.clear();
        self.pending_context_records.clear();
        self.pending_user_response = None;
        self.pending_delivery_boundary = None;
        Ok(())
    }
}

fn instruction_fingerprint(item: &ResponseItem) -> ThreadStoreResult<[u8; 32]> {
    // JSON objects can deserialize with different key order. Preserve item identity without
    // retaining every ID-less instruction's user text or inline image in the rollback plan.
    let mut value = serde_json::to_value(item)
        .map_err(|err| migration_error(format!("serialize rollback instruction: {err}")))?;
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value)
        .map_err(|err| migration_error(format!("serialize rollback instruction: {err}")))?;
    Ok(Sha256::digest(bytes).into())
}

fn explicit_event_turn_id(event: &EventMsg) -> Option<&str> {
    match event {
        EventMsg::ExecCommandEnd(event) => Some(event.turn_id.as_str()),
        EventMsg::PatchApplyEnd(event) => Some(event.turn_id.as_str()),
        EventMsg::DynamicToolCallResponse(event) => Some(event.turn_id.as_str()),
        EventMsg::EnteredReviewMode(event) => event.turn_id.as_deref(),
        EventMsg::ExitedReviewMode(event) => event.turn_id.as_deref(),
        _ => None,
    }
    .filter(|turn_id| !turn_id.is_empty())
}

fn user_response_matches_event(content: &[ContentItem], event: &UserMessageEvent) -> bool {
    let mut text = String::new();
    let mut images = Vec::new();
    let mut file_ids = Vec::new();
    let mut image_order = Vec::new();
    let mut audio = Vec::new();
    for item in content {
        match item {
            ContentItem::InputText { text: item_text } => text.push_str(item_text),
            ContentItem::InputImage { image, .. } => match image {
                ImageReference::Inline { image_url } => {
                    image_order.push(UserMessageImageKind::Inline);
                    images.push(image_url.as_str());
                }
                ImageReference::File { file_id } => {
                    image_order.push(UserMessageImageKind::File);
                    file_ids.push(file_id.as_str());
                }
            },
            ContentItem::InputAudio { audio_url } => audio.push(audio_url.as_str()),
            ContentItem::OutputText { .. } => return false,
        }
    }
    text == event.message
        && (!event.has_complete_image_order() || image_order == event.image_order)
        && images
            == event
                .images
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        && file_ids
            == event
                .file_ids
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        && audio
            == event
                .audio
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        && event.local_images.is_empty()
        && event.local_audio.is_empty()
}
