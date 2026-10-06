use super::*;
use crate::context::GuardianContextMode;
use crate::context::is_guardian_context_message;
use crate::context::world_state::WorldStateSnapshot;
use crate::context_manager::is_user_turn_boundary;
use codex_guardian_context::TranscriptHistory;
use codex_history::ResponseItemEnvelope;
use codex_history::ReviewInputRecord;
use codex_history::ReviewTranscriptApplicability;
use codex_history::is_api_message;
use codex_protocol::protocol::SessionContextWindow;
use codex_rollout::validated_segment_state_checkpoint;
use codex_utils_output_truncation::TruncationPolicy;
use uuid::Uuid;

// Return value of `Session::reconstruct_history_from_rollout`, bundling the rebuilt history with
// the resume/fork hydration metadata derived from the same replay.
#[derive(Debug, PartialEq)]
pub(super) struct RolloutReconstruction {
    pub(super) history: Vec<ResponseItemEnvelope>,
    pub(super) retained_context: codex_history::RetainedContext,
    pub(super) guardian_history: Option<codex_history::GuardianHistoryCheckpoint>,
    pub(super) last_started_turn_id: Option<String>,
    pub(super) previous_turn_settings: Option<PreviousTurnSettings>,
    pub(super) reference_context_item: Option<TurnContextItem>,
    pub(super) world_state_baseline: Option<WorldStateSnapshot>,
    pub(super) window_number: u64,
    pub(super) first_window_id: Option<Uuid>,
    pub(super) previous_window_id: Option<Uuid>,
    pub(super) window_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy)]
struct ReconstructedWindow {
    number: u64,
    first_id: Option<Uuid>,
    previous_id: Option<Uuid>,
    id: Option<Uuid>,
}

#[derive(Debug, Default)]
enum TurnReferenceContextItem {
    /// No `TurnContextItem` has been seen for this replay span yet.
    ///
    /// This differs from `Cleared`: `NeverSet` means there is no evidence this turn ever
    /// established a baseline, while `Cleared` means a baseline existed and a later compaction
    /// invalidated it. Only the latter must emit an explicit clearing segment for resume/fork
    /// hydration.
    #[default]
    NeverSet,
    /// A previously established baseline was invalidated by later compaction.
    Cleared,
    /// The latest baseline established by this replay span.
    Latest(Box<TurnContextItem>),
}

#[derive(Debug, Clone, Copy)]
// The selected compaction and its replay tail must belong to the same surviving segment.
struct ReplayCheckpoint<'a> {
    compacted: &'a CompactedItem,
    suffix: &'a [RolloutItem],
}

/// Selects the newest compaction that can safely bound replay.
///
/// Returns `None` when reconstruction must replay all supplied items, either because there is no
/// compaction or the newest compaction cannot bound replay.
fn select_input_compaction(rollout_items: &[RolloutItem]) -> Option<ReplayCheckpoint<'_>> {
    // Only the newest compaction can bound replay. If it is incomplete, an older compaction
    // cannot replace the history or window state that the newer one may have changed.
    let (index, compacted) = rollout_items
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, item)| match item {
            RolloutItem::Compacted(compacted) => Some((index, compacted)),
            _ => None,
        })?;
    // Old unmarked compactions still need predecessor turn metadata. Modern metadata remains
    // authoritative even when a later rollback removes items from its replacement history.
    if compacted.replacement_history.is_none()
        || compacted.window_number.is_none()
        || compacted.resume_metadata.is_none()
        || (compacted.segment_state_checkpoint.is_some()
            && validated_segment_state_checkpoint(compacted, &rollout_items[index + 1..]).is_none())
    {
        return None;
    }
    Some(ReplayCheckpoint {
        compacted,
        suffix: &rollout_items[index + 1..],
    })
}

#[derive(Debug, Default)]
struct ActiveReplaySegment<'a> {
    turn_id: Option<String>,
    turn_completed: bool,
    counts_as_user_turn: bool,
    previous_turn_settings: Option<PreviousTurnSettings>,
    /// Checkpoint value, including an explicit `None` that clears older settings.
    checkpoint_previous_turn_settings: Option<Option<PreviousTurnSettings>>,
    has_segment_state_checkpoint: bool,
    reference_context_item: TurnReferenceContextItem,
    world_state_replay: Vec<&'a RolloutItem>,
    history_checkpoint: Option<ReplayCheckpoint<'a>>,
    window: Option<ReconstructedWindow>,
}

fn turn_ids_are_compatible(active_turn_id: Option<&str>, item_turn_id: Option<&str>) -> bool {
    active_turn_id
        .is_none_or(|turn_id| item_turn_id.is_none_or(|item_turn_id| item_turn_id == turn_id))
}

#[expect(
    clippy::too_many_arguments,
    reason = "segment replay updates distinct reconstruction accumulators from one finalized segment"
)]
fn finalize_active_segment<'a>(
    active_segment: ActiveReplaySegment<'a>,
    history_checkpoint: &mut Option<ReplayCheckpoint<'a>>,
    previous_turn_settings: &mut Option<PreviousTurnSettings>,
    previous_turn_settings_resolved: &mut bool,
    require_completed_turn_settings: bool,
    reference_context_item: &mut TurnReferenceContextItem,
    world_state_replay: &mut Vec<&'a RolloutItem>,
    window: &mut Option<ReconstructedWindow>,
    pending_rollback_turns: &mut usize,
) {
    // Thread rollback drops the newest surviving real user-message boundaries. In replay, that
    // means skipping the next finalized segments that contain a non-contextual
    // `EventMsg::UserMessage`.
    if *pending_rollback_turns > 0 {
        if active_segment.counts_as_user_turn {
            *pending_rollback_turns -= 1;
        }
        return;
    }

    // Full world-state snapshots are persisted after installing initial context. They still
    // establish a baseline when a child fork removes the parent turn's agent message. Do not
    // count these context-only segments as user turns for rollback, or use a snapshot from
    // before the segment's latest compaction.
    let has_context_baseline = active_segment.counts_as_user_turn
        || active_segment
            .world_state_replay
            .iter()
            .take_while(|item| !matches!(item, RolloutItem::Compacted(_)))
            .any(|item| matches!(item, RolloutItem::WorldState(state) if state.full));
    world_state_replay.extend(active_segment.world_state_replay);

    // A surviving replacement-history compaction is a complete history base. Once we
    // know the newest surviving one, older rollout items do not affect rebuilt history.
    if history_checkpoint.is_none()
        && let Some(segment_history_checkpoint) = active_segment.history_checkpoint
    {
        *history_checkpoint = Some(segment_history_checkpoint);
    }

    if window.is_none() {
        *window = active_segment.window;
    }

    // Restore settings from the newest surviving context baseline or certified checkpoint.
    if !*previous_turn_settings_resolved {
        // The checkpoint's comparison baseline can differ from its reference TurnContext.
        // Only a newer user turn supersedes the certified previous-turn settings.
        if (!active_segment.counts_as_user_turn
            || (require_completed_turn_settings && !active_segment.turn_completed))
            && let Some(settings) = active_segment.checkpoint_previous_turn_settings
        {
            *previous_turn_settings = settings;
            *previous_turn_settings_resolved = true;
        } else if has_context_baseline
            && (!require_completed_turn_settings || active_segment.turn_completed)
            && let Some(settings) = active_segment.previous_turn_settings
        {
            *previous_turn_settings = Some(settings);
            *previous_turn_settings_resolved = true;
        } else if let Some(settings) = active_segment.checkpoint_previous_turn_settings {
            *previous_turn_settings = settings;
            *previous_turn_settings_resolved = true;
        }
    }

    // `reference_context_item` comes from the newest surviving context baseline, or
    // from a surviving compaction that explicitly cleared that baseline.
    if matches!(reference_context_item, TurnReferenceContextItem::NeverSet)
        && (has_context_baseline
            || active_segment.has_segment_state_checkpoint
            || matches!(
                active_segment.reference_context_item,
                TurnReferenceContextItem::Cleared
            ))
    {
        *reference_context_item = active_segment.reference_context_item;
    }
}

/// The checkpoint and metadata selected by the same rollback-aware reverse replay.
struct RolloutReplaySelection<'a> {
    history_checkpoint: Option<ReplayCheckpoint<'a>>,
    last_started_turn_id: Option<String>,
    previous_turn_settings: Option<PreviousTurnSettings>,
    reference_context_item: TurnReferenceContextItem,
    world_state_replay: Vec<&'a RolloutItem>,
    window: ReconstructedWindow,
}

fn select_rollout_replay(rollout_items: &[RolloutItem]) -> RolloutReplaySelection<'_> {
    // Select the compaction and suffix that can affect reconstruction.
    let has_legacy_compaction_without_window_number = rollout_items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.window_number.is_none())
    });
    let initial_window = if has_legacy_compaction_without_window_number {
        None
    } else {
        rollout_items.iter().find_map(|item| match item {
            RolloutItem::SessionMeta(session_meta) => session_meta
                .meta
                .context_window
                .as_ref()
                .and_then(reconstructed_window_from_session_context_window),
            _ => None,
        })
    };
    let input_checkpoint = select_input_compaction(rollout_items);
    let replay_items = input_checkpoint.map_or(rollout_items, |checkpoint| {
        if checkpoint.compacted.segment_state_checkpoint.is_some() {
            // Include the certificate itself so reverse replay applies its dispositions.
            let index = rollout_items.len() - checkpoint.suffix.len() - 1;
            &rollout_items[index..]
        } else {
            checkpoint.suffix
        }
    });
    let mut resume_metadata =
        input_checkpoint.and_then(|checkpoint| checkpoint.compacted.resume_metadata.as_ref());
    let mut history_checkpoint = input_checkpoint;
    let mut window = input_checkpoint
        .and_then(|checkpoint| reconstructed_window_from_compaction(checkpoint.compacted));

    let require_completed_turn_settings = rollout_items
        .iter()
        .rev()
        .find_map(|item| match item {
            RolloutItem::Compacted(compacted) => Some(compacted.resume_metadata.is_some()),
            _ => None,
        })
        .unwrap_or(false);
    let mut last_started_turn_id = None;

    let mut previous_turn_settings = None;
    let mut previous_turn_settings_resolved = false;
    let mut reference_context_item = TurnReferenceContextItem::NeverSet;
    let mut world_state_replay = Vec::new();
    // Rollback is "drop the newest N user turns". While scanning in reverse, that becomes
    // "skip the next N user-turn segments we finalize".
    let mut pending_rollback_turns = 0usize;
    // Reverse replay accumulates rollout items into the newest in-progress turn segment until
    // we hit its matching `TurnStarted`, at which point the segment can be finalized.
    let mut active_segment: Option<ActiveReplaySegment<'_>> = None;

    for (index, item) in replay_items.iter().enumerate().rev() {
        let mut reached_segment_state_checkpoint = false;
        match item {
            RolloutItem::Compacted(compacted) => {
                let active_segment =
                    active_segment.get_or_insert_with(ActiveReplaySegment::default);
                if let Some(checkpoint) =
                    validated_segment_state_checkpoint(compacted, &replay_items[index + 1..])
                {
                    active_segment.has_segment_state_checkpoint = true;
                    active_segment.checkpoint_previous_turn_settings =
                        Some(checkpoint.previous_turn_settings.as_ref().map(|settings| {
                            PreviousTurnSettings {
                                model: settings.model.clone(),
                                comp_hash: settings.comp_hash.clone(),
                                cyber_access_program: settings.cyber_access_program,
                                realtime_active: settings.realtime_active,
                            }
                        }));
                    reached_segment_state_checkpoint = pending_rollback_turns == 0;
                } else if compacted.segment_state_checkpoint.is_none()
                    && compacted.replacement_history.is_some()
                    && compacted.window_number.is_some()
                    && let Some(metadata) = &compacted.resume_metadata
                {
                    active_segment.checkpoint_previous_turn_settings =
                        Some(metadata.previous_turn_settings.clone());
                    reached_segment_state_checkpoint = pending_rollback_turns == 0;
                }
                if reached_segment_state_checkpoint {
                    resume_metadata = compacted.resume_metadata.as_ref();
                }
                active_segment.world_state_replay.push(item);
                if active_segment.window.is_none()
                    && let Some(compaction_window) = reconstructed_window_from_compaction(compacted)
                {
                    active_segment.window = Some(compaction_window);
                }
                // Looking backward, compaction clears any older baseline unless a newer
                // `TurnContextItem` in this same segment has already re-established it.
                if matches!(
                    active_segment.reference_context_item,
                    TurnReferenceContextItem::NeverSet
                ) {
                    active_segment.reference_context_item = TurnReferenceContextItem::Cleared;
                }
                if active_segment.history_checkpoint.is_none()
                    && compacted.replacement_history.is_some()
                {
                    active_segment.history_checkpoint = Some(ReplayCheckpoint {
                        compacted,
                        suffix: &replay_items[index + 1..],
                    });
                }
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                pending_rollback_turns = pending_rollback_turns
                    .saturating_add(usize::try_from(rollback.num_turns).unwrap_or(usize::MAX));
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                let active_segment =
                    active_segment.get_or_insert_with(ActiveReplaySegment::default);
                active_segment.turn_completed = true;
                // Reverse replay often sees `TurnComplete` before any turn-scoped metadata.
                // Capture the turn id early so later `TurnContext` / abort items can match it.
                if active_segment.turn_id.is_none() {
                    active_segment.turn_id = Some(event.turn_id.clone());
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                if let Some(active_segment) = active_segment.as_mut() {
                    if active_segment.turn_id.is_none()
                        && let Some(turn_id) = &event.turn_id
                    {
                        active_segment.turn_id = Some(turn_id.clone());
                    }
                } else if let Some(turn_id) = &event.turn_id {
                    active_segment = Some(ActiveReplaySegment {
                        turn_id: Some(turn_id.clone()),
                        ..Default::default()
                    });
                }
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                let active_segment =
                    active_segment.get_or_insert_with(ActiveReplaySegment::default);
                active_segment.counts_as_user_turn = true;
            }
            RolloutItem::TurnContext(ctx) => {
                let active_segment =
                    active_segment.get_or_insert_with(ActiveReplaySegment::default);
                // `TurnContextItem` can attach metadata to an existing segment, but only a
                // real `UserMessage` event should make the segment count as a user turn.
                if active_segment.turn_id.is_none() {
                    active_segment.turn_id = ctx.turn_id.clone();
                }
                if turn_ids_are_compatible(
                    active_segment.turn_id.as_deref(),
                    ctx.turn_id.as_deref(),
                ) {
                    active_segment.previous_turn_settings = Some(PreviousTurnSettings {
                        model: ctx.model.clone(),
                        cyber_access_program: ctx.cyber_access_program,
                        comp_hash: ctx.comp_hash.clone(),
                        realtime_active: ctx.realtime_active,
                    });
                    if matches!(
                        active_segment.reference_context_item,
                        TurnReferenceContextItem::NeverSet
                    ) {
                        active_segment.reference_context_item =
                            TurnReferenceContextItem::Latest(Box::new(ctx.clone()));
                    }
                }
            }
            RolloutItem::WorldState(_) => {
                let active_segment =
                    active_segment.get_or_insert_with(ActiveReplaySegment::default);
                active_segment.world_state_replay.push(item);
            }
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                // Continuation identity follows the latest started turn even when its
                // conversation items are later rolled back, matching the runtime contract.
                last_started_turn_id.get_or_insert_with(|| event.turn_id.clone());
                // `TurnStarted` is the oldest boundary of the active reverse segment.
                if active_segment.as_ref().is_some_and(|active_segment| {
                    turn_ids_are_compatible(
                        active_segment.turn_id.as_deref(),
                        Some(event.turn_id.as_str()),
                    )
                }) && let Some(active_segment) = active_segment.take()
                {
                    finalize_active_segment(
                        active_segment,
                        &mut history_checkpoint,
                        &mut previous_turn_settings,
                        &mut previous_turn_settings_resolved,
                        require_completed_turn_settings,
                        &mut reference_context_item,
                        &mut world_state_replay,
                        &mut window,
                        &mut pending_rollback_turns,
                    );
                }
            }
            RolloutItem::ResponseItem(response_item) => {
                let active_segment =
                    active_segment.get_or_insert_with(ActiveReplaySegment::default);
                active_segment.counts_as_user_turn |= is_user_turn_boundary(&response_item.item);
            }
            RolloutItem::InterAgentCommunication(_) => {
                let active_segment =
                    active_segment.get_or_insert_with(ActiveReplaySegment::default);
                active_segment.counts_as_user_turn = true;
            }
            RolloutItem::EventMsg(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::SessionMeta(_)
            | RolloutItem::RealtimeItem(_)
            | RolloutItem::RetainedContext(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::TokenUsageRecord(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. } => {}
        }
        // Selecting replacement history does not mean its full WorldState was visited.
        // Resolving newer turn metadata does not establish the checkpoint's baseline.
        if reached_segment_state_checkpoint {
            if let Some(active_segment) = active_segment.take() {
                finalize_active_segment(
                    active_segment,
                    &mut history_checkpoint,
                    &mut previous_turn_settings,
                    &mut previous_turn_settings_resolved,
                    require_completed_turn_settings,
                    &mut reference_context_item,
                    &mut world_state_replay,
                    &mut window,
                    &mut pending_rollback_turns,
                );
            }
            break;
        }
    }

    if let Some(mut active_segment) = active_segment.take() {
        // A companion turn context only restores the context baseline. Once that turn
        // completes, its settings are newer than the compaction metadata.
        if resume_metadata.is_some() && !active_segment.turn_completed {
            active_segment.previous_turn_settings = None;
        }
        finalize_active_segment(
            active_segment,
            &mut history_checkpoint,
            &mut previous_turn_settings,
            &mut previous_turn_settings_resolved,
            require_completed_turn_settings,
            &mut reference_context_item,
            &mut world_state_replay,
            &mut window,
            &mut pending_rollback_turns,
        );
    }

    if !previous_turn_settings_resolved {
        previous_turn_settings =
            resume_metadata.and_then(|metadata| metadata.previous_turn_settings.clone());
    }
    let last_started_turn_id = last_started_turn_id
        .or_else(|| resume_metadata.and_then(|metadata| metadata.last_started_turn_id.clone()));

    // Bounded input can start at a checkpoint containing the user turns a later rollback
    // removes. If reverse replay discarded that checkpoint, seed forward replay from it
    // rather than losing the retained turns. Never prefer it over available original items
    // or a surviving checkpoint; those preserve upstream rollback semantics.
    let history_checkpoint = history_checkpoint.or_else(|| {
        let (index, item) = rollout_items.iter().enumerate().find(|(_, item)| {
            matches!(
                item,
                RolloutItem::ResponseItem(_)
                    | RolloutItem::InterAgentCommunication(_)
                    | RolloutItem::Compacted(_)
            )
        })?;
        let RolloutItem::Compacted(compacted) = item else {
            return None;
        };
        compacted.replacement_history.as_ref()?;
        Some(ReplayCheckpoint {
            compacted,
            suffix: &rollout_items[index + 1..],
        })
    });

    let fallback_window_number = u64::try_from(
        rollout_items
            .iter()
            .filter(|item| matches!(item, RolloutItem::Compacted(_)))
            .count(),
    )
    .unwrap_or(u64::MAX);

    RolloutReplaySelection {
        history_checkpoint,
        last_started_turn_id,
        previous_turn_settings,
        reference_context_item,
        world_state_replay,
        window: window.or(initial_window).unwrap_or(ReconstructedWindow {
            number: fallback_window_number,
            first_id: None,
            previous_id: None,
            id: None,
        }),
    }
}

/// Selection must match reconstruction even when the newest compaction cannot bound replay.
pub(super) async fn resolve_review_input_for_reconstruction(
    codex_home: &std::path::Path,
    rollout_items: &mut [RolloutItem],
) -> std::io::Result<()> {
    if !rollout_items.iter().any(|item| match item {
        RolloutItem::Compacted(compacted) => compacted
            .retained_context_replay
            .as_ref()
            .is_some_and(|replay| replay.review_input.is_some()),
        _ => false,
    }) {
        return Ok(());
    }
    let selected_index = select_rollout_replay(rollout_items)
        .history_checkpoint
        .map(|checkpoint| rollout_items.len() - checkpoint.suffix.len() - 1);
    let Some(selected_index) = selected_index else {
        return Ok(());
    };
    let RolloutItem::Compacted(compacted) = &mut rollout_items[selected_index] else {
        unreachable!("reverse replay selects a compaction checkpoint")
    };
    codex_rollout::resolve_review_input(codex_home, compacted).await
}

/// Replays only the original review transcript, using the resumed model's processing policy.
pub(super) fn replay_review_input(
    records: &[ReviewInputRecord],
    policy: TruncationPolicy,
    source_is_non_root_agent: bool,
    guardian_context_mode: GuardianContextMode,
) -> Option<TranscriptHistory> {
    let independent = guardian_context_mode == GuardianContextMode::Independent;
    let mut history = independent.then(TranscriptHistory::default);
    for record in records {
        match record {
            ReviewInputRecord::Baseline {
                applicability,
                root_retains_legacy_transcript,
                history: baseline,
            } => {
                let mut baseline_history = TranscriptHistory::new(0);
                baseline_history.reset(baseline.0.iter());
                let applies = independent
                    || match applicability {
                        ReviewTranscriptApplicability::Both
                        | ReviewTranscriptApplicability::ThreadOwned => true,
                        ReviewTranscriptApplicability::Legacy => {
                            // Suffix eviction can make a complete checkpoint incomplete. Only
                            // its original decision may select a root's legacy transcript.
                            !source_is_non_root_agent
                                && *root_retains_legacy_transcript == Some(true)
                        }
                    };
                history = applies.then_some(baseline_history);
            }
            ReviewInputRecord::ResponseItem { response } => {
                if let Some(history) = &mut history
                    && is_api_message(&response.item, response.metadata.as_ref())
                    && !is_guardian_context_message(&response.item)
                {
                    let processed = ContextManager::process_response_item_for_history(
                        &response.item,
                        response.metadata.as_ref(),
                        policy,
                    );
                    history.record(&ResponseItemEnvelope {
                        item: processed,
                        metadata: response.metadata.clone(),
                    });
                }
            }
            ReviewInputRecord::Rollback {
                boundary,
                boundary_metadata,
            } => {
                if let Some(history) = &mut history {
                    history.truncate_before(&ResponseItemEnvelope {
                        item: boundary.clone(),
                        metadata: boundary_metadata.clone(),
                    });
                }
            }
        }
    }
    history
}

impl Session {
    pub(super) async fn resolve_rollout_review_input(
        &self,
        rollout_items: &mut [RolloutItem],
    ) -> CodexResult<()> {
        let codex_home = self
            .state
            .lock()
            .await
            .session_configuration
            .codex_home()
            .to_path_buf();
        resolve_review_input_for_reconstruction(&codex_home, rollout_items).await?;
        Ok(())
    }

    pub(super) async fn reconstruct_history_from_rollout(
        &self,
        turn_context: &TurnContext,
        rollout_items: &[RolloutItem],
    ) -> RolloutReconstruction {
        let RolloutReplaySelection {
            history_checkpoint,
            last_started_turn_id,
            previous_turn_settings,
            reference_context_item,
            mut world_state_replay,
            window,
        } = select_rollout_replay(rollout_items);

        // Build model-visible history from the selected compaction and its newer suffix.
        let mut history = ContextManager::for_session(
            &turn_context.session_source,
            &turn_context.config.features,
        );
        let mut saw_legacy_compaction_without_replacement_history = false;
        if let Some(checkpoint) = history_checkpoint
            && let Some(items) = &checkpoint.compacted.replacement_history
        {
            history.replace_annotated(items.clone());
            // Keep the backup during replay; the installing session resolves its reviewer.
            if let Some(replay) = &checkpoint.compacted.retained_context_replay {
                let retained = if turn_context.session_source.is_non_root_agent() {
                    &replay.thread_owned_worker
                } else {
                    &replay.thread_owned_root
                };
                history.restore_replayed_review_context(
                    retained,
                    checkpoint.compacted.guardian_history.as_ref(),
                    /*reviewer_compaction_hash*/ None,
                );
                if replay.review_input.is_some() {
                    let records = replay
                        .resolved_review_input
                        .as_ref()
                        .expect("selected review input must resolve before rollout reconstruction");
                    let review_history = replay_review_input(
                        records,
                        turn_context.model_info().truncation_policy.into(),
                        turn_context.session_source.is_non_root_agent(),
                        GuardianContextMode::from_history(
                            history.conversation_history_snapshot().as_ref(),
                        ),
                    );
                    history.restore_replayed_guardian_history(review_history);
                }
            } else {
                history.restore_review_context(
                    checkpoint.compacted.retained_context.as_ref(),
                    checkpoint.compacted.guardian_history.as_ref(),
                    /*reviewer_compaction_hash*/ None,
                );
            }
        }
        let rollout_suffix =
            history_checkpoint.map_or(rollout_items, |checkpoint| checkpoint.suffix);
        for item in rollout_suffix {
            match item {
                RolloutItem::RetainedContext(event) => {
                    history.record_retained_context(event);
                }
                RolloutItem::ResponseItem(response_item) => {
                    history.replay_annotated_item(
                        response_item,
                        turn_context.model_info().truncation_policy.into(),
                    );
                }
                RolloutItem::InterAgentCommunication(communication) => {
                    let response_item = communication.to_model_input_item();
                    history.record_items(
                        std::iter::once(&response_item),
                        turn_context.model_info().truncation_policy.into(),
                    );
                }
                RolloutItem::InterAgentCommunicationMetadata { .. } => {}
                RolloutItem::Compacted(compacted) => {
                    // Reverse replay already chose the newest surviving compaction. Any newer
                    // replacement compaction belongs to a rolled-back turn; replay its original
                    // items so the rollback can still find the removed user boundary.
                    if compacted.replacement_history.is_none() {
                        saw_legacy_compaction_without_replacement_history = true;
                        // Legacy rollouts without `replacement_history` should rebuild the
                        // historical TurnContext at the correct insertion point from persisted
                        // `TurnContextItem`s. These are rare enough that we currently just clear
                        // `reference_context_item`, reinject canonical context at the end of the
                        // resumed conversation, and accept the temporary out-of-distribution
                        // prompt shape.
                        // TODO(ccunningham): if we drop support for None replacement_history compaction items,
                        // we can get rid of this second loop entirely and just build `history` directly in the first loop.
                        let user_messages =
                            compact::collect_annotated_user_messages(history.annotated_items());
                        let rebuilt = compact::build_compacted_history(
                            Vec::new(),
                            &user_messages,
                            &compacted.message,
                        );
                        let retained_context = history.retained_context().clone();
                        history.replace_annotated(rebuilt);
                        history.restore_retained_context(Some(&retained_context));
                    }
                }
                RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                    history.drop_last_n_user_turns(rollback.num_turns);
                }
                RolloutItem::EventMsg(_)
                | RolloutItem::RolloutReference(_)
                | RolloutItem::TurnContext(_)
                | RolloutItem::RealtimeItem(_)
                | RolloutItem::WorldState(_)
                | RolloutItem::SecurityRiskScore(_)
                | RolloutItem::TokenUsageRecord(_)
                | RolloutItem::SessionMeta(_) => {}
            }
        }

        let reference_context_item = match reference_context_item {
            TurnReferenceContextItem::NeverSet | TurnReferenceContextItem::Cleared => None,
            TurnReferenceContextItem::Latest(turn_reference_context_item) => {
                Some(*turn_reference_context_item)
            }
        };
        let reference_context_item = if saw_legacy_compaction_without_replacement_history {
            None
        } else {
            reference_context_item
        };

        // Replay the collected world-state records chronologically so compaction resets and merge
        // patches keep their original meaning.
        world_state_replay.reverse();
        let mut world_state_baseline: Option<WorldStateSnapshot> = None;
        for item in world_state_replay {
            match item {
                RolloutItem::Compacted(_) => world_state_baseline = None,
                RolloutItem::WorldState(world_state) if world_state.full => {
                    world_state_baseline = Some(WorldStateSnapshot::from(&world_state.state));
                }
                RolloutItem::WorldState(world_state) => {
                    let Some(baseline) = world_state_baseline.as_mut() else {
                        tracing::warn!("ignored world-state patch without a full snapshot");
                        continue;
                    };
                    baseline.apply_merge_patch(&world_state.state);
                }
                RolloutItem::SessionMeta(_)
                | RolloutItem::RolloutReference(_)
                | RolloutItem::ResponseItem(_)
                | RolloutItem::InterAgentCommunication(_)
                | RolloutItem::InterAgentCommunicationMetadata { .. }
                | RolloutItem::TurnContext(_)
                | RolloutItem::RealtimeItem(_)
                | RolloutItem::TokenUsageRecord(_)
                | RolloutItem::RetainedContext(_)
                | RolloutItem::SecurityRiskScore(_)
                | RolloutItem::EventMsg(_) => {
                    unreachable!("only world-state replay items are collected")
                }
            }
        }

        RolloutReconstruction {
            retained_context: history.retained_context().clone(),
            guardian_history: history.guardian_history_checkpoint(),
            last_started_turn_id,
            history: history.into_annotated_items(),
            previous_turn_settings,
            reference_context_item,
            world_state_baseline,
            window_number: window.number,
            first_window_id: window.first_id,
            previous_window_id: window.previous_id,
            window_id: window.id,
        }
    }
}

fn parse_uuid_v7(value: &str) -> Option<Uuid> {
    Uuid::parse_str(value)
        .ok()
        .filter(|uuid| uuid.get_version_num() == 7)
}

fn reconstructed_window_from_compaction(compacted: &CompactedItem) -> Option<ReconstructedWindow> {
    Some(ReconstructedWindow {
        number: compacted.window_number?,
        first_id: compacted.first_window_id.as_deref().and_then(parse_uuid_v7),
        previous_id: compacted
            .previous_window_id
            .as_deref()
            .and_then(parse_uuid_v7),
        id: compacted.window_id.as_deref().and_then(parse_uuid_v7),
    })
}

fn reconstructed_window_from_session_context_window(
    context_window: &SessionContextWindow,
) -> Option<ReconstructedWindow> {
    let id = parse_uuid_v7(&context_window.window_id)?;
    Some(ReconstructedWindow {
        number: 0,
        first_id: Some(id),
        previous_id: None,
        id: Some(id),
    })
}
