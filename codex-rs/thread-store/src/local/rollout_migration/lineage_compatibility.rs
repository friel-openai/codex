//! Preserves the initial bounded Legacy response while converting it to Paginated history.
//!
//! `ThreadHistoryBuilder` numbers synthesized items from the oldest segment included in one
//! request. The initial Desktop request includes only the active segment and two predecessors, so
//! its IDs can differ from a complete-lineage replay. Migration keeps the IDs from that initial
//! response and gives older colliding items new explicit IDs. Paginated reads then use one stable
//! ID for every item regardless of page depth.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryItemChange;
use codex_app_server_protocol::ThreadHistoryTurnChange;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;

use super::MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage_stage::stage_legacy_lineage;
use super::migration_error;
use crate::ThreadStoreResult;

pub(super) async fn validate_bounded_desktop_history(
    codex_home: &Path,
    plan: &mut LegacyLineageMigrationPlan,
) -> ThreadStoreResult<()> {
    let source_bytes = plan.sources.iter().try_fold(0_u64, |total, source| {
        total
            .checked_add(source.byte_count)
            .ok_or_else(|| migration_error("lineage migration source byte count overflowed"))
    })?;
    ensure_bounded_compatibility_size(source_bytes)?;
    let stage = tempfile::tempdir().map_err(migration_error)?;
    let staged = stage_legacy_lineage(plan, stage.path()).await?;
    let selected = plan
        .sources
        .last()
        .ok_or_else(|| migration_error("lineage migration has no selected source"))?;
    let inherited = if let Some(dependency) = plan.history_bases.first() {
        codex_rollout::materialize_rollout_lines(codex_home, selected.path.as_path())
            .await
            .map_err(migration_error)?
            .into_iter()
            .filter(|line| {
                line.ordinal
                    .is_some_and(|ordinal| ordinal < dependency.position.end_ordinal_exclusive)
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let mut canonical = canonical_turns_from_rollouts(
        inherited.as_slice(),
        staged_paths(staged.as_slice()).as_slice(),
    )
    .await?;

    let mut materializer =
        codex_rollout::BoundedRolloutMaterializer::new(codex_home, selected.path.as_path());
    let mut reference_limit = DEFAULT_ROLLOUT_REFERENCE_DEPTH;
    let initial = materializer
        .materialize(reference_limit)
        .await
        .map_err(migration_error)?;
    let initial_turns = turns_from_items(
        initial.lines.iter().map(|line| &line.item),
        selected.history_mode,
    );
    plan.synthetic_item_id_remap = derive_initial_synthetic_item_id_remap(
        reference_limit,
        initial_turns.as_slice(),
        canonical.as_slice(),
    )?;
    if !plan.synthetic_item_id_remap.is_empty() {
        let staged = stage_legacy_lineage(plan, stage.path()).await?;
        canonical = canonical_turns_from_rollouts(
            inherited.as_slice(),
            staged_paths(staged.as_slice()).as_slice(),
        )
        .await?;
    }
    compare_turns(
        reference_limit,
        initial_turns.as_slice(),
        canonical.as_slice(),
        /*compare_item_ids*/ true,
    )?;
    if !initial.has_older_reference {
        return Ok(());
    }

    loop {
        if reference_limit >= codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH {
            return Err(migration_error(format!(
                "bounded Legacy Desktop history exceeds {} references",
                codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
            )));
        }
        reference_limit = reference_limit
            .checked_mul(2)
            .unwrap_or(codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH)
            .min(codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH);
        let bounded = materializer
            .materialize(reference_limit)
            .await
            .map_err(migration_error)?;
        let bounded_turns = turns_from_items(
            bounded.lines.iter().map(|line| &line.item),
            selected.history_mode,
        );
        compare_turns(
            reference_limit,
            bounded_turns.as_slice(),
            canonical.as_slice(),
            /*compare_item_ids*/ false,
        )?;
        if !bounded.has_older_reference {
            return Ok(());
        }
    }
}

fn staged_paths(staged: &[super::lineage_stage::StagedLineageTarget]) -> Vec<PathBuf> {
    staged
        .iter()
        .map(|target| target.staged_path.clone())
        .collect()
}

struct CanonicalTurn {
    ordinal: u64,
    turn: Turn,
}

struct CanonicalItem {
    ordinal: u64,
    item: ThreadItem,
}

async fn canonical_turns_from_rollouts(
    inherited: &[codex_rollout::RolloutLine],
    paths: &[PathBuf],
) -> ThreadStoreResult<Vec<Turn>> {
    let mut turns = HashMap::<String, CanonicalTurn>::new();
    let mut items = HashMap::<(String, String), CanonicalItem>::new();
    for line in inherited {
        let ordinal = line
            .ordinal
            .ok_or_else(|| migration_error("inherited Paginated line is missing its ordinal"))?;
        apply_projected_line(&mut turns, &mut items, ordinal, line);
    }
    for path in paths {
        let mut reader = codex_rollout::open_rollout_line_reader(path.as_path())
            .await
            .map_err(migration_error)?;
        while let Some(line) = reader.next_line().await.map_err(migration_error)? {
            let Ok(line) = serde_json::from_str::<codex_rollout::RolloutLine>(line.as_str()) else {
                continue;
            };
            let ordinal = line
                .ordinal
                .ok_or_else(|| migration_error("staged rollout line is missing its ordinal"))?;
            apply_projected_line(&mut turns, &mut items, ordinal, &line);
        }
    }
    let mut items_by_turn = HashMap::<String, Vec<CanonicalItem>>::new();
    for ((turn_id, _), item) in items {
        items_by_turn.entry(turn_id).or_default().push(item);
    }
    let mut turns = turns.into_values().collect::<Vec<_>>();
    turns.sort_by_key(|turn| turn.ordinal);
    Ok(turns
        .into_iter()
        .map(|mut turn| {
            let mut turn_items = items_by_turn.remove(&turn.turn.id).unwrap_or_default();
            turn_items.sort_by_key(|item| item.ordinal);
            turn.turn.items = turn_items.into_iter().map(|item| item.item).collect();
            turn.turn
        })
        .collect())
}

fn apply_projected_line(
    turns: &mut HashMap<String, CanonicalTurn>,
    items: &mut HashMap<(String, String), CanonicalItem>,
    ordinal: u64,
    line: &codex_rollout::RolloutLine,
) {
    let changes = project_rollout_line(line);
    for turn_id in changes.removed_turn_ids {
        turns.remove(turn_id.as_str());
        items.retain(|(item_turn_id, _), _| item_turn_id != &turn_id);
    }
    for turn in changes.changed_turns {
        apply_turn_change(turns, ordinal, turn);
    }
    for item in changes.changed_items {
        apply_item_change(items, ordinal, item);
    }
}

fn ensure_bounded_compatibility_size(source_bytes: u64) -> ThreadStoreResult<()> {
    if source_bytes <= MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES {
        return Ok(());
    }
    Err(migration_error(format!(
        "bounded Legacy Desktop compatibility proof requires {source_bytes} source bytes, which exceeds the fixed {MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES}-byte memory-safety limit; source files were not changed"
    )))
}

fn apply_turn_change(
    turns: &mut HashMap<String, CanonicalTurn>,
    ordinal: u64,
    change: ThreadHistoryTurnChange,
) {
    if let Some(existing) = turns.get_mut(change.turn_id.as_str()) {
        if existing.turn.status == TurnStatus::InProgress {
            existing.turn.status = change.status;
            existing.turn.error = change.error;
            existing.turn.started_at = change.started_at;
            existing.turn.completed_at = change.completed_at;
            existing.turn.duration_ms = change.duration_ms;
        }
        return;
    }
    turns.insert(
        change.turn_id.clone(),
        CanonicalTurn {
            ordinal,
            turn: Turn {
                id: change.turn_id,
                items: Vec::new(),
                items_view: TurnItemsView::Full,
                status: change.status,
                error: change.error,
                started_at: change.started_at,
                completed_at: change.completed_at,
                duration_ms: change.duration_ms,
            },
        },
    );
}

fn apply_item_change(
    items: &mut HashMap<(String, String), CanonicalItem>,
    ordinal: u64,
    change: ThreadHistoryItemChange,
) {
    let key = (change.turn_id, change.item.id().to_string());
    if let Some(existing) = items.get_mut(&key) {
        existing.item = change.item;
    } else {
        items.insert(
            key,
            CanonicalItem {
                ordinal,
                item: change.item,
            },
        );
    }
}

fn turns_from_items<'a>(
    items: impl IntoIterator<Item = &'a RolloutItem>,
    history_mode: ThreadHistoryMode,
) -> Vec<Turn> {
    let mut builder = ThreadHistoryBuilder::new();
    for item in items {
        if codex_rollout::is_persisted_rollout_item(item, history_mode) {
            builder.handle_rollout_item(item);
        }
    }
    builder.finish()
}

fn derive_initial_synthetic_item_id_remap(
    reference_limit: usize,
    bounded: &[Turn],
    canonical: &[Turn],
) -> ThreadStoreResult<HashMap<String, String>> {
    let canonical_by_id = canonical
        .iter()
        .enumerate()
        .map(|(index, turn)| (turn.id.as_str(), (index, turn)))
        .collect::<HashMap<_, _>>();
    let mut previous_index = None;
    let mut matched_migrated_turn = false;
    let mut visible_ids = HashMap::<String, String>::new();
    let mut desired_owners = HashMap::<String, String>::new();
    for turn in bounded {
        let Some((index, canonical_turn)) = canonical_by_id.get(turn.id.as_str()).copied() else {
            if matched_migrated_turn {
                return Err(incompatible(
                    reference_limit,
                    turn.id.as_str(),
                    "turn is absent",
                ));
            }
            continue;
        };
        matched_migrated_turn = true;
        if previous_index.is_some_and(|previous| index <= previous) {
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                "turn order changed",
            ));
        }
        if let Some(reason) =
            first_turn_difference(turn, canonical_turn, /*compare_item_ids*/ false)?
        {
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                reason.as_str(),
            ));
        }
        for (bounded_item, canonical_item) in turn.items.iter().zip(&canonical_turn.items) {
            let canonical_id = canonical_item.id().to_string();
            let desired_id = bounded_item.id().to_string();
            if let Some(previous) = visible_ids.insert(canonical_id.clone(), desired_id.clone())
                && previous != desired_id
            {
                return Err(migration_error(format!(
                    "Legacy item {canonical_id} has two initial Desktop IDs: {previous} and {desired_id}"
                )));
            }
            if let Some(previous_owner) =
                desired_owners.insert(desired_id.clone(), canonical_id.clone())
                && previous_owner != canonical_id
            {
                return Err(migration_error(format!(
                    "initial Legacy Desktop ID {desired_id} identifies both {previous_owner} and {canonical_id}"
                )));
            }
        }
        previous_index = Some(index);
    }
    if !canonical.is_empty() && !matched_migrated_turn {
        return Err(no_migrated_turn_error(reference_limit, bounded, canonical));
    }

    let canonical_ids = canonical
        .iter()
        .flat_map(|turn| turn.items.iter().map(|item| item.id().to_string()))
        .collect::<Vec<_>>();
    let mut occupied = canonical_ids.iter().cloned().collect::<HashSet<_>>();
    occupied.extend(desired_owners.keys().cloned());
    let mut next_item_index = canonical_ids
        .iter()
        .chain(desired_owners.keys())
        .filter_map(|id| id.strip_prefix("item-")?.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| migration_error("Legacy synthetic item ID overflowed"))?;
    let mut remap = visible_ids
        .iter()
        .filter(|(canonical_id, desired_id)| canonical_id != desired_id)
        .map(|(canonical_id, desired_id)| (canonical_id.clone(), desired_id.clone()))
        .collect::<HashMap<_, _>>();
    for canonical_id in canonical_ids {
        if visible_ids.contains_key(canonical_id.as_str())
            || !desired_owners.contains_key(canonical_id.as_str())
        {
            continue;
        }
        let replacement = loop {
            let candidate = format!("item-{next_item_index}");
            next_item_index = next_item_index
                .checked_add(1)
                .ok_or_else(|| migration_error("Legacy synthetic item ID overflowed"))?;
            if occupied.insert(candidate.clone()) {
                break candidate;
            }
        };
        remap.insert(canonical_id, replacement);
    }
    Ok(remap)
}

fn compare_turns(
    reference_limit: usize,
    bounded: &[Turn],
    canonical: &[Turn],
    compare_item_ids: bool,
) -> ThreadStoreResult<()> {
    let canonical_by_id = canonical
        .iter()
        .enumerate()
        .map(|(index, turn)| (turn.id.as_str(), (index, turn)))
        .collect::<HashMap<_, _>>();
    let mut previous_index = None;
    let mut matched_migrated_turn = false;
    for turn in bounded {
        let Some((index, canonical_turn)) = canonical_by_id.get(turn.id.as_str()).copied() else {
            if matched_migrated_turn {
                return Err(incompatible(
                    reference_limit,
                    turn.id.as_str(),
                    "turn is absent",
                ));
            }
            // Existing Paginated history_base and immutable-reference dependencies remain
            // byte-for-byte unchanged. They precede the Legacy suffix staged by this migration,
            // so their turns are intentionally absent from `canonical`.
            continue;
        };
        matched_migrated_turn = true;
        if previous_index.is_some_and(|previous| index <= previous) {
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                "turn order changed",
            ));
        }
        if let Some(reason) = first_turn_difference(turn, canonical_turn, compare_item_ids)? {
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                reason.as_str(),
            ));
        }
        previous_index = Some(index);
    }
    if !canonical.is_empty() && !matched_migrated_turn {
        return Err(no_migrated_turn_error(reference_limit, bounded, canonical));
    }
    Ok(())
}

fn first_turn_difference(
    bounded: &Turn,
    canonical: &Turn,
    compare_item_ids: bool,
) -> ThreadStoreResult<Option<String>> {
    if bounded.status != canonical.status {
        Ok(Some("turn status changed".to_string()))
    } else if bounded.error != canonical.error {
        Ok(Some("turn error changed".to_string()))
    } else if bounded.items_view != canonical.items_view {
        Ok(Some("turn items view changed".to_string()))
    } else if bounded.started_at != canonical.started_at {
        Ok(Some("turn start timestamp changed".to_string()))
    } else if bounded.completed_at != canonical.completed_at {
        Ok(Some("turn completion timestamp changed".to_string()))
    } else if bounded.duration_ms != canonical.duration_ms {
        Ok(Some("turn duration changed".to_string()))
    } else if bounded.items.len() != canonical.items.len() {
        Ok(Some(format!(
            "turn item count changed from {} to {}",
            bounded.items.len(),
            canonical.items.len()
        )))
    } else {
        for (bounded_item, canonical_item) in bounded.items.iter().zip(&canonical.items) {
            if item_without_id(bounded_item)? != item_without_id(canonical_item)? {
                return Ok(Some("turn item content changed".to_string()));
            }
            if compare_item_ids && bounded_item.id() != canonical_item.id() {
                return Ok(Some(format!(
                    "synthetic item ID changed from {} to {}",
                    bounded_item.id(),
                    canonical_item.id()
                )));
            }
        }
        Ok(None)
    }
}

fn item_without_id(item: &ThreadItem) -> ThreadStoreResult<serde_json::Value> {
    let mut value = serde_json::to_value(item).map_err(migration_error)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| migration_error("ThreadItem did not serialize as an object"))?;
    object.remove("id");
    Ok(value)
}

fn no_migrated_turn_error(
    reference_limit: usize,
    bounded: &[Turn],
    canonical: &[Turn],
) -> crate::ThreadStoreError {
    let bounded_ids = bounded
        .iter()
        .map(|turn| turn.id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let canonical_ids = canonical
        .iter()
        .map(|turn| turn.id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    migration_error(format!(
        "bounded Legacy Desktop history is not canonical at reference depth {reference_limit}: no migrated turn retained its stable ID (bounded [{bounded_ids}], migrated [{canonical_ids}]); source files were not changed"
    ))
}

fn incompatible(reference_limit: usize, turn_id: &str, reason: &str) -> crate::ThreadStoreError {
    migration_error(format!(
        "bounded Legacy Desktop history is not canonical at reference depth {reference_limit}: turn {turn_id} {reason}; source files were not changed"
    ))
}

#[cfg(test)]
mod tests {
    use super::MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES;
    use super::ensure_bounded_compatibility_size;

    #[test]
    fn bounded_compatibility_size_limit_is_inclusive_and_fail_closed() {
        assert!(ensure_bounded_compatibility_size(MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES).is_ok());
        let error = ensure_bounded_compatibility_size(MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES + 1)
            .expect_err("oversized compatibility proof must fail closed");
        assert!(error.to_string().contains("source files were not changed"));
    }
}
