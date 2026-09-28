use std::sync::Arc;

use codex_rollout::CompactedItem;

use super::migration_error;
use super::rollback;
use super::rollback_plan::RetainedContextEdit;
use super::rollback_plan::instruction_fingerprint;
use crate::ThreadStoreResult;

/// Applies edits to the original checkpoint so repeated rollbacks retain their original boundaries.
pub(super) fn apply_compaction_edits(
    record_index: usize,
    compacted: &mut CompactedItem,
    edits: &[Arc<RetainedContextEdit>],
    rollbacks: &[u32],
    empty_anchor: bool,
) -> ThreadStoreResult<()> {
    // Resolve every removed instruction against the original Guardian transcript before
    // truncating it. Repeated rollbacks can otherwise mistake an already removed boundary
    // for evicted evidence and discard the surviving prefix.
    let mut guardian_cut = None;
    for edit in edits {
        if (edit.first_removed_message_id.is_some() || edit.first_removed_fingerprint.is_some())
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
                        && Some(instruction_fingerprint(item)?) == edit.first_removed_fingerprint);
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
                guardian_cut = Some(guardian_cut.map_or(cut, |previous: usize| previous.min(cut)));
            }
        }
    }
    for edit in edits {
        edit.apply(compacted);
    }
    if empty_anchor {
        compacted.replacement_history = Some(Vec::new());
        compacted.mcp_resource_origins = None;
        if let Some(history) = compacted.guardian_history.as_mut() {
            history.0.truncate(guardian_cut.unwrap_or(0));
        }
        return Ok(());
    }
    if !rollbacks.is_empty() {
        let replacement_history = compacted.replacement_history.as_mut().ok_or_else(|| {
            migration_error("legacy rollback crosses a compaction without replacement history")
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
    Ok(())
}
