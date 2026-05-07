//! Helpers for truncating rollouts based on "user turn" boundaries.
//!
//! In core, "user turns" are detected by scanning `ResponseItem::Message` items and
//! interpreting them via `event_mapping::parse_turn_item(...)`.

use crate::context_manager::is_user_turn_boundary;
use crate::event_mapping;
use crate::rollout::RolloutRecorder;
use crate::rollout::resolve_fork_reference_rollout_path;
use crate::rollout::resolve_rollout_reference_rollout_path;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InitialHistory;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::RolloutItem;
use std::path::Path;
use tracing::warn;

pub(crate) fn initial_history_has_prior_user_turns(conversation_history: &InitialHistory) -> bool {
    conversation_history.scan_rollout_items(rollout_item_is_user_turn_boundary)
}

fn rollout_item_is_user_turn_boundary(item: &RolloutItem) -> bool {
    match item {
        RolloutItem::ResponseItem(item) => is_user_turn_boundary(item),
        _ => false,
    }
}

/// Return the indices of user message boundaries in a rollout.
///
/// A user message boundary is a `RolloutItem::ResponseItem(ResponseItem::Message { .. })`
/// whose parsed turn item is `TurnItem::UserMessage`.
///
/// Rollouts can contain `ThreadRolledBack` markers. Those markers indicate that the
/// last N user turns were removed from the effective thread history; we apply them here so
/// indexing uses the post-rollback history rather than the raw stream.
pub(crate) fn user_message_positions_in_rollout(items: &[RolloutItem]) -> Vec<usize> {
    let mut user_positions = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        match item {
            RolloutItem::ResponseItem(item @ ResponseItem::Message { .. })
                if matches!(
                    event_mapping::parse_turn_item(item),
                    Some(TurnItem::UserMessage(_))
                ) =>
            {
                user_positions.push(idx);
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                let num_turns = usize::try_from(rollback.num_turns).unwrap_or(usize::MAX);
                let new_len = user_positions.len().saturating_sub(num_turns);
                user_positions.truncate(new_len);
            }
            _ => {}
        }
    }
    user_positions
}

/// Return the indices of fork-turn boundaries in a rollout.
///
/// A fork-turn boundary is either:
/// - a real user message boundary, or
/// - an assistant inter-agent envelope whose parsed `trigger_turn` is `true`.
///
/// Like `user_message_positions_in_rollout`, this applies `ThreadRolledBack` markers so indexing
/// reflects the effective post-rollback history. Rollback counts instruction turns, so a rollback
/// removes the stale suffix starting at the earliest rolled-back instruction-turn boundary instead
/// of simply truncating the mixed fork-boundary list.
pub(crate) fn fork_turn_positions_in_rollout(items: &[RolloutItem]) -> Vec<usize> {
    let mut rollback_turn_positions = Vec::new();
    let mut fork_turn_positions = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        match item {
            RolloutItem::ResponseItem(item) => {
                if is_user_turn_boundary(item) {
                    rollback_turn_positions.push(idx);
                }
                if is_real_user_message_boundary(item) || is_trigger_turn_boundary(item) {
                    fork_turn_positions.push(idx);
                }
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                let num_turns = usize::try_from(rollback.num_turns).unwrap_or(usize::MAX);
                if num_turns == 0 {
                    continue;
                }
                let Some(rollback_start_idx) = rollback_turn_positions
                    .len()
                    .checked_sub(num_turns)
                    .map(|rollback_start| rollback_turn_positions[rollback_start])
                    .or_else(|| rollback_turn_positions.first().copied())
                else {
                    continue;
                };
                let new_rollback_len = rollback_turn_positions.len().saturating_sub(num_turns);
                rollback_turn_positions.truncate(new_rollback_len);
                fork_turn_positions.retain(|position| *position < rollback_start_idx);
            }
            _ => {}
        }
    }
    fork_turn_positions
}

/// Return a prefix of `items` obtained by cutting strictly before the nth user message.
///
/// The boundary index is 0-based from the start of `items` (so `n_from_start = 0` returns
/// a prefix that excludes the first user message and everything after it).
///
/// If `n_from_start` is `usize::MAX`, this returns the full rollout (no truncation).
/// If fewer than or equal to `n_from_start` user messages exist, this returns the full
/// rollout unchanged.
pub(crate) fn truncate_rollout_before_nth_user_message_from_start(
    items: &[RolloutItem],
    n_from_start: usize,
) -> Vec<RolloutItem> {
    if n_from_start == usize::MAX {
        return items.to_vec();
    }

    let user_positions = user_message_positions_in_rollout(items);

    // If fewer than or equal to n user messages exist, keep the full rollout.
    if user_positions.len() <= n_from_start {
        return items.to_vec();
    }

    // Cut strictly before the nth user message (do not keep the nth itself).
    let cut_idx = user_positions[n_from_start];
    items[..cut_idx].to_vec()
}

/// Return a suffix of `items` that keeps the last `n_from_end` fork turns.
///
/// If fewer than or equal to `n_from_end` fork turns exist, this returns the full rollout.
pub(crate) fn truncate_rollout_to_last_n_fork_turns(
    items: &[RolloutItem],
    n_from_end: usize,
) -> Vec<RolloutItem> {
    if n_from_end == 0 {
        return Vec::new();
    }

    let fork_turn_positions = fork_turn_positions_in_rollout(items);
    if fork_turn_positions.len() <= n_from_end {
        return items.to_vec();
    }

    let keep_idx = fork_turn_positions[fork_turn_positions.len() - n_from_end];
    items[keep_idx..].to_vec()
}

pub async fn materialize_rollout_items_for_replay(
    codex_home: &Path,
    rollout_items: &[RolloutItem],
) -> Vec<RolloutItem> {
    materialize_rollout_items_for_replay_at_depth(codex_home, rollout_items, /*depth*/ 0).await
}

async fn materialize_rollout_items_for_replay_at_depth(
    codex_home: &Path,
    rollout_items: &[RolloutItem],
    depth: usize,
) -> Vec<RolloutItem> {
    const MAX_FORK_REFERENCE_DEPTH: usize = 8;
    if depth >= MAX_FORK_REFERENCE_DEPTH {
        warn!("fork reference materialization reached max depth");
        return rollout_items.to_vec();
    }

    let mut materialized = Vec::new();
    for item in rollout_items {
        match item {
            RolloutItem::ForkReference(reference) => {
                let resolved_path =
                    match resolve_fork_reference_rollout_path(codex_home, reference).await {
                        Ok(path) => path,
                        Err(err) => {
                            warn!(
                                "failed to resolve fork reference {}: {err}",
                                reference.rollout_path.display()
                            );
                            reference.rollout_path.clone()
                        }
                    };
                match RolloutRecorder::load_rollout_items(&resolved_path).await {
                    Ok((parent_items, _, _)) => {
                        let parent_materialized =
                            Box::pin(materialize_rollout_items_for_replay_at_depth(
                                codex_home,
                                &parent_items,
                                depth + 1,
                            ))
                            .await;
                        let parent_prefix = truncate_rollout_before_nth_user_message_from_start(
                            &parent_materialized,
                            reference.nth_user_message,
                        );
                        materialized.extend(parent_prefix);
                    }
                    Err(err) => {
                        warn!(
                            "failed to load fork reference {}: {err}",
                            resolved_path.display()
                        );
                    }
                }
            }
            RolloutItem::RolloutReference(reference) => {
                if depth >= reference.max_depth {
                    warn!("rollout reference materialization reached max depth");
                    continue;
                }
                let resolved_path =
                    match resolve_rollout_reference_rollout_path(codex_home, reference).await {
                        Ok(path) => path,
                        Err(err) => {
                            warn!(
                                "failed to resolve rollout reference {}: {err}",
                                reference.rollout_path.display()
                            );
                            reference.rollout_path.clone()
                        }
                    };
                match RolloutRecorder::load_rollout_items(&resolved_path).await {
                    Ok((reference_items, _, _)) => {
                        let reference_materialized =
                            Box::pin(materialize_rollout_items_for_replay_at_depth(
                                codex_home,
                                &reference_items,
                                depth + 1,
                            ))
                            .await;
                        materialized.extend(reference_materialized);
                    }
                    Err(err) => {
                        warn!(
                            "failed to load rollout reference {}: {err}",
                            resolved_path.display()
                        );
                    }
                }
            }
            other => materialized.push(other.clone()),
        }
    }
    materialized
}

fn is_real_user_message_boundary(item: &ResponseItem) -> bool {
    matches!(
        event_mapping::parse_turn_item(item),
        Some(TurnItem::UserMessage(_))
    )
}

fn is_trigger_turn_boundary(item: &ResponseItem) -> bool {
    let ResponseItem::Message { role, content, .. } = item else {
        return false;
    };

    role == "assistant"
        && InterAgentCommunication::from_message_content(content)
            .is_some_and(|communication| communication.trigger_turn)
}

#[cfg(test)]
#[path = "thread_rollout_truncation_tests.rs"]
mod tests;
