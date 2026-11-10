//! Helpers for truncating rollouts based on "user turn" boundaries.
//!
//! User-message positions prefer persisted `EventMsg::UserMessage` boundaries and
//! fall back to `ResponseItem::Message` items for legacy rollouts without those events.

use crate::context_manager::is_user_turn_boundary;
use crate::event_mapping;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_history::InitialHistory;
use codex_history::RolloutItem;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;

pub(crate) fn initial_history_has_prior_user_turns(conversation_history: &InitialHistory) -> bool {
    conversation_history.scan_rollout_items(rollout_item_is_user_turn_boundary)
}

fn rollout_item_is_user_turn_boundary(item: &RolloutItem) -> bool {
    match item {
        RolloutItem::ResponseItem(item) => is_user_turn_boundary(item),
        RolloutItem::Compacted(checkpoint) => checkpoint
            .replacement_history
            .iter()
            .flatten()
            .any(|item| is_user_turn_boundary(item)),
        RolloutItem::InterAgentCommunication(_) => true,
        _ => false,
    }
}

/// Return the indices of user message boundaries in a rollout.
///
/// Prefer surviving `EventMsg::UserMessage` records. If none survive, use response items
/// whose parsed turn item is `TurnItem::UserMessage`. A containing `TurnStarted` is part
/// of the boundary so truncation does not leave the excluded turn appearing in progress.
///
/// Rollouts can contain `ThreadRolledBack` markers. Those markers indicate that the
/// last N user turns were removed from the effective thread history; we apply them here so
/// indexing uses the post-rollback history rather than the raw stream.
pub(crate) fn user_message_positions_in_rollout(items: &[RolloutItem]) -> Vec<usize> {
    let mut event_user_positions = Vec::new();
    let mut response_user_positions = Vec::new();
    let mut active_turn_start = None;
    for (idx, item) in items.iter().enumerate() {
        match item {
            RolloutItem::EventMsg(EventMsg::TurnStarted(_)) => {
                active_turn_start = Some(idx);
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                event_user_positions.push(active_turn_start.unwrap_or(idx));
            }
            RolloutItem::ResponseItem(item)
                if matches!(&item.item, ResponseItem::Message { .. })
                    && matches!(
                        event_mapping::parse_turn_item(&item.item),
                        Some(TurnItem::UserMessage(_))
                    ) =>
            {
                response_user_positions.push(active_turn_start.unwrap_or(idx));
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(_) | EventMsg::TurnAborted(_)) => {
                active_turn_start = None;
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                let num_turns = usize::try_from(rollback.num_turns).unwrap_or(usize::MAX);
                event_user_positions.truncate(event_user_positions.len().saturating_sub(num_turns));
                response_user_positions
                    .truncate(response_user_positions.len().saturating_sub(num_turns));
            }
            _ => {}
        }
    }
    if event_user_positions.is_empty() {
        response_user_positions
    } else {
        event_user_positions
    }
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
    mut items: Vec<RolloutItem>,
    n_from_start: usize,
) -> Vec<RolloutItem> {
    if n_from_start == usize::MAX {
        return items;
    }

    let user_positions = user_message_positions_in_rollout(&items);

    // If fewer than or equal to n user messages exist, keep the full rollout.
    if user_positions.len() <= n_from_start {
        return items;
    }

    // Cut strictly before the nth user message (do not keep the nth itself).
    let cut_idx = user_positions[n_from_start];
    items.truncate(cut_idx);
    items
}

/// Return a rollout prefix ending after the requested persisted terminal turn.
///
/// The turn must still be present in the effective post-rollback history and
/// must have an explicit persisted TurnStarted boundary. Synthetic IDs
/// generated while projecting legacy rollouts are intentionally unsupported
/// because they do not provide a stable raw rollout boundary for a fork.
pub fn truncate_rollout_after_turn_id(
    mut items: Vec<RolloutItem>,
    last_turn_id: &str,
) -> CodexResult<Vec<RolloutItem>> {
    let cut_index = rollout_cut_index_after_turn_id(&items, last_turn_id)?;
    items.truncate(cut_index);
    Ok(items)
}

fn rollout_cut_index_after_turn_id(
    items: &[RolloutItem],
    last_turn_id: &str,
) -> CodexResult<usize> {
    let turns = build_turns_from_rollout_items(items);
    let turn = turns
        .iter()
        .find(|turn| turn.id == last_turn_id)
        .ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "lastTurnId '{last_turn_id}' was not found in the source thread"
            ))
        })?;

    let target_start_index = items
        .iter()
        .position(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::TurnStarted(event))
                    if event.turn_id == last_turn_id
            )
        })
        .ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "lastTurnId '{last_turn_id}' is not a persisted canonical turn in the source thread"
            ))
        })?;

    if matches!(turn.status, TurnStatus::InProgress) {
        return Err(CodexErr::InvalidRequest(format!(
            "lastTurnId '{last_turn_id}' identifies an in-progress turn"
        )));
    }

    Ok(items
        .iter()
        .enumerate()
        .skip(target_start_index.saturating_add(1))
        .find_map(|(index, item)| {
            matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(_))).then_some(index)
        })
        .unwrap_or(items.len()))
}

/// Return a rollout prefix ending immediately before the requested persisted turn.
pub fn truncate_rollout_before_turn_id(
    mut items: Vec<RolloutItem>,
    before_turn_id: &str,
) -> CodexResult<Vec<RolloutItem>> {
    let cut_index = rollout_cut_index_before_turn_id(&items, before_turn_id)?;
    items.truncate(cut_index);
    Ok(items)
}

fn rollout_cut_index_before_turn_id(
    items: &[RolloutItem],
    before_turn_id: &str,
) -> CodexResult<usize> {
    let cut_index = items.iter().position(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TurnStarted(event))
                if event.turn_id == before_turn_id
        )
    });

    let Some(cut_index) = cut_index else {
        // Older rollouts can expose generated turn IDs without a TurnStarted item to fork at.
        if build_turns_from_rollout_items(items)
            .iter()
            .any(|turn| turn.id == before_turn_id)
        {
            return Err(CodexErr::InvalidRequest(format!(
                "beforeTurnId '{before_turn_id}' is not a persisted canonical turn in the source thread"
            )));
        }

        return Err(CodexErr::InvalidRequest(format!(
            "beforeTurnId '{before_turn_id}' was not found in the source thread"
        )));
    };

    // A persisted turn boundary proves the turn exists unless a later rollback removes it.
    if items[cut_index + 1..]
        .iter()
        .any(|item| matches!(item, RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_))))
        && !build_turns_from_rollout_items(items)
            .iter()
            .any(|turn| turn.id == before_turn_id)
    {
        return Err(CodexErr::InvalidRequest(format!(
            "beforeTurnId '{before_turn_id}' was not found in the source thread"
        )));
    }

    Ok(cut_index)
}

/// Return the number of canonical user-message boundaries before a persisted turn.
pub fn user_message_count_before_turn_id(
    items: &[RolloutItem],
    before_turn_id: &str,
) -> CodexResult<usize> {
    let cut_index = rollout_cut_index_before_turn_id(items, before_turn_id)?;
    Ok(user_message_positions_in_rollout(&items[..cut_index]).len())
}

/// Return the number of canonical user-message boundaries through a terminal turn.
pub fn user_message_count_through_turn_id(
    items: &[RolloutItem],
    last_turn_id: &str,
) -> CodexResult<usize> {
    let cut_index = rollout_cut_index_after_turn_id(items, last_turn_id)?;
    Ok(user_message_positions_in_rollout(&items[..cut_index]).len())
}

#[cfg(test)]
#[path = "thread_rollout_truncation_tests.rs"]
mod tests;
