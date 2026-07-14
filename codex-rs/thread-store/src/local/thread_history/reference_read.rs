use std::collections::HashMap;

use chrono::DateTime;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::ThreadId;
use codex_protocol::protocol::RolloutLine;

use super::super::LocalThreadStore;
use super::read::CursorScope;
use super::read::HistoryCursor;
use super::read::page_cursors;
use super::read::page_limit;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListTurnsParams;
use crate::SortDirection;
use crate::StoredThreadItem;
use crate::StoredTurn;
use crate::StoredTurnError;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::TurnPage;

struct LogicalTurn {
    turn_id: String,
    rollout_ordinal: i64,
    status: StoredTurnStatus,
    error: Option<StoredTurnError>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    duration_ms: Option<i64>,
    first_user_item_id: Option<String>,
    final_agent_item_id: Option<String>,
}

struct LogicalItem {
    item: StoredThreadItem,
    rollout_ordinal: i64,
    summary_kind: SummaryKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SummaryKind {
    User,
    Agent,
    Other,
}

struct LogicalHistory {
    turns: HashMap<String, LogicalTurn>,
    items: HashMap<(String, String), LogicalItem>,
}

pub(super) async fn logical_lines_if_referenced(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    include_archived: bool,
) -> ThreadStoreResult<Option<Vec<RolloutLine>>> {
    let Some(path) =
        super::super::read_thread::resolve_rollout_path(store, thread_id, include_archived).await?
    else {
        // SQLite-only stores and tests have no physical rollout to inspect. Their projected rows
        // remain authoritative because they cannot contain a rollout reference.
        return Ok(None);
    };
    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .map_err(thread_store_io_error)?;
    if session_meta.meta.id != thread_id {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "rollout {} does not belong to thread {thread_id}",
                path.display()
            ),
        });
    }
    if !super::super::read_thread::rollout_starts_with_reference(path.as_path()).await? {
        return Ok(None);
    }
    codex_rollout::materialize_rollout_lines(store.config.codex_home.as_path(), path.as_path())
        .await
        .map(Some)
        .map_err(thread_store_io_error)
}

pub(super) fn list_turns(
    lines: Vec<RolloutLine>,
    params: ListTurnsParams,
    scope: CursorScope,
    cursor: Option<HistoryCursor>,
) -> ThreadStoreResult<TurnPage> {
    let history = fold_logical_history(lines)?;
    let limit = usize::try_from(page_limit(params.page_size)?).map_err(thread_history_error)?;
    let mut turns = history.turns.into_values().collect::<Vec<_>>();
    turns.retain(|turn| {
        matches_cursor(turn.rollout_ordinal, params.sort_direction, cursor.as_ref())
    });
    turns.sort_by_key(|turn| turn.rollout_ordinal);
    if matches!(params.sort_direction, SortDirection::Desc) {
        turns.reverse();
    }
    turns.truncate(limit);
    let has_more = turns.len() > params.page_size;
    turns.truncate(params.page_size);
    let (next_cursor, backwards_cursor) = page_cursors(
        params.thread_id,
        &scope,
        turns.first().map(|turn| turn.rollout_ordinal),
        turns.last().map(|turn| turn.rollout_ordinal),
        has_more,
    )?;
    let turns = turns
        .into_iter()
        .map(|turn| {
            let items = match params.items_view {
                StoredTurnItemsView::NotLoaded => Vec::new(),
                StoredTurnItemsView::Summary => summary_items(&history.items, &turn),
            };
            StoredTurn {
                turn_id: turn.turn_id,
                items,
                items_view: params.items_view,
                status: turn.status,
                error: turn.error,
                started_at: turn.started_at,
                completed_at: turn.completed_at,
                duration_ms: turn.duration_ms,
            }
        })
        .collect();
    Ok(TurnPage {
        turns,
        next_cursor,
        backwards_cursor,
    })
}

pub(super) fn list_items(
    lines: Vec<RolloutLine>,
    params: ListItemsParams,
    scope: CursorScope,
    cursor: Option<HistoryCursor>,
) -> ThreadStoreResult<ItemPage> {
    let history = fold_logical_history(lines)?;
    let limit = usize::try_from(page_limit(params.page_size)?).map_err(thread_history_error)?;
    let mut items = history
        .items
        .into_values()
        .filter(|item| {
            params
                .turn_id
                .as_deref()
                .is_none_or(|turn_id| item.item.turn_id == turn_id)
        })
        .filter(|item| matches_cursor(item.rollout_ordinal, params.sort_direction, cursor.as_ref()))
        .collect::<Vec<_>>();
    items.sort_by_key(|item| item.rollout_ordinal);
    if matches!(params.sort_direction, SortDirection::Desc) {
        items.reverse();
    }
    items.truncate(limit);
    let has_more = items.len() > params.page_size;
    items.truncate(params.page_size);
    let (next_cursor, backwards_cursor) = page_cursors(
        params.thread_id,
        &scope,
        items.first().map(|item| item.rollout_ordinal),
        items.last().map(|item| item.rollout_ordinal),
        has_more,
    )?;
    Ok(ItemPage {
        items: items.into_iter().map(|item| item.item).collect(),
        next_cursor,
        backwards_cursor,
    })
}

fn fold_logical_history(lines: Vec<RolloutLine>) -> ThreadStoreResult<LogicalHistory> {
    let mut history = LogicalHistory {
        turns: HashMap::new(),
        items: HashMap::new(),
    };
    for line in lines {
        let ordinal = line
            .ordinal
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "referenced paginated rollout line is missing an ordinal".to_string(),
            })
            .and_then(|ordinal| {
                i64::try_from(ordinal).map_err(|_| ThreadStoreError::Internal {
                    message: "rollout ordinal exceeds SQLite integer range".to_string(),
                })
            })?;
        let created_at_ms = DateTime::parse_from_rfc3339(line.timestamp.as_str())
            .map(|timestamp| timestamp.timestamp_millis())
            .map_err(thread_history_error)?;
        let changes = project_rollout_line(&line);
        for turn_id in changes.removed_turn_ids {
            history.turns.remove(turn_id.as_str());
            history
                .items
                .retain(|(item_turn_id, _), _| item_turn_id != &turn_id);
        }
        for turn in changes.changed_turns {
            let status = stored_turn_status(turn.status);
            let error = turn.error.map(|error| StoredTurnError {
                message: error.message,
                codex_error_info: error.codex_error_info,
                additional_details: error.additional_details,
            });
            history
                .turns
                .entry(turn.turn_id.clone())
                .and_modify(|stored| {
                    stored.status = status;
                    stored.error.clone_from(&error);
                    stored.started_at = turn.started_at;
                    stored.completed_at = turn.completed_at;
                    stored.duration_ms = turn.duration_ms;
                })
                .or_insert(LogicalTurn {
                    turn_id: turn.turn_id,
                    rollout_ordinal: ordinal,
                    status,
                    error,
                    started_at: turn.started_at,
                    completed_at: turn.completed_at,
                    duration_ms: turn.duration_ms,
                    first_user_item_id: None,
                    final_agent_item_id: None,
                });
        }
        for changed in changes.changed_items {
            let item_id = changed.item.id().to_string();
            let key = (changed.turn_id.clone(), item_id.clone());
            let summary_kind = match &changed.item {
                ThreadItem::UserMessage { .. } => SummaryKind::User,
                ThreadItem::AgentMessage { .. } => SummaryKind::Agent,
                ThreadItem::HookPrompt { .. }
                | ThreadItem::InterAgentCommunication { .. }
                | ThreadItem::RawResponseItem { .. }
                | ThreadItem::Plan { .. }
                | ThreadItem::Reasoning { .. }
                | ThreadItem::CommandExecution { .. }
                | ThreadItem::FileChange { .. }
                | ThreadItem::McpToolCall { .. }
                | ThreadItem::DynamicToolCall { .. }
                | ThreadItem::CollabAgentToolCall { .. }
                | ThreadItem::SubAgentActivity { .. }
                | ThreadItem::WebSearch(_)
                | ThreadItem::ImageView { .. }
                | ThreadItem::Sleep { .. }
                | ThreadItem::ImageGeneration(_)
                | ThreadItem::EnteredReviewMode { .. }
                | ThreadItem::ExitedReviewMode { .. }
                | ThreadItem::ContextCompaction { .. } => SummaryKind::Other,
            };
            let item_json = serde_json::to_vec(&changed.item).map_err(thread_history_error)?;
            history
                .items
                .entry(key)
                .and_modify(|stored| {
                    stored.item.item_json.clone_from(&item_json);
                    stored.summary_kind = summary_kind;
                })
                .or_insert(LogicalItem {
                    item: StoredThreadItem {
                        turn_id: changed.turn_id.clone(),
                        item_id: item_id.clone(),
                        created_at_ms,
                        item_json,
                    },
                    rollout_ordinal: ordinal,
                    summary_kind,
                });
        }
    }
    for turn in history.turns.values_mut() {
        let mut turn_items = history
            .items
            .values()
            .filter(|item| item.item.turn_id == turn.turn_id)
            .collect::<Vec<_>>();
        turn_items.sort_by_key(|item| item.rollout_ordinal);
        turn.first_user_item_id = turn_items
            .iter()
            .find(|item| item.summary_kind == SummaryKind::User)
            .map(|item| item.item.item_id.clone());
        turn.final_agent_item_id = turn_items
            .iter()
            .rev()
            .find(|item| item.summary_kind == SummaryKind::Agent)
            .map(|item| item.item.item_id.clone());
    }
    Ok(history)
}

fn summary_items(
    items: &HashMap<(String, String), LogicalItem>,
    turn: &LogicalTurn,
) -> Vec<StoredThreadItem> {
    let mut summary = items
        .values()
        .filter(|item| {
            item.item.turn_id == turn.turn_id
                && (Some(item.item.item_id.as_str()) == turn.first_user_item_id.as_deref()
                    || Some(item.item.item_id.as_str()) == turn.final_agent_item_id.as_deref())
        })
        .collect::<Vec<_>>();
    summary.sort_by_key(|item| item.rollout_ordinal);
    summary.into_iter().map(|item| item.item.clone()).collect()
}

fn matches_cursor(ordinal: i64, direction: SortDirection, cursor: Option<&HistoryCursor>) -> bool {
    let Some(cursor) = cursor else {
        return true;
    };
    match (direction, cursor.include_anchor) {
        (SortDirection::Asc, true) => ordinal >= cursor.rollout_ordinal,
        (SortDirection::Asc, false) => ordinal > cursor.rollout_ordinal,
        (SortDirection::Desc, true) => ordinal <= cursor.rollout_ordinal,
        (SortDirection::Desc, false) => ordinal < cursor.rollout_ordinal,
    }
}

fn stored_turn_status(status: TurnStatus) -> StoredTurnStatus {
    match status {
        TurnStatus::Completed => StoredTurnStatus::Completed,
        TurnStatus::Interrupted => StoredTurnStatus::Interrupted,
        TurnStatus::Failed => StoredTurnStatus::Failed,
        TurnStatus::InProgress => StoredTurnStatus::InProgress,
    }
}

fn thread_history_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to read referenced thread history: {err}"),
    }
}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "reference_read_tests.rs"]
mod tests;
