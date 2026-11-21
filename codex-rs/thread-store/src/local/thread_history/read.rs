use std::collections::HashMap;
use std::collections::HashSet;

use codex_app_server_protocol::ThreadHistoryTurnMetadata;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadItemPayload;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::models::MessagePhase;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Row;

use super::super::LocalThreadStore;
use super::super::rollout_lineage::RolloutLineage;
use super::super::rollout_lineage::RolloutLineageSegment;
use super::super::thread_rollout_resolver::ResolvedThreadRollout;
use super::segment_paging::LazyPage;
use super::segment_paging::SegmentPage;
use super::segment_paging::certified_resume_backwards_cursors;
use super::segment_paging::page_certified_ascending_item_rows;
use super::segment_paging::page_certified_item_rows;
use super::segment_paging::page_certified_single_owner_turn_rows;
use super::segment_paging::page_indexed_item_rows;
use super::segment_paging::page_indexed_turn_rows;
use super::segment_paging::page_item_rows;
use super::segment_paging::page_lazy_item_rows;
use super::segment_paging::page_lazy_turn_rows;
use super::segment_paging::page_turn_rows;
use super::segment_paging::validate_page_size;
use super::sqlite_integer;
use super::turn_lookup::find_source_turn;
use super::uncertified_items;
use super::uncertified_items::SourceItemAnchor;
use super::uncertified_items::SourceTurnEvent;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListItemsPosition;
use crate::ListTurnsParams;
use crate::StoredThreadItem;
use crate::StoredTurn;
use crate::StoredTurnError;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::TurnPage;

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

#[cfg(test)]
static SOURCE_ITEM_PAUSES: std::sync::LazyLock<
    std::sync::Mutex<HashMap<ThreadId, std::sync::Arc<SourceItemPause>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// A one-shot barrier before validating the sources used for an item page.
#[cfg(test)]
struct SourceItemPause {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
fn inject_source_item_pause(thread_id: ThreadId) -> std::sync::Arc<SourceItemPause> {
    let pause = std::sync::Arc::new(SourceItemPause {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    SOURCE_ITEM_PAUSES
        .lock()
        .unwrap()
        .insert(thread_id, pause.clone());
    pause
}

#[cfg(test)]
async fn pause_source_item_before_revalidation(thread_id: ThreadId) {
    let pause = SOURCE_ITEM_PAUSES.lock().unwrap().remove(&thread_id);
    if let Some(pause) = pause {
        pause.entered.notify_one();
        pause.release.notified().await;
    }
}

#[derive(Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(super) enum CursorScope {
    Turns,
    ItemsByCreatedAtOrdinal,
    ItemsByUpdatedAtOrdinal,
}

/// A history position bound to one logical thread and its selected physical rollout.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HistoryCursor {
    pub requested_thread_id: ThreadId,
    pub root_rollout_id: RolloutId,
    pub rollout_ordinal: u64,
    pub include_anchor: bool,
    pub scope: CursorScope,
}

#[derive(Clone, Copy)]
pub(super) struct PhysicalHistoryPosition {
    pub rollout_ordinal: i64,
}

pub(super) struct StoredTurnRow {
    pub position: PhysicalHistoryPosition,
    pub turn_id: String,
    pub root_turn_id: Option<String>,
    pub status: StoredTurnStatus,
    pub error: Option<StoredTurnError>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub duration_ms: Option<i64>,
    pub first_user_item_id: Option<String>,
    pub final_agent_item_id: Option<String>,
    pub summary_items: Vec<StoredThreadItem>,
}

#[derive(sqlx_macros::FromRow)]
pub(super) struct StoredSummaryColumns {
    summary_first_user_turn_id: Option<String>,
    summary_first_user_item_id: Option<String>,
    summary_first_user_rollout_ordinal: Option<i64>,
    summary_first_user_updated_at_ordinal: Option<i64>,
    summary_first_user_created_at_ms: Option<i64>,
    summary_first_user_started_at_ms: Option<i64>,
    summary_first_user_completed_at_ms: Option<i64>,
    summary_first_user_item_json: Option<String>,
    summary_final_agent_turn_id: Option<String>,
    summary_final_agent_item_id: Option<String>,
    summary_final_agent_rollout_ordinal: Option<i64>,
    summary_final_agent_updated_at_ordinal: Option<i64>,
    summary_final_agent_created_at_ms: Option<i64>,
    summary_final_agent_started_at_ms: Option<i64>,
    summary_final_agent_completed_at_ms: Option<i64>,
    summary_final_agent_item_json: Option<String>,
}

struct StoredSummaryItemColumns {
    turn_id: Option<String>,
    item_id: Option<String>,
    rollout_ordinal: Option<i64>,
    updated_at_ordinal: Option<i64>,
    created_at_ms: Option<i64>,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    item_json: Option<String>,
}

pub(super) struct StoredThreadItemRow {
    pub position: PhysicalHistoryPosition,
    pub item: StoredThreadItem,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyProjectionCursor {
    turn_id: String,
    include_anchor: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LegacyProjectionLookup {
    ExistingOnly,
    Backfill,
}

pub(in crate::local) async fn list_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<TurnPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_turns",
    )
    .await?;
    validate_page_size(params.page_size)?;
    if let Some(page) = page_certified_single_owner_turn_rows(store, &params).await? {
        return match page {
            LazyPage::Page(page, lineage) => {
                turns_from_page(store, params.items_view, &lineage, page).await
            }
            LazyPage::Full(lineage) => list_turns_from_lineage(store, params, &lineage).await,
        };
    }
    if let Some(page) = source_backed_turns(store, &params).await? {
        return Ok(page);
    }
    if let Some(lineage) = indexed_same_thread_lineage(store, params.thread_id).await? {
        return list_turns_from_lineage(store, params, &lineage).await;
    }
    if matches!(params.sort_direction, crate::SortDirection::Desc)
        && params.page_size < i64::MAX as usize - 1
    {
        return match page_lazy_turn_rows(
            store,
            params.thread_id,
            params.cursor.as_deref(),
            params.page_size,
            params.items_view,
            /*require_lookahead*/ true,
        )
        .await?
        {
            LazyPage::Page(page, lineage) => {
                turns_from_page(store, params.items_view, &lineage, page).await
            }
            LazyPage::Full(lineage) => list_turns_from_lineage(store, params, &lineage).await,
        };
    }
    let lineage = store
        .resolve_rollout_lineage_for_history_page(params.thread_id)
        .await?;
    list_turns_from_lineage(store, params, &lineage).await
}

/// Summary pointers and terminal state reconstructed from the selected rollout records.
struct AggregateTurn {
    ordinal: u64,
    owner: ThreadId,
    metadata: ThreadHistoryTurnMetadata,
    terminal_ordinal: Option<u64>,
    first_user: Option<String>,
    final_agent: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SummaryKind {
    User,
    FinalAgent,
    LegacyAgent,
    Other,
}

/// The first creation position and latest canonical snapshot of a summary candidate.
struct AggregateItem {
    ordinal: u64,
    item: StoredThreadItem,
    kind: SummaryKind,
}

fn summary_kind(item: &ThreadItem) -> SummaryKind {
    match &item.payload {
        ThreadItemPayload::UserMessage { .. } => SummaryKind::User,
        ThreadItemPayload::AgentMessage {
            phase: Some(MessagePhase::FinalAnswer),
            ..
        } => SummaryKind::FinalAgent,
        ThreadItemPayload::AgentMessage { phase: None, .. } => SummaryKind::LegacyAgent,
        _ => SummaryKind::Other,
    }
}

fn latest_summary_id(items: &HashMap<String, AggregateItem>, kind: SummaryKind) -> Option<String> {
    items
        .iter()
        .filter(|(_, item)| item.kind == kind)
        .max_by_key(|(_, item)| item.ordinal)
        .map(|(id, _)| id.clone())
}

fn reduce_aggregate_sources(
    events: Vec<SourceTurnEvent>,
) -> ThreadStoreResult<(
    HashMap<String, AggregateTurn>,
    HashMap<(ThreadId, String), HashMap<String, AggregateItem>>,
)> {
    let mut turns: HashMap<String, AggregateTurn> = HashMap::new();
    let mut items: HashMap<(ThreadId, String), HashMap<String, AggregateItem>> = HashMap::new();
    for event in events {
        match event {
            SourceTurnEvent::Turn(ordinal, owner, turn) => {
                let id = turn.turn_id.clone();
                let terminal = (turn.status != TurnStatus::InProgress).then_some(ordinal);
                let current = match turns.entry(id.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(AggregateTurn {
                            ordinal,
                            owner,
                            metadata: turn,
                            terminal_ordinal: terminal,
                            first_user: None,
                            final_agent: None,
                        })
                    }
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let current = entry.into_mut();
                        if current.owner != owner {
                            *current = AggregateTurn {
                                ordinal,
                                owner,
                                metadata: turn,
                                terminal_ordinal: terminal,
                                first_user: None,
                                final_agent: None,
                            };
                        } else {
                            if current.metadata.status != TurnStatus::InProgress {
                                continue;
                            }
                            current.metadata.root_turn_id =
                                current.metadata.root_turn_id.clone().or(turn.root_turn_id);
                            current.metadata.status = turn.status;
                            current.metadata.error = turn.error;
                            current.metadata.started_at =
                                turn.started_at.or(current.metadata.started_at);
                            current.metadata.completed_at = turn.completed_at;
                            current.metadata.duration_ms = turn.duration_ms;
                            current.terminal_ordinal = terminal;
                        }
                        current
                    }
                };
                if let Some(found) = items.get(&(owner, id.clone())) {
                    if current.first_user.is_none() {
                        current.first_user = found
                            .iter()
                            .filter(|(_, item)| item.kind == SummaryKind::User)
                            .min_by_key(|(_, item)| item.ordinal)
                            .map(|(id, _)| id.clone());
                    }
                    let final_agent =
                        latest_summary_id(found, SummaryKind::FinalAgent).or_else(|| {
                            (current.metadata.status != TurnStatus::InProgress)
                                .then(|| latest_summary_id(found, SummaryKind::LegacyAgent))
                                .flatten()
                        });
                    if final_agent.is_some() {
                        current.final_agent = final_agent;
                    }
                }
            }
            SourceTurnEvent::SummaryItem(ordinal, owner, turn_id, item, timestamp) => {
                let id = item.id().to_string();
                let kind = summary_kind(&item);
                let json =
                    serde_json::to_vec(&item.payload).map_err(super::thread_history_error)?;
                let entry = items
                    .entry((owner, turn_id.clone()))
                    .or_default()
                    .entry(id.clone())
                    .or_insert_with(|| AggregateItem {
                        ordinal,
                        item: StoredThreadItem {
                            turn_id: turn_id.clone(),
                            item_id: id.clone(),
                            updated_at_ordinal: ordinal,
                            created_at_ms: item.started_at_ms.unwrap_or(timestamp),
                            started_at_ms: item.started_at_ms,
                            completed_at_ms: item.completed_at_ms,
                            item_json: json.clone(),
                        },
                        kind,
                    });
                entry.item.updated_at_ordinal = ordinal;
                entry.item.started_at_ms = entry.item.started_at_ms.or(item.started_at_ms);
                entry.item.completed_at_ms = entry.item.completed_at_ms.or(item.completed_at_ms);
                entry.item.item_json = json;
                entry.kind = kind;
                if let Some(turn) = turns.get_mut(&turn_id)
                    && turn.owner == owner
                    && (turn.metadata.status == TurnStatus::InProgress
                        || (turn.metadata.status == TurnStatus::Completed
                            && turn.terminal_ordinal == Some(turn.ordinal)))
                {
                    if kind == SummaryKind::User && turn.first_user.is_none() {
                        turn.first_user = Some(id.clone());
                    }
                    if kind == SummaryKind::FinalAgent {
                        turn.final_agent = Some(id);
                    }
                }
            }
        }
    }
    Ok((turns, items))
}

/// Preserve the inherited start position only for a verified fork interruption.
fn normalize_inherited_fork_interruptions(
    events: &mut [SourceTurnEvent],
    segments: &[RolloutLineageSegment],
    predecessor_anchors: &[SourceItemAnchor],
    root_anchors: &[SourceItemAnchor],
) -> bool {
    let mut active: HashMap<String, (ThreadId, ThreadHistoryTurnMetadata)> = HashMap::new();
    let mut seen_owners = HashSet::new();
    let mut last_event: Option<(ThreadId, String)> = None;
    for event in events {
        let SourceTurnEvent::Turn(ordinal, owner, turn) = event else {
            continue;
        };
        let physical_owner = *owner;
        let first_for_owner = seen_owners.insert(physical_owner);
        if turn.status == TurnStatus::Interrupted
            && let Some((previous_owner, previous)) = active.get(&turn.turn_id)
            && *previous_owner != physical_owner
        {
            let mut matching = segments.iter().enumerate().filter(|(_, segment)| {
                segment.thread_id() == physical_owner && segment.contains_ordinal(*ordinal)
            });
            let segment = matching.next();
            let verified = matching.next().is_none()
                && segment.is_some_and(|(index, segment)| {
                    segment.uses_history_base
                        && index > 0
                        && segments[index - 1].thread_id() != physical_owner
                        && segment.history_page_start.is_none()
                        && segment.start_ordinal == segment.physical_start_ordinal
                        && !segment.filters_items()
                })
                && first_for_owner
                && last_event.as_ref() == Some(&(*previous_owner, turn.turn_id.clone()))
                && previous.started_at == turn.started_at
                && previous.root_turn_id == turn.root_turn_id
                && !predecessor_anchors
                    .iter()
                    .chain(root_anchors)
                    .any(|anchor| anchor.owner == physical_owner && anchor.ordinal < *ordinal);
            if !verified {
                return false;
            }
            // The child persisted the abort before any local turn or item. Its start and
            // summary items remain in the source turn, as in ThreadHistoryBuilder.
            *owner = *previous_owner;
        }
        if turn.status == TurnStatus::InProgress {
            active.insert(turn.turn_id.clone(), (*owner, turn.clone()));
        } else {
            active.remove(&turn.turn_id);
        }
        last_event = Some((physical_owner, turn.turn_id.clone()));
    }
    true
}

/// Pages canonical turn records when SQLite projections span multiple physical rollout IDs.
async fn source_backed_turns(
    store: &LocalThreadStore,
    params: &ListTurnsParams,
) -> ThreadStoreResult<Option<TurnPage>> {
    let Some((graph, scan, root)) =
        uncertified_items::indexed_sources_with_root(store, params.thread_id).await?
    else {
        return Ok(None);
    };
    let mut events = scan.turn_events.clone();
    events.extend(root.turns);
    events.sort_by_key(SourceTurnEvent::ordinal);
    if !normalize_inherited_fork_interruptions(
        &mut events,
        graph.lineage.segments(),
        &scan.anchors,
        &root.anchors,
    ) {
        return Ok(None);
    }
    let (initial, _) = reduce_aggregate_sources(events.clone())?;
    if initial.is_empty() && !scan.attached {
        if graph.lineage.segments().len() > 900 {
            return Ok(None);
        }
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT EXISTS(SELECT 1 FROM thread_turns WHERE thread_id IN (",
        );
        let mut ids = query.separated(", ");
        for segment in graph.lineage.segments() {
            ids.push_bind(segment.rollout_id().to_string());
        }
        ids.push_unseparated("))");
        let has_rows: i64 = query
            .build_query_scalar()
            .fetch_one(store.thread_history_db().await?)
            .await
            .map_err(super::thread_history_error)?;
        if has_rows != 0 {
            // The compatibility reader may use SQLite; this does not certify its freshness.
            return Ok(None);
        }
    }
    let cursor = parse_cursor(
        params.cursor.as_deref(),
        params.thread_id,
        graph.lineage.root_rollout_id(),
        CursorScope::Turns,
    )?;
    if cursor.as_ref().is_some_and(|cursor| {
        graph
            .lineage
            .segments()
            .iter()
            .filter(|segment| segment.contains_ordinal(cursor.rollout_ordinal))
            .count()
            != 1
    }) {
        return Err(invalid_cursor(
            "position outside or ambiguous in thread lineage",
        ));
    }
    let mut selected = initial
        .iter()
        .filter(|(_, turn)| {
            cursor.as_ref().is_none_or(|cursor| {
                match (params.sort_direction, cursor.include_anchor) {
                    (crate::SortDirection::Asc, true) => turn.ordinal >= cursor.rollout_ordinal,
                    (crate::SortDirection::Asc, false) => turn.ordinal > cursor.rollout_ordinal,
                    (crate::SortDirection::Desc, true) => turn.ordinal <= cursor.rollout_ordinal,
                    (crate::SortDirection::Desc, false) => turn.ordinal < cursor.rollout_ordinal,
                }
            })
        })
        .map(|(id, turn)| (id.clone(), turn.ordinal))
        .collect::<Vec<_>>();
    selected.sort_by_key(|(_, ordinal)| *ordinal);
    if params.sort_direction == crate::SortDirection::Desc {
        selected.reverse();
    }
    let has_more = selected.len() > params.page_size;
    selected.truncate(params.page_size);
    let backwards_cursor = selected
        .first()
        .map(|(_, ordinal)| {
            serialize_cursor(
                params.thread_id,
                graph.lineage.root_rollout_id(),
                CursorScope::Turns,
                sqlite_integer(*ordinal, "rollout ordinal")?,
                true,
            )
        })
        .transpose()?;
    let next_cursor = if has_more {
        selected
            .last()
            .map(|(_, ordinal)| {
                serialize_cursor(
                    params.thread_id,
                    graph.lineage.root_rollout_id(),
                    CursorScope::Turns,
                    sqlite_integer(*ordinal, "rollout ordinal")?,
                    false,
                )
            })
            .transpose()?
    } else {
        None
    };
    let (turns, items) = if params.items_view == StoredTurnItemsView::Summary
        && !selected.is_empty()
    {
        for (id, _) in &selected {
            let owner = initial.get(id).expect("selected turn exists").owner;
            if scan
                .anchors
                .iter()
                .chain(root.anchors.iter())
                .any(|anchor| anchor.turn_id == *id && anchor.owner != owner)
            {
                return Ok(None);
            }
        }
        let ids = selected.iter().map(|(id, _)| id.clone()).collect();
        let Some(item_events) =
            uncertified_items::scan_selected_items(&graph, &scan, ids, root.root_snapshot.as_ref())
                .await?
        else {
            return Err(ThreadStoreError::Conflict {
                message: "could not read selected turn items".to_string(),
            });
        };
        events.extend(item_events);
        events.sort_by_key(SourceTurnEvent::ordinal);
        reduce_aggregate_sources(events)?
    } else {
        (initial, HashMap::new())
    };
    #[cfg(test)]
    pause_source_item_before_revalidation(params.thread_id).await;
    if !uncertified_items::sources_still_current(
        store,
        params.thread_id,
        &graph,
        &scan,
        root.root_snapshot.as_ref(),
    )
    .await?
    {
        return Err(ThreadStoreError::Conflict {
            message: "rotated thread sources changed while listing turns".to_string(),
        });
    }
    let mut output = Vec::with_capacity(selected.len());
    for (id, _) in selected {
        let Some(source) = turns.get(&id) else {
            return Err(ThreadStoreError::Conflict {
                message: "selected source turn changed".to_string(),
            });
        };
        let mut summary = Vec::new();
        if let Some(found) = items.get(&(source.owner, id.clone())) {
            for item_id in [&source.first_user, &source.final_agent]
                .into_iter()
                .flatten()
            {
                if let Some(item) = found.get(item_id) {
                    summary.push((item.ordinal, item.item.clone()));
                }
            }
        }
        summary.sort_by_key(|(ordinal, _)| *ordinal);
        let metadata = &source.metadata;
        let status = match &metadata.status {
            TurnStatus::Completed => StoredTurnStatus::Completed,
            TurnStatus::Interrupted => StoredTurnStatus::Interrupted,
            TurnStatus::Failed => StoredTurnStatus::Failed,
            TurnStatus::InProgress => StoredTurnStatus::InProgress,
        };
        output.push(StoredTurn {
            turn_id: id,
            root_turn_id: metadata.root_turn_id.clone(),
            items: summary.into_iter().map(|(_, item)| item).collect(),
            items_view: params.items_view,
            status,
            error: metadata.error.as_ref().map(|error| StoredTurnError {
                message: error.message.clone(),
                codex_error_info: error.codex_error_info.clone(),
                additional_details: error.additional_details.clone(),
            }),
            started_at: metadata.started_at,
            completed_at: metadata.completed_at,
            duration_ms: metadata.duration_ms,
        });
    }
    Ok(Some(TurnPage {
        turns: output,
        next_cursor,
        backwards_cursor,
    }))
}

pub(in crate::local) async fn try_list_source_backed_turns(
    store: &LocalThreadStore,
    params: &ListTurnsParams,
) -> ThreadStoreResult<Option<TurnPage>> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_turns",
    )
    .await?;
    validate_page_size(params.page_size)?;
    source_backed_turns(store, params).await
}

pub(in crate::local) async fn resume_backwards_cursors(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<(Option<String>, Option<String>)> {
    validate_thread_for_paginated_reads(store, thread_id, true, "list_turns").await?;
    let item_params = ListItemsParams {
        item_ids: None,
        thread_id,
        turn_id: None,
        include_archived: true,
        position: None,
        page_size: 1,
        sort_direction: crate::SortDirection::Desc,
        sort_key: crate::ItemSortKey::CreatedAtOrdinal,
        after_updated_at_ordinal: None,
        before_updated_at_ordinal: None,
    };
    if let Some(cursors) = certified_resume_backwards_cursors(store, &item_params).await? {
        return Ok(cursors);
    }
    if let Some(cursors) = source_resume_cursors(store, thread_id).await? {
        return Ok(cursors);
    }
    let pool = store.thread_history_db().await?;
    let lineage = if let Some(lineage) = indexed_same_thread_lineage(store, thread_id).await? {
        lineage
    } else {
        match page_lazy_turn_rows(
            store,
            thread_id,
            None,
            1,
            StoredTurnItemsView::NotLoaded,
            /*require_lookahead*/ false,
        )
        .await?
        {
            LazyPage::Full(lineage) => lineage,
            LazyPage::Page(_, turn_lineage) => {
                match page_lazy_item_rows(store, &item_params, /*require_lookahead*/ false).await? {
                    LazyPage::Page(_, item_lineage) => {
                        if !same_walked_prefix(&turn_lineage, &item_lineage) {
                            return Err(ThreadStoreError::Conflict {
                                message: "history changed while preparing resume cursors"
                                    .to_string(),
                            });
                        }
                        if turn_lineage.segments().len() >= item_lineage.segments().len() {
                            turn_lineage
                        } else {
                            item_lineage
                        }
                    }
                    LazyPage::Full(lineage) => {
                        if lineage.root_rollout_id() != turn_lineage.root_rollout_id()
                            || lineage.leaf_path() != turn_lineage.leaf_path()
                        {
                            return Err(ThreadStoreError::Conflict {
                                message: "selected rollout changed while preparing resume cursors"
                                    .to_string(),
                            });
                        }
                        lineage
                    }
                }
            }
        }
    };
    let turns = page_turn_rows(
        pool,
        thread_id,
        &lineage,
        None,
        1,
        crate::SortDirection::Desc,
        StoredTurnItemsView::NotLoaded,
    )
    .await?;
    let items = page_item_rows(pool, &lineage, &item_params).await?;
    Ok((turns.backwards_cursor, items.backwards_cursor))
}

/// Derives both resume cursors from the same verified source lineage.
async fn source_resume_cursors(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<(Option<String>, Option<String>)>> {
    let Some((graph, scan, root)) =
        uncertified_items::indexed_sources_with_root(store, thread_id).await?
    else {
        return Ok(None);
    };
    let mut events = scan.turn_events.clone();
    events.extend(root.turns);
    events.sort_by_key(SourceTurnEvent::ordinal);
    if !normalize_inherited_fork_interruptions(
        &mut events,
        graph.lineage.segments(),
        &scan.anchors,
        &root.anchors,
    ) {
        return Ok(None);
    }
    let (turns, _) = reduce_aggregate_sources(events)?;
    let newest_turn = turns.values().map(|turn| turn.ordinal).max();
    let mut creations: HashMap<(ThreadId, &str, &str), u64> = HashMap::new();
    for anchor in scan.anchors.iter().chain(root.anchors.iter()) {
        creations
            .entry((
                anchor.owner,
                anchor.turn_id.as_str(),
                anchor.item_id.as_str(),
            ))
            .and_modify(|ordinal| *ordinal = (*ordinal).min(anchor.ordinal))
            .or_insert(anchor.ordinal);
    }
    let newest_item = creations.values().copied().max();
    if newest_turn.is_none() && newest_item.is_none() && !scan.attached {
        return Ok(None);
    }
    let turn_cursor = newest_turn
        .map(|ordinal| {
            serialize_cursor(
                thread_id,
                graph.lineage.root_rollout_id(),
                CursorScope::Turns,
                sqlite_integer(ordinal, "rollout ordinal")?,
                true,
            )
        })
        .transpose()?;
    let item_cursor = newest_item
        .map(|ordinal| {
            serialize_cursor(
                thread_id,
                graph.lineage.root_rollout_id(),
                CursorScope::ItemsByCreatedAtOrdinal,
                sqlite_integer(ordinal, "rollout ordinal")?,
                true,
            )
        })
        .transpose()?;
    if !uncertified_items::sources_still_current(
        store,
        thread_id,
        &graph,
        &scan,
        root.root_snapshot.as_ref(),
    )
    .await?
    {
        return Err(ThreadStoreError::Conflict {
            message: "rotated thread sources changed while preparing resume cursors".to_string(),
        });
    }
    Ok(Some((turn_cursor, item_cursor)))
}

pub(in crate::local) async fn try_source_resume_backwards_cursors(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<(Option<String>, Option<String>)>> {
    validate_thread_for_paginated_reads(store, thread_id, true, "list_turns").await?;
    source_resume_cursors(store, thread_id).await
}

fn same_walked_prefix(left: &RolloutLineage, right: &RolloutLineage) -> bool {
    left.root_rollout_id() == right.root_rollout_id()
        && left.leaf_path() == right.leaf_path()
        && left
            .segments()
            .iter()
            .rev()
            .zip(right.segments().iter().rev())
            .all(|(left, right)| left == right)
}

async fn list_turns_from_lineage(
    store: &LocalThreadStore,
    params: ListTurnsParams,
    lineage: &RolloutLineage,
) -> ThreadStoreResult<TurnPage> {
    let pool = store.thread_history_db().await?;
    let page = page_turn_rows(
        pool,
        params.thread_id,
        lineage,
        params.cursor.as_deref(),
        params.page_size,
        params.sort_direction,
        params.items_view,
    )
    .await?;
    turns_from_page(store, params.items_view, lineage, page).await
}

async fn turns_from_page(
    store: &LocalThreadStore,
    items_view: StoredTurnItemsView,
    lineage: &RolloutLineage,
    page: SegmentPage<StoredTurnRow>,
) -> ThreadStoreResult<TurnPage> {
    let pool = store.thread_history_db().await?;
    let mut turns = Vec::with_capacity(page.rows.len());
    for turn in page.rows {
        let items = match items_view {
            StoredTurnItemsView::NotLoaded => Vec::new(),
            StoredTurnItemsView::Summary
                if matches!(turn.status, StoredTurnStatus::Interrupted)
                    && turn.first_user_item_id.is_none()
                    && turn.final_agent_item_id.is_none() =>
            {
                // Synthetic fork-boundary rows are interrupted without local summary IDs.
                // Load their summary from the earliest visible source turn.
                load_inherited_summary_items(pool, lineage, &turn).await?
            }
            StoredTurnItemsView::Summary => turn.summary_items,
        };
        turns.push(StoredTurn {
            turn_id: turn.turn_id,
            root_turn_id: turn.root_turn_id,
            items,
            items_view,
            status: turn.status,
            error: turn.error,
            started_at: turn.started_at,
            completed_at: turn.completed_at,
            duration_ms: turn.duration_ms,
        });
    }

    Ok(TurnPage {
        turns,
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

pub(in crate::local) async fn list_items(
    store: &LocalThreadStore,
    mut params: ListItemsParams,
) -> ThreadStoreResult<ItemPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_items",
    )
    .await?;
    if let Some(ids) = &params.item_ids {
        if params.turn_id.is_none()
            || params.position.is_some()
            || params.after_updated_at_ordinal.is_some()
            || params.before_updated_at_ordinal.is_some()
            || params.sort_key != crate::ItemSortKey::CreatedAtOrdinal
            || !(1..=100).contains(&ids.len())
        {
            return Err(ThreadStoreError::InvalidRequest {
                message: "itemIds requires turnId, 1 to 100 IDs, and no cursor or update replay"
                    .to_string(),
            });
        }
        params.page_size = ids.len();
        params.sort_direction = crate::SortDirection::Asc;
    }
    validate_page_size(params.page_size)?;
    if let Some(page) = page_certified_item_rows(store, &params).await? {
        return match page {
            LazyPage::Page(page, _) => Ok(ItemPage {
                items: page.rows.into_iter().map(|row| row.item).collect(),
                next_cursor: page.next_cursor,
                backwards_cursor: page.backwards_cursor,
            }),
            LazyPage::Full(lineage) => {
                let pool = store.thread_history_db().await?;
                let page = page_item_rows(pool, &lineage, &params).await?;
                Ok(ItemPage {
                    items: page.rows.into_iter().map(|row| row.item).collect(),
                    next_cursor: page.next_cursor,
                    backwards_cursor: page.backwards_cursor,
                })
            }
        };
    }
    if let Some(page) = source_backed_items(store, &params).await? {
        return Ok(page);
    }
    let lineage = if let Some(lineage) =
        indexed_same_thread_lineage(store, params.thread_id).await?
    {
        lineage
    } else if params.sort_key == crate::ItemSortKey::CreatedAtOrdinal
        && params.after_updated_at_ordinal.is_none()
        && params.before_updated_at_ordinal.is_none()
        && params.item_ids.is_none()
        && params.page_size < i64::MAX as usize - 1
        && (matches!(params.sort_direction, crate::SortDirection::Desc) || params.turn_id.is_some())
    {
        let page = if matches!(params.sort_direction, crate::SortDirection::Desc) {
            page_lazy_item_rows(store, &params, /*require_lookahead*/ true).await?
        } else {
            page_certified_ascending_item_rows(store, &params).await?
        };
        match page {
            LazyPage::Page(page, _) => {
                return Ok(ItemPage {
                    items: page.rows.into_iter().map(|row| row.item).collect(),
                    next_cursor: page.next_cursor,
                    backwards_cursor: page.backwards_cursor,
                });
            }
            LazyPage::Full(lineage) => lineage,
        }
    } else {
        store
            .resolve_rollout_lineage_for_history_page(params.thread_id)
            .await?
    };
    let pool = store.thread_history_db().await?;
    let page = page_item_rows(pool, &lineage, &params).await?;

    Ok(ItemPage {
        items: page.rows.into_iter().map(|row| row.item).collect(),
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

/// Pages turn items from selected source records rather than a partial SQLite key layout.
pub(super) async fn source_backed_items(
    store: &LocalThreadStore,
    params: &ListItemsParams,
) -> ThreadStoreResult<Option<ItemPage>> {
    source_backed_items_for_turns(store, params, None).await
}

/// Tries one source-verified snapshot for every turn in a Full compatibility page.
pub(in crate::local) async fn try_source_backed_full_turn_items(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    turn_ids: &[String],
) -> ThreadStoreResult<Option<HashMap<String, Vec<StoredThreadItem>>>> {
    let selected_turns: HashSet<String> = turn_ids.iter().cloned().collect();
    if selected_turns.is_empty() || selected_turns.len() != turn_ids.len() {
        return Ok(None);
    }
    validate_thread_for_paginated_reads(store, thread_id, true, "list_items").await?;
    if store.has_current_history_certificate(thread_id).await? {
        return Ok(None);
    }
    let params = ListItemsParams {
        item_ids: None,
        thread_id,
        turn_id: None,
        include_archived: true,
        position: None,
        page_size: 40_000,
        sort_direction: crate::SortDirection::Asc,
        sort_key: crate::ItemSortKey::CreatedAtOrdinal,
        after_updated_at_ordinal: None,
        before_updated_at_ordinal: None,
    };
    let page = match source_backed_items_for_turns(store, &params, Some(&selected_turns)).await {
        Ok(Some(page)) if page.next_cursor.is_none() => page,
        Ok(_) => return Ok(None),
        Err(ThreadStoreError::Internal { message })
            if message == uncertified_items::SUMMARY_SOURCE_SIZE_LIMIT_ERROR
                || message
                    == format!(
                        "failed to access thread history: {}",
                        uncertified_items::SUMMARY_SOURCE_SIZE_LIMIT_ERROR
                    ) =>
        {
            // Individual-source and aggregate limits use different error wrappers. The
            // combined page can exceed either even when individual turns do not.
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    if store.has_current_history_certificate(thread_id).await? {
        return Ok(None);
    }
    let mut items = HashMap::new();
    for item in page.items {
        items
            .entry(item.turn_id.clone())
            .or_insert_with(Vec::new)
            .push(item);
    }
    Ok(Some(items))
}

async fn source_backed_items_for_turns(
    store: &LocalThreadStore,
    params: &ListItemsParams,
    requested_turns: Option<&HashSet<String>>,
) -> ThreadStoreResult<Option<ItemPage>> {
    if params.turn_id.as_deref() == Some("") {
        return Ok(None);
    }
    let turn_id = params.turn_id.as_deref();
    if params.sort_key != crate::ItemSortKey::CreatedAtOrdinal
        || params.after_updated_at_ordinal.is_some()
        || params.before_updated_at_ordinal.is_some()
    {
        return Ok(None);
    }
    if let Some(ListItemsPosition::ItemAnchor { item_id }) = params.position.as_ref()
        && (turn_id.is_none_or(str::is_empty) || item_id.is_empty())
    {
        // Preserve the compatibility reader's request validation for malformed anchors.
        return Ok(None);
    }
    let Some((graph, scan, root)) =
        uncertified_items::indexed_sources_with_root(store, params.thread_id).await?
    else {
        return Ok(None);
    };
    let cursor_text = match params.position.as_ref() {
        Some(ListItemsPosition::Cursor(cursor)) => Some(cursor.as_str()),
        _ => None,
    };
    let cursor = parse_cursor(
        cursor_text,
        params.thread_id,
        graph.lineage.root_rollout_id(),
        CursorScope::ItemsByCreatedAtOrdinal,
    )?;
    if cursor.as_ref().is_some_and(|cursor| {
        graph
            .lineage
            .segments()
            .iter()
            .filter(|segment| segment.contains_ordinal(cursor.rollout_ordinal))
            .count()
            != 1
    }) {
        return Err(invalid_cursor(
            "position outside or ambiguous in thread lineage",
        ));
    }
    let selected_turns: HashSet<String> = if let Some(turns) = requested_turns {
        turns.clone()
    } else if let Some(turn) = turn_id {
        [turn.to_string()].into_iter().collect()
    } else {
        let mut first: HashMap<(ThreadId, &str, &str), u64> = HashMap::new();
        for anchor in scan.anchors.iter().chain(root.anchors.iter()) {
            first
                .entry((anchor.owner, &anchor.turn_id, &anchor.item_id))
                .and_modify(|ordinal| *ordinal = (*ordinal).min(anchor.ordinal))
                .or_insert(anchor.ordinal);
        }
        let mut candidates = first
            .into_iter()
            .filter(|(_, ordinal)| {
                cursor.as_ref().is_none_or(|cursor| {
                    match (params.sort_direction, cursor.include_anchor) {
                        (crate::SortDirection::Asc, true) => *ordinal >= cursor.rollout_ordinal,
                        (crate::SortDirection::Asc, false) => *ordinal > cursor.rollout_ordinal,
                        (crate::SortDirection::Desc, true) => *ordinal <= cursor.rollout_ordinal,
                        (crate::SortDirection::Desc, false) => *ordinal < cursor.rollout_ordinal,
                    }
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(_, ordinal)| *ordinal);
        if params.sort_direction == crate::SortDirection::Desc {
            candidates.reverse();
        }
        candidates
            .into_iter()
            .take(params.page_size.saturating_add(1))
            .map(|((_, turn, _), _)| turn.to_string())
            .collect()
    };
    if selected_turns.is_empty() {
        if scan.anchors.is_empty() && root.anchors.is_empty() && !scan.attached {
            return Ok(None);
        }
        if !uncertified_items::sources_still_current(
            store,
            params.thread_id,
            &graph,
            &scan,
            root.root_snapshot.as_ref(),
        )
        .await?
        {
            return Err(ThreadStoreError::Conflict {
                message: "rotated thread sources changed while listing items".to_string(),
            });
        }
        return Ok(Some(ItemPage {
            items: Vec::new(),
            next_cursor: None,
            backwards_cursor: None,
        }));
    }
    let mut owner_by_turn = HashMap::new();
    for anchor in scan.anchors.iter().chain(root.anchors.iter()) {
        if selected_turns.contains(&anchor.turn_id)
            && owner_by_turn
                .insert(anchor.turn_id.as_str(), anchor.owner)
                .is_some_and(|owner| owner != anchor.owner)
        {
            return Ok(None);
        }
    }
    let Some(events) = uncertified_items::scan_selected_items(
        &graph,
        &scan,
        selected_turns.clone(),
        root.root_snapshot.as_ref(),
    )
    .await?
    else {
        return Err(ThreadStoreError::Conflict {
            message: "could not read selected turn items".to_string(),
        });
    };
    let (_, items) = reduce_aggregate_sources(events)?;
    let all = items
        .into_iter()
        .flat_map(|(_, items)| items.into_values())
        .collect::<Vec<_>>();
    if let Some(requested_turns) = requested_turns {
        let present: HashSet<&str> = all.iter().map(|item| item.item.turn_id.as_str()).collect();
        if all.len() > params.page_size
            || requested_turns
                .iter()
                .any(|turn| !present.contains(turn.as_str()))
        {
            return Ok(None);
        }
    }
    if all.is_empty() && !scan.attached {
        // Only an active root-only turn with no SQLite items can be answered
        // from an empty source scan; other empty scans may hide legacy rows.
        if let Some(turn_id) = turn_id
            && graph.lineage.segments().len() < 900
            && root.turns.iter().any(|event| {
                matches!(event,
                SourceTurnEvent::Turn(_, _, turn)
                    if turn.turn_id == turn_id && turn.status == TurnStatus::InProgress)
            })
            && !root.turns.iter().any(|event| {
                matches!(event,
                SourceTurnEvent::Turn(_, _, turn)
                    if turn.turn_id == turn_id && turn.status != TurnStatus::InProgress)
            })
            && !scan.turn_events.iter().any(|event| {
                matches!(event,
                SourceTurnEvent::Turn(_, _, turn) if turn.turn_id == turn_id)
            })
        {
            let pool = store.thread_history_db().await?;
            let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT EXISTS(SELECT 1 FROM thread_items WHERE turn_id = ",
            );
            query.push_bind(turn_id).push(" AND thread_id IN (");
            let mut ids = query.separated(", ");
            ids.push_bind(params.thread_id.to_string());
            for segment in graph.lineage.segments() {
                ids.push_bind(segment.rollout_id().to_string());
            }
            ids.push_unseparated("))");
            let has_rows: i64 = query
                .build_query_scalar()
                .fetch_one(pool)
                .await
                .map_err(super::thread_history_error)?;
            if has_rows == 0 {
                // The SQLite query is the snapshot point only if no source changed
                // since the root scan. An append could otherwise precede the query
                // while its items are absent from the captured root snapshot.
                #[cfg(test)]
                pause_source_item_before_revalidation(params.thread_id).await;
                if !uncertified_items::sources_still_current(
                    store,
                    params.thread_id,
                    &graph,
                    &scan,
                    None,
                )
                .await?
                {
                    return Err(ThreadStoreError::Conflict {
                        message: "rotated thread sources changed while listing items".to_string(),
                    });
                }
                return Ok(Some(ItemPage {
                    items: Vec::new(),
                    next_cursor: None,
                    backwards_cursor: None,
                }));
            }
        }
        // No canonical items were found; the compatibility reader does not certify SQLite.
        return Ok(None);
    }
    let anchor_ordinal = if let Some(ListItemsPosition::ItemAnchor { item_id }) =
        params.position.as_ref()
    {
        match all.iter().find(|item| item.item.item_id == *item_id) {
            Some(item) => Some(item.ordinal),
            None => {
                if !uncertified_items::sources_still_current(
                    store,
                    params.thread_id,
                    &graph,
                    &scan,
                    root.root_snapshot.as_ref(),
                )
                .await?
                {
                    return Err(ThreadStoreError::Conflict {
                        message: "rotated thread sources changed while listing items".to_string(),
                    });
                }
                return Err(ThreadStoreError::InvalidRequest {
                    message:
                        "cursor.itemId does not identify an item in the requested history scope"
                            .to_string(),
                });
            }
        }
    } else {
        None
    };
    let by_ordinal: HashMap<u64, &AggregateItem> =
        all.iter().map(|item| (item.ordinal, item)).collect();
    if by_ordinal.len() != all.len() {
        return Err(ThreadStoreError::Conflict {
            message: "ambiguous source item positions".to_string(),
        });
    }
    if graph
        .lineage
        .segments()
        .len()
        .saturating_add(selected_turns.len())
        > 900
    {
        if requested_turns.is_some() || params.item_ids.is_some() || anchor_ordinal.is_some() {
            return Ok(None);
        }
        return Err(ThreadStoreError::Internal {
            message: "rotated history exceeds the SQLite comparison limit".to_string(),
        });
    }
    let pool = store.thread_history_db().await?;
    let mut transaction = pool.begin().await.map_err(super::thread_history_error)?;
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT turn_id, item_id, rollout_ordinal, updated_at_ordinal, created_at_ms, started_at_ms, completed_at_ms, item_json FROM thread_items WHERE turn_id IN (",
    );
    let mut turns = query.separated(", ");
    for turn in &selected_turns {
        turns.push_bind(turn.clone());
    }
    turns.push_unseparated(") AND thread_id IN (");
    let mut ids = query.separated(", ");
    for segment in graph.lineage.segments() {
        ids.push_bind(segment.rollout_id().to_string());
    }
    ids.push_unseparated(") LIMIT 40001");
    let rows = query
        .build()
        .fetch_all(&mut *transaction)
        .await
        .map_err(super::thread_history_error)?;
    if rows.len() > 40000 {
        if requested_turns.is_some() {
            return Ok(None);
        }
        return Err(ThreadStoreError::Internal {
            message: "rotated item projection exceeds the comparison limit".to_string(),
        });
    }
    for row in rows {
        let row = stored_thread_item_row(row)?;
        let Some(expected) = u64::try_from(row.position.rollout_ordinal)
            .ok()
            .and_then(|ordinal| by_ordinal.get(&ordinal))
        else {
            continue;
        };
        if row.item.turn_id == expected.item.turn_id
            && row.item.item_id == expected.item.item_id
            && row.item.updated_at_ordinal == expected.item.updated_at_ordinal
        {
            match uncertified_items::compare_item_json(
                &row.item.item_json,
                &expected.item.item_json,
            ) {
                uncertified_items::ItemComparison::Equivalent => {}
                uncertified_items::ItemComparison::Contradict => {
                    return Err(ThreadStoreError::Conflict {
                        message: "thread item projection disagrees with the selected rollout"
                            .to_string(),
                    });
                }
                uncertified_items::ItemComparison::Uncertain => {
                    tracing::warn!(thread_id = %params.thread_id, "thread item representation differs; returning source item")
                }
            }
        }
    }
    drop(by_ordinal);
    let mut selected = all
        .into_iter()
        .filter(|item| {
            if params
                .item_ids
                .as_ref()
                .is_some_and(|ids| !ids.contains(&item.item.item_id))
            {
                return false;
            }
            if let Some(anchor) = anchor_ordinal {
                return match params.sort_direction {
                    crate::SortDirection::Asc => item.ordinal > anchor,
                    crate::SortDirection::Desc => item.ordinal < anchor,
                };
            }
            cursor.as_ref().is_none_or(|cursor| {
                match (params.sort_direction, cursor.include_anchor) {
                    (crate::SortDirection::Asc, true) => item.ordinal >= cursor.rollout_ordinal,
                    (crate::SortDirection::Asc, false) => item.ordinal > cursor.rollout_ordinal,
                    (crate::SortDirection::Desc, true) => item.ordinal <= cursor.rollout_ordinal,
                    (crate::SortDirection::Desc, false) => item.ordinal < cursor.rollout_ordinal,
                }
            })
        })
        .collect::<Vec<_>>();
    selected.sort_by_key(|item| item.ordinal);
    if params.sort_direction == crate::SortDirection::Desc {
        selected.reverse();
    }
    let has_more = selected.len() > params.page_size;
    selected.truncate(params.page_size);
    let backwards_cursor = selected
        .first()
        .map(|item| {
            serialize_cursor(
                params.thread_id,
                graph.lineage.root_rollout_id(),
                CursorScope::ItemsByCreatedAtOrdinal,
                sqlite_integer(item.ordinal, "rollout ordinal")?,
                true,
            )
        })
        .transpose()?;
    let next_cursor = if has_more {
        selected
            .last()
            .map(|item| {
                serialize_cursor(
                    params.thread_id,
                    graph.lineage.root_rollout_id(),
                    CursorScope::ItemsByCreatedAtOrdinal,
                    sqlite_integer(item.ordinal, "rollout ordinal")?,
                    false,
                )
            })
            .transpose()?
    } else {
        None
    };
    #[cfg(test)]
    pause_source_item_before_revalidation(params.thread_id).await;
    if !uncertified_items::sources_still_current(
        store,
        params.thread_id,
        &graph,
        &scan,
        root.root_snapshot.as_ref(),
    )
    .await?
    {
        return Err(ThreadStoreError::Conflict {
            message: "rotated thread sources changed while listing items".to_string(),
        });
    }
    transaction
        .commit()
        .await
        .map_err(super::thread_history_error)?;
    Ok(Some(ItemPage {
        items: selected.into_iter().map(|item| item.item).collect(),
        next_cursor,
        backwards_cursor,
    }))
}

/// Treats a fully projected root thread as one logical ordinal range instead of its segments.
///
/// Immutable predecessors were validated when their SQLite rows were projected. Revalidating
/// every predecessor during an indexed page would make the request depend on thread length.
pub(in crate::local) async fn indexed_same_thread_lineage(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<RolloutLineage>> {
    let Some(resolved) =
        super::super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
            .await?
    else {
        return Ok(None);
    };
    indexed_same_thread_lineage_from_resolved(store, thread_id, resolved).await
}

async fn indexed_same_thread_lineage_from_resolved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    resolved: ResolvedThreadRollout,
) -> ThreadStoreResult<Option<RolloutLineage>> {
    let rollout_id = resolved.rollout_id;
    let rollout_path = resolved.path;
    let authenticated_session_meta = resolved.authenticated_session_meta;
    let Some(projection) = super::projection_state(store, rollout_id).await? else {
        return Ok(None);
    };
    if !projection.lineage_complete {
        return Ok(None);
    }
    let Some(state_db) = store.state_db().await else {
        return Ok(None);
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to read indexed thread metadata: {error}"),
            })?
    else {
        return Ok(None);
    };
    if metadata.archived_at.is_some() {
        return Ok(None);
    }

    let Ok(file_metadata) = tokio::fs::metadata(rollout_path.as_path()).await else {
        return Ok(None);
    };
    if file_metadata.len() != projection.next_byte_offset {
        return Ok(None);
    }

    let session_meta = match authenticated_session_meta {
        Some(session_meta) => session_meta,
        None => {
            let Ok(session_meta) =
                codex_rollout::read_session_meta_line_exact(rollout_path.as_path()).await
            else {
                return Ok(None);
            };
            session_meta.meta
        }
    };
    if session_meta.id != thread_id
        || session_meta.history_mode != ThreadHistoryMode::Paginated
        || session_meta.attached_thread
        || session_meta.forked_from_id.is_some()
        || session_meta.history_base.is_some()
        || session_meta.subagent_history_start_ordinal.is_some()
    {
        return Ok(None);
    }

    Ok(Some(RolloutLineage::from_segments(
        thread_id,
        rollout_id,
        rollout_path.clone(),
        vec![RolloutLineageSegment {
            thread_id,
            rollout_id,
            segment_id: session_meta.segment_id,
            rollout_path,
            start_ordinal: 0,
            physical_start_ordinal: 0,
            history_page_visible: true,
            history_page_start: None,
            end_ordinal_exclusive: None,
            jsonl_end_byte_offset: None,
            end_byte_offset: None,
            byte_boundary_recorded: false,
            physical_size: None,
            filter_texts: Vec::new(),
            goal_supervisor_provenance: Default::default(),
            uses_history_base: false,
            uses_fork_boundary: false,
        }],
    )))
}

/// Checks the complete root checkpoint without resolving the selected rollout a second time.
///
/// Forked histories retain the caller's complete lineage check. A complete same-thread native
/// `history_base` projection is already one logical ordinal range. Empty turns are valid in any
/// status; out-of-band projection mutations invalidate the checkpoint through SQLite triggers.
pub(in crate::local) async fn has_complete_root_projection_for_resolved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    resolved: ResolvedThreadRollout,
) -> ThreadStoreResult<bool> {
    let Some(meta) = resolved.authenticated_session_meta.as_ref() else {
        return Ok(false);
    };
    if meta.id != thread_id
        || meta.history_mode != ThreadHistoryMode::Paginated
        || meta.history_base.is_none()
        || meta.attached_thread
        || meta.forked_from_id.is_some()
        || meta.subagent_history_start_ordinal.is_some()
    {
        return Ok(false);
    }
    let Some(state_db) = store.state_db().await else {
        return Ok(false);
    };
    let Some(metadata) = state_db
        .get_thread(thread_id)
        .await
        .map_err(super::thread_history_error)?
    else {
        return Ok(false);
    };
    if metadata.archived_at.is_some()
        || !codex_rollout::rollout_paths_match(&metadata.rollout_path, &resolved.path).await
    {
        return Ok(false);
    }
    super::projection_certificate::certificate_is_current_for_single_owner(
        store,
        resolved.rollout_id,
        &resolved.path,
        thread_id,
    )
    .await
}

/// Read an existing segmented legacy projection without exposing indexed cursors.
pub(in crate::local) async fn list_segmented_legacy_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<Option<TurnPage>> {
    list_segmented_legacy_turns_with_lookup(store, params, LegacyProjectionLookup::Backfill).await
}

/// Reads a complete legacy projection without creating or rebuilding its history database.
pub(in crate::local) async fn list_existing_segmented_legacy_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<Option<TurnPage>> {
    list_segmented_legacy_turns_with_lookup(store, params, LegacyProjectionLookup::ExistingOnly)
        .await
}

/// Checks legacy projection completeness without starting an initial history backfill.
pub(in crate::local) async fn has_complete_segmented_legacy_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    Ok(
        existing_legacy_projection(store, thread_id, LegacyProjectionLookup::ExistingOnly)
            .await?
            .is_some(),
    )
}

async fn list_segmented_legacy_turns_with_lookup(
    store: &LocalThreadStore,
    params: ListTurnsParams,
    lookup: LegacyProjectionLookup,
) -> ThreadStoreResult<Option<TurnPage>> {
    validate_page_size(params.page_size)?;
    let _live_writer_guard = store.live_writer_locks.lock(params.thread_id).await;
    let Some((pool, rollout_id)) =
        existing_legacy_projection(store, params.thread_id, lookup).await?
    else {
        return Ok(None);
    };
    let indexed_cursor = match params.cursor.as_deref() {
        Some(cursor) => Some(legacy_turn_cursor_to_index(pool, rollout_id, cursor).await?),
        None => None,
    };
    let page = page_indexed_turn_rows(
        pool,
        rollout_id,
        indexed_cursor.as_deref(),
        params.page_size,
        params.sort_direction,
    )
    .await?;
    let mut turns = Vec::with_capacity(page.rows.len());
    for turn in page.rows {
        let items = match params.items_view {
            StoredTurnItemsView::NotLoaded => Vec::new(),
            StoredTurnItemsView::Summary => {
                load_legacy_summary_items(pool, rollout_id, &turn).await?
            }
        };
        turns.push(StoredTurn {
            turn_id: turn.turn_id,
            root_turn_id: turn.root_turn_id,
            items,
            items_view: params.items_view,
            status: turn.status,
            error: turn.error,
            started_at: turn.started_at,
            completed_at: turn.completed_at,
            duration_ms: turn.duration_ms,
        });
    }
    Ok(Some(TurnPage {
        turns,
        next_cursor: indexed_turn_cursor_to_legacy(pool, rollout_id, page.next_cursor.as_deref())
            .await?,
        backwards_cursor: indexed_turn_cursor_to_legacy(
            pool,
            rollout_id,
            page.backwards_cursor.as_deref(),
        )
        .await?,
    }))
}

/// Hydrate indexed items without changing the legacy public items API.
pub(in crate::local) async fn list_segmented_legacy_items(
    store: &LocalThreadStore,
    params: ListItemsParams,
) -> ThreadStoreResult<Option<ItemPage>> {
    validate_page_size(params.page_size)?;
    let _live_writer_guard = store.live_writer_locks.lock(params.thread_id).await;
    let Some((pool, rollout_id)) =
        existing_legacy_projection(store, params.thread_id, LegacyProjectionLookup::Backfill)
            .await?
    else {
        return Ok(None);
    };
    let page = page_indexed_item_rows(pool, rollout_id, &params).await?;
    Ok(Some(ItemPage {
        items: page.rows.into_iter().map(|row| row.item).collect(),
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    }))
}

async fn existing_legacy_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    lookup: LegacyProjectionLookup,
) -> ThreadStoreResult<Option<(&sqlx::SqlitePool, RolloutId)>> {
    let Some(state_db) = store.state_db().await else {
        return Ok(None);
    };
    let Some(metadata) = state_db
        .get_thread(thread_id)
        .await
        .map_err(super::thread_history_error)?
    else {
        return Ok(None);
    };
    if metadata.history_mode != ThreadHistoryMode::Legacy {
        return Ok(None);
    }
    let rollout_id =
        super::super::thread_rollout_resolver::rollout_id_from_path_or_legacy_thread_id(
            metadata.rollout_path.as_path(),
            thread_id,
            ThreadHistoryMode::Legacy,
        )?;

    let session_meta = match codex_rollout::read_session_meta_line(&metadata.rollout_path).await {
        Ok(session_meta) => session_meta.meta,
        Err(_) => return Ok(None),
    };
    if session_meta.id != thread_id
        || session_meta.history_mode != ThreadHistoryMode::Legacy
        || session_meta.segment_id.is_none()
        || session_meta.subagent_history_start_ordinal.is_some()
        || session_meta.history_base.is_some()
    {
        return Ok(None);
    }

    let mut reader = match codex_rollout::open_rollout_line_reader(&metadata.rollout_path).await {
        Ok(reader) => reader,
        Err(_) => return Ok(None),
    };
    let mut saw_session_meta = false;
    while let Some(line) = reader
        .next_line()
        .await
        .map_err(super::thread_history_error)?
    {
        if line.trim().is_empty() {
            continue;
        }
        let line = match codex_rollout::RolloutRecorder::parse_rollout_line_bytes(line.as_bytes()) {
            Ok(Some(line)) => line,
            Ok(None) => continue,
            Err(_) => return Ok(None),
        };
        if !saw_session_meta {
            if !matches!(line.item, RolloutItem::SessionMeta(_)) {
                return Ok(None);
            }
            saw_session_meta = true;
            continue;
        }
        if let RolloutItem::RolloutReference(reference) = line.item
            && (reference.thread_id != Some(thread_id)
                || reference.nth_user_message.is_some()
                || reference
                    .compacted_replacement_history_filter_texts
                    .as_ref()
                    .is_some_and(|filters| !filters.is_empty()))
        {
            return Ok(None);
        }
        break;
    }

    let rollout_len =
        match super::super::live_writer::legacy_rollout_jsonl_len(&metadata.rollout_path).await {
            Ok(len) => len,
            Err(_) => return Ok(None),
        };

    let db_path = store.config.sqlite.thread_history_db_path();
    let db_exists = tokio::fs::try_exists(db_path)
        .await
        .map_err(super::thread_history_error)?;
    if !db_exists && lookup == LegacyProjectionLookup::ExistingOnly {
        return Ok(None);
    }
    if !db_exists
        && let Err(error) = super::super::live_writer::backfill_segmented_legacy_projection(
            store,
            thread_id,
            metadata.rollout_path.as_path(),
        )
        .await
    {
        tracing::warn!("failed to backfill segmented legacy history for {thread_id}: {error}");
        return Ok(None);
    }

    let pool = store.thread_history_db().await?;
    let mut projected_len = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ? LIMIT 1",
    )
    .bind(rollout_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(super::thread_history_error)?;
    let projection_is_current = projected_len
        .and_then(|offset| u64::try_from(offset).ok())
        .is_some_and(|offset| offset == rollout_len);
    if !projection_is_current && lookup == LegacyProjectionLookup::Backfill {
        if let Err(error) = super::super::live_writer::backfill_segmented_legacy_projection(
            store,
            thread_id,
            metadata.rollout_path.as_path(),
        )
        .await
        {
            tracing::warn!("failed to backfill segmented legacy history for {thread_id}: {error}");
            return Ok(None);
        }
        projected_len = sqlx::query_scalar::<_, i64>(
            "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ? LIMIT 1",
        )
        .bind(rollout_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(super::thread_history_error)?;
    }
    Ok(projected_len
        .and_then(|offset| u64::try_from(offset).ok())
        .filter(|offset| *offset == rollout_len)
        .map(|_| (pool, rollout_id)))
}

async fn legacy_turn_cursor_to_index(
    pool: &sqlx::SqlitePool,
    thread_id: RolloutId,
    cursor: &str,
) -> ThreadStoreResult<String> {
    let legacy: LegacyProjectionCursor =
        serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    let ordinal = sqlx::query_scalar::<_, i64>(
        "SELECT rollout_ordinal FROM thread_turns WHERE thread_id = ? AND turn_id = ? LIMIT 1",
    )
    .bind(thread_id.to_string())
    .bind(legacy.turn_id)
    .fetch_optional(pool)
    .await
    .map_err(super::thread_history_error)?
    .ok_or_else(|| invalid_cursor(cursor))?;
    serialize_cursor(
        thread_id,
        thread_id,
        CursorScope::Turns,
        ordinal,
        legacy.include_anchor,
    )
}

async fn indexed_turn_cursor_to_legacy(
    pool: &sqlx::SqlitePool,
    thread_id: ThreadId,
    cursor: Option<&str>,
) -> ThreadStoreResult<Option<String>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let indexed = parse_cursor(Some(cursor), thread_id, thread_id, CursorScope::Turns)?
        .ok_or_else(|| invalid_cursor(cursor))?;
    let ordinal = i64::try_from(indexed.rollout_ordinal).map_err(|_| invalid_cursor(cursor))?;
    let turn_id = sqlx::query_scalar::<_, String>(
        "SELECT turn_id FROM thread_turns WHERE thread_id = ? AND rollout_ordinal = ? LIMIT 1",
    )
    .bind(thread_id.to_string())
    .bind(ordinal)
    .fetch_optional(pool)
    .await
    .map_err(super::thread_history_error)?
    .ok_or_else(|| invalid_cursor(cursor))?;
    serde_json::to_string(&LegacyProjectionCursor {
        turn_id,
        include_anchor: indexed.include_anchor,
    })
    .map(Some)
    .map_err(super::thread_history_error)
}

pub(in crate::local) async fn validate_thread_for_paginated_reads(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    include_archived: bool,
    operation: &'static str,
) -> ThreadStoreResult<()> {
    let Some(state_db) = store.state_db().await else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to read thread metadata: {err}"),
            })?
    else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    if metadata.archived_at.is_some() && !include_archived {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} is archived"),
        });
    }
    match metadata.history_mode {
        ThreadHistoryMode::Legacy => Err(ThreadStoreError::Unsupported { operation }),
        ThreadHistoryMode::Paginated => Ok(()),
    }
}

async fn load_inherited_summary_items(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    turn: &StoredTurnRow,
) -> ThreadStoreResult<Vec<StoredThreadItem>> {
    let source = find_source_turn(pool, lineage, turn.turn_id.as_str()).await?;
    let segment = lineage.segment_for_position(
        source.rollout_id,
        u64::try_from(source.rollout_ordinal)
            .map_err(|_| invalid_cursor("negative rollout ordinal"))?,
    )?;
    let start_ordinal = sqlite_integer(segment.start_ordinal(), "rollout ordinal")?;
    let end_ordinal = segment
        .end_ordinal()
        .map(|ordinal| sqlite_integer(ordinal, "rollout ordinal"))
        .transpose()?;
    let rows = sqlx::query(
        r#"
SELECT turn_id, item_id, updated_at_ordinal, created_at_ms, started_at_ms, completed_at_ms, item_json
FROM thread_items
WHERE thread_id = ?
  AND turn_id = ?
  AND rollout_ordinal >= ?
  AND (? IS NULL OR rollout_ordinal < ?)
  AND (item_id = ? OR item_id = ?)
ORDER BY rollout_ordinal ASC
        "#,
    )
    .bind(source.rollout_id.to_string())
    .bind(turn.turn_id.as_str())
    .bind(start_ordinal)
    .bind(end_ordinal)
    .bind(end_ordinal)
    .bind(source.first_user_item_id)
    .bind(source.final_agent_item_id)
    .fetch_all(pool)
    .await
    .map_err(super::thread_history_error)?;
    let items = rows
        .into_iter()
        .map(stored_thread_item)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    items
        .into_iter()
        .filter_map(|item| match segment.allows_stored_item(&item) {
            Ok(true) => Some(Ok(item)),
            Ok(false) => None,
            Err(err) => Some(Err(err)),
        })
        .collect()
}

/// Preserve legacy summaries, including commentary-only implicit turns.
async fn load_legacy_summary_items(
    pool: &sqlx::SqlitePool,
    thread_id: ThreadId,
    turn: &StoredTurnRow,
) -> ThreadStoreResult<Vec<StoredThreadItem>> {
    let thread_id = thread_id.to_string();
    let rows = sqlx::query(
        r#"
SELECT turn_id, item_id, updated_at_ordinal, created_at_ms, started_at_ms, completed_at_ms, item_json
FROM thread_items
WHERE thread_id = ?
  AND turn_id = ?
  AND (
    item_id = ?
    OR item_id = (
        SELECT item_id
        FROM thread_items
        WHERE thread_id = ?
          AND turn_id = ?
          AND item_type = 'userMessage'
        ORDER BY rollout_ordinal ASC
        LIMIT 1
    )
    OR item_id = (
        SELECT item_id
        FROM thread_items
        WHERE thread_id = ?
          AND turn_id = ?
          AND item_type = 'agentMessage'
        ORDER BY rollout_ordinal DESC
        LIMIT 1
    )
  )
ORDER BY rollout_ordinal ASC
        "#,
    )
    .bind(thread_id.as_str())
    .bind(turn.turn_id.as_str())
    .bind(turn.first_user_item_id.as_deref())
    .bind(thread_id.as_str())
    .bind(turn.turn_id.as_str())
    .bind(thread_id.as_str())
    .bind(turn.turn_id.as_str())
    .fetch_all(pool)
    .await
    .map_err(super::thread_history_error)?;
    rows.into_iter().map(stored_thread_item).collect()
}

pub(super) fn parse_cursor(
    cursor: Option<&str>,
    requested_thread_id: ThreadId,
    root_rollout_id: RolloutId,
    scope: CursorScope,
) -> ThreadStoreResult<Option<HistoryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor_value: HistoryCursor =
        serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    if cursor_value.requested_thread_id != requested_thread_id
        || cursor_value.root_rollout_id != root_rollout_id
        || cursor_value.scope != scope
    {
        return Err(invalid_cursor(cursor));
    }
    Ok(Some(cursor_value))
}

pub(super) fn serialize_cursor(
    requested_thread_id: ThreadId,
    root_rollout_id: RolloutId,
    scope: CursorScope,
    rollout_ordinal: i64,
    include_anchor: bool,
) -> ThreadStoreResult<String> {
    let rollout_ordinal =
        u64::try_from(rollout_ordinal).map_err(|_| invalid_cursor("negative rollout ordinal"))?;
    serde_json::to_string(&HistoryCursor {
        requested_thread_id,
        root_rollout_id,
        rollout_ordinal,
        include_anchor,
        scope,
    })
    .map_err(super::thread_history_error)
}

pub(super) fn stored_turn_row(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredTurnRow> {
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "completed" => StoredTurnStatus::Completed,
        "interrupted" => StoredTurnStatus::Interrupted,
        "failed" => StoredTurnStatus::Failed,
        "inProgress" => StoredTurnStatus::InProgress,
        status => {
            return Err(ThreadStoreError::Internal {
                message: format!("unknown stored turn status: {status}"),
            });
        }
    };
    let error_json = row.try_get::<Option<String>, _>("error_json")?;
    let error = error_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(super::thread_history_error)?;
    Ok(StoredTurnRow {
        position: PhysicalHistoryPosition {
            rollout_ordinal: row.try_get("rollout_ordinal")?,
        },
        turn_id: row.try_get("turn_id")?,
        root_turn_id: row.try_get("root_turn_id")?,
        status,
        error,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        duration_ms: row.try_get("duration_ms")?,
        first_user_item_id: row.try_get("first_user_item_id")?,
        final_agent_item_id: row.try_get("final_agent_item_id")?,
        summary_items: Vec::new(),
    })
}

impl StoredSummaryColumns {
    pub(super) fn into_stored_items(self) -> ThreadStoreResult<Vec<StoredThreadItem>> {
        let mut summary_items = [
            StoredSummaryItemColumns {
                turn_id: self.summary_first_user_turn_id,
                item_id: self.summary_first_user_item_id,
                rollout_ordinal: self.summary_first_user_rollout_ordinal,
                updated_at_ordinal: self.summary_first_user_updated_at_ordinal,
                created_at_ms: self.summary_first_user_created_at_ms,
                started_at_ms: self.summary_first_user_started_at_ms,
                completed_at_ms: self.summary_first_user_completed_at_ms,
                item_json: self.summary_first_user_item_json,
            }
            .into_stored_item()?,
            StoredSummaryItemColumns {
                turn_id: self.summary_final_agent_turn_id,
                item_id: self.summary_final_agent_item_id,
                rollout_ordinal: self.summary_final_agent_rollout_ordinal,
                updated_at_ordinal: self.summary_final_agent_updated_at_ordinal,
                created_at_ms: self.summary_final_agent_created_at_ms,
                started_at_ms: self.summary_final_agent_started_at_ms,
                completed_at_ms: self.summary_final_agent_completed_at_ms,
                item_json: self.summary_final_agent_item_json,
            }
            .into_stored_item()?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        summary_items.sort_by_key(|(rollout_ordinal, _)| *rollout_ordinal);
        Ok(summary_items.into_iter().map(|(_, item)| item).collect())
    }
}

impl StoredSummaryItemColumns {
    fn into_stored_item(self) -> ThreadStoreResult<Option<(i64, StoredThreadItem)>> {
        let Some(item_id) = self.item_id else {
            return Ok(None);
        };
        let (
            Some(turn_id),
            Some(rollout_ordinal),
            Some(updated_at_ordinal),
            Some(created_at_ms),
            Some(item_json),
        ) = (
            self.turn_id,
            self.rollout_ordinal,
            self.updated_at_ordinal,
            self.created_at_ms,
            self.item_json,
        )
        else {
            return Err(ThreadStoreError::Internal {
                message: "stored summary item is missing joined columns".to_string(),
            });
        };
        Ok(Some((
            rollout_ordinal,
            StoredThreadItem {
                turn_id,
                item_id,
                updated_at_ordinal: stored_updated_at_ordinal(updated_at_ordinal)?,
                created_at_ms,
                started_at_ms: self.started_at_ms,
                completed_at_ms: self.completed_at_ms,
                item_json: item_json.into_bytes(),
            },
        )))
    }
}

pub(super) fn stored_thread_item_row(
    row: sqlx::sqlite::SqliteRow,
) -> ThreadStoreResult<StoredThreadItemRow> {
    let rollout_ordinal = row.try_get::<i64, _>("rollout_ordinal")?;
    if rollout_ordinal < 0 {
        return Err(ThreadStoreError::Internal {
            message: format!("invalid stored item rollout ordinal: {rollout_ordinal}"),
        });
    }
    Ok(StoredThreadItemRow {
        position: PhysicalHistoryPosition { rollout_ordinal },
        item: stored_thread_item(row)?,
    })
}

fn stored_thread_item(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredThreadItem> {
    let updated_at_ordinal = stored_updated_at_ordinal(row.try_get("updated_at_ordinal")?)?;
    Ok(StoredThreadItem {
        turn_id: row.try_get("turn_id")?,
        item_id: row.try_get("item_id")?,
        updated_at_ordinal,
        created_at_ms: row.try_get("created_at_ms")?,
        started_at_ms: row.try_get("started_at_ms")?,
        completed_at_ms: row.try_get("completed_at_ms")?,
        item_json: row.try_get::<String, _>("item_json")?.into_bytes(),
    })
}

fn stored_updated_at_ordinal(updated_at_ordinal: i64) -> ThreadStoreResult<u64> {
    u64::try_from(updated_at_ordinal).map_err(|_| ThreadStoreError::Internal {
        message: format!("invalid stored item updated-at ordinal: {updated_at_ordinal}"),
    })
}

pub(super) fn invalid_cursor(cursor: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    }
}
