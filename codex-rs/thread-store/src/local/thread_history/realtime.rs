use std::collections::HashMap;

use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadRealtimeItemContent;
use codex_app_server_protocol::ThreadTimelineEntry;
use codex_protocol::ThreadId;
use codex_protocol::realtime::RealtimeItem;
use serde::Deserialize;
use serde::Serialize;

use super::super::LocalThreadStore;
use super::read::indexed_same_thread_lineage;
use super::read::validate_thread_for_paginated_reads;
use super::segment_paging::validate_page_size;
use super::sqlite_integer;
use super::thread_history_error;
use crate::ListTimelineParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::TimelinePage;
use crate::local::rollout_lineage::RolloutLineageSegment;

#[path = "realtime_canonical.rs"]
mod canonical;

/// Counts physical payload reads independently of SQLite page queries.
#[derive(Default)]
pub(super) struct TimelineReadStats {
    pub(super) canonical_segments: usize,
    pub(super) canonical_bytes: u64,
}

/// Migration emits cumulative Reasoning snapshots under one stable item ID within a turn.
/// Keep the latest snapshot at the first ordinal, including across physical rotations.
struct ReasoningSnapshot {
    /// First accepted ItemCompleted ordinal for this turn and item ID.
    first_ordinal: u64,
    /// Latest cumulative snapshot; summaries must not be concatenated again.
    item: Box<ThreadItem>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TimelineCursor {
    thread_id: ThreadId,
    position: u64,
    kind: u8,
    id: String,
}

// A rollout record can materialize both an item and a turn boundary. Keep the
// boundary order stable, including when the page cuts through one ordinal.
pub(super) fn entry_key(entry: &ThreadTimelineEntry) -> (u64, u8, &str) {
    match entry {
        ThreadTimelineEntry::TurnStarted {
            position, turn_id, ..
        } => (*position, 0, turn_id),
        ThreadTimelineEntry::Item { position, item, .. } => (*position, 1, item.id()),
        ThreadTimelineEntry::Realtime { position, item } => (*position, 2, &item.id),
        ThreadTimelineEntry::TurnCompleted {
            position, turn_id, ..
        } => (*position, 3, turn_id),
    }
}

pub(in crate::local) async fn list_timeline(
    store: &LocalThreadStore,
    params: ListTimelineParams,
) -> ThreadStoreResult<TimelinePage> {
    let (page, stats) = list_timeline_with_read_stats(store, params).await?;
    tracing::debug!(
        canonical_segments = stats.canonical_segments,
        canonical_bytes = stats.canonical_bytes,
        "read thread timeline page"
    );
    Ok(page)
}

pub(super) async fn list_timeline_with_read_stats(
    store: &LocalThreadStore,
    params: ListTimelineParams,
) -> ThreadStoreResult<(TimelinePage, TimelineReadStats)> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        /*include_archived*/ false,
        "thread/timeline/list",
    )
    .await?;
    validate_page_size(params.page_size)?;

    let mut canonical_segments = HashMap::new();
    // A rebuilt or resumed root stores the entire authenticated lineage under its own ID.
    // Applying the selected physical segment's lower bound would hide inherited rows.
    let lineage =
        if let Some(lineage) = indexed_same_thread_lineage(store, params.thread_id).await? {
            canonical_segments.insert(0usize, None);
            lineage
        } else {
            store
                .resolve_rollout_lineage_deferred(params.thread_id)
                .await?
        };
    let mut stats = TimelineReadStats::default();
    let mut reasoning = HashMap::new();
    let pool = store.thread_history_db().await?;
    let cursor = params
        .cursor
        .as_deref()
        .map(serde_json::from_str::<TimelineCursor>)
        .transpose()
        .map_err(|_| ThreadStoreError::InvalidRequest {
            message: "invalid thread timeline cursor".to_string(),
        })?
        .map(|cursor| {
            if cursor.thread_id != params.thread_id {
                Err(ThreadStoreError::InvalidRequest {
                    message: "thread timeline cursor belongs to another thread".to_string(),
                })
            } else {
                Ok(cursor)
            }
        })
        .transpose()?;
    let cursor_position = cursor.as_ref().map(|cursor| cursor.position);
    let cursor_ordinal = cursor_position
        .map(|position| sqlite_integer(position, "rollout ordinal"))
        .transpose()?
        .unwrap_or(i64::MAX);
    let cursor_kind = cursor.as_ref().map_or(4, |cursor| cursor.kind);
    let cursor_id = cursor.as_ref().map_or("", |cursor| cursor.id.as_str());

    let mut rows = Vec::new();
    let max_rows = params.page_size + 1;
    for (segment_index, segment) in lineage.segments().iter().enumerate().rev() {
        let remaining = max_rows.saturating_sub(rows.len());
        if remaining == 0 {
            break;
        }
        if cursor_position.is_some_and(|position| position < segment.start_ordinal()) {
            continue;
        }
        let entries = canonical_entries(
            store,
            lineage.segments(),
            segment_index,
            &mut canonical_segments,
            &mut stats,
            &mut reasoning,
        )
        .await?;
        if let Some(entries) = entries {
            rows.extend(
                entries
                    .iter()
                    .filter(|entry| {
                        entry_key(entry) < (cursor_ordinal as u64, cursor_kind, cursor_id)
                    })
                    .filter_map(|entry| reasoning_entry(entry, &reasoning))
                    .take(remaining),
            );
            continue;
        }
        let upper = segment
            .end_ordinal()
            .map(|value| sqlite_integer(value, "rollout ordinal"))
            .transpose()?
            .unwrap_or(i64::MAX);
        let segment_rows = sqlx::query_as::<_, (i64, i64, String, Option<String>, String)>(
            r#"
WITH starts AS (
SELECT rollout_ordinal, 0 AS kind, turn_id AS id, turn_id,
       json_object('type', 'turnStarted', 'position', rollout_ordinal,
                   'turnId', turn_id, 'startedAt', started_at) AS item_json
FROM thread_turns
WHERE thread_id = ?1 AND rollout_ordinal >= ?2
  AND rollout_ordinal < ?3 AND rollout_ordinal <= ?4
  AND (rollout_ordinal, 0, turn_id) < (?4, ?5, ?6)
ORDER BY rollout_ordinal DESC, turn_id DESC LIMIT ?7
), items AS (
SELECT rollout_ordinal, 1 AS kind, item_id AS id, turn_id, item_json
FROM thread_items
WHERE thread_id = ?1 AND rollout_ordinal >= ?2
  AND rollout_ordinal < ?3 AND rollout_ordinal <= ?4
  AND (rollout_ordinal, 1, item_id) < (?4, ?5, ?6)
ORDER BY rollout_ordinal DESC, item_id DESC LIMIT ?7
), realtime AS (
SELECT rollout_ordinal, 2 AS kind, item_id AS id, NULL AS turn_id, item_json
FROM thread_realtime_items
WHERE thread_id = ?1 AND rollout_ordinal >= ?2
  AND rollout_ordinal < ?3 AND rollout_ordinal <= ?4
  AND (rollout_ordinal, 2, item_id) < (?4, ?5, ?6)
ORDER BY rollout_ordinal DESC, item_id DESC LIMIT ?7
), ends AS (
SELECT rollout_end_ordinal AS rollout_ordinal, 3 AS kind, turn_id AS id, turn_id,
       json_object('type', 'turnCompleted', 'position', rollout_end_ordinal,
                   'turnId', turn_id, 'status', status, 'error', json(error_json),
                   'startedAt', started_at, 'completedAt', completed_at,
                   'durationMs', duration_ms) AS item_json
FROM thread_turns
WHERE thread_id = ?1 AND rollout_end_ordinal >= ?2
  AND rollout_end_ordinal < ?3 AND rollout_end_ordinal <= ?4
  AND (rollout_end_ordinal, 3, turn_id) < (?4, ?5, ?6)
ORDER BY rollout_end_ordinal DESC, turn_id DESC LIMIT ?7
)
SELECT * FROM starts
UNION ALL SELECT * FROM items
UNION ALL SELECT * FROM realtime
UNION ALL SELECT * FROM ends
ORDER BY rollout_ordinal DESC, kind DESC, id DESC
LIMIT ?7
            "#,
        )
        .bind(segment.rollout_id().to_string())
        .bind(sqlite_integer(segment.start_ordinal(), "rollout ordinal")?)
        .bind(upper)
        .bind(cursor_ordinal)
        .bind(i64::from(cursor_kind))
        .bind(cursor_id)
        .bind(i64::try_from(remaining).map_err(thread_history_error)?)
        .fetch_all(pool)
        .await
        .map_err(thread_history_error)?;

        for (position, kind, _id, turn_id, item_json) in segment_rows {
            let position = u64::try_from(position).map_err(thread_history_error)?;
            let entry = match kind {
                0 | 3 => serde_json::from_str::<ThreadTimelineEntry>(&item_json)
                    .map_err(thread_history_error)?,
                1 => ThreadTimelineEntry::Item {
                    position,
                    turn_id: turn_id.ok_or_else(|| thread_history_error("missing turn ID"))?,
                    item: Box::new(
                        serde_json::from_str::<ThreadItem>(&item_json)
                            .map_err(thread_history_error)?,
                    ),
                },
                2 => ThreadTimelineEntry::Realtime {
                    position,
                    item: serde_json::from_str::<RealtimeItem>(&item_json)
                        .map_err(thread_history_error)?
                        .into(),
                },
                _ => return Err(thread_history_error("invalid timeline entry kind")),
            };
            if lineage.segments().len() > 1
                && let ThreadTimelineEntry::Item {
                    position,
                    turn_id,
                    item,
                } = &entry
                && matches!(item.as_ref(), ThreadItem::Reasoning { .. })
            {
                reconcile_reasoning(
                    store,
                    lineage.segments(),
                    segment_index,
                    &mut canonical_segments,
                    &mut stats,
                    &mut reasoning,
                    (*position, turn_id.clone(), item.clone()),
                )
                .await?;
            }
            if let Some(entry) = reasoning_entry(&entry, &reasoning) {
                rows.push(entry);
            }
        }
    }

    let has_more = rows.len() > params.page_size;
    rows.truncate(params.page_size);
    let next_cursor = if has_more {
        rows.last()
            .map(|entry| {
                let (position, kind, id) = entry_key(entry);
                serde_json::to_string(&TimelineCursor {
                    thread_id: params.thread_id,
                    position,
                    kind,
                    id: id.to_string(),
                })
                .map_err(thread_history_error)
            })
            .transpose()?
    } else {
        None
    };

    let (page_start, page_start_kind, page_start_id) =
        rows.last().map(entry_key).unwrap_or_default();
    let mut active_realtime_session_at_page_start = None;
    for (segment_index, segment) in lineage.segments().iter().enumerate().rev() {
        let upper = page_start.min(
            segment
                .end_ordinal()
                .map(|ordinal| ordinal.saturating_sub(1))
                .unwrap_or(u64::MAX),
        );
        if upper < segment.start_ordinal() {
            continue;
        }
        if let Some(entries) = canonical_entries(
            store,
            lineage.segments(),
            segment_index,
            &mut canonical_segments,
            &mut stats,
            &mut reasoning,
        )
        .await?
        {
            let boundary = entries.iter().find_map(|entry| {
                if entry_key(entry) >= (page_start, page_start_kind, page_start_id) {
                    return None;
                }
                let ThreadTimelineEntry::Realtime { item, .. } = entry else {
                    return None;
                };
                match &item.content {
                    ThreadRealtimeItemContent::RealtimeSessionStarted => {
                        Some(Some(item.realtime_session_id.clone()))
                    }
                    ThreadRealtimeItemContent::RealtimeSessionClosed { .. } => Some(None),
                    _ => None,
                }
            });
            if let Some(session_id) = boundary {
                active_realtime_session_at_page_start = session_id;
                break;
            }
            continue;
        }
        let boundary = sqlx::query_as::<_, (String, String)>(
            r#"
SELECT item_type, json_extract(item_json, '$.realtime_session_id')
FROM thread_realtime_items
WHERE thread_id = ?
  AND rollout_ordinal >= ?
  AND rollout_ordinal <= ?
  AND (rollout_ordinal, 2, item_id) < (?, ?, ?)
  AND item_type IN ('realtime_session_started', 'realtime_session_closed')
ORDER BY rollout_ordinal DESC, item_id DESC
LIMIT 1
            "#,
        )
        .bind(segment.rollout_id().to_string())
        .bind(sqlite_integer(segment.start_ordinal(), "rollout ordinal")?)
        .bind(sqlite_integer(upper, "rollout ordinal")?)
        .bind(sqlite_integer(page_start, "rollout ordinal")?)
        .bind(i64::from(page_start_kind))
        .bind(page_start_id)
        .fetch_optional(pool)
        .await
        .map_err(thread_history_error)?;
        if let Some((item_type, session_id)) = boundary {
            if item_type == "realtime_session_started" {
                active_realtime_session_at_page_start = Some(session_id);
            }
            break;
        }
    }

    rows.reverse();
    Ok((
        TimelinePage {
            items: rows,
            next_cursor,
            active_realtime_session_at_page_start,
        },
        stats,
    ))
}

/// Empty SQL results are authoritative only after the physical file was fully projected.
/// Frozen fork snapshots deliberately have new rollout IDs and may have no SQL rows yet.
async fn canonical_entries<'a>(
    store: &LocalThreadStore,
    segments: &[RolloutLineageSegment],
    segment_index: usize,
    cache: &'a mut HashMap<usize, Option<canonical::CanonicalTimeline>>,
    stats: &mut TimelineReadStats,
    reasoning: &mut HashMap<(String, String), ReasoningSnapshot>,
) -> ThreadStoreResult<Option<&'a [ThreadTimelineEntry]>> {
    let segment = &segments[segment_index];
    ensure_segment(store, segment_index, segment, cache, stats).await?;
    let unstarted = cache
        .get(&segment_index)
        .and_then(Option::as_ref)
        .map(|timeline| timeline.unstarted_turns.clone())
        .unwrap_or_default();
    for turn_id in unstarted {
        for (predecessor_index, predecessor) in segments[..segment_index].iter().enumerate().rev() {
            ensure_segment(store, predecessor_index, predecessor, cache, stats).await?;
            let started_at = if let Some(timeline) =
                cache.get(&predecessor_index).and_then(Option::as_ref)
            {
                if timeline.unstarted_turns.contains(&turn_id) {
                    continue;
                }
                timeline.entries.iter().find_map(|entry| match entry {
                    ThreadTimelineEntry::TurnStarted {
                        turn_id: id,
                        started_at,
                        ..
                    } if id == &turn_id => Some(*started_at),
                    _ => None,
                })
            } else {
                let pool = store.thread_history_db().await?;
                sqlx::query_as::<_, (i64, Option<i64>)>(
                    "SELECT rollout_ordinal, started_at FROM thread_turns WHERE thread_id = ? AND turn_id = ? AND rollout_ordinal >= ? AND rollout_ordinal < ?"
                ).bind(predecessor.rollout_id().to_string()).bind(&turn_id)
                    .bind(sqlite_integer(predecessor.start_ordinal(), "rollout ordinal")?)
                    .bind(sqlite_integer(predecessor.end_ordinal().unwrap_or(i64::MAX as u64), "rollout ordinal")?)
                    .fetch_optional(pool).await.map_err(thread_history_error)?
                    .map(|(_, started_at)| started_at)
            };
            if let Some(started_at) = started_at {
                let timeline = cache
                    .get_mut(&segment_index)
                    .and_then(Option::as_mut)
                    .ok_or_else(|| thread_history_error("missing canonical turn state"))?;
                // A local abort completes the inherited turn; it must not introduce a second start.
                timeline.entries.retain(|entry| {
                    !matches!(entry,
                    ThreadTimelineEntry::TurnStarted { turn_id: id, .. } if id == &turn_id)
                });
                for entry in &mut timeline.entries {
                    if let ThreadTimelineEntry::TurnCompleted {
                        turn_id: id,
                        started_at: current,
                        ..
                    } = entry
                        && id == &turn_id
                    {
                        *current = current.or(started_at);
                    }
                }
                timeline.unstarted_turns.retain(|id| id != &turn_id);
                break;
            }
        }
    }
    let snapshots = cache
        .get(&segment_index)
        .and_then(Option::as_ref)
        .map(|timeline| {
            timeline
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    ThreadTimelineEntry::Item {
                        position,
                        turn_id,
                        item,
                    } if matches!(item.as_ref(), ThreadItem::Reasoning { .. }) => {
                        Some((*position, turn_id.clone(), item.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for candidate in snapshots {
        reconcile_reasoning(
            store,
            segments,
            segment_index,
            cache,
            stats,
            reasoning,
            candidate,
        )
        .await?;
    }
    Ok(cache
        .get(&segment_index)
        .and_then(Option::as_ref)
        .map(|timeline| timeline.entries.as_slice()))
}

/// Both canonical and indexed first-position candidates need newer cumulative snapshots.
async fn reconcile_reasoning(
    store: &LocalThreadStore,
    segments: &[RolloutLineageSegment],
    segment_index: usize,
    cache: &mut HashMap<usize, Option<canonical::CanonicalTimeline>>,
    stats: &mut TimelineReadStats,
    reasoning: &mut HashMap<(String, String), ReasoningSnapshot>,
    candidate: (u64, String, Box<ThreadItem>),
) -> ThreadStoreResult<()> {
    let (mut first_ordinal, turn_id, mut item) = candidate;
    let key = (turn_id.clone(), item.id().to_string());
    if reasoning.contains_key(&key) {
        return Ok(());
    }
    // A complete SQL candidate already stores its first position across the entire prefix.
    let current_has_start = cache
        .get(&segment_index)
        .and_then(Option::as_ref)
        .is_none_or(|timeline| {
            !timeline.unstarted_turns.contains(&turn_id)
                && timeline.entries.iter().any(|entry| {
                    matches!(entry,
                ThreadTimelineEntry::TurnStarted { turn_id: id, .. } if id == &turn_id)
                })
        });
    if !current_has_start {
        for (predecessor_index, predecessor) in segments[..segment_index].iter().enumerate().rev() {
            ensure_segment(store, predecessor_index, predecessor, cache, stats).await?;
            if let Some(timeline) = cache.get(&predecessor_index).and_then(Option::as_ref) {
                if let Some(position) = timeline.entries.iter().find_map(|entry| match entry {
                    ThreadTimelineEntry::Item {
                        position,
                        turn_id: id,
                        item: old,
                    } if id == &turn_id && old.id() == item.id() => Some(*position),
                    _ => None,
                }) {
                    first_ordinal = first_ordinal.min(position);
                }
                // Migration calls ensure_turn before reasoning and clears it at boundaries.
                // A matching genuine start proves that this ID cannot have an earlier owner.
                if !timeline.unstarted_turns.contains(&turn_id)
                    && timeline.entries.iter().any(|entry| {
                        matches!(entry,
                        ThreadTimelineEntry::TurnStarted { turn_id: id, .. } if id == &turn_id)
                    })
                {
                    break;
                }
            } else {
                let pool = store.thread_history_db().await?;
                if let Some(position) = sqlx::query_scalar::<_, i64>(
                        "SELECT rollout_ordinal FROM thread_items WHERE thread_id = ? AND turn_id = ? AND item_id = ? AND rollout_ordinal >= ? AND rollout_ordinal < ?"
                    ).bind(predecessor.rollout_id().to_string()).bind(&turn_id).bind(item.id())
                        .bind(sqlite_integer(predecessor.start_ordinal(), "rollout ordinal")?)
                        .bind(sqlite_integer(predecessor.end_ordinal().unwrap_or(i64::MAX as u64), "rollout ordinal")?)
                        .fetch_optional(pool).await.map_err(thread_history_error)? {
                        first_ordinal = first_ordinal.min(u64::try_from(position).map_err(thread_history_error)?);
                        break;
                    }
            }
        }
    }
    let current_has_end = if let Some(timeline) = cache.get(&segment_index).and_then(Option::as_ref)
    {
        timeline.entries.iter().any(|entry| {
            matches!(entry,
                ThreadTimelineEntry::TurnCompleted { turn_id: id, .. } if id == &turn_id)
        })
    } else {
        let current = &segments[segment_index];
        sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM thread_turns WHERE thread_id = ? AND turn_id = ? AND rollout_end_ordinal >= ? AND rollout_end_ordinal < ?"
            ).bind(current.rollout_id().to_string()).bind(&turn_id)
                .bind(sqlite_integer(current.start_ordinal(), "rollout ordinal")?)
                .bind(sqlite_integer(current.end_ordinal().unwrap_or(i64::MAX as u64), "rollout ordinal")?)
                .fetch_optional(store.thread_history_db().await?).await.map_err(thread_history_error)?.is_some()
    };
    if !current_has_end {
        // On an older cursor page, inspect newer records only for this reasoning candidate.
        // Migration clears reasoning at a terminal boundary, so later turns cannot update it.
        for (newer_index, newer) in segments.iter().enumerate().skip(segment_index + 1) {
            ensure_segment(store, newer_index, newer, cache, stats).await?;
            if let Some(timeline) = cache.get(&newer_index).and_then(Option::as_ref) {
                if let Some(updated) = timeline.entries.iter().find_map(|entry| match entry {
                    ThreadTimelineEntry::Item {
                        turn_id: id,
                        item: updated,
                        ..
                    } if id == &turn_id && updated.id() == item.id() => Some(updated.clone()),
                    _ => None,
                }) {
                    item = updated;
                }
                if timeline.entries.iter().any(|entry| {
                    matches!(entry,
                        ThreadTimelineEntry::TurnCompleted { turn_id: id, .. } if id == &turn_id)
                }) {
                    break;
                }
            } else {
                let pool = store.thread_history_db().await?;
                let start = sqlite_integer(newer.start_ordinal(), "rollout ordinal")?;
                let end = sqlite_integer(
                    newer.end_ordinal().unwrap_or(i64::MAX as u64),
                    "rollout ordinal",
                )?;
                if let Some(json) = sqlx::query_scalar::<_, String>(
                        "SELECT item_json FROM thread_items WHERE thread_id = ? AND turn_id = ? AND item_id = ? AND rollout_ordinal = ? AND updated_at_ordinal >= ? AND updated_at_ordinal < ?"
                    ).bind(newer.rollout_id().to_string()).bind(&turn_id).bind(item.id())
                        .bind(sqlite_integer(first_ordinal, "rollout ordinal")?).bind(start).bind(end)
                        .fetch_optional(pool).await.map_err(thread_history_error)? {
                        item = Box::new(serde_json::from_str(&json).map_err(thread_history_error)?);
                    }
                if sqlx::query_scalar::<_, i64>(
                        "SELECT 1 FROM thread_turns WHERE thread_id = ? AND turn_id = ? AND rollout_end_ordinal >= ? AND rollout_end_ordinal < ?"
                    ).bind(newer.rollout_id().to_string()).bind(&turn_id).bind(start).bind(end)
                        .fetch_optional(pool).await.map_err(thread_history_error)?.is_some() {
                        break;
                    }
            }
        }
    }
    reasoning.insert(
        key,
        ReasoningSnapshot {
            first_ordinal,
            item,
        },
    );
    Ok(())
}

fn reasoning_entry(
    entry: &ThreadTimelineEntry,
    reasoning: &HashMap<(String, String), ReasoningSnapshot>,
) -> Option<ThreadTimelineEntry> {
    if let ThreadTimelineEntry::Item {
        position,
        turn_id,
        item,
    } = entry
        && let Some(snapshot) = reasoning.get(&(turn_id.clone(), item.id().to_string()))
    {
        return (*position == snapshot.first_ordinal).then(|| ThreadTimelineEntry::Item {
            position: *position,
            turn_id: turn_id.clone(),
            item: snapshot.item.clone(),
        });
    }
    Some(entry.clone())
}

async fn ensure_segment(
    store: &LocalThreadStore,
    segment_index: usize,
    segment: &RolloutLineageSegment,
    cache: &mut HashMap<usize, Option<canonical::CanonicalTimeline>>,
    stats: &mut TimelineReadStats,
) -> ThreadStoreResult<()> {
    let rollout_id = segment.rollout_id();
    if let std::collections::hash_map::Entry::Vacant(e) = cache.entry(segment_index) {
        let projection = super::projection_state(store, rollout_id).await?;
        let file_len = tokio::fs::metadata(segment.rollout_path())
            .await
            .map_err(thread_history_error)?
            .len();
        let fully_projected = projection.is_some_and(|projection| {
            projection.lineage_complete
                && projection.next_byte_offset == file_len
                && segment
                    .end_ordinal()
                    .is_none_or(|end| end == projection.next_ordinal)
                && segment
                    .jsonl_end_byte_offset()
                    .is_none_or(|end| end == projection.next_byte_offset)
        });
        let canonical = if fully_projected {
            None
        } else {
            let timeline = canonical::read_segment(segment).await?;
            stats.canonical_segments += 1;
            stats.canonical_bytes += timeline.bytes_read;
            Some(timeline)
        };
        e.insert(canonical);
    }
    Ok(())
}
