//! Source-backed turn-item reads before a complete lineage certificate exists.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::io::BufRead;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_app_server_protocol::ThreadItem;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use futures::StreamExt;
use serde::Deserialize;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde_json::value::RawValue;

use super::super::LocalThreadStore;
use super::super::rollout_lineage::HistoryPageWalk;
use super::super::rollout_lineage::RolloutLineage;
use super::super::rollout_lineage::RolloutLineageSegment;
use super::super::rollout_lineage::WalkStep;
use super::super::rollout_lineage::read_rollout_head;
use super::projection_certificate::FileIdentity;
use super::projection_certificate::leaf_identity;
use super::read::CursorScope;
use super::read::parse_cursor;
use super::read::stored_thread_item_row;
use super::segment_paging::page_item_rows_with_connection;
use super::thread_history_error;
use crate::ItemPage;
use crate::ItemSortKey;
use crate::ListItemsParams;
use crate::ListItemsPosition;
use crate::StoredThreadItem;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

const MAX_CACHED_THREADS: usize = 8;
const MAX_ANCESTOR_TURNS: usize = 200_000;
const MAX_ANCESTOR_TURN_BYTES: usize = 8 * 1024 * 1024;
const MAX_ANCHOR_BYTES: usize = 32 * 1024 * 1024;
const MAX_ROOT_ITEMS: usize = 20_000;
const MAX_SUMMARY_SOURCE_BYTES: usize = 64 * 1024 * 1024;
const SCAN_CONCURRENCY: usize = 8;

/// Whether two item snapshots are equivalent, contradict, or differ in an unknown representation.
pub(super) enum ItemComparison {
    Equivalent,
    Contradict,
    Uncertain,
}

struct UniqueFields(BTreeMap<String, Box<RawValue>>);

impl<'de> Deserialize<'de> for UniqueFields {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldsVisitor;
        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = UniqueFields;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an item object with unique keys")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                    if fields.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate item field"));
                    }
                }
                Ok(UniqueFields(fields))
            }
        }
        deserializer.deserialize_map(FieldsVisitor)
    }
}

fn conflicting_values(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            a.iter().any(|(key, value)| {
                b.get(key)
                    .is_some_and(|other| conflicting_values(value, other))
            })
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            a.len() != b.len() || a.iter().zip(b).any(|(a, b)| conflicting_values(a, b))
        }
        (a, b) => a != b,
    }
}

/// Only the two historical optional fields equate absent with null; other ambiguity uses source.
pub(super) fn compare_item_json(left: &[u8], right: &[u8]) -> ItemComparison {
    if left == right {
        return ItemComparison::Equivalent;
    }
    let (Ok(UniqueFields(mut left)), Ok(UniqueFields(mut right))) = (
        serde_json::from_slice::<UniqueFields>(left),
        serde_json::from_slice::<UniqueFields>(right),
    ) else {
        return ItemComparison::Uncertain;
    };
    let item_type = |fields: &BTreeMap<String, Box<RawValue>>| {
        fields
            .get("type")
            .and_then(|value| serde_json::from_str::<String>(value.get()).ok())
    };
    let left_type = item_type(&left);
    let right_type = item_type(&right);
    let optional = match (left_type.as_deref(), right_type.as_deref()) {
        (Some("mcpToolCall"), Some("mcpToolCall")) => Some("environmentId"),
        (Some("agentMessage"), Some("agentMessage")) => Some("moderation"),
        _ => None,
    };
    if let Some(field) = optional {
        let a = left.get(field);
        let b = right.get(field);
        if matches!((a, b), (None, Some(value)) | (Some(value), None) if value.get() == "null") {
            left.remove(field);
            right.remove(field);
            if left.len() == right.len()
                && left.iter().all(|(key, value)| {
                    right
                        .get(key)
                        .is_some_and(|other| value.get() == other.get())
                })
            {
                return ItemComparison::Equivalent;
            }
        }
    }
    let contradictory = left.iter().any(|(key, a)| {
        right.get(key).is_some_and(|b| {
            match (
                serde_json::from_str::<serde_json::Value>(a.get()),
                serde_json::from_str::<serde_json::Value>(b.get()),
            ) {
                (Ok(a), Ok(b)) => conflicting_values(&a, &b),
                _ => false,
            }
        })
    });
    if contradictory {
        ItemComparison::Contradict
    } else {
        ItemComparison::Uncertain
    }
}

// The app-server dispatches this future across executor threads.
#[allow(dead_code)]
fn assert_request_future_is_send(store: &LocalThreadStore, params: &ListItemsParams) {
    fn require_send<T: Send>(_: T) {}
    require_send(try_list_uncertified_turn_items(store, params));
}

/// A complete set of turn IDs in the selected immutable predecessor prefixes.
pub(in crate::local) struct UncertifiedItemScan {
    root: RolloutLineageSegment,
    ancestors: Vec<RolloutLineageSegment>,
    identities: Vec<FileIdentity>,
    turn_ids: HashSet<String>,
    pub(super) turn_segments: HashMap<String, Vec<usize>>,
    pub(super) turn_events: Vec<SourceTurnEvent>,
    pub(super) anchors: Vec<SourceItemAnchor>,
}

pub(super) struct DiscoveredLineage {
    pub(super) lineage: RolloutLineage,
    pub(super) identities: Vec<FileIdentity>,
}

fn supported_owner_order(graph: &DiscoveredLineage) -> bool {
    let mut seen = HashSet::new();
    let mut previous = None;
    for segment in graph.lineage.segments() {
        let owner = segment.thread_id();
        if previous != Some(owner) && !seen.insert(owner) {
            return false;
        }
        previous = Some(owner);
    }
    true
}

/// Shares one validated predecessor index across the requests for a logical thread.
pub(super) async fn indexed_sources(
    store: &LocalThreadStore,
    id: ThreadId,
) -> ThreadStoreResult<Option<(DiscoveredLineage, Arc<UncertifiedItemScan>)>> {
    let mut walk = HistoryPageWalk::new(store, id).await?;
    if !matches!(walk.next_older_deferred().await?, WalkStep::Segment) || walk.complete() {
        return Ok(None);
    }
    let slot = {
        let mut scans = store.uncertified_item_scans.lock().await;
        if !scans.contains_key(&id) && scans.len() >= MAX_CACHED_THREADS {
            let idle = scans
                .iter()
                .find_map(|(id, scan)| (Arc::strong_count(scan) == 1).then_some(*id));
            if let Some(id) = idle {
                scans.remove(&id);
            } else {
                drop(scans);
                return index_uncached_sources(store, id).await;
            }
        }
        scans
            .entry(id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone()
    };
    let mut cached = slot.lock().await;
    if let Some(scan) = cached.as_ref()
        && let Some(graph) = discover_cached(store, id, scan).await?
    {
        if !supported_owner_order(&graph) {
            return Ok(None);
        }
        return Ok(Some((graph, scan.clone())));
    }
    let Some(graph) = discover(store, id).await? else {
        return Err(ThreadStoreError::Conflict {
            message: "could not resolve rotated history sources".to_string(),
        });
    };
    let Some(scan) = scan_ancestors(&graph).await? else {
        return Err(ThreadStoreError::Conflict {
            message: "could not verify rotated history sources".to_string(),
        });
    };
    let scan = Arc::new(scan);
    let Some(current) = discover_cached(store, id, &scan).await? else {
        return Err(ThreadStoreError::Conflict {
            message: "rotated history sources changed while indexing".to_string(),
        });
    };
    if !supported_owner_order(&current) {
        return Ok(None);
    }
    *cached = Some(scan.clone());
    Ok(Some((current, scan)))
}

async fn index_uncached_sources(
    store: &LocalThreadStore,
    id: ThreadId,
) -> ThreadStoreResult<Option<(DiscoveredLineage, Arc<UncertifiedItemScan>)>> {
    let Some(graph) = discover(store, id).await? else {
        return Err(ThreadStoreError::Conflict {
            message: "could not resolve rotated history sources".to_string(),
        });
    };
    let Some(scan) = scan_ancestors(&graph).await? else {
        return Err(ThreadStoreError::Conflict {
            message: "could not verify rotated history sources".to_string(),
        });
    };
    let scan = Arc::new(scan);
    let Some(current) = discover_cached(store, id, &scan).await? else {
        return Err(ThreadStoreError::Conflict {
            message: "rotated history sources changed while indexing".to_string(),
        });
    };
    if !supported_owner_order(&current) {
        return Ok(None);
    }
    Ok(Some((current, scan)))
}

/// Rechecks selected filenames, cutoffs and physical identities after a page is assembled.
pub(super) async fn sources_still_current(
    store: &LocalThreadStore,
    id: ThreadId,
    graph: &DiscoveredLineage,
    scan: &UncertifiedItemScan,
) -> ThreadStoreResult<bool> {
    Ok(discover_cached(store, id, scan)
        .await?
        .is_some_and(|current| {
            current.lineage.segments() == graph.lineage.segments()
                && current.identities == graph.identities
        }))
}

/// Reads the mutable root once for turn metadata, item anchors, and requested snapshots.
pub(super) async fn scan_root_index(
    graph: &DiscoveredLineage,
    turns: Option<HashSet<String>>,
) -> ThreadStoreResult<Option<SourceScan>> {
    let root = graph
        .lineage
        .segments()
        .last()
        .expect("a discovered lineage has a root")
        .clone();
    let cancelled = ScanCancellation::default();
    let stop = cancelled.0.clone();
    match tokio::task::spawn_blocking(move || {
        scan_source(&root, Some(""), true, turns.as_ref(), true, &stop)
    })
    .await
    .map_err(thread_history_error)?
    {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == io::ErrorKind::OutOfMemory => {
            Err(ThreadStoreError::Internal {
                message: error.to_string(),
            })
        }
        Err(_) => Ok(None),
    }
}

/// Reads only predecessor segments that contain items for the requested turns.
pub(super) async fn scan_selected_items(
    graph: &DiscoveredLineage,
    scan: &UncertifiedItemScan,
    turns: HashSet<String>,
) -> ThreadStoreResult<Option<Vec<SourceTurnEvent>>> {
    let mut indexes = HashSet::new();
    for turn in &turns {
        if let Some(segments) = scan.turn_segments.get(turn) {
            indexes.extend(segments.iter().copied());
        }
    }
    indexes.insert(graph.lineage.segments().len() - 1);
    let cancelled = ScanCancellation::default();
    let stop = cancelled.0.clone();
    let turns = Arc::new(turns);
    let mut jobs = futures::stream::iter(indexes)
        .map(|index| {
            let segment = graph.lineage.segments()[index].clone();
            let turns = turns.clone();
            let stop = stop.clone();
            async move {
                (
                    index,
                    tokio::task::spawn_blocking(move || {
                        scan_source(&segment, Some(""), false, Some(&turns), false, &stop)
                    })
                    .await,
                )
            }
        })
        .buffer_unordered(SCAN_CONCURRENCY);
    let mut results = Vec::new();
    let mut bytes = 0usize;
    while let Some((index, result)) = jobs.next().await {
        let source = match result.map_err(thread_history_error)? {
            Ok(Some(source)) => source,
            Err(error) if error.kind() == io::ErrorKind::OutOfMemory => {
                return Err(ThreadStoreError::Internal {
                    message: error.to_string(),
                });
            }
            _ => return Ok(None),
        };
        bytes = bytes.saturating_add(source.summary_bytes);
        check_summary_source_size(bytes).map_err(thread_history_error)?;
        results.push((index, source.turns));
    }
    results.sort_by_key(|(index, _)| *index);
    Ok(Some(
        results.into_iter().flat_map(|(_, events)| events).collect(),
    ))
}

/// Returns a page only if the canonical sources prove the target has no ancestor items.
pub(in crate::local) async fn try_list_uncertified_turn_items(
    store: &LocalThreadStore,
    params: &ListItemsParams,
) -> ThreadStoreResult<Option<ItemPage>> {
    if matches!(
        params.position.as_ref(),
        Some(ListItemsPosition::ItemAnchor { .. })
    ) || params.item_ids.is_some()
        || params.sort_key != ItemSortKey::CreatedAtOrdinal
        || params.after_updated_at_ordinal.is_some()
        || params.before_updated_at_ordinal.is_some()
        || params.page_size == 0
        || params.page_size >= MAX_ROOT_ITEMS
    {
        return Ok(None);
    }
    super::read::validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_items",
    )
    .await?;
    match super::read::source_backed_items(store, params).await {
        Ok(Some(page)) => return Ok(Some(page)),
        Ok(None) | Err(ThreadStoreError::InvalidRequest { .. }) => {}
        Err(error) => return Err(error),
    }
    let Some(turn_id) = params.turn_id.as_deref().filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    // This lookup only avoids scanning for an ineligible request. It is not an absence proof.
    let mut selected = HistoryPageWalk::new(store, params.thread_id).await?;
    if selected.root_path().extension().and_then(|s| s.to_str()) != Some("jsonl") {
        return Ok(None);
    }
    let pool = store.thread_history_db().await?;
    let has_row = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(SELECT 1 FROM thread_items WHERE thread_id = ? AND turn_id = ?)",
    )
    .bind(selected.root_rollout_id().to_string())
    .bind(turn_id)
    .fetch_one(pool)
    .await
    .map_err(thread_history_error)?;
    if has_row == 0 {
        return Ok(None);
    }
    if !matches!(selected.next_older_deferred().await?, WalkStep::Segment) {
        return Ok(None);
    }
    let initial_root = selected.current().clone();
    if let Some(ListItemsPosition::Cursor(cursor)) = params.position.as_ref() {
        let Ok(Some(cursor)) = parse_cursor(
            Some(cursor),
            params.thread_id,
            selected.root_rollout_id(),
            CursorScope::ItemsByCreatedAtOrdinal,
        ) else {
            return Ok(None);
        };
        if cursor.rollout_ordinal < initial_root.start_ordinal() {
            return Ok(None);
        }
    }
    let initial_identity = selected.deferred_identities()[0].clone();
    let cancelled = ScanCancellation::default();
    let initial_items = scan_root(initial_root.clone(), turn_id.to_owned(), &cancelled).await?;
    let Some(initial_items) = initial_items.filter(|scan| !scan.items.is_empty()) else {
        return Ok(None);
    };
    let slot = {
        let mut scans = store.uncertified_item_scans.lock().await;
        if !scans.contains_key(&params.thread_id) && scans.len() >= MAX_CACHED_THREADS {
            let idle = scans
                .iter()
                .find_map(|(id, scan)| (Arc::strong_count(scan) == 1).then_some(*id));
            if let Some(id) = idle {
                scans.remove(&id);
            } else {
                return Ok(None);
            }
        }
        scans
            .entry(params.thread_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone()
    };
    let (graph, cached) = {
        // This lock coordinates only source scans; it never holds a rollout or SQLite writer lock.
        let mut cached = slot.lock().await;
        let current = if let Some(scan) = cached.as_ref() {
            discover_cached(store, params.thread_id, scan).await?
        } else {
            None
        };
        if let Some(graph) = current {
            (graph, cached.clone())
        } else {
            let Some(graph) = discover(store, params.thread_id).await? else {
                return Ok(None);
            };
            *cached = scan_ancestors(&graph).await?.map(Arc::new);
            let Some(scan) = cached.as_ref() else {
                return Ok(None);
            };
            // A concurrent append can change the mutable root while predecessors are scanned.
            let Some(current) = discover_cached(store, params.thread_id, scan).await? else {
                return Ok(None);
            };
            (current, cached.clone())
        }
    };
    let Some(cached) = cached else {
        return Ok(None);
    };
    let root = graph
        .lineage
        .segments()
        .last()
        .expect("discover requires a root");
    // The active checkpoint is in decoded bytes, so a compressed root cannot use this path.
    if root.rollout_path().extension().and_then(|s| s.to_str()) != Some("jsonl") {
        return Ok(None);
    }
    if cached.turn_ids.contains(turn_id) {
        return Ok(None);
    }
    let root_items = if root == &initial_root && graph.identities.last() == Some(&initial_identity)
    {
        initial_items
    } else {
        let Some(current) = scan_root(root.clone(), turn_id.to_owned(), &cancelled).await? else {
            return Ok(None);
        };
        current
    };
    let mut transaction = pool.begin().await.map_err(thread_history_error)?;
    let checkpoint = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(graph.lineage.root_rollout_id().to_string())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(thread_history_error)?;
    if checkpoint.is_none_or(|offset| {
        offset < 0 || offset == i64::MAX || Some(offset as u64) != root.physical_size
    }) {
        return Ok(None);
    }
    let rows = sqlx::query(
        "SELECT turn_id, item_id, rollout_ordinal, updated_at_ordinal, created_at_ms, \
         started_at_ms, completed_at_ms, item_json FROM thread_items \
         WHERE thread_id = ? AND turn_id = ? ORDER BY rollout_ordinal LIMIT ?",
    )
    .bind(graph.lineage.root_rollout_id().to_string())
    .bind(turn_id)
    .bind((MAX_ROOT_ITEMS + 1) as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(thread_history_error)?;
    let rows = rows
        .into_iter()
        .map(stored_thread_item_row)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    if rows.len() != root_items.items.len()
        || rows.iter().zip(&root_items.items).any(|(row, expected)| {
            row.position.rollout_ordinal != expected.0 || row.item != expected.1
        })
    {
        return Ok(None);
    }
    let root_lineage = RolloutLineage::from_segments(
        params.thread_id,
        graph.lineage.root_rollout_id(),
        graph.lineage.leaf_path().to_path_buf(),
        vec![root.clone()],
    );
    let page = page_item_rows_with_connection(&mut transaction, &root_lineage, params).await?;
    let Some(current) = discover_cached(store, params.thread_id, &cached).await? else {
        return Ok(None);
    };
    if current.lineage.segments() != graph.lineage.segments()
        || current.identities != graph.identities
        || !identities_unchanged(&current).await
    {
        return Ok(None);
    }
    transaction.commit().await.map_err(thread_history_error)?;
    Ok(Some(ItemPage {
        items: page.rows.into_iter().map(|row| row.item).collect(),
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    }))
}

async fn scan_root(
    root: RolloutLineageSegment,
    turn: String,
    cancelled: &ScanCancellation,
) -> ThreadStoreResult<Option<SourceScan>> {
    let stop = cancelled.0.clone();
    Ok(tokio::task::spawn_blocking(move || {
        scan_source(&root, Some(&turn), false, None, false, &stop)
    })
    .await
    .map_err(thread_history_error)?
    .ok()
    .flatten())
}

pub(super) async fn discover(
    store: &LocalThreadStore,
    id: ThreadId,
) -> ThreadStoreResult<Option<DiscoveredLineage>> {
    let mut walk = HistoryPageWalk::new(store, id).await?;
    loop {
        match walk.next_older_deferred().await? {
            WalkStep::Segment => {}
            WalkStep::End => break,
            WalkStep::FullResolution => return Ok(None),
        }
    }
    let mut identities = walk.deferred_identities().to_vec();
    identities.reverse();
    let lineage = walk.into_lineage();
    if lineage.segments().is_empty() || lineage.segments().len() != identities.len() {
        return Ok(None);
    }
    Ok(Some(DiscoveredLineage {
        lineage,
        identities,
    }))
}

/// Rechecks the selected root, filename index, and every cached physical identity.
pub(super) async fn discover_cached(
    store: &LocalThreadStore,
    id: ThreadId,
    cached: &UncertifiedItemScan,
) -> ThreadStoreResult<Option<DiscoveredLineage>> {
    let walk = HistoryPageWalk::new(store, id).await?;
    if walk.root_rollout_id() != cached.root.rollout_id()
        || walk.root_path() != cached.root.rollout_path()
        || !walk.selected_is_current().await?
    {
        return Ok(None);
    }
    let Some(identity) = leaf_identity(walk.root_path()).await else {
        return Ok(None);
    };
    let Ok(head) = read_rollout_head(walk.root_path()).await else {
        return Ok(None);
    };
    let meta = &head.session_meta.meta;
    let expected_base = match cached.ancestors.last() {
        Some(segment) => {
            let Some((ordinal, offset)) =
                segment.end_ordinal().zip(segment.jsonl_end_byte_offset())
            else {
                return Ok(None);
            };
            Some(HistoryPosition {
                thread_id: segment.rollout_id(),
                end_ordinal_exclusive: ordinal,
                end_byte_offset: offset,
            })
        }
        None => None,
    };
    if meta.id != id
        || meta.history_mode != ThreadHistoryMode::Paginated
        || meta.attached_thread
        || meta.subagent_history_start_ordinal.is_some()
        || meta.history_base != expected_base
        || head.leading_reference.is_some()
        || head.first_local_ordinal != cached.root.start_ordinal()
        || leaf_identity(walk.root_path()).await.as_ref() != Some(&identity)
    {
        return Ok(None);
    }
    let Ok(paths) =
        codex_rollout::index_rollout_paths_by_rollout_id(&store.config.codex_home).await
    else {
        return Ok(None);
    };
    if cached.ancestors.iter().any(|segment| {
        paths
            .get(&segment.rollout_id())
            .is_none_or(|path| path != segment.rollout_path())
    }) {
        return Ok(None);
    }
    let Ok(metadata) = tokio::fs::metadata(walk.root_path()).await else {
        return Ok(None);
    };
    let mut root = cached.root.clone();
    root.physical_size = Some(metadata.len());
    root.end_byte_offset = Some(metadata.len());
    root.jsonl_end_byte_offset = Some(metadata.len());
    let mut segments = cached.ancestors.clone();
    segments.push(root);
    let mut identities = cached.identities.clone();
    identities.push(identity);
    let graph = DiscoveredLineage {
        lineage: RolloutLineage::from_segments(
            id,
            walk.root_rollout_id(),
            walk.root_path().to_path_buf(),
            segments,
        ),
        identities,
    };
    if !identities_unchanged(&graph).await {
        return Ok(None);
    }
    Ok(Some(graph))
}

async fn identities_unchanged(graph: &DiscoveredLineage) -> bool {
    let sources = graph
        .lineage
        .segments()
        .iter()
        .zip(&graph.identities)
        .map(|(segment, identity)| (segment.rollout_path().to_path_buf(), identity.clone()))
        .collect::<Vec<_>>();
    futures::stream::iter(sources)
        .map(|(path, expected)| async move {
            leaf_identity(&path).await.as_ref() == Some(&expected)
        })
        .buffer_unordered(16)
        .all(|same| async move { same })
        .await
}

async fn ancestor_identities_unchanged(graph: &DiscoveredLineage) -> bool {
    let count = graph.lineage.segments().len().saturating_sub(1);
    let sources = graph.lineage.segments()[..count]
        .iter()
        .zip(&graph.identities[..count])
        .map(|(segment, identity)| (segment.rollout_path().to_path_buf(), identity.clone()))
        .collect::<Vec<_>>();
    futures::stream::iter(sources)
    .map(|(path, expected)| async move {
        leaf_identity(&path).await.as_ref() == Some(&expected)
    })
    .buffer_unordered(16)
    .all(|same| async move { same })
    .await
}

pub(super) async fn scan_ancestors(
    graph: &DiscoveredLineage,
) -> ThreadStoreResult<Option<UncertifiedItemScan>> {
    let segments = graph.lineage.segments();
    let root = segments.last().expect("discover requires a root").clone();
    let cancelled = ScanCancellation::default();
    let stop = cancelled.0.clone();
    let ancestors = segments[..segments.len() - 1].to_vec();
    let mut scans = futures::stream::iter(ancestors)
        .enumerate()
        .map(move |(index, segment)| {
            let stop = stop.clone();
            async move {
                (
                    index,
                    tokio::task::spawn_blocking(move || {
                        scan_source(&segment, None, true, None, true, &stop)
                    })
                    .await,
                )
            }
        })
        .buffer_unordered(SCAN_CONCURRENCY);
    let mut turn_ids = HashSet::new();
    let mut turn_segments: HashMap<String, Vec<usize>> = HashMap::new();
    let mut results = Vec::new();
    let mut anchor_bytes = 0usize;
    let mut anchor_count = 0usize;
    let mut turn_bytes = 0usize;
    let mut event_count = 0usize;
    while let Some((index, result)) = scans.next().await {
        let scan = match result.map_err(thread_history_error)? {
            Ok(Some(scan)) => scan,
            Err(error) if error.kind() == io::ErrorKind::OutOfMemory => {
                return Err(ThreadStoreError::Internal {
                    message: error.to_string(),
                });
            }
            _ => return Ok(None),
        };
        for turn in &scan.turn_ids {
            if turn_ids.insert(turn.clone()) {
                turn_bytes = turn_bytes.saturating_add(turn.len());
            }
            turn_segments.entry(turn.clone()).or_default().push(index);
        }
        anchor_bytes = anchor_bytes.saturating_add(scan.anchor_bytes);
        anchor_count = anchor_count.saturating_add(scan.anchors.len());
        event_count = event_count.saturating_add(scan.turns.len());
        if turn_ids.len() > MAX_ANCESTOR_TURNS
            || turn_bytes > MAX_ANCESTOR_TURN_BYTES
            || anchor_bytes > MAX_ANCHOR_BYTES
            || anchor_count > MAX_ANCESTOR_TURNS
            || event_count > MAX_ANCESTOR_TURNS
        {
            return Err(ThreadStoreError::Internal {
                message: "rotated history index exceeds its source verification limit".to_string(),
            });
        }
        results.push((index, scan));
    }
    if !ancestor_identities_unchanged(graph).await {
        return Ok(None);
    }
    results.sort_by_key(|(index, _)| *index);
    let mut turn_events = Vec::new();
    let mut anchors = Vec::new();
    for (_, scan) in results {
        turn_events.extend(scan.turns);
        anchors.extend(scan.anchors);
    }
    Ok(Some(UncertifiedItemScan {
        root,
        ancestors: segments[..segments.len() - 1].to_vec(),
        identities: graph.identities[..graph.identities.len() - 1].to_vec(),
        turn_ids,
        turn_segments,
        turn_events,
        anchors,
    }))
}

#[derive(Default)]
struct ScanCancellation(Arc<AtomicBool>);

impl Drop for ScanCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct ScanReader<R> {
    inner: R,
    stop: Arc<AtomicBool>,
}

impl<R: Read> Read for ScanReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(io::Error::other("history scan cancelled"));
        }
        self.inner.read(buffer)
    }
}

#[derive(Deserialize)]
struct Record<'a> {
    ordinal: u64,
    #[serde(borrow)]
    timestamp: Cow<'a, str>,
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(borrow)]
    payload: &'a RawValue,
}

#[derive(Deserialize)]
struct EventHeader<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(default, borrow)]
    turn_id: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    item: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct ItemIdentity<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
}

/// The original creation position of an item in a selected physical source.
#[derive(Clone)]
pub(super) struct SourceItemAnchor {
    pub(super) ordinal: u64,
    pub(super) owner: ThreadId,
    pub(super) turn_id: String,
    pub(super) item_id: String,
}

/// Canonical turn and display-summary item events in physical ordinal order.
#[derive(Clone)]
pub(super) enum SourceTurnEvent {
    Turn(
        u64,
        ThreadId,
        codex_app_server_protocol::ThreadHistoryTurnMetadata,
    ),
    SummaryItem(u64, ThreadId, String, ThreadItem, i64),
}

impl SourceTurnEvent {
    pub(super) fn ordinal(&self) -> u64 {
        match self {
            Self::Turn(ordinal, ..) | Self::SummaryItem(ordinal, ..) => *ordinal,
        }
    }
}

#[derive(Default)]
pub(super) struct SourceScan {
    turn_ids: HashSet<String>,
    turn_bytes: usize,
    items: Vec<(i64, StoredThreadItem)>,
    pub(super) turns: Vec<SourceTurnEvent>,
    summary_bytes: usize,
    pub(super) anchors: Vec<SourceItemAnchor>,
    anchor_bytes: usize,
}

fn check_summary_source_size(bytes: usize) -> io::Result<()> {
    if bytes > MAX_SUMMARY_SOURCE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "rotated turn-summary source verification exceeds its 64 MiB limit",
        ));
    }
    Ok(())
}

/// Streams the exact selected prefix and validates both recorded boundary coordinates.
fn scan_source(
    segment: &RolloutLineageSegment,
    requested_turn: Option<&str>,
    collect_turns: bool,
    summary_turns: Option<&HashSet<String>>,
    collect_anchors: bool,
    stop: &Arc<AtomicBool>,
) -> io::Result<Option<SourceScan>> {
    let path = segment.rollout_path();
    let file = std::fs::File::open(path)?;
    let source: Box<dyn Read> = if path.extension().and_then(|s| s.to_str()) == Some("zst") {
        Box::new(zstd::stream::read::Decoder::new(file)?)
    } else {
        Box::new(file)
    };
    let mut reader = io::BufReader::new(ScanReader {
        inner: source,
        stop: stop.clone(),
    });
    let boundary = segment.end_ordinal().zip(segment.jsonl_end_byte_offset());
    if requested_turn.is_none() && boundary.is_none() {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    let mut offset = 0_u64;
    let mut previous = None;
    let mut result = SourceScan::default();
    let mut items: HashMap<String, (i64, StoredThreadItem)> = HashMap::new();
    loop {
        bytes.clear();
        let count = reader
            .by_ref()
            .take(16 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)?;
        if count == 0 {
            if boundary.is_some_and(|(_, end)| end != offset) {
                return Ok(None);
            }
            break;
        }
        let next = offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("history offset overflow"))?;
        if bytes.last() != Some(&b'\n') || bytes.len() > 16 * 1024 * 1024 {
            return Ok(None);
        }
        let Ok(record) = serde_json::from_slice::<Record<'_>>(&bytes) else {
            return Ok(None);
        };
        if let Some((end_ordinal, end_offset)) = boundary {
            if offset == end_offset {
                if record.ordinal < end_ordinal {
                    return Ok(None);
                }
                io::copy(&mut reader, &mut io::sink())?;
                break;
            }
            if offset > end_offset || next > end_offset || record.ordinal >= end_ordinal {
                return Ok(None);
            }
        }
        if previous.is_some_and(|value: u64| value.checked_add(1) != Some(record.ordinal)) {
            return Ok(None);
        }
        previous = Some(record.ordinal);
        if record.kind == "rollout_reference" || record.kind == "fork_reference" {
            return Ok(None);
        }
        if record.kind == "event_msg" {
            let Ok(header) = serde_json::from_str::<EventHeader<'_>>(record.payload.get()) else {
                return Ok(None);
            };
            if header.kind == "thread_rolled_back" {
                return Ok(None);
            }
            if collect_turns
                && matches!(
                    header.kind.as_ref(),
                    "task_started"
                        | "turn_started"
                        | "task_complete"
                        | "turn_complete"
                        | "turn_aborted"
                )
            {
                let Ok(event) = serde_json::from_str::<EventMsg>(record.payload.get()) else {
                    return Ok(None);
                };
                let line = codex_rollout::RolloutLine {
                    timestamp: record.timestamp.to_string(),
                    ordinal: Some(record.ordinal),
                    item: codex_rollout::RolloutItem::EventMsg(event),
                };
                result.turns.extend(
                    codex_app_server_protocol::project_rollout_line(&line)
                        .changed_turns
                        .into_iter()
                        .map(|turn| {
                            SourceTurnEvent::Turn(record.ordinal, segment.thread_id(), turn)
                        }),
                );
                if result.turns.len() > MAX_ANCESTOR_TURNS {
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "rotated turn-source verification exceeds its event limit",
                    ));
                }
            }
            if header.kind == "item_completed"
                && let Some(turn) = header.turn_id.as_deref()
                && summary_turns.is_some_and(|selected| selected.contains(turn))
            {
                let Ok(EventMsg::ItemCompleted(event)) =
                    serde_json::from_str::<EventMsg>(record.payload.get())
                else {
                    return Ok(None);
                };
                let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(&record.timestamp) else {
                    return Ok(None);
                };
                result.summary_bytes = result
                    .summary_bytes
                    .saturating_add(record.payload.get().len());
                check_summary_source_size(result.summary_bytes)?;
                result.turns.push(SourceTurnEvent::SummaryItem(
                    record.ordinal,
                    segment.thread_id(),
                    turn.to_string(),
                    ThreadItem::from(&event),
                    timestamp.timestamp_millis(),
                ));
            }
            if collect_anchors && header.kind == "item_completed" {
                let (Some(turn), Some(raw)) = (header.turn_id.as_deref(), header.item) else {
                    return Ok(None);
                };
                let Ok(item) = serde_json::from_str::<ItemIdentity<'_>>(raw.get()) else {
                    return Ok(None);
                };
                result.anchor_bytes = result
                    .anchor_bytes
                    .saturating_add(turn.len() + item.id.len());
                if result.anchor_bytes > MAX_ANCHOR_BYTES
                    || result.anchors.len() >= MAX_ANCESTOR_TURNS
                {
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "rotated item index exceeds its source verification limit",
                    ));
                }
                result.anchors.push(SourceItemAnchor {
                    ordinal: record.ordinal,
                    owner: segment.thread_id(),
                    turn_id: turn.to_string(),
                    item_id: item.id.into_owned(),
                });
            }
            if header.kind == "item_completed" {
                let Some(turn_id) = header.turn_id else {
                    return Ok(None);
                };
                if let Some(requested) = requested_turn {
                    if turn_id != requested {
                        offset = next;
                        continue;
                    }
                    let Ok(EventMsg::ItemCompleted(event)) =
                        serde_json::from_str(record.payload.get())
                    else {
                        return Ok(None);
                    };
                    let item = ThreadItem::from(&event);
                    let Ok(json) = serde_json::to_vec(&item.payload) else {
                        return Ok(None);
                    };
                    let Ok(ordinal) = i64::try_from(record.ordinal) else {
                        return Ok(None);
                    };
                    let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(&record.timestamp)
                    else {
                        return Ok(None);
                    };
                    let id = item.id().to_string();
                    let entry = items.entry(id.clone()).or_insert_with(|| {
                        (
                            ordinal,
                            StoredThreadItem {
                                turn_id: requested.to_owned(),
                                item_id: id,
                                updated_at_ordinal: record.ordinal,
                                created_at_ms: item
                                    .started_at_ms
                                    .unwrap_or_else(|| timestamp.timestamp_millis()),
                                started_at_ms: item.started_at_ms,
                                completed_at_ms: item.completed_at_ms,
                                item_json: json.clone(),
                            },
                        )
                    });
                    entry.1.updated_at_ordinal = record.ordinal;
                    entry.1.started_at_ms = entry.1.started_at_ms.or(item.started_at_ms);
                    entry.1.completed_at_ms = entry.1.completed_at_ms.or(item.completed_at_ms);
                    entry.1.item_json = json;
                    if items.len() > MAX_ROOT_ITEMS {
                        return Ok(None);
                    }
                } else {
                    let id = turn_id.into_owned();
                    if result.turn_ids.insert(id.clone()) {
                        result.turn_bytes = result.turn_bytes.saturating_add(id.len());
                    }
                    if result.turn_ids.len() > MAX_ANCESTOR_TURNS
                        || result.turn_bytes > MAX_ANCESTOR_TURN_BYTES
                    {
                        return Ok(None);
                    }
                }
            }
        }
        offset = next;
    }
    result.items = items.into_values().collect();
    result.items.sort_by_key(|item| item.0);
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::ItemComparison;
    use super::MAX_SUMMARY_SOURCE_BYTES;
    use super::check_summary_source_size;
    use super::compare_item_json;

    #[test]
    fn item_comparison_accepts_only_known_missing_null_defaults() {
        for (missing, null) in [
            (
                br#"{"type":"mcpToolCall","id":"m"}"#.as_slice(),
                br#"{"type":"mcpToolCall","id":"m","environmentId":null}"#.as_slice(),
            ),
            (
                br#"{"type":"agentMessage","id":"a","text":"answer"}"#.as_slice(),
                br#"{"type":"agentMessage","id":"a","text":"answer","moderation":null}"#.as_slice(),
            ),
        ] {
            assert!(matches!(
                compare_item_json(missing, null),
                ItemComparison::Equivalent
            ));
            assert!(matches!(
                compare_item_json(null, missing),
                ItemComparison::Equivalent
            ));
        }
        let one = br#"{"type":"mcpToolCall","id":"m","environmentId":"one"}"#;
        let two = br#"{"type":"mcpToolCall","id":"m","environmentId":"two"}"#;
        assert!(matches!(
            compare_item_json(one, one),
            ItemComparison::Equivalent
        ));
        assert!(matches!(
            compare_item_json(one, two),
            ItemComparison::Contradict
        ));
        assert!(matches!(
            compare_item_json(
                br#"{"type":"agentMessage","id":"a","moderation":null}"#,
                br#"{"type":"agentMessage","id":"a","moderation":{"decision":"blocked"}}"#
            ),
            ItemComparison::Contradict
        ));
        assert!(matches!(
            compare_item_json(
                br#"{"type":"agentMessage","id":"a","text":"old"}"#,
                br#"{"type":"agentMessage","id":"a","text":"new","moderation":null}"#
            ),
            ItemComparison::Contradict
        ));
        assert!(matches!(
            compare_item_json(
                br#"{"type":"agentMessage","id":"a","future":1}"#,
                br#"{"type":"agentMessage","id":"a"}"#
            ),
            ItemComparison::Uncertain
        ));
        assert!(matches!(
            compare_item_json(
                br#"{"type":"agentMessage","id":"a","id":"b"}"#,
                br#"{"type":"agentMessage","id":"a"}"#
            ),
            ItemComparison::Uncertain
        ));
    }

    #[test]
    fn summary_source_size_limit_reports_availability() {
        assert!(check_summary_source_size(MAX_SUMMARY_SOURCE_BYTES).is_ok());
        let error = check_summary_source_size(MAX_SUMMARY_SOURCE_BYTES + 1).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory);
        assert!(error.to_string().contains("64 MiB limit"));
    }
}
