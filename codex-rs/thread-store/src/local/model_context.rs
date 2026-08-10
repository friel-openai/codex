use std::fs::File;
use std::fs::Metadata;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::ScanOutcome;

use super::LocalThreadStore;
use super::read_thread;
use super::rollout_lineage::RolloutLineage;
use crate::LoadThreadHistoryParams;
use crate::StoredModelContext;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
#[path = "model_context_tests.rs"]
mod tests;

/// Loads rollout items needed to reconstruct the latest model-visible context.
///
/// Plain paginated JSONL rollouts use a reverse scan. A certified segment-state checkpoint returns
/// the canonical head `SessionMeta` followed by that newest suffix. An unmarked compaction is not
/// sufficient because sticky settings and token state may exist before it. Without a certified
/// checkpoint, the reader scans the complete compatibility lineage and returns a bounded replay.
///
/// Indexed segmented legacy rollouts with certified checkpoints use the same bounded active scan.
/// Unmarked, unindexed, or inherited rollouts still replay canonical compatibility history.
pub(super) async fn load_latest_model_context(
    store: &LocalThreadStore,
    params: LoadThreadHistoryParams,
) -> ThreadStoreResult<StoredModelContext> {
    let path = match params.rollout_path.as_ref() {
        Some(path) => read_thread::resolve_requested_rollout_path(store, path.clone()).await?,
        None => read_thread::resolve_rollout_path(store, params.thread_id, params.include_archived)
            .await?
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!("no rollout found for thread id {}", params.thread_id),
            })?,
    };

    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read session metadata {}: {err}", path.display()),
        })?;
    if session_meta.meta.id != params.thread_id {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout at {} belongs to thread {}, not {}",
                path.display(),
                session_meta.meta.id,
                params.thread_id
            ),
        });
    }

    let items = if let Some(items) =
        scan_projected_active_model_context(store, &path, &session_meta).await?
    {
        items
    } else if matches!(session_meta.meta.history_mode, ThreadHistoryMode::Paginated)
        || (matches!(session_meta.meta.history_mode, ThreadHistoryMode::Legacy)
            && session_meta.meta.segment_id.is_some())
    {
        if matches!(session_meta.meta.history_mode, ThreadHistoryMode::Legacy) {
            let (lines, _, parse_errors) =
                codex_rollout::RolloutRecorder::load_rollout_lines(path.as_path())
                    .await
                    .map_err(thread_store_io_error)?;
            if parse_errors != 0 {
                return Err(ThreadStoreError::Internal {
                    message: format!(
                        "failed to load latest model context {}: rollout contains {parse_errors} invalid record(s)",
                        path.display()
                    ),
                });
            }
            codex_rollout::materialize_model_context_rollout_items_from(
                store.config.codex_home.as_path(),
                lines,
            )
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!(
                    "failed to load latest model context {}: {err}",
                    path.display()
                ),
            })?
        } else {
            let lineage = match params.rollout_path {
                Some(_) => {
                    store
                        .resolve_rollout_lineage_from_path(params.thread_id, path.clone())
                        .await?
                }
                None => store.resolve_rollout_lineage(params.thread_id).await?,
            };
            scan_model_context_from_lineage(lineage, session_meta).await?
        }
    } else {
        read_thread::load_history_items(store.config.codex_home.as_path(), path.as_path()).await?
    };

    Ok(StoredModelContext {
        thread_id: params.thread_id,
        items,
    })
}

/// Uses a complete active checkpoint without traversing immutable same-thread predecessors.
///
/// A certified segment-state checkpoint is authoritative from the JSONL file itself; projection
/// state and predecessor availability cannot invalidate it. Unmarked, incomplete, malformed, or
/// concurrently replaced files preserve the complete-lineage compatibility implementation.
pub(super) async fn scan_projected_active_model_context(
    _store: &LocalThreadStore,
    path: &Path,
    session_meta: &SessionMetaLine,
) -> ThreadStoreResult<Option<Vec<RolloutItem>>> {
    let compressed_active = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".jsonl.zst"));
    if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl")
        && !compressed_active
    {
        return Ok(None);
    }

    let before = tokio::fs::metadata(path)
        .await
        .map_err(thread_store_io_error)?;
    let active_scan = if compressed_active {
        let (lines, _, parse_errors) = codex_rollout::RolloutRecorder::load_rollout_lines(path)
            .await
            .map_err(thread_store_io_error)?;
        if parse_errors != 0 {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "active rollout {} contains {parse_errors} invalid record(s)",
                    path.display()
                ),
            });
        }
        scan_loaded_active_model_context(lines, session_meta.clone())
    } else {
        let path_for_scan = path.to_path_buf();
        let meta_for_scan = session_meta.clone();
        tokio::task::spawn_blocking(move || {
            scan_projected_active_model_context_blocking(&path_for_scan, meta_for_scan)
        })
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to join indexed model context scan: {err}"),
        })?
        .map_err(thread_store_io_error)?
    };
    let Some(active_scan) = active_scan else {
        return Ok(None);
    };
    if !active_scan.segment_checkpoint {
        return Ok(None);
    }

    let after = tokio::fs::metadata(path)
        .await
        .map_err(thread_store_io_error)?;
    if !unchanged_active_rollout(&before, &after).map_err(thread_store_io_error)? {
        return Ok(None);
    }

    tracing::debug!(
        outcome = "active_checkpoint_hit",
        active_segments_opened = 1_u64,
        referenced_segments_opened = 0_u64,
        active_segment_bytes = before.len(),
        records_scanned = active_scan.records_scanned,
        compressed_active,
        "loaded latest model context from the active rollout segment"
    );

    Ok(Some(active_scan.items))
}

struct ActiveModelContextScan {
    items: Vec<RolloutItem>,
    segment_checkpoint: bool,
    records_scanned: u64,
}

fn scan_loaded_active_model_context(
    lines: Vec<RolloutLine>,
    session_meta: SessionMetaLine,
) -> Option<ActiveModelContextScan> {
    let mut scan = ModelContextScan::default();
    for (index, line) in lines.into_iter().rev().enumerate() {
        let records_scanned = index as u64 + 1;
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            continue;
        }
        let progress = scan.push(line.item);
        if progress.is_complete() {
            let segment_checkpoint = scan.completed_at_segment_checkpoint();
            return Some(ActiveModelContextScan {
                items: scan.finish(session_meta),
                segment_checkpoint,
                records_scanned,
            });
        }
    }
    None
}

fn scan_projected_active_model_context_blocking(
    path: &Path,
    session_meta: SessionMetaLine,
) -> io::Result<Option<ActiveModelContextScan>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    line.clear();
    if reader.read_line(&mut line)? != 0 {
        serde_json::from_str::<RolloutLine>(&line)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    }

    let mut scanner = ReverseJsonlScanner::new(reader.into_inner())?;
    let mut scan = ModelContextScan::default();
    let mut records_scanned = 0_u64;
    while let Some(outcome) = scanner.scan_next::<RolloutLine>()? {
        let line = match outcome {
            ScanOutcome::Parsed(line) => line,
            ScanOutcome::Rejected(err) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };
        records_scanned += 1;
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            continue;
        }
        let progress = scan.push(line.item);
        if progress.is_complete() {
            let segment_checkpoint = scan.completed_at_segment_checkpoint();
            return Ok(Some(ActiveModelContextScan {
                items: scan.finish(session_meta),
                segment_checkpoint,
                records_scanned,
            }));
        }
    }
    Ok(None)
}

fn unchanged_active_rollout(before: &Metadata, after: &Metadata) -> io::Result<bool> {
    if before.len() != after.len() || before.modified()? != after.modified()? {
        return Ok(false);
    }
    #[cfg(unix)]
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
    {
        return Ok(false);
    }
    Ok(true)
}

/// Loads startup context from a fork's frozen inherited prefix.
pub(super) async fn load_for_fork(
    lineage: RolloutLineage,
    history_base: Option<HistoryPosition>,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let source_path = lineage
        .segments()
        .last()
        .map(|segment| segment.rollout_path.as_path())
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let session_meta = codex_rollout::read_session_meta_line(source_path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read session metadata {}: {err}",
                source_path.display()
            ),
        })?;
    match history_base {
        Some(history_base) => {
            let lineage = lineage.truncate_at(history_base).await?;
            scan_model_context_from_lineage(lineage, session_meta).await
        }
        None => Ok(vec![RolloutItem::SessionMeta(session_meta)]),
    }
}

/// Loads the complete logical prefix selected for a prepared fork.
///
/// Unlike [`load_for_fork`], this is response hydration rather than model input, so it must not
/// stop at a replacement-history checkpoint.
pub(super) async fn load_full_for_fork(
    lineage: RolloutLineage,
    history_base: Option<HistoryPosition>,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let source_path = lineage
        .segments()
        .last()
        .map(|segment| segment.rollout_path.as_path())
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let session_meta = codex_rollout::read_session_meta_line(source_path)
        .await
        .map_err(thread_store_io_error)?;
    let Some(history_base) = history_base else {
        return Ok(vec![RolloutItem::SessionMeta(session_meta)]);
    };
    let lineage = lineage.truncate_at(history_base).await?;
    let mut items = vec![RolloutItem::SessionMeta(session_meta)];
    for segment in lineage.segments() {
        let (lines, _, parse_errors) =
            codex_rollout::RolloutRecorder::load_rollout_lines(segment.rollout_path.as_path())
                .await
                .map_err(thread_store_io_error)?;
        if parse_errors != 0 {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to load prepared fork history: {} contains {parse_errors} invalid record(s)",
                    segment.rollout_path.display()
                ),
            });
        }
        for line in lines {
            let Some(ordinal) = line.ordinal else {
                continue;
            };
            if ordinal < segment.start_ordinal
                || segment
                    .end_ordinal_exclusive
                    .is_some_and(|end| ordinal >= end)
            {
                continue;
            }
            let mut item = line.item;
            if matches!(
                item,
                RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
            ) || !segment.filter_rollout_item(&mut item)
            {
                continue;
            }
            items.push(item);
        }
    }
    Ok(items)
}

async fn scan_model_context_from_lineage(
    lineage: RolloutLineage,
    session_meta: SessionMetaLine,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    if lineage.segments().iter().any(|segment| {
        segment
            .rollout_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".zst"))
    }) {
        return scan_loaded_model_context_from_lineage(&lineage, session_meta).await;
    }
    let scan = tokio::task::spawn_blocking(move || {
        scan_model_context_from_lineage_blocking(&lineage, session_meta)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join model context scan: {err}"),
    })?;
    match scan {
        Ok(items) => Ok(items),
        Err(err) => Err(ThreadStoreError::Internal {
            message: format!("failed to scan paginated model context lineage: {err}"),
        }),
    }
}

fn scan_model_context_from_lineage_blocking(
    lineage: &RolloutLineage,
    session_meta: SessionMetaLine,
) -> io::Result<Vec<RolloutItem>> {
    let mut scan = ModelContextScan::default();
    'segments: for segment in lineage.segments().iter().rev() {
        let file = File::open(segment.rollout_path.as_path())?;
        let mut scanner = match segment.end_byte_offset {
            Some(end_byte_offset) => ReverseJsonlScanner::new_at(file, end_byte_offset)?,
            None => ReverseJsonlScanner::new(file)?,
        };
        while let Some(outcome) = scanner.scan_next::<RolloutLine>()? {
            let line = match outcome {
                ScanOutcome::Parsed(line) => line,
                ScanOutcome::Rejected(err) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, err));
                }
            };
            if let Some(ordinal) = line.ordinal
                && (ordinal < segment.start_ordinal
                    || segment
                        .end_ordinal_exclusive
                        .is_some_and(|end| ordinal >= end))
            {
                continue;
            }
            // Each physical segment contributes only its local delta. Its head metadata is
            // replaced with the requested thread's canonical SessionMeta after replay.
            let mut item = line.item;
            if matches!(&item, RolloutItem::SessionMeta(_)) {
                break;
            }
            if matches!(&item, RolloutItem::RolloutReference(_))
                || !segment.filter_rollout_item(&mut item)
            {
                continue;
            }
            if scan.push(item).is_complete() {
                break 'segments;
            }
        }
    }

    let canonical_meta = session_meta.clone();
    let mut items = scan.finish(session_meta);
    if !matches!(items.first(), Some(RolloutItem::SessionMeta(_))) {
        items.insert(0, RolloutItem::SessionMeta(canonical_meta));
    }
    Ok(items)
}

async fn scan_loaded_model_context_from_lineage(
    lineage: &RolloutLineage,
    session_meta: SessionMetaLine,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let mut scan = ModelContextScan::default();
    'segments: for segment in lineage.segments().iter().rev() {
        let (lines, _, parse_errors) =
            codex_rollout::RolloutRecorder::load_rollout_lines(segment.rollout_path.as_path())
                .await
                .map_err(thread_store_io_error)?;
        if parse_errors != 0 {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to scan paginated model context lineage: {} contains {parse_errors} invalid record(s)",
                    segment.rollout_path.display()
                ),
            });
        }
        for line in lines.into_iter().rev() {
            if let Some(ordinal) = line.ordinal
                && (ordinal < segment.start_ordinal
                    || segment
                        .end_ordinal_exclusive
                        .is_some_and(|end| ordinal >= end))
            {
                continue;
            }
            let mut item = line.item;
            if matches!(&item, RolloutItem::SessionMeta(_)) {
                break;
            }
            if matches!(&item, RolloutItem::RolloutReference(_))
                || !segment.filter_rollout_item(&mut item)
            {
                continue;
            }
            if scan.push(item).is_complete() {
                break 'segments;
            }
        }
    }
    let canonical_meta = session_meta.clone();
    let mut items = scan.finish(session_meta);
    if !matches!(items.first(), Some(RolloutItem::SessionMeta(_))) {
        items.insert(0, RolloutItem::SessionMeta(canonical_meta));
    }
    Ok(items)
}

fn thread_store_io_error(err: io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to scan paginated model context lineage: {err}"),
    }
}
