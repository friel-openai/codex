use std::fs::File;
use std::io;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
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
/// Plain paginated JSONL rollouts use a reverse scan. When it finds both a usable replacement-
/// history checkpoint and the completed user-turn context needed for resume metadata, the returned
/// replay starts with the canonical head `SessionMeta` followed by that newest suffix. When no
/// bounded cutoff is available, the scan continues to the beginning and returns the complete
/// replay it already accumulated.
///
/// Legacy and compressed rollout shapes keep the existing full-history path.
pub(super) async fn load_latest_model_context(
    store: &LocalThreadStore,
    params: LoadThreadHistoryParams,
) -> ThreadStoreResult<StoredModelContext> {
    let path = read_thread::resolve_rollout_path(store, params.thread_id, params.include_archived)
        .await?
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("no rollout found for thread id {}", params.thread_id),
        })?;

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

    let items = if matches!(session_meta.meta.history_mode, ThreadHistoryMode::Paginated) {
        let lineage = store.resolve_rollout_lineage(params.thread_id).await?;
        scan_model_context_from_lineage(lineage, session_meta).await?
    } else {
        read_thread::load_history_items(store.config.codex_home.as_path(), path.as_path()).await?
    };

    Ok(StoredModelContext {
        thread_id: params.thread_id,
        items,
    })
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
            let ScanOutcome::Parsed(line) = outcome else {
                continue;
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
            match scan.push(item) {
                ModelContextScanProgress::Continue => {}
                ModelContextScanProgress::Complete => break 'segments,
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
            match scan.push(item) {
                ModelContextScanProgress::Continue => {}
                ModelContextScanProgress::Complete => break 'segments,
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
