//! Reads durable history into bounded batches ending at complete records, each with a checkpoint.

use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::BufReader as AsyncBufReader;
use tracing::warn;

use super::LocalThreadStore;
use super::rollout_lineage::AncestorReads;
use super::rollout_lineage::RolloutLineage;
use super::thread_history::ProjectedRolloutLine;
use super::thread_history::RolloutProjectionStep;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[path = "thread_history_materialization_pipeline.rs"]
mod pipeline;

const SQLITE_PROJECTION_METRIC: &str = "codex.thread_history.sqlite_projection";
const SQLITE_PROJECTION_ANOMALY_METRIC: &str = "codex.thread_history.sqlite_projection.anomaly";
const BATCH_SOURCE_BYTES: u64 = 1024 * 1024;
const BATCH_STEPS: usize = 256;

/// Do not resume a previous SQLite prefix from a copy changed during this operation.
///
/// Legacy checkpoints do not identify their source bytes. A copy selected on a later invocation
/// cannot be distinguished from the copy that produced an existing checkpoint.
pub(super) async fn ensure_fallback_projection_compatible(
    store: &LocalThreadStore,
    lineage: &RolloutLineage,
) -> ThreadStoreResult<()> {
    if !lineage.has_exclusions() {
        return Ok(());
    }
    for segment in lineage.segments() {
        let Some(_original) = lineage.excluded_copy_for(&segment.rollout_path) else {
            continue;
        };
        if let Some(state) =
            super::thread_history::projection_state(store, segment.rollout_id()).await?
            && (state.next_byte_offset != 0 || state.next_ordinal != 0)
        {
            return Err(ThreadStoreError::Conflict { message: "cannot resume an existing projection from an alternate ancestor without verified prefix equality".to_string() });
        }
    }
    Ok(())
}

struct ProjectionRollout {
    path: PathBuf,
    reads: AncestorReads,
}

pub(super) async fn materialize_to_sqlite(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
) -> ThreadStoreResult<()> {
    materialize_to_sqlite_with_reads(store, thread_id, rollout_path, &AncestorReads::default())
        .await
}

/// Project this rollout and identify an unreadable ancestor before committing a batch.
pub(super) async fn materialize_to_sqlite_with_reads(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    reads: &AncestorReads,
) -> ThreadStoreResult<()> {
    materialize_projection(
        store,
        thread_id,
        ProjectionRollout {
            path: rollout_path.to_owned(),
            reads: reads.clone(),
        },
        /*flushed_len*/ None,
    )
    .await
}

pub(super) async fn materialize_durable_suffix(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    flushed_len: Option<u64>,
) -> ThreadStoreResult<()> {
    materialize_projection(
        store,
        thread_id,
        ProjectionRollout {
            path: rollout_path.to_owned(),
            reads: AncestorReads::default(),
        },
        flushed_len,
    )
    .await
}

async fn materialize_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout: ProjectionRollout,
    flushed_len: Option<u64>,
) -> ThreadStoreResult<()> {
    if store.state_db.is_none() {
        return Ok(());
    }
    let reads = rollout.reads.clone();
    let before = if reads.has_tracked() {
        Some(super::thread_history::projection_state(store, thread_id).await)
    } else {
        None
    };
    // A later ancestor can fail after this projection has committed. Remember progress even
    // when this call succeeds, so the entire operation cannot retry with another copy.
    let result = materialize_to_sqlite_with_state_db(store, thread_id, rollout, flushed_len).await;
    if reads.has_tracked() {
        let after = super::thread_history::projection_state(store, thread_id).await;
        let position = |state: Option<super::thread_history::RolloutProjectionState>| {
            state.map(|state| (state.next_byte_offset, state.next_ordinal))
        };
        let before = before.and_then(Result::ok).map(position);
        let after = after.ok().map(position);
        let changed_or_unknown = before.is_none() || after.is_none() || before != after;
        let failed_with_existing_state =
            result.is_err() && before.flatten().is_some_and(|state| state != (0, 0));
        if changed_or_unknown || failed_with_existing_state {
            reads.prevent_retry();
        }
    }
    record_projection_outcome(&result);
    result
}

async fn materialize_to_sqlite_with_state_db(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout: ProjectionRollout,
    flushed_len: Option<u64>,
) -> ThreadStoreResult<()> {
    let projection_state = super::thread_history::projection_state(store, thread_id).await?;
    let mut start_offset = projection_state
        .as_ref()
        .map_or(0, |state| state.next_byte_offset);
    if start_offset > 0 && Some(start_offset) == flushed_len {
        return Ok(());
    }
    if rollout
        .reads
        .cutoff(&rollout.path)
        .is_some_and(|end| start_offset >= end.end_byte_offset)
    {
        return Ok(());
    }
    if let Some(state) = projection_state.as_ref()
        && !rollout.reads.tracks(&rollout.path)
        && start_offset != 0
        && first_rollout_ordinal(&rollout.path, false).await? == Some(state.next_ordinal)
    {
        // The stable rollout was replaced after its immutable prefix was projected. Recover the
        // physical cursor without changing the lineage ordinal or deleting the existing rows.
        super::thread_history::reset_projection_for_replacement(
            store,
            thread_id,
            state.next_ordinal,
        )
        .await?;
        start_offset = 0;
    }
    if projection_state.is_none()
        && !rollout.reads.tracks(&rollout.path)
        && codex_rollout::existing_rollout_path(&rollout.path)
            .await
            .is_none()
    {
        return Ok(());
    }
    let session_meta = if rollout.reads.tracks(&rollout.path) {
        codex_rollout::read_session_meta_line_exact(&rollout.path).await
    } else {
        codex_rollout::read_session_meta_line(&rollout.path).await
    }
    .map_err(|err| {
        rollout.reads.record_failure(&rollout.path);
        ThreadStoreError::Internal {
            message: format!(
                "failed to read rollout metadata {}: {err}",
                rollout.path.display()
            ),
        }
    })?
    .meta;
    let initial_ordinal = match session_meta.history_base {
        Some(base) => base.end_ordinal_exclusive,
        None => first_rollout_ordinal(&rollout.path, rollout.reads.tracks(&rollout.path))
            .await
            .map_err(|err| {
                rollout.reads.record_failure(&rollout.path);
                err
            })?
            .unwrap_or(0),
    };
    let subagent_history_start_ordinal = session_meta.subagent_history_start_ordinal;
    let expected_ordinal = projection_state
        .as_ref()
        .map_or(initial_ordinal, |state| state.next_ordinal);
    pipeline::read_and_apply(
        store,
        thread_id,
        rollout,
        start_offset,
        expected_ordinal,
        initial_ordinal,
        subagent_history_start_ordinal,
    )
    .await
}

/// Project legacy-visible history without changing canonical rollout records or their ordinals.
pub(super) async fn materialize_legacy_to_sqlite(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    builder: &mut ThreadHistoryBuilder,
) -> ThreadStoreResult<()> {
    materialize_legacy_to_sqlite_backfill(store, thread_id, rollout_path, builder, true).await
}

pub(super) async fn materialize_legacy_to_sqlite_backfill(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    builder: &mut ThreadHistoryBuilder,
    complete: bool,
) -> ThreadStoreResult<()> {
    let result =
        materialize_legacy_to_sqlite_inner(store, thread_id, rollout_path, builder, complete).await;
    if result.is_err() {
        builder.reset();
    }
    result
}

async fn materialize_legacy_to_sqlite_inner(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    builder: &mut ThreadHistoryBuilder,
    complete: bool,
) -> ThreadStoreResult<()> {
    const MAX_PROJECTION_BATCH_RECORDS: usize = 256;
    const MAX_PROJECTION_BATCH_BYTES: u64 = 4 * 1024 * 1024;

    let projection_state = super::thread_history::projection_state(store, thread_id).await?;
    let mut batch_start_offset = projection_state.as_ref().map_or(0, |state| {
        if state.next_byte_offset
            == super::thread_history::INCOMPLETE_LEGACY_PROJECTION_BYTE_OFFSET.unsigned_abs()
        {
            0
        } else {
            state.next_byte_offset
        }
    });
    let mut next_rollout_ordinal = projection_state
        .as_ref()
        .map_or(0, |state| state.next_ordinal);
    let initial_ordinal = next_rollout_ordinal;
    let end_offset = match tokio::fs::metadata(rollout_path).await {
        Ok(metadata) => metadata.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && batch_start_offset == 0 => {
            return Ok(());
        }
        Err(err) => return Err(thread_store_io_error(err)),
    };
    let byte_count =
        end_offset
            .checked_sub(batch_start_offset)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "durable rollout shrank before legacy projection".to_string(),
            })?;
    if byte_count == 0 {
        return Ok(());
    }

    let mut file = tokio::fs::File::open(rollout_path)
        .await
        .map_err(thread_store_io_error)?;
    file.seek(SeekFrom::Start(batch_start_offset))
        .await
        .map_err(thread_store_io_error)?;
    let mut reader = AsyncBufReader::new(file.take(byte_count));
    let mut line_bytes = Vec::new();
    let mut next_offset = batch_start_offset;
    let mut projections = Vec::with_capacity(MAX_PROJECTION_BATCH_RECORDS);

    loop {
        line_bytes.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line_bytes)
            .await
            .map_err(thread_store_io_error)?;
        if bytes_read == 0 || !line_bytes.ends_with(b"\n") {
            break;
        }

        let start_byte_offset = next_offset;
        next_offset = next_offset
            .checked_add(
                u64::try_from(bytes_read).map_err(|_| ThreadStoreError::Internal {
                    message: "legacy rollout line exceeds addressable range".to_string(),
                })?,
            )
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "legacy rollout byte offset overflow".to_string(),
            })?;

        if !line_bytes.iter().all(u8::is_ascii_whitespace) {
            match codex_rollout::parse_rollout_line_bytes(&line_bytes) {
                Ok(line) => {
                    let created_at_ms = DateTime::parse_from_rfc3339(line.timestamp.as_str())
                        .map(|timestamp| timestamp.timestamp_millis())
                        .map_err(thread_history_error)?;
                    let is_physical_rollout_record =
                        matches!(&line.item, RolloutItem::SessionMeta(_))
                            && next_rollout_ordinal != 0
                            || matches!(&line.item, RolloutItem::RolloutReference(_));
                    let changes = if !is_physical_rollout_record
                        && let Some(item) = codex_rollout::persisted_rollout_item(
                            &line.item,
                            ThreadHistoryMode::Legacy,
                        ) {
                        builder.handle_rollout_item_with_changes(item.as_ref())
                    } else {
                        ThreadHistoryChangeSet::default()
                    };
                    projections.push(RolloutProjectionStep::Line(Box::new(
                        ProjectedRolloutLine {
                            ordinal: next_rollout_ordinal,
                            start_byte_offset,
                            end_byte_offset: next_offset,
                            fallback_created_at_ms: Some(created_at_ms),
                            changes,
                            realtime_item: match line.item {
                                RolloutItem::RealtimeItem(item) => Some(item),
                                _ => None,
                            },
                        },
                    )));
                    next_rollout_ordinal =
                        next_rollout_ordinal.checked_add(1).ok_or_else(|| {
                            ThreadStoreError::Internal {
                                message: "legacy rollout projection ordinal overflow".to_string(),
                            }
                        })?;
                }
                Err(err) => {
                    warn!(
                        "skipping rejected legacy rollout line while projecting {rollout_path:?}: {err}"
                    );
                }
            }
        }

        if projections.len() >= MAX_PROJECTION_BATCH_RECORDS
            || next_offset - batch_start_offset >= MAX_PROJECTION_BATCH_BYTES
        {
            let batch = std::mem::replace(
                &mut projections,
                Vec::with_capacity(MAX_PROJECTION_BATCH_RECORDS),
            );
            super::thread_history::apply_legacy_projection(
                store,
                thread_id,
                batch_start_offset,
                next_offset,
                initial_ordinal,
                batch,
                false,
            )
            .await?;
            batch_start_offset = next_offset;
        }
    }

    if next_offset != batch_start_offset || complete {
        super::thread_history::apply_legacy_projection(
            store,
            thread_id,
            batch_start_offset,
            next_offset,
            initial_ordinal,
            projections,
            complete,
        )
        .await?;
    }

    Ok(())
}

async fn first_rollout_ordinal(rollout_path: &Path, exact: bool) -> ThreadStoreResult<Option<u64>> {
    let mut reader = if exact {
        codex_rollout::open_rollout_line_reader_exact(rollout_path).await
    } else {
        codex_rollout::open_rollout_line_reader(rollout_path).await
    }
    .map_err(thread_store_io_error)?;
    while let Some(line) = reader.next_line().await.map_err(thread_store_io_error)? {
        if line.trim().is_empty() {
            continue;
        }
        let line =
            codex_rollout::parse_rollout_line(line.as_str()).map_err(thread_history_error)?;
        return Ok(line.ordinal);
    }
    Ok(None)
}

/// Read and project the complete durable prefix in one blocking task.
fn read_projection_batches(
    rollout: &ProjectionRollout,
    start_offset: u64,
    expected_ordinal: u64,
    thread_id: ThreadId,
    subagent_history_start_ordinal: Option<u64>,
    is_cancelled: impl Fn() -> bool,
    mut emit: impl FnMut(Vec<RolloutProjectionStep>, u64) -> ThreadStoreResult<()>,
) -> ThreadStoreResult<()> {
    let rollout_path = &rollout.path;
    let mut file = match rollout.reads.open(rollout_path, &is_cancelled) {
        Ok(file) => file,
        Err(err)
            if err.kind() == std::io::ErrorKind::NotFound
                && start_offset == 0
                && !rollout.reads.tracks(rollout_path) =>
        {
            return Ok(());
        }
        Err(err) => return Err(thread_store_io_error(err)),
    };
    let file_end_offset = match rollout.reads.cutoff(rollout_path) {
        Some(end) => end.end_byte_offset,
        None => file.seek(SeekFrom::End(0)).map_err(thread_store_io_error)?,
    };
    let byte_count = file_end_offset.checked_sub(start_offset).ok_or_else(|| {
        rollout.reads.record_failure(rollout_path);
        ThreadStoreError::Internal {
            message: format!(
                "durable rollout shrank before projection: {}",
                rollout_path.display()
            ),
        }
    })?;
    file.seek(SeekFrom::Start(start_offset))
        .map_err(thread_store_io_error)?;
    // Freeze this pass at the captured end, without retaining the entire durable suffix.
    let mut reader = BufReader::new(file.take(byte_count));
    let mut line_bytes = Vec::new();
    // Only project the newline-terminated prefix; leave a trailing partial record for the next
    // pass.
    let mut next_ordinal = expected_ordinal;
    let mut next_offset = start_offset;
    let mut line_start_offset = start_offset;
    let mut batch_start_offset = start_offset;
    let mut batch = Vec::new();
    loop {
        if is_cancelled() {
            return Err(thread_store_io_error(std::io::Error::other(
                "rollout read cancelled",
            )));
        }
        // Flush only between complete records: an ordinal gap and its following line must
        // share a checkpoint. Byte-only progress from skipped records also bounds batches.
        if next_offset - batch_start_offset >= BATCH_SOURCE_BYTES || batch.len() >= BATCH_STEPS {
            emit(std::mem::take(&mut batch), next_offset)?;
            batch_start_offset = next_offset;
        }
        line_bytes.clear();
        if reader
            .read_until(b'\n', &mut line_bytes)
            .map_err(thread_store_io_error)?
            == 0
        {
            break;
        }
        if !line_bytes.ends_with(b"\n") {
            break;
        }
        let line_end_offset = line_start_offset
            .checked_add(u64::try_from(line_bytes.len()).map_err(|_| {
                ThreadStoreError::Internal {
                    message: "durable rollout byte offset overflow".to_string(),
                }
            })?)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "durable rollout byte offset overflow".to_string(),
            })?;
        if line_bytes.iter().all(u8::is_ascii_whitespace) {
            next_offset = line_end_offset;
            line_start_offset = line_end_offset;
            continue;
        }
        let value = match serde_json::from_slice::<serde_json::Value>(&line_bytes) {
            Ok(value) => value,
            Err(err) => {
                warn!(
                    thread_id = %thread_id,
                    rollout_path = %rollout_path.display(),
                    line_start_byte_offset = line_start_offset,
                    line_end_byte_offset = line_end_offset,
                    expected_ordinal = next_ordinal,
                    error = %err,
                    "skipping malformed rollout line during projection"
                );
                record_projection_anomaly(ProjectionAnomaly::MalformedJson);
                next_offset = line_end_offset;
                line_start_offset = line_end_offset;
                continue;
            }
        };
        let raw_ordinal = value.get("ordinal").and_then(serde_json::Value::as_u64);
        let line = match codex_rollout::decode_rollout_line(value) {
            Ok(line) => line,
            Err(err) => {
                warn!(
                    thread_id = %thread_id,
                    rollout_path = %rollout_path.display(),
                    line_start_byte_offset = line_start_offset,
                    line_end_byte_offset = line_end_offset,
                    expected_ordinal = next_ordinal,
                    line_ordinal = ?raw_ordinal,
                    error = %err,
                    "skipping unknown rollout line during projection"
                );
                record_projection_anomaly(ProjectionAnomaly::UnknownLine);
                // Unknown records only advance the byte checkpoint. A writer that could not
                // decode this line may have reused its ordinal, so let the next decoded line
                // resolve reuse versus a gap.
                next_offset = line_end_offset;
                line_start_offset = line_end_offset;
                continue;
            }
        };
        let ordinal = match line.ordinal {
            Some(ordinal) => ordinal,
            None => {
                warn!(
                    thread_id = %thread_id,
                    rollout_path = %rollout_path.display(),
                    line_start_byte_offset = line_start_offset,
                    line_end_byte_offset = line_end_offset,
                    expected_ordinal = next_ordinal,
                    "skipping paginated rollout line without an ordinal"
                );
                record_projection_anomaly(ProjectionAnomaly::MissingOrdinal);
                next_offset = line_end_offset;
                line_start_offset = line_end_offset;
                continue;
            }
        };
        if ordinal < next_ordinal {
            warn!(
                thread_id = %thread_id,
                rollout_path = %rollout_path.display(),
                line_start_byte_offset = line_start_offset,
                line_end_byte_offset = line_end_offset,
                expected_ordinal = next_ordinal,
                line_ordinal = ordinal,
                "skipping duplicate or regressed rollout ordinal during projection"
            );
            record_projection_anomaly(ProjectionAnomaly::DuplicateOrRegressedOrdinal);
            next_offset = line_end_offset;
            line_start_offset = line_end_offset;
            continue;
        }
        let is_inherited_subagent_history =
            subagent_history_start_ordinal.is_some_and(|start| ordinal < start);
        let changes = if is_inherited_subagent_history {
            ThreadHistoryChangeSet::default()
        } else {
            project_rollout_line(&line)
        };
        let fallback_created_at_ms = if changes
            .changed_items
            .iter()
            .any(|change| change.item.started_at_ms.is_none())
            || (!is_inherited_subagent_history
                && matches!(&line.item, RolloutItem::RealtimeItem(_)))
        {
            match DateTime::parse_from_rfc3339(line.timestamp.as_str()) {
                Ok(timestamp) => Some(timestamp.timestamp_millis()),
                Err(err) => {
                    warn!(
                        thread_id = %thread_id,
                        rollout_path = %rollout_path.display(),
                        line_start_byte_offset = line_start_offset,
                        line_end_byte_offset = line_end_offset,
                        expected_ordinal = next_ordinal,
                        line_ordinal = ordinal,
                        error = %err,
                        "skipping rollout line with invalid timestamp during projection"
                    );
                    record_projection_anomaly(ProjectionAnomaly::InvalidTimestamp);
                    let end_ordinal_exclusive =
                        ordinal
                            .checked_add(1)
                            .ok_or_else(|| ThreadStoreError::Internal {
                                message: "rollout ordinal exceeds SQLite integer range".to_string(),
                            })?;
                    batch.push(RolloutProjectionStep::SkippedOrdinalRange {
                        start_ordinal: next_ordinal,
                        end_ordinal_exclusive,
                    });
                    next_ordinal = end_ordinal_exclusive;
                    next_offset = line_end_offset;
                    line_start_offset = line_end_offset;
                    continue;
                }
            }
        } else {
            None
        };
        if ordinal > next_ordinal {
            warn!(
                thread_id = %thread_id,
                rollout_path = %rollout_path.display(),
                line_start_byte_offset = line_start_offset,
                line_end_byte_offset = line_end_offset,
                expected_ordinal = next_ordinal,
                line_ordinal = ordinal,
                skipped_ordinal_start = next_ordinal,
                skipped_ordinal_end_exclusive = ordinal,
                "skipping missing rollout ordinal range during projection"
            );
            record_projection_anomaly(ProjectionAnomaly::ForwardOrdinalGap);
            batch.push(RolloutProjectionStep::SkippedOrdinalRange {
                start_ordinal: next_ordinal,
                end_ordinal_exclusive: ordinal,
            });
        }
        let next_line_ordinal =
            ordinal
                .checked_add(1)
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "rollout ordinal exceeds SQLite integer range".to_string(),
                })?;
        batch.push(RolloutProjectionStep::Line(Box::new(
            ProjectedRolloutLine {
                ordinal,
                start_byte_offset: line_start_offset,
                end_byte_offset: line_end_offset,
                fallback_created_at_ms,
                changes,
                realtime_item: match line.item {
                    RolloutItem::RealtimeItem(item) if !is_inherited_subagent_history => Some(item),
                    _ => None,
                },
            },
        )));
        next_ordinal = next_line_ordinal;
        next_offset = line_end_offset;
        line_start_offset = line_end_offset;
    }
    if reader.get_ref().limit() != 0 {
        return Err(thread_store_io_error(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "durable rollout shrank during projection",
        )));
    }
    if next_offset != batch_start_offset {
        emit(batch, next_offset)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ProjectionAnomaly {
    MalformedJson,
    UnknownLine,
    MissingOrdinal,
    DuplicateOrRegressedOrdinal,
    ForwardOrdinalGap,
    InvalidTimestamp,
}

impl ProjectionAnomaly {
    fn tag(self) -> &'static str {
        match self {
            Self::MalformedJson => "malformed_json",
            Self::UnknownLine => "unknown_line",
            Self::MissingOrdinal => "missing_ordinal",
            Self::DuplicateOrRegressedOrdinal => "duplicate_or_regressed_ordinal",
            Self::ForwardOrdinalGap => "forward_ordinal_gap",
            Self::InvalidTimestamp => "invalid_timestamp",
        }
    }
}

fn record_projection_outcome(result: &ThreadStoreResult<()>) {
    let Some(metrics) = codex_otel::global() else {
        return;
    };
    let outcome = if result.is_ok() { "success" } else { "error" };
    let _ = metrics.counter(
        SQLITE_PROJECTION_METRIC,
        /*inc*/ 1,
        &[("outcome", outcome)],
    );
}

fn record_projection_anomaly(anomaly: ProjectionAnomaly) {
    let Some(metrics) = codex_otel::global() else {
        return;
    };
    let _ = metrics.counter(
        SQLITE_PROJECTION_ANOMALY_METRIC,
        /*inc*/ 1,
        &[("kind", anomaly.tag())],
    );
}

fn thread_history_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to project thread history: {err}"),
    }
}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "thread_history_materialization_tests.rs"]
mod tests;
