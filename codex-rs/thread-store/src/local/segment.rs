use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;
use serde_json::Value;
use sha2::Digest as _;
use sha2::Sha256;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use super::LocalThreadStore;
use crate::FreezeRolloutSegmentParams;
use crate::FrozenRolloutSegment;
use crate::ThreadPersistenceMode;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) async fn freeze_thread_segment(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    freeze_thread_segment_locked(store, thread_id, params).await
}

pub(super) async fn freeze_thread_segment_locked(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    let live_entry = store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .map(|entry| (entry.recorder.clone(), entry.history_mode));
    if let Some((recorder, _history_mode)) = live_entry.as_ref() {
        recorder.persist().await.map_err(thread_store_io_error)?;
        {
            let mut live_recorders = store.live_recorders.lock().await;
            let entry = live_recorders
                .get_mut(&thread_id)
                .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            entry.persistence_mode = ThreadPersistenceMode::Durable;
        }
        recorder.flush().await.map_err(thread_store_io_error)?;
    }

    let recorded_path = match live_entry.as_ref() {
        Some((recorder, _history_mode)) => recorder.rollout_path().to_path_buf(),
        None => super::read_thread::resolve_rollout_path(
            store, thread_id, /*include_archived*/ true,
        )
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?,
    };
    let source_path = codex_rollout::existing_rollout_path(recorded_path.as_path())
        .await
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("thread {thread_id} does not have a readable rollout"),
        })?;
    let stable_path = codex_rollout::plain_rollout_path(recorded_path.as_path());
    let (source_meta, next_rollout_ordinal, existing_reference, source_lines, skipped_records) =
        validate_source_rollout(source_path.as_path(), thread_id).await?;
    let history_mode = source_meta.meta.history_mode;
    if let Some((_recorder, live_history_mode)) = live_entry.as_ref()
        && *live_history_mode != history_mode
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "live writer history mode does not match rollout metadata for thread {thread_id}"
            ),
        });
    }
    if params.is_snapshot()
        && let Some(reference) = existing_reference
    {
        let reference =
            stabilize_rollout_reference(store, reference, &mut HashSet::new(), /*depth*/ 0).await?;
        return Ok(FrozenRolloutSegment {
            reference,
            source_session_meta: source_meta,
            history_mode,
            next_rollout_ordinal,
        });
    }

    if params.is_snapshot() {
        let segment_id = snapshot_segment_id(source_lines.as_slice())?;
        let immutable_path = immutable_segment_path(
            store.config.codex_home.as_path(),
            thread_id,
            Some(segment_id),
            codex_rollout::plain_rollout_path(source_path.as_path()).as_path(),
        )?;
        install_snapshot_segment(
            source_lines.as_slice(),
            immutable_path.as_path(),
            Some(segment_id),
        )
        .await?;
        return Ok(FrozenRolloutSegment {
            reference: RolloutReferenceItem {
                rollout_path: immutable_path,
                thread_id: Some(thread_id),
                rollout_timestamp: rollout_timestamp_from_path(stable_path.as_path()),
                segment_id: Some(segment_id),
                max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            },
            source_session_meta: source_meta,
            history_mode,
            next_rollout_ordinal,
        });
    }

    if matches!(history_mode, ThreadHistoryMode::Legacy) && live_entry.is_some() {
        super::live_writer::restore_segmented_legacy_history_builder_if_needed(
            store,
            thread_id,
            source_path.as_path(),
        )
        .await?;
    }

    let legacy_projection_builder = if matches!(history_mode, ThreadHistoryMode::Legacy)
        && live_entry.is_some()
    {
        let builder = store
            .live_recorders
            .lock()
            .await
            .get(&thread_id)
            .filter(|entry| entry.legacy_history_projection_enabled)
            .map(|entry| Arc::clone(&entry.legacy_history_builder));
        if let Some(builder) = builder {
            let mut builder_guard = Arc::clone(&builder).lock_owned().await;
            let result = super::thread_history_materialization::materialize_legacy_to_sqlite(
                store,
                thread_id,
                source_path.as_path(),
                &mut builder_guard,
            )
            .await;
            if let Err(error) = result {
                builder_guard.reset();
                drop(builder_guard);
                super::live_writer::invalidate_segmented_legacy_projection(store, thread_id).await;
                return Err(error);
            }
            drop(builder_guard);
            Some(builder)
        } else {
            None
        }
    } else {
        if matches!(history_mode, ThreadHistoryMode::Paginated) && live_entry.is_some() {
            super::thread_history_materialization::materialize_to_sqlite(
                store,
                thread_id,
                source_path.as_path(),
            )
            .await?;
        }
        None
    };
    let legacy_next_projection_ordinal = if legacy_projection_builder.is_some() {
        Some(
            super::thread_history::projection_state(store, thread_id)
                .await?
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!(
                        "segmented legacy history projection for {thread_id} has no checkpoint"
                    ),
                })?
                .next_ordinal,
        )
    } else {
        None
    };

    let segment_id = source_meta.meta.segment_id;
    let immutable_path = immutable_segment_path(
        store.config.codex_home.as_path(),
        thread_id,
        segment_id,
        source_path.as_path(),
    )?;
    if skipped_records {
        install_snapshot_segment(
            source_lines.as_slice(),
            immutable_path.as_path(),
            segment_id,
        )
        .await?;
    } else {
        install_immutable_segment(source_path.as_path(), immutable_path.as_path()).await?;
    }

    let reference = RolloutReferenceItem {
        rollout_path: immutable_path,
        thread_id: Some(thread_id),
        rollout_timestamp: rollout_timestamp_from_path(stable_path.as_path()),
        segment_id,
        max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    let staged_path = staged_rollout_path(stable_path.as_path());
    let config = rollout_config(store, &source_meta.meta);
    let initial_rollout_ordinal = next_rollout_ordinal.unwrap_or(0);
    let staged_recorder = RolloutRecorder::new(
        &config,
        RolloutRecorderParams::CreateAtPath {
            path: staged_path.clone(),
            session_meta: Box::new(source_meta.meta.clone()),
            base_instructions: source_meta
                .meta
                .base_instructions
                .clone()
                .unwrap_or_default(),
            dynamic_tools: source_meta.meta.dynamic_tools.clone().unwrap_or_default(),
            initial_rollout_ordinal,
        },
    )
    .await
    .map_err(thread_store_io_error)?;
    let mut initial_items = Vec::with_capacity(params.initial_items().len() + 1);
    initial_items.push(RolloutItem::RolloutReference(reference.clone()));
    initial_items.extend_from_slice(params.initial_items());
    if let Err(err) = staged_recorder
        .record_canonical_items(initial_items.as_slice())
        .await
    {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    if let Err(err) = staged_recorder.flush().await {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    staged_recorder
        .shutdown()
        .await
        .map_err(thread_store_io_error)?;

    if let Some((recorder, _history_mode)) = live_entry.as_ref() {
        recorder.shutdown().await.map_err(thread_store_io_error)?;
    }
    if let Err(err) = replace_stable_rollout(staged_path.clone(), stable_path.clone()).await {
        if live_entry.is_some()
            && let Ok(recorder) =
                RolloutRecorder::new(&config, RolloutRecorderParams::resume(stable_path.clone()))
                    .await
            && let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id)
        {
            entry.recorder = recorder;
        }
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(ThreadStoreError::Internal {
            message: format!(
                "failed to atomically replace rollout {} with {}: {err}",
                stable_path.display(),
                staged_path.display()
            ),
        });
    }
    if source_path != stable_path {
        match fs::remove_file(source_path.as_path()).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(thread_store_io_error(err)),
        }
    }

    if live_entry.is_some() {
        let recorder =
            RolloutRecorder::new(&config, RolloutRecorderParams::resume(stable_path.clone()))
                .await
                .map_err(thread_store_io_error)?;
        let mut live_recorders = store.live_recorders.lock().await;
        let entry = live_recorders
            .get_mut(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        entry.recorder = recorder;
        entry.history_mode = history_mode;
        entry.persistence_mode = ThreadPersistenceMode::Durable;
    }

    super::live_writer::sync_materialized_rollout_path(store, thread_id, stable_path.as_path())
        .await?;

    if matches!(history_mode, ThreadHistoryMode::Paginated) {
        super::thread_history::reset_projection_for_replacement(
            store,
            thread_id,
            initial_rollout_ordinal,
        )
        .await?;
        super::thread_history_materialization::materialize_to_sqlite(
            store,
            thread_id,
            stable_path.as_path(),
        )
        .await?;
    } else if let (Some(builder), Some(next_projection_ordinal)) =
        (legacy_projection_builder, legacy_next_projection_ordinal)
    {
        super::thread_history::reset_projection_for_replacement(
            store,
            thread_id,
            next_projection_ordinal,
        )
        .await?;
        let mut builder = builder.lock_owned().await;
        let result = super::thread_history_materialization::materialize_legacy_to_sqlite(
            store,
            thread_id,
            stable_path.as_path(),
            &mut builder,
        )
        .await;
        if let Err(error) = result {
            builder.reset();
            drop(builder);
            super::live_writer::invalidate_segmented_legacy_projection(store, thread_id).await;
            return Err(error);
        }
    }

    Ok(FrozenRolloutSegment {
        reference,
        source_session_meta: source_meta,
        history_mode,
        next_rollout_ordinal,
    })
}

pub(super) async fn freeze_paginated_prefix_locked(
    store: &LocalThreadStore,
    source_thread_id: ThreadId,
    source_rollout_path: &Path,
    prefix_thread_id: ThreadId,
    prefix_rollout_path: &Path,
    end_ordinal_exclusive: u64,
    end_byte_offset: u64,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    let (source_session_meta, _, _, _, _) =
        validate_source_rollout(source_rollout_path, source_thread_id).await?;
    let history_mode = source_session_meta.meta.history_mode;
    if prefix_rollout_path != codex_rollout::plain_rollout_path(prefix_rollout_path) {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "prepared fork prefix {} was not materialized before freezing",
                prefix_rollout_path.display()
            ),
        });
    }
    let prefix_bytes = fs::read(prefix_rollout_path)
        .await
        .map_err(thread_store_io_error)?;
    let end_byte_offset =
        usize::try_from(end_byte_offset).map_err(|_| ThreadStoreError::Internal {
            message: format!("fork byte offset for {prefix_thread_id} exceeds addressable memory"),
        })?;
    let prefix =
        prefix_bytes
            .get(..end_byte_offset)
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: "fork boundary exceeds inherited source history".to_string(),
            })?;
    if !prefix.ends_with(b"\n") {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "fork boundary for {prefix_thread_id} is not a complete rollout record"
            ),
        });
    }
    let mut prefix_lines = prefix
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .map(|line| {
            serde_json::from_slice::<RolloutLine>(line).map_err(|err| ThreadStoreError::Internal {
                message: format!(
                    "failed to read prepared rollout prefix for {prefix_thread_id}: {err}"
                ),
            })
        })
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    match prefix_lines.first().map(|line| &line.item) {
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == prefix_thread_id => {}
        Some(RolloutItem::SessionMeta(_)) => {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "prepared rollout prefix {} does not belong to thread {prefix_thread_id}",
                    prefix_rollout_path.display()
                ),
            });
        }
        _ => {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "prepared rollout prefix {} does not start with session metadata",
                    prefix_rollout_path.display()
                ),
            });
        }
    }
    let actual_end_ordinal = prefix_lines
        .last()
        .and_then(|line| line.ordinal)
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "prepared rollout prefix for {prefix_thread_id} has no terminal ordinal"
            ),
        })?;
    if actual_end_ordinal != end_ordinal_exclusive {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "prepared rollout prefix for {prefix_thread_id} ended at ordinal \
                 {actual_end_ordinal}, expected {end_ordinal_exclusive}"
            ),
        });
    }
    for line in prefix_lines.iter_mut().skip(1) {
        let RolloutItem::RolloutReference(reference) = &mut line.item else {
            continue;
        };
        *reference = stabilize_rollout_reference(
            store,
            reference.clone(),
            &mut HashSet::new(),
            /*depth*/ 0,
        )
        .await?;
    }
    let segment_id = snapshot_segment_id(prefix_lines.as_slice())?;
    let immutable_path = immutable_segment_path(
        store.config.codex_home.as_path(),
        prefix_thread_id,
        Some(segment_id),
        prefix_rollout_path,
    )?;
    install_snapshot_segment(
        prefix_lines.as_slice(),
        immutable_path.as_path(),
        Some(segment_id),
    )
    .await?;
    Ok(FrozenRolloutSegment {
        reference: RolloutReferenceItem {
            rollout_path: immutable_path,
            thread_id: Some(prefix_thread_id),
            rollout_timestamp: rollout_timestamp_from_path(prefix_rollout_path),
            segment_id: Some(segment_id),
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
        source_session_meta,
        history_mode,
        next_rollout_ordinal: Some(end_ordinal_exclusive),
    })
}

/// Retains a referenced immutable snapshot while nested references are stabilized oldest first.
struct StabilizationFrame {
    reference: RolloutReferenceItem,
    thread_id: ThreadId,
    identity: (ThreadId, Option<SegmentId>),
    resolved_path: PathBuf,
    lines: Vec<RolloutLine>,
    next_line: usize,
    nested_reference_changed: bool,
    graph_depth: usize,
}

async fn stabilize_rollout_reference(
    store: &LocalThreadStore,
    reference: RolloutReferenceItem,
    active_references: &mut HashSet<(ThreadId, Option<SegmentId>)>,
    depth: usize,
) -> ThreadStoreResult<RolloutReferenceItem> {
    let mut inserted_references = Vec::new();
    let result = stabilize_rollout_reference_iteratively(
        store,
        reference,
        active_references,
        &mut inserted_references,
        depth,
    )
    .await;
    for identity in inserted_references {
        active_references.remove(&identity);
    }
    result
}

async fn stabilize_rollout_reference_iteratively(
    store: &LocalThreadStore,
    reference: RolloutReferenceItem,
    active_references: &mut HashSet<(ThreadId, Option<SegmentId>)>,
    inserted_references: &mut Vec<(ThreadId, Option<SegmentId>)>,
    depth: usize,
) -> ThreadStoreResult<RolloutReferenceItem> {
    let mut frames = vec![
        load_stabilization_frame(
            store,
            reference,
            active_references,
            inserted_references,
            depth,
        )
        .await?,
    ];

    while let Some(frame) = frames.last_mut() {
        let mut nested_reference = None;
        while let Some(line) = frame.lines.get(frame.next_line) {
            frame.next_line += 1;
            if let RolloutItem::RolloutReference(reference) = &line.item {
                nested_reference = Some(reference.clone());
                break;
            }
        }

        if let Some(nested_reference) = nested_reference {
            let referenced_thread_id =
                nested_reference
                    .thread_id
                    .ok_or_else(|| ThreadStoreError::Internal {
                        message: format!(
                            "rollout reference {} is missing thread_id",
                            nested_reference.rollout_path.display()
                        ),
                    })?;
            let fork_boundary = referenced_thread_id != frame.thread_id
                || nested_reference.nth_user_message.is_some();
            let graph_depth = frame.graph_depth + usize::from(fork_boundary);
            frames.push(
                load_stabilization_frame(
                    store,
                    nested_reference,
                    active_references,
                    inserted_references,
                    graph_depth,
                )
                .await?,
            );
            continue;
        }

        let Some(completed) = frames.pop() else {
            return Err(ThreadStoreError::Internal {
                message: "rollout reference stabilization stack is empty".to_string(),
            });
        };
        let stabilized = if !completed.nested_reference_changed
            && is_immutable_segment_path(
                &store.config.codex_home,
                completed.resolved_path.as_path(),
                completed.thread_id,
                completed.reference.segment_id,
            ) {
            completed.reference
        } else {
            let segment_id = snapshot_segment_id(completed.lines.as_slice())?;
            let immutable_path = immutable_segment_path(
                store.config.codex_home.as_path(),
                completed.thread_id,
                Some(segment_id),
                codex_rollout::plain_rollout_path(completed.resolved_path.as_path()).as_path(),
            )?;
            install_snapshot_segment(
                completed.lines.as_slice(),
                immutable_path.as_path(),
                Some(segment_id),
            )
            .await?;
            RolloutReferenceItem {
                rollout_path: immutable_path,
                thread_id: Some(completed.thread_id),
                rollout_timestamp: completed.reference.rollout_timestamp,
                segment_id: Some(segment_id),
                max_depth: completed.reference.max_depth,
                nth_user_message: completed.reference.nth_user_message,
                compacted_replacement_history_filter_texts: completed
                    .reference
                    .compacted_replacement_history_filter_texts,
            }
        };
        active_references.remove(&completed.identity);

        let Some(parent) = frames.last_mut() else {
            return Ok(stabilized);
        };
        let Some(RolloutItem::RolloutReference(parent_reference)) = parent
            .lines
            .get_mut(parent.next_line.saturating_sub(1))
            .map(|line| &mut line.item)
        else {
            return Err(ThreadStoreError::Internal {
                message: "stabilized rollout reference has no parent reference".to_string(),
            });
        };
        parent.nested_reference_changed |= !rollout_references_equal(parent_reference, &stabilized);
        *parent_reference = stabilized;
    }

    Err(ThreadStoreError::Internal {
        message: "rollout reference stabilization completed without a result".to_string(),
    })
}

async fn load_stabilization_frame(
    store: &LocalThreadStore,
    reference: RolloutReferenceItem,
    active_references: &mut HashSet<(ThreadId, Option<SegmentId>)>,
    inserted_references: &mut Vec<(ThreadId, Option<SegmentId>)>,
    graph_depth: usize,
) -> ThreadStoreResult<StabilizationFrame> {
    if graph_depth >= MAX_ROLLOUT_REFERENCE_DEPTH {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout reference graph exceeds maximum depth of {MAX_ROLLOUT_REFERENCE_DEPTH}"
            ),
        });
    }
    let thread_id = reference
        .thread_id
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout reference {} is missing thread_id",
                reference.rollout_path.display()
            ),
        })?;
    let identity = (thread_id, reference.segment_id);
    if !active_references.insert(identity) {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout reference cycle detected at {thread_id}/{}",
                reference
                    .segment_id
                    .map(|segment_id| segment_id.to_string())
                    .unwrap_or_else(|| "initial".to_string())
            ),
        });
    }
    inserted_references.push(identity);

    let resolved_path =
        codex_rollout::resolve_rollout_reference_path(&store.config.codex_home, &reference)
            .await
            .map_err(thread_store_io_error)?;
    let (lines, loaded_thread_id, parse_errors) =
        RolloutRecorder::load_rollout_lines(resolved_path.as_path())
            .await
            .map_err(thread_store_io_error)?;
    if parse_errors != 0 {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout {} contains {parse_errors} invalid record(s)",
                resolved_path.display()
            ),
        });
    }
    if loaded_thread_id != Some(thread_id) {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "rollout {} does not belong to thread {thread_id}",
                resolved_path.display()
            ),
        });
    }

    Ok(StabilizationFrame {
        reference,
        thread_id,
        identity,
        resolved_path,
        lines,
        next_line: 1,
        nested_reference_changed: false,
        graph_depth,
    })
}

fn is_immutable_segment_path(
    codex_home: &Path,
    path: &Path,
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
) -> bool {
    path.starts_with(
        codex_home
            .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(
                segment_id
                    .map(|segment_id| segment_id.to_string())
                    .unwrap_or_else(|| "initial".to_string()),
            ),
    )
}

fn rollout_references_equal(left: &RolloutReferenceItem, right: &RolloutReferenceItem) -> bool {
    left.rollout_path == right.rollout_path
        && left.thread_id == right.thread_id
        && left.rollout_timestamp == right.rollout_timestamp
        && left.segment_id == right.segment_id
        && left.max_depth == right.max_depth
        && left.nth_user_message == right.nth_user_message
        && left.compacted_replacement_history_filter_texts
            == right.compacted_replacement_history_filter_texts
}

async fn validate_source_rollout(
    path: &Path,
    thread_id: ThreadId,
) -> ThreadStoreResult<(
    SessionMetaLine,
    Option<u64>,
    Option<RolloutReferenceItem>,
    Vec<RolloutLine>,
    bool,
)> {
    let (lines, loaded_thread_id, parse_errors) = RolloutRecorder::load_rollout_lines(path)
        .await
        .map_err(thread_store_io_error)?;
    if loaded_thread_id != Some(thread_id) {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "rollout {} does not belong to thread {thread_id}",
                path.display()
            ),
        });
    }
    let source_meta = match lines.first().map(|line| &line.item) {
        Some(RolloutItem::SessionMeta(meta)) => meta.clone(),
        _ => {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "rollout {} does not start with session metadata",
                    path.display()
                ),
            });
        }
    };
    if parse_errors != 0 && source_meta.meta.history_mode != ThreadHistoryMode::Legacy {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout {} contains {parse_errors} invalid record(s)",
                path.display()
            ),
        });
    }
    let next_rollout_ordinal = validate_ordinals(lines.as_slice(), source_meta.meta.history_mode)?;
    let existing_reference = match lines.as_slice() {
        [
            RolloutLine {
                item: RolloutItem::SessionMeta(_),
                ..
            },
            RolloutLine {
                item: RolloutItem::RolloutReference(reference),
                ..
            },
        ] => Some(reference.clone()),
        _ => None,
    };
    Ok((
        source_meta,
        next_rollout_ordinal,
        existing_reference,
        lines,
        parse_errors != 0,
    ))
}

// Full-history forks freeze the parent's current rollout into an immutable segment and store a
// RolloutReference instead of copying inherited events. The content-derived ID lets unchanged
// snapshots share that segment while later parent appends produce a new fork boundary. This
// segmentation is what deduplicates fork history; it is not legacy compatibility machinery.
// The hash preimage omits SessionMeta.segment_id because the installed metadata stores the
// resulting ID; canonical object ordering makes the preimage stable across process reloads.
fn snapshot_segment_id(lines: &[RolloutLine]) -> ThreadStoreResult<SegmentId> {
    let mut hasher = Sha256::new();
    for encoded in canonical_snapshot_lines(lines, /*segment_id*/ None)? {
        hasher.update(encoded);
        hasher.update(b"\n");
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ok(SegmentId::from_bytes(bytes))
}

async fn install_snapshot_segment(
    lines: &[RolloutLine],
    destination: &Path,
    segment_id: Option<SegmentId>,
) -> ThreadStoreResult<()> {
    let parent = immutable_segment_parent(destination)?;
    fs::create_dir_all(parent.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let temporary_path = immutable_segment_temporary_path(destination);
    let write_result = async {
        let mut temporary_file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temporary_path.as_path())
            .await?;
        for encoded in canonical_snapshot_lines(lines, segment_id).map_err(io::Error::other)? {
            temporary_file.write_all(encoded.as_slice()).await?;
            temporary_file.write_all(b"\n").await?;
        }
        temporary_file.sync_all().await?;
        Ok::<(), io::Error>(())
    }
    .await;
    if let Err(err) = write_result {
        let _ = fs::remove_file(temporary_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    commit_immutable_segment(temporary_path, destination, parent).await
}

fn canonical_snapshot_lines(
    lines: &[RolloutLine],
    segment_id: Option<SegmentId>,
) -> ThreadStoreResult<Vec<Vec<u8>>> {
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| {
            let mut line = line.clone();
            if index == 0
                && let RolloutItem::SessionMeta(meta) = &mut line.item
            {
                meta.meta.segment_id = segment_id;
            }
            let value = serde_json::to_value(line).map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to serialize rollout snapshot: {err}"),
            })?;
            serde_json::to_vec(&canonicalize_json(&value)).map_err(|err| {
                ThreadStoreError::Internal {
                    message: format!("failed to encode canonical rollout snapshot: {err}"),
                }
            })
        })
        .collect()
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            let mut sorted = serde_json::Map::with_capacity(map.len());
            for (key, value) in entries {
                sorted.insert(key.clone(), canonicalize_json(value));
            }
            Value::Object(sorted)
        }
        _ => value.clone(),
    }
}

fn validate_ordinals(
    lines: &[RolloutLine],
    history_mode: ThreadHistoryMode,
) -> ThreadStoreResult<Option<u64>> {
    match history_mode {
        ThreadHistoryMode::Legacy => {
            if lines.iter().any(|line| line.ordinal.is_some()) {
                return Err(ThreadStoreError::Internal {
                    message: "legacy rollout contains a paginated ordinal".to_string(),
                });
            }
            Ok(None)
        }
        ThreadHistoryMode::Paginated => {
            let mut ordinals = lines.iter().map(|line| {
                line.ordinal.ok_or_else(|| ThreadStoreError::Internal {
                    message: "paginated rollout line is missing an ordinal".to_string(),
                })
            });
            let Some(first) = ordinals.next() else {
                return Err(ThreadStoreError::Internal {
                    message: "paginated rollout is empty".to_string(),
                });
            };
            let mut expected = first?;
            for ordinal in ordinals {
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| ThreadStoreError::Internal {
                        message: "paginated rollout ordinal overflow".to_string(),
                    })?;
                if ordinal? != expected {
                    return Err(ThreadStoreError::Internal {
                        message: format!("paginated rollout expected ordinal {expected}"),
                    });
                }
            }
            expected
                .checked_add(1)
                .map(Some)
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "paginated rollout ordinal overflow".to_string(),
                })
        }
    }
}

fn rollout_config(store: &LocalThreadStore, meta: &SessionMeta) -> RolloutConfig {
    RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite: store.config.sqlite.clone(),
        cwd: meta.cwd.clone(),
        model_provider_id: meta
            .model_provider
            .clone()
            .unwrap_or_else(|| store.config.default_model_provider_id.clone()),
        generate_memories: meta.memory_mode.as_deref() != Some("disabled"),
    }
}

fn immutable_segment_path(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    source_path: &Path,
) -> ThreadStoreResult<PathBuf> {
    let file_name = source_path
        .file_name()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout {} does not have a file name",
                source_path.display()
            ),
        })?;
    Ok(codex_home
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(
            segment_id
                .map(|segment_id| segment_id.to_string())
                .unwrap_or_else(|| "initial".to_string()),
        )
        .join(file_name))
}

async fn install_immutable_segment(source: &Path, destination: &Path) -> ThreadStoreResult<()> {
    let parent = immutable_segment_parent(destination)?;
    fs::create_dir_all(parent.as_path())
        .await
        .map_err(thread_store_io_error)?;
    // Copy and flush before installing the destination name. Linking the live source directly
    // would let later appends mutate a segment that references already treat as immutable.
    let temporary_path = immutable_segment_temporary_path(destination);
    let copy_result = async {
        let mut source_file = fs::File::open(source).await?;
        let mut temporary_file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temporary_path.as_path())
            .await?;
        tokio::io::copy(&mut source_file, &mut temporary_file).await?;
        temporary_file.sync_all().await
    }
    .await;
    if let Err(err) = copy_result {
        let _ = fs::remove_file(temporary_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    commit_immutable_segment(temporary_path, destination, parent).await
}

fn immutable_segment_parent(destination: &Path) -> ThreadStoreResult<PathBuf> {
    destination
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "immutable rollout segment {} does not have a parent",
                destination.display()
            ),
        })
}

fn immutable_segment_temporary_path(destination: &Path) -> PathBuf {
    let mut temporary_path = destination.as_os_str().to_os_string();
    temporary_path.push(format!(".install-{}.tmp", SegmentId::new()));
    PathBuf::from(temporary_path)
}

async fn commit_immutable_segment(
    temporary_path: PathBuf,
    destination: &Path,
    parent: PathBuf,
) -> ThreadStoreResult<()> {
    let result = async {
        match fs::hard_link(temporary_path.as_path(), destination).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                if !files_equal(temporary_path.as_path(), destination).await? {
                    // The existing segment may already be referenced. Never replace its contents
                    // while recovering an interrupted rotation; fail closed instead.
                    return Err(ThreadStoreError::Conflict {
                        message: format!(
                            "immutable rollout segment {} already exists with different contents",
                            destination.display()
                        ),
                    });
                }
            }
            Err(err) => {
                return Err(ThreadStoreError::Internal {
                    message: format!(
                        "failed to install immutable rollout segment {}: {err}",
                        destination.display()
                    ),
                });
            }
        }
        #[cfg(unix)]
        tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to join immutable segment directory sync: {err}"),
            })?
            .map_err(thread_store_io_error)?;
        #[cfg(not(unix))]
        let _ = parent;
        Ok(())
    }
    .await;
    let _ = fs::remove_file(temporary_path.as_path()).await;
    result
}

async fn files_equal(left: &Path, right: &Path) -> ThreadStoreResult<bool> {
    let left_len = fs::metadata(left)
        .await
        .map_err(thread_store_io_error)?
        .len();
    let right_len = fs::metadata(right)
        .await
        .map_err(thread_store_io_error)?
        .len();
    if left_len != right_len {
        return Ok(false);
    }
    let mut left = fs::File::open(left).await.map_err(thread_store_io_error)?;
    let mut right = fs::File::open(right).await.map_err(thread_store_io_error)?;
    let mut left_buffer = vec![0; 64 * 1024];
    let mut right_buffer = vec![0; 64 * 1024];
    loop {
        let left_count = left
            .read(left_buffer.as_mut_slice())
            .await
            .map_err(thread_store_io_error)?;
        let right_count = right
            .read(right_buffer.as_mut_slice())
            .await
            .map_err(thread_store_io_error)?;
        if left_count != right_count || left_buffer[..left_count] != right_buffer[..right_count] {
            return Ok(false);
        }
        if left_count == 0 {
            return Ok(true);
        }
    }
}

async fn replace_stable_rollout(staged: PathBuf, stable: PathBuf) -> io::Result<()> {
    let staged_for_write = staged.clone();
    let stable_for_write = stable.clone();
    tokio::task::spawn_blocking(move || {
        let contents = std::fs::read_to_string(staged_for_write.as_path())?;
        codex_utils_path::write_atomically(stable_for_write.as_path(), contents.as_str())
    })
    .await
    .map_err(|err| io::Error::other(format!("failed to join rollout replacement: {err}")))??;
    let _ = fs::remove_file(staged).await;
    Ok(())
}

fn staged_rollout_path(stable_path: &Path) -> PathBuf {
    let mut staged = stable_path.as_os_str().to_os_string();
    staged.push(format!(".staged-{}.tmp", SegmentId::new()));
    PathBuf::from(staged)
}

fn rollout_timestamp_from_path(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    let core = file_name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
    core.match_indices('-').rev().find_map(|(index, _)| {
        ThreadId::from_string(&core[index + 1..])
            .ok()
            .map(|_| core[..index].to_string())
    })
}

fn thread_store_io_error(err: io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "segment_tests.rs"]
mod tests;
