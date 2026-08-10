#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex as StdMutex;

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
use futures::FutureExt;
use serde_json::Value;
use sha2::Digest as _;
use sha2::Sha256;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use tokio::sync::Notify;
use tracing::warn;

use super::LocalThreadStore;
use crate::FreezeRolloutSegmentParams;
use crate::FrozenRolloutSegment;
use crate::ThreadPersistenceMode;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

mod checkpoint_persistence;

#[cfg(test)]
use checkpoint_persistence::CheckpointPersistenceFailurePoint;
#[cfg(test)]
use checkpoint_persistence::inject_checkpoint_persistence_failure;
pub(super) use checkpoint_persistence::persist_segment_checkpoint;
#[cfg(test)]
use checkpoint_persistence::take_checkpoint_persistence_failure;

pub(super) async fn freeze_thread_segment(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    if params.is_snapshot() {
        let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
        return freeze_thread_segment_locked(store, thread_id, params).await;
    }

    // Keep the pre-checkpoint Local API cancellation-safe for compatibility. Production
    // checkpoint owners use `persist_segment_checkpoint`, which can also recover a precommit
    // rotation failure through an atomic active-file replacement.
    let store_for_rotation = store.clone();
    let rotation = tokio::spawn(async move {
        let _live_writer_guard = store_for_rotation.live_writer_locks.lock(thread_id).await;
        let result = AssertUnwindSafe(freeze_thread_segment_locked_with_publication(
            &store_for_rotation,
            thread_id,
            params,
        ))
        .catch_unwind()
        .await;
        match result {
            Ok(Ok(result)) => match result.publication {
                FrozenSegmentPublication::ActiveRolloutReplaced(
                    StableRolloutPublication::Durable,
                ) => Ok(result.frozen),
                FrozenSegmentPublication::ActiveRolloutReplaced(
                    StableRolloutPublication::DurabilityUnknown { error },
                ) => {
                    store_for_rotation
                        .live_recorders
                        .lock()
                        .await
                        .remove(&thread_id);
                    Err(ThreadStoreError::Conflict {
                        message: format!(
                            "segment rotation for thread {thread_id} committed without a durability acknowledgement; restart before continuing: {error}"
                        ),
                    })
                }
                FrozenSegmentPublication::ActiveRolloutUnchanged => {
                    Err(ThreadStoreError::Internal {
                        message: "segment rotation left the active rollout unchanged".to_string(),
                    })
                }
            },
            Ok(Err(error)) => Err(error),
            Err(_) => {
                store_for_rotation
                    .live_recorders
                    .lock()
                    .await
                    .remove(&thread_id);
                Err(ThreadStoreError::Conflict {
                    message: format!(
                        "segment rotation for thread {thread_id} failed at an indeterminate commit point; restart before continuing"
                    ),
                })
            }
        }
    });
    match rotation.await {
        Ok(result) => result,
        Err(error) => {
            store.live_recorders.lock().await.remove(&thread_id);
            Err(ThreadStoreError::Conflict {
                message: format!(
                    "segment rotation task for thread {thread_id} failed at an indeterminate commit point; restart before continuing: {error}"
                ),
            })
        }
    }
}

/// Physical publication state produced while freezing a rollout segment.
enum FrozenSegmentPublication {
    /// A snapshot reused or installed immutable history without replacing the active rollout.
    ActiveRolloutUnchanged,
    /// Rotation replaced the active rollout and reported its publication durability.
    ActiveRolloutReplaced(StableRolloutPublication),
}

/// Internal freeze result retaining commit classification for checkpoint persistence.
struct FrozenRolloutSegmentResult {
    frozen: FrozenRolloutSegment,
    publication: FrozenSegmentPublication,
}

pub(super) async fn freeze_thread_segment_locked(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    Ok(
        freeze_thread_segment_locked_with_publication(store, thread_id, params)
            .await?
            .frozen,
    )
}

async fn freeze_thread_segment_locked_with_publication(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> ThreadStoreResult<FrozenRolloutSegmentResult> {
    params
        .validate()
        .map_err(|error| ThreadStoreError::InvalidRequest {
            message: error.to_string(),
        })?;
    let has_live_entry = store.live_recorders.lock().await.contains_key(&thread_id);
    let live_entry = if has_live_entry {
        let (recorder, history_mode, _persistence_mode) =
            super::live_writer::live_writer_parts(store, thread_id).await?;
        Some((recorder, history_mode))
    } else {
        None
    };
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
        return Ok(FrozenRolloutSegmentResult {
            frozen: FrozenRolloutSegment {
                reference,
                source_session_meta: source_meta,
                history_mode,
                next_rollout_ordinal,
            },
            publication: FrozenSegmentPublication::ActiveRolloutUnchanged,
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
        return Ok(FrozenRolloutSegmentResult {
            frozen: FrozenRolloutSegment {
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
            },
            publication: FrozenSegmentPublication::ActiveRolloutUnchanged,
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

    let source_segment_id = source_meta.meta.segment_id;
    let (segment_id, immutable_path) = select_rotation_immutable_identity(
        store.config.codex_home.as_path(),
        thread_id,
        source_path.as_path(),
        source_segment_id,
        skipped_records,
    )
    .await?;
    let pending_immutable_marker = prepare_rotation_immutable_install(
        store.config.codex_home.as_path(),
        immutable_path.as_path(),
        source_lines.as_slice(),
    )
    .await?;
    let immutable_install = if skipped_records || segment_id != source_segment_id {
        install_snapshot_segment_unchecked(
            source_lines.as_slice(),
            immutable_path.as_path(),
            segment_id,
        )
        .await
    } else {
        install_immutable_segment_unchecked(source_path.as_path(), immutable_path.as_path()).await
    };
    if let Err(error) = immutable_install {
        cleanup_unpublished_rotation_install(
            immutable_path.as_path(),
            pending_immutable_marker.as_deref(),
        )
        .await;
        return Err(error);
    }
    #[cfg(test)]
    if take_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterImmutableInstall,
    ) {
        cleanup_unpublished_rotation_install(
            immutable_path.as_path(),
            pending_immutable_marker.as_deref(),
        )
        .await;
        return Err(ThreadStoreError::Internal {
            message: "injected failure after immutable installation".to_string(),
        });
    }

    let reference = RolloutReferenceItem {
        rollout_path: immutable_path.clone(),
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
    let staged_recorder = match RolloutRecorder::new(
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
    {
        Ok(recorder) => recorder,
        Err(error) => {
            cleanup_unpublished_rotation_install(
                immutable_path.as_path(),
                pending_immutable_marker.as_deref(),
            )
            .await;
            return Err(thread_store_io_error(error));
        }
    };
    let mut initial_items = Vec::with_capacity(params.initial_items().len() + 1);
    initial_items.push(RolloutItem::RolloutReference(reference.clone()));
    initial_items.extend_from_slice(params.initial_items());
    if let Err(err) = staged_recorder
        .record_canonical_items(initial_items.as_slice())
        .await
    {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        cleanup_unpublished_rotation_install(
            immutable_path.as_path(),
            pending_immutable_marker.as_deref(),
        )
        .await;
        return Err(thread_store_io_error(err));
    }
    if let Err(err) = staged_recorder.flush().await {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        cleanup_unpublished_rotation_install(
            immutable_path.as_path(),
            pending_immutable_marker.as_deref(),
        )
        .await;
        return Err(thread_store_io_error(err));
    }
    if let Err(error) = staged_recorder.shutdown().await {
        let _ = fs::remove_file(staged_path.as_path()).await;
        cleanup_unpublished_rotation_install(
            immutable_path.as_path(),
            pending_immutable_marker.as_deref(),
        )
        .await;
        return Err(thread_store_io_error(error));
    }

    if let Some((recorder, _history_mode)) = live_entry.as_ref() {
        {
            let mut live_recorders = store.live_recorders.lock().await;
            let entry = live_recorders
                .get_mut(&thread_id)
                .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            entry.recovery = Some(super::LiveRecorderRecovery {
                config: config.clone(),
                rollout_path: stable_path.clone(),
            });
        }
        if let Err(error) = recorder.shutdown().await {
            let _ = fs::remove_file(staged_path.as_path()).await;
            cleanup_unpublished_rotation_install(
                immutable_path.as_path(),
                pending_immutable_marker.as_deref(),
            )
            .await;
            return Err(thread_store_io_error(error));
        }
    }
    let publication = match replace_stable_rollout(staged_path.clone(), stable_path.clone()).await {
        Ok(publication) => publication,
        Err(err) => {
            let _ = fs::remove_file(staged_path.as_path()).await;
            cleanup_unpublished_rotation_install(
                immutable_path.as_path(),
                pending_immutable_marker.as_deref(),
            )
            .await;
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to atomically replace rollout {} with {}: {err}",
                    stable_path.display(),
                    staged_path.display()
                ),
            });
        }
    };
    #[cfg(test)]
    let publication = if take_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::NominalDurabilityUnknown,
    ) {
        StableRolloutPublication::DurabilityUnknown {
            error: ThreadStoreError::Internal {
                message: "injected nominal rotation durability failure".to_string(),
            },
        }
    } else {
        publication
    };
    match (&publication, pending_immutable_marker) {
        (StableRolloutPublication::Durable, Some(marker_path)) => {
            if let Err(error) = remove_pending_immutable_marker(marker_path.as_path()).await {
                warn!(%thread_id, %error, "segment rotation committed but its immutable-install marker remains");
            }
        }
        (StableRolloutPublication::DurabilityUnknown { error }, Some(_)) => {
            warn!(%thread_id, %error, "segment rotation committed without a durability acknowledgement; retaining its immutable-install marker");
        }
        (StableRolloutPublication::DurabilityUnknown { error }, None) => {
            warn!(%thread_id, %error, "segment rotation committed without a durability acknowledgement");
        }
        (StableRolloutPublication::Durable, None) => {}
    }
    #[cfg(test)]
    pause_after_committed_replace(thread_id).await;
    if matches!(&publication, StableRolloutPublication::Durable) && source_path != stable_path {
        match fs::remove_file(source_path.as_path()).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                warn!(%err, "segment rotation committed but old compressed rollout remains")
            }
        }
    }

    if live_entry.is_some() {
        if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
            entry.history_mode = history_mode;
            entry.persistence_mode = ThreadPersistenceMode::Durable;
        }
        #[cfg(test)]
        let injected_reopen_failure = take_injected_reopen_failure(thread_id);
        #[cfg(not(test))]
        let injected_reopen_failure = false;
        let reopen_result = if injected_reopen_failure {
            Err(ThreadStoreError::Internal {
                message: "injected segment recorder reopen failure".to_string(),
            })
        } else {
            super::live_writer::live_writer_parts(store, thread_id)
                .await
                .map(|_| ())
        };
        if let Err(err) = reopen_result {
            warn!(%thread_id, %err, "segment rotation committed; live writer will reopen on its next operation");
        }
    }

    if let Err(err) =
        super::live_writer::sync_materialized_rollout_path(store, thread_id, stable_path.as_path())
            .await
    {
        warn!(%thread_id, %err, "segment rotation committed but rollout-path synchronization failed");
    }

    if matches!(history_mode, ThreadHistoryMode::Paginated) {
        let projection_result = async {
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
            .await
        }
        .await;
        if let Err(err) = projection_result {
            warn!(%thread_id, %err, "segment rotation committed but paginated projection repair failed");
        }
    } else if let (Some(builder), Some(next_projection_ordinal)) =
        (legacy_projection_builder, legacy_next_projection_ordinal)
    {
        let reset_result = super::thread_history::reset_projection_for_replacement(
            store,
            thread_id,
            next_projection_ordinal,
        )
        .await;
        let mut builder = builder.lock_owned().await;
        let result = match reset_result {
            Ok(()) => {
                super::thread_history_materialization::materialize_legacy_to_sqlite(
                    store,
                    thread_id,
                    stable_path.as_path(),
                    &mut builder,
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Err(err) = result {
            builder.reset();
            drop(builder);
            super::live_writer::invalidate_segmented_legacy_projection(store, thread_id).await;
            warn!(%thread_id, %err, "segment rotation committed but legacy projection repair failed");
        }
    }

    Ok(FrozenRolloutSegmentResult {
        frozen: FrozenRolloutSegment {
            reference,
            source_session_meta: source_meta,
            history_mode,
            next_rollout_ordinal,
        },
        publication: FrozenSegmentPublication::ActiveRolloutReplaced(publication),
    })
}

async fn cleanup_unpublished_rotation_install(destination: &Path, marker: Option<&Path>) {
    let Some(marker) = marker else {
        return;
    };
    if let Err(error) = remove_file_if_present(destination).await {
        warn!(%error, path = %destination.display(), "failed to remove unpublished immutable rollout segment");
    }
    if let Err(error) = remove_pending_immutable_marker(marker).await {
        warn!(%error, path = %marker.display(), "failed to remove unpublished immutable rollout marker");
    }
}

#[cfg(test)]
#[derive(Clone)]
struct CommittedReplacePause {
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

#[cfg(test)]
static COMMITTED_REPLACE_PAUSES: LazyLock<StdMutex<HashMap<ThreadId, CommittedReplacePause>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

#[cfg(test)]
static INJECTED_REOPEN_FAILURES: LazyLock<StdMutex<HashSet<ThreadId>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));

#[cfg(test)]
fn install_committed_replace_pause(thread_id: ThreadId) -> CommittedReplacePause {
    let pause = CommittedReplacePause {
        reached: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    COMMITTED_REPLACE_PAUSES
        .lock()
        .expect("committed replace pause mutex")
        .insert(thread_id, pause.clone());
    pause
}

#[cfg(test)]
async fn pause_after_committed_replace(thread_id: ThreadId) {
    let pause = COMMITTED_REPLACE_PAUSES
        .lock()
        .expect("committed replace pause mutex")
        .remove(&thread_id);
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.release.notified().await;
    }
}

#[cfg(test)]
fn inject_next_reopen_failure(thread_id: ThreadId) {
    INJECTED_REOPEN_FAILURES
        .lock()
        .expect("injected reopen failure mutex")
        .insert(thread_id);
}

#[cfg(test)]
fn take_injected_reopen_failure(thread_id: ThreadId) -> bool {
    INJECTED_REOPEN_FAILURES
        .lock()
        .expect("injected reopen failure mutex")
        .remove(&thread_id)
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
    reject_pending_rotation_marker(destination).await?;
    install_snapshot_segment_unchecked(lines, destination, segment_id).await
}

async fn install_snapshot_segment_unchecked(
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

#[cfg(test)]
async fn install_immutable_segment(source: &Path, destination: &Path) -> ThreadStoreResult<()> {
    reject_pending_rotation_marker(destination).await?;
    install_immutable_segment_unchecked(source, destination).await
}

async fn install_immutable_segment_unchecked(
    source: &Path,
    destination: &Path,
) -> ThreadStoreResult<()> {
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

fn pending_immutable_marker_path(destination: &Path) -> PathBuf {
    let mut marker = destination.as_os_str().to_os_string();
    marker.push(".rotation-pending");
    PathBuf::from(marker)
}

async fn reject_pending_rotation_marker(destination: &Path) -> ThreadStoreResult<()> {
    let marker = pending_immutable_marker_path(destination);
    if fs::try_exists(marker.as_path())
        .await
        .map_err(thread_store_io_error)?
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "immutable rollout segment {} has an unfinished rotation",
                destination.display()
            ),
        });
    }
    Ok(())
}

/// Claims an immutable destination before rotation publishes a reference to it.
///
/// A marker left by a crashed rotation proves that the destination was installed while the source
/// writer lock was held. Snapshot/fork installation rejects the marker, so an unreferenced marked
/// destination can be removed without changing another rollout's history.
async fn prepare_rotation_immutable_install(
    codex_home: &Path,
    destination: &Path,
    active_lines: &[RolloutLine],
) -> ThreadStoreResult<Option<PathBuf>> {
    let marker = pending_immutable_marker_path(destination);
    let marker_exists = fs::try_exists(marker.as_path())
        .await
        .map_err(thread_store_io_error)?;
    if marker_exists {
        if rollout_lines_reference_path(codex_home, active_lines, destination).await? {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "active rollout already references marked immutable segment {}",
                    destination.display()
                ),
            });
        }
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "immutable rollout segment {} has an unfinished rotation and cannot be reused",
                destination.display()
            ),
        });
    }
    if fs::try_exists(destination)
        .await
        .map_err(thread_store_io_error)?
    {
        return Ok(None);
    }

    let parent = immutable_segment_parent(destination)?;
    fs::create_dir_all(parent.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let mut marker_file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(marker.as_path())
        .await
        .map_err(thread_store_io_error)?;
    marker_file
        .write_all(b"rotation has not published this immutable segment\n")
        .await
        .map_err(thread_store_io_error)?;
    marker_file
        .sync_all()
        .await
        .map_err(thread_store_io_error)?;
    sync_directory(parent).await?;
    Ok(Some(marker))
}

/// Selects a fresh immutable identity instead of deleting an uncertain existing destination.
///
/// A marker or differing existing file can survive after another thread observed the immutable
/// path. Its reference is not discoverable from the current thread alone, so retry leaves that
/// destination untouched and rewrites the current source under a new segment identity.
async fn select_rotation_immutable_identity(
    codex_home: &Path,
    thread_id: ThreadId,
    source_path: &Path,
    preferred_segment_id: Option<SegmentId>,
    canonical_snapshot_required: bool,
) -> ThreadStoreResult<(Option<SegmentId>, PathBuf)> {
    let preferred_source_path = if canonical_snapshot_required {
        codex_rollout::plain_rollout_path(source_path)
    } else {
        source_path.to_path_buf()
    };
    let preferred = immutable_segment_path(
        codex_home,
        thread_id,
        preferred_segment_id,
        preferred_source_path.as_path(),
    )?;
    let preferred_marker = pending_immutable_marker_path(preferred.as_path());
    let marker_exists = fs::try_exists(preferred_marker.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let preferred_exists = fs::try_exists(preferred.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let preferred_matches_source = !canonical_snapshot_required
        && preferred_exists
        && files_equal(source_path, preferred.as_path()).await?;
    if !marker_exists && (!preferred_exists || preferred_matches_source) {
        return Ok((preferred_segment_id, preferred));
    }

    loop {
        let segment_id = SegmentId::new();
        // A fresh identity rewrites SessionMeta, so its immutable representation is canonical
        // JSON even when the active source is compressed.
        let snapshot_source_path = codex_rollout::plain_rollout_path(source_path);
        let destination = immutable_segment_path(
            codex_home,
            thread_id,
            Some(segment_id),
            snapshot_source_path.as_path(),
        )?;
        let marker = pending_immutable_marker_path(destination.as_path());
        let destination_exists = fs::try_exists(destination.as_path())
            .await
            .map_err(thread_store_io_error)?;
        let marker_exists = fs::try_exists(marker.as_path())
            .await
            .map_err(thread_store_io_error)?;
        if !destination_exists && !marker_exists {
            return Ok((Some(segment_id), destination));
        }
    }
}

async fn rollout_lines_reference_path(
    codex_home: &Path,
    lines: &[RolloutLine],
    destination: &Path,
) -> ThreadStoreResult<bool> {
    let destination_identity = codex_rollout::read_session_meta_line(destination)
        .await
        .ok()
        .map(|meta| (meta.meta.id, meta.meta.segment_id));
    let mut pending = lines
        .iter()
        .filter_map(|line| match &line.item {
            RolloutItem::RolloutReference(reference) => Some(reference.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut visited = HashSet::new();
    while let Some(reference) = pending.pop() {
        if rollout_paths_identical(reference.rollout_path.as_path(), destination).await?
            || destination_identity.is_some_and(|identity| {
                (reference.thread_id, reference.segment_id) == (Some(identity.0), identity.1)
            })
        {
            return Ok(true);
        }
        let referenced_thread_id =
            reference
                .thread_id
                .ok_or_else(|| ThreadStoreError::Conflict {
                    message: format!(
                        "rollout reference {} is missing thread_id",
                        reference.rollout_path.display()
                    ),
                })?;
        let resolved = codex_rollout::resolve_rollout_reference_path(codex_home, &reference)
            .await
            .map_err(thread_store_io_error)?;
        if rollout_paths_identical(resolved.as_path(), destination).await? {
            return Ok(true);
        }
        let canonical = fs::canonicalize(resolved.as_path())
            .await
            .unwrap_or(resolved.clone());
        if !visited.insert(canonical) {
            continue;
        }
        let (referenced_lines, loaded_thread_id, parse_errors) =
            RolloutRecorder::load_rollout_lines(resolved.as_path())
                .await
                .map_err(thread_store_io_error)?;
        if parse_errors != 0 || loaded_thread_id != Some(referenced_thread_id) {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "rollout reference {} did not resolve to a valid rollout for {referenced_thread_id}",
                    reference.rollout_path.display()
                ),
            });
        }
        pending.extend(
            referenced_lines
                .into_iter()
                .filter_map(|line| match line.item {
                    RolloutItem::RolloutReference(reference) => Some(reference),
                    _ => None,
                }),
        );
    }
    Ok(false)
}

async fn rollout_paths_identical(left: &Path, right: &Path) -> ThreadStoreResult<bool> {
    if left == right {
        return Ok(true);
    }
    let left = match fs::canonicalize(left).await {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(thread_store_io_error(error)),
    };
    let right = match fs::canonicalize(right).await {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(thread_store_io_error(error)),
    };
    Ok(left == right)
}

async fn remove_pending_immutable_marker(marker: &Path) -> ThreadStoreResult<()> {
    remove_file_if_present(marker).await?;
    if let Some(parent) = marker.parent() {
        sync_directory(parent.to_path_buf()).await?;
    }
    Ok(())
}

async fn remove_file_if_present(path: &Path) -> ThreadStoreResult<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(thread_store_io_error(error)),
    }
}

async fn sync_directory(parent: PathBuf) -> ThreadStoreResult<()> {
    #[cfg(unix)]
    tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!("failed to join immutable segment directory sync: {error}"),
        })?
        .map_err(thread_store_io_error)?;
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
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

/// Result of publishing a staged active rollout.
enum StableRolloutPublication {
    /// The replacement file and its directory were synchronized after the atomic rename.
    Durable,
    /// The replacement is visible, but storage did not acknowledge its durability.
    DurabilityUnknown { error: ThreadStoreError },
}

async fn replace_stable_rollout(
    staged: PathBuf,
    stable: PathBuf,
) -> io::Result<StableRolloutPublication> {
    let staged_for_write = staged.clone();
    let stable_for_write = stable.clone();
    let replace_result = tokio::task::spawn_blocking(move || {
        let contents = std::fs::read_to_string(staged_for_write.as_path())?;
        codex_utils_path::write_atomically(stable_for_write.as_path(), contents.as_str())
    })
    .await;
    match replace_result {
        Ok(result) => result?,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("rollout replacement task was cancelled: {error}"),
    }
    let _ = fs::remove_file(staged).await;
    let publication = match sync_stable_rollout_publication(stable.as_path()).await {
        Ok(()) => StableRolloutPublication::Durable,
        Err(error) => StableRolloutPublication::DurabilityUnknown { error },
    };
    Ok(publication)
}

async fn sync_stable_rollout_publication(stable: &Path) -> ThreadStoreResult<()> {
    fs::File::open(stable)
        .await
        .map_err(thread_store_io_error)?
        .sync_all()
        .await
        .map_err(thread_store_io_error)?;
    let parent =
        stable
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("rollout {} does not have a parent", stable.display()),
            })?;
    sync_directory(parent).await
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

#[cfg(test)]
#[path = "checkpoint_persistence_tests.rs"]
mod checkpoint_persistence_tests;
