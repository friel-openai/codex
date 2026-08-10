#[cfg(test)]
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::path::Path;
#[cfg(test)]
use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex as StdMutex;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;
use futures::FutureExt;
use tokio::fs;
use tracing::warn;

use super::FrozenSegmentPublication;
use super::StableRolloutPublication;
use super::freeze_thread_segment_locked_with_publication;
use super::replace_stable_rollout;
use super::rollout_config;
use super::staged_rollout_path;
use super::thread_store_io_error;
use crate::FreezeRolloutSegmentParams;
use crate::SegmentCheckpointPersistenceOutcome;
use crate::ThreadPersistenceMode;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::local::LiveRecorderRecovery;
use crate::local::LocalThreadStore;

/// Persists one checkpoint by rotation or an atomic active-file replacement.
///
/// The spawned owner preserves the operation if its caller is cancelled. A task failure is
/// indeterminate because it may have happened immediately after the atomic replacement.
pub(in crate::local) async fn persist_segment_checkpoint(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> SegmentCheckpointPersistenceOutcome {
    let store_for_persistence = store.clone();
    match tokio::spawn(async move {
        let _live_writer_guard = store_for_persistence
            .live_writer_locks
            .lock(thread_id)
            .await;
        let result = AssertUnwindSafe(persist_segment_checkpoint_locked(
            &store_for_persistence,
            thread_id,
            params,
        ))
        .catch_unwind()
        .await;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(_) => SegmentCheckpointPersistenceOutcome::Indeterminate {
                error: ThreadStoreError::Internal {
                    message: format!(
                        "checkpoint persistence task for {thread_id} panicked at an indeterminate commit point"
                    ),
                },
            },
        };
        if matches!(
            &outcome,
            SegmentCheckpointPersistenceOutcome::Indeterminate { .. }
        ) {
            store_for_persistence
                .live_recorders
                .lock()
                .await
                .remove(&thread_id);
        }
        outcome
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            store.live_recorders.lock().await.remove(&thread_id);
            SegmentCheckpointPersistenceOutcome::Indeterminate {
                error: ThreadStoreError::Internal {
                    message: format!("checkpoint persistence task for {thread_id} failed: {error}"),
                },
            }
        }
    }
}

async fn persist_segment_checkpoint_locked(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> SegmentCheckpointPersistenceOutcome {
    if !store.live_recorders.lock().await.contains_key(&thread_id) {
        return SegmentCheckpointPersistenceOutcome::NotCommitted {
            error: ThreadStoreError::ThreadNotFound { thread_id },
        };
    }
    if params.is_snapshot() {
        return SegmentCheckpointPersistenceOutcome::NotCommitted {
            error: ThreadStoreError::InvalidRequest {
                message: "a segment-state checkpoint must rotate or replace the active rollout"
                    .to_string(),
            },
        };
    }
    if let Err(error) = params.validate() {
        return SegmentCheckpointPersistenceOutcome::NotCommitted {
            error: ThreadStoreError::InvalidRequest {
                message: error.to_string(),
            },
        };
    }

    match freeze_thread_segment_locked_with_publication(store, thread_id, params.clone()).await {
        Ok(result) => match result.publication {
            FrozenSegmentPublication::ActiveRolloutReplaced(StableRolloutPublication::Durable) => {
                SegmentCheckpointPersistenceOutcome::Committed
            }
            FrozenSegmentPublication::ActiveRolloutReplaced(
                StableRolloutPublication::DurabilityUnknown { error },
            ) => SegmentCheckpointPersistenceOutcome::Indeterminate { error },
            FrozenSegmentPublication::ActiveRolloutUnchanged => {
                SegmentCheckpointPersistenceOutcome::NotCommitted {
                    error: ThreadStoreError::Internal {
                        message: "checkpoint rotation did not replace the active rollout"
                            .to_string(),
                    },
                }
            }
        },
        Err(rotation_error) => {
            warn!(%thread_id, %rotation_error, "segment rotation failed before commit; replacing the active rollout with an atomic checkpoint append");
            match append_checkpoint_atomically_locked(store, thread_id, &params).await {
                Ok(AtomicCheckpointAppend::Committed) => {
                    SegmentCheckpointPersistenceOutcome::Committed
                }
                Ok(AtomicCheckpointAppend::Indeterminate { error }) => {
                    SegmentCheckpointPersistenceOutcome::Indeterminate {
                        error: ThreadStoreError::Internal {
                            message: format!(
                                "segment rotation failed: {rotation_error}; atomic checkpoint append committed but durability is indeterminate: {error}"
                            ),
                        },
                    }
                }
                Err(append_error) => SegmentCheckpointPersistenceOutcome::NotCommitted {
                    error: ThreadStoreError::Internal {
                        message: format!(
                            "segment rotation failed: {rotation_error}; atomic checkpoint append failed without changing the active rollout: {append_error}"
                        ),
                    },
                },
            }
        }
    }
}

/// Publication result for an atomic checkpoint append.
///
/// An indeterminate result means the active-file replacement is visible but its durability could
/// not be acknowledged. An error means the active rollout was not replaced and no checkpoint item
/// remains queued in the live recorder.
enum AtomicCheckpointAppend {
    /// The complete checkpoint batch replaced the active rollout durably.
    Committed,
    /// The active rollout was replaced, but storage did not acknowledge its durability.
    Indeterminate { error: ThreadStoreError },
}

async fn append_checkpoint_atomically_locked(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: &FreezeRolloutSegmentParams,
) -> ThreadStoreResult<AtomicCheckpointAppend> {
    let (recorder, history_mode, _persistence_mode) =
        crate::local::live_writer::live_writer_parts(store, thread_id).await?;
    recorder.persist().await.map_err(thread_store_io_error)?;
    {
        let mut live_recorders = store.live_recorders.lock().await;
        let entry = live_recorders
            .get_mut(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        entry.persistence_mode = ThreadPersistenceMode::Durable;
    }
    recorder.flush().await.map_err(thread_store_io_error)?;

    let stable_path = codex_rollout::plain_rollout_path(recorder.rollout_path());
    let source_path = codex_rollout::existing_rollout_path(stable_path.as_path())
        .await
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("thread {thread_id} does not have a readable active rollout"),
        })?;
    if source_path != stable_path {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "atomic checkpoint append requires a materialized active rollout for thread {thread_id}"
            ),
        });
    }

    let staged_path = staged_rollout_path(stable_path.as_path());
    if let Err(error) = copy_active_rollout(source_path.as_path(), staged_path.as_path()).await {
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(error);
    }
    let staged_meta = match codex_rollout::read_session_meta_line(staged_path.as_path()).await {
        Ok(meta) => meta,
        Err(error) => {
            let _ = fs::remove_file(staged_path.as_path()).await;
            return Err(thread_store_io_error(error));
        }
    };
    let config = rollout_config(store, &staged_meta.meta);
    let staged_recorder =
        match RolloutRecorder::new(&config, RolloutRecorderParams::resume(staged_path.clone()))
            .await
        {
            Ok(recorder) => recorder,
            Err(error) => {
                let _ = fs::remove_file(staged_path.as_path()).await;
                return Err(thread_store_io_error(error));
            }
        };
    let items = codex_rollout::persisted_rollout_items(params.initial_items(), history_mode);
    let stage_result = async {
        #[cfg(test)]
        if take_checkpoint_persistence_failure(
            thread_id,
            CheckpointPersistenceFailurePoint::AfterFirstStagedRecord,
        ) {
            let first = items
                .first()
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: "first-record failure requires a nonempty checkpoint".to_string(),
                })?;
            staged_recorder
                .record_canonical_items(std::slice::from_ref(first))
                .await
                .map_err(thread_store_io_error)?;
            staged_recorder
                .flush()
                .await
                .map_err(thread_store_io_error)?;
            return Err(ThreadStoreError::Internal {
                message: "injected failure after the first staged checkpoint record".to_string(),
            });
        }
        staged_recorder
            .record_canonical_items(items.as_slice())
            .await
            .map_err(thread_store_io_error)?;
        staged_recorder
            .flush()
            .await
            .map_err(thread_store_io_error)?;
        staged_recorder
            .shutdown()
            .await
            .map_err(thread_store_io_error)
    }
    .await;
    if let Err(error) = stage_result {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(error);
    }
    #[cfg(test)]
    if take_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::AfterCompleteStagedBatch,
    ) {
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(ThreadStoreError::Internal {
            message: "injected failure after the complete staged checkpoint batch".to_string(),
        });
    }

    {
        let mut live_recorders = store.live_recorders.lock().await;
        let entry = live_recorders
            .get_mut(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        entry.recovery = Some(LiveRecorderRecovery {
            config: config.clone(),
            rollout_path: stable_path.clone(),
        });
    }
    if let Err(error) = recorder.shutdown().await {
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(thread_store_io_error(error));
    }
    let publication = match replace_stable_rollout(staged_path.clone(), stable_path.clone()).await {
        Ok(publication) => publication,
        Err(error) => {
            let _ = fs::remove_file(staged_path.as_path()).await;
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to atomically replace rollout {} with staged checkpoint {}: {error}",
                    stable_path.display(),
                    staged_path.display()
                ),
            });
        }
    };
    #[cfg(test)]
    let publication = if take_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::FallbackDurabilityUnknown,
    ) {
        StableRolloutPublication::DurabilityUnknown {
            error: ThreadStoreError::Internal {
                message: "injected fallback checkpoint durability failure".to_string(),
            },
        }
    } else {
        publication
    };
    #[cfg(test)]
    if take_checkpoint_persistence_failure(
        thread_id,
        CheckpointPersistenceFailurePoint::PanicAfterAtomicReplace,
    ) {
        panic!("injected panic after atomic checkpoint replacement");
    }
    if let StableRolloutPublication::DurabilityUnknown { error } = publication {
        return Ok(AtomicCheckpointAppend::Indeterminate { error });
    }

    if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
        entry.history_mode = history_mode;
        entry.persistence_mode = ThreadPersistenceMode::Durable;
    }
    if let Err(error) = crate::local::live_writer::live_writer_parts(store, thread_id).await {
        warn!(%thread_id, %error, "checkpoint append committed; live writer will reopen on its next operation");
    }
    if let Err(error) = crate::local::live_writer::sync_materialized_rollout_path(
        store,
        thread_id,
        stable_path.as_path(),
    )
    .await
    {
        warn!(%thread_id, %error, "checkpoint append committed but rollout-path synchronization failed");
    }
    match history_mode {
        ThreadHistoryMode::Paginated => {
            if let Err(error) = crate::local::thread_history_materialization::materialize_to_sqlite(
                store,
                thread_id,
                stable_path.as_path(),
            )
            .await
            {
                warn!(%thread_id, %error, "checkpoint append committed but paginated projection repair failed");
            }
        }
        ThreadHistoryMode::Legacy => {
            if let Ok((reopened, _, _)) =
                crate::local::live_writer::live_writer_parts(store, thread_id).await
                && let Err(error) = crate::local::live_writer::project_segmented_legacy_rollout(
                    store, thread_id, &reopened,
                )
                .await
            {
                warn!(%thread_id, %error, "checkpoint append committed but legacy projection repair failed");
            }
        }
    }
    Ok(AtomicCheckpointAppend::Committed)
}

async fn copy_active_rollout(source_path: &Path, staged_path: &Path) -> ThreadStoreResult<()> {
    let mut source = fs::File::open(source_path)
        .await
        .map_err(thread_store_io_error)?;
    let mut staged = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(staged_path)
        .await
        .map_err(thread_store_io_error)?;
    tokio::io::copy(&mut source, &mut staged)
        .await
        .map_err(thread_store_io_error)?;
    staged.sync_all().await.map_err(thread_store_io_error)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(super) enum CheckpointPersistenceFailurePoint {
    AfterImmutableInstall,
    AfterFirstStagedRecord,
    AfterCompleteStagedBatch,
    NominalDurabilityUnknown,
    FallbackDurabilityUnknown,
    PanicAfterAtomicReplace,
}

#[cfg(test)]
static CHECKPOINT_PERSISTENCE_FAILURES: LazyLock<
    StdMutex<HashSet<(ThreadId, CheckpointPersistenceFailurePoint)>>,
> = LazyLock::new(|| StdMutex::new(HashSet::new()));

#[cfg(test)]
pub(super) fn inject_checkpoint_persistence_failure(
    thread_id: ThreadId,
    point: CheckpointPersistenceFailurePoint,
) {
    CHECKPOINT_PERSISTENCE_FAILURES
        .lock()
        .expect("checkpoint persistence failure mutex")
        .insert((thread_id, point));
}

#[cfg(test)]
pub(super) fn take_checkpoint_persistence_failure(
    thread_id: ThreadId,
    point: CheckpointPersistenceFailurePoint,
) -> bool {
    CHECKPOINT_PERSISTENCE_FAILURES
        .lock()
        .expect("checkpoint persistence failure mutex")
        .remove(&(thread_id, point))
}
