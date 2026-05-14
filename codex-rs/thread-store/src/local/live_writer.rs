use std::path::Path;
use std::path::PathBuf;

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;
use codex_rollout::builder_from_items;
use codex_rollout::read_session_meta_line;
use tokio::fs;
#[cfg(test)]
use tokio::sync::Notify;
use tracing::warn;

use super::LocalThreadStore;
use super::create_thread;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::RotateThreadSegmentParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
#[derive(Clone)]
struct SegmentRotationCommitHook {
    thread_id: ThreadId,
    staged: Arc<Notify>,
    resume: Arc<Notify>,
}

#[cfg(test)]
static SEGMENT_ROTATION_COMMIT_HOOK: LazyLock<Mutex<Option<SegmentRotationCommitHook>>> =
    LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
pub(super) fn install_segment_rotation_commit_hook(
    thread_id: ThreadId,
    staged: Arc<Notify>,
    resume: Arc<Notify>,
) {
    let mut guard = SEGMENT_ROTATION_COMMIT_HOOK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(SegmentRotationCommitHook {
        thread_id,
        staged,
        resume,
    });
}

#[cfg(test)]
pub(super) fn clear_segment_rotation_commit_hook() {
    let mut guard = SEGMENT_ROTATION_COMMIT_HOOK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = None;
}

pub(super) async fn create_thread(
    store: &LocalThreadStore,
    params: CreateThreadParams,
) -> ThreadStoreResult<()> {
    let thread_id = params.thread_id;
    store.ensure_live_recorder_absent(thread_id).await?;
    let recorder = create_thread::create_thread(store, params).await?;
    store.insert_live_recorder(thread_id, recorder).await
}

pub(super) async fn resume_thread(
    store: &LocalThreadStore,
    params: ResumeThreadParams,
) -> ThreadStoreResult<()> {
    store.ensure_live_recorder_absent(params.thread_id).await?;
    let (rollout_path, history) = match (params.rollout_path, params.history) {
        (Some(rollout_path), history) => (rollout_path, history),
        (None, history) => {
            let thread = super::read_thread::read_thread(
                store,
                ReadThreadParams {
                    thread_id: params.thread_id,
                    include_archived: params.include_archived,
                    include_history: history.is_none(),
                },
            )
            .await?;
            let rollout_path = thread
                .rollout_path
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!("thread {} does not have a rollout path", params.thread_id),
                })?;
            (
                rollout_path,
                history.or_else(|| thread.history.map(|history| history.items)),
            )
        }
    };
    let state_builder = history
        .as_deref()
        .and_then(|items| builder_from_items(items, rollout_path.as_path()));
    let cwd = params
        .metadata
        .cwd
        .clone()
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: "local thread store requires a cwd".to_string(),
        })?;
    let config = RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite_home: store.config.sqlite_home.clone(),
        cwd,
        model_provider_id: params.metadata.model_provider.clone(),
        generate_memories: matches!(params.metadata.memory_mode, ThreadMemoryMode::Enabled),
    };
    let state_db_ctx = store.state_db().await;
    let recorder = RolloutRecorder::new(
        &config,
        RolloutRecorderParams::resume(
            rollout_path,
            create_thread::event_persistence_mode(params.event_persistence_mode),
        ),
        state_db_ctx,
        state_builder,
    )
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to resume local thread recorder: {err}"),
    })?;
    store.insert_live_recorder(params.thread_id, recorder).await
}

pub(super) async fn append_items(
    store: &LocalThreadStore,
    params: AppendThreadItemsParams,
) -> ThreadStoreResult<()> {
    store
        .live_recorder(params.thread_id)
        .await?
        .record_items(params.items.as_slice())
        .await
        .map_err(thread_store_io_error)
}

pub(super) async fn persist_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    store
        .live_recorder(thread_id)
        .await?
        .persist()
        .await
        .map_err(thread_store_io_error)
}

pub(super) async fn flush_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    store
        .live_recorder(thread_id)
        .await?
        .flush()
        .await
        .map_err(thread_store_io_error)
}

pub(super) async fn shutdown_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let recorder = store.live_recorder(thread_id).await?;
    recorder.shutdown().await.map_err(thread_store_io_error)?;
    store.live_recorders.lock().await.remove(&thread_id);
    Ok(())
}

pub(super) async fn discard_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    store
        .live_recorders
        .lock()
        .await
        .remove(&thread_id)
        .map(|_| ())
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })
}

pub(super) async fn rollout_path(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<PathBuf> {
    Ok(store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?
        .rollout_path()
        .to_path_buf())
}

pub(super) async fn rotate_thread_segment(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: RotateThreadSegmentParams,
) -> ThreadStoreResult<()> {
    let old_recorder = store.live_recorder(thread_id).await?;
    old_recorder.flush().await.map_err(thread_store_io_error)?;
    let old_rollout_path = old_recorder.rollout_path().to_path_buf();
    let old_meta = read_session_meta_line(old_rollout_path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read current rollout metadata from {}: {err}",
                old_rollout_path.display()
            ),
        })?;
    if old_meta.meta.id != thread_id {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "live rollout {} belongs to thread {} instead of {thread_id}",
                old_rollout_path.display(),
                old_meta.meta.id
            ),
        });
    }

    let cwd = params
        .metadata
        .cwd
        .clone()
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: "local thread store requires a cwd".to_string(),
        })?;
    let config = RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite_home: store.config.sqlite_home.clone(),
        cwd,
        model_provider_id: params.metadata.model_provider.clone(),
        generate_memories: matches!(params.metadata.memory_mode, ThreadMemoryMode::Enabled),
    };
    let event_persistence_mode = params.event_persistence_mode;
    if let Err(err) = old_recorder.shutdown().await {
        warn!(
            "failed to close previous rollout segment {} for thread {thread_id}: {err}",
            old_rollout_path.display()
        );
    }

    let current_path = store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?
        .rollout_path()
        .to_path_buf();
    if current_path != old_rollout_path {
        return Err(ThreadStoreError::Conflict {
            message: format!("live writer for thread {thread_id} changed during segment rotation"),
        });
    }

    let archived_path = archived_segment_path(
        store.config.codex_home.as_path(),
        thread_id,
        old_meta.meta.segment_id,
        old_rollout_path.as_path(),
    )?;
    fs::create_dir_all(
        archived_path
            .parent()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!(
                    "archived rollout segment path {} does not have a parent",
                    archived_path.display()
                ),
            })?,
    )
    .await
    .map_err(thread_store_io_error)?;
    fs::copy(old_rollout_path.as_path(), archived_path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to copy previous rollout segment {} to {}: {err}",
                old_rollout_path.display(),
                archived_path.display()
            ),
        })?;

    let mut initial_items = Vec::with_capacity(params.initial_items.len() + 1);
    initial_items.push(RolloutItem::RolloutReference(RolloutReferenceItem {
        rollout_path: archived_path.clone(),
        thread_id: Some(thread_id),
        rollout_timestamp: rollout_timestamp_from_path(old_rollout_path.as_path()),
        segment_id: old_meta.meta.segment_id,
        max_depth: params.previous_segment_reference_depth,
    }));
    initial_items.extend(params.initial_items);

    let staged_rollout_path = staged_rollout_path(old_rollout_path.as_path())?;
    let staged_recorder = match RolloutRecorder::new(
        &config,
        RolloutRecorderParams::CreateAtPath {
            path: staged_rollout_path.clone(),
            conversation_id: thread_id,
            forked_from_id: old_meta.meta.forked_from_id,
            source: params.source,
            base_instructions: params.base_instructions,
            dynamic_tools: params.dynamic_tools,
            session_timestamp: Some(old_meta.meta.timestamp.clone()),
            event_persistence_mode: create_thread::event_persistence_mode(event_persistence_mode),
        },
        /*state_db_ctx*/ None,
        /*state_builder*/ None,
    )
    .await
    {
        Ok(staged_recorder) => staged_recorder,
        Err(err) => {
            remove_rotation_artifacts(
                staged_rollout_path.as_path(),
                archived_path.as_path(),
                "staged recorder initialization",
            )
            .await;
            return Err(ThreadStoreError::Internal {
                message: format!("failed to initialize rotated local thread recorder: {err}"),
            });
        }
    };
    if let Err(err) = staged_recorder.record_items(initial_items.as_slice()).await {
        let _ = staged_recorder.shutdown().await;
        remove_rotation_artifacts(
            staged_rollout_path.as_path(),
            archived_path.as_path(),
            "staged recorder write",
        )
        .await;
        return Err(thread_store_io_error(err));
    }
    if let Err(err) = staged_recorder.flush().await {
        let _ = staged_recorder.shutdown().await;
        remove_rotation_artifacts(
            staged_rollout_path.as_path(),
            archived_path.as_path(),
            "staged recorder flush",
        )
        .await;
        return Err(thread_store_io_error(err));
    }
    if let Err(err) = staged_recorder.shutdown().await {
        remove_rotation_artifacts(
            staged_rollout_path.as_path(),
            archived_path.as_path(),
            "staged recorder shutdown",
        )
        .await;
        return Err(thread_store_io_error(err));
    }

    wait_for_segment_rotation_commit_hook(thread_id).await;

    if let Err(err) = replace_live_rollout_with_staged_segment(
        staged_rollout_path.as_path(),
        old_rollout_path.as_path(),
    )
    .await
    {
        remove_rotation_artifacts(
            staged_rollout_path.as_path(),
            archived_path.as_path(),
            "staged recorder install",
        )
        .await;
        return Err(err);
    }

    let state_db_ctx = store.state_db().await;
    let (committed_items, _, _) = RolloutRecorder::load_rollout_items(old_rollout_path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to load committed rotated rollout {}: {err}",
                old_rollout_path.display()
            ),
        })?;
    let state_builder = builder_from_items(committed_items.as_slice(), old_rollout_path.as_path());
    codex_rollout::state_db::reconcile_rollout(
        state_db_ctx.as_deref(),
        old_rollout_path.as_path(),
        config.model_provider_id.as_str(),
        state_builder.as_ref(),
        committed_items.as_slice(),
        Some(false),
        /*new_thread_memory_mode*/ None,
    )
    .await;
    let new_recorder = RolloutRecorder::new(
        &config,
        RolloutRecorderParams::resume(
            old_rollout_path.clone(),
            create_thread::event_persistence_mode(event_persistence_mode),
        ),
        state_db_ctx,
        state_builder,
    )
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to resume rotated local thread recorder: {err}"),
    })?;

    let mut live_recorders = store.live_recorders.lock().await;
    let current_path = live_recorders
        .get(&thread_id)
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?
        .rollout_path()
        .to_path_buf();
    if current_path != old_rollout_path {
        return Err(ThreadStoreError::Conflict {
            message: format!("live writer for thread {thread_id} changed during segment rotation"),
        });
    }
    live_recorders.insert(thread_id, new_recorder);
    Ok(())
}

fn archived_segment_path(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    old_rollout_path: &Path,
) -> ThreadStoreResult<PathBuf> {
    let old_file_name = old_rollout_path
        .file_name()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "previous rollout segment path {} does not have a file name",
                old_rollout_path.display()
            ),
        })?;
    let archived_root = codex_home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    Ok(match segment_id {
        Some(segment_id) => archived_root
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join(old_file_name),
        None => archived_root.join(old_file_name),
    })
}

fn staged_rollout_path(live_rollout_path: &Path) -> ThreadStoreResult<PathBuf> {
    let file_name = live_rollout_path
        .file_name()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "live rollout path {} does not have a file name",
                live_rollout_path.display()
            ),
        })?;
    let mut staged_file_name = file_name.to_os_string();
    staged_file_name.push(format!(".staged-{}.tmp", SegmentId::new()));
    Ok(live_rollout_path.with_file_name(staged_file_name))
}

async fn replace_live_rollout_with_staged_segment(
    staged_rollout_path: &Path,
    live_rollout_path: &Path,
) -> ThreadStoreResult<()> {
    match fs::rename(staged_rollout_path, live_rollout_path).await {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            fs::copy(staged_rollout_path, live_rollout_path)
                .await
                .map_err(|copy_err| ThreadStoreError::Internal {
                    message: format!(
                        "failed to replace live rollout {} from staged rollout {}: rename failed: {rename_err}; copy failed: {copy_err}",
                        live_rollout_path.display(),
                        staged_rollout_path.display()
                    ),
                })?;
            fs::remove_file(staged_rollout_path)
                .await
                .map_err(|remove_err| ThreadStoreError::Internal {
                    message: format!(
                        "failed to remove staged rollout {} after copying it to {}: {remove_err}",
                        staged_rollout_path.display(),
                        live_rollout_path.display()
                    ),
                })?;
            Ok(())
        }
    }
}

async fn remove_rotation_artifacts(staged_rollout_path: &Path, archived_path: &Path, stage: &str) {
    for path in [staged_rollout_path, archived_path] {
        if fs::try_exists(path).await.unwrap_or(false)
            && let Err(err) = fs::remove_file(path).await
        {
            warn!(
                "failed to remove rollout rotation artifact {} after {stage}: {err}",
                path.display()
            );
        }
    }
}

#[cfg(test)]
async fn wait_for_segment_rotation_commit_hook(thread_id: ThreadId) {
    let hook = {
        let guard = SEGMENT_ROTATION_COMMIT_HOOK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.clone()
    };
    if let Some(hook) = hook
        && hook.thread_id == thread_id
    {
        hook.staged.notify_one();
        hook.resume.notified().await;
    }
}

#[cfg(not(test))]
async fn wait_for_segment_rotation_commit_hook(_thread_id: ThreadId) {}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

fn rollout_timestamp_from_path(path: &std::path::Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    let core = file_name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
    core.match_indices('-').rev().find_map(|(index, _)| {
        ThreadId::from_string(&core[index + 1..])
            .ok()
            .map(|_| core[..index].to_string())
    })
}
