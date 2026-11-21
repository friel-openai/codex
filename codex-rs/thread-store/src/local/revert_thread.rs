use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;

use super::LocalThreadStore;
use super::paginated_fork;
use super::thread_rollout_resolver;
use crate::ForkBoundary;
use crate::RevertThreadParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
#[derive(Default)]
pub(super) struct RevertPublicationPause {
    pub(super) persisted: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Notify,
    pub(super) completed: tokio::sync::Notify,
}

#[cfg(test)]
pub(super) static REVERT_PUBLICATION_PAUSES: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<ThreadId, std::sync::Arc<RevertPublicationPause>>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Revert an unloaded paginated thread by creating a new immutable rollout file.
///
/// Old rollouts stay intact. The new file references the retained prefix, and the only mutable
/// cutover is the existing SQLite rollout-path pointer for the thread.
pub(super) async fn revert(
    store: &LocalThreadStore,
    params: RevertThreadParams,
) -> ThreadStoreResult<()> {
    let RevertThreadParams {
        thread_id,
        before_turn_id,
        multi_agent_version,
    } = params;
    let state_db = store
        .state_db()
        .await
        .ok_or(ThreadStoreError::Unsupported {
            operation: "revert_thread",
        })?;
    let selected = thread_rollout_resolver::resolve_current(store, thread_id).await?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    let mut history_access = super::goal_supervisor_runtime_repair::repair_compatibility_history_before_access(
        store, thread_id, &selected.path,
    ).await?;
    store.ensure_live_recorder_absent(thread_id).await?;
    let lineage = store.resolve_rollout_lineage(thread_id).await?;
    if lineage.has_exclusions() {
        return Err(ThreadStoreError::Conflict {
            message: "history repair cannot bind an alternate revert lineage to its selected graph".to_string(),
        });
    }
    let reservation = history_access.writer_reservation().ok_or_else(|| ThreadStoreError::Conflict {
        message: "revert history repair did not retain writer ownership".to_string(),
    })?;
    if lineage.segments().iter().any(|segment| !reservation.contains(segment.thread_id())) {
        return Err(ThreadStoreError::Conflict { message: "revert lineage expanded outside history repair ownership".to_string() });
    }
    let writer_lock = history_access.take_writer_lock(thread_id).ok_or_else(|| ThreadStoreError::Conflict {
        message: format!("thread {thread_id} already has an active writer"),
    })?;

    // Resolution may return a compressed sibling. Keep SQLite's exact stored path for the CAS.
    let stored_metadata = state_db
        .get_thread(thread_id)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read thread metadata for {thread_id}: {err}"),
        })?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    let expected_sqlite_path = stored_metadata.rollout_path;
    let current_rollout = thread_rollout_resolver::resolve_current(store, thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    if current_rollout.rollout_id != selected.rollout_id || current_rollout.path != selected.path
        || current_rollout.rollout_id != lineage.root_rollout_id() || current_rollout.path.as_path() != lineage.leaf_path() {
        return Err(ThreadStoreError::Conflict { message: format!("thread {thread_id} changed while its writers were reserved") });
    }
    let source_path = current_rollout.path;
    let mut source_meta = codex_rollout::read_session_meta_line_exact(source_path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read current paginated rollout {}: {err}",
                source_path.display()
            ),
        })?
        .meta;
    if source_meta.id != current_rollout.thread_id {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("current rollout for {thread_id} belongs to another thread"),
        });
    }
    if source_meta.history_mode != ThreadHistoryMode::Paginated {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} does not use paginated history"),
        });
    }

    // Older binaries can omit creator fields when replacing a rollout during revert.
    source_meta.creator_user_id = source_meta
        .creator_user_id
        .or(stored_metadata.creator_user_id);
    source_meta.creator_account_id = source_meta
        .creator_account_id
        .or(stored_metadata.creator_account_id);

    // Preserve old-reader compatibility when introducing the first reference to a standalone
    // source. Already-shared ancestors stay read-only; their offsets address decoded JSONL bytes.
    let boundary = &ForkBoundary::BeforeTurn(before_turn_id);
    let standalone_rollout_id = source_meta
        .history_base
        .is_none()
        .then_some(current_rollout.rollout_id);
    super::thread_history_materialization::ensure_fallback_projection_compatible(store, &lineage).await?;
    // History repair covered the selected graph, not a later independently resolved copy.
    lineage.reads.prevent_retry();
    let reservation = history_access.writer_reservation().expect("validated history repair ownership");
    let history_base = lineage
        .read_with_fallback(store, |mut lineage| async move {
            if lineage.segments().iter().any(|segment| !reservation.contains(segment.thread_id())) {
                return Err(ThreadStoreError::Conflict { message: "revert lineage expanded outside the reserved writers".to_string() });
            }
            super::thread_history_materialization::ensure_fallback_projection_compatible(store, &lineage).await?;
            for segment in &mut lineage.segments {
                if Some(segment.rollout_id()) == standalone_rollout_id {
                    segment.rollout_path = codex_rollout::materialize_rollout_for_reference(
                        segment.rollout_path.as_path(),
                    )
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!(
                            "failed to materialize rollout {} for revert: {err}",
                            segment.rollout_path.display()
                        ),
                    })?;
                }
                super::thread_history_materialization::materialize_to_sqlite_with_reads(
                    store,
                    segment.rollout_id(),
                    segment.rollout_path.as_path(),
                    &lineage.reads,
                )
                .await?;
            }
            paginated_fork::history_base_at_boundary(store, thread_id, boundary, &lineage).await
        })
        .await?;

    let forked_from_ordinal_exclusive =
        codex_rollout::forked_from_ordinal_exclusive(&source_meta, Some(source_path.as_path()))
            .map(|cutoff| {
                // Reverting into inherited history can shrink, but never grow, the parent prefix.
                cutoff.min(history_base.map_or(0, |base| base.end_ordinal_exclusive))
            });

    source_meta.multi_agent_version = multi_agent_version.or(source_meta.multi_agent_version);
    let rollout_id = ThreadId::new();
    // Publication keeps every reservation until the SQLite cutover completes, even if the
    // request is cancelled after the replacement file is created.
    let publication_store = store.clone();
    tokio::spawn(async move {
        let _history_access = history_access;
        let recorder = create_replacement_recorder(
            &publication_store, source_meta, rollout_id, history_base,
            forked_from_ordinal_exclusive, writer_lock,
        ).await?;
        let replacement_path = recorder.rollout_path().to_path_buf();
        if let Err(error) = recorder.persist().await {
            let _ = recorder.shutdown().await;
            let _ = tokio::fs::remove_file(&replacement_path).await;
            return Err(thread_store_io_error(error));
        }
        if let Err(error) = recorder.shutdown().await {
            let _ = tokio::fs::remove_file(&replacement_path).await;
            return Err(thread_store_io_error(error));
        }
        #[cfg(test)]
        let pause = REVERT_PUBLICATION_PAUSES.lock().expect("revert pause mutex").remove(&thread_id);
        #[cfg(test)]
        if let Some(pause) = &pause {
            pause.persisted.notify_one();
            pause.resume.notified().await;
        }
        let replaced = state_db.replace_rollout_path_if_current(
            thread_id, expected_sqlite_path.as_path(), replacement_path.as_path(),
        ).await.map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to switch thread {thread_id} to reverted rollout: {err}"),
        })?;
        if !replaced {
            let _ = tokio::fs::remove_file(&replacement_path).await;
            return Err(ThreadStoreError::Conflict {
                message: format!("thread {thread_id} changed while it was being reverted"),
            });
        }
        #[cfg(test)]
        if let Some(pause) = pause { pause.completed.notify_one(); }
        Ok(())
    }).await.map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join revert publication for {thread_id}: {err}"),
    })?
}

async fn create_replacement_recorder(
    store: &LocalThreadStore,
    source_meta: codex_rollout::SessionMeta,
    rollout_id: ThreadId,
    history_base: Option<codex_protocol::protocol::HistoryPosition>,
    forked_from_ordinal_exclusive: Option<u64>,
    writer_lock: super::WriterLockGuard,
) -> ThreadStoreResult<RolloutRecorder> {
    let config = RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite: store.config.sqlite.clone(),
        cwd: source_meta.cwd.clone(),
        model_provider_id: source_meta
            .model_provider
            .clone()
            .unwrap_or_else(|| store.config.default_model_provider_id.clone()),
        generate_memories: source_meta.memory_mode.as_deref() != Some("disabled"),
    };
    let mut params = RolloutRecorderParams::new(
        source_meta.id,
        source_meta.forked_from_id,
        source_meta.parent_thread_id,
        source_meta.source,
        source_meta.thread_source,
        source_meta.originator,
        source_meta.base_instructions.unwrap_or_default(),
        source_meta.dynamic_tools.unwrap_or_default(),
    )
    .with_creator(
        source_meta.creator_user_id,
        source_meta.creator_account_id,
        source_meta.account_scoped,
    )
    .with_session_id(source_meta.session_id)
    .with_attached_thread(source_meta.attached_thread)
    .with_rollout_id(rollout_id)
    .with_selected_capability_roots(source_meta.selected_capability_roots)
    .with_runtime_workspace_roots(source_meta.runtime_workspace_roots)
    .with_multi_agent_version(source_meta.multi_agent_version)
    .with_history_mode(ThreadHistoryMode::Paginated)
    .with_history_base(history_base)
    .with_forked_from_ordinal_exclusive(forked_from_ordinal_exclusive)
    .with_subagent_history_start_ordinal(source_meta.subagent_history_start_ordinal);
    if let Some(context_window) = source_meta.context_window {
        params = params.with_initial_window_id(context_window.window_id);
    }
    RolloutRecorder::new_with_writer_lock(&config, params, writer_lock)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to create reverted rollout: {err}"),
        })
}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "revert_thread_tests.rs"]
mod tests;
