//! Completes selected-rollout migration before serializing resume lifecycle changes.

use super::*;
use codex_thread_store::ReadThreadsParams;

impl ThreadRequestProcessor {
    pub(super) async fn wait_for_resume_shutdown_drain(
        thread_id: ThreadId,
        mut drain: watch::Receiver<bool>,
    ) -> Result<(), JSONRPCErrorError> {
        // Only the wait can expire. The old listener still owns terminal events.
        let _ = tokio::time::timeout(
            THREAD_LISTENER_DRAIN_TIMEOUT,
            drain.wait_for(|drained| *drained),
        )
        .await
        .map_err(|_| thread_closing_for_resume(thread_id))?;
        Ok(())
    }

    pub(super) async fn acquire_thread_resume_permit(
        &self,
        params: &ThreadResumeParams,
        mut permit_duration: Option<&mut Duration>,
    ) -> Result<SemaphorePermit<'_>, JSONRPCErrorError> {
        let mut prepared = params.history.is_some();
        let requested_id = ThreadId::from_string(&params.thread_id).ok();
        loop {
            let running = if let Some(thread_id) = requested_id {
                get_loaded_thread_for_persistence(&self.thread_manager, thread_id)
                    .await?
                    .is_some()
            } else {
                false
            };
            if !prepared && !running {
                let source = self
                    .read_stored_thread_for_resume(
                        &params.thread_id,
                        params.path.as_ref(),
                        /*include_history*/ false,
                    )
                    .await?;
                if params.path.is_some() {
                    // Explicit paths can identify external or stale rollouts. Only migrate the
                    // selected source; the protected resume still validates the requested path.
                    let indexed = self
                        .thread_store
                        .read_threads(ReadThreadsParams {
                            thread_ids: vec![source.thread_id],
                        })
                        .await
                        .map_err(thread_store_resume_read_error)?;
                    let selected_path = indexed.iter().any(|current| {
                        current
                            .rollout_path
                            .as_ref()
                            .zip(source.rollout_path.as_ref())
                            .is_some_and(|(current, requested)| {
                                path_utils::paths_match_after_normalization(
                                    codex_rollout::plain_rollout_path(current).as_path(),
                                    codex_rollout::plain_rollout_path(requested).as_path(),
                                )
                            })
                    });
                    let unindexed_local_path = indexed.is_empty()
                        && source.rollout_path.as_ref().is_some_and(|path| {
                            path.starts_with(
                                self.config.codex_home.join(codex_rollout::SESSIONS_SUBDIR),
                            )
                        });
                    if selected_path || unindexed_local_path {
                        self.thread_store
                            .read_thread(StoreReadThreadParams {
                                thread_id: source.thread_id,
                                include_archived: true,
                                include_history: false,
                            })
                            .await
                            .map_err(thread_store_resume_read_error)?;
                    }
                }
                prepared = true;
            }
            let started = Instant::now();
            let permit = self.acquire_thread_list_state_permit().await;
            if let Some(duration) = permit_duration.as_deref_mut() {
                *duration = duration.saturating_add(started.elapsed());
            }
            let permit = permit?;
            if !prepared
                && let Some(thread_id) = requested_id
                && get_loaded_thread_for_persistence(&self.thread_manager, thread_id)
                    .await?
                    .is_none()
            {
                // An idle runtime can unload while this request waits for the lifecycle permit.
                drop(permit);
                continue;
            }
            // Do not reuse preflight metadata: archive, deletion and source selection may have
            // changed while migration ran. Existing resume checks run again under this permit.
            return Ok(permit);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_rpc_gate::ConnectionRpcGate;
    use crate::request_serialization::QueuedInitializedRequest;
    use crate::request_serialization::RequestSerializationAccess;
    use crate::request_serialization::RequestSerializationQueueKey;
    use crate::request_serialization::RequestSerializationQueues;

    #[tokio::test(start_paused = true)]
    async fn cold_resume_with_pending_shutdown_drain_releases_thread_queue() {
        let thread_id = ThreadId::new();
        let key = RequestSerializationQueueKey::Thread {
            thread_id: thread_id.to_string(),
        };
        let state_manager = Arc::new(ThreadStateManager::new());
        let state = state_manager.thread_state(thread_id).await;
        let queues = RequestSerializationQueues::default();
        let gate = Arc::new(ConnectionRpcGate::new());
        let (old_listener, drain) = watch::channel(false);
        let (started_tx, started_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let (follower_tx, follower_rx) = oneshot::channel();

        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(Arc::clone(&gate), {
                    let state_manager = Arc::clone(&state_manager);
                    let drain = drain.clone();
                    async move {
                        let _guard = state_manager.lock_goal_resume(thread_id).await;
                        started_tx.send(()).unwrap();
                        let result = ThreadRequestProcessor::wait_for_resume_shutdown_drain(
                            thread_id, drain,
                        )
                        .await;
                        resume_tx.send(result).unwrap();
                    }
                }),
            )
            .await;
        started_rx.await.unwrap();
        queues
            .enqueue(
                key,
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate, {
                    let state_manager = Arc::clone(&state_manager);
                    async move {
                        let _guard = state_manager.lock_goal_resume(thread_id).await;
                        follower_tx.send(()).unwrap();
                    }
                }),
            )
            .await;
        let start = tokio::time::Instant::now();
        let error = resume_rx.await.unwrap().unwrap_err();
        assert_eq!(error.message, thread_closing_for_resume(thread_id).message);
        assert!(start.elapsed() >= THREAD_LISTENER_DRAIN_TIMEOUT);
        follower_rx.await.unwrap();
        assert!(Arc::ptr_eq(
            &state,
            &state_manager.thread_state(thread_id).await
        ));
        assert!(!*drain.borrow());

        old_listener.send_replace(true);
        ThreadRequestProcessor::wait_for_resume_shutdown_drain(thread_id, drain)
            .await
            .unwrap();
    }
}
