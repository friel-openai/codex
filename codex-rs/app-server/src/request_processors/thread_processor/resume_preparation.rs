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

    pub(super) async fn acquire_thread_resume_permit<'a>(
        &'a self,
        params: &ThreadResumeParams,
        force_exclusive: bool,
        mut permit_duration: Option<&mut Duration>,
    ) -> Result<ResumePermit<'a>, JSONRPCErrorError> {
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
            let permit = ResumePermit::acquire(self, params, force_exclusive).await;
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

    #[tokio::test]
    async fn exclusive_list_permit_blocks_an_unrelated_resume() {
        let old_list_permit = Semaphore::new(1);
        let _first = old_list_permit.acquire().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), old_list_permit.acquire())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn unrelated_resume_and_queued_read_complete_while_another_resume_is_held() {
        let first_id = ThreadId::new();
        let other_id = ThreadId::new();
        let state = Arc::new(ThreadStateManager::new());
        let list = Arc::new(ThreadListState::default());
        let queues = RequestSerializationQueues::default();
        let gate = Arc::new(ConnectionRpcGate::new());
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (release_first_tx, release_first_rx) = oneshot::channel();
        let (read_tx, read_rx) = oneshot::channel();
        let key = |thread_id: ThreadId| RequestSerializationQueueKey::Thread {
            thread_id: thread_id.to_string(),
        };

        queues
            .enqueue(
                key(first_id),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(Arc::clone(&gate), {
                    let state = Arc::clone(&state);
                    let list = Arc::clone(&list);
                    async move {
                        let _permit =
                            ResumePermit::acquire_for_id(&state, &list, Some(first_id), true)
                                .await
                                .unwrap();
                        first_started_tx.send(()).unwrap();
                        release_first_rx.await.unwrap();
                    }
                }),
            )
            .await;
        first_started_rx.await.unwrap();
        queues
            .enqueue(
                key(other_id),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(Arc::clone(&gate), {
                    let state = Arc::clone(&state);
                    let list = Arc::clone(&list);
                    async move {
                        let _permit =
                            ResumePermit::acquire_for_id(&state, &list, Some(other_id), true)
                                .await
                                .unwrap();
                    }
                }),
            )
            .await;
        queues
            .enqueue(
                key(other_id),
                RequestSerializationAccess::Exclusive,
                // thread/read uses the same per-thread queue as thread/resume.
                QueuedInitializedRequest::new(gate, async move {
                    read_tx.send(()).unwrap();
                }),
            )
            .await;
        let read_completed = tokio::time::timeout(Duration::from_secs(1), read_rx).await;
        release_first_tx.send(()).unwrap();
        assert!(
            read_completed.is_ok_and(|result| result.is_ok()),
            "unrelated queued read was blocked by the held resume"
        );
    }

    #[tokio::test]
    async fn unrelated_resume_does_not_wait_for_a_thread_resume_reservation() {
        let first_id = ThreadId::new();
        let other_id = ThreadId::new();
        let state = Arc::new(ThreadStateManager::new());
        let list = Arc::new(ThreadListState::default());
        let first = ResumePermit::acquire_for_id(&state, &list, Some(first_id), true)
            .await
            .unwrap();

        // Replacing listener state must not replace a still-held resume reservation.
        state.thread_state(first_id).await;
        state.remove_thread_state(first_id).await;
        let (same_tx, mut same_rx) = oneshot::channel();
        let same = tokio::spawn({
            let state = Arc::clone(&state);
            let list = Arc::clone(&list);
            async move {
                let _permit = ResumePermit::acquire_for_id(&state, &list, Some(first_id), true)
                    .await
                    .unwrap();
                same_tx.send(()).unwrap();
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut same_rx)
                .await
                .is_err()
        );

        let other = tokio::time::timeout(Duration::from_secs(1), async {
            let _permit = ResumePermit::acquire_for_id(&state, &list, Some(other_id), true)
                .await
                .unwrap();
        })
        .await;
        assert!(other.is_ok());

        // Archive/delete/project mutations must still wait for all resumes.
        let writer = list.acquire_exclusive();
        tokio::pin!(writer);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut writer)
                .await
                .is_err()
        );
        // Tokio gives a queued mutation priority over later unrelated resumes.
        let late = ResumePermit::acquire_for_id(&state, &list, Some(other_id), true);
        tokio::pin!(late);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut late)
                .await
                .is_err()
        );
        drop(first);
        let writer = tokio::time::timeout(Duration::from_secs(1), &mut writer)
            .await
            .expect("mutation should acquire after the resume releases")
            .unwrap();
        assert!(matches!(
            same_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut late)
                .await
                .is_err()
        );
        drop(writer);
        tokio::time::timeout(Duration::from_secs(1), &mut same_rx)
            .await
            .expect("same-thread resume should acquire after the mutation")
            .unwrap();
        same.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut late)
            .await
            .expect("later resume should acquire after the mutation");
    }

    #[tokio::test]
    async fn same_thread_waiter_does_not_block_shared_permit_upgrade() {
        let thread_id = ThreadId::new();
        let state = Arc::new(ThreadStateManager::new());
        let list = Arc::new(ThreadListState::default());
        let first = ResumePermit::acquire_for_id(&state, &list, Some(thread_id), true)
            .await
            .unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let (acquired_tx, mut acquired_rx) = oneshot::channel();
        let same = tokio::spawn({
            let state = Arc::clone(&state);
            let list = Arc::clone(&list);
            async move {
                started_tx.send(()).unwrap();
                let _permit = ResumePermit::acquire_for_id(&state, &list, Some(thread_id), true)
                    .await
                    .unwrap();
                acquired_tx.send(()).unwrap();
            }
        });
        started_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut acquired_rx)
                .await
                .is_err()
        );
        let upgraded = tokio::time::timeout(Duration::from_secs(1), first.upgrade())
            .await
            .expect("same-ID waiter must not hold a permit needed by the upgrade")
            .unwrap();
        assert!(acquired_rx.try_recv().is_err());
        drop(upgraded);
        tokio::time::timeout(Duration::from_secs(1), acquired_rx)
            .await
            .expect("same-ID waiter should proceed after release")
            .unwrap();
        same.await.unwrap();
    }

    #[tokio::test]
    async fn changed_resume_source_does_not_use_another_threads_reservation() {
        let selected = ThreadId::new();
        let state = ThreadStateManager::new();
        let list = ThreadListState::default();
        let permit = ResumePermit::acquire_for_id(&state, &list, Some(selected), true)
            .await
            .unwrap();
        assert!(permit.check_id(selected).is_ok());
        assert!(permit.check_id(ThreadId::new()).is_err());
        drop(permit);
        assert!(
            ResumePermit::acquire_for_id(&state, &list, None, true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn unknown_or_agent_resume_keeps_exclusive_list_ordering() {
        use codex_protocol::protocol::InternalSessionSource;
        use codex_protocol::protocol::SessionSource;

        assert!(resume_permit::source_kind_can_share(
            &SessionSource::Cli,
            None
        ));
        assert!(!resume_permit::source_kind_can_share(
            &SessionSource::Unknown,
            None
        ));
        assert!(!resume_permit::source_kind_can_share(
            &SessionSource::Internal(InternalSessionSource::Guardian),
            None,
        ));
        assert!(!resume_permit::source_kind_can_share(
            &SessionSource::Cli,
            Some(ThreadId::new()),
        ));

        let state = ThreadStateManager::new();
        let list = ThreadListState::default();
        // An explicit-history request without an ID must retain the old exclusion.
        let first = ResumePermit::acquire_for_id(&state, &list, None, false)
            .await
            .unwrap();
        let next = ResumePermit::acquire_for_id(&state, &list, Some(ThreadId::new()), true);
        tokio::pin!(next);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut next)
                .await
                .is_err()
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), &mut next)
            .await
            .expect("root resume should proceed after the exclusive resume");
    }

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
