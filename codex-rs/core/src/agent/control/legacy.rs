use super::*;
use crate::codex_thread::CodexThread;
#[cfg(test)]
use codex_protocol::error::CodexErrorDetails;
use std::time::Duration;

const INTERNAL_HELPER_TURN_END_TIMEOUT: Duration = Duration::from_secs(10);
const INTERNAL_HELPER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

/// Shut down a removed internal helper after its terminal tool has returned.
///
/// Internal helpers finish themselves from inside their active turn. Shutting the session down
/// synchronously would abort that turn before its terminal result and `TurnComplete` are persisted.
/// The removed `CodexThread` keeps shutdown possible even when an app-server listener retains
/// another reference to it.
fn retire_removed_internal_helper(agent_id: ThreadId, thread: Arc<CodexThread>) {
    tokio::spawn(async move {
        let mut status_rx = thread.subscribe_status();
        let wait_for_terminal_status = async {
            while matches!(
                status_rx.borrow().clone(),
                AgentStatus::PendingInit | AgentStatus::Running
            ) {
                if status_rx.changed().await.is_err() {
                    break;
                }
            }
        };
        if tokio::time::timeout(INTERNAL_HELPER_TURN_END_TIMEOUT, wait_for_terminal_status)
            .await
            .is_err()
        {
            warn!(
                "internal helper {agent_id} did not finish its terminal turn within {:?}; forcing shutdown",
                INTERNAL_HELPER_TURN_END_TIMEOUT
            );
        }
        match tokio::time::timeout(INTERNAL_HELPER_SHUTDOWN_TIMEOUT, thread.shutdown_and_wait())
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!("failed to shut down finished internal helper {agent_id}: {err}");
            }
            Err(_) => {
                warn!(
                    "timed out after {:?} shutting down finished internal helper {agent_id}",
                    INTERNAL_HELPER_SHUTDOWN_TIMEOUT
                );
            }
        }
    });
}

impl AgentControl {
    /// Submit a shutdown request for a live agent without marking it explicitly closed in
    /// persisted spawn-edge state.
    pub(crate) async fn shutdown_live_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let lifecycle = self
            .get_agent_metadata(agent_id)
            .map(|metadata| metadata.lifecycle)
            .unwrap_or_default();
        let _transition = lifecycle.lock_transition().await;
        let state = self.upgrade()?;
        let result = if let Ok(thread) = state.get_thread(agent_id).await {
            thread.session.ensure_rollout_materialized().await;
            let flush_result = thread.session.flush_rollout().await;
            let shutdown_result = if matches!(thread.agent_status().await, AgentStatus::Shutdown) {
                Ok(String::new())
            } else {
                state
                    .send_op(agent_id, Op::Shutdown {}, /*parent_turn_id*/ None)
                    .await
            };
            thread.wait_until_terminated().await;
            match flush_result {
                Ok(()) => shutdown_result,
                Err(err) => Err(err.into()),
            }
        } else {
            state
                .send_op(agent_id, Op::Shutdown {}, /*parent_turn_id*/ None)
                .await
        };
        let _ = state.remove_thread(&agent_id).await;
        self.forget_agent_residency(agent_id);
        self.state.release_spawned_thread(agent_id);
        result
    }

    pub(crate) async fn finish_internal_helper_thread(
        &self,
        agent_id: ThreadId,
    ) -> CodexResult<()> {
        let state = self.upgrade()?;
        {
            let _lifecycle_mutation = state.lock_lifecycle_mutation().await;
            if !state.is_thread_closing(agent_id)
                && let Some(agent_graph_store) = state.agent_graph_store()
                && let Err(err) = agent_graph_store
                    .transition_open_thread_spawn_edge_to_closed(agent_id)
                    .await
            {
                warn!("failed to persist thread-spawn edge status for {agent_id}: {err}");
            }
        }
        let mut flush_result = Ok(());
        if let Ok(thread) = state.get_thread(agent_id).await {
            flush_result = thread.session.flush_rollout().await;
        }
        let removed_thread = state.remove_thread(&agent_id).await;
        self.forget_agent_residency(agent_id);
        self.state.release_spawned_thread(agent_id);
        if let Some(thread) = removed_thread {
            retire_removed_internal_helper(agent_id, thread);
        }
        flush_result?;
        Ok(())
    }

    /// Remove terminal supervisor helpers left open by an earlier process before spawning the
    /// current supervisor. A running supervisor is returned so its parent can adopt it instead of
    /// creating a duplicate.
    pub(crate) async fn reconcile_goal_supervisor_state(
        &self,
        parent_thread_id: ThreadId,
        supervisor_path: &AgentPath,
    ) -> CodexResult<Option<ThreadId>> {
        let state = self.upgrade()?;
        let mut candidate_ids = self
            .state
            .agent_id_for_path(supervisor_path)
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(agent_graph_store) = state.agent_graph_store() {
            let persisted_ids = agent_graph_store
                .list_thread_spawn_children(
                    parent_thread_id,
                    Some(codex_agent_graph_store::ThreadSpawnEdgeStatus::Open),
                )
                .await
                .map_err(|err| {
                    CodexErr::Fatal(format!(
                        "failed to inspect persisted supervisor state for {parent_thread_id}: {err}"
                    ))
                })?;
            for thread_id in persisted_ids {
                if !candidate_ids.contains(&thread_id) {
                    candidate_ids.push(thread_id);
                }
            }
        }

        let mut running_supervisor = None;
        for thread_id in candidate_ids {
            if !self
                .is_goal_supervisor_for_parent(&state, thread_id, parent_thread_id)
                .await
            {
                continue;
            }
            if matches!(
                self.get_status(thread_id).await,
                AgentStatus::PendingInit | AgentStatus::Running
            ) {
                running_supervisor.get_or_insert(thread_id);
                continue;
            }
            self.finish_internal_helper_thread(thread_id).await?;
        }
        Ok(running_supervisor)
    }

    async fn is_goal_supervisor_for_parent(
        &self,
        state: &Arc<ThreadManagerState>,
        thread_id: ThreadId,
        parent_thread_id: ThreadId,
    ) -> bool {
        if let Some(snapshot) = self.get_agent_config_snapshot(thread_id).await
            && matches!(
                snapshot.session_source,
                SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: source_parent_thread_id,
                    agent_role: Some(ref agent_role),
                    ..
                }) if source_parent_thread_id == parent_thread_id
                    && agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME
            )
        {
            return true;
        }
        let Ok(stored_thread) = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await
        else {
            return false;
        };
        matches!(
            stored_thread.source,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: source_parent_thread_id,
                agent_role: Some(ref agent_role),
                ..
            }) if source_parent_thread_id == parent_thread_id
                && agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME
        )
    }

    pub(crate) async fn goal_supervisor_parent_for_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> Option<ThreadId> {
        let state = self.upgrade().ok()?;
        let helper_thread = state.get_thread(helper_thread_id).await.ok()?;
        let snapshot = helper_thread.session.thread_config_snapshot().await;
        let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            agent_role: Some(agent_role),
            ..
        }) = snapshot.session_source
        else {
            return None;
        };
        (agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
            .then_some(parent_thread_id)
    }

    pub(crate) async fn finish_goal_supervisor_helper_after_followup(
        &self,
        helper_thread_id: ThreadId,
    ) -> CodexResult<bool> {
        let Some(parent_thread_id) = self
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await
        else {
            return Ok(false);
        };
        let state = self.upgrade()?;
        let parent_thread = state.get_thread(parent_thread_id).await?;
        crate::goal_supervisor::finish_supervisor_helper_after_followup(
            &parent_thread.session,
            helper_thread_id,
        )
        .await
        .map_err(|err| {
            CodexErr::Fatal(format!(
                "failed to finish goal supervisor helper {helper_thread_id}: {err}"
            ))
        })
    }

    pub(crate) async fn defer_failed_goal_supervisor_helper(
        &self,
        parent_thread_id: ThreadId,
        helper_thread_id: ThreadId,
        terminal_status: AgentStatus,
    ) -> bool {
        let Ok(state) = self.upgrade() else {
            return false;
        };
        let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
            return false;
        };
        match crate::goal_supervisor::defer_failed_supervisor_helper(
            &parent_thread.session,
            helper_thread_id,
            terminal_status,
        )
        .await
        {
            Ok(deferred) => deferred,
            Err(err) => {
                warn!("failed to defer goal supervisor helper {helper_thread_id}: {err}");
                false
            }
        }
    }

    pub(crate) async fn record_goal_supervisor_followup_action(
        &self,
        parent_thread_id: ThreadId,
        delivered_parent_message: &InterAgentCommunication,
    ) -> CodexResult<bool> {
        let state = self.upgrade()?;
        let parent_thread = state.get_thread(parent_thread_id).await?;
        crate::goal_supervisor::record_followup_action(
            &parent_thread.session,
            delivered_parent_message,
        )
        .await
        .map_err(|err| {
            CodexErr::Fatal(format!(
                "failed to record Goal Supervisor follow-up action: {err}"
            ))
        })?;
        Ok(true)
    }

    pub(crate) async fn snooze_goal_supervisor_helper(
        &self,
        helper_thread_id: ThreadId,
        delay_seconds: u64,
        reason: Option<&str>,
    ) -> Option<u64> {
        let parent_thread_id = self
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await?;
        let state = self.upgrade().ok()?;
        let parent_thread = state.get_thread(parent_thread_id).await.ok()?;
        match crate::goal_supervisor::snooze_supervisor_helper(
            &parent_thread.session,
            helper_thread_id,
            delay_seconds,
            reason,
        )
        .await
        {
            Ok(delay_seconds) => delay_seconds,
            Err(err) => {
                warn!("failed to snooze goal supervisor helper {helper_thread_id}: {err}");
                None
            }
        }
    }

    pub(crate) async fn compact_parent_for_goal_supervisor_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> CodexResult<SupervisorParentCompactionResult> {
        let Some(parent_thread_id) = self
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await
        else {
            return Ok(SupervisorParentCompactionResult::NotSupervisorHelper);
        };
        let state = self.upgrade()?;
        let parent_thread = state.get_thread(parent_thread_id).await?;
        tokio::spawn(async move {
            if parent_thread.session.active_turn.lock().await.is_some() {
                let checkpoint_admission = parent_thread
                    .session
                    .lock_checkpoint_admission("finish a busy Goal Supervisor helper")
                    .await
                    .map_err(|err| CodexErr::InvalidRequest(err.to_string()))?;
                let finished = parent_thread
                    .session
                    .with_inherited_checkpoint_admission(
                        crate::goal_supervisor::finish_supervisor_helper(
                            &parent_thread.session,
                            helper_thread_id,
                        ),
                    )
                    .await
                    .map_err(|err| CodexErr::InvalidRequest(err.to_string()))?;
                drop(checkpoint_admission);
                if !finished {
                    return Err(CodexErr::InvalidRequest(format!(
                        "Goal Supervisor helper {helper_thread_id} was not active while its parent was busy"
                    )));
                }
                return Ok(SupervisorParentCompactionResult::ParentBusy { parent_thread_id });
            }

            let submission = state
                .send_op_admitted(parent_thread_id, Op::Compact, /*parent_turn_id*/ None)
                .await?;
            parent_thread
                .session
                .with_inherited_checkpoint_admission(async {
                    crate::goal_supervisor::record_compact_parent_context_action(
                        &parent_thread.session,
                    )
                    .await?;
                    let finished = crate::goal_supervisor::finish_supervisor_helper(
                        &parent_thread.session,
                        helper_thread_id,
                    )
                    .await?;
                    if !finished {
                        anyhow::bail!(
                            "Goal Supervisor helper {helper_thread_id} was not active after parent compaction"
                        );
                    }
                    anyhow::Ok(())
                })
                .await
                .map_err(|err| CodexErr::InvalidRequest(err.to_string()))?;
            Ok(SupervisorParentCompactionResult::Submitted {
                parent_thread_id,
                submission_id: submission.into_id(),
            })
        })
        .await
        .map_err(|_| CodexErr::InternalAgentDied)?
    }

    /// Shut down `agent_id` and any live descendants reachable from the in-memory spawn tree.
    #[cfg(test)]
    pub(crate) async fn shutdown_agent_tree(&self, agent_id: ThreadId) -> CodexResult<String> {
        let descendant_ids = self.live_thread_spawn_descendants(agent_id).await?;
        let result = self.shutdown_live_agent(agent_id).await;
        for descendant_id in descendant_ids {
            match self.shutdown_live_agent(descendant_id).await {
                Ok(_) => {}
                Err(err)
                    if matches!(
                        err.details(),
                        CodexErrorDetails::ThreadNotFound(_) | CodexErrorDetails::InternalAgentDied
                    ) => {}
                Err(err) => return Err(err),
            }
        }
        result
    }
}
