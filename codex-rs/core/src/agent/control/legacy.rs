use super::*;

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
            thread.session.flush_rollout().await?;
            let result = if matches!(thread.agent_status().await, AgentStatus::Shutdown) {
                Ok(String::new())
            } else {
                state.send_op(agent_id, Op::Shutdown {}).await
            };
            thread.wait_until_terminated().await;
            result
        } else {
            state.send_op(agent_id, Op::Shutdown {}).await
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
        let mut flush_result = Ok(());
        if let Ok(thread) = state.get_thread(agent_id).await {
            if !thread.config_snapshot().await.ephemeral
                && let Some(agent_graph_store) = state.agent_graph_store()
                && let Err(err) = agent_graph_store
                    .set_thread_spawn_edge_status(
                        agent_id,
                        codex_agent_graph_store::ThreadSpawnEdgeStatus::Closed,
                    )
                    .await
            {
                warn!("failed to persist thread-spawn edge status for {agent_id}: {err}");
            }
            flush_result = thread.session.flush_rollout().await;
        }
        let _ = state.remove_thread(&agent_id).await;
        self.forget_agent_residency(agent_id);
        self.state.release_spawned_thread(agent_id);
        flush_result?;
        Ok(())
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

    pub(crate) async fn finish_goal_supervisor_helper(&self, helper_thread_id: ThreadId) -> bool {
        let Some(parent_thread_id) = self
            .goal_supervisor_parent_for_helper(helper_thread_id)
            .await
        else {
            return false;
        };
        let Ok(state) = self.upgrade() else {
            return false;
        };
        let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
            return false;
        };
        match crate::goal_supervisor::finish_supervisor_helper(
            &parent_thread.session,
            helper_thread_id,
        )
        .await
        {
            Ok(finished) => finished,
            Err(err) => {
                warn!("failed to finish goal supervisor helper {helper_thread_id}: {err}");
                false
            }
        }
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
    ) -> bool {
        let Ok(state) = self.upgrade() else {
            return false;
        };
        let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
            return false;
        };
        crate::goal_supervisor::record_followup_action(
            &parent_thread.session,
            delivered_parent_message,
        )
        .await;
        true
    }

    pub(crate) async fn snooze_goal_supervisor_helper(
        &self,
        helper_thread_id: ThreadId,
        delay_seconds: u64,
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
        if parent_thread.session.active_turn.lock().await.is_some() {
            return Ok(SupervisorParentCompactionResult::ParentBusy { parent_thread_id });
        }

        let submission_id = state.send_op(parent_thread_id, Op::Compact).await?;
        crate::goal_supervisor::record_compact_parent_context_action(&parent_thread.session).await;
        Ok(SupervisorParentCompactionResult::Submitted {
            parent_thread_id,
            submission_id,
        })
    }

    pub(crate) async fn send_goal_supervisor_snooze_event(
        &self,
        parent_thread_id: ThreadId,
        delay_seconds: u64,
    ) -> CodexResult<()> {
        let state = self.upgrade()?;
        let parent_thread = state.get_thread(parent_thread_id).await?;
        parent_thread
            .session
            .send_event_raw(Event {
                id: format!("goal-supervisor-snooze-{}", ThreadId::new()),
                msg: codex_protocol::protocol::EventMsg::Warning(WarningEvent {
                    message: format!(
                        "Supervisor snoozed for {}.",
                        format_supervisor_snooze_duration(delay_seconds)
                    ),
                }),
            })
            .await;
        Ok(())
    }

    /// Mark `agent_id` as explicitly closed in persisted spawn-edge state, then shut down the
    /// agent and any live descendants reached from the in-memory tree.
    pub(crate) async fn close_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let state = self.upgrade()?;
        let known_agent = self.state.agent_metadata_for_thread(agent_id).is_some();
        match state.get_thread(agent_id).await {
            Ok(thread) => {
                if !thread.config_snapshot().await.ephemeral
                    && let Some(agent_graph_store) = state.agent_graph_store()
                    && let Err(err) = agent_graph_store
                        .set_thread_spawn_edge_status(
                            agent_id,
                            codex_agent_graph_store::ThreadSpawnEdgeStatus::Closed,
                        )
                        .await
                {
                    warn!("failed to persist thread-spawn edge status for {agent_id}: {err}");
                }
            }
            Err(CodexErr::ThreadNotFound(_)) if known_agent => {
                if let Some(agent_graph_store) = state.agent_graph_store()
                    && let Err(err) = agent_graph_store
                        .set_thread_spawn_edge_status(
                            agent_id,
                            codex_agent_graph_store::ThreadSpawnEdgeStatus::Closed,
                        )
                        .await
                {
                    return Err(CodexErr::Fatal(format!(
                        "failed to persist stale thread-spawn edge status for {agent_id}: {err}"
                    )));
                }
            }
            Err(CodexErr::ThreadNotFound(_)) => {}
            Err(err) => {
                warn!("failed to inspect agent before close {agent_id}: {err}");
            }
        }
        match Box::pin(self.shutdown_agent_tree(agent_id)).await {
            Err(CodexErr::ThreadNotFound(_)) | Err(CodexErr::InternalAgentDied) if known_agent => {
                Ok(String::new())
            }
            result => result,
        }
    }

    /// Shut down `agent_id` and any live descendants reachable from the in-memory spawn tree.
    pub(crate) async fn shutdown_agent_tree(&self, agent_id: ThreadId) -> CodexResult<String> {
        let descendant_ids = self.live_thread_spawn_descendants(agent_id).await?;
        let result = self.shutdown_live_agent(agent_id).await;
        for descendant_id in descendant_ids {
            match self.shutdown_live_agent(descendant_id).await {
                Ok(_) | Err(CodexErr::ThreadNotFound(_)) | Err(CodexErr::InternalAgentDied) => {}
                Err(err) => return Err(err),
            }
        }
        result
    }
}
