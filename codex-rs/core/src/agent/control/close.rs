use super::*;
use codex_state::ClosedThreadSpawnSubtree;
use std::collections::HashSet;

/// Temporary in-memory fence for one first-attempt permanent close.
///
/// Dropping an armed fence rolls back only the IDs installed by this close. Durable
/// PermanentlyClosed edges remain the source of truth when cancellation happens after commit.
struct InProgressCloseFence {
    state: Arc<ThreadManagerState>,
    owner_thread_id: ThreadId,
    target_thread_id: ThreadId,
    thread_ids: Vec<ThreadId>,
    phase: CloseFencePhase,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CloseFencePhase {
    Prepared,
    DurableMutationInFlight,
    Complete,
}

impl InProgressCloseFence {
    fn install(
        state: Arc<ThreadManagerState>,
        owner_thread_id: ThreadId,
        target_thread_id: ThreadId,
        thread_ids: Vec<ThreadId>,
    ) -> Self {
        state.mark_threads_closing(thread_ids.iter().copied());
        Self {
            state,
            owner_thread_id,
            target_thread_id,
            thread_ids,
            phase: CloseFencePhase::Prepared,
        }
    }

    fn begin_durable_mutation(&mut self) {
        self.state.remember_in_flight_agent_subtree_close(
            self.owner_thread_id,
            self.target_thread_id,
            self.thread_ids.clone(),
        );
        self.phase = CloseFencePhase::DurableMutationInFlight;
    }

    fn rollback(mut self) {
        self.state
            .unmark_threads_closing(self.thread_ids.iter().copied());
        self.state
            .forget_in_flight_agent_subtree_close(self.target_thread_id);
        self.phase = CloseFencePhase::Complete;
    }

    fn complete(
        mut self,
        owner_thread_id: ThreadId,
        target_thread_id: ThreadId,
        member_thread_ids: Vec<ThreadId>,
    ) {
        self.state.replace_threads_closing(
            self.thread_ids.iter().copied(),
            member_thread_ids.iter().copied(),
        );
        self.state
            .finish_agent_subtree_close(owner_thread_id, target_thread_id, member_thread_ids);
        self.phase = CloseFencePhase::Complete;
    }
}

impl Drop for InProgressCloseFence {
    fn drop(&mut self) {
        if self.phase == CloseFencePhase::Prepared {
            self.state
                .unmark_threads_closing(self.thread_ids.iter().copied());
        } else if self.phase == CloseFencePhase::DurableMutationInFlight {
            self.state
                .abandon_in_flight_agent_subtree_close(self.target_thread_id);
        }
    }
}

impl AgentControl {
    /// Permanently close one owned descendant subtree and remove its current runtime state.
    ///
    /// Persisted edges are closed before any fallible runtime cleanup. The closed edge is the
    /// authorization fence that prevents ordinary resume from restoring the subtree.
    pub(crate) async fn close_agent_subtree(
        &self,
        caller_thread_id: ThreadId,
        agent_id: ThreadId,
    ) -> CodexResult<CloseAgentSubtreeReport> {
        let state = self.upgrade()?;
        let root_thread_id = self
            .state
            .agent_id_for_path(&AgentPath::root())
            .ok_or_else(|| CodexErr::Fatal("agent tree is missing its root thread".to_string()))?;
        if agent_id == root_thread_id {
            return Err(CodexErr::UnsupportedOperation(
                "root is not a spawned agent".to_string(),
            ));
        }
        if agent_id == caller_thread_id {
            return Err(CodexErr::UnsupportedOperation(
                "an agent cannot close itself".to_string(),
            ));
        }
        if self.is_goal_supervisor_agent(&state, agent_id).await? {
            return Err(CodexErr::UnsupportedOperation(
                "goal supervisor agents cannot be closed with close_agent".to_string(),
            ));
        }

        let lifecycle_mutation = state.lock_lifecycle_mutation().await;
        let registered_subtree = self.state.registered_subtree_thread_ids(agent_id);
        let remembered_close = state.closed_agent_subtree_memory(agent_id);
        if remembered_close
            .as_ref()
            .is_some_and(|remembered| remembered.owner_thread_id != caller_thread_id)
        {
            return Err(CodexErr::UnsupportedOperation(format!(
                "agent subtree {agent_id} is owned by another caller"
            )));
        }
        if remembered_close
            .as_ref()
            .is_some_and(|remembered| !remembered.complete && remembered.in_flight_active)
        {
            return Err(CodexErr::UnsupportedOperation(format!(
                "agent subtree {agent_id} is already being permanently closed"
            )));
        }
        let already_fenced = state.is_thread_permanently_closing(agent_id);
        let completed_fence = remembered_close
            .as_ref()
            .is_some_and(|remembered| remembered.complete);
        let graph_store = state.agent_graph_store();
        let registered_owned = self
            .state
            .registered_subtree_thread_ids(caller_thread_id)
            .contains(&agent_id);
        let registered_ephemeral = registered_owned
            && self
                .state
                .agent_metadata_for_thread(agent_id)
                .is_some_and(|metadata| metadata.ephemeral);
        let current_only_parent_by_thread_id =
            if registered_owned && let Some(store) = graph_store.as_deref() {
                let mut caller_owned_thread_ids = HashSet::from([caller_thread_id]);
                caller_owned_thread_ids.extend(
                    store
                        .list_permanent_close_thread_spawn_descendants(caller_thread_id)
                        .await
                        .map_err(|err| {
                            CodexErr::Fatal(format!(
                                "failed to authorize current-only agent subtree {agent_id}: {err}"
                            ))
                        })?,
                );
                self.current_only_descendant_parents_with_prepared_ownership(
                    caller_thread_id,
                    &caller_owned_thread_ids,
                    Some(store),
                )
                .await?
            } else {
                HashMap::new()
            };
        let current_only_parent = current_only_parent_by_thread_id.get(&agent_id).copied();
        let current_only_target_edges = current_only_parent.map(|_| {
            let mut edges = Vec::new();
            let mut child_thread_id = agent_id;
            while let Some(parent_thread_id) = current_only_parent_by_thread_id
                .get(&child_thread_id)
                .copied()
            {
                edges.push(codex_state::CurrentOnlyThreadSpawnEdge {
                    parent_thread_id,
                    child_thread_id,
                });
                child_thread_id = parent_thread_id;
            }
            edges.reverse();
            edges
        });
        // Permanent close materializes the selected current-only target even when the live agent
        // is ephemeral. The PClosed edge is the restart-safe cleanup authorization and cannot be
        // confused with ordinary persisted membership because it is never Open.
        let mut durable_current_only_edges = current_only_target_edges.clone();
        let non_persisted_fallback =
            current_only_parent.is_some() || (graph_store.is_none() && registered_ephemeral);
        if graph_store.is_none()
            && registered_owned
            && self
                .state
                .agent_metadata_for_thread(agent_id)
                .is_some_and(|metadata| !metadata.ephemeral)
        {
            return Err(CodexErr::UnsupportedOperation(
                "cannot close a non-ephemeral agent without an ownership store".to_string(),
            ));
        }
        if graph_store.is_none() && !non_persisted_fallback {
            return Err(CodexErr::UnsupportedOperation(format!(
                "agent {agent_id} is not an owned descendant"
            )));
        }

        if !completed_fence && graph_store.is_none() && registered_ephemeral {
            for thread_id in &registered_subtree {
                let Some(metadata) = self.state.agent_metadata_for_thread(*thread_id) else {
                    return Err(CodexErr::UnsupportedOperation(
                        "cannot close an unregistered ephemeral subtree without an ownership store"
                            .to_string(),
                    ));
                };
                if !metadata.ephemeral {
                    return Err(CodexErr::UnsupportedOperation(
                        "cannot close a non-ephemeral subtree without an ownership store"
                            .to_string(),
                    ));
                }
            }
        }
        let mut prepared_subtree = Vec::new();
        let mut prepared_persisted_thread_ids = HashSet::new();
        let mut durable_retry_subtree = None;
        let mut legacy_repair_parent = None;
        let mut current_only_descendant_parent_by_thread_id = HashMap::new();
        if !completed_fence && let Some(store) = graph_store.as_ref() {
            let selected_identity = store
                .find_open_thread_spawn_descendant_by_id(caller_thread_id, agent_id)
                .await
                .map_err(|err| {
                    CodexErr::Fatal(format!(
                        "failed to authorize persisted agent subtree {agent_id}: {err}"
                    ))
                })?;
            if selected_identity.is_some() {
                prepared_subtree.push(agent_id);
                prepared_subtree.extend(
                    store
                        .list_permanent_close_thread_spawn_descendants(agent_id)
                        .await
                        .map_err(|err| {
                            CodexErr::Fatal(format!(
                                "failed to prepare persisted agent subtree {agent_id}: {err}"
                            ))
                        })?,
                );
            } else {
                durable_retry_subtree = store
                    .get_permanently_closed_thread_spawn_subtree(caller_thread_id, agent_id)
                    .await
                    .map_err(|err| {
                        CodexErr::Fatal(format!(
                            "failed to load permanently closed agent subtree {agent_id}: {err}"
                        ))
                    })?;
                if let Some(subtree) = durable_retry_subtree.as_ref() {
                    prepared_subtree.extend(subtree.members.iter().map(|member| member.thread_id));
                    prepared_subtree.extend(
                        store
                            .list_permanent_close_thread_spawn_descendants(agent_id)
                            .await
                            .map_err(|err| {
                                CodexErr::Fatal(format!(
                                    "failed to prepare permanently closed agent subtree {agent_id}: {err}"
                                ))
                            })?,
                    );
                } else if non_persisted_fallback {
                    prepared_subtree.push(agent_id);
                    prepared_subtree.extend(
                        store
                            .list_permanent_close_thread_spawn_descendants(agent_id)
                            .await
                            .map_err(|err| {
                                CodexErr::Fatal(format!(
                                    "failed to prepare current-only agent subtree {agent_id}: {err}"
                                ))
                            })?,
                    );
                } else if let Some(parent_thread_id) =
                    self.legacy_closed_subagent_parent(&state, agent_id).await?
                {
                    legacy_repair_parent = Some(parent_thread_id);
                    prepared_subtree.push(agent_id);
                    prepared_subtree.extend(
                        store
                            .list_thread_spawn_descendants(
                                agent_id,
                                Some(codex_agent_graph_store::ThreadSpawnEdgeStatus::Open),
                            )
                            .await
                            .map_err(|err| {
                                CodexErr::Fatal(format!(
                                    "failed to prepare legacy agent subtree {agent_id}: {err}"
                                ))
                            })?,
                    );
                } else {
                    return Err(CodexErr::UnsupportedOperation(format!(
                        "agent {agent_id} is not an owned descendant"
                    )));
                }
            }
        } else if !completed_fence && non_persisted_fallback {
            prepared_subtree.push(agent_id);
        }
        if !completed_fence {
            let owned_thread_ids = prepared_subtree.iter().copied().collect::<HashSet<_>>();
            prepared_persisted_thread_ids.clone_from(&owned_thread_ids);
            if let Some(store) = graph_store.as_deref() {
                current_only_descendant_parent_by_thread_id = self
                    .current_only_descendant_parents_with_prepared_ownership(
                        agent_id,
                        &owned_thread_ids,
                        Some(store),
                    )
                    .await?;
                prepared_subtree
                    .extend(current_only_descendant_parent_by_thread_id.keys().copied());
            } else {
                prepared_subtree.extend(
                    self.state
                        .registered_ephemeral_descendants_within(&owned_thread_ids),
                );
            }
        }
        prepared_subtree.sort_by_key(ToString::to_string);
        prepared_subtree.dedup();
        let mut durable_current_only_descendant_ids = HashSet::new();
        for thread_id in current_only_descendant_parent_by_thread_id.keys().copied() {
            let mut child_thread_id = thread_id;
            let mut path_thread_ids = HashSet::new();
            while !prepared_persisted_thread_ids.contains(&child_thread_id) {
                if !path_thread_ids.insert(child_thread_id) {
                    return Err(CodexErr::Fatal(format!(
                        "current-only agent {thread_id} has a cyclic parent chain"
                    )));
                }
                durable_current_only_descendant_ids.insert(child_thread_id);
                child_thread_id = current_only_descendant_parent_by_thread_id
                    .get(&child_thread_id)
                    .copied()
                    .ok_or_else(|| {
                        CodexErr::Fatal(format!(
                            "current-only agent {thread_id} is not connected to selected subtree {agent_id}"
                        ))
                    })?;
            }
        }
        let durable_current_only_descendant_edges = ordered_current_only_descendant_edges(
            agent_id,
            prepared_persisted_thread_ids,
            current_only_descendant_parent_by_thread_id
                .into_iter()
                .filter(|(thread_id, _)| durable_current_only_descendant_ids.contains(thread_id))
                .collect(),
        )?;
        if let Some(edges) = durable_current_only_edges.as_mut() {
            edges.extend(durable_current_only_descendant_edges.iter().copied());
        }
        if prepared_subtree
            .iter()
            .any(|thread_id| state.is_thread_under_membership_eviction(*thread_id))
        {
            return Err(CodexErr::UnsupportedOperation(format!(
                "agent subtree {agent_id} is being archived or deleted"
            )));
        }
        let mut in_progress_fence = if already_fenced {
            None
        } else {
            Some(InProgressCloseFence::install(
                Arc::clone(&state),
                caller_thread_id,
                agent_id,
                prepared_subtree.clone(),
            ))
        };
        drop(lifecycle_mutation);

        let persisted_subtree = if completed_fence {
            let Some(remembered_close) = remembered_close.as_ref() else {
                return Err(CodexErr::Fatal(
                    "completed close fence did not retain its member set".to_string(),
                ));
            };
            Some(ClosedThreadSpawnSubtree {
                members: remembered_close
                    .member_thread_ids
                    .iter()
                    .copied()
                    .map(|thread_id| codex_state::ClosedThreadSpawnSubtreeMember {
                        thread_id,
                        depth: 0,
                    })
                    .collect(),
                newly_closed_edge_count: 0,
            })
        } else if let (Some(store), Some(_)) = (graph_store.as_ref(), durable_retry_subtree) {
            if let Some(in_progress_fence) = in_progress_fence.as_mut() {
                in_progress_fence.begin_durable_mutation();
            }
            match store
                .extend_permanently_closed_thread_spawn_subtree_with_current_only_descendants(
                    caller_thread_id,
                    agent_id,
                    durable_current_only_descendant_edges.clone(),
                )
                .await
            {
                Ok(Some(subtree)) => Some(subtree),
                Ok(None) => {
                    if let Some(in_progress_fence) = in_progress_fence.take() {
                        in_progress_fence.rollback();
                    }
                    return Err(CodexErr::UnsupportedOperation(format!(
                        "agent {agent_id} is no longer a permanently closed owned descendant"
                    )));
                }
                Err(err) => {
                    if let Some(in_progress_fence) = in_progress_fence.take() {
                        in_progress_fence.rollback();
                    }
                    return Err(CodexErr::Fatal(format!(
                        "failed to extend permanently closed agent subtree {agent_id}: {err}"
                    )));
                }
            }
        } else if let (Some(store), Some(current_only_ownership_edges)) =
            (graph_store.as_ref(), durable_current_only_edges)
        {
            if let Some(in_progress_fence) = in_progress_fence.as_mut() {
                in_progress_fence.begin_durable_mutation();
            }
            match store
                .close_current_only_thread_spawn_subtree(
                    caller_thread_id,
                    agent_id,
                    current_only_ownership_edges,
                )
                .await
            {
                Ok(Some(subtree)) => Some(subtree),
                Ok(None) => {
                    if let Some(in_progress_fence) = in_progress_fence.take() {
                        in_progress_fence.rollback();
                    }
                    return Err(CodexErr::UnsupportedOperation(format!(
                        "agent {agent_id} no longer has current-only ownership"
                    )));
                }
                Err(err) => {
                    if let Some(in_progress_fence) = in_progress_fence.take() {
                        in_progress_fence.rollback();
                    }
                    return Err(CodexErr::Fatal(format!(
                        "failed to persist current-only agent subtree {agent_id}: {err}"
                    )));
                }
            }
        } else if let (Some(store), Some(expected_parent_thread_id)) =
            (graph_store.as_ref(), legacy_repair_parent)
        {
            if let Some(in_progress_fence) = in_progress_fence.as_mut() {
                in_progress_fence.begin_durable_mutation();
            }
            let repair_result = if durable_current_only_descendant_edges.is_empty() {
                store
                    .repair_legacy_closed_thread_spawn_subtree(
                        caller_thread_id,
                        agent_id,
                        expected_parent_thread_id,
                    )
                    .await
            } else {
                store
                    .repair_legacy_closed_thread_spawn_subtree_with_current_only_descendants(
                        caller_thread_id,
                        agent_id,
                        expected_parent_thread_id,
                        durable_current_only_descendant_edges.clone(),
                    )
                    .await
            };
            match repair_result {
                Ok(Some(subtree)) => Some(subtree),
                Ok(None) => {
                    if let Some(in_progress_fence) = in_progress_fence.take() {
                        in_progress_fence.rollback();
                    }
                    return Err(CodexErr::UnsupportedOperation(format!(
                        "agent {agent_id} is not a legacy owned descendant"
                    )));
                }
                Err(err) => {
                    if let Some(in_progress_fence) = in_progress_fence.take() {
                        in_progress_fence.rollback();
                    }
                    return Err(CodexErr::Fatal(format!(
                        "failed to repair legacy closed agent subtree {agent_id}: {err}"
                    )));
                }
            }
        } else {
            if let Some(in_progress_fence) = in_progress_fence.as_mut() {
                in_progress_fence.begin_durable_mutation();
            }
            match graph_store.as_ref() {
                Some(store) => match if durable_current_only_descendant_edges.is_empty() {
                    store
                        .close_open_thread_spawn_subtree(caller_thread_id, agent_id)
                        .await
                } else {
                    store
                        .close_open_thread_spawn_subtree_with_current_only_descendants(
                            caller_thread_id,
                            agent_id,
                            durable_current_only_descendant_edges,
                        )
                        .await
                } {
                    Ok(Some(subtree)) => Some(subtree),
                    Ok(None) if non_persisted_fallback => None,
                    Ok(None) => {
                        if let Some(in_progress_fence) = in_progress_fence.take() {
                            in_progress_fence.rollback();
                        }
                        return Err(CodexErr::UnsupportedOperation(format!(
                            "agent {agent_id} is not an owned descendant"
                        )));
                    }
                    Err(err) => {
                        if let Some(in_progress_fence) = in_progress_fence.take() {
                            in_progress_fence.rollback();
                        }
                        return Err(CodexErr::Fatal(format!(
                            "failed to close persisted agent subtree {agent_id}: {err}"
                        )));
                    }
                },
                None => None,
            }
        };
        let member_ids =
            self.closed_subtree_member_ids(persisted_subtree.as_ref(), &prepared_subtree);
        if !completed_fence {
            if let Some(in_progress_fence) = in_progress_fence.take() {
                in_progress_fence.complete(caller_thread_id, agent_id, member_ids.clone());
            } else {
                state.replace_threads_closing(
                    remembered_close
                        .into_iter()
                        .flat_map(|remembered| remembered.member_thread_ids),
                    member_ids.iter().copied(),
                );
                state.finish_agent_subtree_close(caller_thread_id, agent_id, member_ids.clone());
            }
        }

        let mut report = CloseAgentSubtreeReport::default();
        if let Some(subtree) = persisted_subtree {
            report.closed_edges = subtree.members.len();
            report.newly_closed_edges = subtree.newly_closed_edge_count;
        }
        report.closed_agents = member_ids.len();
        let mut cleanup_errors = Vec::new();
        self.terminate_closed_agent_members(&state, &member_ids, &mut report, &mut cleanup_errors)
            .await;
        self.cleanup_closed_agent_persistence(
            &state,
            &member_ids,
            &mut report,
            &mut cleanup_errors,
        )
        .await;

        if cleanup_errors.is_empty() {
            Ok(report)
        } else {
            Err(CodexErr::Fatal(format!(
                "closed agent subtree {agent_id}, but cleanup failed: {}",
                cleanup_errors.join("; ")
            )))
        }
    }

    async fn legacy_closed_subagent_parent(
        &self,
        state: &ThreadManagerState,
        agent_id: ThreadId,
    ) -> CodexResult<Option<ThreadId>> {
        let stored = match state
            .read_stored_thread(ReadThreadParams {
                thread_id: agent_id,
                include_archived: true,
                include_history: false,
            })
            .await
        {
            Ok(stored) => stored,
            Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        let Some(stored_parent_thread_id) = stored.parent_thread_id else {
            return Ok(None);
        };
        match stored.source {
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id, ..
            }) if parent_thread_id == stored_parent_thread_id => Ok(Some(parent_thread_id)),
            SessionSource::SubAgent(_)
            | SessionSource::Cli
            | SessionSource::VSCode
            | SessionSource::Exec
            | SessionSource::Mcp
            | SessionSource::Custom(_)
            | SessionSource::Internal(_)
            | SessionSource::Unknown => Ok(None),
        }
    }

    async fn is_goal_supervisor_agent(
        &self,
        state: &ThreadManagerState,
        agent_id: ThreadId,
    ) -> CodexResult<bool> {
        if self
            .state
            .agent_metadata_for_thread(agent_id)
            .and_then(|metadata| metadata.agent_role)
            .as_deref()
            == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
        {
            return Ok(true);
        }
        match state.get_thread(agent_id).await {
            Ok(thread) => {
                return Ok(thread
                    .config_snapshot()
                    .await
                    .session_source
                    .get_agent_role()
                    .as_deref()
                    == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME));
            }
            Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {}
            Err(err) => return Err(err),
        }
        if let Some(state_db) = state.state_db().await
            && let Some(metadata) = state_db.get_thread(agent_id).await.map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to read indexed agent metadata for {agent_id}: {err}"
                ))
            })?
        {
            let stored_source =
                serde_json::from_str::<SessionSource>(&metadata.source).map_err(|err| {
                    CodexErr::Fatal(format!(
                        "failed to parse indexed agent source for {agent_id}: {err}"
                    ))
                })?;
            return Ok(metadata.agent_role.as_deref()
                == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
                || stored_source.get_agent_role().as_deref()
                    == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME));
        }
        match state
            .read_stored_thread(ReadThreadParams {
                thread_id: agent_id,
                include_archived: true,
                include_history: false,
            })
            .await
        {
            Ok(stored) => Ok(stored.agent_role.as_deref()
                == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
                || stored.source.get_agent_role().as_deref()
                    == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)),
            Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    #[cfg(test)]
    pub(crate) async fn close_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        let caller_thread_id = self
            .state
            .agent_id_for_path(&AgentPath::root())
            .ok_or_else(|| CodexErr::Fatal("agent tree is missing its root thread".to_string()))?;
        self.close_agent_subtree(caller_thread_id, agent_id).await?;
        Ok(String::new())
    }

    fn closed_subtree_member_ids(
        &self,
        persisted_subtree: Option<&ClosedThreadSpawnSubtree>,
        prepared_subtree: &[ThreadId],
    ) -> Vec<ThreadId> {
        let mut member_depths = persisted_subtree
            .into_iter()
            .flat_map(|subtree| {
                subtree
                    .members
                    .iter()
                    .map(|member| (member.thread_id, member.depth))
            })
            .collect::<Vec<_>>();
        let mut seen = member_depths
            .iter()
            .map(|(thread_id, _)| *thread_id)
            .collect::<HashSet<_>>();
        for thread_id in prepared_subtree {
            if seen.insert(*thread_id) {
                let depth = self
                    .state
                    .agent_metadata_for_thread(*thread_id)
                    .and_then(|metadata| metadata.depth)
                    .and_then(|depth| u32::try_from(depth).ok())
                    .unwrap_or_default();
                member_depths.push((*thread_id, depth));
            }
        }
        member_depths.sort_unstable_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| left.0.to_string().cmp(&right.0.to_string()))
        });
        member_depths
            .into_iter()
            .map(|(thread_id, _)| thread_id)
            .collect()
    }

    async fn terminate_closed_agent_members(
        &self,
        state: &Arc<ThreadManagerState>,
        member_ids: &[ThreadId],
        report: &mut CloseAgentSubtreeReport,
        cleanup_errors: &mut Vec<String>,
    ) {
        for thread_id in member_ids {
            let was_loaded = state.get_thread(*thread_id).await.is_ok();
            let was_registered = self.state.agent_metadata_for_thread(*thread_id).is_some();
            if !was_loaded && !was_registered {
                continue;
            }
            match self.shutdown_live_agent(*thread_id).await {
                Ok(_) => {}
                Err(err)
                    if matches!(
                        err.details(),
                        CodexErrorDetails::ThreadNotFound(_) | CodexErrorDetails::InternalAgentDied
                    ) => {}
                Err(err) => cleanup_errors.push(format!(
                    "failed to stop runtime for closed agent {thread_id}: {err}"
                )),
            }
            let remains_loaded = state.get_thread(*thread_id).await.is_ok();
            let remains_registered = self.state.agent_metadata_for_thread(*thread_id).is_some();
            report.stopped_runtimes += usize::from(was_loaded && !remains_loaded);
            report.evicted_identities += usize::from(was_registered && !remains_registered);
        }
    }

    async fn cleanup_closed_agent_persistence(
        &self,
        state: &Arc<ThreadManagerState>,
        member_ids: &[ThreadId],
        report: &mut CloseAgentSubtreeReport,
        cleanup_errors: &mut Vec<String>,
    ) {
        let Some(state_db) = state.state_db().await else {
            return;
        };
        match state_db
            .thread_goals()
            .pause_active_thread_goals_and_clear_supervisor_states(member_ids)
            .await
        {
            Ok(counts) => report.paused_goals += counts.paused_goal_threads,
            Err(err) => cleanup_errors.push(format!(
                "failed to pause Goals for closed agent subtree: {err}"
            )),
        }
        match state_db
            .thread_queue()
            .delete_thread_queues(member_ids)
            .await
        {
            Ok(count) => report.cleared_queued_items += count,
            Err(err) => cleanup_errors.push(format!(
                "failed to clear queued input for closed agent subtree: {err}"
            )),
        }
    }
}

fn ordered_current_only_descendant_edges(
    target_thread_id: ThreadId,
    mut included_thread_ids: HashSet<ThreadId>,
    mut parent_by_thread_id: HashMap<ThreadId, ThreadId>,
) -> CodexResult<Vec<codex_state::CurrentOnlyThreadSpawnEdge>> {
    included_thread_ids.insert(target_thread_id);
    let mut edges = Vec::with_capacity(parent_by_thread_id.len());
    while !parent_by_thread_id.is_empty() {
        let mut candidates = parent_by_thread_id
            .iter()
            .map(|(child_thread_id, parent_thread_id)| (*child_thread_id, *parent_thread_id))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(child_thread_id, _)| child_thread_id.to_string());
        let mut progressed = false;
        for (child_thread_id, parent_thread_id) in candidates {
            if !included_thread_ids.contains(&parent_thread_id) {
                continue;
            }
            parent_by_thread_id.remove(&child_thread_id);
            included_thread_ids.insert(child_thread_id);
            edges.push(codex_state::CurrentOnlyThreadSpawnEdge {
                parent_thread_id,
                child_thread_id,
            });
            progressed = true;
        }
        if !progressed {
            return Err(CodexErr::Fatal(format!(
                "current-only agent subtree {target_thread_id} has an invalid parent chain"
            )));
        }
    }
    Ok(edges)
}
