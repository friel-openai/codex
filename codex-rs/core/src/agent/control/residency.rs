use super::LocalAgentControl;
use super::LocalAgentRuntime;
use super::runtime::AgentTreeMembership;
use crate::agent::AgentStatus;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::thread_manager::ThreadManagerState;
use codex_protocol::ThreadId;
use codex_protocol::error::AgentErrorContext;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrKind;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::warn;

/// Session-scoped LRU of loaded V1 and V2 agents that can be reconstructed from persisted rollout.
#[derive(Default)]
pub(super) struct AgentResidency {
    /// Loaded residents plus in-flight reservations for new or reloaded agents.
    state: Mutex<AgentResidencyState>,
}

/// Mutable residency accounting protected by `AgentResidency::state`.
#[derive(Default)]
struct AgentResidencyState {
    /// Loaded agent IDs, ordered from least to most recently used.
    residents: VecDeque<ThreadId>,
    /// Slots reserved before a thread has finished loading and can enter `residents`.
    pending_slots: usize,
}

/// A pending resident slot that must be committed after a thread loads successfully.
pub(super) struct AgentResidencySlot {
    /// Shared LRU that owns the pending slot.
    residency: Arc<AgentResidency>,
    /// Whether dropping this reservation must return the pending slot.
    active: bool,
}

impl AgentResidencySlot {
    pub(super) fn commit(mut self, thread_id: ThreadId) {
        self.residency.commit_slot(thread_id);
        self.active = false;
    }
}

impl Drop for AgentResidencySlot {
    fn drop(&mut self) {
        if self.active {
            self.residency.release_pending_slot();
        }
    }
}

impl LocalAgentControl {
    pub(super) async fn reserve_agent_residency_slot(
        &self,
        state: &Arc<ThreadManagerState>,
        config: &Config,
        multi_agent_version: MultiAgentVersion,
        membership: &AgentTreeMembership,
        protected_thread_id: Option<ThreadId>,
    ) -> CodexResult<AgentResidencySlot> {
        let capacity = config
            .effective_agent_max_threads(multi_agent_version)
            .unwrap_or(usize::MAX);
        Arc::clone(&self.runtime.agent_residency)
            .reserve_slot(self, state, capacity, membership, protected_thread_id)
            .await
    }

    pub(super) async fn touch_loaded_agent_residency(
        &self,
        state: &Arc<ThreadManagerState>,
        thread_id: ThreadId,
    ) {
        if let Ok(thread) = state.get_thread(thread_id).await {
            let _ = self.runtime.pin_v2_residency(state, &thread).await;
        }
    }

    pub(super) fn forget_agent_residency(&self, thread_id: ThreadId) {
        self.runtime.agent_residency.remove(thread_id);
    }
}

impl LocalAgentRuntime {
    /// Pins and touches the registered runtime without waiting for unrelated eviction.
    pub(crate) async fn pin_v2_residency(
        &self,
        state: &ThreadManagerState,
        thread: &Arc<CodexThread>,
    ) -> CodexResult<Option<tokio::sync::OwnedRwLockReadGuard<()>>> {
        // Roots are not evicted, but ownership transfer must also exclude their queued mail.
        let guard = Arc::clone(&thread.residency_gate).read_owned().await;
        let thread_id = thread.session.thread_id;
        if !Arc::ptr_eq(thread, &state.get_thread(thread_id).await?) {
            return Err(CodexErr::ThreadNotFound(thread_id));
        }
        if is_resident_candidate(thread) {
            self.agent_residency.touch(thread_id);
        }
        Ok(Some(guard))
    }
}

/// Result of scanning the current LRU once for an unloadable resident.
enum EvictionResult {
    Unloaded,
    Retry,
    Unavailable,
}

impl AgentResidency {
    async fn reserve_slot(
        self: Arc<Self>,
        control: &LocalAgentControl,
        manager: &Arc<ThreadManagerState>,
        capacity: usize,
        membership: &AgentTreeMembership,
        protected_thread_id: Option<ThreadId>,
    ) -> CodexResult<AgentResidencySlot> {
        loop {
            if self.try_reserve_pending_slot(capacity) {
                return Ok(AgentResidencySlot {
                    residency: self,
                    active: true,
                });
            }
            match self
                .try_unload_one_resident(control, manager, membership, protected_thread_id)
                .await
            {
                EvictionResult::Unloaded | EvictionResult::Retry => {}
                EvictionResult::Unavailable => {
                    return Err(CodexErr::new(CodexErrorDetails::AgentLimitReached {
                        max_threads: capacity,
                    })
                    .with_agent_context(AgentErrorContext::ResidencyCapacity));
                }
            }
        }
    }

    fn try_reserve_pending_slot(&self, capacity: usize) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.residents.len().saturating_add(state.pending_slots) >= capacity {
            return false;
        }
        state.pending_slots += 1;
        true
    }

    async fn try_unload_one_resident(
        self: &Arc<Self>,
        control: &LocalAgentControl,
        manager: &Arc<ThreadManagerState>,
        membership: &AgentTreeMembership,
        protected_thread_id: Option<ThreadId>,
    ) -> EvictionResult {
        // Keep shutting-down workers counted until removal. Each runtime's write guard
        // excludes delivery and competing evictions without blocking unrelated workers.
        let candidates = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .residents
            .clone();
        let mut pending_completion = None;
        for candidate_thread_id in candidates {
            if Some(candidate_thread_id) == protected_thread_id {
                continue;
            }
            let candidate_thread = {
                let threads = manager.threads.read().await;
                match threads.get(&candidate_thread_id) {
                    Some(thread) if is_resident_candidate(thread) => Arc::clone(thread),
                    Some(_) | None => {
                        // A reload cannot publish between the lookup and stale-entry removal.
                        self.remove(candidate_thread_id);
                        return EvictionResult::Unloaded;
                    }
                }
            };
            let lifecycle = control
                .runtime
                .registry
                .agent_lifecycle(candidate_thread_id)
                .unwrap_or_default();
            let Some(transition) = lifecycle.try_lock_transition() else {
                continue;
            };
            let Ok(residency_guard) =
                Arc::clone(&candidate_thread.residency_gate).try_write_owned()
            else {
                continue;
            };
            if !manager
                .get_thread(candidate_thread_id)
                .await
                .is_ok_and(|registered| Arc::ptr_eq(&registered, &candidate_thread))
                || !is_unloadable(candidate_thread.as_ref()).await
            {
                continue;
            }
            if lifecycle.completion_watcher_active() {
                self.touch(candidate_thread_id);
                drop(residency_guard);
                drop(transition);
                pending_completion = Some(lifecycle);
                continue;
            }
            // Once shutdown is submitted, cancellation cannot revoke it. The eviction task
            // keeps both lifecycle and delivery excluded, and capacity reserved, through removal.
            let manager = Arc::clone(manager);
            let residency = Arc::clone(self);
            let teardown = membership
                .clone()
                .into_teardown_guard("resident_eviction", Some(candidate_thread_id));
            let eviction = tokio::spawn(async move {
                let _transition = transition;
                let _residency_guard = residency_guard;
                candidate_thread.ensure_rollout_materialized().await;
                if let Err(err) = candidate_thread.shutdown_and_wait().await {
                    teardown
                        .record_shutdown_failure("stop_resident", CodexErrKind::from(&err).into());
                    warn!(
                        "failed to stop resident thread before unloading {candidate_thread_id}: {err}"
                    );
                    teardown.complete();
                    return false;
                }
                // The submission loop has stopped and the residency guard excludes senders.
                // Preserve unread queue-only mail before dropping the session that held it.
                let mail = candidate_thread
                    .session
                    .input_queue
                    .drain_mailbox()
                    .await
                    .into_iter()
                    .map(|mail| mail.communication)
                    .collect();
                // A concurrent tree shutdown deliberately discards its unread mail.
                let _ = candidate_thread
                    .session
                    .services
                    .local_agent_runtime
                    .mailboxes
                    .enqueue(candidate_thread_id, /*id*/ None, mail);
                let environments = candidate_thread.environment_selections().await;
                let mut threads = manager.threads.write().await;
                if threads
                    .get(&candidate_thread_id)
                    .is_some_and(|registered| !Arc::ptr_eq(registered, &candidate_thread))
                {
                    teardown.complete();
                    return false;
                }
                candidate_thread
                    .session
                    .services
                    .local_agent_runtime
                    .registry
                    .save_evicted_environments(candidate_thread_id, environments);
                // Keep publication excluded until both entries have been removed.
                threads.remove(&candidate_thread_id);
                residency.remove(candidate_thread_id);
                teardown.complete();
                true
            });
            match eviction.await {
                Ok(true) => return EvictionResult::Unloaded,
                Ok(false) => {}
                Err(err) => warn!("resident eviction task failed: {err}"),
            }
        }
        if let Some(lifecycle) = pending_completion {
            lifecycle.wait_for_completion_watcher().await;
            EvictionResult::Retry
        } else {
            EvictionResult::Unavailable
        }
    }

    fn touch(&self, thread_id: ThreadId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        touch_resident(&mut state.residents, thread_id);
    }

    fn remove(&self, thread_id: ThreadId) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .residents
            .retain(|resident_thread_id| *resident_thread_id != thread_id);
    }

    fn commit_slot(&self, thread_id: ThreadId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending_slots = state.pending_slots.saturating_sub(1);
        touch_resident(&mut state.residents, thread_id);
    }

    fn release_pending_slot(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending_slots = state.pending_slots.saturating_sub(1);
    }
}

fn touch_resident(residents: &mut VecDeque<ThreadId>, thread_id: ThreadId) {
    residents.retain(|resident_thread_id| *resident_thread_id != thread_id);
    residents.push_back(thread_id);
}

fn is_resident_candidate(thread: &CodexThread) -> bool {
    is_resident_session_source(&thread.session_source)
}

pub(super) fn is_resident_session_source(session_source: &SessionSource) -> bool {
    matches!(session_source, SessionSource::SubAgent(_))
        && !crate::goal_supervisor::is_goal_supervisor_helper_source(session_source)
}

pub(super) async fn is_unloadable(thread: &CodexThread) -> bool {
    matches!(
        thread.agent_status().await,
        AgentStatus::Completed(_)
            | AgentStatus::Errored(_)
            | AgentStatus::Interrupted
            | AgentStatus::Shutdown
    ) && thread.session.active_turn.lock().await.is_none()
        && !thread.session.has_outstanding_durable_sleep()
        && !thread
            .session
            .input_queue
            .has_trigger_turn_mailbox_items()
            .await
}

impl LocalAgentControl {
    /// Persist and stop a loaded agent without releasing its addressability metadata.
    pub(super) async fn unload_agent_thread(
        &self,
        manager: &Arc<ThreadManagerState>,
        thread_id: ThreadId,
    ) -> CodexResult<bool> {
        let Ok(thread) = manager.get_thread(thread_id).await else {
            return Ok(false);
        };
        thread.ensure_rollout_materialized().await;
        thread.flush_rollout().await?;
        let environments = thread.environment_selections().await;
        thread.shutdown_and_wait().await?;
        thread
            .session
            .services
            .local_agent_runtime
            .registry
            .save_evicted_environments(thread_id, environments);
        Ok(manager.remove_thread(&thread_id).await.is_some())
    }
}

#[cfg(test)]
#[path = "residency_tests.rs"]
mod tests;
