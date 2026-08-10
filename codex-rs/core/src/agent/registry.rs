use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use rand::prelude::IndexedRandom;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::OwnedMutexGuard;

/// This structure is used to add some limits on the multi-agent capabilities for Codex. In
/// the current implementation, it limits:
/// * Total number of sub-agents (i.e. threads) per user session
///
/// This structure is shared by all agents in the same user session (because the `AgentControl`
/// is).
#[derive(Default)]
pub(crate) struct AgentRegistry {
    active_agents: Mutex<ActiveAgents>,
    total_count: AtomicUsize,
}

#[derive(Default)]
struct ActiveAgents {
    agent_tree: HashMap<String, AgentMetadata>,
    thread_paths: HashMap<ThreadId, String>,
    used_agent_nicknames: HashSet<String>,
    nickname_reset_count: usize,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AgentMetadata {
    pub(crate) agent_id: Option<ThreadId>,
    /// Immediate owner in the current root-scoped agent tree.
    pub(crate) parent_thread_id: Option<ThreadId>,
    /// Depth recorded by the authoritative open ownership path.
    pub(crate) depth: Option<i32>,
    pub(crate) agent_path: Option<AgentPath>,
    pub(crate) agent_nickname: Option<String>,
    pub(crate) agent_role: Option<String>,
    /// Whether this identity belongs to an ephemeral thread with no persisted ownership edge.
    pub(crate) ephemeral: bool,
    pub(crate) last_task_message: Option<String>,
    /// Serializes loaded/cold transitions for this addressable agent. The lock lives with the
    /// registry entry so unloading the heavy `CodexThread` does not permit concurrent reloads.
    pub(crate) lifecycle: Arc<AgentLifecycle>,
}

/// Runtime coordination retained for an addressable agent after its `CodexThread` becomes cold.
#[derive(Debug, Default)]
pub(crate) struct AgentLifecycle {
    /// Guards all loaded/cold transitions and submissions that require a loaded thread.
    transition: Arc<AsyncMutex<()>>,
    /// Keeps a transactionally transferred descendant discoverable while its runtime stays cold.
    visible_when_cold: AtomicBool,
    /// Preserves a completed agent's final status after its heavy thread state is unloaded.
    cold_terminal_status: Mutex<Option<AgentStatus>>,
    /// Prevents duplicate completion-watcher tasks for one registered agent.
    completion_watcher_registered: AtomicBool,
    /// Blocks delivery and residency changes while a terminal status awaits parent notification.
    completion_transition_pending: AtomicBool,
    /// Retains terminal status events until parent delivery is acknowledged.
    completion_statuses: Mutex<CompletionStatusState>,
    /// Wakes the registered watcher when a terminal status is recorded.
    completion_status_available: Notify,
    /// Wakes input delivery after the pending completion transition finishes.
    completion_watcher_finished: Notify,
}

/// One terminal status event awaiting delivery to the parent agent.
#[derive(Clone, Debug)]
struct CompletionStatusEntry {
    event_id: String,
    status: AgentStatus,
}

/// One terminal status reserved for delivery until its parent mutation succeeds.
#[derive(Clone, Debug)]
pub(crate) struct CompletionStatusClaim {
    entry: CompletionStatusEntry,
}

impl CompletionStatusClaim {
    pub(crate) fn event_id(&self) -> &str {
        &self.entry.event_id
    }

    pub(crate) fn status(&self) -> &AgentStatus {
        &self.entry.status
    }
}

/// Lossless completion state kept separately from the latest-value `AgentStatus` watch channel.
#[derive(Debug, Default)]
struct CompletionStatusState {
    pending: VecDeque<CompletionStatusEntry>,
    last_claimed_event_id: Option<String>,
    publication_generation: u64,
}

/// Selects whether a terminal can create deferred work without canonical parent metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompletionRecordMode {
    #[cfg(test)]
    Retain,
    RetainAndRegisterWatcher,
    RequireRegisteredWatcher,
}

/// Result of publishing one definitive terminal status into the lifecycle.
pub(crate) struct CompletionStatusPublication {
    pub(crate) recorded: bool,
    pub(crate) registration: Option<CompletionWatcherRegistration>,
}

/// Why input delivery stopped waiting for an agent's completion watcher.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompletionWatcherWaitOutcome {
    WatcherRetired,
    PendingTerminal,
}

/// Clears completion-watcher ownership even when the watcher exits through an error path.
pub(crate) struct CompletionWatcherRegistration {
    lifecycle: Arc<AgentLifecycle>,
    registered: bool,
}

impl AgentLifecycle {
    pub(crate) async fn lock_transition(self: &Arc<Self>) -> OwnedMutexGuard<()> {
        Arc::clone(&self.transition).lock_owned().await
    }

    pub(crate) fn completion_watcher_active(&self) -> bool {
        self.completion_transition_pending.load(Ordering::Acquire)
    }

    pub(crate) fn completion_watcher_registered(&self) -> bool {
        self.completion_watcher_registered.load(Ordering::Acquire)
    }

    pub(crate) fn mark_visible_when_cold(&self) {
        self.visible_when_cold.store(true, Ordering::Release);
    }

    pub(crate) fn is_visible_when_cold(&self) -> bool {
        self.visible_when_cold.load(Ordering::Acquire)
    }

    pub(crate) fn try_start_completion_watcher(
        self: &Arc<Self>,
    ) -> Option<CompletionWatcherRegistration> {
        let _state = self
            .completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.try_start_completion_watcher_locked()
    }

    fn try_start_completion_watcher_locked(
        self: &Arc<Self>,
    ) -> Option<CompletionWatcherRegistration> {
        self.completion_watcher_registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(CompletionWatcherRegistration {
            lifecycle: Arc::clone(self),
            registered: true,
        })
    }

    /// Records a terminal status before the latest-value status channel can advance again.
    #[cfg(test)]
    pub(crate) fn record_completion_status(
        self: &Arc<Self>,
        event_id: String,
        status: AgentStatus,
    ) -> bool {
        self.publish_completion_status_inner(event_id, status, CompletionRecordMode::Retain, || {})
            .recorded
    }

    /// Publishes a terminal status and reserves its watcher before the status becomes observable.
    pub(crate) fn publish_completion_status<F>(
        self: &Arc<Self>,
        event_id: String,
        status: AgentStatus,
        publish_status: F,
    ) -> CompletionStatusPublication
    where
        F: FnOnce(),
    {
        self.publish_completion_status_inner(
            event_id,
            status,
            CompletionRecordMode::RetainAndRegisterWatcher,
            publish_status,
        )
    }

    /// Publishes a terminal only while an existing watcher owns incomplete legacy metadata.
    pub(crate) fn publish_completion_status_for_registered_watcher<F>(
        self: &Arc<Self>,
        event_id: String,
        status: AgentStatus,
        publish_status: F,
    ) -> CompletionStatusPublication
    where
        F: FnOnce(),
    {
        self.publish_completion_status_inner(
            event_id,
            status,
            CompletionRecordMode::RequireRegisteredWatcher,
            publish_status,
        )
    }

    fn publish_completion_status_inner<F>(
        self: &Arc<Self>,
        event_id: String,
        status: AgentStatus,
        mode: CompletionRecordMode,
        publish_status: F,
    ) -> CompletionStatusPublication
    where
        F: FnOnce(),
    {
        let mut state = self
            .completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if mode == CompletionRecordMode::RequireRegisteredWatcher
            && !self.completion_watcher_registered.load(Ordering::Acquire)
        {
            publish_status();
            return CompletionStatusPublication {
                recorded: false,
                registration: None,
            };
        }
        if state.last_claimed_event_id.as_deref() == Some(event_id.as_str()) {
            publish_status();
            return CompletionStatusPublication {
                recorded: false,
                registration: None,
            };
        }
        if let Some(existing) = state
            .pending
            .iter_mut()
            .find(|entry| entry.event_id == event_id)
        {
            existing.status = status;
        } else {
            state
                .pending
                .push_back(CompletionStatusEntry { event_id, status });
        }
        state.publication_generation = state.publication_generation.wrapping_add(1);
        self.completion_transition_pending
            .store(true, Ordering::Release);
        let registration = (mode == CompletionRecordMode::RetainAndRegisterWatcher)
            .then(|| self.try_start_completion_watcher_locked())
            .flatten();
        publish_status();
        drop(state);
        self.completion_status_available.notify_waiters();
        CompletionStatusPublication {
            recorded: true,
            registration,
        }
    }

    /// Reserves delivery while an ObserveCurrent watcher checks the existing status.
    pub(crate) fn begin_completion_transition(&self) {
        self.completion_transition_pending
            .store(true, Ordering::Release);
    }

    pub(crate) fn has_pending_completion_status(&self) -> bool {
        !self
            .completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .is_empty()
    }

    /// Returns the oldest terminal without removing it from durable-delivery ownership.
    pub(crate) fn try_claim_completion_status(&self) -> Option<CompletionStatusClaim> {
        self.completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .front()
            .cloned()
            .map(|entry| CompletionStatusClaim { entry })
    }

    /// Removes a terminal only after the watcher has durably notified its canonical parent.
    pub(crate) fn acknowledge_completion_status(&self, claim: &CompletionStatusClaim) -> bool {
        let mut state = self
            .completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.front().map(|entry| entry.event_id.as_str())
            != Some(claim.entry.event_id.as_str())
        {
            return false;
        }
        let Some(entry) = state.pending.pop_front() else {
            return false;
        };
        state.last_claimed_event_id = Some(entry.event_id);
        true
    }

    pub(crate) async fn wait_for_completion_status_claim(&self) -> CompletionStatusClaim {
        loop {
            if let Some(claim) = self.try_claim_completion_status() {
                return claim;
            }
            self.finish_completion_transition();
            let available = self.completion_status_available.notified();
            tokio::pin!(available);
            // `notify_waiters` stores no permit, so enable before the second queue check to cover
            // a notification delivered between constructing and polling `Notified`.
            let _ = available.as_mut().enable();
            if let Some(claim) = self.try_claim_completion_status() {
                return claim;
            }
            available.await;
        }
    }

    pub(crate) fn finish_completion_transition(&self) {
        let state = self
            .completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.pending.is_empty() {
            return;
        }
        self.completion_transition_pending
            .store(false, Ordering::Release);
        drop(state);
        self.completion_watcher_finished.notify_waiters();
    }

    pub(crate) async fn wait_for_completion_watcher(&self) {
        while self.completion_watcher_active() {
            let finished = self.completion_watcher_finished.notified();
            tokio::pin!(finished);
            // `notify_waiters` does not store a permit. Register before the atomic recheck so a
            // watcher cannot finish between the recheck and the first poll of `Notified`.
            let _ = finished.as_mut().enable();
            if !self.completion_watcher_active() {
                return;
            }
            finished.await;
        }
    }

    /// Waits for watcher retirement without missing a terminal published by the watcher target.
    pub(crate) async fn wait_for_completion_watcher_or_pending_terminal(
        &self,
        transition: OwnedMutexGuard<()>,
    ) -> CompletionWatcherWaitOutcome {
        let mut transition = Some(transition);
        let mut initial_publication_generation = None;
        loop {
            let status_available = self.completion_status_available.notified();
            let watcher_finished = self.completion_watcher_finished.notified();
            tokio::pin!(status_available);
            tokio::pin!(watcher_finished);
            let _ = status_available.as_mut().enable();
            let _ = watcher_finished.as_mut().enable();

            let outcome = {
                let state = self
                    .completion_statuses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let initial_generation =
                    *initial_publication_generation.get_or_insert(state.publication_generation);
                if !state.pending.is_empty() || state.publication_generation != initial_generation {
                    Some(CompletionWatcherWaitOutcome::PendingTerminal)
                } else if !self.completion_watcher_active() {
                    Some(CompletionWatcherWaitOutcome::WatcherRetired)
                } else {
                    None
                }
            };
            if let Some(outcome) = outcome {
                return outcome;
            }

            drop(transition.take());
            tokio::select! {
                _ = status_available.as_mut() => {}
                _ = watcher_finished.as_mut() => {}
            }
        }
    }
}

impl CompletionWatcherRegistration {
    /// Retires this watcher only if no terminal status arrived after its quiescence check.
    pub(crate) fn try_retire(&mut self) -> bool {
        let state = self
            .lifecycle
            .completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.pending.is_empty() {
            return false;
        }
        self.registered = false;
        self.lifecycle
            .completion_watcher_registered
            .store(false, Ordering::Release);
        self.lifecycle
            .completion_transition_pending
            .store(false, Ordering::Release);
        drop(state);
        self.lifecycle.completion_watcher_finished.notify_waiters();
        true
    }

    fn clear_registration(&mut self) {
        if !self.registered {
            return;
        }
        let mut state = self
            .lifecycle
            .completion_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.registered = false;
        state.pending.clear();
        self.lifecycle
            .completion_watcher_registered
            .store(false, Ordering::Release);
        self.lifecycle
            .completion_transition_pending
            .store(false, Ordering::Release);
        drop(state);
        self.lifecycle.completion_watcher_finished.notify_waiters();
    }
}

impl Drop for CompletionWatcherRegistration {
    fn drop(&mut self) {
        self.clear_registration();
    }
}

fn format_agent_nickname(name: &str, nickname_reset_count: usize) -> String {
    match nickname_reset_count {
        0 => name.to_string(),
        reset_count => {
            let value = reset_count + 1;
            let suffix = match value % 100 {
                11..=13 => "th",
                _ => match value % 10 {
                    1 => "st", // codespell:ignore
                    2 => "nd", // codespell:ignore
                    3 => "rd", // codespell:ignore
                    _ => "th", // codespell:ignore
                },
            };
            format!("{name} the {value}{suffix}")
        }
    }
}

fn session_depth(session_source: &SessionSource) -> i32 {
    match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { depth, .. }) => *depth,
        SessionSource::SubAgent(_) => 0,
        _ => 0,
    }
}

pub(crate) fn next_thread_spawn_depth(session_source: &SessionSource) -> i32 {
    session_depth(session_source).saturating_add(1)
}

pub(crate) fn exceeds_thread_spawn_depth_limit(depth: i32, max_depth: i32) -> bool {
    depth > max_depth
}

fn is_uncounted_agent_metadata(agent_metadata: &AgentMetadata) -> bool {
    matches!(
        agent_metadata.agent_role.as_deref(),
        Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
    )
}

impl AgentRegistry {
    pub(crate) fn registered_subtree_thread_ids(&self, root_thread_id: ThreadId) -> Vec<ThreadId> {
        let active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut children = HashMap::<ThreadId, Vec<ThreadId>>::new();
        for metadata in active_agents.agent_tree.values() {
            let (Some(thread_id), Some(parent_thread_id)) =
                (metadata.agent_id, metadata.parent_thread_id)
            else {
                continue;
            };
            children
                .entry(parent_thread_id)
                .or_default()
                .push(thread_id);
        }
        let mut subtree = vec![root_thread_id];
        let mut stack = children.remove(&root_thread_id).unwrap_or_default();
        while let Some(thread_id) = stack.pop() {
            subtree.push(thread_id);
            stack.extend(children.remove(&thread_id).unwrap_or_default());
        }
        subtree
    }

    /// Return registered ephemeral descendants whose parent chain stays within `owned_thread_ids`.
    pub(crate) fn registered_ephemeral_descendants_within(
        &self,
        owned_thread_ids: &HashSet<ThreadId>,
    ) -> Vec<ThreadId> {
        let active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut included = owned_thread_ids.clone();
        let mut descendants = Vec::new();
        loop {
            let mut changed = false;
            for metadata in active_agents.agent_tree.values() {
                let (Some(thread_id), Some(parent_thread_id)) =
                    (metadata.agent_id, metadata.parent_thread_id)
                else {
                    continue;
                };
                if metadata.ephemeral
                    && included.contains(&parent_thread_id)
                    && included.insert(thread_id)
                {
                    descendants.push(thread_id);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        descendants.sort_by_key(ToString::to_string);
        descendants
    }

    pub(crate) fn reserve_spawn_slot(
        self: &Arc<Self>,
        max_threads: Option<usize>,
    ) -> Result<SpawnReservation> {
        if let Some(max_threads) = max_threads {
            if !self.try_increment_spawned(max_threads) {
                return Err(CodexErr::new(CodexErrorDetails::AgentLimitReached {
                    max_threads,
                }));
            }
        } else {
            self.total_count.fetch_add(1, Ordering::AcqRel);
        }
        Ok(SpawnReservation {
            state: Arc::clone(self),
            active: true,
            reserved_agent_nickname: None,
            reserved_agent_path: None,
            counted: true,
        })
    }

    pub(crate) fn reserve_uncounted_spawn_slot(self: &Arc<Self>) -> SpawnReservation {
        SpawnReservation {
            state: Arc::clone(self),
            active: true,
            reserved_agent_nickname: None,
            reserved_agent_path: None,
            counted: false,
        }
    }

    pub(crate) fn release_spawned_thread(&self, thread_id: ThreadId) {
        let removed_counted_agent = {
            let mut active_agents = self
                .active_agents
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            active_agents
                .thread_paths
                .remove(&thread_id)
                .and_then(|key| active_agents.agent_tree.remove(key.as_str()))
                .is_some_and(|metadata| {
                    !metadata.agent_path.as_ref().is_some_and(AgentPath::is_root)
                        && !is_uncounted_agent_metadata(&metadata)
                })
        };
        if removed_counted_agent {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn register_root_thread(&self, thread_id: ThreadId) {
        let mut active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root_path = AgentPath::ROOT.to_string();
        let root_thread_id = active_agents
            .agent_tree
            .entry(root_path.clone())
            .or_insert_with(|| AgentMetadata {
                agent_id: Some(thread_id),
                agent_path: Some(AgentPath::root()),
                ..Default::default()
            })
            .agent_id;
        if let Some(root_thread_id) = root_thread_id {
            active_agents.thread_paths.insert(root_thread_id, root_path);
        }
    }

    pub(crate) fn agent_id_for_path(&self, agent_path: &AgentPath) -> Option<ThreadId> {
        self.active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .agent_tree
            .get(agent_path.as_str())
            .and_then(|metadata| metadata.agent_id)
    }

    pub(crate) fn agent_metadata_for_thread(&self, thread_id: ThreadId) -> Option<AgentMetadata> {
        let active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active_agents
            .thread_paths
            .get(&thread_id)
            .and_then(|path| active_agents.agent_tree.get(path))
            .cloned()
    }

    pub(crate) fn registered_path_prefix_thread_ids(
        &self,
        agent_path: &AgentPath,
    ) -> Vec<ThreadId> {
        let active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active_agents
            .agent_tree
            .iter()
            .filter_map(|(registered_path, metadata)| {
                let suffix = agent_path.as_str().strip_prefix(registered_path)?;
                (suffix.is_empty() || suffix.starts_with('/'))
                    .then_some(metadata.agent_id)
                    .flatten()
            })
            .collect()
    }

    pub(crate) fn live_agents(&self) -> Vec<AgentMetadata> {
        self.active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .agent_tree
            .values()
            .filter(|metadata| {
                metadata.agent_id.is_some()
                    && !metadata.agent_path.as_ref().is_some_and(AgentPath::is_root)
            })
            .cloned()
            .collect()
    }

    pub(crate) fn update_last_task_message(&self, thread_id: ThreadId, last_task_message: String) {
        let mut active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metadata) = active_agents
            .agent_tree
            .values_mut()
            .find(|metadata| metadata.agent_id == Some(thread_id))
        {
            metadata.last_task_message = Some(last_task_message);
        }
    }

    pub(crate) fn clear_last_task_message(&self, thread_id: ThreadId) {
        let mut active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metadata) = active_agents
            .agent_tree
            .values_mut()
            .find(|metadata| metadata.agent_id == Some(thread_id))
        {
            metadata.last_task_message = None;
        }
    }

    fn register_spawned_thread(&self, agent_metadata: AgentMetadata) {
        let Some(thread_id) = agent_metadata.agent_id else {
            return;
        };
        let mut active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = agent_metadata
            .agent_path
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("thread:{thread_id}"));
        if let Some(agent_nickname) = agent_metadata.agent_nickname.clone() {
            active_agents.used_agent_nicknames.insert(agent_nickname);
        }
        if let Some(previous_key) = active_agents.thread_paths.insert(thread_id, key.clone())
            && previous_key != key
        {
            active_agents.agent_tree.remove(previous_key.as_str());
        }
        if let Some(previous_metadata) = active_agents.agent_tree.insert(key, agent_metadata)
            && let Some(previous_thread_id) = previous_metadata.agent_id
            && previous_thread_id != thread_id
        {
            active_agents.thread_paths.remove(&previous_thread_id);
        }
    }

    fn reserve_agent_nickname(&self, names: &[&str], preferred: Option<&str>) -> Option<String> {
        let mut active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let agent_nickname = if let Some(preferred) = preferred {
            preferred.to_string()
        } else {
            if names.is_empty() {
                return None;
            }
            let available_names: Vec<String> = names
                .iter()
                .map(|name| format_agent_nickname(name, active_agents.nickname_reset_count))
                .filter(|name| !active_agents.used_agent_nicknames.contains(name))
                .collect();
            if let Some(name) = available_names.choose(&mut rand::rng()) {
                name.clone()
            } else {
                active_agents.used_agent_nicknames.clear();
                active_agents.nickname_reset_count += 1;
                if let Some(metrics) = codex_otel::global() {
                    let _ = metrics.counter(
                        "codex.multi_agent.nickname_pool_reset",
                        /*inc*/ 1,
                        &[],
                    );
                }
                format_agent_nickname(
                    names.choose(&mut rand::rng())?,
                    active_agents.nickname_reset_count,
                )
            }
        };
        active_agents
            .used_agent_nicknames
            .insert(agent_nickname.clone());
        Some(agent_nickname)
    }

    fn reserve_agent_path(&self, agent_path: &AgentPath) -> Result<()> {
        let mut active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match active_agents.agent_tree.entry(agent_path.to_string()) {
            Entry::Occupied(_) => Err(CodexErr::UnsupportedOperation(format!(
                "agent path `{agent_path}` already exists"
            ))),
            Entry::Vacant(entry) => {
                entry.insert(AgentMetadata {
                    agent_path: Some(agent_path.clone()),
                    ..Default::default()
                });
                Ok(())
            }
        }
    }

    fn release_reserved_agent_path(&self, agent_path: &AgentPath) {
        let mut active_agents = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active_agents
            .agent_tree
            .get(agent_path.as_str())
            .is_some_and(|metadata| metadata.agent_id.is_none())
        {
            active_agents.agent_tree.remove(agent_path.as_str());
        }
    }

    fn try_increment_spawned(&self, max_threads: usize) -> bool {
        let mut current = self.total_count.load(Ordering::Acquire);
        loop {
            if current >= max_threads {
                return false;
            }
            match self.total_count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(updated) => current = updated,
            }
        }
    }
}

pub(crate) struct SpawnReservation {
    state: Arc<AgentRegistry>,
    active: bool,
    reserved_agent_nickname: Option<String>,
    reserved_agent_path: Option<AgentPath>,
    counted: bool,
}

impl SpawnReservation {
    pub(crate) fn reserve_agent_nickname_with_preference(
        &mut self,
        names: &[&str],
        preferred: Option<&str>,
    ) -> Result<String> {
        let agent_nickname = self
            .state
            .reserve_agent_nickname(names, preferred)
            .ok_or_else(|| {
                CodexErr::UnsupportedOperation("no available agent nicknames".to_string())
            })?;
        self.reserved_agent_nickname = Some(agent_nickname.clone());
        Ok(agent_nickname)
    }

    pub(crate) fn reserve_agent_path(&mut self, agent_path: &AgentPath) -> Result<()> {
        self.state.reserve_agent_path(agent_path)?;
        self.reserved_agent_path = Some(agent_path.clone());
        Ok(())
    }

    pub(crate) fn commit(mut self, agent_metadata: AgentMetadata) {
        self.reserved_agent_nickname = None;
        self.reserved_agent_path = None;
        self.state.register_spawned_thread(agent_metadata);
        self.active = false;
    }
}

impl Drop for SpawnReservation {
    fn drop(&mut self) {
        if self.active {
            if let Some(agent_path) = self.reserved_agent_path.take() {
                self.state.release_reserved_agent_path(&agent_path);
            }
            if self.counted {
                self.state.total_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
