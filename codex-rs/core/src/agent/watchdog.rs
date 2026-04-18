use super::control::AgentControl;
use super::registry::AgentRegistry;
use super::registry::exceeds_thread_spawn_depth_limit;
use super::status::is_final;
use crate::config::Config;
use crate::thread_manager::ThreadManagerState;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::user_input::UserInput;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::warn;

const WATCHDOG_TICK_SECONDS: i64 = 1;
const WATCHDOG_MIN_SNOOZE_SECONDS: u64 = 30;
const WATCHDOG_MAX_SNOOZE_SECONDS: u64 = 60 * 60;

#[derive(Clone)]
pub(crate) struct WatchdogRegistration {
    pub(crate) owner_thread_id: ThreadId,
    pub(crate) target_thread_id: ThreadId,
    pub(crate) child_depth: i32,
    pub(crate) interval_s: i64,
    pub(crate) prompt: String,
    pub(crate) config: Config,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RemovedWatchdog {
    pub(crate) target_thread_id: ThreadId,
    pub(crate) active_helper_id: Option<ThreadId>,
}

struct WatchdogEntry {
    registration: WatchdogRegistration,
    interval: Duration,
    last_trigger: Instant,
    active_helper_id: Option<ThreadId>,
    snoozed_until: Option<Instant>,
    owner_idle_since: Option<Instant>,
    owner_was_running: bool,
    force_due_once: bool,
    generation: i64,
}

pub(crate) struct WatchdogManager {
    manager: Weak<ThreadManagerState>,
    state: Arc<AgentRegistry>,
    registrations: Mutex<HashMap<ThreadId, WatchdogEntry>>,
    suppressed_helpers: Mutex<HashSet<ThreadId>>,
    started: AtomicBool,
    next_generation: AtomicI64,
}

impl WatchdogManager {
    pub(crate) fn new(manager: Weak<ThreadManagerState>, state: Arc<AgentRegistry>) -> Arc<Self> {
        Arc::new(Self {
            manager,
            state,
            registrations: Mutex::new(HashMap::new()),
            suppressed_helpers: Mutex::new(HashSet::new()),
            started: AtomicBool::new(false),
            next_generation: AtomicI64::new(1),
        })
    }

    pub(crate) fn start(self: &Arc<Self>) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let manager = Arc::clone(self);
        tokio::spawn(async move {
            manager.run_loop().await;
        });
    }

    pub(crate) async fn register(
        self: &Arc<Self>,
        registration: WatchdogRegistration,
    ) -> CodexResult<Vec<RemovedWatchdog>> {
        if exceeds_thread_spawn_depth_limit(
            registration.child_depth,
            registration.config.agent_max_depth,
        ) {
            let max_depth = registration.config.agent_max_depth;
            return Err(CodexErr::UnsupportedOperation(format!(
                "agent depth limit reached: max depth is {max_depth}"
            )));
        }

        let interval = interval_duration(registration.interval_s)?;
        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
        let now = Instant::now();
        let entry = WatchdogEntry {
            registration,
            interval,
            last_trigger: now,
            active_helper_id: None,
            snoozed_until: None,
            owner_idle_since: Some(now),
            owner_was_running: false,
            force_due_once: false,
            generation,
        };

        let mut registrations = self.registrations.lock().await;
        let superseded_targets = registrations
            .iter()
            .filter_map(|(target_thread_id, existing_entry)| {
                (existing_entry.registration.owner_thread_id == entry.registration.owner_thread_id
                    && *target_thread_id != entry.registration.target_thread_id)
                    .then_some(*target_thread_id)
            })
            .collect::<Vec<_>>();
        let mut superseded = Vec::new();
        for superseded_target in superseded_targets {
            if let Some(removed) = registrations.remove(&superseded_target) {
                if let Some(helper_id) = removed.active_helper_id {
                    self.suppressed_helpers.lock().await.remove(&helper_id);
                }
                superseded.push(RemovedWatchdog {
                    target_thread_id: superseded_target,
                    active_helper_id: removed.active_helper_id,
                });
            }
        }
        registrations.insert(entry.registration.target_thread_id, entry);
        Ok(superseded)
    }

    pub(crate) async fn unregister_for_owner(
        &self,
        owner_thread_id: ThreadId,
    ) -> Vec<RemovedWatchdog> {
        let mut registrations = self.registrations.lock().await;
        let mut removed = Vec::new();
        registrations.retain(|target_thread_id, entry| {
            if entry.registration.owner_thread_id == owner_thread_id {
                removed.push(RemovedWatchdog {
                    target_thread_id: *target_thread_id,
                    active_helper_id: entry.active_helper_id,
                });
                false
            } else {
                true
            }
        });
        removed
    }

    pub(crate) async fn unregister_handle(
        &self,
        target_thread_id: ThreadId,
    ) -> Option<RemovedWatchdog> {
        let mut registrations = self.registrations.lock().await;
        registrations
            .remove(&target_thread_id)
            .map(|removed| RemovedWatchdog {
                target_thread_id,
                active_helper_id: removed.active_helper_id,
            })
    }

    pub(crate) async fn is_watchdog_handle(&self, target_thread_id: ThreadId) -> bool {
        self.registrations
            .lock()
            .await
            .contains_key(&target_thread_id)
    }

    #[cfg(test)]
    pub(crate) async fn set_active_helper_for_tests(
        &self,
        target_thread_id: ThreadId,
        helper_thread_id: ThreadId,
    ) {
        if let Some(entry) = self.registrations.lock().await.get_mut(&target_thread_id) {
            entry.active_helper_id = Some(helper_thread_id);
        }
    }

    pub(crate) async fn owner_for_active_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> Option<ThreadId> {
        let registrations = self.registrations.lock().await;
        registrations.values().find_map(|entry| {
            (entry.active_helper_id == Some(helper_thread_id))
                .then_some(entry.registration.owner_thread_id)
        })
    }

    pub(crate) async fn target_for_active_helper(
        &self,
        helper_thread_id: ThreadId,
    ) -> Option<ThreadId> {
        let registrations = self.registrations.lock().await;
        registrations.iter().find_map(|(target_thread_id, entry)| {
            (entry.active_helper_id == Some(helper_thread_id)).then_some(*target_thread_id)
        })
    }

    async fn run_loop(self: Arc<Self>) {
        let tick = tick_duration();
        loop {
            self.run_once().await;
            if self.manager.upgrade().is_none() {
                break;
            }
            tokio::time::sleep(tick).await;
        }
    }

    pub(crate) async fn run_once(self: &Arc<Self>) {
        let Some(manager_state) = self.manager.upgrade() else {
            self.registrations.lock().await.clear();
            return;
        };
        let snapshots = {
            let registrations = self.registrations.lock().await;
            registrations
                .iter()
                .map(|(target_id, entry)| (*target_id, entry.generation))
                .collect::<Vec<_>>()
        };
        let now = Instant::now();
        for (target_id, generation) in snapshots {
            self.evaluate(&manager_state, target_id, generation, now)
                .await;
        }
    }

    async fn evaluate(
        self: &Arc<Self>,
        manager_state: &Arc<ThreadManagerState>,
        target_thread_id: ThreadId,
        generation: i64,
        now: Instant,
    ) {
        let Some(snapshot) = self.snapshot(target_thread_id, generation).await else {
            return;
        };

        let owner_thread = manager_state.get_thread(snapshot.owner_thread_id).await;
        let owner_status = match owner_thread.as_ref() {
            Ok(thread) => thread.agent_status().await,
            Err(_) => AgentStatus::NotFound,
        };
        let control_for_spawn = AgentControl::from_parts(
            self.manager.clone(),
            Arc::clone(&self.state),
            Arc::clone(self),
        );
        if is_watchdog_terminated(&owner_status) {
            let _ = control_for_spawn
                .shutdown_live_agent(target_thread_id)
                .await;
            return;
        }

        let force_due = self
            .take_force_due_if_generation(target_thread_id, generation)
            .await;
        let owner_running = is_running(&owner_status) && !force_due;
        let owner_idle_since = self
            .update_owner_idle_state_if_generation(target_thread_id, generation, owner_running, now)
            .await;
        if owner_running {
            return;
        }
        let owner_idle_since = owner_idle_since.or(snapshot.owner_idle_since);
        let Some(owner_idle_since) = owner_idle_since else {
            return;
        };
        if !force_due && now.duration_since(owner_idle_since) < snapshot.interval {
            return;
        }

        if let Some(helper_id) = snapshot.active_helper_id {
            let helper_status = get_status(manager_state, helper_id).await;
            if !is_final(&helper_status) {
                return;
            }
            let helper_suppressed = self.take_suppressed_helper(helper_id).await;
            if let AgentStatus::Completed(Some(message)) = helper_status
                && !helper_suppressed
                && let Err(err) = control_for_spawn
                    .send_watchdog_wakeup(snapshot.owner_thread_id, message)
                    .await
            {
                warn!(
                    helper_id = %helper_id,
                    owner_thread_id = %snapshot.owner_thread_id,
                    "watchdog helper forward failed: {err}"
                );
            }
            let _ = control_for_spawn.shutdown_live_agent(helper_id).await;
            self.update_after_spawn(
                target_thread_id,
                generation,
                now,
                /*active_helper_id*/ None,
            )
            .await;
            return;
        }

        if let Some(snoozed_until) = snapshot.snoozed_until {
            if now < snoozed_until {
                return;
            }
            self.clear_snooze_if_generation(target_thread_id, generation)
                .await;
        }

        if !force_due && now.duration_since(snapshot.last_trigger) < snapshot.interval {
            return;
        }

        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: snapshot.owner_thread_id,
            depth: snapshot.child_depth,
            agent_path: None,
            agent_nickname: None,
            agent_role: Some("watchdog".to_string()),
        });
        let mut helper_config = snapshot.config.clone();
        helper_config.ephemeral = true;
        let helper_prompt = watchdog_helper_prompt(snapshot.owner_thread_id, &snapshot.prompt);
        let spawn_result = control_for_spawn
            .spawn_agent_with_metadata(
                helper_config,
                Op::UserInput {
                    environments: None,
                    items: vec![UserInput::Text {
                        text: helper_prompt,
                        text_elements: Vec::new(),
                    }],
                    final_output_json_schema: None,
                    responsesapi_client_metadata: None,
                },
                Some(session_source),
                Default::default(),
            )
            .await;

        match spawn_result {
            Ok(helper) => {
                self.update_after_spawn(target_thread_id, generation, now, Some(helper.thread_id))
                    .await;
            }
            Err(err) => {
                warn!("watchdog spawn failed for target {target_thread_id}: {err}");
                self.update_after_spawn(
                    target_thread_id,
                    generation,
                    now,
                    /*active_helper_id*/ None,
                )
                .await;
            }
        }
    }

    async fn snapshot(
        &self,
        target_thread_id: ThreadId,
        generation: i64,
    ) -> Option<WatchdogSnapshot> {
        let registrations = self.registrations.lock().await;
        let entry = registrations.get(&target_thread_id)?;
        if entry.generation != generation {
            return None;
        }
        Some(WatchdogSnapshot {
            owner_thread_id: entry.registration.owner_thread_id,
            child_depth: entry.registration.child_depth,
            prompt: entry.registration.prompt.clone(),
            config: entry.registration.config.clone(),
            interval: entry.interval,
            last_trigger: entry.last_trigger,
            active_helper_id: entry.active_helper_id,
            snoozed_until: entry.snoozed_until,
            owner_idle_since: entry.owner_idle_since,
        })
    }

    async fn update_owner_idle_state_if_generation(
        &self,
        target_thread_id: ThreadId,
        generation: i64,
        owner_running: bool,
        now: Instant,
    ) -> Option<Instant> {
        let mut registrations = self.registrations.lock().await;
        let entry = registrations.get_mut(&target_thread_id)?;
        if entry.generation != generation {
            return None;
        }
        if owner_running {
            entry.owner_idle_since = None;
            entry.snoozed_until = None;
            entry.owner_was_running = true;
            return None;
        }
        if entry.owner_was_running || entry.owner_idle_since.is_none() {
            entry.owner_idle_since = Some(now);
        }
        entry.owner_was_running = false;
        entry.owner_idle_since
    }

    async fn take_force_due_if_generation(
        &self,
        target_thread_id: ThreadId,
        generation: i64,
    ) -> bool {
        let mut registrations = self.registrations.lock().await;
        let Some(entry) = registrations.get_mut(&target_thread_id) else {
            return false;
        };
        if entry.generation != generation || !entry.force_due_once {
            return false;
        }
        entry.force_due_once = false;
        true
    }

    async fn clear_snooze_if_generation(&self, target_thread_id: ThreadId, generation: i64) {
        let mut registrations = self.registrations.lock().await;
        let Some(entry) = registrations.get_mut(&target_thread_id) else {
            return;
        };
        if entry.generation == generation {
            entry.snoozed_until = None;
        }
    }

    pub(crate) async fn take_suppressed_helper(&self, helper_thread_id: ThreadId) -> bool {
        self.suppressed_helpers
            .lock()
            .await
            .remove(&helper_thread_id)
    }

    #[cfg(test)]
    pub(crate) async fn helper_is_suppressed_for_tests(&self, helper_thread_id: ThreadId) -> bool {
        self.suppressed_helpers
            .lock()
            .await
            .contains(&helper_thread_id)
    }

    pub(crate) async fn snooze_active_helper(
        &self,
        helper_thread_id: ThreadId,
        requested_delay_seconds: Option<u64>,
    ) -> Option<WatchdogSnoozeResult> {
        let mut registrations = self.registrations.lock().await;
        let (target_thread_id, entry) =
            registrations
                .iter_mut()
                .find_map(|(target_thread_id, entry)| {
                    (entry.active_helper_id == Some(helper_thread_id))
                        .then_some((*target_thread_id, entry))
                })?;
        let delay_seconds = requested_delay_seconds
            .map(|seconds| seconds.clamp(WATCHDOG_MIN_SNOOZE_SECONDS, WATCHDOG_MAX_SNOOZE_SECONDS))
            .unwrap_or_else(|| entry.interval.as_secs().max(1));
        entry.snoozed_until = Some(Instant::now() + Duration::from_secs(delay_seconds));
        entry.active_helper_id = None;
        self.suppressed_helpers
            .lock()
            .await
            .insert(helper_thread_id);
        Some(WatchdogSnoozeResult {
            target_thread_id,
            delay_seconds,
        })
    }

    async fn update_after_spawn(
        &self,
        target_thread_id: ThreadId,
        generation: i64,
        now: Instant,
        active_helper_id: Option<ThreadId>,
    ) {
        let mut registrations = self.registrations.lock().await;
        let Some(entry) = registrations.get_mut(&target_thread_id) else {
            return;
        };
        if entry.generation != generation {
            return;
        }
        entry.last_trigger = now;
        entry.active_helper_id = active_helper_id;
        entry.owner_idle_since = Some(now);
        entry.snoozed_until = None;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WatchdogSnoozeResult {
    pub(crate) target_thread_id: ThreadId,
    pub(crate) delay_seconds: u64,
}

#[derive(Clone)]
struct WatchdogSnapshot {
    owner_thread_id: ThreadId,
    child_depth: i32,
    prompt: String,
    config: Config,
    interval: Duration,
    last_trigger: Instant,
    active_helper_id: Option<ThreadId>,
    snoozed_until: Option<Instant>,
    owner_idle_since: Option<Instant>,
}

fn is_running(status: &AgentStatus) -> bool {
    matches!(status, AgentStatus::PendingInit | AgentStatus::Running)
}

fn is_watchdog_terminated(status: &AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Completed(_)
            | AgentStatus::Errored(_)
            | AgentStatus::Shutdown
            | AgentStatus::NotFound
    )
}

async fn get_status(manager_state: &Arc<ThreadManagerState>, thread_id: ThreadId) -> AgentStatus {
    match manager_state.get_thread(thread_id).await {
        Ok(thread) => thread.agent_status().await,
        Err(_) => AgentStatus::NotFound,
    }
}

fn interval_duration(interval_s: i64) -> CodexResult<Duration> {
    if interval_s <= 0 {
        return Err(CodexErr::UnsupportedOperation(
            "watchdog interval must be greater than zero".to_string(),
        ));
    }
    Ok(Duration::from_secs(interval_s as u64))
}

fn tick_duration() -> Duration {
    Duration::from_secs(WATCHDOG_TICK_SECONDS as u64)
}

fn watchdog_helper_prompt(owner_thread_id: ThreadId, prompt: &str) -> String {
    if prompt.trim().is_empty() {
        format!("Target agent id: {owner_thread_id}")
    } else {
        format!("Target agent id: {owner_thread_id}\n\n{prompt}")
    }
}

#[cfg(test)]
mod tests {
    use super::watchdog_helper_prompt;
    use codex_protocol::ThreadId;

    #[test]
    fn watchdog_helper_prompt_includes_owner_and_task() {
        let owner_thread_id = ThreadId::default();
        assert_eq!(
            watchdog_helper_prompt(owner_thread_id, "check in"),
            format!("Target agent id: {owner_thread_id}\n\ncheck in")
        );
    }
}
