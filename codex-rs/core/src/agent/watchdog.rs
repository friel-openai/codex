use super::control::AgentControl;
use super::guards::Guards;
use super::guards::MAX_THREAD_SPAWN_DEPTH;
use super::guards::exceeds_thread_spawn_depth_limit;
use super::status::is_final;
use crate::codex::load_watchdog_prompt;
use crate::config::Config;
use crate::error::CodexErr;
use crate::error::Result as CodexResult;
use crate::thread_manager::ThreadManagerState;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

pub(crate) const DEFAULT_WATCHDOG_INTERVAL_S: i64 = 60;

const WATCHDOG_TICK_SECONDS: i64 = 5;

#[derive(Clone)]
pub(crate) struct WatchdogRegistration {
    pub(crate) owner_thread_id: ThreadId,
    pub(crate) target_thread_id: ThreadId,
    pub(crate) child_depth: i32,
    pub(crate) interval_s: i64,
    pub(crate) prompt: String,
    pub(crate) config: Config,
}

struct WatchdogEntry {
    registration: WatchdogRegistration,
    interval: Duration,
    last_trigger: Instant,
    active_helper_id: Option<ThreadId>,
    generation: i64,
}

pub(crate) struct WatchdogManager {
    manager: Weak<ThreadManagerState>,
    guards: Arc<Guards>,
    registrations: Mutex<HashMap<ThreadId, WatchdogEntry>>,
    started: AtomicBool,
    next_generation: AtomicI64,
}

impl WatchdogManager {
    pub(crate) fn new(manager: Weak<ThreadManagerState>, guards: Arc<Guards>) -> Arc<Self> {
        Arc::new(Self {
            manager,
            guards,
            registrations: Mutex::new(HashMap::new()),
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
    ) -> CodexResult<()> {
        if exceeds_thread_spawn_depth_limit(registration.child_depth) {
            return Err(CodexErr::UnsupportedOperation(format!(
                "agent depth limit reached: max depth is {MAX_THREAD_SPAWN_DEPTH}"
            )));
        }
        let interval = interval_duration(registration.interval_s)?;
        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
        let entry = WatchdogEntry {
            registration,
            interval,
            last_trigger: Instant::now(),
            active_helper_id: None,
            generation,
        };

        let mut registrations = self.registrations.lock().await;
        registrations.insert(entry.registration.target_thread_id, entry);
        Ok(())
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

        let snapshots: Vec<(ThreadId, i64)> = {
            let registrations = self.registrations.lock().await;
            registrations
                .iter()
                .map(|(target_id, entry)| (*target_id, entry.generation))
                .collect()
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

        let target_status = get_status(manager_state, snapshot.target_thread_id).await;
        if is_final(&target_status) {
            self.remove_if_generation(target_thread_id, generation)
                .await;
            return;
        }

        let mut active_helper_id = snapshot.active_helper_id;
        if let Some(helper_id) = snapshot.active_helper_id {
            let helper_status = get_status(manager_state, helper_id).await;
            if is_final(&helper_status) {
                self.clear_active_helper_if_generation(target_thread_id, generation)
                    .await;
                active_helper_id = None;
            } else {
                return;
            }
        }

        if active_helper_id.is_some()
            || now.duration_since(snapshot.last_trigger) < snapshot.interval
        {
            return;
        }

        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: snapshot.owner_thread_id,
            depth: snapshot.child_depth,
        });
        let helper_prompt = watchdog_prompt(
            &snapshot.config,
            snapshot.target_thread_id,
            &snapshot.prompt,
        )
        .await;
        let control_for_spawn = AgentControl::from_parts(
            self.manager.clone(),
            Arc::clone(&self.guards),
            Arc::clone(self),
        );

        let spawn_result = control_for_spawn
            .fork_agent(
                snapshot.config.clone(),
                helper_prompt,
                snapshot.target_thread_id,
                usize::MAX,
                session_source,
            )
            .await;

        match spawn_result {
            Ok(helper_id) => {
                info!("watchdog spawned helper {helper_id} for target {target_thread_id}");
                self.update_after_spawn(target_thread_id, generation, now, Some(helper_id))
                    .await;
            }
            Err(err) => {
                warn!("watchdog spawn failed for target {target_thread_id}: {err}");
                self.update_after_spawn(target_thread_id, generation, now, None)
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
            target_thread_id: entry.registration.target_thread_id,
            child_depth: entry.registration.child_depth,
            prompt: entry.registration.prompt.clone(),
            config: entry.registration.config.clone(),
            interval: entry.interval,
            last_trigger: entry.last_trigger,
            active_helper_id: entry.active_helper_id,
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
    }

    async fn remove_if_generation(&self, target_thread_id: ThreadId, generation: i64) {
        let mut registrations = self.registrations.lock().await;
        let Some(entry) = registrations.get(&target_thread_id) else {
            return;
        };
        if entry.generation == generation {
            registrations.remove(&target_thread_id);
        }
    }

    async fn clear_active_helper_if_generation(&self, target_thread_id: ThreadId, generation: i64) {
        let mut registrations = self.registrations.lock().await;
        let Some(entry) = registrations.get_mut(&target_thread_id) else {
            return;
        };
        if entry.generation == generation {
            entry.active_helper_id = None;
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub(crate) async fn force_due_for_tests(&self, target_thread_id: ThreadId) {
        let mut registrations = self.registrations.lock().await;
        let Some(entry) = registrations.get_mut(&target_thread_id) else {
            return;
        };
        entry.last_trigger = Instant::now() - entry.interval;
    }
}

#[derive(Clone)]
struct WatchdogSnapshot {
    owner_thread_id: ThreadId,
    target_thread_id: ThreadId,
    child_depth: i32,
    prompt: String,
    config: Config,
    interval: Duration,
    last_trigger: Instant,
    active_helper_id: Option<ThreadId>,
}

async fn get_status(
    manager_state: &Arc<ThreadManagerState>,
    thread_id: ThreadId,
) -> codex_protocol::protocol::AgentStatus {
    let Ok(thread) = manager_state.get_thread(thread_id).await else {
        return codex_protocol::protocol::AgentStatus::NotFound;
    };
    thread.agent_status().await
}

fn interval_duration(interval_s: i64) -> CodexResult<Duration> {
    if interval_s <= 0 {
        return Err(CodexErr::UnsupportedOperation(
            "interval_s must be greater than zero".to_string(),
        ));
    }
    let seconds = u64::try_from(interval_s).map_err(|_| {
        CodexErr::UnsupportedOperation(format!("interval_s out of range: {interval_s}"))
    })?;
    Ok(Duration::from_secs(seconds))
}

fn tick_duration() -> Duration {
    let seconds = u64::try_from(WATCHDOG_TICK_SECONDS).unwrap_or(5);
    Duration::from_secs(seconds)
}

async fn watchdog_prompt(config: &Config, target_thread_id: ThreadId, prompt: &str) -> String {
    let watchdog_prompt = load_watchdog_prompt(&config.codex_home).await;
    format!(
        "{watchdog_prompt}\n\nTarget agent id: {target_thread_id}\n\nOriginal prompt:\n{prompt}"
    )
}
