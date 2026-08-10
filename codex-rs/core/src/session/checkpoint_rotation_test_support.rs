use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use codex_protocol::ThreadId;
use tokio::sync::Notify;

/// Coordinates deterministic checkpoint-capture races without changing production storage APIs.
struct CheckpointCapturePauseState {
    reached: Notify,
    release: Notify,
}

static CHECKPOINT_CAPTURE_PAUSES: LazyLock<
    Mutex<HashMap<ThreadId, Arc<CheckpointCapturePauseState>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

static CHECKPOINT_PERSISTENCE_FAILURES: LazyLock<Mutex<HashSet<ThreadId>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static CHECKPOINT_PERSISTENCE_INDETERMINATE: LazyLock<Mutex<HashSet<ThreadId>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static CHECKPOINT_OWNER_PANICS: LazyLock<Mutex<HashSet<ThreadId>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Test handle that pauses one thread after checkpoint capture and before segment rotation.
pub(crate) struct CheckpointCapturePause {
    thread_id: ThreadId,
    state: Arc<CheckpointCapturePauseState>,
}

impl CheckpointCapturePause {
    pub(crate) fn install(thread_id: ThreadId) -> Self {
        let state = Arc::new(CheckpointCapturePauseState {
            reached: Notify::new(),
            release: Notify::new(),
        });
        let previous = CHECKPOINT_CAPTURE_PAUSES
            .lock()
            .expect("checkpoint capture pause mutex")
            .insert(thread_id, Arc::clone(&state));
        assert!(
            previous.is_none(),
            "checkpoint capture pause already installed"
        );
        Self { thread_id, state }
    }

    pub(crate) async fn wait_until_reached(&self) {
        self.state.reached.notified().await;
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }
}

impl Drop for CheckpointCapturePause {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut pauses = CHECKPOINT_CAPTURE_PAUSES
            .lock()
            .expect("checkpoint capture pause mutex");
        if pauses
            .get(&self.thread_id)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            pauses.remove(&self.thread_id);
        }
    }
}

pub(crate) async fn pause_after_checkpoint_capture(thread_id: ThreadId) {
    let state = CHECKPOINT_CAPTURE_PAUSES
        .lock()
        .expect("checkpoint capture pause mutex")
        .get(&thread_id)
        .cloned();
    let Some(state) = state else {
        return;
    };
    state.reached.notify_one();
    state.release.notified().await;
    let mut pauses = CHECKPOINT_CAPTURE_PAUSES
        .lock()
        .expect("checkpoint capture pause mutex");
    if pauses
        .get(&thread_id)
        .is_some_and(|registered| Arc::ptr_eq(registered, &state))
    {
        pauses.remove(&thread_id);
    }
}

/// Test handle that makes one thread's checkpoint rotation and fallback append fail together.
pub(crate) struct CheckpointPersistenceFailure {
    thread_id: ThreadId,
}

impl CheckpointPersistenceFailure {
    pub(crate) fn install(thread_id: ThreadId) -> Self {
        let inserted = CHECKPOINT_PERSISTENCE_FAILURES
            .lock()
            .expect("checkpoint persistence failure mutex")
            .insert(thread_id);
        assert!(inserted, "checkpoint persistence failure already installed");
        Self { thread_id }
    }
}

impl Drop for CheckpointPersistenceFailure {
    fn drop(&mut self) {
        CHECKPOINT_PERSISTENCE_FAILURES
            .lock()
            .expect("checkpoint persistence failure mutex")
            .remove(&self.thread_id);
    }
}

pub(crate) fn checkpoint_persistence_should_fail(thread_id: ThreadId) -> bool {
    CHECKPOINT_PERSISTENCE_FAILURES
        .lock()
        .expect("checkpoint persistence failure mutex")
        .contains(&thread_id)
}

/// Test handle that makes one thread's checkpoint result indeterminate.
pub(crate) struct CheckpointPersistenceIndeterminate {
    thread_id: ThreadId,
}

impl CheckpointPersistenceIndeterminate {
    pub(crate) fn install(thread_id: ThreadId) -> Self {
        let inserted = CHECKPOINT_PERSISTENCE_INDETERMINATE
            .lock()
            .expect("indeterminate checkpoint persistence mutex")
            .insert(thread_id);
        assert!(
            inserted,
            "indeterminate checkpoint persistence already installed"
        );
        Self { thread_id }
    }
}

impl Drop for CheckpointPersistenceIndeterminate {
    fn drop(&mut self) {
        CHECKPOINT_PERSISTENCE_INDETERMINATE
            .lock()
            .expect("indeterminate checkpoint persistence mutex")
            .remove(&self.thread_id);
    }
}

pub(crate) fn checkpoint_persistence_should_be_indeterminate(thread_id: ThreadId) -> bool {
    CHECKPOINT_PERSISTENCE_INDETERMINATE
        .lock()
        .expect("indeterminate checkpoint persistence mutex")
        .contains(&thread_id)
}

/// Test handle that panics a checkpoint owner while it owns the session mutation guard.
pub(crate) struct CheckpointOwnerPanic {
    thread_id: ThreadId,
}

impl CheckpointOwnerPanic {
    pub(crate) fn install(thread_id: ThreadId) -> Self {
        let inserted = CHECKPOINT_OWNER_PANICS
            .lock()
            .expect("checkpoint owner panic mutex")
            .insert(thread_id);
        assert!(inserted, "checkpoint owner panic already installed");
        Self { thread_id }
    }
}

impl Drop for CheckpointOwnerPanic {
    fn drop(&mut self) {
        CHECKPOINT_OWNER_PANICS
            .lock()
            .expect("checkpoint owner panic mutex")
            .remove(&self.thread_id);
    }
}

pub(crate) fn checkpoint_owner_should_panic(thread_id: ThreadId) -> bool {
    CHECKPOINT_OWNER_PANICS
        .lock()
        .expect("checkpoint owner panic mutex")
        .contains(&thread_id)
}
