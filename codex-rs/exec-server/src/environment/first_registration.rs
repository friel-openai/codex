use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;

type Slot<T> = OnceLock<Weak<T>>;
type Pending<T> = Mutex<HashMap<String, Vec<Weak<Slot<T>>>>>;

/// One capture of a current registration or the first later registration for an ID.
/// Removing or replacing that registration cannot transfer the capture.
pub struct FirstRegistration<T> {
    environment_id: String,
    slot: Arc<Slot<T>>,
    pending: Weak<Pending<T>>,
}

impl<T> FirstRegistration<T> {
    /// Returns the captured registration while it remains alive.
    pub fn get(&self) -> Option<Arc<T>> {
        self.slot.get()?.upgrade()
    }
}

impl<T> Drop for FirstRegistration<T> {
    fn drop(&mut self) {
        let Some(pending) = self.pending.upgrade() else {
            return;
        };
        let mut pending = pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entries) = pending.get_mut(&self.environment_id) {
            let slot = Arc::downgrade(&self.slot);
            entries.retain(|entry| entry.strong_count() != 0 && !entry.ptr_eq(&slot));
            if entries.is_empty() {
                pending.remove(&self.environment_id);
            }
        }
    }
}

/// Pending first-registration captures, synchronized with the caller's registry lock.
#[derive(Debug)]
pub(super) struct FirstRegistrationWatches<T> {
    pending: Arc<Pending<T>>,
}

impl<T> Default for FirstRegistrationWatches<T> {
    fn default() -> Self {
        Self {
            pending: Arc::default(),
        }
    }
}

impl<T> FirstRegistrationWatches<T> {
    /// The caller holds the registry read or write lock while obtaining `current` and calling this.
    pub(super) fn capture(
        &self,
        environment_id: &str,
        current: Option<&Arc<T>>,
    ) -> FirstRegistration<T> {
        let slot = Arc::new(OnceLock::new());
        if let Some(environment) = current {
            let _ = slot.set(Arc::downgrade(environment));
        } else {
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(environment_id.to_string())
                .or_default()
                .push(Arc::downgrade(&slot));
        }
        FirstRegistration {
            environment_id: environment_id.to_string(),
            slot,
            pending: Arc::downgrade(&self.pending),
        }
    }

    /// The caller holds the same registry write lock while installing the registration.
    pub(super) fn inserted(&self, environment_id: &str, environment: &Arc<T>) {
        let captures = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(environment_id);
        if let Some(captures) = captures {
            for capture in captures {
                if let Some(capture) = capture.upgrade() {
                    let _ = capture.set(Arc::downgrade(environment));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::RwLock;

    #[derive(Default)]
    struct Registry {
        current: RwLock<HashMap<String, Arc<u8>>>,
        watches: FirstRegistrationWatches<u8>,
    }

    impl Registry {
        fn capture(&self, id: &str) -> FirstRegistration<u8> {
            let current = self.current.read().expect("registry");
            self.watches.capture(id, current.get(id))
        }

        fn insert(&self, id: &str, value: Arc<u8>) {
            let mut current = self.current.write().expect("registry");
            current.insert(id.to_string(), Arc::clone(&value));
            self.watches.inserted(id, &value);
        }
    }

    #[test]
    fn first_registration_never_becomes_a_replacement() {
        let registry = Registry::default();
        let capture = registry.capture("worker");
        assert!(capture.get().is_none());
        let first = Arc::new(1);
        registry.insert("worker", Arc::clone(&first));
        let replacement = Arc::new(2);
        registry.insert("worker", Arc::clone(&replacement));
        assert!(Arc::ptr_eq(&capture.get().expect("first"), &first));
        drop(first);
        assert!(capture.get().is_none());
    }

    #[test]
    fn dropping_capture_removes_pending_watch() {
        let registry = Registry::default();
        let capture = registry.capture("worker");
        assert_eq!(
            registry.watches.pending.lock().expect("pending")["worker"].len(),
            1
        );
        drop(capture);
        assert!(registry.watches.pending.lock().expect("pending").is_empty());
        registry.insert("worker", Arc::new(1));
        assert!(registry.watches.pending.lock().expect("pending").is_empty());
    }

    #[test]
    fn cancellation_racing_insert_leaves_no_pending_watch() {
        for _ in 0..100 {
            let registry = Arc::new(Registry::default());
            let capture = registry.capture("worker");
            let barrier = Arc::new(Barrier::new(2));
            std::thread::scope(|scope| {
                let writer_registry = Arc::clone(&registry);
                let writer_barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    writer_barrier.wait();
                    writer_registry.insert("worker", Arc::new(1));
                });
                barrier.wait();
                drop(capture);
            });
            assert!(registry.watches.pending.lock().expect("pending").is_empty());
        }
    }

    #[test]
    fn capture_and_first_insert_cannot_miss_each_other() {
        for _ in 0..100 {
            let registry = Arc::new(Registry::default());
            let barrier = Arc::new(Barrier::new(2));
            let first = Arc::new(1);
            std::thread::scope(|scope| {
                let writer_registry = Arc::clone(&registry);
                let writer_barrier = Arc::clone(&barrier);
                let writer_first = Arc::clone(&first);
                scope.spawn(move || {
                    writer_barrier.wait();
                    writer_registry.insert("worker", writer_first);
                });
                barrier.wait();
                let capture = registry.capture("worker");
                scope.spawn(move || {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    while capture.get().is_none() && std::time::Instant::now() < deadline {
                        std::thread::yield_now();
                    }
                    assert!(Arc::ptr_eq(&capture.get().expect("first"), &first));
                });
            });
        }
    }
}
