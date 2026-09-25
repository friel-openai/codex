//! Cross-process ownership of mutable rollout files.
//!
//! Compression and thread-store use the same coordination lock when opening or removing a
//! per-thread lock file. Without that coordination, unlinking a released file could let two
//! processes lock different inodes for the same thread.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_protocol::ThreadId;
use tracing::warn;

const WRITER_LOCK_DIR: &str = "thread-writer-locks";
const COORDINATION_LOCK_FILE: &str = ".coordination.lock";

/// Coordinates writer ownership within one Codex home.
#[derive(Debug)]
pub struct WriterLockCoordinator {
    directory: PathBuf,
    cleanup_attempted: AtomicBool,
}

/// Keeps writer or immutable fork-reader ownership until all shared guard users finish.
#[derive(Debug)]
pub struct WriterLockGuard {
    coordinator: Arc<WriterLockCoordinator>,
    path: PathBuf,
    // Recorder tasks share this guard, so conversions must serialize without unique Arc access.
    state: Mutex<WriterLockState>,
}

/// A shared lock excludes new writers while the existing owner admits immutable fork readers.
#[derive(Debug)]
enum WriterLockState {
    Exclusive(File),
    Shared(File),
    /// A failed lock conversion must not leave the recorder authorized to write.
    Unreserved,
}

impl WriterLockCoordinator {
    /// Uses the same lock namespace as existing local thread-store writers.
    pub fn new(codex_home: &Path) -> Self {
        Self {
            directory: codex_home.join(WRITER_LOCK_DIR),
            cleanup_attempted: AtomicBool::new(false),
        }
    }

    /// Acquires exclusive writer ownership, returning `WouldBlock` for an active writer.
    pub fn acquire(self: &Arc<Self>, thread_id: ThreadId) -> io::Result<WriterLockGuard> {
        let _coordination_lock = self.lock_coordination()?;
        if !self.cleanup_attempted.swap(true, Ordering::Relaxed)
            && let Err(err) = self.remove_stale_thread_locks()
        {
            warn!("failed to clean up stale thread writer locks: {err}");
        }

        let path = self.directory.join(format!("{thread_id}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "failed to open thread writer lock {}: {err}",
                        path.display()
                    ),
                )
            })?;

        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("thread {thread_id} already has an active writer"),
                ));
            }
            Err(std::fs::TryLockError::Error(err)) => {
                return Err(io::Error::new(
                    err.kind(),
                    format!(
                        "failed to acquire thread writer lock {}: {err}",
                        path.display()
                    ),
                ));
            }
        }

        Ok(WriterLockGuard {
            coordinator: Arc::clone(self),
            path,
            state: Mutex::new(WriterLockState::Exclusive(file)),
        })
    }

    /// Holds coordination through publication after probing that the thread is idle.
    /// Every writer takes coordination before opening its thread lock, so the probe
    /// itself can be released. Encoding and verification must finish before this call.
    pub(crate) fn try_acquire_for_publication(
        &self,
        thread_id: ThreadId,
    ) -> io::Result<Option<File>> {
        let coordination_lock = self.lock_coordination()?;
        let path = self.directory.join(format!("{thread_id}.lock"));
        match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => match file.try_lock() {
                Ok(()) => Ok(Some(coordination_lock)),
                Err(std::fs::TryLockError::WouldBlock) => Ok(None),
                Err(std::fs::TryLockError::Error(err)) => Err(err),
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Some(coordination_lock)),
            Err(err) => Err(err),
        }
    }

    /// Reserves immutable fork history without receiving writer authority. Sharing the existing
    /// lock inode also excludes compression and older binaries' deletion operations.
    pub fn acquire_fork_reader(
        self: &Arc<Self>,
        thread_id: ThreadId,
    ) -> io::Result<WriterLockGuard> {
        let _coordination = self.lock_coordination()?;
        let path = self.directory.join(format!("{thread_id}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock_shared() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("thread {thread_id} has not exported a fork snapshot"),
                ));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
        Ok(WriterLockGuard {
            coordinator: Arc::clone(self),
            path,
            state: Mutex::new(WriterLockState::Shared(file)),
        })
    }

    fn lock_coordination(&self) -> io::Result<File> {
        fs::create_dir_all(&self.directory).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to create thread writer lock directory {}: {err}",
                    self.directory.display()
                ),
            )
        })?;
        let path = self.directory.join(COORDINATION_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "failed to open thread writer coordination lock {}: {err}",
                        path.display()
                    ),
                )
            })?;
        file.lock().map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to acquire thread writer coordination lock {}: {err}",
                    path.display()
                ),
            )
        })?;
        Ok(file)
    }

    fn remove_stale_thread_locks(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(thread_id) = file_name.strip_suffix(".lock") else {
                continue;
            };
            if ThreadId::from_string(thread_id).is_err() {
                continue;
            }

            let path = entry.path();
            let file = match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => {
                    warn!(
                        "failed to inspect thread writer lock {}: {err}",
                        path.display()
                    );
                    continue;
                }
            };
            match file.try_lock() {
                Ok(()) => {
                    drop(file);
                    if let Err(err) = fs::remove_file(&path)
                        && err.kind() != io::ErrorKind::NotFound
                    {
                        warn!(
                            "failed to remove stale thread writer lock {}: {err}",
                            path.display()
                        );
                    }
                }
                Err(std::fs::TryLockError::WouldBlock) => {}
                Err(std::fs::TryLockError::Error(err)) => {
                    warn!(
                        "failed to inspect thread writer lock {}: {err}",
                        path.display()
                    );
                }
            }
        }
        Ok(())
    }
}

impl WriterLockGuard {
    /// Retains the sole logical writer, but admits read-only fork lifetime reservations.
    /// The coordination lock excludes acquisition and cleanup across the explicit unlock/relock;
    /// converting an already-locked handle is not portable.
    pub fn share_for_fork(&self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("writer lock state is poisoned"))?;
        let _coordination = self.coordinator.lock_coordination()?;
        match std::mem::replace(&mut *state, WriterLockState::Unreserved) {
            WriterLockState::Exclusive(file) => {
                file.unlock()?;
                file.try_lock_shared().map_err(io::Error::from)?;
                *state = WriterLockState::Shared(file);
                Ok(())
            }
            WriterLockState::Shared(file) => {
                *state = WriterLockState::Shared(file);
                Ok(())
            }
            WriterLockState::Unreserved => Err(io::Error::other("writer lock is closed")),
        }
    }

    /// Destruction requires exclusive ownership, unlike appending to the mutable source tail.
    /// Returns WouldBlock if an initializing fork still holds a reader reservation.
    pub fn require_exclusive(&self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("writer lock state is poisoned"))?;
        let _coordination = self.coordinator.lock_coordination()?;
        match std::mem::replace(&mut *state, WriterLockState::Unreserved) {
            WriterLockState::Exclusive(file) => {
                *state = WriterLockState::Exclusive(file);
                Ok(())
            }
            WriterLockState::Shared(file) => {
                file.unlock()?;
                match file.try_lock() {
                    Ok(()) => {
                        *state = WriterLockState::Exclusive(file);
                        Ok(())
                    }
                    Err(error) => {
                        // Restore exclusion before returning. Failed restoration leaves an
                        // unreserved guard so the caller can fence its drained recorder.
                        file.try_lock_shared().map_err(io::Error::from)?;
                        *state = WriterLockState::Shared(file);
                        Err(error.into())
                    }
                }
            }
            WriterLockState::Unreserved => Err(io::Error::other("writer lock is closed")),
        }
    }

    /// Callers must stop using their recorder when a failed conversion loses its reservation.
    pub fn is_reserved(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| !matches!(*state, WriterLockState::Unreserved))
    }
}

impl Drop for WriterLockGuard {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        let file = match std::mem::replace(state, WriterLockState::Unreserved) {
            WriterLockState::Exclusive(file) | WriterLockState::Shared(file) => Some(file),
            WriterLockState::Unreserved => None,
        };
        let coordination = self.coordinator.lock_coordination();
        // Closing only this descriptor does not release locks inherited by a forked process.
        // Unlock explicitly, then close before deletion so cleanup also works on Windows.
        if let Some(file) = file {
            if let Err(error) = file.unlock() {
                warn!("failed to unlock thread writer reservation: {error}");
            }
            drop(file);
        }
        let coordination_lock = match coordination {
            Ok(lock) => lock,
            Err(err) => {
                warn!("failed to coordinate thread writer lock cleanup: {err}");
                return;
            }
        };

        // An imported fork may outlive the source process. Keep its inode so compression and
        // subsequent writers cannot acquire a different lock file for the same thread.
        match OpenOptions::new().read(true).write(true).open(&self.path) {
            Ok(file) => match file.try_lock() {
                Ok(()) => drop(file),
                Err(std::fs::TryLockError::WouldBlock) => return,
                Err(std::fs::TryLockError::Error(error)) => {
                    warn!("failed to inspect writer lock during cleanup: {error}");
                    return;
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => return,
            Err(error) => {
                warn!("failed to inspect writer lock during cleanup: {error}");
                return;
            }
        }
        if let Err(err) = fs::remove_file(&self.path)
            && err.kind() != io::ErrorKind::NotFound
        {
            warn!(
                "failed to remove thread writer lock {}: {err}",
                self.path.display()
            );
        }
        drop(coordination_lock);
    }
}

#[cfg(test)]
#[path = "writer_lock_tests.rs"]
mod tests;
