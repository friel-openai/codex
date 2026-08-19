//! Coordinates maintenance jobs that replace local rollout files.
//!
//! Rollout compression and legacy rollout migration both publish by renaming a replacement over an
//! existing rollout path. They must not do that at the same time for one Codex home, so they share
//! this process-scoped, nonblocking file lock.
//!
//! This is separate from per-thread writer locks, which protect live rollout appenders. It is also
//! separate from compression's durable run marker, which throttles how often compression scans.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;

const ROLLOUT_MAINTENANCE_LOCK: &str = "rollout-maintenance.lock";

/// Holds exclusive ownership of operations that replace local rollout files.
pub struct RolloutMaintenanceGuard {
    _file: File,
}

/// Try to exclude rollout compression and migration for one Codex home.
pub fn try_acquire_rollout_maintenance_lock(
    codex_home: &Path,
) -> io::Result<Option<RolloutMaintenanceGuard>> {
    let directory = codex_home.join(".tmp");
    fs::create_dir_all(&directory)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join(ROLLOUT_MAINTENANCE_LOCK))?;

    match file.try_lock() {
        Ok(()) => Ok(Some(RolloutMaintenanceGuard { _file: file })),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Wait for exclusive ownership of operations that replace local rollout files.
///
/// The operating system releases the file lock if its owning process exits. Callers that require
/// maintenance to finish can cancel this future instead of translating ordinary contention into
/// an unrecoverable user-visible error.
pub async fn acquire_rollout_maintenance_lock(
    codex_home: &Path,
) -> io::Result<RolloutMaintenanceGuard> {
    let mut delay = std::time::Duration::from_millis(25);
    loop {
        if let Some(guard) = try_acquire_rollout_maintenance_lock(codex_home)? {
            return Ok(guard);
        }
        tokio::time::sleep(delay).await;
        delay = delay
            .saturating_mul(2)
            .min(std::time::Duration::from_millis(500));
    }
}
