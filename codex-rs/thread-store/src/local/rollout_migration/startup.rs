//! Coordinates automatic rollout migration with interactive thread loads.
//!
//! App-server startup inventories selected Legacy and `RolloutReference`-backed Paginated
//! rollouts, then processes them by modification time without delaying startup. A request that
//! loads one thread promotes that thread ahead of pending background work and waits for the same
//! migration attempt. List and search operations do not enter this coordinator.
//!
//! The older creation-ordered Legacy cursor remains below for compatibility with the explicit
//! startup migration entry point and its persisted skip fingerprints. Automatic native
//! `history_base` conversion does not trust that cursor because it may predate Paginated reference
//! conversion.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use chrono::NaiveDateTime;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::StateDbHandle;
use codex_state::RolloutMigrationCursor;
use codex_state::RolloutMigrationSkippedRollout;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tracing::warn;

use super::LocalThreadStore;
use super::RolloutMigrationMode;
use super::RolloutMigrationOptions;
use super::RolloutMigrationStatus;
use super::find_rollout_paths;
use super::lineage::contains_convertible_rollout_reference;
use super::lineage::has_leading_filtered_rollout_reference;
use super::migration_error;
use super::publish::pending_migration_thread_ids;
use super::telemetry::RolloutMigrationTrigger;
use crate::ThreadStoreResult;
use crate::local::live_writer;
use crate::local::thread_rollout_resolver;

const LEGACY_TO_PAGINATED_MIGRATION_ID: &str = "legacy_to_paginated_v1";
const NATIVE_HISTORY_BASE_MIGRATION_ID: &str = "native_history_base_v1";
const EMPTY_SKIP_REASON: &str = "empty";
const MALFORMED_SESSION_META_SKIP_REASON: &str = "malformed_session_meta";
const NATIVE_OR_COMPATIBLE_REASON: &str = "native_or_compatible";
const CURSOR_LOOKBACK_SECONDS: i64 = 48 * 60 * 60;

/// Serializes automatic migration and lets a thread-specific load move its thread ahead of
/// ordinary background work.
#[derive(Default)]
pub(crate) struct StartupMigrationCoordinator {
    enabled: AtomicBool,
    state: Mutex<CoordinatorState>,
    #[cfg(test)]
    processed: Mutex<Vec<ThreadId>>,
}

#[derive(Default)]
struct CoordinatorState {
    #[cfg(test)]
    discovery_complete: bool,
    worker_running: bool,
    priority: VecDeque<ThreadId>,
    entries: HashMap<ThreadId, MigrationEntry>,
}

struct MigrationEntry {
    path: PathBuf,
    modified_at: SystemTime,
    requested: bool,
    status: MigrationEntryStatus,
    completion: watch::Sender<MigrationCompletion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationEntryStatus {
    Pending,
    Running,
    Deferred,
    Complete,
}

#[derive(Clone, Debug)]
enum MigrationCompletion {
    Pending,
    Complete(Result<(), String>),
}

struct MigrationWork {
    thread_id: ThreadId,
    path: PathBuf,
    requested: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RolloutFingerprint {
    size_bytes: i64,
    modified_at_ns: i64,
}

enum StartupInspection {
    Paginated,
    Compatible,
    Legacy,
    ReferenceBacked,
    Skipped,
    Unresolved,
}

pub(super) fn start_automatic_rollout_migration(store: LocalThreadStore) {
    if store
        .rollout_migration_coordinator
        .enabled
        .swap(true, Ordering::AcqRel)
    {
        return;
    }
    tokio::spawn(async move {
        match discover_rollouts(&store).await {
            Ok(entries) => {
                let mut state = store.rollout_migration_coordinator.state.lock().await;
                for (thread_id, path, modified_at) in entries {
                    state.entries.entry(thread_id).or_insert_with(|| {
                        let (completion, _) = watch::channel(MigrationCompletion::Pending);
                        MigrationEntry {
                            path,
                            modified_at,
                            requested: false,
                            status: MigrationEntryStatus::Pending,
                            completion,
                        }
                    });
                }
            }
            Err(error) => {
                warn!("failed to discover rollouts for automatic migration: {error}");
            }
        }
        #[cfg(test)]
        {
            store
                .rollout_migration_coordinator
                .state
                .lock()
                .await
                .discovery_complete = true;
        }
        ensure_worker(&store).await;
    });
}

pub(super) async fn await_thread_migration(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    if live_writer::rollout_path(store, thread_id).await.is_ok() {
        return Ok(());
    }
    let enabled = store
        .rollout_migration_coordinator
        .enabled
        .load(Ordering::Acquire);
    if !enabled {
        return Ok(());
    }

    let existing_receiver = {
        let mut state = store.rollout_migration_coordinator.state.lock().await;
        subscribe_to_existing(&mut state, thread_id)
    };
    let mut receiver = if let Some(receiver) = existing_receiver {
        receiver
    } else {
        let Some(resolved) =
            thread_rollout_resolver::resolve_current_including_archived(store, thread_id).await?
        else {
            return Ok(());
        };
        let modified_at = rollout_modified_at(resolved.path.as_path()).await?;
        let mut state = store.rollout_migration_coordinator.state.lock().await;
        if let Some(receiver) = subscribe_to_existing(&mut state, thread_id) {
            receiver
        } else {
            let (completion, receiver) = watch::channel(MigrationCompletion::Pending);
            state.entries.insert(
                thread_id,
                MigrationEntry {
                    path: resolved.path,
                    modified_at,
                    requested: true,
                    status: MigrationEntryStatus::Pending,
                    completion,
                },
            );
            state.priority.push_back(thread_id);
            receiver
        }
    };
    ensure_worker(store).await;

    loop {
        let completion = receiver.borrow().clone();
        match completion {
            MigrationCompletion::Pending => {
                receiver.changed().await.map_err(|_| {
                    migration_error(format!(
                        "automatic rollout migration stopped before thread {thread_id} completed"
                    ))
                })?;
            }
            MigrationCompletion::Complete(Ok(())) => return Ok(()),
            MigrationCompletion::Complete(Err(message)) => {
                return Err(migration_error(message));
            }
        }
    }
}

fn subscribe_to_existing(
    state: &mut CoordinatorState,
    thread_id: ThreadId,
) -> Option<watch::Receiver<MigrationCompletion>> {
    let entry = state.entries.get_mut(&thread_id)?;
    entry.requested = true;
    if entry.status == MigrationEntryStatus::Deferred {
        entry.status = MigrationEntryStatus::Pending;
    }
    let pending = entry.status == MigrationEntryStatus::Pending;
    let receiver = entry.completion.subscribe();
    if pending && !state.priority.contains(&thread_id) {
        state.priority.push_back(thread_id);
    }
    Some(receiver)
}

async fn discover_rollouts(
    store: &LocalThreadStore,
) -> ThreadStoreResult<Vec<(ThreadId, PathBuf, SystemTime)>> {
    let mut entries = Vec::new();
    let pending = pending_migration_thread_ids(&store.config.codex_home).await?;
    let terminal = startup_state_db(store)?
        .list_rollout_migration_skipped_rollouts(NATIVE_HISTORY_BASE_MIGRATION_ID)
        .await
        .map_err(migration_error)?
        .into_iter()
        .map(|entry| (entry.rollout_path.clone(), entry))
        .collect::<HashMap<_, _>>();
    for path in find_all_rollout_paths(store).await? {
        let Ok(metadata) = codex_rollout::read_session_meta_line(path.as_path()).await else {
            continue;
        };
        if let Some(state_db) = &store.state_db
            && let Some(selected) = state_db
                .get_thread(metadata.meta.id)
                .await
                .map_err(migration_error)?
            && codex_rollout::plain_rollout_path(selected.rollout_path.as_path())
                != codex_rollout::plain_rollout_path(path.as_path())
        {
            continue;
        }
        if !pending.contains(&metadata.meta.id) {
            let relative_path = relative_rollout_path(store, path.as_path());
            let fingerprint = rollout_fingerprint(path.as_path()).await?;
            if terminal
                .get(relative_path.as_str())
                .is_some_and(|entry| fingerprint_matches(entry, fingerprint))
            {
                continue;
            }
            match inspect_rollout_path(store, path.as_path()).await? {
                StartupInspection::Legacy | StartupInspection::ReferenceBacked => {}
                StartupInspection::Paginated | StartupInspection::Compatible => {
                    record_terminal_inspection(store, path.as_path(), fingerprint).await?;
                    continue;
                }
                StartupInspection::Skipped | StartupInspection::Unresolved => continue,
            }
        }
        entries.push((
            metadata.meta.id,
            path.clone(),
            rollout_modified_at(path.as_path()).await?,
        ));
    }
    entries.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.1.cmp(&right.1)));
    Ok(entries)
}

async fn rollout_modified_at(path: &Path) -> ThreadStoreResult<SystemTime> {
    tokio::fs::metadata(path)
        .await
        .and_then(|metadata| metadata.modified())
        .map_err(migration_error)
}

async fn ensure_worker(store: &LocalThreadStore) {
    let should_spawn = {
        let mut state = store.rollout_migration_coordinator.state.lock().await;
        if state.worker_running {
            false
        } else {
            state.worker_running = true;
            true
        }
    };
    if should_spawn {
        let store = store.clone();
        tokio::spawn(async move { run_worker(store).await });
    }
}

async fn run_worker(store: LocalThreadStore) {
    loop {
        let Some(work) = next_work(&store).await else {
            return;
        };
        #[cfg(test)]
        store
            .rollout_migration_coordinator
            .processed
            .lock()
            .await
            .push(work.thread_id);
        let result = store
            .migrate_rollout_path_on_startup(work.path.clone(), (!work.requested).then_some(32))
            .await;
        if let Ok(Some(outcome)) = &result
            && matches!(
                outcome.status,
                RolloutMigrationStatus::Migrated | RolloutMigrationStatus::AlreadyPaginated
            )
            && let Ok(fingerprint) = rollout_fingerprint(outcome.rollout_path.as_path()).await
            && let Err(error) =
                record_terminal_inspection(&store, outcome.rollout_path.as_path(), fingerprint)
                    .await
        {
            warn!(
                thread_id = %work.thread_id,
                path = %outcome.rollout_path.display(),
                "failed to record automatic rollout migration fingerprint: {error}"
            );
        }
        let retry_delay = {
            let mut state = store.rollout_migration_coordinator.state.lock().await;
            let Some(entry) = state.entries.get_mut(&work.thread_id) else {
                continue;
            };
            match result {
                Ok(Some(outcome)) if outcome.status == RolloutMigrationStatus::SkippedBusy => {
                    if entry.requested {
                        entry.status = MigrationEntryStatus::Pending;
                        state.priority.push_back(work.thread_id);
                        Some(std::time::Duration::from_millis(50))
                    } else {
                        entry.status = MigrationEntryStatus::Deferred;
                        None
                    }
                }
                Ok(Some(outcome)) => {
                    if outcome.status == RolloutMigrationStatus::Failed {
                        let message = outcome.message.unwrap_or_else(|| {
                            format!(
                                "automatic rollout migration failed for {}",
                                work.path.display()
                            )
                        });
                        warn!(
                            thread_id = %work.thread_id,
                            path = %work.path.display(),
                            message = %message,
                            "automatic rollout migration left the compatible source unchanged"
                        );
                        finish_entry(entry, Err(message));
                    } else {
                        finish_entry(entry, Ok(()));
                    }
                    None
                }
                Ok(None) => {
                    finish_entry(entry, Ok(()));
                    None
                }
                Err(crate::ThreadStoreError::Conflict { .. }) => {
                    if entry.requested {
                        entry.status = MigrationEntryStatus::Pending;
                        state.priority.push_back(work.thread_id);
                        Some(std::time::Duration::from_millis(250))
                    } else {
                        entry.status = MigrationEntryStatus::Deferred;
                        None
                    }
                }
                Err(error) => {
                    finish_entry(entry, Err(error.to_string()));
                    None
                }
            }
        };
        if let Some(delay) = retry_delay {
            tokio::time::sleep(delay).await;
        }
    }
}

async fn next_work(store: &LocalThreadStore) -> Option<MigrationWork> {
    let mut state = store.rollout_migration_coordinator.state.lock().await;
    let mut priority_thread = None;
    while let Some(thread_id) = state.priority.pop_front() {
        if state
            .entries
            .get(&thread_id)
            .is_some_and(|entry| entry.status == MigrationEntryStatus::Pending)
        {
            priority_thread = Some(thread_id);
            break;
        }
    }
    let thread_id = priority_thread.or_else(|| {
        state
            .entries
            .iter()
            .filter(|(_, entry)| entry.status == MigrationEntryStatus::Pending)
            .max_by(|left, right| {
                left.1
                    .modified_at
                    .cmp(&right.1.modified_at)
                    .then_with(|| right.1.path.cmp(&left.1.path))
            })
            .map(|(thread_id, _)| *thread_id)
    });
    let Some(thread_id) = thread_id else {
        state.worker_running = false;
        return None;
    };
    let entry = state.entries.get_mut(&thread_id)?;
    entry.status = MigrationEntryStatus::Running;
    Some(MigrationWork {
        thread_id,
        path: entry.path.clone(),
        requested: entry.requested,
    })
}

fn finish_entry(entry: &mut MigrationEntry, result: Result<(), String>) {
    entry.status = MigrationEntryStatus::Complete;
    entry
        .completion
        .send_replace(MigrationCompletion::Complete(result));
}

fn fingerprint_matches(
    entry: &RolloutMigrationSkippedRollout,
    fingerprint: RolloutFingerprint,
) -> bool {
    entry.rollout_size_bytes == fingerprint.size_bytes
        && entry.rollout_modified_at_ns == fingerprint.modified_at_ns
}

async fn record_terminal_inspection(
    store: &LocalThreadStore,
    path: &Path,
    fingerprint: RolloutFingerprint,
) -> ThreadStoreResult<()> {
    startup_state_db(store)?
        .record_rollout_migration_skip(
            NATIVE_HISTORY_BASE_MIGRATION_ID,
            &RolloutMigrationSkippedRollout {
                rollout_path: relative_rollout_path(store, path),
                rollout_size_bytes: fingerprint.size_bytes,
                rollout_modified_at_ns: fingerprint.modified_at_ns,
                skip_reason: NATIVE_OR_COMPATIBLE_REASON.to_string(),
            },
        )
        .await
        .map_err(migration_error)
}

#[cfg(test)]
pub(super) async fn processed_thread_ids(store: &LocalThreadStore) -> Vec<ThreadId> {
    store
        .rollout_migration_coordinator
        .processed
        .lock()
        .await
        .clone()
}

#[cfg(test)]
pub(super) async fn automatic_migration_idle(store: &LocalThreadStore) -> bool {
    let state = store.rollout_migration_coordinator.state.lock().await;
    state.discovery_complete
        && !state.worker_running
        && state.entries.values().all(|entry| {
            !matches!(
                entry.status,
                MigrationEntryStatus::Pending | MigrationEntryStatus::Running
            )
        })
}

pub(super) async fn migrate_rollouts_on_startup(store: &LocalThreadStore) -> ThreadStoreResult<()> {
    let Some(state_db) = store.state_db.as_ref() else {
        return Ok(());
    };
    let paths = find_all_rollout_paths(store).await?;
    let skipped_rollouts = state_db
        .list_rollout_migration_skipped_rollouts(LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .map_err(migration_error)?;
    if !pending_migration_thread_ids(&store.config.codex_home)
        .await?
        .is_empty()
    {
        return migrate_all_rollouts(store, paths, skipped_rollouts.as_slice()).await;
    }
    let (unchanged_skips, invalidated_skip) =
        revalidate_skipped_rollouts(store, skipped_rollouts.as_slice()).await?;
    let state = state_db
        .get_rollout_migration_state(LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .map_err(migration_error)?;

    if state.is_none() || invalidated_skip {
        return migrate_all_rollouts(store, paths, skipped_rollouts.as_slice()).await;
    }

    let last_checked_thread = state.and_then(|state| state.last_checked_thread);
    let lookback_created_at = last_checked_thread.as_ref().map(|cursor| {
        cursor
            .thread_created_at
            .saturating_sub(CURSOR_LOOKBACK_SECONDS)
    });
    let candidates = paths
        .iter()
        .filter(|path| {
            let relative_path = relative_rollout_path(store, path);
            !unchanged_skips.contains(relative_path.as_str())
                && thread_creation_cursor(path).is_none_or(|cursor| {
                    lookback_created_at.is_none_or(|lookback_created_at| {
                        cursor.thread_created_at >= lookback_created_at
                    })
                })
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(());
    }

    let mut unresolved = false;
    for path in candidates {
        match inspect_rollout_path(store, path).await? {
            StartupInspection::Paginated
            | StartupInspection::Compatible
            | StartupInspection::Skipped => {}
            StartupInspection::Legacy | StartupInspection::ReferenceBacked => {
                return migrate_all_rollouts(store, paths, skipped_rollouts.as_slice()).await;
            }
            StartupInspection::Unresolved => unresolved = true,
        }
    }
    if unresolved {
        return Ok(());
    }

    advance_last_checked_thread(store, paths.as_slice()).await
}

async fn migrate_all_rollouts(
    store: &LocalThreadStore,
    paths_before_migration: Vec<PathBuf>,
    existing_skips: &[RolloutMigrationSkippedRollout],
) -> ThreadStoreResult<()> {
    let report = store
        .migrate_rollouts_with_progress_for_trigger(
            RolloutMigrationOptions {
                mode: RolloutMigrationMode::Apply,
                thread_ids: Vec::new(),
                max_mib_per_second: None,
            },
            |_| {},
            RolloutMigrationTrigger::Startup,
        )
        .await?;
    let existing_skip_paths = existing_skips
        .iter()
        .map(|skipped_rollout| skipped_rollout.rollout_path.as_str())
        .collect::<HashSet<_>>();
    let mut terminal = true;
    let mut reported_paths = HashSet::new();
    for outcome in &report.outcomes {
        let relative_path = relative_rollout_path(store, &outcome.rollout_path);
        reported_paths.insert(relative_path.clone());
        match outcome.status {
            RolloutMigrationStatus::Migrated | RolloutMigrationStatus::AlreadyPaginated => {
                if existing_skip_paths.contains(relative_path.as_str()) {
                    remove_skip(store, relative_path.as_str()).await?;
                }
            }
            RolloutMigrationStatus::SkippedEmpty | RolloutMigrationStatus::Failed => {
                if !matches!(
                    inspect_rollout_path(store, &outcome.rollout_path).await?,
                    StartupInspection::Skipped
                ) {
                    terminal = false;
                }
            }
            RolloutMigrationStatus::Eligible | RolloutMigrationStatus::SkippedBusy => {
                terminal = false
            }
        }
    }
    if !terminal {
        return Ok(());
    }
    for skipped_rollout in existing_skips {
        if !reported_paths.contains(skipped_rollout.rollout_path.as_str()) {
            remove_skip(store, skipped_rollout.rollout_path.as_str()).await?;
        }
    }
    // Only mark the pre-migration snapshot; newer rollouts wait for the next startup check.
    advance_last_checked_thread(store, paths_before_migration.as_slice()).await
}

async fn inspect_rollout_path(
    store: &LocalThreadStore,
    path: &Path,
) -> ThreadStoreResult<StartupInspection> {
    let before = rollout_fingerprint(path).await?;
    match codex_rollout::read_session_meta_line(path).await {
        Ok(metadata) if metadata.meta.history_mode == ThreadHistoryMode::Legacy => {
            Ok(StartupInspection::Legacy)
        }
        Ok(_) => {
            if contains_convertible_rollout_reference(store.config.codex_home.as_path(), path)
                .await?
            {
                Ok(StartupInspection::ReferenceBacked)
            } else if has_leading_filtered_rollout_reference(path).await? {
                Ok(StartupInspection::Compatible)
            } else {
                Ok(StartupInspection::Paginated)
            }
        }
        Err(error) => {
            let after = rollout_fingerprint(path).await?;
            if before != after || !matches!(error.kind(), ErrorKind::Other | ErrorKind::InvalidData)
            {
                return Ok(StartupInspection::Unresolved);
            }
            record_skip(store, path, before).await?;
            Ok(StartupInspection::Skipped)
        }
    }
}

async fn record_skip(
    store: &LocalThreadStore,
    path: &Path,
    fingerprint: RolloutFingerprint,
) -> ThreadStoreResult<()> {
    let state_db = startup_state_db(store)?;
    let skipped_rollout = RolloutMigrationSkippedRollout {
        rollout_path: relative_rollout_path(store, path),
        rollout_size_bytes: fingerprint.size_bytes,
        rollout_modified_at_ns: fingerprint.modified_at_ns,
        skip_reason: if fingerprint.size_bytes == 0 {
            EMPTY_SKIP_REASON.to_string()
        } else {
            MALFORMED_SESSION_META_SKIP_REASON.to_string()
        },
    };
    state_db
        .record_rollout_migration_skip(LEGACY_TO_PAGINATED_MIGRATION_ID, &skipped_rollout)
        .await
        .map_err(migration_error)
}

async fn remove_skip(store: &LocalThreadStore, rollout_path: &str) -> ThreadStoreResult<()> {
    startup_state_db(store)?
        .remove_rollout_migration_skip(LEGACY_TO_PAGINATED_MIGRATION_ID, rollout_path)
        .await
        .map_err(migration_error)
}

async fn revalidate_skipped_rollouts(
    store: &LocalThreadStore,
    skipped_rollouts: &[RolloutMigrationSkippedRollout],
) -> ThreadStoreResult<(HashSet<String>, bool)> {
    let mut unchanged_skips = HashSet::new();
    let mut invalidated_skip = false;
    for skipped_rollout in skipped_rollouts {
        let path = store.config.codex_home.join(&skipped_rollout.rollout_path);
        let fingerprint = match rollout_fingerprint(&path).await {
            Ok(fingerprint) => fingerprint,
            Err(_) => {
                invalidated_skip = true;
                continue;
            }
        };
        if fingerprint.size_bytes == skipped_rollout.rollout_size_bytes
            && fingerprint.modified_at_ns == skipped_rollout.rollout_modified_at_ns
        {
            unchanged_skips.insert(skipped_rollout.rollout_path.clone());
        } else {
            invalidated_skip = true;
        }
    }
    Ok((unchanged_skips, invalidated_skip))
}

async fn advance_last_checked_thread(
    store: &LocalThreadStore,
    paths: &[PathBuf],
) -> ThreadStoreResult<()> {
    let last_checked_thread = paths
        .iter()
        .filter_map(|path| thread_creation_cursor(path))
        .max();
    startup_state_db(store)?
        .advance_rollout_migration_state(
            LEGACY_TO_PAGINATED_MIGRATION_ID,
            last_checked_thread.as_ref(),
        )
        .await
        .map_err(migration_error)
}

fn startup_state_db(store: &LocalThreadStore) -> ThreadStoreResult<&StateDbHandle> {
    store
        .state_db
        .as_ref()
        .ok_or_else(|| migration_error("startup migration requires state db"))
}

async fn find_all_rollout_paths(store: &LocalThreadStore) -> ThreadStoreResult<Vec<PathBuf>> {
    let mut paths =
        find_rollout_paths(&store.config.codex_home.join(codex_rollout::SESSIONS_SUBDIR)).await?;
    paths.extend(
        find_rollout_paths(
            &store
                .config
                .codex_home
                .join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR),
        )
        .await?,
    );
    Ok(paths)
}

fn thread_creation_cursor(path: &Path) -> Option<RolloutMigrationCursor> {
    let name = path.file_name()?.to_str()?;
    let stem = name
        .strip_suffix(".jsonl.zst")
        .or_else(|| name.strip_suffix(".jsonl"))?
        .strip_prefix("rollout-")?;
    let separator = stem.len().checked_sub(37)?;
    let thread_id = stem.get(separator + 1..)?;
    ThreadId::from_string(thread_id).ok()?;
    let timestamp = NaiveDateTime::parse_from_str(stem.get(..separator)?, "%Y-%m-%dT%H-%M-%S")
        .ok()?
        .and_utc()
        .timestamp();
    Some(RolloutMigrationCursor {
        thread_created_at: timestamp,
        thread_id: thread_id.to_string(),
    })
}

fn relative_rollout_path(store: &LocalThreadStore, path: &Path) -> String {
    path.strip_prefix(&store.config.codex_home)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

async fn rollout_fingerprint(path: &Path) -> ThreadStoreResult<RolloutFingerprint> {
    let metadata = tokio::fs::metadata(path).await.map_err(migration_error)?;
    let size_bytes = i64::try_from(metadata.len()).map_err(migration_error)?;
    let modified_at_ns = metadata
        .modified()
        .map_err(migration_error)?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(migration_error)?
        .as_nanos();
    let modified_at_ns = i64::try_from(modified_at_ns).map_err(migration_error)?;
    Ok(RolloutFingerprint {
        size_bytes,
        modified_at_ns,
    })
}

#[cfg(test)]
#[path = "startup_tests.rs"]
mod tests;
