//! Serializes archive moves with publication of new durable migration dependencies.

use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;

use super::ClassifiedMigrationResult;
use super::LocalThreadStore;
use super::MigrationLocks;
use super::RolloutMigrationFailure;
use super::RolloutMigrationFailureReason;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::parse_lineage_migration_journal;
use super::migration_error;
use super::publish::migration_journal_path;
use super::publish::pending_migration_thread_ids;
use super::publish::write_migration_journal_guarded;
use super::thread_history;
use super::with_failure_reason;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(in crate::local) struct ArchivePublicationGuard(File);

impl Drop for ArchivePublicationGuard {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to release archive publication lock");
        }
    }
}

pub(in crate::local) fn try_acquire(home: &Path) -> ThreadStoreResult<ArchivePublicationGuard> {
    let directory = home.join(".tmp");
    std::fs::create_dir_all(&directory).map_err(migration_error)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("rollout-archive-publication.lock"))
        .map_err(migration_error)?;
    match file.try_lock() {
        Ok(()) => Ok(ArchivePublicationGuard(file)),
        Err(std::fs::TryLockError::WouldBlock) => Err(ThreadStoreError::Conflict {
            message: "rollout archive or migration journal publication is busy".to_string(),
        }),
        Err(std::fs::TryLockError::Error(error)) => Err(migration_error(error)),
    }
}

pub(super) fn pins(journal: &LineageMigrationJournal) -> HashSet<(ThreadId, PathBuf)> {
    journal
        .sources
        .iter()
        .chain(&journal.authentication_sources)
        .map(|source| (source.thread_id, source.path.clone()))
        .chain(
            journal
                .history_bases
                .iter()
                .map(|source| (source.thread_id, source.path.clone())),
        )
        .chain(
            journal
                .reference_dependencies
                .iter()
                .map(|source| (source.thread_id, source.path.clone())),
        )
        .chain(
            journal
                .targets
                .iter()
                .chain(journal.review_input_target.iter())
                .map(|target| (target.thread_id, target.path.clone())),
        )
        .collect()
}

fn physical_path(path: &Path) -> ThreadStoreResult<PathBuf> {
    match std::fs::canonicalize(path) {
        Ok(path) => Ok(codex_rollout::plain_rollout_path(&path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // Targets need not exist yet. Resolve existing parents so SQLite aliases and
            // compressed/plain names still compare to the exact files archive will move.
            let mut missing = Vec::new();
            let mut parent = path;
            loop {
                match std::fs::canonicalize(parent) {
                    Ok(mut resolved) => {
                        for component in missing.into_iter().rev() {
                            resolved.push(component);
                        }
                        return Ok(codex_rollout::plain_rollout_path(&resolved));
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        missing.push(parent.file_name().ok_or_else(|| {
                            migration_error("migration journal path has no resolvable parent")
                        })?);
                        parent = parent.parent().ok_or_else(|| {
                            migration_error("migration journal path has no parent")
                        })?;
                    }
                    Err(error) => return Err(migration_error(error)),
                }
            }
        }
        Err(error) => Err(migration_error(error)),
    }
}

async fn selected_physical_path(path: &Path) -> ThreadStoreResult<Option<PathBuf>> {
    let plain = codex_rollout::plain_rollout_path(path);
    let mut compressed_filename = plain
        .file_name()
        .ok_or_else(|| migration_error("selected rollout has no filename"))?
        .to_os_string();
    compressed_filename.push(".zst");
    let compressed = plain.with_file_name(compressed_filename);
    for candidate in [plain, compressed] {
        match tokio::fs::symlink_metadata(&candidate).await {
            Ok(_) => {
                return tokio::fs::canonicalize(candidate)
                    .await
                    .map(Some)
                    .map_err(migration_error);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(migration_error(error)),
        }
    }
    Ok(None)
}

pub(in crate::local) async fn check_archive(
    store: &super::LocalThreadStore,
    thread_id: ThreadId,
    moves: &[(PathBuf, PathBuf)],
) -> ThreadStoreResult<()> {
    let home = &store.config.codex_home;
    let paths = moves
        .iter()
        .flat_map(|(source, destination)| [source, destination])
        .map(|path| physical_path(path))
        .collect::<ThreadStoreResult<HashSet<_>>>()?;
    for owner in pending_migration_thread_ids(home).await? {
        let bytes = match tokio::fs::read(migration_journal_path(home, owner)).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(migration_error(error)),
        };
        let affected = if bytes.is_empty() && owner == thread_id {
            true
        } else if bytes.is_empty() {
            let state = store.state_db().await.ok_or_else(|| {
                migration_error("cannot authenticate a pending migration without SQLite metadata")
            })?;
            let metadata = state
                .get_thread(owner)
                .await
                .map_err(migration_error)?
                .ok_or_else(|| {
                    migration_error("pending migration has no selected SQLite rollout")
                })?;
            match selected_physical_path(&metadata.rollout_path).await? {
                Some(selected) => paths.contains(&codex_rollout::plain_rollout_path(&selected)),
                None => {
                    // An empty marker cannot persist a moved path. With both old forms absent,
                    // protect this exact physical filename without selecting a replacement.
                    let selected = codex_rollout::plain_rollout_path(&metadata.rollout_path);
                    paths
                        .iter()
                        .any(|path| path.file_name() == selected.file_name())
                }
            }
        } else {
            let journal = parse_lineage_migration_journal(&bytes)?;
            if journal.selected_thread_id != owner {
                return Err(migration_error(
                    "migration journal filename does not match its owner",
                ));
            }
            let mut affected = owner == thread_id;
            for (dependency, path) in pins(&journal) {
                affected |= dependency == thread_id || paths.contains(&physical_path(&path)?);
            }
            affected
        };
        if affected {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "thread {thread_id} is needed by pending rollout migration {owner}"
                ),
            });
        }
    }
    Ok(())
}

pub(super) async fn validate_new_source_paths(
    journal: &LineageMigrationJournal,
) -> ThreadStoreResult<()> {
    let sources = journal
        .sources
        .iter()
        .chain(&journal.authentication_sources)
        .map(|source| (source.thread_id, Some(source.segment_id), &source.path))
        .chain(
            journal
                .history_bases
                .iter()
                .map(|source| (source.thread_id, None, &source.path)),
        )
        .chain(journal.reference_dependencies.iter().map(|source| {
            (
                source.thread_id,
                Some(Some(source.segment_id)),
                &source.path,
            )
        }));
    for (thread_id, segment_id, path) in sources {
        let metadata = codex_rollout::read_session_meta_line_exact(path)
            .await
            .map_err(|error| ThreadStoreError::Conflict {
                message: format!("migration source moved before journal publication: {error}"),
            })?;
        if metadata.meta.id != thread_id
            || segment_id.is_some_and(|expected| metadata.meta.segment_id != expected)
        {
            return Err(ThreadStoreError::Conflict {
                message: "migration source identity changed before journal publication".to_string(),
            });
        }
    }
    Ok(())
}

impl LocalThreadStore {
    pub(super) async fn begin_ordinary_migration(
        &self,
        thread_id: ThreadId,
        path: &Path,
        expected_mode: ThreadHistoryMode,
        locks: &Arc<MigrationLocks>,
    ) -> ClassifiedMigrationResult<codex_state::ThreadMetadata> {
        let store = self.clone();
        let path = path.to_path_buf();
        let locks = Arc::clone(locks);
        tokio::spawn(async move {
            let gate = Arc::new(try_acquire(&store.config.codex_home)?);
            let pending = migration_journal_path(&store.config.codex_home, thread_id);
            // No cleanup is eligible until all nonmutating checks have accepted this exact source.
            let indexed =
                preflight_ordinary_marker(&store, thread_id, &path, expected_mode, &pending)
                    .await?;
            let rollout_id = super::migration_rollout_id(&path, thread_id);
            let started = async {
                with_failure_reason(
                    thread_history::delete_thread(&store, rollout_id).await,
                    RolloutMigrationFailureReason::SqliteMaterializationFailed,
                )?;
                with_failure_reason(
                    write_migration_journal_guarded(&pending, &locks, Arc::clone(&gate)).await,
                    RolloutMigrationFailureReason::RolloutPublishFailed,
                )?;
                Ok::<_, RolloutMigrationFailure>(())
            }
            .await;
            if let Err(failure) = started {
                return match store
                    .cleanup_failed_unpublished_migration(thread_id, &path, &pending, &locks)
                    .await
                {
                    Ok(()) => Err(failure),
                    Err(cleanup) => Err(RolloutMigrationFailure::new(
                        failure.reason,
                        migration_error(format!(
                            "{}; failed to clean up started migration: {cleanup}",
                            failure.error
                        )),
                    )),
                };
            }
            Ok(indexed)
        })
        .await
        .map_err(|error| RolloutMigrationFailure::from(migration_error(error)))?
    }
}

async fn preflight_ordinary_marker(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    path: &Path,
    expected_mode: ThreadHistoryMode,
    pending: &Path,
) -> ThreadStoreResult<codex_state::ThreadMetadata> {
    let conflict = || ThreadStoreError::Conflict {
        message: "ordinary migration source changed before journal publication".to_string(),
    };
    let metadata = codex_rollout::read_session_meta_line_exact(path)
        .await
        .map_err(|_| conflict())?;
    if metadata.meta.id != thread_id || metadata.meta.history_mode != expected_mode {
        return Err(conflict());
    }
    let state = store.state_db().await.ok_or_else(conflict)?;
    let indexed = state
        .get_thread(thread_id)
        .await
        .map_err(migration_error)?
        .ok_or_else(conflict)?;
    match selected_physical_path(&indexed.rollout_path).await? {
        Some(selected) if selected != std::fs::canonicalize(path).map_err(|_| conflict())? => {
            return Err(conflict());
        }
        None => {
            // Preserve same_rollout_moved's filename binding, but distinguish true absence
            // of both old compression forms from lookup errors. The caller already resolved
            // this readable current file; do not select another path or repair SQLite here.
            if codex_rollout::plain_rollout_path(&indexed.rollout_path).file_name()
                != codex_rollout::plain_rollout_path(path).file_name()
            {
                return Err(conflict());
            }
        }
        Some(_) => {}
    }
    match tokio::fs::read(pending).await {
        Ok(bytes) if bytes.is_empty() => {}
        Ok(_) => {
            return Err(ThreadStoreError::Conflict {
                message: "ordinary migration cannot replace a nonempty pending journal".to_string(),
            });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(migration_error(error)),
    }
    Ok(indexed)
}

#[cfg(test)]
#[path = "archive_gate_tests.rs"]
mod tests;
