//! Explicit offline expiry. Archive timestamps select candidates; complete physical history
//! determines which candidates must remain because retained files reference them.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_rollout::RolloutItem;
use serde::Serialize;
use sqlx::Row;

use super::LocalThreadStore;
use super::delete_thread;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

// A malformed record must not allocate memory proportional to an entire rollout file.
const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

/// The result of one explicit retention request, not a persisted expiry policy.
#[derive(Debug, Serialize)]
pub struct ArchivedRetentionReport {
    pub cutoff: DateTime<Utc>,
    /// Old archived threads with no references from files outside the deletion set.
    pub eligible_thread_ids: Vec<ThreadId>,
    /// Old archived threads retained to preserve other history files.
    pub referenced_thread_ids: Vec<ThreadId>,
    pub deleted_thread_ids: Vec<ThreadId>,
    /// False means no existing metadata database could supply archive timestamps.
    pub archive_metadata_available: bool,
}

/// An existing SQLite selection and its authoritative archive time.
#[derive(Debug, PartialEq, Eq)]
struct ArchiveMetadata {
    selected_path: PathBuf,
    archived: bool,
    archived_at: Option<i64>,
}

/// Every physical representation contributes edges, including non-leading references.
struct PhysicalRollout {
    owner: ThreadId,
    rollout_id: Option<ThreadId>,
    references: Vec<Reference>,
}

/// References use physical rollout IDs, not necessarily the stable owning thread ID.
enum Reference {
    Id(ThreadId),
    Rollout(RolloutReferenceItem),
    ReviewInput(ThreadId),
}

impl LocalThreadStore {
    /// Inspect or expire archived threads older than an explicitly supplied duration.
    ///
    /// `offline` is an operator assertion: all other processes using this Codex home must be
    /// stopped and stay stopped. Per-thread locks cannot fence a paused fork that has released
    /// its source writer reservation, nor older binaries that publish new references.
    pub async fn prune_archived(
        &self,
        older_than: Duration,
        apply: bool,
        offline: bool,
    ) -> ThreadStoreResult<ArchivedRetentionReport> {
        self.prune_archived_at(older_than, apply, offline, Utc::now())
            .await
    }

    async fn prune_archived_at(
        &self,
        older_than: Duration,
        apply: bool,
        offline: bool,
        now: DateTime<Utc>,
    ) -> ThreadStoreResult<ArchivedRetentionReport> {
        if older_than.is_zero() {
            return Err(invalid("retention duration must be positive"));
        }
        if apply && !offline {
            return Err(invalid(
                "deletion requires --offline: stop all other processes using this Codex home and keep them stopped until completion",
            ));
        }
        let duration = chrono::Duration::from_std(older_than).map_err(internal)?;
        let cutoff = now
            .checked_sub_signed(duration)
            .ok_or_else(|| invalid("retention duration is out of range"))?;
        let mut report = ArchivedRetentionReport {
            cutoff,
            eligible_thread_ids: Vec::new(),
            referenced_thread_ids: Vec::new(),
            deleted_thread_ids: Vec::new(),
            archive_metadata_available: false,
        };
        let Some(metadata) = read_archive_metadata(self).await? else {
            return Ok(report);
        };
        report.archive_metadata_available = true;
        let candidates = metadata
            .iter()
            .filter_map(|(id, meta)| {
                (meta.archived
                    && meta
                        .archived_at
                        .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
                        .is_some_and(|archived_at| archived_at < cutoff))
                .then_some(*id)
            })
            .collect::<HashSet<_>>();
        if candidates.is_empty() {
            return Ok(report);
        }

        let home = std::fs::canonicalize(&self.config.codex_home).map_err(internal)?;
        let paths = inventory(&home).map_err(internal)?;
        let stamps = file_stamps(&paths).map_err(internal)?;
        // Header discovery precedes reservations; a second inventory and complete scan below
        // reject changes. Offline apply additionally forbids new publishers throughout the run.
        let header_paths = paths.clone();
        let mut owners = tokio::task::spawn_blocking(move || {
            header_paths
                .iter()
                .map(|path| read_owner(path))
                .collect::<io::Result<HashSet<_>>>()
        })
        .await
        .map_err(internal)?
        .map_err(internal)?;
        owners.extend(metadata.keys().copied());
        let mut owners = owners.into_iter().collect::<Vec<_>>();
        owners.sort_unstable_by_key(ToString::to_string);
        let mut lifecycle_guards = Vec::new();
        let mut writer_mutexes = Vec::new();
        let mut writer_guards = Vec::new();
        for &id in &owners {
            let lifecycle = self
                .live_writer_locks
                .coordination(id)
                .await
                .lifecycle
                .clone()
                .try_write_owned()
                .map_err(|_| invalid(format!("thread {id} has an active lifecycle operation")))?;
            let writer_mutex = self.live_writer_locks.try_lock(id).await?;
            if self.live_recorders.lock().await.contains_key(&id) {
                return Err(invalid(format!(
                    "thread {id} has an active writer; stop it before pruning"
                )));
            }
            let writer = self.acquire_writer_lock(id)?;
            // Offline is an operator precondition, not established by these conflict checks.
            // Retained threads need not consume one open lock file each throughout the scan.
            if candidates.contains(&id) {
                lifecycle_guards.push(lifecycle);
                writer_mutexes.push(writer_mutex);
                writer_guards.push(writer);
            }
        }
        let _maintenance = codex_rollout::try_acquire_rollout_maintenance_lock(&home)
            .map_err(internal)?
            .ok_or_else(|| invalid("rollout maintenance is active; retry after it finishes"))?;
        if inventory(&home).map_err(internal)? != paths
            || read_archive_metadata(self).await?.as_ref() != Some(&metadata)
        {
            return Err(invalid(
                "thread metadata or rollout inventory changed; retry with all other Codex processes stopped",
            ));
        }
        let scan_paths = paths.clone();
        let files = tokio::task::spawn_blocking(move || scan_files(&scan_paths))
            .await
            .map_err(internal)?
            .map_err(internal)?;
        let edges = resolve_edges(&home, &files).await?;
        for (id, meta) in &metadata {
            let selected = codex_rollout::existing_rollout_path(&meta.selected_path).await
                .ok_or_else(|| invalid(format!("thread {id} has a missing selected rollout; cannot establish complete history")))?;
            let selected = std::fs::canonicalize(selected).map_err(internal)?;
            if files.get(&selected).is_none_or(|file| file.owner != *id) {
                return Err(invalid(format!(
                    "thread {id} selects history outside the authenticated inventory"
                )));
            }
        }
        let archived_root = home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
        let mut owned_by_thread = HashMap::new();
        for &id in &candidates {
            let selected = codex_rollout::existing_rollout_path(&metadata[&id].selected_path)
                .await
                .ok_or_else(|| {
                    invalid(format!(
                        "archived thread {id} has a missing selected rollout"
                    ))
                })?;
            let selected = std::fs::canonicalize(selected).map_err(internal)?;
            if !selected.starts_with(&archived_root)
                || files.get(&selected).is_none_or(|file| file.owner != id)
            {
                return Err(invalid(format!(
                    "archived thread {id} does not select an authenticated archived rollout"
                )));
            }
            let mut owned = delete_thread::owned_rollouts_for_thread(self, id).await?;
            if owned.is_empty() {
                return Err(invalid(format!("cannot enumerate archived thread {id}")));
            }
            let mut deletion_representations = HashSet::new();
            for rollout in &mut owned {
                let path = std::fs::canonicalize(&rollout.path).map_err(internal)?;
                if files.get(&path).is_none_or(|file| file.owner != id) {
                    return Err(invalid(format!(
                        "cannot authenticate owned rollout {}",
                        path.display()
                    )));
                }
                // A mixed active/archive selection is not an expired archived thread.
                if !path.starts_with(&archived_root) {
                    return Err(invalid(format!(
                        "archived thread {id} still has unarchived rollout files"
                    )));
                }
                rollout.path = path;
                // Canonical deletion removes both physical representations, while discovery
                // prefers plain JSONL. Authenticate the exact files deletion will remove.
                let plain = codex_rollout::plain_rollout_path(&rollout.path);
                for sibling in [plain.clone(), plain.with_extension("jsonl.zst")] {
                    if !sibling.exists() {
                        continue;
                    }
                    if files.get(&sibling).is_none_or(|file| file.owner != id) {
                        return Err(invalid(format!(
                            "conflicting ownership for deletion sibling {}",
                            sibling.display()
                        )));
                    }
                    deletion_representations.insert(sibling);
                }
            }
            // A store opened without a state handle discovers canonical names only. Do not
            // delete the thread's state while its selected noncanonical alias remains intact.
            if !deletion_representations.contains(&selected) {
                return Err(invalid(format!(
                    "selected archived rollout for thread {id} is outside its authenticated deletion set"
                )));
            }
            owned_by_thread.insert(id, owned);
        }
        let mut eligible = candidates.clone();
        loop {
            let mut deletable_paths = HashSet::new();
            for id in &eligible {
                for rollout in &owned_by_thread[id] {
                    let plain = codex_rollout::plain_rollout_path(&rollout.path);
                    for sibling in [plain.clone(), plain.with_extension("jsonl.zst")] {
                        if sibling.exists() {
                            deletable_paths
                                .insert(std::fs::canonicalize(sibling).map_err(internal)?);
                        }
                    }
                }
            }
            let mut retained = files
                .keys()
                .filter(|path| !deletable_paths.contains(*path))
                .cloned()
                .collect::<HashSet<_>>();
            let mut pending = retained.iter().cloned().collect::<Vec<_>>();
            while let Some(path) = pending.pop() {
                for target in &edges[&path] {
                    if retained.insert(target.clone()) {
                        pending.push(target.clone());
                    }
                }
            }
            let before = eligible.len();
            eligible.retain(|id| {
                !retained
                    .iter()
                    .any(|path| deletable_paths.contains(path) && files[path].owner == *id)
            });
            if eligible.len() == before {
                break;
            }
        }
        report.eligible_thread_ids = eligible.iter().copied().collect();
        report.referenced_thread_ids = candidates.difference(&eligible).copied().collect();
        report
            .eligible_thread_ids
            .sort_unstable_by_key(ToString::to_string);
        report
            .referenced_thread_ids
            .sort_unstable_by_key(ToString::to_string);
        if !report.eligible_thread_ids.is_empty() {
            self.ensure_no_attached_threads(&report.eligible_thread_ids)
                .await?;
        }
        if !apply || eligible.is_empty() {
            return Ok(report);
        }
        #[cfg(test)]
        tests::before_apply(self).await;
        // Recheck archive timestamps after the potentially long scan, before any destructive work.
        if inventory(&home).map_err(internal)? != paths
            || file_stamps(&paths).map_err(internal)? != stamps
            || read_archive_metadata(self).await?.as_ref() != Some(&metadata)
        {
            return Err(invalid(
                "thread metadata or rollout inventory changed during inspection; nothing was deleted",
            ));
        }
        let deletion_order = deletion_order(&eligible, &files, &edges)?;
        for id in &deletion_order {
            order_owned_rollouts(owned_by_thread.get_mut(id).expect("planned thread"), &edges)?;
        }
        let mut store = self.clone();
        if store.state_db.is_none() {
            store.state_db = Some(
                codex_state::StateRuntime::init(
                    self.config.sqlite.clone(),
                    self.config.default_model_provider_id.clone(),
                )
                .await
                .map_err(internal)?,
            );
        }
        for id in deletion_order {
            #[cfg(test)]
            tests::before_thread_delete(&store, id)?;
            if let Some(cleanup) = &store.thread_data_cleanup {
                cleanup(vec![id]).await?;
            }
            if let Err(failure) = delete_thread::delete_thread_after_reference_check(
                &store,
                id,
                owned_by_thread.remove(&id).expect("planned thread"),
                &mut writer_guards,
            )
            .await
            {
                if failure.thread_unavailable
                    || codex_rollout::existing_rollout_path(&metadata[&id].selected_path)
                        .await
                        .is_none()
                {
                    delete_thread::delete_state_rows(&store, &[id]).await?;
                }
                return Err(failure.error);
            }
            // Keep completed deletions retryable if the next thread fails. Leaving their state
            // selections behind would make the next strict inventory look incomplete.
            delete_thread::delete_state_rows(&store, &[id]).await?;
            report.deleted_thread_ids.push(id);
        }
        Ok(report)
    }
}

async fn read_archive_metadata(
    store: &LocalThreadStore,
) -> ThreadStoreResult<Option<HashMap<ThreadId, ArchiveMetadata>>> {
    let path = store.config.sqlite.state_db_path();
    if !path.try_exists().map_err(internal)? {
        return Ok(None);
    }
    let pool = store
        .config
        .sqlite
        .open_read_only_pool(&path, Some(Duration::from_secs(5)))
        .await
        .map_err(internal)?;
    let rows = sqlx::query("SELECT id, rollout_path, archived, archived_at FROM threads")
        .fetch_all(&pool)
        .await;
    pool.close().await;
    let mut metadata = HashMap::new();
    for row in rows.map_err(internal)? {
        let id: String = row.try_get("id").map_err(internal)?;
        metadata.insert(
            ThreadId::from_string(&id).map_err(internal)?,
            ArchiveMetadata {
                selected_path: PathBuf::from(
                    row.try_get::<String, _>("rollout_path").map_err(internal)?,
                ),
                archived: row.try_get("archived").map_err(internal)?,
                archived_at: row.try_get("archived_at").map_err(internal)?,
            },
        );
    }
    Ok(Some(metadata))
}

fn inventory(home: &Path) -> io::Result<Vec<PathBuf>> {
    let mut pending = vec![
        home.join(codex_rollout::SESSIONS_SUBDIR),
        home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR),
        home.join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR),
    ];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(error)
                if error.kind() == io::ErrorKind::NotFound && path.parent() == Some(home) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        if meta.is_dir() {
            for entry in std::fs::read_dir(path)? {
                pending.push(entry?.path());
            }
        } else if meta.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
        {
            files.push(path);
        } else {
            return Err(io::Error::other(format!(
                "unsupported or in-flight rollout entry {}; nothing can be pruned",
                path.display()
            )));
        }
    }
    files.sort();
    Ok(files)
}

fn file_stamps(paths: &[PathBuf]) -> io::Result<Vec<(u64, std::time::SystemTime)>> {
    paths
        .iter()
        .map(|path| {
            let metadata = std::fs::symlink_metadata(path)?;
            if !metadata.is_file() {
                return Err(io::Error::other("rollout representation changed"));
            }
            Ok((metadata.len(), metadata.modified()?))
        })
        .collect()
}

fn order_owned_rollouts(
    owned: &mut Vec<delete_thread::OwnedRollout>,
    edges: &HashMap<PathBuf, HashSet<PathBuf>>,
) -> ThreadStoreResult<()> {
    let groups = owned
        .iter()
        .map(|rollout| codex_rollout::plain_rollout_path(&rollout.path))
        .collect::<HashSet<_>>();
    let mut dependencies: HashMap<PathBuf, HashSet<PathBuf>> = groups
        .iter()
        .map(|path| (path.clone(), HashSet::new()))
        .collect();
    for (path, targets) in edges {
        let source = codex_rollout::plain_rollout_path(path);
        if let Some(parents) = dependencies.get_mut(&source) {
            for target in targets {
                let target = codex_rollout::plain_rollout_path(target);
                if target != source && groups.contains(&target) {
                    parents.insert(target);
                }
            }
        }
    }
    let mut incoming = groups
        .iter()
        .map(|path| (path.clone(), 0_usize))
        .collect::<HashMap<_, _>>();
    for parents in dependencies.values() {
        for parent in parents {
            *incoming.get_mut(parent).expect("owned parent") += 1;
        }
    }
    let mut ready = incoming
        .iter()
        .filter_map(|(path, count)| (*count == 0).then_some(path.clone()))
        .collect::<Vec<_>>();
    let mut order = HashMap::new();
    while let Some(path) = ready.pop() {
        order.insert(path.clone(), order.len());
        for parent in &dependencies[&path] {
            let count = incoming.get_mut(parent).expect("owned parent");
            *count -= 1;
            if *count == 0 {
                ready.push(parent.clone());
            }
        }
    }
    if order.len() != groups.len() {
        return Err(invalid(
            "cyclic references within an expired thread; refusing partial deletion",
        ));
    }
    owned.sort_by_key(|rollout| order[&codex_rollout::plain_rollout_path(&rollout.path)]);
    Ok(())
}

// Children are removed before parents. If a later I/O operation fails, surviving threads do
// not reference an already-deleted parent. Cycles cannot provide this retry guarantee.
fn deletion_order(
    eligible: &HashSet<ThreadId>,
    files: &BTreeMap<PathBuf, PhysicalRollout>,
    edges: &HashMap<PathBuf, HashSet<PathBuf>>,
) -> ThreadStoreResult<Vec<ThreadId>> {
    let mut dependencies: HashMap<ThreadId, HashSet<ThreadId>> =
        eligible.iter().map(|id| (*id, HashSet::new())).collect();
    for (path, targets) in edges {
        let owner = files[path].owner;
        if let Some(dependencies) = dependencies.get_mut(&owner) {
            for target in targets {
                let parent = files[target].owner;
                if parent != owner && eligible.contains(&parent) {
                    dependencies.insert(parent);
                }
            }
        }
    }
    let mut incoming: HashMap<ThreadId, usize> = eligible.iter().map(|id| (*id, 0)).collect();
    for parents in dependencies.values() {
        for parent in parents {
            *incoming.get_mut(parent).expect("candidate parent") += 1;
        }
    }
    let mut ready = incoming
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<Vec<_>>();
    let mut order = Vec::new();
    while !ready.is_empty() {
        ready.sort_unstable_by_key(ToString::to_string);
        let id = ready.remove(0);
        order.push(id);
        for parent in &dependencies[&id] {
            let count = incoming.get_mut(parent).expect("candidate parent");
            *count -= 1;
            if *count == 0 {
                ready.push(*parent);
            }
        }
    }
    if order.len() != eligible.len() {
        return Err(invalid(
            "cyclic references between expired threads; refusing partial deletion",
        ));
    }
    Ok(order)
}

fn scan_files(paths: &[PathBuf]) -> io::Result<BTreeMap<PathBuf, PhysicalRollout>> {
    paths
        .iter()
        .map(|path| scan_file(path).map(|file| (path.clone(), file)))
        .collect()
}

fn open_physical_reader(path: &Path) -> io::Result<BufReader<Box<dyn Read>>> {
    let file = File::open(path)?;
    let input: Box<dyn Read> = if path.extension().is_some_and(|ext| ext == "zst") {
        Box::new(zstd::stream::read::Decoder::new(file)?)
    } else {
        Box::new(file)
    };
    Ok(BufReader::new(input))
}

fn read_bounded_record(
    reader: &mut impl BufRead,
    path: &Path,
    line: &mut Vec<u8>,
) -> io::Result<bool> {
    line.clear();
    let read = reader.take(MAX_RECORD_BYTES + 1).read_until(b'\n', line)?;
    if read == 0 {
        return Ok(false);
    }
    if read as u64 > MAX_RECORD_BYTES || !line.ends_with(b"\n") {
        return Err(io::Error::other(format!(
            "oversized or truncated rollout record in {}",
            path.display()
        )));
    }
    Ok(true)
}

fn read_owner(path: &Path) -> io::Result<ThreadId> {
    let mut reader = open_physical_reader(path)?;
    let mut line = Vec::new();
    if !read_bounded_record(&mut reader, path, &mut line)? {
        return Err(io::Error::other(format!(
            "empty rollout {}",
            path.display()
        )));
    }
    let RolloutItem::SessionMeta(meta) = codex_rollout::parse_rollout_line_bytes(&line)?.item
    else {
        return Err(io::Error::other(format!(
            "missing rollout header in {}",
            path.display()
        )));
    };
    Ok(meta.meta.id)
}

fn scan_file(path: &Path) -> io::Result<PhysicalRollout> {
    let mut reader = open_physical_reader(path)?;
    let mut line = Vec::new();
    let mut result = None;
    let review_input = codex_rollout::plain_rollout_path(path)
        .file_name()
        .is_some_and(|name| name == "review-input.jsonl");
    while read_bounded_record(&mut reader, path, &mut line)? {
        if review_input && result.is_some() {
            #[derive(serde::Deserialize)]
            struct ReviewLine {
                record: codex_rollout::ReviewInputRecord,
            }
            let record: ReviewLine = serde_json::from_slice(&line)?;
            let _ = record.record;
            continue;
        }
        let item = codex_rollout::parse_rollout_line_bytes(&line)?.item;
        if result.is_none() {
            let RolloutItem::SessionMeta(meta) = &item else {
                return Err(io::Error::other(format!(
                    "missing rollout header in {}",
                    path.display()
                )));
            };
            result = Some(PhysicalRollout {
                owner: meta.meta.id,
                rollout_id: codex_rollout::rollout_id_from_path(path),
                references: Vec::new(),
            });
        }
        let physical = result.as_mut().expect("authenticated header");
        match item {
            RolloutItem::SessionMeta(meta) => {
                if meta.meta.id != physical.owner {
                    return Err(io::Error::other("conflicting session metadata"));
                }
                if let Some(base) = meta.meta.history_base {
                    physical.references.push(Reference::Id(base.thread_id));
                }
            }
            RolloutItem::RolloutReference(reference) => {
                physical.references.push(Reference::Rollout(reference))
            }
            RolloutItem::Compacted(compacted) => {
                if let Some(position) = compacted
                    .retained_context_replay
                    .and_then(|replay| replay.review_input)
                {
                    physical
                        .references
                        .push(Reference::ReviewInput(position.thread_id));
                }
            }
            _ => {}
        }
    }
    result.ok_or_else(|| io::Error::other(format!("empty rollout {}", path.display())))
}

async fn resolve_edges(
    home: &Path,
    files: &BTreeMap<PathBuf, PhysicalRollout>,
) -> ThreadStoreResult<HashMap<PathBuf, HashSet<PathBuf>>> {
    let mut by_id: HashMap<ThreadId, Vec<PathBuf>> = HashMap::new();
    for (path, file) in files {
        if let Some(id) = file.rollout_id {
            by_id.entry(id).or_default().push(path.clone());
        }
    }
    let mut edges = HashMap::new();
    for (path, file) in files {
        let mut targets = HashSet::new();
        for reference in &file.references {
            match reference {
                Reference::Id(id) => {
                    targets.extend(
                        by_id
                            .get(id)
                            .ok_or_else(|| {
                                invalid(format!(
                                    "missing history_base {id} referenced by {}",
                                    path.display()
                                ))
                            })?
                            .iter()
                            .cloned(),
                    );
                }
                Reference::Rollout(reference) => {
                    let resolved = codex_rollout::resolve_rollout_reference_path(home, reference)
                        .await
                        .map_err(internal)?;
                    let resolved = std::fs::canonicalize(resolved).map_err(internal)?;
                    if !files.contains_key(&resolved) {
                        return Err(invalid(format!(
                            "reference from {} leaves the inspected history",
                            path.display()
                        )));
                    }
                    targets.insert(resolved);
                }
                Reference::ReviewInput(id) => {
                    let target = codex_rollout::review_input_segment_path(home, *id);
                    let target = codex_rollout::existing_rollout_path(&target)
                        .await
                        .ok_or_else(|| invalid(format!("missing review input {id}")))?;
                    let target = std::fs::canonicalize(target).map_err(internal)?;
                    if !files.contains_key(&target) {
                        return Err(invalid("review input leaves the inspected history"));
                    }
                    targets.insert(target);
                }
            }
        }
        edges.insert(path.clone(), targets);
    }
    Ok(edges)
}

fn invalid(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: message.into(),
    }
}
fn internal(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: error.to_string(),
    }
}

#[cfg(test)]
#[path = "prune_archived_tests.rs"]
mod tests;
