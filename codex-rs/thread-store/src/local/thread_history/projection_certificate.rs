//! Certifies when a staged root projection contains the selected lineage's item rows.

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::RolloutId;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sqlx::Sqlite;
use sqlx::Transaction;
use tokio::io::AsyncReadExt;

use super::super::LocalThreadStore;
use super::super::rollout_lineage::RetrySourceStamp;
use super::super::rollout_lineage::RolloutLineage;
use super::thread_history_error;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// The exact files and ordinal ranges used to publish one complete root projection.
#[derive(Serialize, Deserialize)]
pub(in crate::local) struct ProjectionCertificate {
    version: u8,
    root_rollout_id: RolloutId,
    root_path: PathBuf,
    sources: Vec<CertifiedSource>,
    /// Digest of the certified mutable root, when small enough to revalidate on append.
    #[serde(default)]
    root_sha256: Option<[u8; 32]>,
}

/// Physical identity and selected cutoff of a file used in a staged projection.
#[derive(Serialize, Deserialize)]
struct CertifiedSource {
    owner_id: codex_protocol::ThreadId,
    rollout_id: RolloutId,
    path: PathBuf,
    start_ordinal: u64,
    end_ordinal: Option<u64>,
    end_byte_offset: Option<u64>,
    identity: FileIdentity,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::local) struct FileIdentity {
    device: u64,
    inode: u64,
    size: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}

impl FileIdentity {
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Appends may change timestamps and size, but not the selected physical file.
    pub(super) fn same_file_with_at_least(&self, current: &Self) -> bool {
        self.device == current.device && self.inode == current.inode && current.size >= self.size
    }
}

const MAX_APPEND_PROOF_BYTES: u64 = 64 * 1024 * 1024;

#[cfg(unix)]
pub(super) fn file_identity(metadata: &std::fs::Metadata) -> Option<FileIdentity> {
    use std::os::unix::fs::MetadataExt;

    metadata.file_type().is_file().then_some(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.len(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        ctime: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec(),
    })
}

#[cfg(not(unix))]
pub(super) fn file_identity(_metadata: &std::fs::Metadata) -> Option<FileIdentity> {
    None
}

pub(in crate::local) async fn leaf_identity(path: &Path) -> Option<FileIdentity> {
    file_identity(&tokio::fs::symlink_metadata(path).await.ok()?)
}

/// Hashes the exact leaf and its old prefix while rejecting replacement or concurrent writes.
async fn hash_leaf(
    path: &Path,
    prefix_len: u64,
) -> Option<([u8; 32], [u8; 32], [u8; 32], FileIdentity)> {
    let before = file_identity(&tokio::fs::symlink_metadata(path).await.ok()?)?;
    if before.size > MAX_APPEND_PROOF_BYTES || prefix_len > before.size {
        return None;
    }
    let mut file = tokio::fs::File::open(path).await.ok()?;
    if file_identity(&file.metadata().await.ok()?).as_ref() != Some(&before) {
        return None;
    }
    let mut all = sha2::Sha256::new();
    let mut prefix = sha2::Sha256::new();
    let mut suffix = sha2::Sha256::new();
    let mut offset = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).await.ok()?;
        if count == 0 {
            break;
        }
        let remaining = prefix_len.saturating_sub(offset);
        let prefix_count = usize::try_from(remaining.min(count as u64)).ok()?;
        prefix.update(&buffer[..prefix_count]);
        suffix.update(&buffer[prefix_count..count]);
        all.update(&buffer[..count]);
        offset = offset.checked_add(count as u64)?;
    }
    let after = file_identity(&tokio::fs::symlink_metadata(path).await.ok()?)?;
    if offset != before.size
        || after != before
        || file_identity(&file.metadata().await.ok()?).as_ref() != Some(&before)
    {
        return None;
    }
    Some((
        prefix.finalize().into(),
        suffix.finalize().into(),
        all.finalize().into(),
        after,
    ))
}

impl ProjectionCertificate {
    /// Captures identities only after the rebuild has verified its full-file SHA256 stamps.
    pub(in crate::local) async fn capture(
        lineage: &RolloutLineage,
        stamps: &[RetrySourceStamp],
    ) -> ThreadStoreResult<Option<Self>> {
        #[cfg(not(unix))]
        {
            let _ = (lineage, stamps);
            return Ok(None);
        }
        #[cfg(unix)]
        {
            if lineage.segments().len() != stamps.len()
                || lineage.segments().is_empty()
                || lineage.has_attached_visibility
                || lineage
                    .segments()
                    .iter()
                    .any(|segment| segment.filters_items())
            {
                return Ok(None);
            }
            let mut sources = Vec::with_capacity(stamps.len());
            for (segment, stamp) in lineage.segments().iter().zip(stamps) {
                let path = segment.rollout_path().to_path_buf();
                if path.to_str().is_none() {
                    return Ok(None);
                }
                let metadata = tokio::fs::symlink_metadata(&path)
                    .await
                    .map_err(thread_history_error)?;
                if !stamp.matches_metadata(&metadata) {
                    return Err(ThreadStoreError::Conflict {
                        message: "projection source changed before certificate publication"
                            .to_string(),
                    });
                }
                if !strict_source_records(&path, segment.end_ordinal()).await? {
                    return Ok(None);
                }
                let after = tokio::fs::symlink_metadata(&path)
                    .await
                    .map_err(thread_history_error)?;
                if !stamp.matches_metadata(&after) {
                    return Err(ThreadStoreError::Conflict {
                        message: "projection source changed during certificate validation"
                            .to_string(),
                    });
                }
                let Some(identity) = file_identity(&metadata) else {
                    return Ok(None);
                };
                sources.push(CertifiedSource {
                    owner_id: segment.thread_id(),
                    rollout_id: segment.rollout_id(),
                    path,
                    start_ordinal: segment.start_ordinal(),
                    end_ordinal: segment.end_ordinal(),
                    end_byte_offset: segment.jsonl_end_byte_offset(),
                    identity,
                });
            }
            let root_sha256 = if let Some(root) = sources.last()
                && root.path.extension().and_then(|value| value.to_str()) == Some("jsonl")
            {
                hash_leaf(&root.path, root.identity.size)
                    .await
                    .filter(|(_, _, _, identity)| identity == &root.identity)
                    .map(|(_, _, digest, _)| digest)
            } else {
                None
            };
            Ok(Some(Self {
                version: 1,
                root_rollout_id: lineage.root_rollout_id(),
                root_path: lineage.leaf_path().to_path_buf(),
                sources,
                root_sha256,
            }))
        }
    }

    async fn files_unchanged(&self) -> bool {
        for source in &self.sources {
            let Ok(metadata) = tokio::fs::symlink_metadata(&source.path).await else {
                return false;
            };
            if file_identity(&metadata).as_ref() != Some(&source.identity) {
                return false;
            }
        }
        true
    }
}

/// A skipped or unordered source record cannot support an absence certificate.
async fn strict_source_records(path: &Path, end: Option<u64>) -> ThreadStoreResult<bool> {
    let mut reader = codex_rollout::open_rollout_line_reader_exact(path)
        .await
        .map_err(thread_history_error)?;
    let mut previous = None;
    while let Some(line) = reader.next_line().await.map_err(thread_history_error)? {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(Some(record)) =
            codex_rollout::RolloutRecorder::parse_rollout_line_bytes(line.as_bytes())
        else {
            return Ok(false);
        };
        let Some(ordinal) = record.ordinal else {
            return Ok(false);
        };
        if end.is_some_and(|end| ordinal >= end) {
            break;
        }
        match &record.item {
            codex_rollout::RolloutItem::RolloutReference(_) => return Ok(false),
            codex_rollout::RolloutItem::SessionMeta(meta)
                if meta.meta.attached_thread
                    || meta.meta.subagent_history_start_ordinal.is_some() =>
            {
                return Ok(false);
            }
            _ => {}
        }
        if previous.is_some_and(|previous: u64| previous.checked_add(1) != Some(ordinal))
            || chrono::DateTime::parse_from_rfc3339(&record.timestamp).is_err()
        {
            return Ok(false);
        }
        previous = Some(ordinal);
    }
    Ok(true)
}

/// Retains a verified ancestor projection only for a participating, append-only leaf write.
#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_leaf_refresh(
    transaction: &mut Transaction<'_, Sqlite>,
    root_id: &str,
    path: &Path,
    start_offset: u64,
    next_offset: u64,
    projections: &[super::RolloutProjectionStep],
    captured_leaf: &FileIdentity,
    source_sha256: [u8; 32],
) -> ThreadStoreResult<Option<ProjectionCertificate>> {
    if next_offset <= start_offset || path.extension().and_then(|v| v.to_str()) != Some("jsonl") {
        return Ok(None);
    }
    let tables = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (\
         'frodex_history_lineage_proofs', 'frodex_history_lineage_proof_sources')",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(thread_history_error)?;
    if tables != 2 {
        return Ok(None);
    }
    let Some(encoded) = sqlx::query_scalar::<_, String>(
        "SELECT proof_json FROM frodex_history_lineage_proofs WHERE root_id = ?",
    )
    .bind(root_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(thread_history_error)?
    else {
        return Ok(None);
    };
    let Ok(mut proof) = serde_json::from_str::<ProjectionCertificate>(&encoded) else {
        return Ok(None);
    };
    if proof.version != 1
        || proof.root_rollout_id.to_string() != root_id
        || proof.root_path != path
        || proof.sources.is_empty()
        || !proof_storage_is_valid(transaction, root_id, &proof).await?
    {
        return Ok(None);
    }
    let Some(old_sha256) = proof.root_sha256 else {
        return Ok(None);
    };
    let Some((root, ancestors)) = proof.sources.split_last_mut() else {
        return Ok(None);
    };
    if root.path != path || root.end_ordinal.is_some() || root.identity.size != start_offset {
        return Ok(None);
    }
    for source in ancestors.iter() {
        let Ok(metadata) = tokio::fs::symlink_metadata(&source.path).await else {
            return Ok(None);
        };
        if file_identity(&metadata).as_ref() != Some(&source.identity) {
            return Ok(None);
        }
    }
    let Some((prefix, suffix, digest, identity)) = hash_leaf(path, start_offset).await else {
        return Ok(None);
    };
    if prefix != old_sha256
        || suffix != source_sha256
        || &identity != captured_leaf
        || identity.size != next_offset
        || identity.device != root.identity.device
        || identity.inode != root.identity.inode
        || !strict_source_records(path, None).await.unwrap_or(false)
    {
        return Ok(None);
    }
    let Ok(leaf_start) = i64::try_from(root.start_ordinal) else {
        return Ok(None);
    };
    for projection in projections {
        let super::RolloutProjectionStep::Line(line) = projection else {
            return Ok(None);
        };
        if !line.changes.removed_turn_ids.is_empty() {
            return Ok(None);
        }
        for turn in &line.changes.changed_turns {
            let old = sqlx::query_scalar::<_, i64>(
                "SELECT rollout_ordinal FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
            )
            .bind(root_id)
            .bind(&turn.turn_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(thread_history_error)?;
            if old.is_some_and(|ordinal| ordinal < leaf_start) {
                return Ok(None);
            }
        }
        for item in &line.changes.changed_items {
            let old = sqlx::query_scalar::<_, i64>(
                "SELECT rollout_ordinal FROM thread_items WHERE thread_id = ? AND turn_id = ? AND item_id = ?",
            )
            .bind(root_id)
            .bind(&item.turn_id)
            .bind(item.item.id())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(thread_history_error)?;
            if old.is_some_and(|ordinal| ordinal < leaf_start) {
                return Ok(None);
            }
        }
    }
    root.identity = identity;
    proof.root_sha256 = Some(digest);
    Ok(Some(proof))
}

/// Installs optional proof storage in the rebuildable history database, not the state schema.
pub(super) async fn install_tables_and_triggers(
    transaction: &mut Transaction<'_, Sqlite>,
) -> ThreadStoreResult<()> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS frodex_history_lineage_proofs (root_id TEXT PRIMARY KEY, proof_json TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS frodex_history_lineage_proof_sources (root_id TEXT NOT NULL, source_id TEXT NOT NULL, PRIMARY KEY(root_id, source_id))",
        "CREATE INDEX IF NOT EXISTS frodex_history_lineage_proof_sources_by_source ON frodex_history_lineage_proof_sources(source_id, root_id)",
        "CREATE TRIGGER IF NOT EXISTS frodex_history_lineage_proof_cleanup AFTER DELETE ON frodex_history_lineage_proofs BEGIN DELETE FROM frodex_history_lineage_proof_sources WHERE root_id = OLD.root_id; END",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(thread_history_error)?;
    }
    for table in [
        "thread_items",
        "thread_turns",
        "thread_history_projection_state",
    ] {
        for (event, ids) in [
            ("INSERT", "NEW.thread_id"),
            ("UPDATE", "OLD.thread_id, NEW.thread_id"),
            ("DELETE", "OLD.thread_id"),
        ] {
            let name = format!(
                "frodex_lineage_proof_{table}_{}",
                event.to_ascii_lowercase()
            );
            let statement = format!(
                "CREATE TRIGGER IF NOT EXISTS {name} AFTER {event} ON {table} BEGIN \
                 DELETE FROM frodex_history_lineage_proofs WHERE root_id IN \
                 (SELECT root_id FROM frodex_history_lineage_proof_sources WHERE source_id IN ({ids})) \
                 OR root_id IN ({ids}); END"
            );
            // Every interpolated SQL fragment comes from the fixed lists above.
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&mut **transaction)
                .await
                .map_err(thread_history_error)?;
        }
    }
    Ok(())
}

/// Checks the original source rows against staging before publication replaces the root rows.
pub(super) async fn coverage_is_complete(
    transaction: &mut Transaction<'_, Sqlite>,
    certificate: &ProjectionCertificate,
    selected_id: &str,
    staged_id: &str,
) -> ThreadStoreResult<bool> {
    if certificate.root_rollout_id.to_string() != selected_id {
        return Ok(false);
    }
    if !certificate.files_unchanged().await {
        return Err(ThreadStoreError::Conflict {
            message: "projection sources changed before certificate publication".to_string(),
        });
    }
    let Some(root_source) = certificate.sources.last() else {
        return Ok(false);
    };
    let root_owner = root_source.owner_id;
    let root_id = certificate.root_rollout_id.to_string();
    let root_state = sqlx::query_as::<_, (i64, i64)>(
        "SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(&root_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(thread_history_error)?;
    let Some((root_offset, root_ordinal)) = root_state else {
        return Ok(false);
    };
    if root_offset < 0
        || root_offset == i64::MAX
        || root_ordinal < 0
        || root_source.path != certificate.root_path
        || root_offset as u64 != root_source.identity.size
    {
        return Ok(false);
    }
    for source in &certificate.sources {
        // The complete root checkpoint also covers that thread's rotated predecessors.
        let same_owner = source.owner_id == root_owner;
        let indexed_id = if same_owner {
            certificate.root_rollout_id
        } else {
            source.rollout_id
        };
        let state = sqlx::query_as::<_, (i64, i64)>(
            "SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
        )
        .bind(indexed_id.to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(thread_history_error)?;
        let Some((offset, ordinal)) = state else {
            return Ok(false);
        };
        if offset < 0 || offset == i64::MAX || ordinal < 0 {
            return Ok(false);
        }
        if source.end_ordinal.is_some_and(|end| (ordinal as u64) < end)
            || (!same_owner
                && source
                    .end_byte_offset
                    .is_some_and(|end| (offset as u64) < end))
            || (source.end_ordinal.is_none()
                && (source.path != certificate.root_path || offset as u64 != source.identity.size))
        {
            return Ok(false);
        }
        let Ok(start) = i64::try_from(source.start_ordinal) else {
            return Ok(false);
        };
        let Ok(end) = source.end_ordinal.map(i64::try_from).transpose() else {
            return Ok(false);
        };
        let missing = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM thread_items AS source WHERE source.thread_id = ? \
             AND source.rollout_ordinal >= ? AND (? IS NULL OR source.rollout_ordinal < ?) \
             AND NOT EXISTS (SELECT 1 FROM thread_items AS root WHERE root.thread_id = ? \
             AND root.turn_id = source.turn_id AND root.item_id = source.item_id \
             AND root.rollout_ordinal = source.rollout_ordinal \
             AND root.updated_at_ordinal = source.updated_at_ordinal \
             AND root.created_at_ms = source.created_at_ms \
             AND root.started_at_ms IS source.started_at_ms \
             AND root.completed_at_ms IS source.completed_at_ms \
             AND root.item_json = source.item_json))",
        )
        .bind(indexed_id.to_string())
        .bind(start)
        .bind(end)
        .bind(end)
        .bind(staged_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(thread_history_error)?;
        if missing != 0 {
            return Ok(false);
        }
        if same_owner {
            let missing_turn = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(SELECT 1 FROM thread_turns AS source WHERE source.thread_id = ? \
                 AND source.rollout_ordinal >= ? AND (? IS NULL OR source.rollout_ordinal < ?) \
                 AND NOT EXISTS (SELECT 1 FROM thread_turns AS root WHERE root.thread_id = ? \
                 AND root.turn_id = source.turn_id AND root.rollout_ordinal = source.rollout_ordinal \
                 AND root.root_turn_id IS source.root_turn_id AND root.status = source.status \
                 AND root.error_json IS source.error_json AND root.started_at IS source.started_at \
                 AND root.completed_at IS source.completed_at AND root.duration_ms IS source.duration_ms \
                 AND root.first_user_item_id IS source.first_user_item_id \
                 AND root.final_agent_item_id IS source.final_agent_item_id))",
            )
            .bind(indexed_id.to_string())
            .bind(start)
            .bind(end)
            .bind(end)
            .bind(staged_id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(thread_history_error)?;
            if missing_turn != 0 {
                return Ok(false);
            }
        }
    }
    if !certificate.files_unchanged().await {
        return Err(ThreadStoreError::Conflict {
            message: "projection sources changed during certificate publication".to_string(),
        });
    }
    Ok(true)
}

/// Installs the proof after the same transaction publishes the covered staging rows.
pub(super) async fn publish(
    transaction: &mut Transaction<'_, Sqlite>,
    certificate: &ProjectionCertificate,
) -> ThreadStoreResult<()> {
    if !certificate.files_unchanged().await {
        return Err(ThreadStoreError::Conflict {
            message: "projection sources changed during certificate publication".to_string(),
        });
    }
    if let Some(expected) = certificate.root_sha256 {
        let Some(root) = certificate.sources.last() else {
            return Err(thread_history_error("certificate has no root source"));
        };
        let Some((_, _, actual, identity)) = hash_leaf(&root.path, root.identity.size).await else {
            return Err(thread_history_error(
                "certificate root could not be revalidated",
            ));
        };
        if actual != expected || identity != root.identity {
            return Err(ThreadStoreError::Conflict {
                message: "projection root changed during certificate publication".to_string(),
            });
        }
    }
    let root = certificate.root_rollout_id.to_string();
    let encoded = serde_json::to_string(certificate).map_err(thread_history_error)?;
    sqlx::query("DELETE FROM frodex_history_lineage_proofs WHERE root_id = ?")
        .bind(&root)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;
    sqlx::query("DELETE FROM frodex_history_lineage_proof_sources WHERE root_id = ?")
        .bind(&root)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;
    sqlx::query("INSERT INTO frodex_history_lineage_proofs(root_id, proof_json) VALUES (?, ?)")
        .bind(&root)
        .bind(encoded)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;
    for source in &certificate.sources {
        sqlx::query("INSERT OR IGNORE INTO frodex_history_lineage_proof_sources(root_id, source_id) VALUES (?, ?)")
            .bind(&root)
            .bind(source.rollout_id.to_string())
            .execute(&mut **transaction)
            .await
            .map_err(thread_history_error)?;
    }
    Ok(())
}

/// A lower bound and the physical files that must remain unchanged through the page read.
pub(super) struct CertifiedItemBound {
    certificate: ProjectionCertificate,
    pub(super) minimum: Option<u64>,
}

impl CertifiedItemBound {
    pub(super) async fn files_unchanged(&self) -> bool {
        self.certificate.files_unchanged().await
    }

    pub(super) fn is_single_owner(&self, owner: codex_protocol::ThreadId) -> bool {
        self.certificate
            .sources
            .iter()
            .all(|source| source.owner_id == owner)
    }

    pub(super) fn contains_unambiguous_ordinal(&self, ordinal: u64) -> bool {
        self.certificate
            .sources
            .iter()
            .filter(|source| {
                ordinal >= source.start_ordinal
                    && source.end_ordinal.is_none_or(|end| ordinal < end)
            })
            .take(2)
            .count()
            == 1
    }

    /// Identifies the one certified physical source containing a creation ordinal.
    pub(super) fn source_for_ordinal(
        &self,
        ordinal: u64,
    ) -> Option<(codex_protocol::ThreadId, RolloutId)> {
        let mut matches = self.certificate.sources.iter().filter(|source| {
            ordinal >= source.start_ordinal && source.end_ordinal.is_none_or(|end| ordinal < end)
        });
        let source = matches.next()?;
        matches
            .next()
            .is_none()
            .then_some((source.owner_id, source.rollout_id))
    }
}

async fn proof_storage_is_valid(
    transaction: &mut Transaction<'_, Sqlite>,
    root_id: &str,
    proof: &ProjectionCertificate,
) -> ThreadStoreResult<bool> {
    let source_ids = sqlx::query_scalar::<_, String>(
        "SELECT source_id FROM frodex_history_lineage_proof_sources WHERE root_id = ?",
    )
    .bind(root_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(thread_history_error)?
    .into_iter()
    .collect::<HashSet<_>>();
    let expected_sources = proof
        .sources
        .iter()
        .map(|source| source.rollout_id.to_string())
        .collect::<HashSet<_>>();
    if source_ids != expected_sources {
        return Ok(false);
    }
    let trigger_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name IN (\
         'frodex_history_lineage_proof_cleanup', \
         'frodex_lineage_proof_thread_items_insert', \
         'frodex_lineage_proof_thread_items_update', \
         'frodex_lineage_proof_thread_items_delete', \
         'frodex_lineage_proof_thread_turns_insert', \
         'frodex_lineage_proof_thread_turns_update', \
         'frodex_lineage_proof_thread_turns_delete', \
         'frodex_lineage_proof_thread_history_projection_state_insert', \
         'frodex_lineage_proof_thread_history_projection_state_update', \
         'frodex_lineage_proof_thread_history_projection_state_delete')",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(thread_history_error)?;
    Ok(trigger_count == 10)
}

/// Reads a root-bound item lower bound in the same SQLite snapshot as its page.
pub(super) async fn certified_item_lower_bound(
    transaction: &mut Transaction<'_, Sqlite>,
    root_rollout_id: RolloutId,
    root_path: &Path,
    turn_id: &str,
) -> ThreadStoreResult<Option<CertifiedItemBound>> {
    let table_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (\
         'frodex_history_lineage_proofs', 'frodex_history_lineage_proof_sources')",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(thread_history_error)?;
    if table_count != 2 {
        return Ok(None);
    }
    let proof = sqlx::query_scalar::<_, String>(
        "SELECT proof_json FROM frodex_history_lineage_proofs WHERE root_id = ?",
    )
    .bind(root_rollout_id.to_string())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(thread_history_error)?;
    let Some(proof) = proof else {
        return Ok(None);
    };
    let Ok(proof) = serde_json::from_str::<ProjectionCertificate>(&proof) else {
        return Ok(None);
    };
    if proof.version != 1
        || proof.root_rollout_id != root_rollout_id
        || proof.root_path != root_path
        || proof.sources.is_empty()
        || !proof.files_unchanged().await
    {
        return Ok(None);
    }
    if !proof_storage_is_valid(transaction, &root_rollout_id.to_string(), &proof).await? {
        return Ok(None);
    }
    let minimum = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MIN(rollout_ordinal) FROM thread_items WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(root_rollout_id.to_string())
    .bind(turn_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(thread_history_error)?;
    if !proof.files_unchanged().await {
        return Ok(None);
    }
    let minimum = match minimum.map(u64::try_from).transpose() {
        Ok(minimum) => minimum,
        Err(_) => return Ok(None),
    };
    Ok(Some(CertifiedItemBound {
        certificate: proof,
        minimum,
    }))
}

pub(in crate::local) async fn certificate_is_current(
    store: &LocalThreadStore,
    root_rollout_id: RolloutId,
    root_path: &Path,
) -> ThreadStoreResult<bool> {
    let pool = store.thread_history_db().await?;
    let mut transaction = pool.begin().await.map_err(thread_history_error)?;
    Ok(
        certified_item_lower_bound(&mut transaction, root_rollout_id, root_path, "")
            .await?
            .is_some(),
    )
}

/// Confirms that a complete root projection contains only this thread's physical sources.
pub(in crate::local) async fn certificate_is_current_for_single_owner(
    store: &LocalThreadStore,
    root_rollout_id: RolloutId,
    root_path: &Path,
    owner: codex_protocol::ThreadId,
) -> ThreadStoreResult<bool> {
    let pool = store.thread_history_db().await?;
    let mut transaction = pool.begin().await.map_err(thread_history_error)?;
    Ok(
        certified_item_lower_bound(&mut transaction, root_rollout_id, root_path, "")
            .await?
            .is_some_and(|proof| proof.is_single_owner(owner)),
    )
}

#[cfg(test)]
mod tests {
    use super::CertifiedItemBound;
    use super::CertifiedSource;
    use super::FileIdentity;
    use super::ProjectionCertificate;
    use super::strict_source_records;
    use codex_protocol::protocol::RolloutReferenceItem;
    use codex_protocol::protocol::SessionMeta;
    use codex_protocol::protocol::SessionMetaLine;
    use codex_rollout::RolloutItem;
    use codex_rollout::RolloutLine;

    #[test]
    fn source_ordinal_requires_one_certified_range() {
        let first = codex_protocol::ThreadId::new();
        let second = codex_protocol::ThreadId::new();
        let identity = FileIdentity {
            device: 1,
            inode: 1,
            size: 1,
            mtime: 0,
            mtime_nsec: 0,
            ctime: 0,
            ctime_nsec: 0,
        };
        let source = |owner_id, start_ordinal, end_ordinal| CertifiedSource {
            owner_id,
            rollout_id: owner_id,
            path: std::path::PathBuf::from("source"),
            start_ordinal,
            end_ordinal,
            end_byte_offset: None,
            identity: identity.clone(),
        };
        let proof = CertifiedItemBound {
            certificate: ProjectionCertificate {
                version: 1,
                root_rollout_id: second,
                root_path: std::path::PathBuf::from("root"),
                sources: vec![source(first, 0, Some(5)), source(second, 4, None)],
                root_sha256: None,
            },
            minimum: None,
        };
        assert_eq!(proof.source_for_ordinal(3), Some((first, first)));
        assert_eq!(proof.source_for_ordinal(4), None);
        assert_eq!(proof.source_for_ordinal(5), Some((second, second)));
    }

    #[tokio::test]
    async fn reference_and_subagent_history_do_not_get_coverage_certificates() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("source.jsonl");
        let line = |item| RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(0),
            item,
        };
        let reference = line(RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: home.path().join("parent.jsonl"),
            thread_id: None,
            rollout_id: None,
            rollout_timestamp: None,
            segment_id: None,
            max_depth: 2,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }));
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&reference).unwrap()),
        )
        .unwrap();
        assert!(!strict_source_records(&path, None).await.unwrap());

        let metadata = line(RolloutItem::SessionMeta(Box::new(SessionMetaLine {
            meta: SessionMeta {
                subagent_history_start_ordinal: Some(1),
                ..SessionMeta::default()
            },
            git: None,
        })));
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&metadata).unwrap()),
        )
        .unwrap();
        assert!(!strict_source_records(&path, None).await.unwrap());
    }
}
