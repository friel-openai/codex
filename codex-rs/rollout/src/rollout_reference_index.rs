//! Indexes direct fork references found in local rollout files.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;

use crate::ARCHIVED_SESSIONS_SUBDIR;
use crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR;
use crate::RolloutItem;
use crate::SESSIONS_SUBDIR;

/// Direct history-base edges discovered from local rollout metadata.
///
/// This indexes immutable rollout IDs, not a thread's selected lineage. Callers use it to answer
/// cheap inverse-reference questions without each reimplementing rollout discovery.
#[derive(Debug, Default)]
pub struct RolloutReferenceIndex {
    rollouts_by_id: HashMap<RolloutId, IndexedRollout>,
    direct_references_by_rollout: HashMap<RolloutId, HashSet<RolloutId>>,
    reference_counts_by_rollout: HashMap<RolloutId, usize>,
}

#[derive(Debug)]
struct IndexedRollout {
    thread_id: ThreadId,
    path: PathBuf,
    history_base: Option<HistoryPosition>,
}

impl RolloutReferenceIndex {
    /// Scans active and archived local rollout metadata.
    pub async fn scan(codex_home: &Path) -> io::Result<Self> {
        Self::scan_paths(
            codex_home,
            vec![
                codex_home.join(ARCHIVED_SESSIONS_SUBDIR),
                codex_home.join(SESSIONS_SUBDIR),
            ],
        )
        .await
    }

    /// Scans only unarchived rollouts to locate files that still need to be archived.
    ///
    /// Reference counts exclude archived history and must not be used to decide whether a
    /// rollout can be deleted or compressed.
    pub async fn scan_unarchived(codex_home: &Path) -> io::Result<Self> {
        Self::scan_paths(codex_home, vec![codex_home.join(SESSIONS_SUBDIR)]).await
    }

    async fn scan_paths(codex_home: &Path, mut stack: Vec<PathBuf>) -> io::Result<Self> {
        let canonical_home = tokio::fs::canonicalize(codex_home).await.ok();
        let mut rollouts_by_id = HashMap::new();
        let mut direct_references_by_rollout = HashMap::new();
        while let Some(directory) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(directory.as_path()).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            loop {
                let Some(entry) = entries.next_entry().await? else {
                    break;
                };
                let path = entry.path();
                let file_type = entry.file_type().await?;
                if file_type.is_dir() {
                    stack.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if crate::compression::parse_rollout_file_name(file_name).is_none() {
                    continue;
                }
                let Ok((meta, leading_reference)) =
                    read_direct_reference_metadata(path.as_path()).await
                else {
                    continue;
                };
                let rollout_id =
                    crate::rollout_id_from_path(path.as_path()).unwrap_or(meta.meta.id);
                let history_base = meta.meta.history_base;
                if let Some(history_base) = history_base {
                    direct_references_by_rollout
                        .entry(rollout_id)
                        .or_insert_with(HashSet::new)
                        .insert(history_base.thread_id);
                }
                if let Some(reference) = leading_reference
                    && !references_detached_segment(codex_home, &reference)
                    && !canonical_home
                        .as_ref()
                        .is_some_and(|home| references_detached_segment(home, &reference))
                    && let Some(referenced_rollout_id) =
                        reference.rollout_id.or(reference.thread_id)
                {
                    direct_references_by_rollout
                        .entry(rollout_id)
                        .or_insert_with(HashSet::new)
                        .insert(referenced_rollout_id);
                }
                if let Entry::Vacant(entry) = rollouts_by_id.entry(rollout_id) {
                    entry.insert(IndexedRollout {
                        thread_id: meta.meta.id,
                        path,
                        history_base,
                    });
                }
            }
        }

        let mut reference_counts_by_rollout = HashMap::new();
        for (rollout_id, direct_references) in &direct_references_by_rollout {
            for referenced_rollout_id in direct_references {
                if referenced_rollout_id == rollout_id {
                    continue;
                }
                *reference_counts_by_rollout
                    .entry(*referenced_rollout_id)
                    .or_default() += 1;
            }
        }
        Ok(Self {
            rollouts_by_id,
            direct_references_by_rollout,
            reference_counts_by_rollout,
        })
    }

    /// Returns how many other discovered rollouts directly reference `rollout_id`.
    pub fn reference_count(&self, rollout_id: RolloutId) -> usize {
        self.reference_counts_by_rollout
            .get(&rollout_id)
            .copied()
            .unwrap_or_default()
    }

    /// Returns both history-base and leading legacy-reference edges for this rollout.
    pub fn direct_references(&self, rollout_id: RolloutId) -> Option<&HashSet<RolloutId>> {
        self.direct_references_by_rollout.get(&rollout_id)
    }

    /// Returns the direct history-base edge for `rollout_id`, if one was discovered.
    pub fn history_base(&self, rollout_id: RolloutId) -> Option<&HistoryPosition> {
        self.rollouts_by_id
            .get(&rollout_id)
            .and_then(|rollout| rollout.history_base.as_ref())
    }

    /// Returns rollout IDs and paths whose session metadata belongs to `thread_id`.
    pub fn rollouts_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> impl Iterator<Item = (RolloutId, &Path)> {
        self.rollouts_by_id
            .iter()
            .filter(move |(_, rollout)| rollout.thread_id == thread_id)
            .map(|(rollout_id, rollout)| (*rollout_id, rollout.path.as_path()))
    }
}

/// Immutable snapshot references protect the snapshot path, not the mutable rollout that supplied
/// its bytes. The snapshot lives outside the active and archived roots maintained by compression
/// and thread deletion, so indexing its source rollout ID would incorrectly pin that mutable file.
fn references_detached_segment(codex_home: &Path, reference: &RolloutReferenceItem) -> bool {
    let (Some(thread_id), Some(segment_id)) = (reference.thread_id, reference.segment_id) else {
        return false;
    };
    let expected_parent = codex_home
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string());
    let Ok(relative_path) = reference.rollout_path.strip_prefix(expected_parent) else {
        return false;
    };
    let mut components = relative_path.components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

async fn read_direct_reference_metadata(
    path: &Path,
) -> io::Result<(SessionMetaLine, Option<RolloutReferenceItem>)> {
    let mut reader = crate::compression::open_rollout_line_reader_exact(path).await?;
    let mut session_meta = None;
    let mut leading_reference = None;
    while let Some(line) = reader.next_line().await? {
        let Ok(Some(line)) =
            crate::recorder::RolloutRecorder::parse_rollout_line_bytes(line.trim().as_bytes())
        else {
            continue;
        };
        match line.item {
            RolloutItem::SessionMeta(meta) if session_meta.is_none() => {
                session_meta = Some(meta);
            }
            RolloutItem::RolloutReference(reference) if session_meta.is_some() => {
                leading_reference = Some(reference);
                break;
            }
            _ if session_meta.is_some() => break,
            _ => {}
        }
    }
    session_meta
        .map(|meta| (meta, leading_reference))
        .ok_or_else(|| {
            io::Error::other(format!(
                "rollout {} has no session metadata",
                path.display()
            ))
        })
}

#[cfg(test)]
#[path = "rollout_reference_index_tests.rs"]
mod tests;
