//! Optional persisted copies of complete, source-verified predecessor scans.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use codex_protocol::ThreadId;
use sha2::Digest;
use sha2::Sha256;

use super::LocalThreadStore;
use super::MAX_ANCESTOR_TURN_BYTES;
use super::MAX_ANCESTOR_TURNS;
use super::MAX_ANCHOR_BYTES;
use super::SourceTurnEvent;
use super::UncertifiedItemScan;

// Change the version whenever source-scan or cached-record semantics change.
const SCAN_VERSION: i64 = 1;
const MAX_PERSISTED_BYTES: usize = 64 * 1024 * 1024;
const MAX_PERSISTED_SCANS: i64 = 8;
const WRITE_TIMEOUT: Duration = Duration::from_millis(250);

/// The scan contains turn metadata, not the summary items read for a particular page.
pub(super) mod turn_events {
    use codex_app_server_protocol::ThreadHistoryTurnMetadata;
    use codex_app_server_protocol::TurnError;
    use codex_app_server_protocol::TurnStatus;
    use codex_protocol::ThreadId;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
    use serde::ser::Error;
    use serde::ser::SerializeSeq;

    use super::super::SourceTurnEvent;

    #[derive(Serialize)]
    struct TurnRef<'a> {
        ordinal: u64,
        owner: ThreadId,
        turn_id: &'a str,
        root_turn_id: &'a Option<String>,
        status: &'a TurnStatus,
        error: &'a Option<TurnError>,
        started_at: Option<i64>,
        completed_at: Option<i64>,
        duration_ms: Option<i64>,
    }

    #[derive(Deserialize)]
    struct Turn {
        ordinal: u64,
        owner: ThreadId,
        turn_id: String,
        root_turn_id: Option<String>,
        status: TurnStatus,
        error: Option<TurnError>,
        started_at: Option<i64>,
        completed_at: Option<i64>,
        duration_ms: Option<i64>,
    }

    pub(in crate::local) fn serialize<S: Serializer>(
        events: &[SourceTurnEvent],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(events.len()))?;
        for event in events {
            let SourceTurnEvent::Turn(ordinal, owner, turn) = event else {
                return Err(S::Error::custom(
                    "unexpected summary item in predecessor scan",
                ));
            };
            sequence.serialize_element(&TurnRef {
                ordinal: *ordinal,
                owner: *owner,
                turn_id: &turn.turn_id,
                root_turn_id: &turn.root_turn_id,
                status: &turn.status,
                error: &turn.error,
                started_at: turn.started_at,
                completed_at: turn.completed_at,
                duration_ms: turn.duration_ms,
            })?;
        }
        sequence.end()
    }

    pub(in crate::local) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<SourceTurnEvent>, D::Error> {
        Vec::<Turn>::deserialize(deserializer).map(|turns| {
            turns
                .into_iter()
                .map(|turn| {
                    SourceTurnEvent::Turn(
                        turn.ordinal,
                        turn.owner,
                        ThreadHistoryTurnMetadata {
                            turn_id: turn.turn_id,
                            root_turn_id: turn.root_turn_id,
                            status: turn.status,
                            error: turn.error,
                            started_at: turn.started_at,
                            completed_at: turn.completed_at,
                            duration_ms: turn.duration_ms,
                        },
                    )
                })
                .collect()
        })
    }
}

/// Refuses serialization before allocating more than the persistent cache limit.
#[derive(Default)]
struct BoundedBytes(Vec<u8>);

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_PERSISTED_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other(
                "source scan exceeds persistent cache limit",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Loads only scans published by this version and anchored to an existing projection row.
pub(super) async fn load(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    root_rollout_id: ThreadId,
) -> Option<Arc<UncertifiedItemScan>> {
    let pool = store.thread_history_db().await.ok()?;
    let (bytes, digest): (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT scan, sha256 FROM frodex_source_scan_cache AS cache \
         WHERE cache.thread_id = ? AND cache.root_id = ? AND cache.version = ? \
         AND length(cache.scan) <= ? \
         AND EXISTS (SELECT 1 FROM thread_history_projection_state AS state \
                     WHERE state.thread_id = cache.root_id) \
         AND 2 = (SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name IN \
                  ('frodex_source_scan_root_delete', 'frodex_source_scan_root_move'))",
    )
    .bind(thread_id.to_string())
    .bind(root_rollout_id.to_string())
    .bind(SCAN_VERSION)
    .bind(MAX_PERSISTED_BYTES as i64)
    .fetch_optional(pool)
    .await
    .ok()??;
    tokio::task::spawn_blocking(move || {
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual.as_slice() != digest.as_slice() {
            return None;
        }
        let scan: UncertifiedItemScan = serde_json::from_slice(&bytes).ok()?;
        validate(scan, thread_id, root_rollout_id).map(Arc::new)
    })
    .await
    .ok()?
}

/// Rejects malformed cached data before any segment index can be used as an array index.
fn validate(
    mut scan: UncertifiedItemScan,
    thread_id: ThreadId,
    root_rollout_id: ThreadId,
) -> Option<UncertifiedItemScan> {
    if scan.root.thread_id() != thread_id
        || scan.root.rollout_id() != root_rollout_id
        || scan.ancestors.is_empty()
        || scan.ancestors.len() != scan.identities.len()
        || scan.anchors.len() > MAX_ANCESTOR_TURNS
        || scan.turn_events.len() > MAX_ANCESTOR_TURNS
    {
        return None;
    }
    let mut paths = HashSet::new();
    if !paths.insert(scan.root.rollout_path()) {
        return None;
    }
    let mut previous_end = None;
    for (segment, identity) in scan.ancestors.iter().zip(&scan.identities) {
        let end = segment.end_ordinal()?;
        segment.jsonl_end_byte_offset()?;
        if end < segment.start_ordinal()
            || previous_end.is_some_and(|previous| previous > segment.start_ordinal())
            || segment.physical_size != Some(identity.size())
            || segment.filters_items()
            || !paths.insert(segment.rollout_path())
        {
            return None;
        }
        previous_end = Some(end);
    }
    if previous_end.is_some_and(|end| end > scan.root.start_ordinal()) {
        return None;
    }
    let find_segment = |owner, ordinal| {
        let index = scan
            .ancestors
            .partition_point(|segment| segment.start_ordinal() <= ordinal)
            .checked_sub(1)?;
        let segment = &scan.ancestors[index];
        (segment.thread_id() == owner && segment.contains_ordinal(ordinal)).then_some(index)
    };
    if scan.turn_events.iter().any(|event| match event {
        SourceTurnEvent::Turn(ordinal, owner, _) => find_segment(*owner, *ordinal).is_none(),
        SourceTurnEvent::SummaryItem(..) => true,
    }) {
        return None;
    }
    let mut turn_ids = HashSet::new();
    let mut turn_segments: HashMap<String, Vec<usize>> = HashMap::new();
    let mut anchor_bytes = 0_usize;
    let mut turn_bytes = 0_usize;
    let mut previous_anchor = None;
    for anchor in &scan.anchors {
        if previous_anchor.is_some_and(|previous| previous >= anchor.ordinal) {
            return None;
        }
        previous_anchor = Some(anchor.ordinal);
        anchor_bytes = anchor_bytes.saturating_add(anchor.turn_id.len() + anchor.item_id.len());
        if anchor_bytes > MAX_ANCHOR_BYTES {
            return None;
        }
        let index = find_segment(anchor.owner, anchor.ordinal)?;
        if turn_ids.insert(anchor.turn_id.clone()) {
            turn_bytes = turn_bytes.saturating_add(anchor.turn_id.len());
            if turn_bytes > MAX_ANCESTOR_TURN_BYTES {
                return None;
            }
        }
        let indexes = turn_segments.entry(anchor.turn_id.clone()).or_default();
        if indexes.last() != Some(&index) {
            indexes.push(index);
        }
    }
    scan.turn_ids = turn_ids;
    scan.turn_segments = turn_segments;
    Some(scan)
}

/// Best-effort publication; cache contention or an absent checkpoint never blocks history reads.
pub(super) async fn save(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    root_rollout_id: ThreadId,
    scan: Arc<UncertifiedItemScan>,
) {
    let eligible = tokio::time::timeout(WRITE_TIMEOUT, async {
        let pool = store.thread_history_db().await.ok()?;
        let found = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM thread_history_projection_state WHERE thread_id = ?)",
        )
        .bind(root_rollout_id.to_string())
        .fetch_one(pool)
        .await
        .ok()?;
        Some(found != 0)
    })
    .await
    .ok()
    .flatten()
    .unwrap_or(false);
    if !eligible {
        return;
    }
    let encoded = tokio::task::spawn_blocking(move || {
        let mut bytes = BoundedBytes::default();
        serde_json::to_writer(&mut bytes, &*scan).ok()?;
        let digest = Sha256::digest(&bytes.0).to_vec();
        Some((bytes.0, digest))
    })
    .await
    .ok()
    .flatten();
    let Some((bytes, digest)) = encoded else {
        return;
    };
    let _ = tokio::time::timeout(WRITE_TIMEOUT, async {
        let pool = store.thread_history_db().await.ok()?;
        let mut transaction = pool.begin().await.ok()?;
        for statement in [
            "CREATE TABLE IF NOT EXISTS frodex_source_scan_cache \
             (thread_id TEXT NOT NULL, root_id TEXT PRIMARY KEY, version INTEGER NOT NULL, \
              scan BLOB NOT NULL, sha256 BLOB NOT NULL)",
            "CREATE TRIGGER IF NOT EXISTS frodex_source_scan_root_delete \
             AFTER DELETE ON thread_history_projection_state BEGIN \
             DELETE FROM frodex_source_scan_cache WHERE root_id = OLD.thread_id; END",
            "CREATE TRIGGER IF NOT EXISTS frodex_source_scan_root_move \
             AFTER UPDATE OF thread_id ON thread_history_projection_state \
             WHEN OLD.thread_id != NEW.thread_id BEGIN \
             DELETE FROM frodex_source_scan_cache WHERE root_id = OLD.thread_id; END",
        ] {
            sqlx::query(statement)
                .execute(&mut *transaction)
                .await
                .ok()?;
        }
        sqlx::query(
            "INSERT OR REPLACE INTO frodex_source_scan_cache \
             (thread_id, root_id, version, scan, sha256) \
             SELECT ?, ?, ?, ?, ? WHERE EXISTS \
             (SELECT 1 FROM thread_history_projection_state WHERE thread_id = ?)",
        )
        .bind(thread_id.to_string())
        .bind(root_rollout_id.to_string())
        .bind(SCAN_VERSION)
        .bind(bytes)
        .bind(digest)
        .bind(root_rollout_id.to_string())
        .execute(&mut *transaction)
        .await
        .ok()?;
        sqlx::query(
            "DELETE FROM frodex_source_scan_cache WHERE rowid IN \
             (SELECT rowid FROM frodex_source_scan_cache ORDER BY rowid DESC LIMIT -1 OFFSET ?)",
        )
        .bind(MAX_PERSISTED_SCANS)
        .execute(&mut *transaction)
        .await
        .ok()?;
        transaction.commit().await.ok()?;
        Some(())
    })
    .await;
}
