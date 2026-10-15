use super::*;
use std::io::Write;
use std::sync::LazyLock;
use std::sync::Mutex;

use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use crate::local::test_support::test_config;
use crate::local::test_support::write_archived_session_file;
use crate::local::test_support::write_session_file;

const DAY: Duration = Duration::from_secs(86_400);

/// Isolated metadata changes at the last pre-deletion check, keyed by each test's home.
static UNARCHIVE_BEFORE_APPLY: LazyLock<Mutex<HashMap<PathBuf, ThreadId>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A one-shot failure before a selected thread's destructive work starts.
static FAIL_BEFORE_DELETE: LazyLock<Mutex<HashMap<PathBuf, ThreadId>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) fn before_thread_delete(
    store: &LocalThreadStore,
    id: ThreadId,
) -> ThreadStoreResult<()> {
    let mut failures = FAIL_BEFORE_DELETE.lock().unwrap();
    if failures.get(&store.config.codex_home) == Some(&id) {
        failures.remove(&store.config.codex_home);
        return Err(invalid("injected thread deletion failure"));
    }
    Ok(())
}

pub(super) async fn before_apply(store: &LocalThreadStore) {
    let id = UNARCHIVE_BEFORE_APPLY
        .lock()
        .unwrap()
        .remove(&store.config.codex_home);
    if let Some(id) = id {
        let pool = store
            .config
            .sqlite
            .open_read_write_pool(&store.config.sqlite.state_db_path())
            .await
            .unwrap();
        sqlx::query("UPDATE threads SET archived = 0, archived_at = NULL WHERE id = ?")
            .bind(id.to_string())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}

async fn store(home: &TempDir) -> LocalThreadStore {
    let config = test_config(home.path());
    let db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .unwrap();
    LocalThreadStore::new(config, Some(db))
}

async fn add_thread(
    store: &LocalThreadStore,
    number: u128,
    archived_at: Option<DateTime<Utc>>,
    archived: bool,
) -> (ThreadId, PathBuf) {
    let uuid = Uuid::from_u128(number);
    let id = ThreadId::from_string(&uuid.to_string()).unwrap();
    let path = if archived {
        write_archived_session_file(&store.config.codex_home, "2025-01-03T12-00-00", uuid).unwrap()
    } else {
        write_session_file(&store.config.codex_home, "2025-01-03T12-00-00", uuid).unwrap()
    };
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        id,
        path.clone(),
        now() - chrono::Duration::days(100),
        SessionSource::Cli,
    )
    .build("test-provider");
    metadata.cwd = store.config.codex_home.clone();
    metadata.archived_at = archived_at;
    store
        .state_db
        .as_ref()
        .unwrap()
        .upsert_thread(&metadata)
        .await
        .unwrap();
    // Keep archive status and archive date independently testable, including legacy unknown dates.
    let pool = store
        .config
        .sqlite
        .open_read_write_pool(&store.config.sqlite.state_db_path())
        .await
        .unwrap();
    sqlx::query("UPDATE threads SET archived = ? WHERE id = ?")
        .bind(archived)
        .bind(id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    (id, path)
}

fn append_reference(path: &Path, target: &Path, id: ThreadId, rollout_id: ThreadId) {
    let item = codex_rollout::RolloutLine {
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: target.to_path_buf(),
            thread_id: Some(id),
            rollout_id: Some(rollout_id),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: 100,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    };
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(file, "{}", serde_json::to_string(&item).unwrap()).unwrap();
}

fn set_history_base(path: &Path, id: ThreadId) {
    let contents = std::fs::read_to_string(path).unwrap();
    let mut lines = contents
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    lines[0]["payload"]["history_base"] =
        serde_json::json!({"thread_id": id, "end_ordinal_exclusive": 1, "end_byte_offset": 1});
    let mut file = File::create(path).unwrap();
    for line in lines {
        writeln!(file, "{line}").unwrap();
    }
}

#[test]
fn prune_header_discovery_bounds_plain_and_compressed_first_records() {
    let home = TempDir::new().unwrap();
    let plain = home.path().join("oversized.jsonl");
    File::create(&plain)
        .unwrap()
        .set_len(MAX_RECORD_BYTES + 1)
        .unwrap();
    let compressed = plain.with_extension("jsonl.zst");
    let mut encoder =
        zstd::stream::write::Encoder::new(File::create(&compressed).unwrap(), 1).unwrap();
    io::copy(&mut File::open(&plain).unwrap(), &mut encoder).unwrap();
    encoder.finish().unwrap();
    for path in [&plain, &compressed] {
        let error = read_owner(path).unwrap_err();
        assert!(error.to_string().contains("oversized or truncated"));
    }
}

#[test]
fn prune_header_discovery_rejects_invalid_first_records_without_skipping_them() {
    let home = TempDir::new().unwrap();
    let uuid = Uuid::from_u128(1);
    let path = write_session_file(home.path(), "2025-01-03T12-00-00", uuid).unwrap();
    let valid = std::fs::read(&path).unwrap();
    let id = ThreadId::from_string(&uuid.to_string()).unwrap();
    assert_eq!(read_owner(&path).unwrap(), id);
    for prefix in [b"\n".as_slice(), b"malformed\n", b"{}\n"] {
        let mut file = File::create(&path).unwrap();
        file.write_all(prefix).unwrap();
        file.write_all(&valid).unwrap();
        assert!(read_owner(&path).is_err());
    }
}

#[tokio::test]
async fn prune_missing_database_does_not_create_metadata_or_delete_rollouts() {
    let home = TempDir::new().unwrap();
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let path = write_archived_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(1))
        .unwrap();
    let before = std::fs::read(&path).unwrap();
    for apply in [false, true] {
        let report = store
            .prune_archived_at(DAY, apply, true, now())
            .await
            .unwrap();
        assert!(!report.archive_metadata_available);
        assert!(report.eligible_thread_ids.is_empty());
    }
    assert!(!store.config.sqlite.state_db_path().exists());
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[tokio::test]
async fn prune_preview_uses_archive_age_and_strict_cutoff_without_deletion() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let old = now() - chrono::Duration::days(2);
    let (eligible, path) = add_thread(&store, 1, Some(old), true).await;
    let (_, boundary) = add_thread(&store, 2, Some(now() - chrono::Duration::days(1)), true).await;
    let (_, recent) = add_thread(&store, 3, Some(now()), true).await;
    let (_, active) = add_thread(&store, 4, Some(old), false).await;
    let (_, unknown) = add_thread(&store, 5, None, true).await;
    let before = std::fs::read(&path).unwrap();
    let report = store
        .prune_archived_at(DAY, false, false, now())
        .await
        .unwrap();
    assert_eq!(report.eligible_thread_ids, vec![eligible]);
    assert!(report.deleted_thread_ids.is_empty());
    assert_eq!(std::fs::read(path).unwrap(), before);
    for path in [boundary, recent, active, unknown] {
        assert!(path.exists());
    }
}

#[tokio::test]
async fn prune_requires_positive_duration_and_explicit_offline_assertion() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let (_, path) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    assert!(
        store
            .prune_archived_at(Duration::ZERO, true, true, now())
            .await
            .is_err()
    );
    assert!(
        store
            .prune_archived_at(DAY, true, false, now())
            .await
            .is_err()
    );
    assert!(path.exists());
}

#[tokio::test]
async fn prune_apply_deletes_only_expired_archive_and_is_repeatable() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let (id, path) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    let (_, recent) = add_thread(&store, 2, Some(now()), true).await;
    let report = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap();
    assert_eq!(report.deleted_thread_ids, vec![id]);
    assert!(!path.exists());
    assert!(recent.exists());
    assert!(
        store
            .state_db
            .as_ref()
            .unwrap()
            .get_thread(id)
            .await
            .unwrap()
            .is_none()
    );
    let report = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap();
    assert!(report.deleted_thread_ids.is_empty());
}

#[tokio::test]
async fn prune_retains_transitive_nonleading_references_from_active_and_recent_threads() {
    for archived_child in [false, true] {
        let home = TempDir::new().unwrap();
        let store = store(&home).await;
        let old = now() - chrono::Duration::days(2);
        let (ancestor_id, ancestor) = add_thread(&store, 1, Some(old), true).await;
        let (parent_id, parent) = add_thread(&store, 2, Some(old), true).await;
        let (_, child) =
            add_thread(&store, 3, archived_child.then_some(now()), archived_child).await;
        append_reference(&parent, &ancestor, ancestor_id, ancestor_id);
        append_reference(&child, &parent, parent_id, parent_id);
        let report = store
            .prune_archived_at(DAY, true, true, now())
            .await
            .unwrap();
        assert!(report.deleted_thread_ids.is_empty());
        assert_eq!(report.referenced_thread_ids.len(), 2);
        for path in [ancestor, parent, child] {
            assert!(path.exists());
        }
    }
}

#[tokio::test]
async fn prune_retains_native_physical_history_base_and_detached_references() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let old = now() - chrono::Duration::days(2);
    let (id, original) = add_thread(&store, 1, Some(old), true).await;
    let physical = ThreadId::from_string(&Uuid::from_u128(101).to_string()).unwrap();
    let path = codex_rollout::rollout_path_with_rollout_id(&original, physical).unwrap();
    std::fs::rename(&original, &path).unwrap();
    store
        .state_db
        .as_ref()
        .unwrap()
        .mark_archived(id, &path, old)
        .await
        .unwrap();
    let (_, child) = add_thread(&store, 2, None, false).await;
    set_history_base(&child, physical);
    let (other_id, other) = add_thread(&store, 3, Some(old), true).await;
    let detached_root = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join("snapshot");
    std::fs::create_dir_all(&detached_root).unwrap();
    let detached = detached_root.join("snapshot.jsonl");
    std::fs::copy(&child, &detached).unwrap();
    append_reference(&detached, &other, other_id, other_id);
    let report = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap();
    assert_eq!(report.referenced_thread_ids.len(), 2);
    assert!(report.deleted_thread_ids.is_empty());
    for file in [path, other, detached] {
        assert!(file.exists());
    }
}

#[tokio::test]
async fn prune_scans_compressed_siblings_and_can_delete_compressed_archives() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let old = now() - chrono::Duration::days(2);
    let (parent_id, parent) = add_thread(&store, 1, Some(old), true).await;
    let (_, child) = add_thread(&store, 2, None, false).await;
    let plain_child = std::fs::read(&child).unwrap();
    append_reference(&child, &parent, parent_id, parent_id);
    let compressed_child = child.with_extension("jsonl.zst");
    std::fs::write(
        &compressed_child,
        zstd::stream::encode_all(File::open(&child).unwrap(), 1).unwrap(),
    )
    .unwrap();
    std::fs::write(&child, plain_child).unwrap();
    let (delete_id, delete) = add_thread(&store, 3, Some(old), true).await;
    let compressed_delete = delete.with_extension("jsonl.zst");
    std::fs::write(
        &compressed_delete,
        zstd::stream::encode_all(File::open(&delete).unwrap(), 1).unwrap(),
    )
    .unwrap();
    std::fs::remove_file(&delete).unwrap();
    let report = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap();
    assert_eq!(report.deleted_thread_ids, vec![delete_id]);
    assert_eq!(report.referenced_thread_ids, vec![parent_id]);
    assert!(!compressed_delete.exists());
    assert!(parent.exists());
    assert!(compressed_child.exists());
}

#[tokio::test]
async fn prune_refuses_conflicting_owners_in_plain_and_compressed_deletion_siblings() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let (_, archived) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    let (_, active) = add_thread(&store, 2, None, false).await;
    let compressed = archived.with_extension("jsonl.zst");
    let bytes = zstd::stream::encode_all(File::open(&active).unwrap(), 1).unwrap();
    std::fs::write(&compressed, &bytes).unwrap();
    let before = std::fs::read(&archived).unwrap();
    let error = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflicting ownership"));
    assert_eq!(std::fs::read(archived).unwrap(), before);
    assert_eq!(std::fs::read(compressed).unwrap(), bytes);
    assert!(active.exists());
}

#[tokio::test]
async fn prune_without_state_handle_refuses_selected_alias_outside_canonical_deletion_set() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let old = now() - chrono::Duration::days(2);
    let (id, canonical) = add_thread(&store, 1, Some(old), true).await;
    let alias = canonical.with_file_name("selected-alias.jsonl");
    let bytes = std::fs::read(&canonical).unwrap();
    std::fs::write(&alias, &bytes).unwrap();
    store
        .state_db
        .as_ref()
        .unwrap()
        .mark_archived(id, &alias, old)
        .await
        .unwrap();
    let cli_store = LocalThreadStore::new(test_config(home.path()), None);
    for apply in [false, true] {
        let error = cli_store
            .prune_archived_at(DAY, apply, true, now())
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("outside its authenticated deletion set")
        );
    }
    assert_eq!(std::fs::read(&canonical).unwrap(), bytes);
    assert_eq!(std::fs::read(&alias).unwrap(), bytes);
    let metadata = store
        .state_db
        .as_ref()
        .unwrap()
        .get_thread(id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.rollout_path, alias);
    assert_eq!(metadata.archived_at, Some(old));
}

#[tokio::test]
async fn prune_refuses_corrupt_truncated_missing_and_inflight_history_before_deletion() {
    for corruption in ["malformed", "truncated", "missing", "inflight"] {
        let home = TempDir::new().unwrap();
        let store = store(&home).await;
        let (_, archived) =
            add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
        let (_, active) = add_thread(&store, 2, None, false).await;
        match corruption {
            "malformed" => {
                writeln!(
                    std::fs::OpenOptions::new()
                        .append(true)
                        .open(&active)
                        .unwrap(),
                    "not json"
                )
                .unwrap();
            }
            "truncated" => {
                write!(
                    std::fs::OpenOptions::new()
                        .append(true)
                        .open(&active)
                        .unwrap(),
                    "{{"
                )
                .unwrap();
            }
            "missing" => {
                set_history_base(
                    &active,
                    ThreadId::from_string(&Uuid::from_u128(99).to_string()).unwrap(),
                );
            }
            "inflight" => {
                std::fs::write(active.with_extension("tmp"), "pending publication").unwrap();
            }
            _ => unreachable!(),
        }
        let before = std::fs::read(&archived).unwrap();
        assert!(
            store
                .prune_archived_at(DAY, true, true, now())
                .await
                .is_err(),
            "{corruption}"
        );
        assert_eq!(std::fs::read(archived).unwrap(), before);
    }
}

#[tokio::test]
async fn prune_refuses_active_writer_maintenance_and_lifecycle_reservations() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let (id, path) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    let other_store = LocalThreadStore::new(test_config(home.path()), None);
    let writer = other_store.acquire_writer_lock(id).unwrap();
    assert!(
        store
            .prune_archived_at(DAY, true, true, now())
            .await
            .is_err()
    );
    drop(writer);
    let maintenance = codex_rollout::try_acquire_rollout_maintenance_lock(home.path())
        .unwrap()
        .unwrap();
    assert!(
        store
            .prune_archived_at(DAY, true, true, now())
            .await
            .is_err()
    );
    drop(maintenance);
    let lifecycle = store.live_writer_locks.reserve_lifecycle(id).await;
    assert!(
        store
            .prune_archived_at(DAY, true, true, now())
            .await
            .is_err()
    );
    drop(lifecycle);
    assert!(path.exists());
}

#[tokio::test]
async fn prune_rechecks_archived_state_after_scan_before_deleting() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let (id, path) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    UNARCHIVE_BEFORE_APPLY
        .lock()
        .unwrap()
        .insert(home.path().to_path_buf(), id);
    let error = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("changed during inspection"));
    assert!(path.exists());
}

#[tokio::test]
async fn prune_deletes_expired_children_before_their_expired_ancestors() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let old = now() - chrono::Duration::days(2);
    let (parent_id, parent) = add_thread(&store, 1, Some(old), true).await;
    let (child_id, child) = add_thread(&store, 2, Some(old), true).await;
    append_reference(&child, &parent, parent_id, parent_id);
    let report = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap();
    assert_eq!(report.deleted_thread_ids, vec![child_id, parent_id]);
    assert!(!parent.exists() && !child.exists());
}

#[tokio::test]
async fn prune_completed_threads_are_removed_from_metadata_before_later_failure() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let old = now() - chrono::Duration::days(2);
    let (parent_id, parent) = add_thread(&store, 1, Some(old), true).await;
    let (child_id, child) = add_thread(&store, 2, Some(old), true).await;
    append_reference(&child, &parent, parent_id, parent_id);
    FAIL_BEFORE_DELETE
        .lock()
        .unwrap()
        .insert(home.path().to_path_buf(), parent_id);
    assert!(
        store
            .prune_archived_at(DAY, true, true, now())
            .await
            .unwrap_err()
            .to_string()
            .contains("injected")
    );
    assert!(!child.exists());
    assert!(parent.exists());
    assert!(
        store
            .state_db
            .as_ref()
            .unwrap()
            .get_thread(child_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .state_db
            .as_ref()
            .unwrap()
            .get_thread(parent_id)
            .await
            .unwrap()
            .is_some()
    );
    let retry = store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap();
    assert_eq!(retry.deleted_thread_ids, vec![parent_id]);
    assert!(!parent.exists());
}

#[tokio::test]
async fn prune_orders_same_thread_physical_children_before_ancestors() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let (id, selected) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    let physical = ThreadId::from_string(&Uuid::from_u128(101).to_string()).unwrap();
    let ancestor =
        codex_rollout::history_rollout_path_with_rollout_id(&selected, physical).unwrap();
    std::fs::copy(&selected, &ancestor).unwrap();
    set_history_base(&selected, physical);
    let files = scan_files(&inventory(home.path()).unwrap()).unwrap();
    let edges = resolve_edges(home.path(), &files).await.unwrap();
    let mut owned = delete_thread::owned_rollouts_for_thread(&store, id)
        .await
        .unwrap();
    assert_eq!(owned[0].path, ancestor);
    order_owned_rollouts(&mut owned, &edges).unwrap();
    assert_eq!(owned[0].path, selected);
    assert_eq!(owned[1].path, ancestor);
}

#[tokio::test]
async fn prune_runs_host_cleanup_only_for_threads_being_deleted_not_preview() {
    let home = TempDir::new().unwrap();
    let calls = std::sync::Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let store = store(&home).await.with_thread_data_cleanup(move |ids| {
        let observed = observed.clone();
        Box::pin(async move {
            observed.lock().unwrap().extend(ids);
            Ok(())
        })
    });
    let (id, path) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    let (_, recent) = add_thread(&store, 2, Some(now()), true).await;
    store
        .prune_archived_at(DAY, false, false, now())
        .await
        .unwrap();
    assert!(calls.lock().unwrap().is_empty());
    store
        .prune_archived_at(DAY, true, true, now())
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![id]);
    assert!(!path.exists());
    assert!(recent.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn prune_refuses_symlinked_history_entries() {
    let home = TempDir::new().unwrap();
    let store = store(&home).await;
    let (_, path) = add_thread(&store, 1, Some(now() - chrono::Duration::days(2)), true).await;
    std::os::unix::fs::symlink(&path, path.with_file_name("alias.jsonl")).unwrap();
    assert!(
        store
            .prune_archived_at(DAY, true, true, now())
            .await
            .is_err()
    );
    assert!(path.exists());
}
