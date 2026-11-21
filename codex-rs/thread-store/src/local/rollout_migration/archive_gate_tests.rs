use std::fs;
use std::path::Path;
use std::time::Duration;

use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use pretty_assertions::assert_eq;

use super::super::lineage::plan_legacy_lineage;
use super::super::lineage_journal::write_lineage_migration_journal;
use super::super::tests::indexed_store;
use super::super::tests::segment_reference;
use super::super::tests::user_message;
use super::super::tests::write_legacy_segment;
use super::super::tests::write_rollout;
use super::*;
use crate::ArchiveThreadParams;
use crate::ThreadStore;

async fn journal(home: &Path, path: &Path) -> LineageMigrationJournal {
    LineageMigrationJournal::from_plan(
        &plan_legacy_lineage(home, path)
            .await
            .expect("authenticated lineage plan"),
    )
}

#[tokio::test]
async fn archive_preserves_legacy_bytes_while_migration_job_is_held() {
    let home = tempfile::tempdir().expect("home");
    let id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        id,
        SessionSource::Cli,
        vec![user_message("legacy")],
    );
    let original = fs::read(&path).expect("original bytes");
    let store = indexed_store(home.path()).await;
    store.start_automatic_rollout_migration();
    let _job = codex_rollout::try_acquire_rollout_maintenance_job_lock(home.path())
        .expect("job lock")
        .expect("uncontended job");
    tokio::time::timeout(
        Duration::from_secs(2),
        store.archive_thread(ArchiveThreadParams { thread_id: id }),
    )
    .await
    .expect("archive must not wait for history conversion")
    .expect("archive");
    let metadata = store
        .state_db()
        .await
        .expect("state")
        .get_thread(id)
        .await
        .expect("metadata")
        .expect("thread");
    assert!(metadata.archived_at.is_some());
    assert_eq!(metadata.history_mode, ThreadHistoryMode::Legacy);
    assert_eq!(
        fs::read(metadata.rollout_path).expect("archived bytes"),
        original
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn archive_rejects_own_pending_marker_without_moving_files_or_metadata() {
    let home = tempfile::tempdir().expect("home");
    let id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        id,
        SessionSource::Cli,
        vec![user_message("legacy")],
    );
    let store = indexed_store(home.path()).await;
    let original = fs::read(&path).expect("original bytes");
    let pending = migration_journal_path(home.path(), id);
    fs::create_dir_all(pending.parent().expect("journal directory")).expect("directory");
    fs::write(&pending, []).expect("interrupted ordinary migration marker");
    assert!(matches!(
        store
            .archive_thread(ArchiveThreadParams { thread_id: id })
            .await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert_eq!(fs::read(&path).expect("unchanged rollout"), original);
    assert_eq!(
        fs::read(&pending).expect("retained marker"),
        Vec::<u8>::new()
    );
    let metadata = store
        .state_db()
        .await
        .expect("state")
        .get_thread(id)
        .await
        .expect("metadata")
        .expect("thread");
    assert_eq!(metadata.rollout_path, path);
    assert!(metadata.archived_at.is_none());
}

#[tokio::test]
async fn published_cross_thread_journal_blocks_damaged_sqlite_alias_but_not_disjoint_archive() {
    let home = tempfile::tempdir().expect("home");
    let owner = ThreadId::new();
    let dependency = ThreadId::new();
    let alias_owner = ThreadId::new();
    let disjoint = ThreadId::new();
    let source = write_rollout(
        home.path(),
        owner,
        SessionSource::Cli,
        vec![user_message("owner")],
    );
    let alias = write_rollout(
        home.path(),
        dependency,
        SessionSource::Cli,
        vec![user_message("dependency")],
    );
    let unrelated = write_rollout(
        home.path(),
        disjoint,
        SessionSource::Cli,
        vec![user_message("unrelated")],
    );
    let store = indexed_store(home.path()).await;
    let mut manifest = journal(home.path(), &source).await;
    manifest
        .authentication_sources
        .extend(journal(home.path(), &alias).await.sources);
    let locks = store
        .acquire_migration_locks_for_test(owner)
        .await
        .expect("migration ownership");
    let pending = migration_journal_path(home.path(), owner);
    write_lineage_migration_journal(&pending, &manifest, &locks)
        .await
        .expect("publish journal first");
    drop(locks);
    let journal_bytes = fs::read(&pending).expect("durable journal");
    let damaged = b"{damaged session metadata\n";
    fs::write(&alias, damaged).expect("damaged selected alias");
    let state = store.state_db().await.expect("state");
    let metadata = codex_state::ThreadMetadataBuilder::new(
        alias_owner,
        alias.clone(),
        chrono::Utc::now(),
        SessionSource::Cli,
    )
    .build(&store.config.default_model_provider_id);
    state
        .upsert_thread(&metadata)
        .await
        .expect("SQLite selects another owner's alias");
    assert!(matches!(
        store
            .archive_thread(ArchiveThreadParams {
                thread_id: alias_owner
            })
            .await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert_eq!(fs::read(&alias).expect("retained alias"), damaged);
    assert_eq!(fs::read(&pending).expect("retained journal"), journal_bytes);
    let current = state
        .get_thread(alias_owner)
        .await
        .expect("metadata")
        .expect("alias owner");
    assert_eq!(current.rollout_path, alias);
    assert!(current.archived_at.is_none());

    let unrelated_bytes = fs::read(&unrelated).expect("unrelated bytes");
    store
        .archive_thread(ArchiveThreadParams {
            thread_id: disjoint,
        })
        .await
        .expect("disjoint journal does not block");
    let current = state
        .get_thread(disjoint)
        .await
        .expect("metadata")
        .expect("disjoint thread");
    assert!(current.archived_at.is_some());
    assert_eq!(
        fs::read(current.rollout_path).expect("archived unrelated bytes"),
        unrelated_bytes
    );
    assert_eq!(
        fs::read(&pending).expect("unchanged unrelated journal"),
        journal_bytes
    );
}

#[tokio::test]
async fn archive_first_prevents_publication_of_stale_dependency_paths() {
    let home = tempfile::tempdir().expect("home");
    let owner = ThreadId::new();
    let dependency = ThreadId::new();
    let source = write_rollout(
        home.path(),
        owner,
        SessionSource::Cli,
        vec![user_message("owner")],
    );
    let dependency_path = write_rollout(
        home.path(),
        dependency,
        SessionSource::Cli,
        vec![user_message("dependency")],
    );
    let store = indexed_store(home.path()).await;
    let mut manifest = journal(home.path(), &source).await;
    manifest
        .authentication_sources
        .extend(journal(home.path(), &dependency_path).await.sources);
    let locks = store
        .acquire_migration_locks_for_test(owner)
        .await
        .expect("migration owns selected thread only");
    store
        .archive_thread(ArchiveThreadParams {
            thread_id: dependency,
        })
        .await
        .expect("archive wins before publication");
    let pending = migration_journal_path(home.path(), owner);
    assert!(matches!(
        write_lineage_migration_journal(&pending, &manifest, &locks).await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(
        !pending.exists(),
        "a stale plan must not become a durable recovery obligation"
    );
    assert!(manifest.targets.iter().all(|target| !target.path.exists()));
    let metadata = store
        .state_db()
        .await
        .expect("state")
        .get_thread(dependency)
        .await
        .expect("metadata")
        .expect("dependency");
    assert!(metadata.archived_at.is_some());
    assert!(metadata.rollout_path.exists());
}

#[tokio::test]
async fn archived_target_alias_prevents_publication_without_moving_sources() {
    for compressed in [false, true] {
        let home = tempfile::tempdir().expect("home");
        let owner = ThreadId::new();
        let alias_owner = ThreadId::new();
        let source = write_rollout(
            home.path(),
            owner,
            SessionSource::Cli,
            vec![user_message("owner")],
        );
        let predecessor_id = codex_protocol::SegmentId::new();
        let predecessor = home
            .path()
            .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(owner.to_string())
            .join(predecessor_id.to_string())
            .join(source.file_name().expect("source filename"));
        write_legacy_segment(
            &predecessor,
            home.path(),
            owner,
            predecessor_id,
            vec![user_message("predecessor")],
        );
        write_legacy_segment(
            &source,
            home.path(),
            owner,
            codex_protocol::SegmentId::new(),
            vec![
                segment_reference(predecessor.clone(), owner, predecessor_id),
                user_message("owner"),
            ],
        );
        let store = indexed_store(home.path()).await;
        let manifest = journal(home.path(), &source).await;
        let target = &manifest.targets[0];
        assert!(!target.selected);
        assert!(
            target.path.starts_with(
                home.path()
                    .join(codex_rollout::SESSIONS_SUBDIR)
                    .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
            )
        );
        let target_path = if compressed {
            target.path.with_extension("jsonl.zst")
        } else {
            target.path.clone()
        };
        fs::create_dir_all(target_path.parent().expect("target directory")).expect("directory");
        let damaged = b"{damaged target selected through another SQLite owner\n";
        let target_bytes = if compressed {
            zstd::stream::encode_all(&damaged[..], 0).expect("compressed damaged target")
        } else {
            damaged.to_vec()
        };
        fs::write(&target_path, &target_bytes).expect("existing target alias");
        let state = store.state_db().await.expect("state");
        let alias = codex_state::ThreadMetadataBuilder::new(
            alias_owner,
            target_path.clone(),
            chrono::Utc::now(),
            SessionSource::Cli,
        )
        .build(&store.config.default_model_provider_id);
        state
            .upsert_thread(&alias)
            .await
            .expect("selected target alias");
        let originals = manifest
            .sources
            .iter()
            .map(|input| {
                (
                    input.path.clone(),
                    fs::read(&input.path).expect("source bytes"),
                )
            })
            .collect::<Vec<_>>();
        let locks = store
            .acquire_migration_locks_for_test(owner)
            .await
            .expect("original migration writer");
        store
            .archive_thread(ArchiveThreadParams {
                thread_id: alias_owner,
            })
            .await
            .expect("only the different-ID target alias moves");
        let archived = home
            .path()
            .join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR)
            .join(target_path.file_name().expect("target filename"));
        let alias = state
            .get_thread(alias_owner)
            .await
            .expect("metadata")
            .expect("alias");
        assert!(alias.archived_at.is_some());
        assert_eq!(alias.rollout_path, archived);
        assert_eq!(
            fs::read(&archived).expect("archived target bytes"),
            target_bytes
        );
        let pending = migration_journal_path(home.path(), owner);
        assert!(matches!(
            write_lineage_migration_journal(&pending, &manifest, &locks).await,
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert!(!pending.exists());
        assert!(!target_path.exists());
        assert!(manifest.targets.iter().all(|target| !target.path.exists()));
        for (path, bytes) in originals {
            assert_eq!(fs::read(path).expect("unmoved source bytes"), bytes);
        }
        assert_eq!(
            fs::read(&archived).expect("retained archived target"),
            target_bytes
        );
    }
}

#[tokio::test]
async fn publication_contention_is_nonblocking_and_corrupt_journals_fail_closed() {
    let home = tempfile::tempdir().expect("home");
    let id = ThreadId::new();
    let source = write_rollout(
        home.path(),
        id,
        SessionSource::Cli,
        vec![user_message("owner")],
    );
    let store = indexed_store(home.path()).await;
    let manifest = journal(home.path(), &source).await;
    let locks = store
        .acquire_migration_locks_for_test(id)
        .await
        .expect("writer");
    let gate = try_acquire(home.path()).expect("archive owns publication boundary");
    let pending = migration_journal_path(home.path(), id);
    assert!(matches!(
        write_lineage_migration_journal(&pending, &manifest, &locks).await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(!pending.exists());
    drop(gate);
    drop(locks);
    fs::create_dir_all(pending.parent().expect("parent")).expect("directory");
    fs::write(&pending, b"{invalid journal").expect("corrupt interrupted journal");
    let before = fs::read(&source).expect("source");
    assert!(
        store
            .archive_thread(ArchiveThreadParams { thread_id: id })
            .await
            .is_err()
    );
    assert_eq!(fs::read(&source).expect("retained source"), before);
    assert_eq!(
        fs::read(&pending).expect("retained journal"),
        b"{invalid journal"
    );
}

#[tokio::test]
async fn cancelled_archive_retains_gate_and_writer_until_sqlite_commit() {
    let home = tempfile::tempdir().expect("home");
    let id = ThreadId::new();
    let source = write_rollout(
        home.path(),
        id,
        SessionSource::Cli,
        vec![user_message("owner")],
    );
    let store = indexed_store(home.path()).await;
    let original = fs::read(&source).expect("source");
    let pool = store
        .config
        .sqlite
        .open_read_write_pool(&store.config.sqlite.state_db_path())
        .await
        .expect("independent SQLite connection");
    let transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("hold SQLite commit boundary");
    let archive_store = store.clone();
    let request = tokio::spawn(async move {
        archive_store
            .archive_thread(ArchiveThreadParams { thread_id: id })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while source.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("archive reaches renamed but uncommitted state");
    request.abort();
    assert!(request.await.expect_err("cancelled caller").is_cancelled());
    assert!(matches!(
        try_acquire(home.path()),
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(matches!(
        store.acquire_writer_lock(id),
        Err(ThreadStoreError::Conflict { .. })
    ));
    transaction.commit().await.expect("release SQLite writer");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match try_acquire(home.path()) {
                Ok(guard) => {
                    drop(guard);
                    break;
                }
                Err(ThreadStoreError::Conflict { .. }) => tokio::task::yield_now().await,
                Err(error) => panic!("gate error: {error}"),
            }
        }
    })
    .await
    .expect("owned archive finishes after caller cancellation");
    let metadata = store
        .state_db()
        .await
        .expect("state")
        .get_thread(id)
        .await
        .expect("metadata")
        .expect("thread");
    assert!(metadata.archived_at.is_some());
    assert_eq!(
        fs::read(metadata.rollout_path).expect("archived bytes"),
        original
    );
    drop(
        store
            .acquire_writer_lock(id)
            .expect("writer released after commit"),
    );
}

#[test]
fn cancelled_journal_publication_retains_gate_through_physical_sync() {
    use std::future::Future;
    use std::task::Poll;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("isolated runtime");
    runtime.block_on(async {
        for ordinary in [false, true] {
            let home = tempfile::tempdir().expect("home");
            let id = ThreadId::new();
            let source = write_rollout(
                home.path(),
                id,
                SessionSource::Cli,
                vec![user_message("owner")],
            );
            let store = indexed_store(home.path()).await;
            let manifest = journal(home.path(), &source).await;
            let locks = store
                .acquire_migration_locks_for_test(id)
                .await
                .expect("writer");
            let gate = try_acquire(home.path()).expect("publication gate");
            validate_new_source_paths(&manifest)
                .await
                .expect("authenticate paths under gate");
            let pending = migration_journal_path(home.path(), id);
            let bytes = serde_json::to_vec(&manifest).expect("journal bytes");
            let (release, wait) = std::sync::mpsc::channel();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).expect("occupied blocking worker");
                wait.recv().expect("release blocking worker");
            });
            ready.await.expect("blocking worker occupied");
            let mut publication = Box::pin(async {
                if ordinary {
                    super::super::publish::write_migration_journal_guarded(
                        &pending,
                        &locks,
                        Arc::new(gate),
                    )
                    .await
                } else {
                    super::super::lineage_journal::publish_journal(
                        &pending,
                        bytes,
                        &locks,
                        Some(gate),
                    )
                    .await
                }
            });
            std::future::poll_fn(|cx| {
                assert!(publication.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(publication);
            drop(locks);
            assert!(!pending.exists());
            assert!(matches!(
                try_acquire(home.path()),
                Err(ThreadStoreError::Conflict { .. })
            ));
            assert!(matches!(
                store.acquire_writer_lock(id),
                Err(ThreadStoreError::Conflict { .. })
            ));
            release.send(()).expect("release physical publication");
            blocker.await.expect("blocking worker joined");
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if let Ok(gate) = try_acquire(home.path()) {
                        drop(gate);
                        if let Ok(writer) = store.acquire_writer_lock(id) {
                            drop(writer);
                            break;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("physical publication releases both leases");
            if ordinary {
                assert_eq!(
                    fs::read(pending).expect("published empty marker"),
                    Vec::<u8>::new()
                );
            } else {
                assert_eq!(
                    parse_lineage_migration_journal(&fs::read(pending).expect("published journal"))
                        .expect("typed journal"),
                    manifest
                );
            }
        }
    });
}

#[tokio::test]
async fn ordinary_marker_first_protects_a_damaged_sqlite_alias() {
    for relocated in [false, true] {
        let home = tempfile::tempdir().expect("home");
        let owner = ThreadId::new();
        let alias_owner = ThreadId::new();
        let selected = write_rollout(
            home.path(),
            owner,
            SessionSource::Cli,
            vec![user_message("owner")],
        );
        let store = indexed_store(home.path()).await;
        let source = if relocated {
            let directory = home.path().join("sessions/relocated");
            fs::create_dir_all(&directory).expect("relocated directory");
            let current = directory.join(selected.file_name().expect("selected filename"));
            fs::rename(&selected, &current).expect("move selected file without repairing SQLite");
            current
        } else {
            selected.clone()
        };
        let locks = store
            .acquire_migration_locks_for_test(owner)
            .await
            .expect("writer");
        assert!(
            store
                .begin_ordinary_migration(owner, &source, ThreadHistoryMode::Legacy, &locks)
                .await
                .is_ok()
        );
        drop(locks);
        let pending = migration_journal_path(home.path(), owner);
        assert_eq!(
            fs::read(&pending).expect("ordinary empty marker"),
            Vec::<u8>::new()
        );
        let damaged = b"{damaged selected alias\n";
        fs::write(&source, damaged).expect("damaged header");
        let state = store.state_db().await.expect("state");
        let alias = codex_state::ThreadMetadataBuilder::new(
            alias_owner,
            source.clone(),
            chrono::Utc::now(),
            SessionSource::Cli,
        )
        .build(&store.config.default_model_provider_id);
        state.upsert_thread(&alias).await.expect("selected alias");
        assert!(matches!(
            store
                .archive_thread(ArchiveThreadParams {
                    thread_id: alias_owner
                })
                .await,
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert_eq!(fs::read(&source).expect("unmoved alias"), damaged);
        assert_eq!(
            fs::read(&pending).expect("retained ordinary marker"),
            Vec::<u8>::new()
        );
        for (id, expected_path) in [(owner, &selected), (alias_owner, &source)] {
            let row = state
                .get_thread(id)
                .await
                .expect("metadata")
                .expect("owner");
            assert_eq!(&row.rollout_path, expected_path);
            assert!(row.archived_at.is_none());
        }
    }
}

#[tokio::test]
async fn ordinary_move_first_and_rejected_publications_leave_history_and_journals_unchanged() {
    let home = tempfile::tempdir().expect("home");
    let owner = ThreadId::new();
    let source = write_rollout(
        home.path(),
        owner,
        SessionSource::Cli,
        vec![user_message("owner")],
    );
    let store = indexed_store(home.path()).await;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("history");
    sqlx::query("INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, 42, 3)")
        .bind(owner.to_string()).execute(&pool).await.expect("retained history sentinel");
    let manifest = journal(home.path(), &source).await;
    let pending = migration_journal_path(home.path(), owner);
    let locks = store
        .acquire_migration_locks_for_test(owner)
        .await
        .expect("writer");
    let gate = try_acquire(home.path()).expect("archive boundary");
    let rejected = store
        .begin_ordinary_migration(owner, &source, ThreadHistoryMode::Legacy, &locks)
        .await;
    assert!(matches!(
        rejected,
        Err(super::super::RolloutMigrationFailure {
            error: ThreadStoreError::Conflict { .. },
            ..
        })
    ));
    assert!(!pending.exists());
    drop(gate);
    write_lineage_migration_journal(&pending, &manifest, &locks)
        .await
        .expect("existing typed journal");
    let bytes = fs::read(&pending).expect("typed journal bytes");
    let rejected = store
        .begin_ordinary_migration(owner, &source, ThreadHistoryMode::Legacy, &locks)
        .await;
    assert!(matches!(
        rejected,
        Err(super::super::RolloutMigrationFailure {
            error: ThreadStoreError::Conflict { .. },
            ..
        })
    ));
    assert_eq!(fs::read(&pending).expect("typed journal retained"), bytes);
    fs::remove_file(&pending).expect("test completes pending transaction before archive");
    let alias_owner = ThreadId::new();
    let damaged = b"{damaged selected alias\n";
    fs::write(&source, damaged).expect("damaged header");
    let state = store.state_db().await.expect("state");
    let alias = codex_state::ThreadMetadataBuilder::new(
        alias_owner,
        source.clone(),
        chrono::Utc::now(),
        SessionSource::Cli,
    )
    .build(&store.config.default_model_provider_id);
    state.upsert_thread(&alias).await.expect("selected alias");
    // The different SQLite owner can move this file while the migration writer is held.
    store
        .archive_thread(ArchiveThreadParams {
            thread_id: alias_owner,
        })
        .await
        .expect("alias archive wins despite original migration writer");
    let rejected = store
        .begin_ordinary_migration(owner, &source, ThreadHistoryMode::Legacy, &locks)
        .await;
    assert!(matches!(
        rejected,
        Err(super::super::RolloutMigrationFailure {
            error: ThreadStoreError::Conflict { .. },
            ..
        })
    ));
    assert!(!pending.exists());
    let retained = sqlx::query_as::<_, (i64, i64)>("SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?")
        .bind(owner.to_string()).fetch_one(&pool).await.expect("unchanged history projection");
    assert_eq!(retained, (42, 3));
    let owner_row = state
        .get_thread(owner)
        .await
        .expect("metadata")
        .expect("owner");
    assert_eq!(owner_row.rollout_path, source);
    assert!(owner_row.archived_at.is_none());
    assert!(!source.exists());
    let alias_row = state
        .get_thread(alias_owner)
        .await
        .expect("metadata")
        .expect("alias owner");
    assert!(alias_row.archived_at.is_some());
    assert_eq!(
        alias_row.rollout_path,
        home.path()
            .join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR)
            .join(source.file_name().expect("source filename"))
    );
    assert_eq!(
        fs::read(&alias_row.rollout_path).expect("archived alias"),
        damaged
    );
    assert!(manifest.targets.iter().all(|target| !target.path.exists()));
}
