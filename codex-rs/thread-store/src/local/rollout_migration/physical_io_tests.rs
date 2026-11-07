use std::time::Duration;

use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;

use super::super::LocalThreadStore;
use super::super::lineage::plan_legacy_lineage;
use super::super::lineage_journal::LineageMigrationJournal;
use super::super::lineage_journal::write_lineage_migration_journal;
use super::super::publish;
use super::super::tests::indexed_store;
use super::super::tests::user_message;
use super::super::tests::write_rollout;
use super::*;
use crate::ThreadStoreError;

async fn reserve_migration(
    store: &LocalThreadStore,
    home: &Path,
    selected: ThreadId,
    ancestor: ThreadId,
) -> Arc<MigrationLocks> {
    let locks = Arc::new(MigrationLocks {
        _maintenance: Some(Arc::new(
            codex_rollout::try_acquire_rollout_migration_dependency_lock(
                home,
                &[selected, ancestor],
            )
            .expect("reserve dependencies and capacity")
            .expect("available migration job"),
        )),
        _ancestors: std::sync::OnceLock::new(),
        _writer: store
            .acquire_writer_lock(selected)
            .expect("selected writer"),
        _live_writer: store.live_writer_locks.lock(selected).await,
    });
    assert!(
        locks
            ._ancestors
            .set(
                store
                    .try_reserve_rollout_writers(&[ancestor])
                    .await
                    .expect("ancestor writer")
            )
            .is_ok()
    );
    locks
}

fn fill_remaining_capacity(home: &Path) -> Vec<codex_rollout::RolloutMaintenanceJobGuard> {
    (1..codex_rollout::MAX_CONCURRENT_ROLLOUT_MIGRATIONS)
        .map(|_| {
            codex_rollout::try_acquire_rollout_migration_dependency_lock(home, &[ThreadId::new()])
                .expect("reserve independent capacity")
                .expect("remaining capacity")
        })
        .collect()
}

async fn assert_reserved(
    store: &LocalThreadStore,
    home: &Path,
    selected: ThreadId,
    ancestor: ThreadId,
) {
    for id in [selected, ancestor] {
        assert!(matches!(
            store.acquire_writer_lock(id),
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert!(matches!(
            store.live_writer_locks.try_lock(id).await,
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert!(
            codex_rollout::try_acquire_rollout_migration_dependency_lock(home, &[id])
                .expect("probe owned dependency")
                .is_none()
        );
    }
    assert!(
        codex_rollout::try_acquire_rollout_migration_dependency_lock(home, &[ThreadId::new()])
            .expect("probe occupied capacity")
            .is_none()
    );
    assert!(
        codex_rollout::try_acquire_rollout_maintenance_lock(home)
            .expect("probe exclusive maintenance")
            .is_none()
    );
}

async fn assert_released(
    store: &LocalThreadStore,
    home: &Path,
    selected: ThreadId,
    ancestor: ThreadId,
) {
    let live = tokio::time::timeout(
        Duration::from_secs(5),
        store.live_writer_locks.lock(selected),
    )
    .await
    .expect("physical worker released its final local writer");
    drop(live);
    for id in [selected, ancestor] {
        store
            .acquire_writer_lock(id)
            .expect("cross-process writer released");
        assert!(store.live_writer_locks.try_lock(id).await.is_ok());
        assert!(
            codex_rollout::try_acquire_rollout_migration_dependency_lock(home, &[id])
                .expect("probe released dependency and capacity")
                .is_some()
        );
    }
}

async fn occupy_blocking_worker() -> (std::sync::mpsc::Sender<()>, JoinHandle<()>) {
    let (release, wait) = std::sync::mpsc::channel();
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker = tokio::task::spawn_blocking(move || {
        started.send(()).expect("report occupied worker");
        let _ = wait.recv();
    });
    ready.await.expect("blocking worker started");
    (release, worker)
}

#[test]
fn cancelled_publication_workers_retain_migration_ownership() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("isolated runtime");
    runtime.block_on(async {
        for operation in [
            "rename",
            "link",
            "remove",
            "remove_directory",
            "marker",
            "journal",
            "sync",
            "open",
            "decompress",
        ] {
            let home = tempfile::tempdir().expect("Codex home");
            let selected = ThreadId::new();
            let ancestor = ThreadId::new();
            let rollout = write_rollout(
                home.path(),
                selected,
                SessionSource::Cli,
                vec![user_message("source")],
            );
            let store = indexed_store(home.path()).await;
            let journal = LineageMigrationJournal::from_plan(
                &plan_legacy_lineage(home.path(), &rollout)
                    .await
                    .expect("journal plan"),
            );
            let source = home.path().join("source");
            let target = home.path().join("target");
            let directory = home.path().join("remove-directory");
            let marker = publish::migration_journal_path(home.path(), selected);
            let source_bytes = if operation == "decompress" {
                zstd::stream::encode_all(b"physical bytes".as_slice(), 3)
                    .expect("compressed source")
            } else {
                b"physical bytes".to_vec()
            };
            std::fs::write(&source, &source_bytes).expect("source bytes");
            let permissions = std::fs::metadata(&source)
                .expect("source permissions")
                .permissions();
            std::fs::create_dir(&directory).expect("removal directory");
            let locks = reserve_migration(&store, home.path(), selected, ancestor).await;
            let _capacity = fill_remaining_capacity(home.path());
            let (release, blocker) = occupy_blocking_worker().await;
            let mut work = Box::pin(async {
                match operation {
                    "rename" => rename(&source, &target, &locks).await,
                    "link" => hard_link(&source, &target, &locks).await,
                    "remove" => remove_file(&source, &locks).await,
                    "remove_directory" => remove_dir_all(&directory, &locks).await,
                    "marker" => publish::write_migration_journal(&marker, &locks)
                        .await
                        .map_err(io::Error::other),
                    "journal" => write_lineage_migration_journal(&marker, &journal, &locks)
                        .await
                        .map_err(io::Error::other),
                    "sync" => publish::sync_parent_directory(&source, &locks)
                        .await
                        .map_err(io::Error::other),
                    "open" => OutputFile::create(
                        &target,
                        permissions,
                        &IoOwner::Migration(Arc::clone(&locks)),
                    )
                    .await
                    .map(|_| ()),
                    "decompress" => publish::decompress_rollout_to_path(&source, &target, &locks)
                        .await
                        .map_err(io::Error::other),
                    _ => unreachable!(),
                }
            });
            std::future::poll_fn(|cx| {
                assert!(
                    work.as_mut().poll(cx).is_pending(),
                    "{operation}: real worker must be queued"
                );
                Poll::Ready(())
            })
            .await;
            drop(work);
            drop(locks);
            assert_reserved(&store, home.path(), selected, ancestor).await;
            assert!(source.exists());
            assert!(!target.exists());
            assert!(!marker.exists());
            assert!(directory.exists());
            release.send(()).expect("release blocking worker");
            blocker.await.expect("join blocker");
            assert_released(&store, home.path(), selected, ancestor).await;
            match operation {
                "rename" | "link" => assert_eq!(
                    std::fs::read(&target).expect("published bytes"),
                    b"physical bytes"
                ),
                "remove" => assert!(!source.exists()),
                "remove_directory" => assert!(!directory.exists()),
                "marker" => assert_eq!(std::fs::read(&marker).expect("published marker"), b""),
                "journal" => assert_eq!(
                    serde_json::from_slice::<LineageMigrationJournal>(
                        &std::fs::read(&marker).expect("published journal")
                    )
                    .expect("journal JSON"),
                    journal
                ),
                "sync" => assert_eq!(
                    std::fs::read(&source).expect("synchronized source"),
                    b"physical bytes"
                ),
                "open" => assert_eq!(std::fs::read(&target).expect("created output"), b""),
                "decompress" => {
                    assert_eq!(
                        std::fs::read(&target).expect("decompressed output"),
                        b"physical bytes"
                    );
                    assert_eq!(
                        std::fs::read(&source).expect("unchanged compressed source"),
                        source_bytes
                    );
                    #[cfg(unix)]
                    assert_eq!(
                        std::fs::metadata(&target)
                            .expect("output permissions")
                            .permissions()
                            .mode()
                            & 0o777,
                        0o600
                    );
                }
                _ => unreachable!(),
            }
        }
    });
}

#[test]
fn cancelled_staging_write_is_bounded_and_retains_migration_ownership() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("isolated runtime");
    runtime.block_on(async {
        let home = tempfile::tempdir().expect("Codex home");
        let selected = ThreadId::new();
        let ancestor = ThreadId::new();
        let store = indexed_store(home.path()).await;
        let locks = reserve_migration(&store, home.path(), selected, ancestor).await;
        let _capacity = fill_remaining_capacity(home.path());
        let owner = IoOwner::Migration(Arc::clone(&locks));
        let path = home.path().join("staged.jsonl");
        std::fs::write(&path, b"").expect("staged fixture");
        let permissions = std::fs::metadata(&path).expect("permissions").permissions();
        let mut output = OutputFile::create(&path, permissions, &owner)
            .await
            .expect("guarded output");
        let bytes = vec![b'x'; 2 * super::super::PROJECTION_BATCH_BYTES as usize];
        let (release, blocker) = occupy_blocking_worker().await;
        let mut write = Box::pin(output.write_all(&bytes));
        std::future::poll_fn(|cx| {
            assert!(
                write.as_mut().poll(cx).is_pending(),
                "a second batch must wait for the first physical write"
            );
            Poll::Ready(())
        })
        .await;
        drop(write);
        let worker = output
            .pending
            .take()
            .expect("one actual queued write")
            .worker;
        drop(output);
        drop(owner);
        drop(locks);
        assert_reserved(&store, home.path(), selected, ancestor).await;
        assert_eq!(std::fs::metadata(&path).expect("unwritten output").len(), 0);
        release.send(()).expect("release blocking worker");
        blocker.await.expect("join blocker");
        worker
            .await
            .expect("join physical write")
            .expect("physical write");
        assert_released(&store, home.path(), selected, ancestor).await;
        assert_eq!(
            std::fs::read(path).expect("one physical batch"),
            bytes[..super::super::PROJECTION_BATCH_BYTES as usize]
        );
    });
}

#[tokio::test]
async fn staging_output_propagates_physical_write_and_sync_errors() {
    let home = Arc::new(tempfile::tempdir().expect("scratch directory"));
    let path = home.path().join("read-only-handle");
    std::fs::write(&path, b"unchanged").expect("source bytes");
    let expected = std::fs::File::open(&path)
        .expect("read-only handle")
        .write_all(b"replacement")
        .expect_err("read-only handles cannot write");
    let mut output = OutputFile {
        file: Arc::new(Mutex::new(
            std::fs::File::open(&path).expect("read-only handle"),
        )),
        owner: IoOwner::Scratch(Arc::clone(&home)),
        pending: None,
    };
    output
        .write_all(b"replacement")
        .await
        .expect("buffer owns pending bytes");
    let error = output
        .flush()
        .await
        .expect_err("flush must join the failed physical write");
    assert_eq!(
        (error.kind(), error.raw_os_error()),
        (expected.kind(), expected.raw_os_error())
    );
    assert_eq!(
        std::fs::read(path).expect("source remains intact"),
        b"unchanged"
    );

    let owner = IoOwner::Scratch(Arc::clone(&home));
    let source = home.path().join("corrupt.zst");
    let target = home.path().join("decoded.jsonl");
    let mut source_bytes =
        zstd::stream::encode_all(b"decoded bytes".as_slice(), 3).expect("compressed prefix");
    source_bytes.extend_from_slice(b"invalid next frame");
    std::fs::write(&source, &source_bytes).expect("corrupt source");
    let mut decoder =
        zstd::stream::read::Decoder::new(std::fs::File::open(&source).expect("compressed source"))
            .expect("decoder");
    let expected = io::copy(&mut decoder, &mut io::sink()).expect_err("invalid next frame");
    let error = owner
        .decompress_rollout_to_path(&source, &target)
        .await
        .expect_err("physical decompression failure must propagate");
    assert_eq!(error.kind(), expected.kind());
    assert_eq!(error.to_string(), expected.to_string());
    assert_eq!(
        std::fs::read(&source).expect("unchanged compressed source"),
        source_bytes
    );

    let directory = home.path().join("not-an-output-file");
    std::fs::create_dir(&directory).expect("invalid output fixture");
    let expected = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&directory)
        .expect_err("a directory cannot be opened for writing");
    let error = owner
        .decompress_rollout_to_path(&source, &directory)
        .await
        .expect_err("physical output-open failure must propagate");
    assert_eq!(
        (error.kind(), error.raw_os_error()),
        (expected.kind(), expected.raw_os_error())
    );

    #[cfg(unix)]
    {
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().expect("non-syncable handle");
        let file = std::fs::File::from(std::os::fd::OwnedFd::from(socket));
        let expected = file.sync_all().expect_err("sockets cannot be fsynced");
        let mut output = OutputFile {
            file: Arc::new(Mutex::new(file)),
            owner: IoOwner::Scratch(Arc::clone(&home)),
            pending: None,
        };
        let error = output
            .sync_all()
            .await
            .expect_err("physical fsync failure must propagate");
        assert_eq!(
            (error.kind(), error.raw_os_error()),
            (expected.kind(), expected.raw_os_error())
        );
    }
}

#[test]
fn scratch_output_confines_paths_and_retains_directory_until_worker_completion() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("isolated runtime");
    runtime.block_on(async {
        let home = Arc::new(tempfile::tempdir().expect("scratch directory"));
        let outside = tempfile::tempdir().expect("outside directory");
        let owner = IoOwner::Scratch(Arc::clone(&home));
        let path = home.path().join("output");
        let source = outside.path().join("source.zst");
        let source_bytes = zstd::stream::encode_all(b"pending scratch bytes".as_slice(), 3)
            .expect("compressed source");
        std::fs::write(&source, &source_bytes).expect("source fixture");
        std::fs::write(&path, b"").expect("output fixture");
        let permissions = std::fs::metadata(&path).expect("permissions").permissions();
        for rejected in [
            outside.path().join("output"),
            home.path().join("nested/../escaped"),
        ] {
            assert!(
                OutputFile::create(&rejected, permissions.clone(), &owner)
                    .await
                    .is_err()
            );
            assert!(
                owner
                    .decompress_rollout_to_path(&source, &rejected)
                    .await
                    .is_err()
            );
            assert!(!rejected.exists());
        }
        assert!(
            owner
                .rename(&path, &outside.path().join("renamed"))
                .await
                .is_err()
        );
        assert!(owner.sync_parent_directory(home.path()).await.is_err());
        #[cfg(unix)]
        {
            let link = home.path().join("outside-link");
            std::os::unix::fs::symlink(outside.path(), &link).expect("escape link");
            assert!(
                OutputFile::create(&link.join("output"), permissions.clone(), &owner)
                    .await
                    .is_err()
            );
            assert!(
                owner
                    .decompress_rollout_to_path(&source, &link.join("output"))
                    .await
                    .is_err()
            );
            assert!(!outside.path().join("output").exists());
        }
        let mut output = OutputFile::create(&path, permissions, &owner)
            .await
            .expect("confined output");
        let root = home.path().to_path_buf();
        let weak = Arc::downgrade(&home);
        let (release, blocker) = occupy_blocking_worker().await;
        output
            .write_all(b"pending scratch bytes")
            .await
            .expect("buffer owns pending bytes");
        let worker = output.pending.take().expect("actual queued write").worker;
        drop(output);
        drop(owner);
        drop(home);
        assert!(weak.upgrade().is_some());
        assert!(
            root.exists(),
            "scratch cleanup must wait for physical output"
        );
        release.send(()).expect("release blocking worker");
        blocker.await.expect("join blocker");
        worker
            .await
            .expect("join physical write")
            .expect("scratch write");
        assert!(weak.upgrade().is_none());
        assert!(!root.exists());

        let home = Arc::new(tempfile::tempdir().expect("decompression scratch directory"));
        let owner = IoOwner::Scratch(Arc::clone(&home));
        let path = home.path().join("output");
        owner
            .decompress_rollout_to_path(&source, &path)
            .await
            .expect("confined decompression");
        assert_eq!(
            std::fs::read(&path).expect("decoded scratch bytes"),
            b"pending scratch bytes"
        );
        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(&path)
                .expect("output permissions")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        std::fs::remove_file(&path).expect("reset decompression output");
        let root = home.path().to_path_buf();
        let weak = Arc::downgrade(&home);
        let (release, blocker) = occupy_blocking_worker().await;
        let mut work = Box::pin(owner.decompress_rollout_to_path(&source, &path));
        std::future::poll_fn(|cx| {
            assert!(work.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(work);
        drop(owner);
        drop(home);
        assert!(weak.upgrade().is_some());
        assert!(root.exists(), "scratch cleanup must wait for decompression");
        assert!(!path.exists());
        release.send(()).expect("release blocking worker");
        blocker.await.expect("join blocker");
        tokio::task::spawn_blocking(|| ())
            .await
            .expect("drain the single blocking worker queue");
        assert!(weak.upgrade().is_none());
        assert!(!root.exists());
        assert_eq!(
            std::fs::read(&source).expect("unchanged compressed source"),
            source_bytes
        );
    });
}
