use std::fs;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use codex_protocol::ThreadId;
use tempfile::TempDir;

use super::COORDINATION_LOCK_FILE;
use super::WRITER_LOCK_DIR;
use super::WriterLockCoordinator;
use super::WriterLockState;
use pretty_assertions::assert_eq;
use std::io::ErrorKind;

#[test]
fn writer_locks_reject_competing_owners_and_release_their_files() {
    let home = TempDir::new().expect("temp dir");
    let primary = Arc::new(WriterLockCoordinator::new(home.path()));
    let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::default();
    let other_thread_id = ThreadId::default();

    let owner = Arc::new(primary.acquire(thread_id).expect("acquire writer lock"));
    let lock_path = home
        .path()
        .join(WRITER_LOCK_DIR)
        .join(format!("{thread_id}.lock"));
    assert!(lock_path.exists());

    // Another process is opening a writer. Distinct threads can start together,
    // but even during this overlap the same thread must still have one owner.
    let opening = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(
            home.path()
                .join(WRITER_LOCK_DIR)
                .join(COORDINATION_LOCK_FILE),
        )
        .expect("open coordination");
    opening.lock_shared().expect("another writer is opening");
    let err = match secondary.acquire(thread_id) {
        Ok(_) => panic!("competing owner should fail"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::WouldBlock);
    let other_owner = secondary
        .acquire(other_thread_id)
        .expect("other thread should acquire its own lock");

    let next_owner = std::thread::scope(|scope| {
        let _opening = opening;
        let (closed, wait_for_close) = mpsc::channel();
        scope.spawn(move || {
            drop(owner);
            let _ = closed.send(());
        });
        wait_for_close
            .recv_timeout(Duration::from_secs(/*secs*/ 10))
            .expect("writer should close while another writer is opening");
        assert!(lock_path.exists());
        secondary
            .acquire(thread_id)
            .expect("released thread should accept another owner")
    });
    drop(next_owner);
    drop(other_owner);

    let entries = fs::read_dir(home.path().join(WRITER_LOCK_DIR))
        .expect("read lock directory")
        .map(|entry| entry.expect("lock directory entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, vec![COORDINATION_LOCK_FILE]);
}

#[test]
fn imported_reader_outlives_source_writer() {
    let home = TempDir::new().expect("temp dir");
    let primary = Arc::new(WriterLockCoordinator::new(home.path()));
    let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::default();
    let owner = primary.acquire(thread_id).expect("writer");
    let lock_path = home
        .path()
        .join(WRITER_LOCK_DIR)
        .join(format!("{thread_id}.lock"));
    owner.share_for_fork().expect("export");
    let reader = secondary.acquire_fork_reader(thread_id).expect("import");
    assert!(
        primary
            .try_acquire_for_publication(thread_id)
            .unwrap()
            .is_none()
    );
    drop(owner);
    assert!(lock_path.exists());
    assert_eq!(
        primary
            .acquire(thread_id)
            .expect_err("reader excludes writer")
            .kind(),
        ErrorKind::WouldBlock
    );
    drop(reader);
    assert!(!lock_path.exists());
    drop(primary.acquire(thread_id).expect("writer after reader"));
}

#[test]
fn fork_reader_release_restores_exclusive_deletion() {
    let home = TempDir::new().expect("temp dir");
    let primary = Arc::new(WriterLockCoordinator::new(home.path()));
    let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::default();
    let owner = Arc::new(primary.acquire(thread_id).expect("writer"));
    assert_eq!(
        secondary
            .acquire_fork_reader(thread_id)
            .expect_err("writer is exclusive")
            .kind(),
        ErrorKind::WouldBlock
    );
    let recorder_owner = Arc::clone(&owner);
    owner.share_for_fork().expect("export");
    let reader = secondary.acquire_fork_reader(thread_id).expect("import");
    assert_eq!(
        recorder_owner
            .require_exclusive()
            .expect_err("reader is active")
            .kind(),
        ErrorKind::WouldBlock
    );
    assert!(owner.is_reserved());
    drop(reader);
    recorder_owner
        .require_exclusive()
        .expect("delete after import finishes");
    assert_eq!(
        secondary
            .acquire_fork_reader(thread_id)
            .expect_err("writer is exclusive again")
            .kind(),
        ErrorKind::WouldBlock
    );
}

#[test]
fn final_guard_drop_unlocks_inherited_descriptors() {
    for shared in [false, true] {
        let home = TempDir::new().expect("temp dir");
        let primary = Arc::new(WriterLockCoordinator::new(home.path()));
        let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
        let thread_id = ThreadId::default();
        let owner = Arc::new(primary.acquire(thread_id).expect("writer"));
        if shared {
            owner.share_for_fork().expect("export");
        }
        let inherited = match &*owner.state.lock().expect("writer state") {
            WriterLockState::Exclusive(file) | WriterLockState::Shared(file) => {
                file.try_clone().expect("inherited descriptor")
            }
            WriterLockState::Unreserved => panic!("writer must own a reservation"),
        };
        let recorder_owner = Arc::clone(&owner);
        drop(owner);
        assert_eq!(
            secondary
                .acquire(thread_id)
                .expect_err("recorder still owns writer")
                .kind(),
            ErrorKind::WouldBlock
        );
        drop(recorder_owner);
        let next = secondary
            .acquire(thread_id)
            .expect("released despite inherited descriptor");
        drop(inherited);
        drop(next);
    }
}

#[test]
fn first_acquisition_removes_stale_locks_without_removing_active_locks() {
    let home = TempDir::new().expect("temp dir");
    let primary = Arc::new(WriterLockCoordinator::new(home.path()));
    let active_thread_id = ThreadId::default();
    let active_owner = primary
        .acquire(active_thread_id)
        .expect("acquire active writer lock");

    let stale_thread_id = ThreadId::default();
    let stale_path = home
        .path()
        .join(WRITER_LOCK_DIR)
        .join(format!("{stale_thread_id}.lock"));
    fs::File::create(&stale_path).expect("create stale writer lock");

    let secondary = Arc::new(WriterLockCoordinator::new(home.path()));
    let secondary_owner = secondary
        .acquire(ThreadId::default())
        .expect("acquire writer lock after cleanup");

    assert!(!stale_path.exists());
    let err = match secondary.acquire(active_thread_id) {
        Ok(_) => panic!("active writer should survive cleanup"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::WouldBlock);

    drop(secondary_owner);
    drop(active_owner);
}

#[test]
fn publication_skips_live_writers_and_keeps_coordination_locked() {
    let home = TempDir::new().expect("temp dir");
    let publisher = Arc::new(WriterLockCoordinator::new(home.path()));
    let writer = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::default();
    let owner = writer.acquire(thread_id).expect("writer owns thread");
    assert!(
        publisher
            .try_acquire_for_publication(thread_id)
            .unwrap()
            .is_none()
    );
    drop(owner);

    let publication = publisher
        .try_acquire_for_publication(thread_id)
        .unwrap()
        .unwrap();
    let coordination = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(writer.directory.join(COORDINATION_LOCK_FILE))
        .unwrap();
    assert!(matches!(
        coordination.try_lock(),
        Err(fs::TryLockError::WouldBlock)
    ));
    let error = writer
        .acquire(ThreadId::default())
        .err()
        .expect("coordination contention must return an error instead of hanging");
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    drop(publication);
    coordination
        .try_lock()
        .expect("publication releases coordination");
    drop(coordination);
    // An existing but unlocked file is also idle; file existence is not ownership.
    fs::File::create(writer.directory.join(format!("{thread_id}.lock"))).unwrap();
    drop(
        publisher
            .try_acquire_for_publication(thread_id)
            .unwrap()
            .expect("stale lock is idle"),
    );
    assert!(writer.acquire(thread_id).is_ok());
}

#[test]
fn writer_release_preserves_independently_acquired_fork_reader() {
    let home = TempDir::new().expect("temp dir");
    let coordinator = Arc::new(WriterLockCoordinator::new(home.path()));
    let other = Arc::new(WriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::new();
    let writer = Arc::new(coordinator.acquire(thread_id).expect("writer"));
    writer.share_for_fork().expect("export");
    let reader = other.acquire_fork_reader(thread_id).expect("fork reader");
    // dup and fork share the writer's open file description, unlike the separate reader.
    let inherited = match &*writer.state.lock().expect("writer state") {
        WriterLockState::Shared(file) => file.try_clone().expect("inherited descriptor"),
        _ => panic!("expected shared writer"),
    };
    drop(writer);
    assert_eq!(
        coordinator
            .acquire(thread_id)
            .expect_err("the independent reader must still exclude archive")
            .kind(),
        ErrorKind::WouldBlock
    );
    drop(reader);
    let next_writer = coordinator
        .acquire(thread_id)
        .expect("only independent reservations may retain ownership");
    drop(inherited);
    drop(next_writer);
}
