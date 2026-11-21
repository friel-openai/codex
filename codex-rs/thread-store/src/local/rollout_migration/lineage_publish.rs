//! Publishes and verifies staged lineage targets.

use std::io;
use std::io::Read;
use std::io::Seek;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use sha2::Digest;
use sha2::Sha256;

use super::MigrationLocks;
use super::lineage::hash_file;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationJournalTarget;
use super::lineage_journal::write_lineage_migration_journal;
use super::migration_error;
use super::physical_io;
use super::physical_io::IoOwner;
use super::publish::compress_rollout_to_path;
use super::publish::sync_parent_directory;
use crate::ThreadStoreResult;

pub(super) async fn publish_lineage_targets(
    journal_path: &Path,
    journal: &mut LineageMigrationJournal,
    locks: &Arc<MigrationLocks>,
) -> ThreadStoreResult<()> {
    journal.validate_review_input_target()?;
    let codex_home = journal_path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| migration_error("lineage migration journal has no Codex home"))?;
    if let Some(target) = journal
        .review_input_target
        .as_mut()
        .filter(|target| target.staged_path.is_some())
    {
        publish_one_target(target, codex_home, /*review_input*/ true, locks).await?;
        write_lineage_migration_journal(journal_path, journal, locks).await?;
    }
    for index in 0..journal.targets.len() {
        publish_one_target(
            &mut journal.targets[index],
            codex_home,
            /*review_input*/ false,
            locks,
        )
        .await?;
        write_lineage_migration_journal(journal_path, journal, locks).await?;
    }
    Ok(())
}

pub(super) async fn verify_published_lineage_targets(
    journal: &LineageMigrationJournal,
) -> ThreadStoreResult<()> {
    journal.validate_review_input_target()?;
    for target in journal.targets.iter().chain(
        journal
            .review_input_target
            .iter()
            .filter(|target| target.staged_path.is_some()),
    ) {
        verify_published_target(target).await?;
    }
    Ok(())
}

async fn publish_one_target(
    target: &mut LineageMigrationJournalTarget,
    codex_home: &Path,
    review_input: bool,
    locks: &Arc<MigrationLocks>,
) -> ThreadStoreResult<()> {
    if review_input
        && target.path != codex_rollout::review_input_segment_path(codex_home, target.rollout_id)
    {
        return Err(migration_error(
            "review-input target is outside its Codex home",
        ));
    }
    if tokio::fs::try_exists(target.path.as_path())
        .await
        .map_err(migration_error)?
    {
        let sha256 = verify_published_target(target).await?;
        // The file can be visible before its directory entries have been synced and recorded.
        if review_input || target.published_sha256.is_none() {
            sync_publication_directories(&target.path, codex_home, locks).await?;
        }
        target.published_sha256 = Some(sha256);
        return Ok(());
    }
    let staged_path = target
        .staged_path
        .as_ref()
        .ok_or_else(|| migration_error("lineage migration target has no staged path"))?;
    let parent = target
        .path
        .parent()
        .ok_or_else(|| migration_error("lineage migration target has no parent"))?;
    IoOwner::Migration(Arc::clone(locks))
        .create_dir_all(parent)
        .await
        .map_err(migration_error)?;
    let compressed = target
        .path
        .extension()
        .is_some_and(|extension| extension == "zst");
    if review_input {
        // Other checkpoints may already reference the content-derived destination. Never replace
        // it or delete it during rollback; only the transaction's staged link is temporary.
        match physical_io::hard_link(staged_path, &target.path, locks).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                verify_published_target(target).await?;
            }
            Err(error) => return Err(migration_error(error)),
        }
        sync_publication_directories(&target.path, codex_home, locks).await?;
        target.published_sha256 = Some(verify_published_target(target).await?);
    } else {
        let temporary;
        let source = if compressed {
            temporary = staged_path.with_extension("publish.zst.tmp");
            let permissions = tokio::fs::metadata(staged_path)
                .await
                .map_err(migration_error)?
                .permissions();
            compress_rollout_to_path(
                staged_path,
                &temporary,
                permissions,
                /*modified_at*/ None,
                locks,
            )
            .await?;
            temporary.as_path()
        } else {
            staged_path.as_path()
        };
        let rename = physical_io::rename(source, &target.path, locks).await;
        target.published_sha256 =
            Some(finish_rollout_publication(rename, source, target, codex_home, locks).await?);
    }
    Ok(())
}

async fn finish_rollout_publication(
    rename: io::Result<()>,
    source: &Path,
    target: &LineageMigrationJournalTarget,
    codex_home: &Path,
    locks: &Arc<MigrationLocks>,
) -> ThreadStoreResult<String> {
    match rename {
        Ok(()) => sync_parent_directory(&target.path, locks).await?,
        Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {
            copy_rollout_for_publication(source, target, codex_home, locks).await?;
        }
        Err(error) => return Err(migration_error(error)),
    }
    verify_published_target(target).await
}

async fn copy_rollout_for_publication(
    source: &Path,
    target: &LineageMigrationJournalTarget,
    codex_home: &Path,
    locks: &Arc<MigrationLocks>,
) -> ThreadStoreResult<()> {
    let directory_entries = publication_directory_entries(&target.path, codex_home)?;
    let expected_plain_sha = target
        .sha256
        .clone()
        .ok_or_else(|| migration_error("lineage migration target has no staged SHA-256"))?;
    let expected_published_sha = target.published_sha256.clone();
    let source = source.to_path_buf();
    let destination = target.path.clone();
    physical_io::run(locks, move || {
        let parent = destination
            .parent()
            .ok_or_else(|| io::Error::other("lineage migration target has no parent"))?;
        let mut input = std::fs::File::open(source)?;
        // A second bind mount can reject rename even on the same filesystem. Keep staging
        // intact for recovery and publish only a verified file created on the destination mount.
        let mut temporary = tempfile::Builder::new()
            .prefix(".rollout-migration-publish-")
            .tempfile_in(parent)?;
        let output = temporary.as_file_mut();
        io::copy(&mut input, output)?;
        output.rewind()?;
        let plain_sha = if destination.extension().is_some_and(|ext| ext == "zst") {
            hash_reader(zstd::stream::read::Decoder::new(&mut *output)?)?
        } else {
            hash_reader(&mut *output)?
        };
        let published_sha_matches = if let Some(expected) = expected_published_sha {
            output.rewind()?;
            hash_reader(&mut *output)? == expected
        } else {
            true
        };
        if plain_sha != expected_plain_sha || !published_sha_matches {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "copied lineage migration target failed verification: {}",
                    destination.display()
                ),
            ));
        }
        output.set_permissions(input.metadata()?.permissions())?;
        output.sync_all()?;
        match temporary.persist_noclobber(destination) {
            Ok(_) => {}
            // Verify the existing file against the journal after returning from the copy.
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.error),
        }
        sync_publication_directories_blocking(&directory_entries)
    })
    .await
    .map_err(migration_error)
}

async fn sync_publication_directories(
    path: &Path,
    codex_home: &Path,
    locks: &Arc<MigrationLocks>,
) -> ThreadStoreResult<()> {
    let directory_entries = publication_directory_entries(path, codex_home)?;
    physical_io::run(locks, move || {
        sync_publication_directories_blocking(&directory_entries)
    })
    .await
    .map_err(migration_error)
}

fn sync_publication_directories_blocking(directory_entries: &[PathBuf]) -> io::Result<()> {
    // Sync newly created containing directories before SQLite can select the published rollout.
    for entry in directory_entries {
        physical_io::sync_parent_directory_blocking(entry)?;
    }
    Ok(())
}

fn publication_directory_entries(
    path: &Path,
    codex_home: &Path,
) -> ThreadStoreResult<Vec<PathBuf>> {
    if !path.starts_with(codex_home) || path == codex_home {
        return Err(migration_error(
            "lineage migration target has no Codex-home ancestor",
        ));
    }
    let mut entries = Vec::new();
    let mut entry = path;
    loop {
        entries.push(entry.to_path_buf());
        let parent = entry.parent().ok_or_else(|| {
            migration_error("lineage migration target has no Codex-home ancestor")
        })?;
        if parent == codex_home {
            return Ok(entries);
        }
        entry = parent;
    }
}

async fn verify_published_target(
    target: &LineageMigrationJournalTarget,
) -> ThreadStoreResult<String> {
    if !tokio::fs::symlink_metadata(&target.path)
        .await
        .map_err(migration_error)?
        .file_type()
        .is_file()
    {
        return Err(migration_error(
            "published lineage target is not a regular file",
        ));
    }
    let expected_plain_sha = target
        .sha256
        .as_deref()
        .ok_or_else(|| migration_error("lineage migration target has no staged SHA-256"))?;
    let compressed = target
        .path
        .extension()
        .is_some_and(|extension| extension == "zst");
    let plain_sha = if compressed {
        hash_decompressed(target.path.as_path()).await?
    } else {
        hash_file(target.path.as_path()).await?.1
    };
    if plain_sha != expected_plain_sha {
        return Err(migration_error(format!(
            "published lineage migration target failed verification: {}",
            target.path.display()
        )));
    }
    let published_sha = hash_file(target.path.as_path()).await?.1;
    if target
        .published_sha256
        .as_deref()
        .is_some_and(|expected| expected != published_sha)
    {
        return Err(migration_error(format!(
            "published lineage migration target changed: {}",
            target.path.display()
        )));
    }
    Ok(published_sha)
}

async fn hash_decompressed(path: &Path) -> ThreadStoreResult<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let input = std::fs::File::open(path)?;
        hash_reader(zstd::stream::read::Decoder::new(input)?)
    })
    .await
    .map_err(migration_error)?
    .map_err(migration_error)
}

fn hash_reader(mut input: impl Read) -> io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 256 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::ThreadId;

    use crate::LocalThreadStore;
    use crate::local::test_support::test_config;

    const SYNTHETIC_ROLLOUT: &[u8] = b"{\"synthetic\":true}\n";

    async fn staged_predecessor(
        home: &Path,
        compressed: bool,
    ) -> (LineageMigrationJournalTarget, Arc<MigrationLocks>) {
        let thread_id = ThreadId::new();
        let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
        let stage = home
            .join("rollout-migrations")
            .join(format!("{thread_id}.staging"));
        let destination = home.join("sessions/rollout_segments/2025/01/03");
        std::fs::create_dir_all(&stage).expect("create staging directory");
        std::fs::create_dir_all(&destination).expect("create destination directory");
        let (path, staged_path, bytes) = if compressed {
            (
                destination.join(format!("{filename}.zst")),
                stage.join("00000000.publish.zst.tmp"),
                zstd::stream::encode_all(SYNTHETIC_ROLLOUT, 3).expect("compress fixture"),
            )
        } else {
            (
                destination.join(filename),
                stage.join("00000000.jsonl"),
                SYNTHETIC_ROLLOUT.to_vec(),
            )
        };
        std::fs::write(&staged_path, bytes).expect("write staged rollout");
        let store = LocalThreadStore::new(test_config(home), None);
        let locks = store
            .acquire_migration_locks_for_test(thread_id)
            .await
            .expect("reserve migration writer");
        (
            LineageMigrationJournalTarget {
                thread_id,
                rollout_id: ThreadId::new(),
                segment_id: None,
                path,
                selected: false,
                predecessor_segment_id: None,
                staged_path: Some(staged_path),
                start_ordinal: None,
                end_ordinal_exclusive: None,
                byte_count: None,
                record_count: None,
                sha256: Some(format!("{:x}", Sha256::digest(SYNTHETIC_ROLLOUT))),
                published_sha256: None,
            },
            locks,
        )
    }

    fn cross_device_rename() -> io::Result<()> {
        Err(io::ErrorKind::CrossesDevices.into())
    }

    #[tokio::test]
    async fn cross_device_rename_publishes_plain_and_compressed_lineage_without_consuming_stage() {
        for compressed in [false, true] {
            let home = tempfile::tempdir().expect("Codex home");
            let (target, locks) = staged_predecessor(home.path(), compressed).await;
            let source = target.staged_path.as_deref().expect("staged rollout path");
            let encoded = std::fs::read(source).expect("read staged rollout");
            let published_sha = finish_rollout_publication(
                cross_device_rename(),
                source,
                &target,
                home.path(),
                &locks,
            )
            .await
            .expect("publish across mount boundary");
            assert_eq!(published_sha, format!("{:x}", Sha256::digest(&encoded)));
            assert_eq!(
                std::fs::read(&target.path).expect("read published file"),
                encoded
            );
            assert_eq!(
                std::fs::read(source).expect("retain staged rollout"),
                encoded
            );

            let directories = publication_directory_entries(&target.path, home.path())
                .expect("bounded directory entries")
                .into_iter()
                .map(|entry| entry.parent().expect("containing directory").to_path_buf())
                .collect::<Vec<_>>();
            assert_eq!(
                directories,
                [
                    home.path().join("sessions/rollout_segments/2025/01/03"),
                    home.path().join("sessions/rollout_segments/2025/01"),
                    home.path().join("sessions/rollout_segments/2025"),
                    home.path().join("sessions/rollout_segments"),
                    home.path().join("sessions"),
                    home.path().to_path_buf(),
                ]
            );
            let entries = std::fs::read_dir(&directories[0])
                .expect("read destination directory")
                .collect::<io::Result<Vec<_>>>()
                .expect("read destination entries");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].path(), target.path);
        }
    }

    #[tokio::test]
    async fn cross_device_publication_rejects_corruption_and_does_not_clobber() {
        let home = tempfile::tempdir().expect("Codex home");
        let (mut target, locks) = staged_predecessor(home.path(), /*compressed*/ false).await;
        let source = target.staged_path.clone().expect("staged rollout path");
        for (bytes, published_sha256) in [
            (b"changed staging".as_slice(), None),
            (SYNTHETIC_ROLLOUT, Some("wrong encoded digest".to_string())),
        ] {
            std::fs::write(&source, bytes).expect("write rejected staging data");
            target.published_sha256 = published_sha256;
            let error = finish_rollout_publication(
                cross_device_rename(),
                &source,
                &target,
                home.path(),
                &locks,
            )
            .await
            .expect_err("do not publish an unauthenticated copy");
            assert!(
                error
                    .to_string()
                    .contains("copied lineage migration target failed verification")
            );
            assert_eq!(
                std::fs::read_dir(target.path.parent().expect("destination parent"))
                    .expect("read destination directory")
                    .count(),
                0
            );
        }
        target.published_sha256 = None;

        let error = finish_rollout_publication(
            Err(io::ErrorKind::PermissionDenied.into()),
            &source,
            &target,
            home.path(),
            &locks,
        )
        .await
        .expect_err("unrelated rename failures must not copy");
        assert!(
            error
                .to_string()
                .contains(&io::Error::from(io::ErrorKind::PermissionDenied).to_string())
        );
        assert!(!target.path.exists());

        // Simulate a destination that appears after the caller's existence check.
        std::fs::write(&target.path, b"conflicting file").expect("create conflicting destination");
        let error = finish_rollout_publication(
            cross_device_rename(),
            &source,
            &target,
            home.path(),
            &locks,
        )
        .await
        .expect_err("do not accept an incompatible destination");
        assert!(
            error
                .to_string()
                .contains("published lineage migration target failed verification")
        );
        assert_eq!(
            std::fs::read(&target.path).expect("retain conflicting destination"),
            b"conflicting file"
        );
        assert_eq!(
            std::fs::read(source).expect("retain staged rollout"),
            SYNTHETIC_ROLLOUT
        );
    }

    #[test]
    fn review_input_sync_includes_every_directory_through_codex_home() {
        let home = tempfile::tempdir().expect("Codex home");
        let rollout_id = ThreadId::new();
        let path = codex_rollout::review_input_segment_path(home.path(), rollout_id);
        let directories = publication_directory_entries(&path, home.path())
            .expect("bounded directory entries")
            .into_iter()
            .map(|entry| entry.parent().expect("containing directory").to_path_buf())
            .collect::<Vec<_>>();
        let detached_root = home
            .path()
            .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR);
        assert_eq!(
            directories,
            vec![
                detached_root.join(rollout_id.to_string()),
                detached_root,
                home.path().to_path_buf(),
            ]
        );
    }

    #[test]
    fn review_input_sync_rejects_paths_outside_codex_home() {
        let home = tempfile::tempdir().expect("Codex home");
        let outside = tempfile::tempdir().expect("different directory");
        assert!(publication_directory_entries(outside.path(), home.path()).is_err());
        assert!(publication_directory_entries(home.path(), home.path()).is_err());
    }
}
