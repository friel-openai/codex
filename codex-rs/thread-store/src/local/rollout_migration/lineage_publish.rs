//! Publishes and verifies staged lineage targets.

use std::io::Read;
use std::path::Path;

use sha2::Digest;
use sha2::Sha256;

use super::lineage::hash_file;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationJournalTarget;
use super::lineage_journal::write_lineage_migration_journal;
use super::migration_error;
use super::publish::compress_rollout_to_path;
use super::publish::sync_parent_directory;
use crate::ThreadStoreResult;

pub(super) async fn publish_lineage_targets(
    journal_path: &Path,
    journal: &mut LineageMigrationJournal,
) -> ThreadStoreResult<()> {
    journal.validate_review_input_target()?;
    if let Some(target) = journal
        .review_input_target
        .as_mut()
        .filter(|target| target.staged_path.is_some())
    {
        let codex_home = journal_path
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| migration_error("lineage migration journal has no Codex home"))?;
        publish_one_target(target, Some(codex_home)).await?;
        write_lineage_migration_journal(journal_path, journal).await?;
    }
    for index in 0..journal.targets.len() {
        publish_one_target(
            &mut journal.targets[index],
            /*review_input_codex_home*/ None,
        )
        .await?;
        write_lineage_migration_journal(journal_path, journal).await?;
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
    review_input_codex_home: Option<&Path>,
) -> ThreadStoreResult<()> {
    if let Some(codex_home) = review_input_codex_home
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
        target.published_sha256 = Some(verify_published_target(target).await?);
        if let Some(codex_home) = review_input_codex_home {
            sync_review_input_directories(target, codex_home).await?;
        }
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
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(migration_error)?;
    let compressed = target
        .path
        .extension()
        .is_some_and(|extension| extension == "zst");
    if review_input_codex_home.is_some() {
        // Other checkpoints may already reference the content-derived destination. Never replace
        // it or delete it during rollback; only the transaction's staged link is temporary.
        match tokio::fs::hard_link(staged_path, &target.path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                verify_published_target(target).await?;
            }
            Err(error) => return Err(migration_error(error)),
        }
    } else if compressed {
        let temporary = staged_path.with_extension("publish.zst.tmp");
        let permissions = tokio::fs::metadata(staged_path)
            .await
            .map_err(migration_error)?
            .permissions();
        compress_rollout_to_path(
            staged_path,
            temporary.as_path(),
            permissions,
            /*modified_at*/ None,
        )
        .await?;
        tokio::fs::rename(temporary.as_path(), target.path.as_path())
            .await
            .map_err(migration_error)?;
    } else {
        tokio::fs::rename(staged_path, target.path.as_path())
            .await
            .map_err(migration_error)?;
    }
    if let Some(codex_home) = review_input_codex_home {
        sync_review_input_directories(target, codex_home).await?;
    } else {
        sync_parent_directory(target.path.as_path()).await?;
    }
    target.published_sha256 = Some(verify_published_target(target).await?);
    Ok(())
}

async fn sync_review_input_directories(
    target: &LineageMigrationJournalTarget,
    codex_home: &Path,
) -> ThreadStoreResult<()> {
    // A matching hard link can be visible before its creator synchronizes the directory. Sync
    // every containing entry, including a newly created UUID directory and detached-storage root.
    for entry in review_input_directory_entries(&target.path, codex_home)? {
        sync_parent_directory(entry).await?;
    }
    Ok(())
}

fn review_input_directory_entries<'a>(
    path: &'a Path,
    codex_home: &Path,
) -> ThreadStoreResult<Vec<&'a Path>> {
    if !path.starts_with(codex_home) || path == codex_home {
        return Err(migration_error(
            "review-input target has no Codex-home ancestor",
        ));
    }
    let mut entries = Vec::new();
    let mut entry = path;
    loop {
        entries.push(entry);
        let parent = entry
            .parent()
            .ok_or_else(|| migration_error("review-input target has no Codex-home ancestor"))?;
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
    tokio::task::spawn_blocking(move || -> ThreadStoreResult<String> {
        let input = std::fs::File::open(path).map_err(migration_error)?;
        let mut decoder = zstd::stream::read::Decoder::new(input).map_err(migration_error)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 256 * 1024];
        loop {
            let read = decoder
                .read(buffer.as_mut_slice())
                .map_err(migration_error)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(format!("{:x}", hasher.finalize()))
    })
    .await
    .map_err(migration_error)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::ThreadId;

    #[test]
    fn review_input_sync_includes_every_directory_through_codex_home() {
        let home = tempfile::tempdir().expect("Codex home");
        let rollout_id = ThreadId::new();
        let path = codex_rollout::review_input_segment_path(home.path(), rollout_id);
        let directories = review_input_directory_entries(&path, home.path())
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
        assert!(review_input_directory_entries(outside.path(), home.path()).is_err());
        assert!(review_input_directory_entries(home.path(), home.path()).is_err());
    }
}
