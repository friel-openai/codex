use super::*;
use codex_protocol::SegmentId;
use pretty_assertions::assert_eq;
use std::path::Path;
use std::path::PathBuf;

pub(super) fn native_segment_path(home: &Path, rollout_id: Uuid) -> PathBuf {
    home.join(crate::SESSIONS_SUBDIR)
        .join(crate::ROLLOUT_SEGMENTS_SUBDIR)
        .join("2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{rollout_id}.jsonl"))
}

pub(super) fn detached_segment_path(
    home: &Path,
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    rollout_id: Uuid,
) -> PathBuf {
    home.join(crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(
            segment_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "initial".to_string()),
        )
        .join(format!("rollout-2025-01-03T12-00-00-{rollout_id}.jsonl"))
}

fn write_segment(
    path: &Path,
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
) -> anyhow::Result<()> {
    write_paginated_rollout(path, thread_id, "sealed history", 0)?;
    let mut lines = fs::read_to_string(path)?
        .lines()
        .map(crate::parse_rollout_line)
        .collect::<Result<Vec<_>, _>>()?;
    let RolloutItem::SessionMeta(meta) = &mut lines[0].item else {
        panic!("session metadata");
    };
    meta.meta.segment_id = segment_id;
    let bytes = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    fs::write(path, format!("{bytes}\n"))?;
    Ok(())
}

#[tokio::test]
async fn worker_compresses_native_detached_and_initial_segments_without_changing_bytes()
-> anyhow::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let paths = [
        (native_segment_path(home.path(), Uuid::new_v4()), None),
        (
            detached_segment_path(home.path(), thread_id, None, Uuid::new_v4()),
            None,
        ),
        (
            detached_segment_path(home.path(), thread_id, Some(segment_id), Uuid::new_v4()),
            Some(segment_id),
        ),
    ];
    let mut expected = Vec::new();
    for (path, id) in &paths {
        write_segment(path, thread_id, *id)?;
        set_old_mtime(path)?;
        expected.push((
            fs::read(path)?,
            fs::metadata(path)?.modified()?,
            open_rollout_line_reader(path).await?,
        ));
    }
    worker::run(home.path().to_path_buf(), Startup).await?;
    for ((path, _), (bytes, modified, mut open_reader)) in paths.iter().zip(expected) {
        assert!(!path.exists());
        let compressed = compressed_rollout_path(path);
        assert_eq!(fs::metadata(&compressed)?.modified()?, modified);
        assert_eq!(
            zstd::stream::decode_all(fs::File::open(&compressed)?)?,
            bytes
        );
        assert_eq!(
            crate::read_rollout_prefix(path, bytes.len())?,
            Some(bytes.clone())
        );
        let mut already_open_bytes = String::new();
        while let Some(line) = open_reader.next_line().await? {
            already_open_bytes.push_str(&line);
            already_open_bytes.push('\n');
        }
        assert_eq!(already_open_bytes.into_bytes(), bytes);
        let (items, id, errors) = RolloutRecorder::load_rollout_items(path).await?;
        assert_eq!(id, Some(thread_id));
        assert_eq!(errors, 0);
        assert_eq!(items.len(), 2);
        let mut corrupt = fs::read(&compressed)?;
        *corrupt.last_mut().expect("checksum") ^= 1;
        assert!(zstd::stream::decode_all(corrupt.as_slice()).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn segment_age_uses_mtime_not_filename_and_rejects_future_mtime() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let now = SystemTime::now();
    let mut paths = Vec::new();
    for modified in [
        now - Duration::from_secs(6 * 24 * 60 * 60),
        now + Duration::from_secs(24 * 60 * 60),
    ] {
        for path in [
            native_segment_path(home.path(), Uuid::new_v4()),
            detached_segment_path(home.path(), thread_id, None, Uuid::new_v4()),
        ] {
            write_segment(&path, thread_id, None)?;
            fs::File::options()
                .write(true)
                .open(&path)?
                .set_times(FileTimes::new().set_modified(modified))?;
            paths.push(path);
        }
    }
    worker::run(home.path().to_path_buf(), Startup).await?;
    for path in paths {
        assert!(path.exists());
        assert!(!compressed_rollout_path(&path).exists());
    }
    Ok(())
}

#[tokio::test]
async fn worker_preserves_young_segments_and_segments_reserved_by_writers_or_fork_readers()
-> anyhow::Result<()> {
    for reserved in [false, true] {
        for fork_reader in [false, true] {
            let home = TempDir::new()?;
            let thread_id = ThreadId::new();
            let paths = [
                native_segment_path(home.path(), Uuid::new_v4()),
                detached_segment_path(home.path(), thread_id, None, Uuid::new_v4()),
            ];
            let writers = std::sync::Arc::new(crate::WriterLockCoordinator::new(home.path()));
            let guard = if reserved {
                Some(if fork_reader {
                    writers.acquire_fork_reader(thread_id)?
                } else {
                    writers.acquire(thread_id)?
                })
            } else {
                None
            };
            for path in &paths {
                write_segment(path, thread_id, None)?;
                if reserved {
                    set_old_mtime(path)?;
                }
            }
            worker::run(home.path().to_path_buf(), Startup).await?;
            for path in paths {
                assert!(path.exists());
                assert!(!compressed_rollout_path(&path).exists());
            }
            drop(guard);
        }
    }
    Ok(())
}

#[tokio::test]
async fn worker_excludes_unknown_segment_layouts_and_mismatched_identities() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let valid = detached_segment_path(home.path(), thread_id, Some(segment_id), Uuid::new_v4());
    let paths = [
        detached_segment_path(home.path(), ThreadId::new(), None, Uuid::new_v4()),
        valid,
        home.path()
            .join(crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join("unknown")
            .join(format!(
                "rollout-2025-01-03T12-00-00-{}.jsonl",
                Uuid::new_v4()
            )),
        crate::review_input_segment_path(home.path(), thread_id),
        native_segment_path(home.path(), Uuid::new_v4())
            .parent()
            .unwrap()
            .join("extra")
            .join(format!(
                "rollout-2025-01-03T12-00-00-{}.jsonl",
                Uuid::new_v4()
            )),
    ];
    for path in &paths {
        write_segment(path, thread_id, None)?;
        set_old_mtime(path)?;
    }
    worker::run(home.path().to_path_buf(), Startup).await?;
    for path in paths {
        assert!(path.exists(), "unexpectedly compressed {path:?}");
        assert!(!compressed_rollout_path(&path).exists());
    }
    Ok(())
}

#[tokio::test]
async fn worker_preserves_plain_segment_when_compressed_sibling_is_stale_or_corrupt()
-> anyhow::Result<()> {
    for corrupt in [false, true] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = detached_segment_path(home.path(), thread_id, None, Uuid::new_v4());
        write_segment(&path, thread_id, None)?;
        set_old_mtime(&path)?;
        let expected = fs::read(&path)?;
        let sibling = if corrupt {
            b"not zstd".to_vec()
        } else {
            zstd::stream::encode_all(&b"obsolete bytes\n"[..], 3)?
        };
        fs::write(compressed_rollout_path(&path), &sibling)?;
        worker::run(home.path().to_path_buf(), Startup).await?;
        assert_eq!(fs::read(&path)?, expected);
        assert_eq!(fs::read(compressed_rollout_path(&path))?, sibling);
        let (items, id, errors) = RolloutRecorder::load_rollout_items(&path).await?;
        assert_eq!(id, Some(thread_id));
        assert_eq!(errors, 0);
        assert_eq!(items.len(), 2);
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn worker_preserves_linked_segments_and_does_not_follow_symlinked_roots() -> anyhow::Result<()>
{
    let home = TempDir::new()?;
    let outside = TempDir::new()?;
    let thread_id = ThreadId::new();
    let original = detached_segment_path(outside.path(), thread_id, None, Uuid::new_v4());
    write_segment(&original, thread_id, None)?;
    set_old_mtime(&original)?;
    std::os::unix::fs::symlink(
        outside.path().join(crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR),
        home.path().join(crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR),
    )?;
    let hard_link = native_segment_path(home.path(), Uuid::new_v4());
    fs::create_dir_all(hard_link.parent().unwrap())?;
    fs::hard_link(&original, &hard_link)?;
    let symlink = native_segment_path(home.path(), Uuid::new_v4());
    std::os::unix::fs::symlink(&original, &symlink)?;
    worker::run(home.path().to_path_buf(), Startup).await?;
    for path in [&original, &hard_link, &symlink] {
        assert!(path.exists());
        assert!(!compressed_rollout_path(path).exists());
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn worker_detects_segment_replacement_with_identical_size_and_mtime() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = detached_segment_path(home.path(), thread_id, None, Uuid::new_v4());
    write_segment(&path, thread_id, None)?;
    set_old_mtime(&path)?;
    let modified = fs::metadata(&path)?.modified()?;
    let original = fs::read(&path)?;
    let (started, resume) = worker::pause_encoding(&path);
    let compression = tokio::spawn(worker::run(home.path().to_path_buf(), Startup));
    tokio::task::spawn_blocking(move || started.recv_timeout(Duration::from_secs(5))).await??;
    let replacement = path.with_extension("replacement");
    fs::write(&replacement, &original)?;
    fs::File::options()
        .write(true)
        .open(&replacement)?
        .set_times(FileTimes::new().set_modified(modified))?;
    fs::rename(&replacement, &path)?;
    resume.send(())?;
    compression.await??;
    assert_eq!(fs::read(&path)?, original);
    assert!(!compressed_rollout_path(&path).exists());
    Ok(())
}

#[tokio::test]
async fn foreground_reader_cancels_segment_compression_even_after_worker_abort()
-> anyhow::Result<()> {
    for native in [false, true] {
        for abort_worker in [false, true] {
            let home = TempDir::new()?;
            let thread_id = ThreadId::new();
            let path = if native {
                native_segment_path(home.path(), Uuid::new_v4())
            } else {
                detached_segment_path(home.path(), thread_id, None, Uuid::new_v4())
            };
            write_segment(&path, thread_id, None)?;
            set_old_mtime(&path)?;
            let expected = fs::read(&path)?;
            let (started, resume) = worker::pause_encoding(&path);
            let mut compression = Some(tokio::spawn(worker::run(
                home.path().to_path_buf(),
                Startup,
            )));
            tokio::task::spawn_blocking(move || started.recv_timeout(Duration::from_secs(5)))
                .await??;
            let reader_home = home.path().to_path_buf();
            let reader = tokio::spawn(async move {
                crate::acquire_rollout_maintenance_read_lock(&reader_home).await
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while !crate::maintenance::foreground_maintenance_waiting(home.path())? {
                    tokio::task::yield_now().await;
                }
                Ok::<_, io::Error>(())
            })
            .await??;
            if abort_worker {
                let task = compression.take().unwrap();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                assert!(crate::try_acquire_rollout_maintenance_lock(home.path())?.is_none());
            }
            resume.send(())?;
            let reader = tokio::time::timeout(Duration::from_secs(5), reader).await???;
            if let Some(task) = compression {
                task.await??;
            }
            assert_eq!(fs::read(&path)?, expected);
            assert!(!compressed_rollout_path(&path).exists());
            assert!(!home.path().join(".tmp/rollout-compression.lock").exists());
            drop(reader);
        }
    }
    Ok(())
}

#[tokio::test]
async fn writer_acquired_during_segment_encoding_prevents_publication() -> anyhow::Result<()> {
    for native in [false, true] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = if native {
            native_segment_path(home.path(), Uuid::new_v4())
        } else {
            detached_segment_path(home.path(), thread_id, None, Uuid::new_v4())
        };
        write_segment(&path, thread_id, None)?;
        set_old_mtime(&path)?;
        let expected = fs::read(&path)?;
        let (started, resume) = worker::pause_encoding(&path);
        let compression = tokio::spawn(worker::run(home.path().to_path_buf(), Startup));
        tokio::task::spawn_blocking(move || started.recv_timeout(Duration::from_secs(5))).await??;
        let writers = std::sync::Arc::new(crate::WriterLockCoordinator::new(home.path()));
        let writer = writers.acquire(thread_id)?;
        resume.send(())?;
        compression.await??;
        assert_eq!(fs::read(&path)?, expected);
        assert!(!compressed_rollout_path(&path).exists());
        drop(writer);
    }
    Ok(())
}

#[tokio::test]
async fn detached_cleanup_removes_only_compression_temps() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let directory = home.path().join(crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR);
    fs::create_dir_all(&directory)?;
    let compression = directory.join("rollout-compress-stale.tmp");
    let installer = directory.join("review-input.install-stale.tmp");
    for path in [&compression, &installer] {
        fs::write(path, "temporary bytes")?;
        set_old_mtime(path)?;
    }
    worker::run(home.path().to_path_buf(), Startup).await?;
    assert!(!compression.exists());
    assert!(installer.exists());
    Ok(())
}
