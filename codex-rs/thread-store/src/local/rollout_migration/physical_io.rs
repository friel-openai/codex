//! Retains migration or private scratch ownership until physical filesystem work finishes.

use std::fs::Permissions;
use std::future::Future;
use std::io;
use std::io::Write;
use std::path::Component;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

use super::MigrationLocks;

#[derive(Clone)]
pub(super) enum IoOwner {
    Migration(Arc<MigrationLocks>),
    Scratch(Arc<tempfile::TempDir>),
}

fn spawn_owned<T: Send + 'static>(
    owner: IoOwner,
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> JoinHandle<io::Result<T>> {
    tokio::task::spawn_blocking(move || match owner {
        IoOwner::Migration(locks) => {
            let _locks = locks;
            work()
        }
        IoOwner::Scratch(directory) => {
            let _directory = directory;
            work()
        }
    })
}

pub(super) async fn run<T: Send + 'static>(
    locks: &Arc<MigrationLocks>,
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    spawn_owned(IoOwner::Migration(Arc::clone(locks)), work)
        .await
        .map_err(io::Error::other)?
}

pub(super) async fn rename(
    source: &Path,
    target: &Path,
    locks: &Arc<MigrationLocks>,
) -> io::Result<()> {
    IoOwner::Migration(Arc::clone(locks))
        .rename(source, target)
        .await
}

pub(super) async fn hard_link(
    source: &Path,
    target: &Path,
    locks: &Arc<MigrationLocks>,
) -> io::Result<()> {
    let source = source.to_path_buf();
    let target = target.to_path_buf();
    run(locks, move || std::fs::hard_link(source, target)).await
}

pub(super) async fn remove_file(path: &Path, locks: &Arc<MigrationLocks>) -> io::Result<()> {
    let path = path.to_path_buf();
    run(locks, move || std::fs::remove_file(path)).await
}

pub(super) async fn remove_dir_all(path: &Path, locks: &Arc<MigrationLocks>) -> io::Result<()> {
    let path = path.to_path_buf();
    run(locks, move || std::fs::remove_dir_all(path)).await
}

impl IoOwner {
    fn validate_path(&self, path: &Path) -> io::Result<()> {
        if let Self::Scratch(directory) = self
            && (!path.starts_with(directory.path())
                || path.components().any(|part| part == Component::ParentDir))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scratch output is outside its retained temporary directory",
            ));
        }
        Ok(())
    }

    fn validate_existing_path(&self, path: &Path) -> io::Result<()> {
        self.validate_path(path)?;
        if let Self::Scratch(directory) = self {
            let mut ancestor = directory.path().to_path_buf();
            let reject_symbolic_link = |path: &Path| match std::fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "scratch output crosses a symbolic link",
                    ));
                }
                Ok(_) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            };
            reject_symbolic_link(&ancestor)?;
            for component in path
                .strip_prefix(directory.path())
                .map_err(io::Error::other)?
                .components()
            {
                ancestor.push(component.as_os_str());
                reject_symbolic_link(&ancestor)?;
            }
        }
        Ok(())
    }

    pub(super) async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.validate_path(path)?;
        let path = path.to_path_buf();
        let owner = self.clone();
        spawn_owned(self.clone(), move || {
            owner.validate_existing_path(&path)?;
            std::fs::create_dir_all(path)
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(super) async fn decompress_rollout_to_path(
        &self,
        compressed_path: &Path,
        plain_path: &Path,
    ) -> io::Result<()> {
        self.validate_path(plain_path)?;
        let compressed_path = compressed_path.to_path_buf();
        let plain_path = plain_path.to_path_buf();
        let owner = self.clone();
        spawn_owned(self.clone(), move || {
            owner.validate_existing_path(&plain_path)?;
            let input = std::fs::File::open(compressed_path)?;
            let mut decoder = zstd::stream::read::Decoder::new(input)?;
            let mut options = std::fs::OpenOptions::new();
            options.create(true).truncate(true).write(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut output = options.open(plain_path)?;
            #[cfg(unix)]
            output.set_permissions(Permissions::from_mode(0o600))?;
            io::copy(&mut decoder, &mut output)?;
            output.flush()
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(super) async fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.validate_path(source)?;
        self.validate_path(target)?;
        let source = source.to_path_buf();
        let target = target.to_path_buf();
        let owner = self.clone();
        spawn_owned(self.clone(), move || {
            owner.validate_existing_path(&source)?;
            owner.validate_existing_path(&target)?;
            std::fs::rename(source, target)
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(super) async fn sync_parent_directory(&self, path: &Path) -> io::Result<()> {
        self.validate_path(path)?;
        self.validate_path(
            path.parent()
                .ok_or_else(|| io::Error::other("rollout path has no parent directory"))?,
        )?;
        let path = path.to_path_buf();
        let owner = self.clone();
        spawn_owned(self.clone(), move || {
            owner.validate_existing_path(&path)?;
            sync_parent_directory_blocking(&path)
        })
        .await
        .map_err(io::Error::other)?
    }
}

pub(super) fn sync_parent_directory_blocking(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("rollout path has no parent directory"))?;
        std::fs::File::open(parent)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum OutputOperation {
    Write,
    Flush,
}

struct PendingOutput {
    operation: OutputOperation,
    worker: JoinHandle<io::Result<()>>,
}

pub(super) struct OutputFile {
    file: Arc<Mutex<std::fs::File>>,
    owner: IoOwner,
    pending: Option<PendingOutput>,
}

impl OutputFile {
    pub(super) async fn create(
        path: &Path,
        permissions: Permissions,
        owner: &IoOwner,
    ) -> io::Result<Self> {
        owner.validate_path(path)?;
        let path = path.to_path_buf();
        let path_owner = owner.clone();
        let file = spawn_owned(owner.clone(), move || {
            path_owner.validate_existing_path(&path)?;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?;
            file.set_permissions(permissions)?;
            Ok(file)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            owner: owner.clone(),
            pending: None,
        })
    }

    pub(super) async fn sync_all(&mut self) -> io::Result<()> {
        self.flush().await?;
        let file = Arc::clone(&self.file);
        spawn_owned(self.owner.clone(), move || {
            file.lock()
                .map_err(|_| io::Error::other("migration output mutex poisoned"))?
                .sync_all()
        })
        .await
        .map_err(io::Error::other)?
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<OutputOperation>>> {
        let Some(pending) = self.pending.as_mut() else {
            return Poll::Ready(Ok(None));
        };
        let result = ready!(Pin::new(&mut pending.worker).poll(cx));
        let operation = pending.operation;
        self.pending = None;
        Poll::Ready(result.map_err(io::Error::other)?.map(|()| Some(operation)))
    }
}

impl AsyncWrite for OutputFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self.poll_pending(cx))?;
        let count = bytes.len().min(super::PROJECTION_BATCH_BYTES as usize);
        if count == 0 {
            return Poll::Ready(Ok(0));
        }
        let bytes = bytes[..count].to_vec();
        let file = Arc::clone(&self.file);
        self.pending = Some(PendingOutput {
            operation: OutputOperation::Write,
            worker: spawn_owned(self.owner.clone(), move || {
                file.lock()
                    .map_err(|_| io::Error::other("migration output mutex poisoned"))?
                    .write_all(&bytes)
            }),
        });
        // Acceptance transfers bytes, not physical completion. The worker owns the lease;
        // flush joins it, including its write error, before publication can proceed.
        Poll::Ready(Ok(count))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if matches!(ready!(self.poll_pending(cx))?, Some(OutputOperation::Flush)) {
            return Poll::Ready(Ok(()));
        }
        let file = Arc::clone(&self.file);
        self.pending = Some(PendingOutput {
            operation: OutputOperation::Flush,
            worker: spawn_owned(self.owner.clone(), move || {
                file.lock()
                    .map_err(|_| io::Error::other("migration output mutex poisoned"))?
                    .flush()
            }),
        });
        ready!(self.poll_pending(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
#[path = "physical_io_tests.rs"]
mod tests;
