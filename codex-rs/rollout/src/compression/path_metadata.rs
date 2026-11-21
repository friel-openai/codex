//! Resolves rollout metadata with shared plain-before-compressed precedence.

use std::fs::Metadata;
use std::path::Path;
use std::path::PathBuf;

use super::path::compressed_rollout_path;
use super::path::plain_rollout_path;

#[cfg(test)]
type LookupPause = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
#[cfg(test)]
static LOOKUP_PAUSES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<PathBuf, LookupPause>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
pub(super) fn pause_after_plain_miss(
    path: &Path,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    LOOKUP_PAUSES
        .lock()
        .expect("lookup pauses")
        .insert(plain_rollout_path(path), (started_tx, resume_rx));
    (started_rx, resume_tx)
}

/// Resolves a regular rollout file and its metadata without leaving the blocking worker.
pub(crate) fn existing_rollout_with_metadata_sync(path: &Path) -> Option<(PathBuf, Metadata)> {
    let plain_path = plain_rollout_path(path);
    let compressed_path = compressed_rollout_path(&plain_path);
    let plain_missing = match std::fs::metadata(&plain_path) {
        Ok(metadata) if metadata.is_file() => return Some((plain_path, metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        _ => false,
    };
    #[cfg(test)]
    {
        let pause = LOOKUP_PAUSES
            .lock()
            .expect("lookup pauses")
            .remove(&plain_path);
        if let Some((started, resume)) = pause {
            let _ = started.send(());
            let _ = resume.recv();
        }
    }
    match std::fs::metadata(&compressed_path) {
        Ok(metadata) if metadata.is_file() => Some((compressed_path, metadata)),
        Err(error) if plain_missing && error.kind() == std::io::ErrorKind::NotFound => {
            // Materialization publishes the plain file before removing the compressed sibling.
            std::fs::metadata(&plain_path)
                .ok()
                .filter(Metadata::is_file)
                .map(|metadata| (plain_path, metadata))
        }
        _ => None,
    }
}
