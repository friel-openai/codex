//! Bounded rollout-reference resolution for thread-list summaries.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::RolloutReferenceItem;

use super::ARCHIVED_SESSIONS_SUBDIR;
use super::ROTATED_ROLLOUT_SEGMENTS_SUBDIR;
use super::compression;
use super::list::read_first_session_meta_line;
use super::list::rollout_path_for_timestamp_file;

/// Caches deterministic reference resolutions for one thread-list request.
#[derive(Default)]
pub(super) struct ListingSummaryContext {
    resolved_references: HashMap<(ThreadId, SegmentId), Option<PathBuf>>,
}

pub(super) async fn resolve_rollout_reference(
    codex_home: &Path,
    reference: &RolloutReferenceItem,
    context: &mut ListingSummaryContext,
) -> io::Result<Option<PathBuf>> {
    let cache_key = reference.thread_id.zip(reference.segment_id);
    if let Some(cache_key) = cache_key
        && let Some(path) = context.resolved_references.get(&cache_key)
    {
        return Ok(path.clone());
    }

    let mut resolved =
        validated_reference_candidate(reference.rollout_path.as_path(), reference).await?;

    if resolved.is_none()
        && let (Some(thread_id), Some(segment_id)) = (reference.thread_id, reference.segment_id)
    {
        let rotated_dir = codex_home
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string());
        resolved = find_valid_reference_in_directory(rotated_dir.as_path(), reference).await?;

        if resolved.is_none() {
            let archived_dir = codex_home
                .join(ARCHIVED_SESSIONS_SUBDIR)
                .join(thread_id.to_string())
                .join(segment_id.to_string());
            resolved = find_valid_reference_in_directory(archived_dir.as_path(), reference).await?;
        }
    }

    if resolved.is_none()
        && let (Some(thread_id), Some(rollout_timestamp)) =
            (reference.thread_id, reference.rollout_timestamp.as_deref())
    {
        let file_name = format!("rollout-{rollout_timestamp}-{thread_id}.jsonl");
        if let Some(active_path) =
            rollout_path_for_timestamp_file(codex_home, rollout_timestamp, file_name.as_str())
        {
            resolved = validated_reference_candidate(active_path.as_path(), reference).await?;
        }
        if resolved.is_none() {
            let archived_path = codex_home.join(ARCHIVED_SESSIONS_SUBDIR).join(file_name);
            resolved = validated_reference_candidate(archived_path.as_path(), reference).await?;
        }
    }

    if let Some(cache_key) = cache_key {
        context
            .resolved_references
            .insert(cache_key, resolved.clone());
    }
    Ok(resolved)
}

async fn validated_reference_candidate(
    path: &Path,
    reference: &RolloutReferenceItem,
) -> io::Result<Option<PathBuf>> {
    let Some(path) = compression::existing_rollout_path(path).await else {
        return Ok(None);
    };
    let Ok(session_meta) = read_first_session_meta_line(path.as_path()).await else {
        return Ok(None);
    };
    if reference
        .thread_id
        .is_some_and(|thread_id| thread_id != session_meta.meta.id)
        || reference
            .segment_id
            .is_some_and(|segment_id| session_meta.meta.segment_id != Some(segment_id))
    {
        return Ok(None);
    }
    Ok(Some(path))
}

async fn find_valid_reference_in_directory(
    directory: &Path,
    reference: &RolloutReferenceItem,
) -> io::Result<Option<PathBuf>> {
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let Some(rollout_file) = compression::RolloutFile::from_path(entry.path()) else {
            continue;
        };
        if let Some(path) = validated_reference_candidate(rollout_file.path(), reference).await? {
            return Ok(Some(path));
        }
    }
    Ok(None)
}
