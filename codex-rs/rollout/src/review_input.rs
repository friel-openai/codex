//! Resolves finite transcript-only migration inputs without adding them to rollout history.

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_history::ReviewInputRecord;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use serde::Deserialize;
use serde_json::Value;

use crate::CompactedItem;
use crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR;
use crate::RolloutItem;

/// Detached review inputs are not owned by a mutable thread's deletion lifecycle.
pub fn review_input_segment_path(codex_home: &Path, rollout_id: ThreadId) -> PathBuf {
    codex_home
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(rollout_id.to_string())
        .join("review-input.jsonl")
}

/// Loads exactly one immutable review prefix, including its original transcript baseline.
/// The endpoint is validated against both physical JSONL bytes and contiguous ordinals.
pub async fn load_review_input_prefix(
    codex_home: &Path,
    position: HistoryPosition,
) -> io::Result<Vec<ReviewInputRecord>> {
    let path = review_input_segment_path(codex_home, position.thread_id);
    let path = crate::existing_rollout_path(&path).await.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("review input segment {} was not found", position.thread_id),
        )
    })?;
    let root = tokio::fs::canonicalize(codex_home)
        .await?
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR);
    let path = tokio::fs::canonicalize(path).await?;
    if !path.starts_with(root) {
        return Err(invalid_review_input("segment is outside detached storage"));
    }
    tokio::task::spawn_blocking(move || {
        let max_bytes = usize::try_from(position.end_byte_offset)
            .map_err(|_| invalid_review_input("byte endpoint exceeds addressable memory"))?;
        let bytes = crate::read_rollout_prefix(&path, max_bytes)?
            .ok_or_else(|| invalid_review_input("segment is not a regular file"))?;
        if bytes.len() != max_bytes || !bytes.ends_with(b"\n") {
            return Err(invalid_review_input(
                "byte endpoint is not a complete record boundary",
            ));
        }
        let mut encoded_lines = bytes[..bytes.len() - 1].split(|byte| *byte == b'\n');
        let header = encoded_lines
            .next()
            .ok_or_else(|| invalid_review_input("segment has no session metadata"))?;
        let header = crate::decode_rollout_line(serde_json::from_slice(header)?)?;
        let RolloutItem::SessionMeta(meta) = header.item else {
            return Err(invalid_review_input("first record is not session metadata"));
        };
        if header.ordinal != Some(0)
            || meta.meta.id != position.thread_id
            || meta.meta.segment_id.map(|id| id.to_string()) != Some(position.thread_id.to_string())
            || meta.meta.history_mode != ThreadHistoryMode::Paginated
            || meta.meta.history_base.is_some()
        {
            return Err(invalid_review_input(
                "session metadata does not identify the segment",
            ));
        }
        let mut records = Vec::new();
        let mut next_ordinal = 1_u64;
        for encoded in encoded_lines {
            let line: ReviewInputLine = serde_json::from_value(serde_json::from_slice(encoded)?)?;
            if line.ordinal != next_ordinal {
                return Err(invalid_review_input("record ordinals are not contiguous"));
            }
            let record: ReviewInputRecord = serde_json::from_value(line.record)?;
            if matches!(record, ReviewInputRecord::Baseline { .. }) != records.is_empty() {
                return Err(invalid_review_input(
                    "prefix must contain exactly one initial baseline",
                ));
            }
            records.push(record);
            next_ordinal = next_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid_review_input("record ordinal overflow"))?;
        }
        if next_ordinal != position.end_ordinal_exclusive
            || !matches!(records.last(), Some(ReviewInputRecord::Rollback { .. }))
        {
            return Err(invalid_review_input(
                "ordinal endpoint must follow a rollback",
            ));
        }
        Ok(records)
    })
    .await
    .map_err(io::Error::other)?
}

/// Hydrates a caller-selected checkpoint without exposing review inputs as model items.
pub async fn resolve_review_input(
    codex_home: &Path,
    compacted: &mut CompactedItem,
) -> io::Result<()> {
    let Some(replay) = compacted.retained_context_replay.as_mut() else {
        return Ok(());
    };
    let Some(position) = replay.review_input else {
        return Ok(());
    };
    if replay.resolved_review_input.is_none() {
        replay.resolved_review_input = Some(Arc::new(
            load_review_input_prefix(codex_home, position).await?,
        ));
    }
    Ok(())
}

/// Separates ordinal framing from the tagged payload's canonical JSON deserialization.
#[derive(Deserialize)]
struct ReviewInputLine {
    /// Zero-based position within the immutable segment, including its metadata header.
    ordinal: u64,
    record: Value,
}

fn invalid_review_input(message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid review input: {message}"),
    )
}

#[cfg(test)]
#[path = "review_input_tests.rs"]
mod tests;
