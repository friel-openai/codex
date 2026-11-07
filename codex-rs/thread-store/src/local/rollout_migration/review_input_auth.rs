//! Authenticates detached review inputs without adding them to conversation replay.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ReviewInputRecord;
use codex_rollout::RolloutItem;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncReadExt;

use super::LegacyLineageSource;
use super::migration_error;
use crate::ThreadStoreResult;

/// Authenticates each physical segment once, preserving first-reference order.
pub(super) async fn authenticate_review_input_sources(
    codex_home: &Path,
    positions: impl IntoIterator<Item = HistoryPosition>,
) -> ThreadStoreResult<Vec<LegacyLineageSource>> {
    let mut indices = HashMap::<ThreadId, usize>::new();
    let mut grouped = Vec::<(ThreadId, BTreeMap<u64, u64>)>::new();
    for position in positions {
        let index = *indices.entry(position.thread_id).or_insert_with(|| {
            grouped.push((position.thread_id, BTreeMap::new()));
            grouped.len() - 1
        });
        if let Some(previous) = grouped[index]
            .1
            .insert(position.end_ordinal_exclusive, position.end_byte_offset)
            && previous != position.end_byte_offset
        {
            return Err(migration_error(
                "review input has conflicting byte endpoints",
            ));
        }
    }
    if grouped.is_empty() {
        return Ok(Vec::new());
    }
    let root = tokio::fs::canonicalize(codex_home)
        .await
        .map_err(migration_error)?
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR);
    let mut sources = Vec::with_capacity(grouped.len());
    for (thread_id, endpoints) in grouped {
        let requested_path = codex_rollout::review_input_segment_path(codex_home, thread_id);
        let path = tokio::fs::canonicalize(&requested_path)
            .await
            .map_err(migration_error)?;
        if !path.starts_with(&root) {
            return Err(migration_error("review input is outside detached storage"));
        }
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(migration_error)?;
        let before = file.metadata().await.map_err(migration_error)?;
        if !before.is_file() {
            return Err(migration_error("review input is not a regular file"));
        }
        let (&end_ordinal_exclusive, &end_byte_offset) = endpoints
            .last_key_value()
            .ok_or_else(|| migration_error("review input has no endpoint"))?;
        if end_byte_offset > before.len() {
            return Err(migration_error(
                "review input endpoint exceeds segment size",
            ));
        }
        let records = codex_rollout::load_review_input_prefix(
            codex_home,
            HistoryPosition {
                thread_id,
                end_ordinal_exclusive,
                end_byte_offset,
            },
        )
        .await
        .map_err(migration_error)?;
        let mut previous_bytes = 0;
        for (&ordinal, &bytes) in &endpoints {
            let record = ordinal
                .checked_sub(2)
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| records.get(index));
            if bytes <= previous_bytes
                || !matches!(record, Some(ReviewInputRecord::Rollback { .. }))
            {
                return Err(migration_error(
                    "review input endpoint must follow a rollback",
                ));
            }
            previous_bytes = bytes;
        }
        drop(records);

        let mut reader = (&mut file).take(before.len());
        let mut buffer = [0_u8; 64 * 1024];
        let mut header = Vec::new();
        let mut hasher = Sha256::new();
        let mut byte_count = 0_u64;
        let mut record_count = 0_u64;
        let mut last_byte = None;
        let mut pending = endpoints.iter().peekable();
        loop {
            let count = reader.read(&mut buffer).await.map_err(migration_error)?;
            if count == 0 {
                break;
            }
            let bytes = &buffer[..count];
            hasher.update(bytes);
            if record_count == 0 {
                let header_end = bytes
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(count, |index| index + 1);
                if header.len() + header_end > super::super::MAX_ROLLOUT_LINE_BYTES {
                    return Err(migration_error(
                        "review input metadata exceeds the record limit",
                    ));
                }
                header.extend_from_slice(&bytes[..header_end]);
            }
            for (index, byte) in bytes.iter().enumerate() {
                if *byte != b'\n' {
                    continue;
                }
                record_count += 1;
                if let Some(&(&ordinal, &end)) = pending.peek()
                    && record_count == ordinal
                {
                    if byte_count + index as u64 + 1 != end {
                        return Err(migration_error(
                            "review input byte and ordinal endpoints differ",
                        ));
                    }
                    pending.next();
                }
            }
            byte_count += count as u64;
            last_byte = bytes.last().copied();
        }
        if byte_count != before.len() || last_byte != Some(b'\n') || pending.next().is_some() {
            return Err(migration_error(
                "review input has incomplete records or endpoints",
            ));
        }
        let line = codex_rollout::parse_rollout_line(
            std::str::from_utf8(&header).map_err(migration_error)?,
        )
        .map_err(migration_error)?;
        let RolloutItem::SessionMeta(metadata) = line.item else {
            return Err(migration_error("review input has no session metadata"));
        };
        let segment_id = SegmentId::from_string(&thread_id.to_string()).map_err(migration_error)?;
        if line.ordinal != Some(0)
            || metadata.meta.id != thread_id
            || metadata.meta.segment_id != Some(segment_id)
            || metadata.meta.history_mode != ThreadHistoryMode::Paginated
            || metadata.meta.history_base.is_some()
        {
            return Err(migration_error(
                "review input metadata does not identify its segment",
            ));
        }
        // Publication makes these files immutable. Detect a concurrent replacement or append
        // rather than authenticating bytes from different versions of the dependency.
        let after = tokio::fs::metadata(&path).await.map_err(migration_error)?;
        if after.len() != before.len()
            || after.modified().map_err(migration_error)?
                != before.modified().map_err(migration_error)?
            || tokio::fs::canonicalize(&requested_path)
                .await
                .map_err(migration_error)?
                != path
        {
            return Err(migration_error(
                "review input changed while being authenticated",
            ));
        }
        sources.push(LegacyLineageSource {
            thread_id,
            rollout_id: thread_id,
            segment_id: Some(segment_id),
            path,
            history_mode: ThreadHistoryMode::Paginated,
            byte_count,
            record_count,
            has_rollback: false,
            canonical_paginated_suffix: false,
            sha256: format!("{:x}", hasher.finalize()),
            review_input_dependencies: Vec::new(),
            timestamp: metadata.meta.timestamp,
            initial_source_line_index: 1,
            initial_next_item_index: 1,
            predecessor: None,
            reference_ordinal: None,
            replay_end: None,
            native_replay: None,
            materialized_predecessor: false,
        });
    }
    Ok(sources)
}
