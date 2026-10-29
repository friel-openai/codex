//! Captures an externally written fork without reserving or modifying its source.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::Metadata;
use std::io;
use std::io::Read as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::ReverseJsonlScanner;
use crate::ScanOutcome;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncSeekExt as _;

use super::ExpansionCursor;
use super::ExpansionState;
use super::MaterializationPolicy;
use super::RolloutLine;
use super::RolloutRecorder;
use super::canonical_session_meta;
use super::compression;
use super::expand_lines;

/// Expands a self-contained Legacy fork from finite, verified source prefixes.
///
/// Appends after each file is opened are excluded. A changed or truncated captured prefix returns
/// `WouldBlock`; replacing a file with the same selected bytes does not change the fork history.
/// The existing materializer still owns reference identity, filters, and native history cutoffs.
pub async fn capture_external_legacy_rollout_lines(
    codex_home: &Path,
    path: &Path,
    thread_id: ThreadId,
) -> io::Result<Vec<RolloutLine>> {
    capture_external_rollout_lines(codex_home, path, thread_id, ThreadHistoryMode::Legacy)
        .await
        .map(|(lines, _)| lines)
}

/// Copies complete visible history and the physical root cutoff without taking its writer.
///
/// This exceptional external-writer operation reads every selected finite lineage prefix. Normal
/// indexed fork preparation does not use it. The cutoff includes records omitted by payload
/// compatibility decoding and root metadata omitted from expanded ancestry.
pub async fn capture_external_paginated_rollout_lines(
    codex_home: &Path,
    path: &Path,
    thread_id: ThreadId,
) -> io::Result<(Vec<RolloutLine>, u64)> {
    let (lines, next_ordinal) =
        capture_external_rollout_lines(codex_home, path, thread_id, ThreadHistoryMode::Paginated)
            .await?;
    Ok((
        lines,
        next_ordinal.expect("paginated capture has a physical cutoff"),
    ))
}

async fn capture_external_rollout_lines(
    codex_home: &Path,
    path: &Path,
    thread_id: ThreadId,
    history_mode: ThreadHistoryMode,
) -> io::Result<(Vec<RolloutLine>, Option<u64>)> {
    let mut prefixes = CapturedPrefixes::default();
    let lines = prefixes.load(path, None, /*strict*/ false).await?;
    let metadata = canonical_session_meta(&lines)?;
    if metadata.meta.id != thread_id || metadata.meta.history_mode != history_mode {
        return Err(conflict(
            "selected external fork source is not the expected thread and history mode",
        ));
    }
    let next_ordinal = if history_mode == ThreadHistoryMode::Paginated {
        Some(prefixes.next_paginated_ordinal(path).await?)
    } else {
        None
    };
    let mut materialized = vec![lines[0].clone()];
    let mut state = ExpansionState {
        retain_source_metadata: false,
        active_segments: HashSet::new(),
        active_history_bases: HashSet::new(),
        cache: None,
        captured_prefixes: Some(&mut prefixes),
    };
    let mut has_older_reference = false;
    materialized.extend(
        expand_lines(
            codex_home,
            lines,
            &mut state,
            ExpansionCursor {
                graph_depth: 0,
                ordinary_reference_depth: 0,
                current_thread_id: thread_id,
            },
            None,
            MaterializationPolicy::Complete,
            &mut has_older_reference,
        )
        .await?,
    );
    prefixes.verify().await?;
    Ok((materialized, next_ordinal))
}

/// Retains one opened handle and one finite physical prefix for each visited rollout file.
#[derive(Default)]
pub(super) struct CapturedPrefixes {
    files: HashMap<PathBuf, CapturedPrefix>,
    /// Original selections must still identify the opened files, including through symlinks.
    selected_paths: HashMap<PathBuf, PathBuf>,
}

/// The bytes selected before a source reader can observe later appends.
struct CapturedPrefix {
    file: tokio::fs::File,
    /// The initial physical length bounds every later request for this opened file.
    metadata: Metadata,
    /// Physical bytes, including compressed bytes when the selected representation is zstd.
    bytes: Arc<[u8]>,
}

impl CapturedPrefixes {
    async fn next_paginated_ordinal(&self, selected_path: &Path) -> io::Result<u64> {
        let path = self.selected_paths[selected_path].clone();
        let bytes = Arc::clone(&self.files[&path].bytes);
        tokio::task::spawn_blocking(move || {
            let bytes = decoded_prefix(&path, &bytes, None)?;
            let mut scanner = ReverseJsonlScanner::new(io::Cursor::new(bytes))?;
            let mut next = None;
            let mut previous = None;
            while let Some(record) = scanner.scan_next::<serde_json::Value>()? {
                let value = match record {
                    ScanOutcome::Parsed(value) => value,
                    ScanOutcome::Rejected(error) if previous.is_none() && error.is_eof() => {
                        continue;
                    }
                    ScanOutcome::Rejected(error) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, error));
                    }
                };
                let ordinal = value
                    .get("ordinal")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| conflict("captured paginated record is missing an ordinal"))?;
                if previous.is_some_and(|previous| ordinal >= previous) {
                    return Err(conflict(
                        "captured paginated ordinals are not strictly increasing",
                    ));
                }
                if next.is_none() {
                    next = Some(
                        ordinal
                            .checked_add(1)
                            .ok_or_else(|| conflict("captured paginated ordinal overflow"))?,
                    );
                }
                previous = Some(ordinal);
            }
            next.ok_or_else(|| conflict("captured paginated source has no physical records"))
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(super) async fn load(
        &mut self,
        path: &Path,
        end: Option<u64>,
        strict: bool,
    ) -> io::Result<Vec<RolloutLine>> {
        let selected_path = path.to_path_buf();
        let path = tokio::fs::canonicalize(path).await?;
        if let Some(previous) = self.selected_paths.insert(selected_path, path.clone())
            && previous != path
        {
            return Err(conflict(
                "external fork source selection changed during capture",
            ));
        }
        if !self.files.contains_key(&path) {
            let file = tokio::fs::File::open(&path).await?;
            let metadata = file.metadata().await?;
            if !metadata.is_file() {
                return Err(conflict("external fork source is not a regular file"));
            }
            self.files.insert(
                path.clone(),
                CapturedPrefix {
                    file,
                    metadata,
                    bytes: Arc::from([]),
                },
            );
        }
        let compressed = compression::plain_rollout_path(&path) != path;
        let captured = self.files.get_mut(&path).expect("captured rollout file");
        let selected_len = if compressed {
            captured.metadata.len()
        } else {
            end.unwrap_or(captured.metadata.len())
        };
        if selected_len > captured.metadata.len() {
            return Err(conflict(
                "external fork cutoff exceeds its originally selected file",
            ));
        }
        if selected_len > captured.bytes.len() as u64 {
            captured.file.seek(io::SeekFrom::Start(0)).await?;
            let mut bytes = Vec::new();
            (&mut captured.file)
                .take(selected_len)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() as u64 != selected_len || !bytes.starts_with(captured.bytes.as_ref()) {
                return Err(conflict("external fork source changed during capture"));
            }
            captured.bytes = bytes.into();
        }
        let bytes = Arc::clone(&captured.bytes);
        tokio::task::spawn_blocking(move || parse_captured_prefix(&path, &bytes, end, strict))
            .await
            .map_err(io::Error::other)?
    }

    pub(super) async fn load_history_base(
        &mut self,
        codex_home: &Path,
        position: HistoryPosition,
    ) -> io::Result<Vec<RolloutLine>> {
        let path = crate::find_rollout_path_by_rollout_id(codex_home, position.thread_id)
            .await?
            .ok_or_else(|| conflict("external fork history_base rollout is missing"))?;
        let path = compression::existing_rollout_path(&path)
            .await
            .ok_or_else(|| conflict("external fork history_base representation disappeared"))?;
        let lines = self
            .load(&path, Some(position.end_byte_offset), /*strict*/ true)
            .await?;
        if lines
            .last()
            .and_then(|line| line.ordinal)
            .and_then(|ordinal| ordinal.checked_add(1))
            != Some(position.end_ordinal_exclusive)
        {
            return Err(conflict(
                "external fork history_base ordinal cutoff changed",
            ));
        }
        Ok(lines)
    }

    async fn verify(&mut self) -> io::Result<()> {
        self.verify_selections().await?;
        for captured in self.files.values_mut() {
            captured.file.seek(io::SeekFrom::Start(0)).await?;
            let mut verified = Vec::new();
            (&mut captured.file)
                .take(captured.bytes.len() as u64)
                .read_to_end(&mut verified)
                .await?;
            if verified.as_slice() != captured.bytes.as_ref() {
                return Err(conflict(
                    "external fork source prefix changed during capture",
                ));
            }
        }
        self.verify_selections().await?;
        Ok(())
    }

    async fn verify_selections(&self) -> io::Result<()> {
        for (selected, canonical) in &self.selected_paths {
            let captured = &self.files[canonical];
            let mut current = tokio::fs::File::open(selected).await.map_err(|_| {
                conflict("external fork source selection disappeared during capture")
            })?;
            if !current.metadata().await?.is_file() {
                return Err(conflict(
                    "external fork source selection changed during capture",
                ));
            }
            let mut verified = Vec::new();
            (&mut current)
                .take(captured.bytes.len() as u64)
                .read_to_end(&mut verified)
                .await?;
            if verified.as_slice() != captured.bytes.as_ref() {
                return Err(conflict(
                    "external fork source selection changed during capture",
                ));
            }
        }
        Ok(())
    }
}

fn parse_captured_prefix(
    path: &Path,
    physical_bytes: &[u8],
    end: Option<u64>,
    strict: bool,
) -> io::Result<Vec<RolloutLine>> {
    let bytes = decoded_prefix(path, physical_bytes, end)?;
    let prefix = match end {
        Some(end) => {
            let end = usize::try_from(end)
                .map_err(|_| conflict("external fork cutoff exceeds addressable memory"))?;
            bytes
                .get(..end)
                .filter(|prefix| prefix.ends_with(b"\n"))
                .ok_or_else(|| conflict("external fork cutoff is not a complete record boundary"))?
        }
        None => bytes.as_ref(),
    };
    let (records, _, mut parse_errors) =
        RolloutRecorder::load_rollout_lines_from_bytes(path, prefix)?;
    let mut lines = records
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>();
    let metadata = canonical_session_meta(&lines)?;
    if end.is_none() && !prefix.ends_with(b"\n") {
        let last_start = prefix
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let tail = &prefix[last_start..];
        let complete_len = if metadata.meta.history_mode == ThreadHistoryMode::Legacy {
            crate::legacy_jsonl::complete_legacy_jsonl_prefix_len(tail)
        } else if RolloutRecorder::parse_rollout_line_bytes(tail).is_err_and(|error| error.is_eof())
        {
            0
        } else {
            tail.len()
        };
        if complete_len < tail.len() {
            let (records, _, errors) = RolloutRecorder::load_rollout_lines_from_bytes(
                path,
                &prefix[..last_start + complete_len],
            )?;
            lines = records.into_iter().map(|(_, line)| line).collect();
            parse_errors = errors;
        }
    }
    let metadata = canonical_session_meta(&lines)?;
    if parse_errors != 0 && (strict || metadata.meta.history_mode != ThreadHistoryMode::Legacy) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid referenced rollout record",
        ));
    }
    Ok(lines)
}

fn decoded_prefix<'a>(
    path: &Path,
    physical_bytes: &'a [u8],
    end: Option<u64>,
) -> io::Result<std::borrow::Cow<'a, [u8]>> {
    if compression::plain_rollout_path(path) == path {
        return Ok(std::borrow::Cow::Borrowed(physical_bytes));
    }
    let mut reader =
        zstd::stream::read::Decoder::new(physical_bytes)?.take(end.unwrap_or(u64::MAX));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(std::borrow::Cow::Owned(bytes))
}

fn conflict(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}

#[cfg(test)]
#[path = "external_legacy_capture_tests.rs"]
mod tests;
