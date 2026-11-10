use std::io;
use std::path::Path;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use serde_json::Value;
use tracing::trace;
use tracing::warn;

use crate::RolloutItem;
use crate::RolloutLine;
use crate::RolloutRecorder;
use crate::recorder::reject_unknown_thread_history_mode;

/// The record policy shared by file loading and frozen FullHistory byte snapshots.
///
/// Damaged ordinary records do not hide later complete records. Native records keep their
/// recorded ordinals; only legacy histories may recover several envelopes from one line.
pub(crate) struct RolloutLineLoader<'a> {
    path: &'a Path,
    /// The first decoded SessionMeta identifies this physical rollout.
    thread_id: Option<ThreadId>,
    /// Suffix recovery must never invent record boundaries in paginated histories.
    history_mode: Option<ThreadHistoryMode>,
    parse_errors: usize,
    saw_non_empty_line: bool,
}

impl<'a> RolloutLineLoader<'a> {
    pub(crate) fn new(path: &'a Path) -> Self {
        Self {
            path,
            thread_id: None,
            history_mode: None,
            parse_errors: 0,
            saw_non_empty_line: false,
        }
    }

    pub(crate) fn push(
        &mut self,
        bytes: &[u8],
        mut accept: impl FnMut(RolloutLine),
    ) -> io::Result<()> {
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(());
        }
        self.saw_non_empty_line = true;
        let value: Value = match serde_json::from_slice(bytes) {
            Ok(value) => value,
            Err(error) => {
                if self.history_mode == Some(ThreadHistoryMode::Legacy)
                    && let Some(recovered) = crate::recover_legacy_jsonl_suffix(bytes)
                {
                    let recovered_record_count = recovered.values.len();
                    let recovered_lines = recovered
                        .values
                        .into_iter()
                        .map(RolloutRecorder::parse_rollout_line_value)
                        .collect::<Result<Vec<_>, _>>();
                    if let Ok(recovered_lines) = recovered_lines {
                        warn!(
                            path = %self.path.display(),
                            discarded_prefix_bytes = recovered.discarded_prefix_bytes,
                            recovered_record_count,
                            "recovered complete legacy rollout records after an invalid JSON prefix"
                        );
                        for line in recovered_lines.into_iter().flatten() {
                            accept(line);
                        }
                        return Ok(());
                    }
                }
                warn!(path = %self.path.display(), %error, "failed to parse rollout line as JSON");
                self.parse_errors = self.parse_errors.saturating_add(1);
                return Ok(());
            }
        };
        if self.thread_id.is_none() {
            // Later SessionMeta records can belong to copied fork history, not this rollout.
            reject_unknown_thread_history_mode(&value)?;
        }
        let is_rollout_reference = matches!(
            value.get("type").and_then(Value::as_str),
            Some("rollout_reference" | "fork_reference")
        );
        let line = match RolloutRecorder::parse_rollout_line_value(value) {
            Ok(Some(line)) => line,
            Ok(None) => {
                trace!("skipping legacy ghost_snapshot rollout line");
                return Ok(());
            }
            Err(error) => {
                if is_rollout_reference {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid rollout reference record",
                    ));
                }
                trace!("failed to parse rollout line: {error}");
                self.parse_errors = self.parse_errors.saturating_add(1);
                return Ok(());
            }
        };
        if self.thread_id.is_none()
            && let RolloutItem::SessionMeta(meta) = &line.item
        {
            self.thread_id = Some(meta.meta.id);
            self.history_mode = Some(meta.meta.history_mode);
        }
        accept(line);
        Ok(())
    }

    pub(crate) fn finish(self) -> io::Result<(Option<ThreadId>, usize)> {
        if !self.saw_non_empty_line {
            return Err(io::Error::other("empty session file"));
        }
        Ok((self.thread_id, self.parse_errors))
    }
}
