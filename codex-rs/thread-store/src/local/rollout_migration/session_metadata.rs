//! Preserves the runtime version recorded after the original Legacy session header.

use std::path::Path;
use std::sync::LazyLock;

use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use regex::bytes::Regex;

use super::MAX_ROLLOUT_LINE_BYTES;
use super::line_parser;
use super::migration_error;
use crate::ThreadStoreResult;

// Unicode escapes can spell either the discriminant key or its value. Other
// records need no payload decode; matches inside message text are harmless.
#[expect(
    clippy::expect_used,
    reason = "the constant candidate regex is exercised by the session metadata tests"
)]
static METADATA_CANDIDATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"session_meta|\\u"#).expect("metadata candidate expression"));

pub(super) async fn canonical_session_meta(path: &Path) -> ThreadStoreResult<RolloutLine> {
    let mut reader = super::open_migration_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut canonical: Option<RolloutLine> = None;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.len() > MAX_ROLLOUT_LINE_BYTES || !METADATA_CANDIDATE.is_match(raw.as_bytes()) {
            continue;
        }
        let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
            continue;
        };
        let RolloutItem::SessionMeta(metadata) = &line.item else {
            continue;
        };
        if let Some(RolloutLine {
            item: RolloutItem::SessionMeta(head),
            ..
        }) = canonical.as_mut()
        {
            // InitialHistory::get_multi_agent_version uses the newest explicit
            // version for this logical thread, even when its header predates it.
            // Other header fields still belong to the original SessionMeta.
            if metadata.meta.id == head.meta.id && metadata.meta.multi_agent_version.is_some() {
                head.meta.multi_agent_version = metadata.meta.multi_agent_version;
            }
        } else {
            canonical = Some(line);
        }
    }
    canonical.ok_or_else(|| migration_error("rollout contains no session metadata"))
}

#[cfg(test)]
#[path = "session_metadata_tests.rs"]
mod tests;
