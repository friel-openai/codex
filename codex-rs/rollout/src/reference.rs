//! Strict resolution and materialization of rollout-reference graphs.

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;

use crate::ARCHIVED_SESSIONS_SUBDIR;
use crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR;
use crate::SESSIONS_SUBDIR;
use crate::compression;
use crate::recorder::RolloutRecorder;

/// The global graph-depth bound used by correctness-critical rollout readers.
///
/// The observed long-lived thread contains 116 linear segments. A limit of 256
/// provides more than twice that capacity while still bounding recursive polling.
pub const MAX_ROLLOUT_REFERENCE_DEPTH: usize = 256;

/// Selects whether reference expansion follows the complete graph or only the recent segment
/// window used for replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaterializationPolicy {
    Complete,
    RecentSegments,
    OrdinaryReferenceLimit(usize),
}

/// Tracks structural recursion and recent-segment depth independently while expanding references.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpansionCursor {
    graph_depth: usize,
    ordinary_reference_depth: usize,
    current_thread_id: ThreadId,
}

impl MaterializationPolicy {
    fn ordinary_reference_limit(
        self,
        reference: &RolloutReferenceItem,
        current_thread_id: ThreadId,
    ) -> Option<usize> {
        if is_fork_boundary_reference(reference, current_thread_id) {
            return None;
        }
        match self {
            Self::Complete => None,
            Self::RecentSegments => Some(reference.max_depth.min(DEFAULT_ROLLOUT_REFERENCE_DEPTH)),
            Self::OrdinaryReferenceLimit(limit) => Some(limit),
        }
    }
}

/// A bounded logical rollout prefix and whether an older ordinary reference was omitted.
pub struct BoundedRolloutLines {
    /// Materialized lines in logical rollout order.
    pub lines: Vec<RolloutLine>,
    /// True when increasing the ordinary-reference limit can reveal older history.
    pub has_older_reference: bool,
}

/// The immutable identity recorded by a rollout reference.
///
/// Rollouts created before segment IDs were introduced use the single `initial` segment for a
/// thread. `LegacyInitial` preserves that identity without treating a missing segment ID as a
/// wildcard for newer segments of the same thread.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ReferenceIdentity {
    Segment {
        thread_id: ThreadId,
        segment_id: SegmentId,
    },
    LegacyInitial {
        thread_id: ThreadId,
    },
}

impl ReferenceIdentity {
    fn thread_id(self) -> ThreadId {
        match self {
            Self::Segment { thread_id, .. } | Self::LegacyInitial { thread_id } => thread_id,
        }
    }

    fn segment_key(self) -> String {
        match self {
            Self::Segment { segment_id, .. } => segment_id.to_string(),
            Self::LegacyInitial { .. } => "initial".to_string(),
        }
    }

    fn description(self) -> String {
        format!("{}/{}", self.thread_id(), self.segment_key())
    }
}

/// Resolves a reference only when a candidate rollout has the recorded immutable identity.
pub async fn resolve_rollout_reference_path(
    codex_home: &Path,
    reference: &RolloutReferenceItem,
) -> io::Result<PathBuf> {
    let identity = reference_identity(reference)?;

    if let Some(path) = validated_candidate(reference.rollout_path.as_path(), identity).await? {
        return Ok(path);
    }

    let expected_file_name = matches!(identity, ReferenceIdentity::LegacyInitial { .. })
        .then(|| compression::plain_rollout_path(reference.rollout_path.as_path()))
        .and_then(|path| path.file_name().map(ToOwned::to_owned));
    let mut legacy_candidates = Vec::new();
    for root in [ROTATED_ROLLOUT_SEGMENTS_SUBDIR, ARCHIVED_SESSIONS_SUBDIR] {
        let directory = codex_home
            .join(root)
            .join(identity.thread_id().to_string())
            .join(identity.segment_key());
        if let Some(path) = find_valid_candidate_in_directory(
            directory.as_path(),
            identity,
            expected_file_name.as_deref(),
        )
        .await?
        {
            if matches!(identity, ReferenceIdentity::LegacyInitial { .. }) {
                legacy_candidates.push(path);
            } else {
                return Ok(path);
            }
        }
    }
    match legacy_candidates.as_slice() {
        [path] => return Ok(path.clone()),
        [_, _, ..] => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "rollout reference {} is ambiguous across immutable segment directories",
                    identity.description()
                ),
            ));
        }
        [] => {}
    }

    if let ReferenceIdentity::Segment {
        thread_id,
        segment_id: _,
    } = identity
        && let Some(rollout_timestamp) = reference.rollout_timestamp.as_deref()
    {
        let file_name = format!("rollout-{rollout_timestamp}-{thread_id}.jsonl");
        if let Some(active_path) =
            rollout_path_for_timestamp_file(codex_home, rollout_timestamp, file_name.as_str())
            && let Some(path) = validated_candidate(active_path.as_path(), identity).await?
        {
            return Ok(path);
        }
        let archived_path = codex_home.join(ARCHIVED_SESSIONS_SUBDIR).join(file_name);
        if let Some(path) = validated_candidate(archived_path.as_path(), identity).await? {
            return Ok(path);
        }
    }

    let identity_description = identity.description();
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "rollout reference {identity_description} could not be resolved from {}",
            reference.rollout_path.display()
        ),
    ))
}

/// Expands a rollout graph while retaining every physical line's lineage ordinal.
pub async fn materialize_rollout_lines(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutLine>> {
    let lines = load_strict_rollout_lines(rollout_path).await?;
    let mut has_older_reference = false;
    materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::Complete,
        &mut has_older_reference,
    )
    .await
}

/// Expands already loaded root lines without rewriting the root `SessionMeta`.
pub async fn materialize_rollout_lines_from(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
) -> io::Result<Vec<RolloutLine>> {
    let mut has_older_reference = false;
    materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::Complete,
        &mut has_older_reference,
    )
    .await
}

/// Expands a rollout graph for user-visible replay while retaining physical lineage ordinals.
///
/// Direct fork-boundary references do not consume the segment window. Ordinary compaction
/// references nested beneath the fork boundary do, which keeps a fork's inherited prefix equal to
/// the source thread's bounded replay prefix.
pub async fn materialize_recent_rollout_lines(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutLine>> {
    let lines = load_strict_rollout_lines(rollout_path).await?;
    materialize_recent_rollout_lines_from(codex_home, lines).await
}

/// Expands already loaded root lines using the recent-segment replay policy.
pub async fn materialize_recent_rollout_lines_from(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
) -> io::Result<Vec<RolloutLine>> {
    let mut has_older_reference = false;
    materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::RecentSegments,
        &mut has_older_reference,
    )
    .await
}

/// Expands at most `ordinary_reference_limit` same-thread predecessor references.
///
/// Direct cross-thread fork references do not consume the limit. Callers can increase the limit
/// until they have enough coherent history while avoiding complete expansion of a long segment
/// chain.
pub async fn materialize_bounded_rollout_lines(
    codex_home: &Path,
    rollout_path: &Path,
    ordinary_reference_limit: usize,
) -> io::Result<BoundedRolloutLines> {
    let lines = load_strict_rollout_lines(rollout_path).await?;
    materialize_bounded_rollout_lines_from(codex_home, lines, ordinary_reference_limit).await
}

async fn materialize_bounded_rollout_lines_from(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
    ordinary_reference_limit: usize,
) -> io::Result<BoundedRolloutLines> {
    let mut has_older_reference = false;
    let lines = materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::OrdinaryReferenceLimit(ordinary_reference_limit),
        &mut has_older_reference,
    )
    .await?;
    Ok(BoundedRolloutLines {
        lines,
        has_older_reference,
    })
}

async fn materialize_rollout_lines_from_with_policy(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
    policy: MaterializationPolicy,
    has_older_reference: &mut bool,
) -> io::Result<Vec<RolloutLine>> {
    let root_thread_id = canonical_session_meta(&lines)?.meta.id;
    let mut active_segments = HashSet::new();

    let mut materialized = Vec::with_capacity(lines.len());
    materialized.push(lines[0].clone());
    materialized.extend(
        expand_lines(
            codex_home,
            lines.into_iter().skip(1).collect(),
            &mut active_segments,
            ExpansionCursor {
                graph_depth: 0,
                ordinary_reference_depth: 0,
                current_thread_id: root_thread_id,
            },
            /*inherited_filter_texts*/ None,
            policy,
            has_older_reference,
        )
        .await?,
    );
    Ok(materialized)
}

/// Expands a rollout graph and discards only physical line metadata.
pub async fn materialize_rollout_items(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutItem>> {
    Ok(materialize_rollout_lines(codex_home, rollout_path)
        .await?
        .into_iter()
        .map(|line| line.item)
        .collect())
}

/// Expands a rollout graph for user-visible replay and discards physical line metadata.
pub async fn materialize_recent_rollout_items(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutItem>> {
    Ok(materialize_recent_rollout_lines(codex_home, rollout_path)
        .await?
        .into_iter()
        .map(|line| line.item)
        .collect())
}

fn expand_lines<'a>(
    codex_home: &'a Path,
    lines: Vec<RolloutLine>,
    active_segments: &'a mut HashSet<ReferenceIdentity>,
    cursor: ExpansionCursor,
    inherited_filter_texts: Option<&'a [String]>,
    policy: MaterializationPolicy,
    has_older_reference: &'a mut bool,
) -> Pin<Box<dyn Future<Output = io::Result<Vec<RolloutLine>>> + Send + 'a>> {
    Box::pin(async move {
        let mut materialized = Vec::with_capacity(lines.len());
        for mut line in lines {
            match &mut line.item {
                RolloutItem::SessionMeta(_) => {}
                RolloutItem::RolloutReference(reference) => {
                    let reference = reference.clone();
                    if policy
                        .ordinary_reference_limit(&reference, cursor.current_thread_id)
                        .is_some_and(|limit| cursor.ordinary_reference_depth >= limit)
                    {
                        *has_older_reference = true;
                        continue;
                    }
                    if cursor.graph_depth >= MAX_ROLLOUT_REFERENCE_DEPTH {
                        return Err(io::Error::other(format!(
                            "rollout reference graph exceeds maximum depth of \
                             {MAX_ROLLOUT_REFERENCE_DEPTH}"
                        )));
                    }
                    let identity = reference_identity(&reference)?;
                    if !active_segments.insert(identity) {
                        let identity_description = identity.description();
                        return Err(io::Error::other(format!(
                            "rollout reference cycle detected at {identity_description}"
                        )));
                    }

                    let path = resolve_rollout_reference_path(codex_home, &reference).await?;
                    let referenced_lines = load_strict_rollout_lines(path.as_path()).await?;
                    let referenced_meta = canonical_session_meta(&referenced_lines)?;
                    validate_identity(referenced_meta, identity, path.as_path())?;
                    let referenced_thread_id = referenced_meta.meta.id;

                    let filter_texts = inherited_filter_texts.or(reference
                        .compacted_replacement_history_filter_texts
                        .as_deref());
                    let next_ordinary_reference_depth =
                        if is_fork_boundary_reference(&reference, cursor.current_thread_id) {
                            cursor.ordinary_reference_depth
                        } else {
                            cursor.ordinary_reference_depth + 1
                        };
                    let mut expanded = expand_lines(
                        codex_home,
                        referenced_lines.into_iter().skip(1).collect(),
                        active_segments,
                        ExpansionCursor {
                            graph_depth: cursor.graph_depth + 1,
                            ordinary_reference_depth: next_ordinary_reference_depth,
                            current_thread_id: referenced_thread_id,
                        },
                        filter_texts,
                        policy,
                        has_older_reference,
                    )
                    .await?;
                    active_segments.remove(&identity);

                    if let Some(filter_texts) = filter_texts {
                        apply_filter(&mut expanded, filter_texts);
                    }
                    if let Some(nth_user_message) = reference.nth_user_message {
                        truncate_before_nth_user_message(&mut expanded, nth_user_message);
                    }
                    materialized.extend(expanded);
                }
                _ => {
                    if inherited_filter_texts
                        .is_none_or(|filter_texts| filter_line(&mut line, filter_texts))
                    {
                        materialized.push(line);
                    }
                }
            }
        }
        Ok(materialized)
    })
}

fn is_fork_boundary_reference(
    reference: &RolloutReferenceItem,
    current_thread_id: ThreadId,
) -> bool {
    reference.nth_user_message.is_some() || reference.thread_id != Some(current_thread_id)
}

fn reference_identity(reference: &RolloutReferenceItem) -> io::Result<ReferenceIdentity> {
    let thread_id = reference.thread_id.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout reference {} is missing thread_id",
                reference.rollout_path.display()
            ),
        )
    })?;
    Ok(match reference.segment_id {
        Some(segment_id) => ReferenceIdentity::Segment {
            thread_id,
            segment_id,
        },
        None => ReferenceIdentity::LegacyInitial { thread_id },
    })
}

async fn validated_candidate(
    path: &Path,
    identity: ReferenceIdentity,
) -> io::Result<Option<PathBuf>> {
    let Some(path) = compression::existing_rollout_path(path).await else {
        return Ok(None);
    };
    let meta = match read_candidate_session_meta(path.as_path()).await {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    Ok(identity_matches(&meta, identity).then_some(path))
}

async fn read_candidate_session_meta(path: &Path) -> io::Result<SessionMetaLine> {
    let mut reader = compression::open_rollout_line_reader(path).await?;
    let Some(first_line) = reader.next_line().await? else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("rollout at {} is empty", path.display()),
        ));
    };
    let line = serde_json::from_str::<RolloutLine>(first_line.as_str()).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout at {} has invalid session metadata: {err}",
                path.display()
            ),
        )
    })?;
    match line.item {
        RolloutItem::SessionMeta(meta) => Ok(meta),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout at {} does not start with session metadata",
                path.display()
            ),
        )),
    }
}

async fn find_valid_candidate_in_directory(
    directory: &Path,
    identity: ReferenceIdentity,
    expected_file_name: Option<&std::ffi::OsStr>,
) -> io::Result<Option<PathBuf>> {
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let mut candidates = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let Some(rollout_file) = compression::RolloutFile::from_path(entry.path()) else {
            continue;
        };
        if expected_file_name.is_some_and(|expected_file_name| {
            std::ffi::OsStr::new(rollout_file.plain_file_name()) != expected_file_name
        }) {
            continue;
        }
        if let Some(path) = validated_candidate(rollout_file.path(), identity).await?
            && !candidates.contains(&path)
        {
            candidates.push(path);
        }
    }
    candidates.sort();
    match candidates.as_slice() {
        [] => Ok(None),
        [path] => Ok(Some(path.clone())),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout reference {} is ambiguous in {}: {} matching files",
                identity.description(),
                directory.display(),
                candidates.len()
            ),
        )),
    }
}

async fn load_strict_rollout_lines(path: &Path) -> io::Result<Vec<RolloutLine>> {
    let (lines, _thread_id, parse_errors) = RolloutRecorder::load_rollout_lines(path).await?;
    if parse_errors != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout at {} contains {parse_errors} invalid record(s)",
                path.display()
            ),
        ));
    }
    canonical_session_meta(&lines)?;
    Ok(lines)
}

fn canonical_session_meta(lines: &[RolloutLine]) -> io::Result<&SessionMetaLine> {
    match lines.first().map(|line| &line.item) {
        Some(RolloutItem::SessionMeta(meta)) => Ok(meta),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rollout does not start with session metadata",
        )),
    }
}

fn validate_identity(
    meta: &SessionMetaLine,
    identity: ReferenceIdentity,
    path: &Path,
) -> io::Result<()> {
    if identity_matches(meta, identity) {
        return Ok(());
    }
    let identity_description = identity.description();
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "rollout at {} does not match reference {identity_description}",
            path.display()
        ),
    ))
}

fn identity_matches(meta: &SessionMetaLine, identity: ReferenceIdentity) -> bool {
    match identity {
        ReferenceIdentity::Segment {
            thread_id,
            segment_id,
        } => meta.meta.id == thread_id && meta.meta.segment_id == Some(segment_id),
        ReferenceIdentity::LegacyInitial { thread_id } => {
            meta.meta.id == thread_id && meta.meta.segment_id.is_none()
        }
    }
}

fn rollout_path_for_timestamp_file(
    codex_home: &Path,
    rollout_timestamp: &str,
    file_name: &str,
) -> Option<PathBuf> {
    Some(
        codex_home
            .join(SESSIONS_SUBDIR)
            .join(rollout_timestamp.get(0..4)?)
            .join(rollout_timestamp.get(5..7)?)
            .join(rollout_timestamp.get(8..10)?)
            .join(file_name),
    )
}

fn apply_filter(lines: &mut Vec<RolloutLine>, filter_texts: &[String]) {
    lines.retain_mut(|line| filter_line(line, filter_texts));
}

fn filter_line(line: &mut RolloutLine, filter_texts: &[String]) -> bool {
    match &mut line.item {
        RolloutItem::Compacted(compacted) => {
            if let Some(replacement_history) = compacted.replacement_history.as_mut() {
                replacement_history
                    .retain(|item| !matches_filtered_developer_message(item, filter_texts));
            }
            true
        }
        RolloutItem::ResponseItem(item) => !matches_filtered_developer_message(item, filter_texts),
        RolloutItem::SessionMeta(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::InterAgentCommunication(_)
        | RolloutItem::InterAgentCommunicationMetadata { .. }
        | RolloutItem::TurnContext(_)
        | RolloutItem::WorldState(_)
        | RolloutItem::EventMsg(_) => true,
    }
}

fn matches_filtered_developer_message(item: &ResponseItem, filter_texts: &[String]) -> bool {
    let ResponseItem::Message { role, content, .. } = item else {
        return false;
    };
    let [ContentItem::InputText { text }] = content.as_slice() else {
        return false;
    };
    role == "developer" && filter_texts.iter().any(|filter_text| filter_text == text)
}

fn truncate_before_nth_user_message(lines: &mut Vec<RolloutLine>, nth_user_message: usize) {
    if nth_user_message == usize::MAX {
        return;
    }
    let mut event_user_positions = Vec::new();
    let mut response_user_positions = Vec::new();
    // A canonical persisted turn begins at `TurnStarted`, before its user response item. Record
    // that boundary so truncation cannot retain the opening event for an excluded turn.
    let mut active_turn_start = None;
    for (index, line) in lines.iter().enumerate() {
        match &line.item {
            RolloutItem::EventMsg(EventMsg::TurnStarted(_)) => {
                active_turn_start = Some(index);
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                event_user_positions.push(active_turn_start.unwrap_or(index));
            }
            RolloutItem::ResponseItem(item) if item.is_user_message() => {
                response_user_positions.push(active_turn_start.unwrap_or(index));
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(_) | EventMsg::TurnAborted(_)) => {
                active_turn_start = None;
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                let count = usize::try_from(rollback.num_turns).unwrap_or(usize::MAX);
                event_user_positions.truncate(event_user_positions.len().saturating_sub(count));
                response_user_positions
                    .truncate(response_user_positions.len().saturating_sub(count));
            }
            RolloutItem::SessionMeta(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::ResponseItem(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::Compacted(_)
            | RolloutItem::TurnContext(_)
            | RolloutItem::WorldState(_)
            | RolloutItem::EventMsg(_) => {}
        }
    }
    // Canonical rollouts contain one `UserMessage` event per real user turn. Prefer those events
    // because model-visible contextual fragments also use `role: "user"`. The response-item
    // fallback preserves reference truncation for older and synthetic rollouts without events.
    let user_positions = if event_user_positions.is_empty() {
        response_user_positions
    } else {
        event_user_positions
    };
    if let Some(cutoff) = user_positions.get(nth_user_message).copied() {
        lines.truncate(cutoff);
    }
}

#[cfg(test)]
#[path = "reference_tests.rs"]
mod tests;
