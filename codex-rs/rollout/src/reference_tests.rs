use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

use super::ExpansionCursor;
use super::MAX_ROLLOUT_REFERENCE_DEPTH;
use super::MaterializationPolicy;
use super::expand_lines;
use super::materialize_bounded_rollout_lines;
use super::materialize_recent_rollout_lines;
use super::materialize_rollout_lines;
use super::resolve_rollout_reference_path;
use crate::ARCHIVED_SESSIONS_SUBDIR;
use crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR;

fn meta_line(thread_id: ThreadId, segment_id: SegmentId, ordinal: u64) -> RolloutLine {
    meta_line_with_segment(thread_id, Some(segment_id), ordinal)
}

fn legacy_meta_line(thread_id: ThreadId, ordinal: u64) -> RolloutLine {
    meta_line_with_segment(thread_id, /*segment_id*/ None, ordinal)
}

fn meta_line_with_segment(
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    ordinal: u64,
) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:00Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id,
                timestamp: "2026-07-13T00:00:00Z".to_string(),
                cwd: PathBuf::from("/tmp"),
                originator: "test".to_string(),
                cli_version: "test".to_string(),
                source: SessionSource::Exec,
                ..SessionMeta::default()
            },
            git: None,
        }),
    }
}

fn agent_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            message: message.to_string(),
            phase: None,
            memory_citation: None,
        })),
    }
}

fn user_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::ResponseItem(ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: message.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }),
    }
}

fn user_event_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: message.to_string(),
            ..Default::default()
        })),
    }
}

fn turn_started_line(turn_id: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_id.to_string(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })),
    }
}

fn turn_complete_line(turn_id: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: turn_id.to_string(),
            started_at: None,
            last_agent_message: None,
            error: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        })),
    }
}

fn reference_line(
    path: PathBuf,
    thread_id: ThreadId,
    segment_id: SegmentId,
    ordinal: u64,
) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:02Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: path,
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(segment_id),
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    }
}

fn legacy_reference_line(path: PathBuf, thread_id: ThreadId, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:02Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: path,
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    }
}

fn write_rollout(path: &Path, lines: &[RolloutLine]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut jsonl = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    jsonl.push('\n');
    fs::write(path, jsonl)
}

fn rollout_file_name(timestamp: &str, thread_id: ThreadId) -> String {
    format!("rollout-{timestamp}-{thread_id}.jsonl")
}

fn event_messages(lines: &[RolloutLine]) -> Vec<&str> {
    lines
        .iter()
        .filter_map(|line| match &line.item {
            RolloutItem::EventMsg(EventMsg::AgentMessage(event)) => Some(event.message.as_str()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn nth_user_message_excludes_corresponding_turn_started_and_suffix() -> io::Result<()> {
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = home.path().join("source.jsonl");
    write_rollout(
        source_path.as_path(),
        &[
            meta_line(source_thread, source_segment, /*ordinal*/ 0),
            agent_line("inherited", /*ordinal*/ 1),
            turn_started_line("retained-turn", /*ordinal*/ 2),
            user_line("retained user", /*ordinal*/ 3),
            agent_line("retained answer", /*ordinal*/ 4),
            turn_complete_line("retained-turn", /*ordinal*/ 5),
            turn_started_line("turn-at-boundary", /*ordinal*/ 6),
            user_line("fork boundary", /*ordinal*/ 7),
            agent_line("after boundary", /*ordinal*/ 8),
        ],
    )?;

    let root_thread = ThreadId::new();
    let root_segment = SegmentId::new();
    let root_path = home.path().join("root.jsonl");
    let mut reference_line = reference_line(
        source_path,
        source_thread,
        source_segment,
        /*ordinal*/ 12,
    );
    let RolloutItem::RolloutReference(reference) = &mut reference_line.item else {
        unreachable!();
    };
    reference.nth_user_message = Some(1);
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(root_thread, root_segment, /*ordinal*/ 11),
            reference_line,
            agent_line("local", /*ordinal*/ 13),
        ],
    )?;

    let lines = materialize_rollout_lines(home.path(), root_path.as_path()).await?;
    let RolloutItem::SessionMeta(meta) = &lines[0].item else {
        panic!("expected root session metadata");
    };
    assert_eq!(meta.meta.id, root_thread);
    assert_eq!(
        event_messages(&lines),
        vec!["inherited", "retained answer", "local"]
    );
    assert!(lines.iter().all(|line| !matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::TurnStarted(event))
            if event.turn_id == "turn-at-boundary"
    )));
    assert_eq!(
        lines.iter().map(|line| line.ordinal).collect::<Vec<_>>(),
        vec![
            Some(11),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(13)
        ]
    );
    assert!(
        lines
            .iter()
            .all(|line| !matches!(&line.item, RolloutItem::RolloutReference(_)))
    );
    Ok(())
}

#[tokio::test]
async fn nth_user_message_uses_real_turn_events_instead_of_contextual_user_items() -> io::Result<()>
{
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = home.path().join("source.jsonl");
    write_rollout(
        source_path.as_path(),
        &[
            meta_line(source_thread, source_segment, /*ordinal*/ 0),
            user_line(
                "<environment_context>context</environment_context>",
                /*ordinal*/ 1,
            ),
            turn_started_line("retained-turn", /*ordinal*/ 2),
            user_event_line("retained user", /*ordinal*/ 3),
            user_line("retained user", /*ordinal*/ 4),
            turn_complete_line("retained-turn", /*ordinal*/ 5),
            turn_started_line("turn-at-boundary", /*ordinal*/ 6),
            user_event_line("fork boundary", /*ordinal*/ 7),
            user_line("fork boundary", /*ordinal*/ 8),
        ],
    )?;

    let root_thread = ThreadId::new();
    let root_segment = SegmentId::new();
    let root_path = home.path().join("root.jsonl");
    let mut reference_line = reference_line(
        source_path,
        source_thread,
        source_segment,
        /*ordinal*/ 10,
    );
    let RolloutItem::RolloutReference(reference) = &mut reference_line.item else {
        unreachable!();
    };
    reference.nth_user_message = Some(1);
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(root_thread, root_segment, /*ordinal*/ 9),
            reference_line,
        ],
    )?;

    let lines = materialize_rollout_lines(home.path(), root_path.as_path()).await?;
    assert!(lines.iter().any(|line| {
        matches!(
            &line.item,
            RolloutItem::ResponseItem(ResponseItem::Message { role, content, .. })
                if role == "user"
                    && matches!(
                        content.as_slice(),
                        [ContentItem::InputText { text }] if text == "retained user"
                    )
        )
    }));
    assert!(lines.iter().all(|line| !matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::TurnStarted(event))
            if event.turn_id == "turn-at-boundary"
    )));
    Ok(())
}

#[tokio::test]
async fn thread_summary_uses_inherited_reference_preview() -> io::Result<()> {
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(source_thread.to_string())
        .join(source_segment.to_string())
        .join("source.jsonl");
    write_rollout(
        source_path.as_path(),
        &[
            meta_line(source_thread, source_segment, /*ordinal*/ 0),
            user_event_line("inherited preview", /*ordinal*/ 1),
        ],
    )?;

    let child_thread = ThreadId::new();
    let child_segment = SegmentId::new();
    let child_path = home
        .path()
        .join(crate::SESSIONS_SUBDIR)
        .join("2026/07/13")
        .join(rollout_file_name("2026-07-13T00-00-01", child_thread));
    write_rollout(
        child_path.as_path(),
        &[
            meta_line(child_thread, child_segment, /*ordinal*/ 2),
            reference_line(
                source_path,
                source_thread,
                source_segment,
                /*ordinal*/ 3,
            ),
        ],
    )?;

    let summary = crate::list::read_thread_item_from_rollout(child_path)
        .await
        .expect("referenced thread should remain discoverable");
    assert_eq!(summary.preview.as_deref(), Some("inherited preview"));
    assert_eq!(
        summary.first_user_message.as_deref(),
        Some("inherited preview")
    );
    Ok(())
}

#[tokio::test]
async fn materialization_accepts_legacy_turn_context_file_uri_cwd() -> io::Result<()> {
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = home.path().join("source.jsonl");
    let source_meta = serde_json::to_string(&meta_line(
        source_thread,
        source_segment,
        /*ordinal*/ 0,
    ))?;
    let (legacy_cwd, expected_cwd) = if cfg!(windows) {
        ("file:///C:/tmp", Path::new(r"C:\tmp"))
    } else {
        ("file:///tmp", Path::new("/tmp"))
    };
    let legacy_turn_context = json!({
        "timestamp": "2026-07-13T00:00:01Z",
        "ordinal": 1,
        "type": "turn_context",
        "payload": {
            "cwd": legacy_cwd,
            "approval_policy": "never",
            "sandbox_policy": { "type": "danger-full-access" },
            "model": "gpt-5",
            "summary": "auto"
        }
    });
    fs::write(
        source_path.as_path(),
        format!("{source_meta}\n{legacy_turn_context}\n"),
    )?;

    let root_thread = ThreadId::new();
    let root_segment = SegmentId::new();
    let root_path = home.path().join("root.jsonl");
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(root_thread, root_segment, /*ordinal*/ 2),
            reference_line(
                source_path,
                source_thread,
                source_segment,
                /*ordinal*/ 3,
            ),
        ],
    )?;

    let lines = materialize_rollout_lines(home.path(), root_path.as_path()).await?;
    let turn_context = lines
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::TurnContext(turn_context) => Some(turn_context),
            _ => None,
        })
        .expect("legacy turn context should be preserved");
    assert_eq!(turn_context.cwd.as_path(), expected_cwd);
    assert_eq!(
        serde_json::to_value(turn_context)?["cwd"],
        json!(expected_cwd)
    );
    Ok(())
}

#[tokio::test]
async fn resolver_rejects_missing_and_mismatched_segments() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let missing = RolloutReferenceItem {
        rollout_path: home.path().join("missing.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &missing)
            .await
            .expect_err("missing reference should fail")
            .kind(),
        io::ErrorKind::NotFound
    );

    let mismatch_path = home.path().join("mismatch.jsonl");
    write_rollout(
        mismatch_path.as_path(),
        &[meta_line(
            ThreadId::new(),
            SegmentId::new(),
            /*ordinal*/ 0,
        )],
    )?;
    let mismatch = RolloutReferenceItem {
        rollout_path: mismatch_path,
        ..missing
    };
    assert!(
        resolve_rollout_reference_path(home.path(), &mismatch)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn resolver_uses_validated_rotated_compressed_segment() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let segment_dir = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string());
    let plain_path = segment_dir.join("rollout-segment.jsonl");
    write_rollout(
        plain_path.as_path(),
        &[
            meta_line(thread_id, segment_id, /*ordinal*/ 0),
            agent_line("compressed", /*ordinal*/ 1),
        ],
    )?;
    let compressed_path = plain_path.with_extension("jsonl.zst");
    let encoded = zstd::stream::encode_all(fs::File::open(&plain_path)?, 1)?;
    fs::write(&compressed_path, encoded)?;
    fs::remove_file(&plain_path)?;

    let reference = RolloutReferenceItem {
        rollout_path: home.path().join("stale.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        compressed_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_accepts_matching_recorded_path() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let recorded_path = home.path().join("legacy.jsonl");
    write_rollout(
        recorded_path.as_path(),
        &[
            legacy_meta_line(thread_id, /*ordinal*/ 0),
            agent_line("legacy", /*ordinal*/ 1),
        ],
    )?;
    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(recorded_path.clone(), thread_id, /*ordinal*/ 2).item
    else {
        unreachable!();
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        recorded_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_uses_initial_after_stable_path_is_replaced() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let file_name = rollout_file_name("2026-07-13T00-00-00", thread_id);
    let stable_path = home.path().join(file_name.as_str());
    write_rollout(
        stable_path.as_path(),
        &[
            meta_line(thread_id, SegmentId::new(), /*ordinal*/ 0),
            agent_line("new", /*ordinal*/ 1),
        ],
    )?;
    let initial_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join("initial")
        .join(file_name);
    write_rollout(
        initial_path.as_path(),
        &[
            legacy_meta_line(thread_id, /*ordinal*/ 0),
            agent_line("legacy", /*ordinal*/ 1),
        ],
    )?;

    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(stable_path, thread_id, /*ordinal*/ 2).item
    else {
        unreachable!();
    };
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        initial_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_rejects_replaced_stable_path_without_initial() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let stable_path = home.path().join("stable.jsonl");
    write_rollout(
        stable_path.as_path(),
        &[meta_line(thread_id, SegmentId::new(), /*ordinal*/ 0)],
    )?;
    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(stable_path, thread_id, /*ordinal*/ 1).item
    else {
        unreachable!();
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("a replacement segment must not satisfy a legacy reference")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_resolves_archived_initial_segment() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let archived_path = home
        .path()
        .join(ARCHIVED_SESSIONS_SUBDIR)
        .join(thread_id.to_string())
        .join("initial")
        .join(rollout_file_name("2026-07-13T00-00-00", thread_id));
    write_rollout(
        archived_path.as_path(),
        &[
            legacy_meta_line(thread_id, /*ordinal*/ 0),
            agent_line("archived", /*ordinal*/ 1),
        ],
    )?;
    let RolloutItem::RolloutReference(reference) = legacy_reference_line(
        home.path()
            .join("elsewhere")
            .join(rollout_file_name("2026-07-13T00-00-00", thread_id)),
        thread_id,
        /*ordinal*/ 2,
    )
    .item
    else {
        unreachable!();
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        archived_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_rejects_ambiguous_initial_segments() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let initial_directory = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join("initial");
    for file_name in [
        rollout_file_name("2026-07-13T00-00-00", thread_id),
        rollout_file_name("2026-07-13T00-01-00", thread_id),
    ] {
        write_rollout(
            initial_directory.join(file_name).as_path(),
            &[legacy_meta_line(thread_id, /*ordinal*/ 0)],
        )?;
    }
    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(PathBuf::new(), thread_id, /*ordinal*/ 1).item
    else {
        unreachable!();
    };

    let error = resolve_rollout_reference_path(home.path(), &reference)
        .await
        .expect_err("multiple legacy initial segments must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("ambiguous"));
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_reference_cycles() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_a = ThreadId::new();
    let segment_a = SegmentId::new();
    let thread_b = ThreadId::new();
    let segment_b = SegmentId::new();
    let path_a = home.path().join("a.jsonl");
    let path_b = home.path().join("b.jsonl");
    write_rollout(
        path_a.as_path(),
        &[
            meta_line(thread_a, segment_a, /*ordinal*/ 0),
            reference_line(path_b.clone(), thread_b, segment_b, /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        path_b.as_path(),
        &[
            meta_line(thread_b, segment_b, /*ordinal*/ 0),
            reference_line(path_a.clone(), thread_a, segment_a, /*ordinal*/ 1),
        ],
    )?;

    let error = materialize_rollout_lines(home.path(), path_a.as_path())
        .await
        .err()
        .expect("cycle should fail");
    assert!(error.to_string().contains("cycle"));
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_legacy_reference_cycles() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_a = ThreadId::new();
    let thread_b = ThreadId::new();
    let path_a = home.path().join("legacy-a.jsonl");
    let path_b = home.path().join("legacy-b.jsonl");
    write_rollout(
        path_a.as_path(),
        &[
            legacy_meta_line(thread_a, /*ordinal*/ 0),
            legacy_reference_line(path_b.clone(), thread_b, /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        path_b.as_path(),
        &[
            legacy_meta_line(thread_b, /*ordinal*/ 0),
            legacy_reference_line(path_a.clone(), thread_a, /*ordinal*/ 1),
        ],
    )?;

    let error = materialize_rollout_lines(home.path(), path_a.as_path())
        .await
        .err()
        .expect("legacy cycle should fail");
    assert!(error.to_string().contains("cycle"));
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_depth_exhaustion() -> io::Result<()> {
    let home = TempDir::new()?;
    let referenced_thread = ThreadId::new();
    let referenced_segment = SegmentId::new();
    let mut has_older_reference = false;
    let error = expand_lines(
        home.path(),
        vec![reference_line(
            home.path().join("unresolved.jsonl"),
            referenced_thread,
            referenced_segment,
            /*ordinal*/ 0,
        )],
        &mut HashSet::new(),
        ExpansionCursor {
            graph_depth: MAX_ROLLOUT_REFERENCE_DEPTH,
            ordinary_reference_depth: 0,
            current_thread_id: ThreadId::new(),
        },
        /*inherited_filter_texts*/ None,
        MaterializationPolicy::Complete,
        &mut has_older_reference,
    )
    .await
    .err()
    .expect("depth exhaustion should fail before resolving the reference");
    assert!(error.to_string().contains("maximum depth"));
    Ok(())
}

#[tokio::test]
async fn recent_materialization_bounds_existing_deep_reference_chains() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let deepest_segment = SegmentId::new();
    let old_segment = SegmentId::new();
    let middle_segment = SegmentId::new();
    let current_segment = SegmentId::new();
    let fork_thread = ThreadId::new();
    let fork_segment = SegmentId::new();

    let deepest_path = home.path().join("deepest.jsonl");
    let old_path = home.path().join("old.jsonl");
    let middle_path = home.path().join("middle.jsonl");
    let current_path = home.path().join("current.jsonl");
    let fork_path = home.path().join("fork.jsonl");

    let deepest_meta =
        serde_json::to_string(&meta_line(thread_id, deepest_segment, /*ordinal*/ 0))?;
    fs::write(
        deepest_path.as_path(),
        format!("{deepest_meta}\n{{malformed rollout line\n"),
    )?;

    let mut deepest_reference = reference_line(
        deepest_path.clone(),
        thread_id,
        deepest_segment,
        /*ordinal*/ 1,
    );
    let RolloutItem::RolloutReference(reference) = &mut deepest_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        old_path.as_path(),
        &[
            meta_line(thread_id, old_segment, /*ordinal*/ 2),
            deepest_reference,
            agent_line("old", /*ordinal*/ 3),
        ],
    )?;

    let mut old_reference =
        reference_line(old_path.clone(), thread_id, old_segment, /*ordinal*/ 4);
    let RolloutItem::RolloutReference(reference) = &mut old_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        middle_path.as_path(),
        &[
            meta_line(thread_id, middle_segment, /*ordinal*/ 5),
            old_reference,
            agent_line("middle", /*ordinal*/ 6),
        ],
    )?;

    let mut middle_reference = reference_line(
        middle_path.clone(),
        thread_id,
        middle_segment,
        /*ordinal*/ 7,
    );
    let RolloutItem::RolloutReference(reference) = &mut middle_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        current_path.as_path(),
        &[
            meta_line(thread_id, current_segment, /*ordinal*/ 8),
            middle_reference,
            agent_line("current", /*ordinal*/ 9),
        ],
    )?;

    let bounded = materialize_recent_rollout_lines(home.path(), current_path.as_path()).await?;
    assert_eq!(event_messages(&bounded), vec!["old", "middle", "current"]);

    let bounded_window = materialize_bounded_rollout_lines(
        home.path(),
        current_path.as_path(),
        /*ordinary_reference_limit*/ 2,
    )
    .await?;
    assert_eq!(
        event_messages(&bounded_window.lines),
        vec!["old", "middle", "current"]
    );
    assert!(bounded_window.has_older_reference);

    let complete_error = materialize_rollout_lines(home.path(), current_path.as_path())
        .await
        .err()
        .expect("complete materialization must inspect the malformed deepest segment");
    assert!(complete_error.to_string().contains("invalid record"));

    let bounded_error = materialize_recent_rollout_lines(home.path(), middle_path.as_path())
        .await
        .err()
        .expect("the malformed segment is inside the window when middle is the root");
    assert!(bounded_error.to_string().contains("invalid record"));

    let mut fork_reference = reference_line(
        current_path.clone(),
        thread_id,
        current_segment,
        /*ordinal*/ 10,
    );
    let RolloutItem::RolloutReference(reference) = &mut fork_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        fork_path.as_path(),
        &[
            meta_line(fork_thread, fork_segment, /*ordinal*/ 11),
            fork_reference,
        ],
    )?;

    let forked = materialize_recent_rollout_lines(home.path(), fork_path.as_path()).await?;
    assert_eq!(event_messages(&forked), event_messages(&bounded));
    Ok(())
}

#[tokio::test]
async fn materialization_accepts_reference_chain_longer_than_legacy_limit() -> io::Result<()> {
    const LEGACY_MAX_ROLLOUT_REFERENCE_DEPTH: usize = 64;

    let home = TempDir::new()?;
    let identities = (0..=LEGACY_MAX_ROLLOUT_REFERENCE_DEPTH + 1)
        .map(|_| (ThreadId::new(), SegmentId::new()))
        .collect::<Vec<_>>();
    let paths = identities
        .iter()
        .enumerate()
        .map(|(index, _)| home.path().join(format!("segment-{index}.jsonl")))
        .collect::<Vec<_>>();
    for index in 0..identities.len() {
        let (thread_id, segment_id) = identities[index];
        let mut lines = vec![meta_line(thread_id, segment_id, /*ordinal*/ 0)];
        if let Some((next_thread_id, next_segment_id)) = identities.get(index + 1).copied() {
            lines.push(reference_line(
                paths[index + 1].clone(),
                next_thread_id,
                next_segment_id,
                /*ordinal*/ 1,
            ));
        }
        write_rollout(paths[index].as_path(), &lines)?;
    }

    let lines = materialize_rollout_lines(home.path(), paths[0].as_path()).await?;
    assert_eq!(lines.len(), 1);
    Ok(())
}
