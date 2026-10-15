use std::fs;
use std::io;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;

use codex_history::CompactedItem;
use codex_history::RolloutItem;
use codex_history::RolloutLine;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::CapturedPrefixes;
use super::capture_external_legacy_rollout_lines;
use super::capture_external_paginated_rollout_lines;
use crate::materialize_rollout_lines;

#[track_caller]
fn assert_rollout_lines_eq(actual: &[RolloutLine], expected: &[RolloutLine]) {
    assert_eq!(
        serde_json::to_value(actual).expect("serialize actual rollout lines"),
        serde_json::to_value(expected).expect("serialize expected rollout lines")
    );
}

fn line(item: RolloutItem, ordinal: Option<u64>) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:00Z".to_string(),
        ordinal,
        item,
    }
}

fn meta_line(
    thread_id: ThreadId,
    history_mode: ThreadHistoryMode,
    ordinal: Option<u64>,
    history_base: Option<HistoryPosition>,
) -> RolloutLine {
    line(
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                timestamp: "2026-07-13T00:00:00Z".to_string(),
                history_mode,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        }),
        ordinal,
    )
}

fn legacy_meta_line(thread_id: ThreadId) -> RolloutLine {
    meta_line(thread_id, ThreadHistoryMode::Legacy, None, None)
}

fn agent_line(message: &str, ordinal: u64) -> RolloutLine {
    line(
        RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            message: message.to_string(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        })),
        Some(ordinal),
    )
}

fn message_line(role: &str, text: &str, ordinal: u64) -> RolloutLine {
    line(
        RolloutItem::ResponseItem(
            ResponseItem::Message {
                id: None,
                role: role.to_string(),
                content: vec![ContentItem::InputText {
                    text: text.to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
        Some(ordinal),
    )
}

fn compacted_line(messages: &[&str], ordinal: u64) -> RolloutLine {
    let replacement_history = messages
        .iter()
        .map(|message| {
            let RolloutItem::ResponseItem(item) = message_line("developer", message, 0).item else {
                unreachable!("message fixture");
            };
            item
        })
        .collect();
    line(
        RolloutItem::Compacted(CompactedItem {
            message: "checkpoint".to_string(),
            replacement_history: Some(replacement_history),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: Some(1),
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
        Some(ordinal),
    )
}

fn reference_line(
    path: PathBuf,
    thread_id: ThreadId,
    filter_texts: Option<Vec<String>>,
    nth_user_message: Option<usize>,
) -> RolloutLine {
    line(
        RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_id: None,
            rollout_path: path,
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message,
            compacted_replacement_history_filter_texts: filter_texts,
        }),
        None,
    )
}

fn active_path(home: &Path, thread_id: ThreadId) -> PathBuf {
    home.join(crate::SESSIONS_SUBDIR)
        .join("2026/07/13")
        .join(format!("rollout-2026-07-13T00-00-00-{thread_id}.jsonl"))
}

fn native_segment_path(home: &Path, thread_id: ThreadId, rollout_id: ThreadId) -> PathBuf {
    let path = crate::rollout_path_with_rollout_id(&active_path(home, thread_id), rollout_id)
        .expect("canonical rollout filename");
    home.join(crate::SESSIONS_SUBDIR)
        .join(crate::ROLLOUT_SEGMENTS_SUBDIR)
        .join("2026/07/13")
        .join(path.file_name().expect("rollout filename"))
}

fn write_rollout(path: &Path, lines: &[RolloutLine]) -> io::Result<()> {
    fs::create_dir_all(path.parent().expect("rollout directory"))?;
    let mut file = fs::File::create(path)?;
    for line in lines {
        serde_json::to_writer(&mut file, line)?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

fn append_line(path: &Path, line: &RolloutLine) -> io::Result<()> {
    let mut file = fs::OpenOptions::new().append(true).open(path)?;
    serde_json::to_writer(&mut file, line)?;
    file.write_all(b"\n")
}

#[tokio::test]
async fn captured_prefix_excludes_complete_records_appended_after_capture() -> io::Result<()> {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = active_path(home.path(), thread_id);
        let expected = vec![
            meta_line(
                thread_id,
                history_mode,
                (history_mode == ThreadHistoryMode::Paginated).then_some(0),
                None,
            ),
            agent_line("captured", 1),
        ];
        write_rollout(&path, &expected)?;
        let mut prefixes = CapturedPrefixes::default();
        assert_rollout_lines_eq(&prefixes.load(&path, None, false).await?, &expected);

        append_line(&path, &agent_line("appended later", 2))?;

        assert_rollout_lines_eq(&prefixes.load(&path, None, false).await?, &expected);
        prefixes.verify().await?;
        if history_mode == ThreadHistoryMode::Paginated {
            assert_eq!(prefixes.next_paginated_ordinal(&path).await?, 2);
            let (captured, next) =
                capture_external_paginated_rollout_lines(home.path(), &path, thread_id).await?;
            let mut extended = expected;
            extended.push(agent_line("appended later", 2));
            assert_rollout_lines_eq(&captured, &extended);
            assert_eq!(next, 3);
        } else {
            assert_eq!(
                capture_external_legacy_rollout_lines(home.path(), &path, thread_id)
                    .await?
                    .len(),
                3
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn captured_prefix_excludes_a_trailing_record_completed_after_capture() -> io::Result<()> {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = active_path(home.path(), thread_id);
        let expected = vec![
            meta_line(
                thread_id,
                history_mode,
                (history_mode == ThreadHistoryMode::Paginated).then_some(0),
                None,
            ),
            agent_line("complete", 1),
        ];
        write_rollout(&path, &expected)?;
        let trailing = serde_json::to_vec(&agent_line("initially incomplete", 2))?;
        let split = trailing.len() / 2;
        let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
        writer.write_all(&trailing[..split])?;
        let mut prefixes = CapturedPrefixes::default();
        assert_rollout_lines_eq(&prefixes.load(&path, None, false).await?, &expected);
        if history_mode == ThreadHistoryMode::Paginated {
            let (captured, next) =
                capture_external_paginated_rollout_lines(home.path(), &path, thread_id).await?;
            assert_rollout_lines_eq(&captured, &expected);
            assert_eq!(next, 2);
        } else {
            assert_rollout_lines_eq(
                &capture_external_legacy_rollout_lines(home.path(), &path, thread_id).await?,
                &expected,
            );
        }

        writer.write_all(&trailing[split..])?;
        writer.write_all(b"\n")?;

        assert_rollout_lines_eq(&prefixes.load(&path, None, false).await?, &expected);
        prefixes.verify().await?;
        if history_mode == ThreadHistoryMode::Paginated {
            assert_eq!(prefixes.next_paginated_ordinal(&path).await?, 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn capture_verification_rejects_a_replacement_with_changed_prefix() -> io::Result<()> {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = active_path(home.path(), thread_id);
        let metadata = meta_line(
            thread_id,
            history_mode,
            (history_mode == ThreadHistoryMode::Paginated).then_some(0),
            None,
        );
        let lines = vec![metadata.clone(), agent_line("captured", 1)];
        write_rollout(&path, &lines)?;
        let mut prefixes = CapturedPrefixes::default();
        prefixes.load(&path, None, false).await?;

        fs::rename(&path, path.with_extension("previous"))?;
        write_rollout(&path, &[metadata, agent_line("replaced", 1)])?;

        assert_eq!(
            prefixes
                .verify()
                .await
                .expect_err("changed replacement prefix must fail")
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }
    Ok(())
}

#[tokio::test]
async fn capture_verification_accepts_a_replacement_with_identical_prefix() -> io::Result<()> {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = active_path(home.path(), thread_id);
        let lines = vec![
            meta_line(
                thread_id,
                history_mode,
                (history_mode == ThreadHistoryMode::Paginated).then_some(0),
                None,
            ),
            agent_line("captured", 1),
        ];
        write_rollout(&path, &lines)?;
        let mut prefixes = CapturedPrefixes::default();
        prefixes.load(&path, None, false).await?;

        fs::rename(&path, path.with_extension("previous"))?;
        write_rollout(&path, &lines)?;
        append_line(&path, &agent_line("replacement append", 2))?;

        prefixes.verify().await?;
        assert_rollout_lines_eq(&prefixes.load(&path, None, false).await?, &lines);
        if history_mode == ThreadHistoryMode::Paginated {
            assert_eq!(prefixes.next_paginated_ordinal(&path).await?, 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn capture_verification_rejects_a_truncated_file() -> io::Result<()> {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = active_path(home.path(), thread_id);
        write_rollout(
            &path,
            &[
                meta_line(
                    thread_id,
                    history_mode,
                    (history_mode == ThreadHistoryMode::Paginated).then_some(0),
                    None,
                ),
                agent_line("captured", 1),
            ],
        )?;
        let mut prefixes = CapturedPrefixes::default();
        prefixes.load(&path, None, false).await?;
        let writer = fs::OpenOptions::new().write(true).open(&path)?;

        writer.set_len(writer.metadata()?.len() - 1)?;

        assert_eq!(
            prefixes
                .verify()
                .await
                .expect_err("truncation must fail")
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }
    Ok(())
}

#[tokio::test]
async fn capture_verification_rejects_a_same_length_prefix_mutation() -> io::Result<()> {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = active_path(home.path(), thread_id);
        let metadata = meta_line(
            thread_id,
            history_mode,
            (history_mode == ThreadHistoryMode::Paginated).then_some(0),
            None,
        );
        write_rollout(&path, &[metadata.clone(), agent_line("before", 1)])?;
        let original_len = fs::metadata(&path)?.len();
        let mut prefixes = CapturedPrefixes::default();
        prefixes.load(&path, None, false).await?;

        write_rollout(&path, &[metadata, agent_line("after!", 1)])?;

        assert_eq!(fs::metadata(&path)?.len(), original_len);
        assert_eq!(
            prefixes
                .verify()
                .await
                .expect_err("mutation must fail")
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }
    Ok(())
}

#[tokio::test]
async fn legacy_capture_recovers_complete_records_after_a_damaged_record() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let metadata = legacy_meta_line(thread_id);
    let recovered_first = agent_line("recovered first", 1);
    let recovered_second = agent_line("recovered second", 2);
    let complete = agent_line("complete later record", 3);
    write_rollout(&path, std::slice::from_ref(&metadata))?;
    let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
    writer.write_all(b"unrecoverable ordinary record\n{broken")?;
    serde_json::to_writer(&mut writer, &recovered_first)?;
    serde_json::to_writer(&mut writer, &recovered_second)?;
    writer.write_all(b"\n")?;
    append_line(&path, &complete)?;

    let captured = capture_external_legacy_rollout_lines(home.path(), &path, thread_id).await?;

    assert_rollout_lines_eq(
        &captured,
        &[metadata, recovered_first, recovered_second, complete],
    );
    assert_rollout_lines_eq(
        &captured,
        &materialize_rollout_lines(home.path(), &path).await?,
    );
    Ok(())
}

#[tokio::test]
async fn legacy_capture_includes_a_complete_final_record_without_a_newline() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let expected = vec![
        legacy_meta_line(thread_id),
        agent_line("complete final record", 1),
    ];
    write_rollout(&path, &expected)?;
    let writer = fs::OpenOptions::new().write(true).open(&path)?;
    writer.set_len(writer.metadata()?.len() - 1)?;

    assert_rollout_lines_eq(
        &capture_external_legacy_rollout_lines(home.path(), &path, thread_id).await?,
        &expected,
    );
    Ok(())
}

#[tokio::test]
async fn legacy_capture_retains_a_complete_record_before_an_incomplete_concatenated_tail()
-> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let metadata = legacy_meta_line(thread_id);
    let complete = agent_line("complete before partial tail", 1);
    write_rollout(&path, std::slice::from_ref(&metadata))?;
    let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
    serde_json::to_writer(&mut writer, &complete)?;
    let incomplete = serde_json::to_vec(&agent_line("incomplete tail", 2))?;
    writer.write_all(&incomplete[..incomplete.len() / 2])?;

    assert_rollout_lines_eq(
        &capture_external_legacy_rollout_lines(home.path(), &path, thread_id).await?,
        &[metadata, complete],
    );
    Ok(())
}

#[tokio::test]
async fn legacy_capture_recovers_concatenated_records_between_damaged_prefix_and_partial_tail()
-> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let metadata = legacy_meta_line(thread_id);
    let first = agent_line("first complete record", 1);
    let second = agent_line("second complete record", 2);
    write_rollout(&path, std::slice::from_ref(&metadata))?;
    let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
    writer.write_all(b"{broken")?;
    serde_json::to_writer(&mut writer, &first)?;
    serde_json::to_writer(&mut writer, &second)?;
    let incomplete = serde_json::to_vec(&agent_line("incomplete tail", 3))?;
    writer.write_all(&incomplete[..incomplete.len() / 2])?;

    assert_rollout_lines_eq(
        &capture_external_legacy_rollout_lines(home.path(), &path, thread_id).await?,
        &[metadata, first, second],
    );
    Ok(())
}

#[tokio::test]
async fn legacy_capture_preserves_arbitrary_precision_token_count_payload() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let metadata = legacy_meta_line(thread_id);
    write_rollout(&path, std::slice::from_ref(&metadata))?;
    let budget_units: serde_json::Number =
        serde_json::from_str("12345678901234567890.1234567890123456789")?;
    let usage = serde_json::json!({
        "input_tokens": 9_007_199_254_740_993_i64,
        "cached_input_tokens": 0,
        "cache_write_input_tokens": 0,
        "output_tokens": 1,
        "reasoning_output_tokens": 0,
        "total_tokens": 9_007_199_254_740_994_i64,
        "codex_rollout_budget_units": budget_units,
    });
    let token_count = serde_json::json!({
        "timestamp": "2026-07-13T00:00:00Z",
        "ordinal": 1,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": usage.clone(),
                "last_token_usage": usage,
                "model_context_window": 258400,
            },
            "rate_limits": {
                "primary": {
                    "used_percent": 0.0,
                    "window_minutes": 60,
                    "resets_at": 1800000000,
                },
            },
        },
    });
    let serialized = serde_json::to_string(&token_count)?;
    let expected = crate::parse_rollout_line(&serialized)?;
    let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
    writer.write_all(serialized.as_bytes())?;

    let captured = capture_external_legacy_rollout_lines(home.path(), &path, thread_id).await?;

    assert_rollout_lines_eq(&captured, &[metadata, expected]);
    let RolloutItem::EventMsg(EventMsg::TokenCount(event)) = &captured[1].item else {
        panic!("expected token count");
    };
    let info = event.info.as_ref().expect("token usage info");
    assert_eq!(info.total_token_usage.input_tokens, 9_007_199_254_740_993);
    assert_eq!(
        info.total_token_usage.codex_rollout_budget_units.as_ref(),
        Some(&budget_units)
    );
    assert_rollout_lines_eq(
        &captured,
        &materialize_rollout_lines(home.path(), &path).await?,
    );
    Ok(())
}

#[tokio::test]
async fn compressed_native_capture_decodes_only_through_its_authenticated_cutoff() -> io::Result<()>
{
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let expected = vec![
        meta_line(thread_id, ThreadHistoryMode::Paginated, Some(0), None),
        agent_line("selected compressed prefix", 1),
    ];
    write_rollout(&path, &expected)?;
    let bytes = fs::read(&path)?;
    let position = HistoryPosition {
        thread_id,
        end_ordinal_exclusive: 2,
        end_byte_offset: bytes.len() as u64,
    };
    let mut compressed = zstd::stream::encode_all(bytes.as_slice(), 0)?;
    compressed.extend_from_slice(b"invalid bytes outside the selected compressed frame");
    assert!(zstd::stream::decode_all(compressed.as_slice()).is_err());
    fs::write(path.with_extension("jsonl.zst"), compressed)?;
    fs::remove_file(&path)?;
    let mut prefixes = CapturedPrefixes::default();

    assert_rollout_lines_eq(
        &prefixes.load_history_base(home.path(), position).await?,
        &expected,
    );
    prefixes.verify().await?;
    Ok(())
}

#[tokio::test]
async fn native_capture_does_not_select_or_validate_bytes_after_its_cutoff() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let expected = vec![
        meta_line(thread_id, ThreadHistoryMode::Paginated, Some(0), None),
        agent_line("selected prefix", 1),
    ];
    write_rollout(&path, &expected)?;
    let end = fs::metadata(&path)?.len();
    append_line(&path, &agent_line("unselected original tail", 2))?;
    let mut prefixes = CapturedPrefixes::default();
    assert_rollout_lines_eq(&prefixes.load(&path, Some(end), true).await?, &expected);
    let canonical = fs::canonicalize(&path)?;
    assert_eq!(
        prefixes.files[&canonical].bytes.len(),
        usize::try_from(end).expect("test cutoff fits usize")
    );

    let mut changed_tail = expected.clone();
    changed_tail.push(agent_line("different unselected tail", 2));
    write_rollout(&path, &changed_tail)?;
    prefixes.verify().await?;
    fs::OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(end)?;
    prefixes.verify().await?;

    assert_rollout_lines_eq(&prefixes.load(&path, Some(end), true).await?, &expected);
    assert_eq!(
        prefixes.files[&canonical].bytes.len(),
        usize::try_from(end).expect("test cutoff fits usize")
    );
    Ok(())
}

#[tokio::test]
async fn native_capture_expansion_stops_at_the_original_file_length() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let prefix = vec![
        meta_line(thread_id, ThreadHistoryMode::Paginated, Some(0), None),
        agent_line("selected prefix", 1),
    ];
    write_rollout(&path, &prefix)?;
    let end = fs::metadata(&path)?.len();
    let original_tail = agent_line("original tail", 2);
    append_line(&path, &original_tail)?;
    let original_len = fs::metadata(&path)?.len();
    let mut prefixes = CapturedPrefixes::default();
    assert_rollout_lines_eq(&prefixes.load(&path, Some(end), true).await?, &prefix);

    append_line(&path, &agent_line("appended after capture", 3))?;

    let mut expected = prefix;
    expected.push(original_tail);
    assert_rollout_lines_eq(&prefixes.load(&path, None, true).await?, &expected);
    let canonical = fs::canonicalize(&path)?;
    let captured = &prefixes.files[&canonical];
    assert_eq!(captured.metadata.len(), original_len);
    assert_eq!(
        captured.bytes.len(),
        usize::try_from(original_len).expect("test length fits usize")
    );
    prefixes.verify().await?;
    Ok(())
}

#[tokio::test]
async fn nested_legacy_capture_preserves_native_cutoff_filters_and_user_cutoff() -> io::Result<()> {
    let home = TempDir::new()?;
    let native_thread = ThreadId::new();
    let predecessor_id = ThreadId::new();
    let predecessor_path = native_segment_path(home.path(), native_thread, predecessor_id);
    write_rollout(
        &predecessor_path,
        &[
            meta_line(native_thread, ThreadHistoryMode::Paginated, Some(0), None),
            message_line("developer", "inner filter", 1),
            message_line("developer", "outer filter", 2),
            message_line("developer", "retained instructions", 3),
            compacted_line(
                &["inner filter", "outer filter", "retained instructions"],
                4,
            ),
            message_line("user", "retained user", 5),
            agent_line("retained answer", 6),
        ],
    )?;
    let position = HistoryPosition {
        thread_id: predecessor_id,
        end_ordinal_exclusive: 7,
        end_byte_offset: fs::metadata(&predecessor_path)?.len(),
    };
    append_line(&predecessor_path, &agent_line("outside native cutoff", 7))?;
    let native_path = active_path(home.path(), native_thread);
    write_rollout(
        &native_path,
        &[
            meta_line(
                native_thread,
                ThreadHistoryMode::Paginated,
                Some(7),
                Some(position),
            ),
            message_line("user", "fork boundary", 8),
            agent_line("after user cutoff", 9),
        ],
    )?;
    let middle_thread = ThreadId::new();
    let middle_path = active_path(home.path(), middle_thread);
    write_rollout(
        &middle_path,
        &[
            legacy_meta_line(middle_thread),
            reference_line(
                native_path,
                native_thread,
                Some(vec!["inner filter".to_string()]),
                None,
            ),
            agent_line("middle suffix after user cutoff", 10),
        ],
    )?;
    let root_thread = ThreadId::new();
    let root_path = active_path(home.path(), root_thread);
    let root_metadata = legacy_meta_line(root_thread);
    write_rollout(
        &root_path,
        &[
            root_metadata.clone(),
            reference_line(
                middle_path,
                middle_thread,
                Some(vec!["outer filter".to_string()]),
                Some(1),
            ),
            agent_line("root suffix", 11),
        ],
    )?;

    let captured =
        capture_external_legacy_rollout_lines(home.path(), &root_path, root_thread).await?;

    assert_rollout_lines_eq(
        &captured,
        &[
            root_metadata,
            message_line("developer", "retained instructions", 3),
            compacted_line(&["retained instructions"], 4),
            message_line("user", "retained user", 5),
            agent_line("retained answer", 6),
            agent_line("root suffix", 11),
        ],
    );
    assert_rollout_lines_eq(
        &captured,
        &materialize_rollout_lines(home.path(), &root_path).await?,
    );
    Ok(())
}

#[tokio::test]
async fn legacy_capture_rejects_invalid_native_byte_and_ordinal_cutoffs() -> io::Result<()> {
    let home = TempDir::new()?;
    let native_thread = ThreadId::new();
    let predecessor_id = ThreadId::new();
    let predecessor_path = native_segment_path(home.path(), native_thread, predecessor_id);
    write_rollout(
        &predecessor_path,
        &[
            meta_line(native_thread, ThreadHistoryMode::Paginated, Some(0), None),
            agent_line("predecessor", 1),
        ],
    )?;
    let predecessor_len = fs::metadata(&predecessor_path)?.len();
    let native_path = active_path(home.path(), native_thread);
    let root_thread = ThreadId::new();
    let root_path = active_path(home.path(), root_thread);
    write_rollout(
        &root_path,
        &[
            legacy_meta_line(root_thread),
            reference_line(native_path.clone(), native_thread, None, None),
        ],
    )?;
    for (end_byte_offset, end_ordinal_exclusive) in
        [(predecessor_len - 1, 2), (predecessor_len, 99)]
    {
        write_rollout(
            &native_path,
            &[meta_line(
                native_thread,
                ThreadHistoryMode::Paginated,
                Some(2),
                Some(HistoryPosition {
                    thread_id: predecessor_id,
                    end_byte_offset,
                    end_ordinal_exclusive,
                }),
            )],
        )?;

        let Err(error) =
            capture_external_legacy_rollout_lines(home.path(), &root_path, root_thread).await
        else {
            panic!("native cutoff must be authenticated");
        };

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }
    Ok(())
}

#[tokio::test]
async fn paginated_capture_uses_metadata_only_root_cutoff_and_bounds_all_ancestors()
-> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let oldest_id = ThreadId::new();
    let oldest_path = native_segment_path(home.path(), thread_id, oldest_id);
    let oldest_message = message_line("user", "oldest inherited message", 1);
    write_rollout(
        &oldest_path,
        &[
            meta_line(thread_id, ThreadHistoryMode::Paginated, Some(0), None),
            oldest_message.clone(),
        ],
    )?;
    let oldest_position = HistoryPosition {
        thread_id: oldest_id,
        end_ordinal_exclusive: 2,
        end_byte_offset: fs::metadata(&oldest_path)?.len(),
    };
    append_line(&oldest_path, &agent_line("outside oldest cutoff", 2))?;

    let predecessor_id = ThreadId::new();
    let predecessor_path = native_segment_path(home.path(), thread_id, predecessor_id);
    let predecessor_message = message_line("user", "immediate inherited message", 3);
    write_rollout(
        &predecessor_path,
        &[
            meta_line(
                thread_id,
                ThreadHistoryMode::Paginated,
                Some(2),
                Some(oldest_position),
            ),
            predecessor_message.clone(),
        ],
    )?;
    let predecessor_position = HistoryPosition {
        thread_id: predecessor_id,
        end_ordinal_exclusive: 4,
        end_byte_offset: fs::metadata(&predecessor_path)?.len(),
    };
    append_line(
        &predecessor_path,
        &agent_line("outside immediate cutoff", 4),
    )?;

    let root_path = active_path(home.path(), thread_id);
    let root_metadata = meta_line(
        thread_id,
        ThreadHistoryMode::Paginated,
        Some(4),
        Some(predecessor_position),
    );
    write_rollout(&root_path, std::slice::from_ref(&root_metadata))?;

    let (captured, source_end_ordinal_exclusive) =
        capture_external_paginated_rollout_lines(home.path(), &root_path, thread_id).await?;

    assert_eq!(source_end_ordinal_exclusive, 5);
    assert_rollout_lines_eq(
        &captured,
        &[root_metadata, oldest_message, predecessor_message],
    );
    assert_rollout_lines_eq(
        &captured,
        &materialize_rollout_lines(home.path(), &root_path).await?,
    );
    Ok(())
}

#[tokio::test]
async fn paginated_capture_counts_a_skipped_final_record_in_the_physical_root_cutoff()
-> io::Result<()> {
    for trailing_newline in [false, true] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let path = active_path(home.path(), thread_id);
        let expected = vec![
            meta_line(thread_id, ThreadHistoryMode::Paginated, Some(0), None),
            agent_line("retained record", 1),
        ];
        write_rollout(&path, &expected)?;
        let skipped = serde_json::json!({
            "timestamp": "2026-07-13T00:00:00Z",
            "ordinal": 2,
            "type": "response_item",
            "payload": {
                "type": "ghost_snapshot",
                "ghost_commit": {
                    "id": "deadbeef",
                    "preexisting_untracked_dirs": [],
                    "preexisting_untracked_files": [],
                },
            },
        });
        let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
        serde_json::to_writer(&mut writer, &skipped)?;
        if trailing_newline {
            writer.write_all(b"\n")?;
        }
        let (decoded, _, parse_errors) = crate::RolloutRecorder::load_rollout_lines(&path).await?;
        assert_eq!(parse_errors, 0);
        assert_rollout_lines_eq(&decoded, &expected);

        let (captured, source_end_ordinal_exclusive) =
            capture_external_paginated_rollout_lines(home.path(), &path, thread_id).await?;

        assert_eq!(source_end_ordinal_exclusive, 3);
        assert_rollout_lines_eq(&captured, &expected);
    }
    Ok(())
}

#[tokio::test]
async fn paginated_capture_excludes_an_incomplete_final_record_from_the_root_cutoff()
-> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let path = active_path(home.path(), thread_id);
    let expected = vec![
        meta_line(thread_id, ThreadHistoryMode::Paginated, Some(0), None),
        agent_line("complete record", 1),
    ];
    write_rollout(&path, &expected)?;
    let incomplete = serde_json::to_vec(&agent_line("incomplete record", 2))?;
    let mut writer = fs::OpenOptions::new().append(true).open(&path)?;
    writer.write_all(&incomplete[..incomplete.len() - 1])?;

    let (captured, source_end_ordinal_exclusive) =
        capture_external_paginated_rollout_lines(home.path(), &path, thread_id).await?;

    assert_eq!(source_end_ordinal_exclusive, 2);
    assert_rollout_lines_eq(&captured, &expected);
    Ok(())
}
