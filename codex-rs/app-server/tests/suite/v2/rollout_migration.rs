use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ExecCommandOutputDeltaEvent;
use codex_protocol::protocol::ExecOutputStream;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::user_input::UserInput as ProtocolUserInput;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::RolloutMigrationMode;
use codex_thread_store::RolloutMigrationOptions;
use codex_thread_store::RolloutMigrationStatus;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const CONTEXT_DEPENDENT_TURN_ID: &str = "01a007a5-e024-7230-bf4e-922358abba37";

struct LegacyMigrationFixture {
    home: TempDir,
    thread_id: ThreadId,
    source_paths: Vec<PathBuf>,
    source_bytes: Vec<Vec<u8>>,
    bounded_items: Vec<(String, String, Vec<u8>)>,
    bounded_turn_ids: HashSet<String>,
    total_item_count: usize,
}

#[tokio::test]
async fn automatic_migration_preserves_reported_legacy_item_ids_across_restart() -> Result<()> {
    for (item_counts, expected_bounded_id, expected_complete_id) in [
        ([397, 160, 1, 1], "item-161", "item-558"),
        ([416, 3, 1, 417], "item-4", "item-420"),
    ] {
        let fixture =
            legacy_migration_fixture(item_counts, expected_bounded_id, expected_complete_id)
                .await?;
        MockResponsesConfig::new("http://127.0.0.1:1").write(fixture.home.path())?;

        let (first_projection, _) = migrate_and_read_public_history(
            fixture.home.path(),
            fixture.thread_id,
            DEFAULT_READ_TIMEOUT,
        )
        .await?;
        let (second_projection, _) = migrate_and_read_public_history(
            fixture.home.path(),
            fixture.thread_id,
            DEFAULT_READ_TIMEOUT,
        )
        .await?;

        assert_eq!(second_projection, first_projection);
        assert_eq!(first_projection.len(), fixture.total_item_count);
        assert_eq!(
            first_projection
                .iter()
                .map(|(_, item_id, _)| item_id)
                .collect::<HashSet<_>>()
                .len(),
            fixture.total_item_count,
            "migration must assign every historical item a distinct ID"
        );
        let migrated_bounded_items = first_projection
            .iter()
            .filter(|(turn_id, _, _)| fixture.bounded_turn_ids.contains(turn_id))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(migrated_bounded_items, fixture.bounded_items);
        assert_eq!(
            fixture
                .source_paths
                .iter()
                .map(fs::read)
                .collect::<std::io::Result<Vec<_>>>()?,
            fixture.source_bytes,
            "automatic migration must retain the Legacy source bytes"
        );
    }

    Ok(())
}

#[tokio::test]
async fn automatic_migration_accepts_paginated_numeric_records_and_restarts() -> Result<()> {
    let home = TempDir::new()?;
    MockResponsesConfig::new("http://127.0.0.1:1").write(home.path())?;
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let predecessor_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_ids[0].to_string())
        .join(&filename);
    let predecessor_end = write_paginated_segment(
        &predecessor_path,
        home.path(),
        thread_id,
        segment_ids[0],
        /*start_ordinal*/ 0,
        vec![
            legacy_turn_started("numeric-predecessor-turn"),
            paginated_completed_user_message(
                thread_id,
                "numeric-predecessor-turn",
                "numeric-predecessor-item",
                "numeric token predecessor",
            ),
            legacy_turn_completed("numeric-predecessor-turn"),
        ],
    )?;
    let active_path = home.path().join("sessions/2025/01/03").join(filename);
    let active_end = write_paginated_segment(
        &active_path,
        home.path(),
        thread_id,
        segment_ids[1],
        predecessor_end,
        vec![
            legacy_segment_reference(predecessor_path.clone(), thread_id, segment_ids[0]),
            legacy_turn_started("numeric-active-turn"),
            paginated_completed_user_message(
                thread_id,
                "numeric-active-turn",
                "numeric-active-item",
                "numeric token active",
            ),
            legacy_turn_completed("numeric-active-turn"),
        ],
    )?;
    let token_count = json!({
        "timestamp": "2026-08-18T21:03:49.690Z",
        "ordinal": active_end,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": 319590193,
                    "cached_input_tokens": 312717039,
                    "cache_write_input_tokens": 6711808,
                    "output_tokens": 364803,
                    "reasoning_output_tokens": 57778,
                    "total_tokens": 319954996
                },
                "last_token_usage": {
                    "input_tokens": 203881,
                    "cached_input_tokens": 0,
                    "cache_write_input_tokens": 203740,
                    "output_tokens": 280,
                    "reasoning_output_tokens": 184,
                    "total_tokens": 204161
                },
                "model_context_window": 258400
            },
            "rate_limits": {
                "limit_id": "codex",
                "limit_name": null,
                "primary": {
                    "used_percent": 0.0,
                    "window_minutes": 1,
                    "resets_at": 1787087041
                },
                "secondary": {
                    "used_percent": 0.0,
                    "window_minutes": 300,
                    "resets_at": 1787102386
                },
                "credits": {
                    "has_credits": true,
                    "unlimited": true,
                    "balance": null
                },
                "individual_limit": null,
                "spend_control_reached": null,
                "plan_type": "business",
                "rate_limit_reached_type": null
            }
        }
    })
    .to_string();
    assert!(
        serde_json::from_str::<RolloutLine>(&token_count).is_err(),
        "fixture must retain the direct arbitrary-precision enum failure"
    );
    writeln!(
        fs::OpenOptions::new().append(true).open(&active_path)?,
        "{token_count}"
    )?;
    let source_bytes = [fs::read(&predecessor_path)?, fs::read(&active_path)?];

    let (first_projection, migrated_path) =
        migrate_and_read_public_history(home.path(), thread_id, DEFAULT_READ_TIMEOUT).await?;
    let migrated_meta = codex_rollout::read_session_meta_line(&migrated_path).await?;
    assert!(migrated_meta.meta.history_base.is_some());
    let (second_projection, restarted_path) =
        migrate_and_read_public_history(home.path(), thread_id, DEFAULT_READ_TIMEOUT).await?;

    assert_eq!(restarted_path, migrated_path);
    assert_eq!(second_projection, first_projection);
    assert_eq!(first_projection.len(), 2);
    assert_eq!(
        [fs::read(predecessor_path)?, fs::read(active_path)?],
        source_bytes
    );
    Ok(())
}

#[tokio::test]
async fn large_unmarked_paginated_history_uses_compatibility_reader_and_restarts() -> Result<()> {
    const OUTPUT_RECORD_COUNT: usize = 60;
    const OUTPUT_CHUNK_BYTES: usize = 900 * 1024;

    let home = TempDir::new()?;
    MockResponsesConfig::new("http://127.0.0.1:1").write(home.path())?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let path = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    let next_ordinal = write_paginated_segment(
        &path,
        home.path(),
        thread_id,
        segment_id,
        /*start_ordinal*/ 0,
        vec![
            RolloutItem::Compacted(CompactedItem {
                message: "ordinary unmarked compaction".to_string(),
                replacement_history: Some(Vec::new()),
                mcp_resource_origins: None,
                window_number: Some(1),
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
                segment_state_checkpoint: None,
            }),
            legacy_turn_started("large-active-turn"),
            paginated_completed_user_message(
                thread_id,
                "large-active-turn",
                "large-active-item",
                "message after ordinary compaction",
            ),
            legacy_turn_completed("large-active-turn"),
        ],
    )?;
    let output_chunk = vec![b'x'; OUTPUT_CHUNK_BYTES];
    let mut file = fs::OpenOptions::new().append(true).open(&path)?;
    for (ordinal, index) in (next_ordinal..).zip(0..OUTPUT_RECORD_COUNT) {
        let line = RolloutLine {
            timestamp: "2025-01-03T12:00:00Z".to_string(),
            ordinal: Some(ordinal),
            item: RolloutItem::EventMsg(EventMsg::ExecCommandOutputDelta(
                ExecCommandOutputDeltaEvent {
                    call_id: format!("large-output-{index}"),
                    stream: ExecOutputStream::Stdout,
                    chunk: output_chunk.clone(),
                },
            )),
        };
        writeln!(file, "{}", serde_json::to_string(&line)?)?;
    }
    drop(file);
    assert!(
        fs::metadata(&path)?.len() > 64 * 1024 * 1024,
        "fixture must exceed the certified-root reverse-scan limit"
    );
    let source_sha256 = file_sha256(&path)?;

    let (first_projection, first_path) =
        migrate_and_read_public_history(home.path(), thread_id, std::time::Duration::from_secs(60))
            .await?;
    let (second_projection, second_path) =
        migrate_and_read_public_history(home.path(), thread_id, std::time::Duration::from_secs(60))
            .await?;

    assert_eq!(first_path, path);
    assert_eq!(second_path, path);
    assert_eq!(first_projection, second_projection);
    assert_eq!(first_projection.len(), 1);
    assert_eq!(first_projection[0].1, "large-active-item");
    assert_eq!(file_sha256(&path)?, source_sha256);
    Ok(())
}

#[tokio::test]
async fn migrated_legacy_thread_cold_resume_preserves_model_context() -> Result<()> {
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("resp-1"),
                responses::ev_assistant_message("msg-1", "legacy assistant message"),
                responses::ev_completed("resp-1"),
            ]),
            responses::sse(vec![
                responses::ev_response_created("resp-2"),
                responses::ev_assistant_message("msg-2", "resumed assistant message"),
                responses::ev_completed("resp-2"),
            ]),
        ],
    )
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut primary = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let start_id = primary
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(ThreadHistoryMode::Legacy),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, primary.read_response(start_id)).await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        primary.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![UserInput::Text {
                text: "legacy user message".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    timeout(DEFAULT_READ_TIMEOUT, primary.shutdown_gracefully()).await??;

    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let state_db =
        codex_state::StateRuntime::init(sqlite.clone(), "mock_provider".to_string()).await?;
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.path().to_path_buf(),
            sqlite,
            default_model_provider_id: "mock_provider".to_string(),
        },
        Some(state_db),
    );
    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            mode: RolloutMigrationMode::Apply,
            max_mib_per_second: Some(1024),
            ..RolloutMigrationOptions::default()
        })
        .await?;
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    drop(store);

    let mut secondary = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let resume_id = secondary
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread.id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread: resumed, ..
    } = timeout(DEFAULT_READ_TIMEOUT, secondary.read_response(resume_id)).await??;
    assert_eq!(resumed.history_mode, ThreadHistoryMode::Paginated);

    timeout(
        DEFAULT_READ_TIMEOUT,
        secondary.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread.id,
            input: vec![UserInput::Text {
                text: "resumed user message".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 2);
    let resumed_request = requests.last().expect("resumed turn request");
    let user_messages = resumed_request.message_input_texts("user");
    assert!(user_messages.contains(&"legacy user message".to_string()));
    assert!(user_messages.contains(&"resumed user message".to_string()));
    assert!(resumed_request.body_contains_text("legacy assistant message"));

    Ok(())
}

#[tokio::test]
async fn automatic_migration_keeps_list_nonblocking_and_gates_resume() -> Result<()> {
    let server = responses::start_mock_server().await;
    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("resp-automatic-migration"),
            responses::ev_assistant_message("msg-automatic-migration", "legacy response"),
            responses::ev_completed("resp-automatic-migration"),
        ]),
    )
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut primary = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let start_id = primary
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(ThreadHistoryMode::Legacy),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, primary.read_response(start_id)).await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        primary.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![UserInput::Text {
                text: "legacy request".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    timeout(DEFAULT_READ_TIMEOUT, primary.shutdown_gracefully()).await??;

    let maintenance_guard = codex_rollout::try_acquire_rollout_maintenance_lock(codex_home.path())?
        .expect("hold rollout maintenance while app-server starts");
    let mut secondary = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let list_id = secondary
        .send_thread_list_request(ThreadListParams {
            cursor: None,
            limit: Some(10),
            sort_key: None,
            sort_direction: None,
            model_providers: None,
            source_kinds: None,
            archived: None,
            project_id: None,
            cwd: None,
            use_state_db_only: true,
            search_term: None,
            parent_thread_id: None,
            ancestor_thread_id: None,
            section_id: None,
        })
        .await?;
    let listed: ThreadListResponse = timeout(
        std::time::Duration::from_millis(250),
        secondary.read_response(list_id),
    )
    .await??;
    assert_eq!(listed.data.len(), 1);

    let resume_id = secondary
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread.id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let mut resumed = Box::pin(secondary.read_response::<ThreadResumeResponse>(resume_id));
    assert!(
        timeout(std::time::Duration::from_millis(100), &mut resumed)
            .await
            .is_err(),
        "thread/resume must wait for the requested thread migration"
    );
    drop(maintenance_guard);
    let ThreadResumeResponse {
        thread: resumed_thread,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, &mut resumed).await??;
    assert_eq!(resumed_thread.history_mode, ThreadHistoryMode::Paginated);
    drop(resumed);

    timeout(DEFAULT_READ_TIMEOUT, secondary.shutdown_gracefully()).await??;
    Ok(())
}

async fn migrate_and_read_public_history(
    home: &Path,
    thread_id: ThreadId,
    read_timeout: std::time::Duration,
) -> Result<(Vec<(String, String, Vec<u8>)>, PathBuf)> {
    let mut app_server = TestAppServer::builder()
        .with_codex_home(home)
        .build_initialized()
        .await?;
    let resume_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse { thread, .. } =
        timeout(read_timeout, app_server.read_response(resume_id)).await??;
    assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
    let rollout_path = thread
        .path
        .clone()
        .expect("resumed local thread rollout path");

    let turns_id = app_server
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor: None,
            limit: Some(10),
            sort_direction: Some(SortDirection::Asc),
            items_view: Some(TurnItemsView::Full),
        })
        .await?;
    let ThreadTurnsListResponse {
        data, next_cursor, ..
    } = timeout(read_timeout, app_server.read_response(turns_id)).await??;
    assert_eq!(next_cursor, None);
    let items = data
        .iter()
        .flat_map(|turn| {
            turn.items.iter().map(|item| {
                Ok((
                    turn.id.clone(),
                    item.id().to_string(),
                    serde_json::to_vec(item)?,
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    timeout(DEFAULT_READ_TIMEOUT, app_server.shutdown_gracefully()).await??;
    Ok((items, rollout_path))
}

async fn legacy_migration_fixture(
    item_counts: [usize; 4],
    expected_bounded_id: &str,
    expected_complete_id: &str,
) -> Result<LegacyMigrationFixture> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_ids = [
        SegmentId::new(),
        SegmentId::new(),
        SegmentId::new(),
        SegmentId::new(),
    ];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let mut predecessor = None;
    let mut source_paths = Vec::new();
    let turn_ids = [
        "older-turn",
        "bounded-prefix-turn",
        CONTEXT_DEPENDENT_TURN_ID,
        "active-turn",
    ];
    for (index, segment_id) in segment_ids.into_iter().enumerate() {
        let path = if index + 1 == segment_ids.len() {
            home.path().join("sessions/2025/01/03").join(&filename)
        } else {
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                .join(thread_id.to_string())
                .join(segment_id.to_string())
                .join(&filename)
        };
        let mut items = Vec::new();
        if let Some((predecessor_path, predecessor_segment_id)) = predecessor.take() {
            items.push(legacy_segment_reference(
                predecessor_path,
                thread_id,
                predecessor_segment_id,
            ));
        }
        let turn_id = turn_ids[index];
        items.push(legacy_turn_started(turn_id));
        items.extend(
            (0..item_counts[index])
                .map(|item_index| legacy_user_message(format!("question-{index}-{item_index}"))),
        );
        items.push(legacy_turn_completed(turn_id));
        write_legacy_segment(&path, home.path(), thread_id, segment_id, items)?;
        predecessor = Some((path.clone(), segment_id));
        source_paths.push(path);
    }
    let source_bytes = source_paths
        .iter()
        .map(fs::read)
        .collect::<std::io::Result<Vec<_>>>()?;
    let selected_path = source_paths.last().expect("active Legacy source");
    let bounded = codex_rollout::materialize_bounded_rollout_lines(
        home.path(),
        selected_path,
        codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
    )
    .await?;
    let bounded_rollout_items = bounded
        .lines
        .iter()
        .map(|line| line.item.clone())
        .collect::<Vec<_>>();
    let bounded_turns = build_turns_from_rollout_items(&bounded_rollout_items);
    let bounded_reported_turn = bounded_turns
        .iter()
        .find(|turn| turn.id == CONTEXT_DEPENDENT_TURN_ID)
        .expect("bounded Desktop history contains the reported turn");
    assert_eq!(
        bounded_reported_turn
            .items
            .iter()
            .map(ThreadItem::id)
            .collect::<Vec<_>>(),
        vec![expected_bounded_id]
    );
    let complete = codex_rollout::materialize_rollout_lines(home.path(), selected_path).await?;
    let complete_rollout_items = complete
        .iter()
        .map(|line| line.item.clone())
        .collect::<Vec<_>>();
    let complete_turns = build_turns_from_rollout_items(&complete_rollout_items);
    let complete_reported_turn = complete_turns
        .iter()
        .find(|turn| turn.id == CONTEXT_DEPENDENT_TURN_ID)
        .expect("complete Legacy history contains the reported turn");
    assert_eq!(
        complete_reported_turn
            .items
            .iter()
            .map(ThreadItem::id)
            .collect::<Vec<_>>(),
        vec![expected_complete_id]
    );
    let bounded_items = bounded_turns
        .iter()
        .flat_map(|turn| {
            turn.items.iter().map(|item| {
                Ok((
                    turn.id.clone(),
                    item.id().to_string(),
                    serde_json::to_vec(item)?,
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let bounded_turn_ids = bounded_turns.iter().map(|turn| turn.id.clone()).collect();

    Ok(LegacyMigrationFixture {
        home,
        thread_id,
        source_paths,
        source_bytes,
        bounded_items,
        bounded_turn_ids,
        total_item_count: item_counts.into_iter().sum(),
    })
}

fn write_legacy_segment(
    path: &Path,
    home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    items: Vec<RolloutItem>,
) -> Result<()> {
    fs::create_dir_all(path.parent().expect("Legacy segment parent"))?;
    let mut file = fs::File::create(path)?;
    let metadata = SessionMeta {
        session_id: thread_id.into(),
        id: thread_id,
        segment_id: Some(segment_id),
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        cwd: home.to_path_buf(),
        originator: "rollout-migration-integration-test".to_string(),
        cli_version: "0.0.0".to_string(),
        source: SessionSource::Cli,
        model_provider: Some("mock_provider".to_string()),
        ..SessionMeta::default()
    };
    for item in std::iter::once(RolloutItem::SessionMeta(SessionMetaLine {
        meta: metadata,
        git: None,
    }))
    .chain(items)
    {
        writeln!(
            file,
            "{}",
            serde_json::to_string(&RolloutLine {
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                ordinal: None,
                item,
            })?
        )?;
    }
    Ok(())
}

fn write_paginated_segment(
    path: &Path,
    home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    start_ordinal: u64,
    items: Vec<RolloutItem>,
) -> Result<u64> {
    fs::create_dir_all(path.parent().expect("Paginated segment parent"))?;
    let mut file = fs::File::create(path)?;
    let metadata = SessionMeta {
        session_id: thread_id.into(),
        id: thread_id,
        segment_id: Some(segment_id),
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        cwd: home.to_path_buf(),
        originator: "rollout-migration-integration-test".to_string(),
        cli_version: "0.0.0".to_string(),
        source: SessionSource::Cli,
        model_provider: Some("mock_provider".to_string()),
        history_mode: ThreadHistoryMode::Paginated.into(),
        ..SessionMeta::default()
    };
    let mut next_ordinal = start_ordinal;
    for item in std::iter::once(RolloutItem::SessionMeta(SessionMetaLine {
        meta: metadata,
        git: None,
    }))
    .chain(items)
    {
        writeln!(
            file,
            "{}",
            serde_json::to_string(&RolloutLine {
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                ordinal: Some(next_ordinal),
                item,
            })?
        )?;
        next_ordinal += 1;
    }
    Ok(next_ordinal)
}

fn legacy_segment_reference(
    path: PathBuf,
    thread_id: ThreadId,
    segment_id: SegmentId,
) -> RolloutItem {
    RolloutItem::RolloutReference(RolloutReferenceItem {
        rollout_id: Some(thread_id),
        rollout_path: path,
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    })
}

fn legacy_turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        trace_id: None,
        started_at: Some(1_735_905_600),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn legacy_user_message(message: String) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message,
        ..UserMessageEvent::default()
    }))
}

fn paginated_completed_user_message(
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    text: &str,
) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item: TurnItem::UserMessage(UserMessageItem {
            id: item_id.to_string(),
            client_id: None,
            content: vec![ProtocolUserInput::Text {
                text: text.to_string(),
                text_elements: Vec::new(),
            }],
        }),
        started_at_ms: None,
        completed_at_ms: 1_735_905_601_000,
    }))
}

fn legacy_turn_completed(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(1_735_905_600),
        completed_at: Some(1_735_905_601),
        duration_ms: Some(1_000),
        time_to_first_token_ms: None,
    }))
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
