use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::to_response;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadItemPayload;
#[cfg(target_os = "linux")]
use codex_app_server_protocol::ThreadItemsListCursor;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeInitialTurnsPageParams;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnItemsView;
#[cfg(target_os = "linux")]
use codex_app_server_protocol::UserInput as ApiUserInput;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::items::CollabAgentTool;
use codex_protocol::items::CollabAgentToolCallItem;
use codex_protocol::items::CollabAgentToolCallStatus;
use codex_protocol::items::SubAgentActivityItem;
use codex_protocol::items::TurnItem as CoreTurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentActivityEvent;
use codex_protocol::protocol::SubAgentActivityKind;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_rollout::RolloutItem;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::FreezeRolloutSegmentParams;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadParams as StoreReadThreadParams;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadStore;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::time::Instant;
use tempfile::TempDir;
use tokio::time::timeout;

#[cfg(windows)]
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
#[cfg(not(windows))]
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

const STALE_ACTIVITY_ID: &str = "stale-activity";
const STALE_SPAWN_ID: &str = "stale-spawn";
const MIXED_SPAWN_ID: &str = "mixed-old-spawn";
const BOUNDARY_ACTIVITY_ID: &str = "boundary-activity";
const BOUNDARY_SPAWN_ID: &str = "boundary-spawn";
const RECENT_INTERACTION_ID: &str = "recent-interaction";

#[tokio::test]
async fn legacy_thread_read_filters_subagents_older_than_five_segments() -> Result<()> {
    let fixture = SubagentHistoryFixture::new(ThreadHistoryMode::Legacy).await?;
    let mut app_server = fixture.app_server().await?;

    let turns = read_thread_turns(&mut app_server, fixture.thread_id).await?;
    assert_eq!(turns.len(), 5, "legacy reads retain the newest five turns");
    assert_projection_wiring(&turns, &fixture);
    let paged_turns = read_all_turn_pages(&mut app_server, fixture.thread_id).await?;
    assert_eq!(paged_turns.len(), 7, "pagination retains turn envelopes");
    assert_projection_wiring(&paged_turns, &fixture);
    let expected_resume_turns = paged_turns.iter().rev().cloned().collect::<Vec<_>>();
    let cold_resume_turns = read_resume_turns(&mut app_server, fixture.thread_id).await?;
    assert_eq!(cold_resume_turns, expected_resume_turns);
    fixture.assert_stored_history_is_unfiltered().await?;
    Ok(())
}

#[tokio::test]
async fn paginated_thread_history_filters_subagents_without_changing_turn_pages() -> Result<()> {
    let fixture = SubagentHistoryFixture::new(ThreadHistoryMode::Paginated).await?;
    let mut app_server = fixture.app_server().await?;

    let read_turns = read_thread_turns(&mut app_server, fixture.thread_id).await?;
    let paged_turns = read_all_turn_pages(&mut app_server, fixture.thread_id).await?;

    assert_eq!(
        read_turns.len(),
        5,
        "thread reads retain the newest five turns"
    );
    assert_projection_wiring(&read_turns, &fixture);
    assert_eq!(paged_turns, read_turns);
    assert_eq!(paged_turns.len(), 5, "filtering must not remove turns");
    assert_projection_wiring(&paged_turns, &fixture);
    let resume_turns = read_resume_turns(&mut app_server, fixture.thread_id).await?;
    assert_eq!(
        resume_turns,
        paged_turns.iter().rev().cloned().collect::<Vec<_>>()
    );
    fixture.assert_stored_history_is_unfiltered().await?;
    Ok(())
}

#[tokio::test]
async fn long_paginated_history_pages_match_cold_resume() -> Result<()> {
    let fixture = SubagentHistoryFixture::new_with_segments(
        ThreadHistoryMode::Paginated,
        32,
        /*with_state_db*/ true,
        /*build_certificate*/ true,
        /*active_checkpoint*/ false,
    )
    .await?;
    let mut app_server = fixture.app_server().await?;
    #[cfg(target_os = "linux")]
    let mut watch = {
        let mut ancestor = fixture.active_rollout_path.clone();
        for _ in 0..8 {
            let meta = codex_rollout::read_session_meta_line_exact(&ancestor).await?;
            let base = meta
                .meta
                .history_base
                .expect("fixture has an older segment");
            ancestor = codex_rollout::find_rollout_path_by_rollout_id(
                fixture.codex_home.path(),
                base.thread_id,
            )
            .await?
            .expect("fixture ancestor exists");
        }
        watch_history_file_opens(&ancestor)?
    };
    let request_id = app_server
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: fixture.thread_id.to_string(),
            cursor: None,
            limit: Some(1),
            sort_direction: Some(SortDirection::Desc),
            items_view: Some(TurnItemsView::NotLoaded),
        })
        .await?;
    let response = timeout(
        READ_TIMEOUT,
        app_server.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await;
    let recent = to_response::<ThreadTurnsListResponse>(response??)?;
    assert_eq!(
        recent
            .data
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        ["turn-31"]
    );
    assert!(recent.next_cursor.is_some());
    #[cfg(target_os = "linux")]
    {
        use std::io::Read;
        let mut events = [0; 256];
        assert_eq!(
            watch.read(&mut events).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    let request_id = app_server
        .send_thread_items_list_request(ThreadItemsListParams {
            thread_id: fixture.thread_id.to_string(),
            turn_id: Some("turn-31".to_string()),
            cursor: None,
            limit: Some(100),
            sort_direction: Some(SortDirection::Asc),
            defer_output: None,
        })
        .await?;
    let response = timeout(
        READ_TIMEOUT,
        app_server.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let items = to_response::<ThreadItemsListResponse>(response)?;
    assert_eq!(items.data.len(), 1);
    assert_eq!(items.data[0].item.id(), "user-31");
    assert!(items.next_cursor.is_none());
    #[cfg(target_os = "linux")]
    {
        use std::io::Read;
        let mut events = [0; 256];
        assert_eq!(
            watch.read(&mut events).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    let pages = read_all_turn_pages(&mut app_server, fixture.thread_id).await?;
    assert_eq!(
        pages.iter().map(|turn| turn.id.clone()).collect::<Vec<_>>(),
        (0..32)
            .map(|index| format!("turn-{index}"))
            .collect::<Vec<_>>()
    );
    let loaded: ThreadLoadedListResponse = app_server
        .request(|request_id| ClientRequest::ThreadLoadedList {
            request_id,
            params: ThreadLoadedListParams::default(),
        })
        .await?;
    assert!(!loaded.data.contains(&fixture.thread_id.to_string()));
    let expected = pages.iter().rev().take(7).cloned().collect::<Vec<_>>();
    let resumed = read_resume_turns(&mut app_server, fixture.thread_id).await?;
    assert_eq!(resumed, expected);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn uncertified_long_history_with_active_checkpoint_cold_resume_and_item_pages() -> Result<()>
{
    use std::io::Read;

    let fixture = SubagentHistoryFixture::new_with_segments(
        ThreadHistoryMode::Paginated,
        348,
        /*with_state_db*/ true,
        /*build_certificate*/ false,
        /*active_checkpoint*/ true,
    )
    .await?;
    let mut app_server = fixture.app_server().await?;
    let mut ancestor = fixture.active_rollout_path.clone();
    for _ in 0..8 {
        let meta = codex_rollout::read_session_meta_line_exact(&ancestor).await?;
        let base = meta
            .meta
            .history_base
            .expect("fixture has an older segment");
        ancestor = codex_rollout::find_rollout_path_by_rollout_id(
            fixture.codex_home.path(),
            base.thread_id,
        )
        .await?
        .expect("fixture ancestor exists");
    }
    let mut watch = watch_history_file_opens(&ancestor)?;
    let resume = ThreadResumeParams {
        thread_id: fixture.thread_id.to_string(),
        exclude_turns: true,
        ..Default::default()
    };
    let measured_timeout = std::time::Duration::from_secs(240);
    let start = Instant::now();
    let request_id = app_server
        .send_thread_resume_request(resume.clone())
        .await?;
    let cold: ThreadResumeResponse =
        timeout(measured_timeout, app_server.read_response(request_id)).await??;
    let cold_elapsed = start.elapsed();
    assert!(cold.thread.turns.is_empty());
    assert!(cold.turns_backwards_cursor.is_some());
    assert!(cold.items_backwards_cursor.is_some());
    let mut events = [0; 256];
    let cold_opened_older = match watch.read(&mut events) {
        Ok(n) => n != 0,
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
        Err(error) => return Err(error.into()),
    };

    let start = Instant::now();
    let request_id = app_server.send_thread_resume_request(resume).await?;
    let warm: ThreadResumeResponse =
        timeout(measured_timeout, app_server.read_response(request_id)).await??;
    let warm_elapsed = start.elapsed();
    assert_eq!(cold.turns_backwards_cursor, warm.turns_backwards_cursor);
    assert_eq!(cold.items_backwards_cursor, warm.items_backwards_cursor);
    eprintln!(
        "348 segments with an active model checkpoint and no lineage certificate: cold={cold_elapsed:?}, warm={warm_elapsed:?}, older file opened during cold={cold_opened_older}"
    );
    // The old background rebuild started after five seconds even without an item request.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    assert_eq!(
        watch.read(&mut events).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "idle resume must not start a full ancestor rebuild"
    );

    let backwards = warm
        .items_backwards_cursor
        .clone()
        .expect("warm resume item cursor");
    for cursor in [Some(backwards), None] {
        let start = Instant::now();
        let mut requests = Vec::new();
        for index in 343..348 {
            requests.push(
                app_server
                    .send_thread_items_list_request(ThreadItemsListParams {
                        thread_id: fixture.thread_id.to_string(),
                        turn_id: Some(format!("turn-{index}")),
                        cursor: cursor.clone().map(ThreadItemsListCursor::Opaque),
                        limit: Some(100),
                        sort_direction: Some(SortDirection::Desc),
                        defer_output: None,
                    })
                    .await?,
            );
        }
        for (index, request_id) in (343..348).zip(requests) {
            let response = timeout(
                measured_timeout,
                app_server.read_stream_until_response_message(RequestId::Integer(request_id)),
            )
            .await??;
            let page = to_response::<ThreadItemsListResponse>(response)?;
            assert_eq!(page.data.len(), if index == 343 { 2 } else { 1 });
            let item = if index == 343 {
                assert_eq!(page.data[0].item.id(), "late-343");
                &page.data[1]
            } else {
                &page.data[0]
            };
            assert_eq!(item.item.id(), format!("user-{index}"));
            let ThreadItemPayload::UserMessage { content, .. } = &item.item.payload else {
                panic!("expected a user message");
            };
            assert_eq!(
                content,
                &[ApiUserInput::Text {
                    text: format!("segment {index}"),
                    text_elements: Vec::new(),
                }]
            );
            assert!(page.next_cursor.is_none());
        }
        eprintln!(
            "348 segments without an initial lineage certificate: five descending item pages (resume cursor={})={:?}",
            cursor.is_some(),
            start.elapsed()
        );
        if cursor.is_some() {
            assert!(
                watch.read(&mut events)? > 0,
                "first requests scan the predecessor"
            );
        } else {
            assert_eq!(
                watch.read(&mut events).unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock,
                "repeated requests must use the source-verified scan"
            );
        }
    }
    assert!(
        !cold_opened_older,
        "cold resume opened an unused older segment"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn watch_history_file_opens(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    // SAFETY: The descriptor is new and becomes owned by the returned File.
    let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let watch = unsafe { std::fs::File::from_raw_fd(fd) };
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    // SAFETY: The descriptor is live and the path is NUL-terminated.
    if unsafe { libc::inotify_add_watch(watch.as_raw_fd(), path.as_ptr(), libc::IN_OPEN) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(watch)
}

struct SubagentHistoryFixture {
    codex_home: TempDir,
    thread_id: ThreadId,
    active_rollout_path: PathBuf,
    history_mode: ThreadHistoryMode,
    boundary_agent_id: ThreadId,
}

impl SubagentHistoryFixture {
    async fn new(history_mode: ThreadHistoryMode) -> Result<Self> {
        Self::new_with_segments(history_mode, 7, false, false, false).await
    }

    async fn new_with_segments(
        history_mode: ThreadHistoryMode,
        segment_count: usize,
        with_state_db: bool,
        build_certificate: bool,
        active_checkpoint: bool,
    ) -> Result<Self> {
        assert!(segment_count >= 7);
        assert!(!build_certificate || with_state_db);
        let codex_home = TempDir::new()?;
        MockResponsesConfig::new("http://127.0.0.1:1")
            .disable_feature(codex_features::Feature::BackgroundPaginatedRolloutMigration)
            .write(codex_home.path())?;
        let thread_id = ThreadId::new();
        let stale_agent_id = ThreadId::new();
        let recent_agent_id = ThreadId::new();
        let boundary_agent_id = ThreadId::new();
        let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
        let state_db = if with_state_db {
            Some(
                codex_state::StateRuntime::init(sqlite.clone(), "mock_provider".to_string())
                    .await?,
            )
        } else {
            None
        };
        let store = LocalThreadStore::new(
            LocalThreadStoreConfig {
                codex_home: codex_home.path().to_path_buf(),
                sqlite,
                default_model_provider_id: "mock_provider".to_string(),
            },
            state_db,
        );
        store
            .create_thread(CreateThreadParams {
                creator_user_id: None,
                creator_account_id: None,
                session_id: thread_id.into(),
                thread_id,
                extra_config: None,
                forked_from_id: None,
                parent_thread_id: None,
                attached_thread: false,
                source: SessionSource::Cli,
                thread_source: None,
                originator: "subagent-history-projection-test".to_string(),
                base_instructions: BaseInstructions::default(),
                dynamic_tools: Vec::new(),
                selected_capability_roots: Vec::new(),
                multi_agent_version: None,
                history_mode,
                history_base: None,
                subagent_history_start_ordinal: None,
                persistence_mode: Default::default(),
                initial_rollout_ordinal: 0,
                initial_window_id: codex_protocol::ContextWindowId::new_uuid_v7(),
                runtime_workspace_roots: None,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(codex_home.path().to_path_buf()),
                    model_provider: "mock_provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await?;
        store
            .persist_thread(thread_id, PersistContext::Standard)
            .await?;

        for segment_index in 0..segment_count {
            let is_replaced = active_checkpoint
                && (segment_count - 5..segment_count - 1).contains(&segment_index);
            let turn_id = if is_replaced {
                format!("old-turn-{segment_index}")
            } else {
                format!("turn-{segment_index}")
            };
            let mut turn_items = vec![CoreTurnItem::UserMessage(UserMessageItem {
                id: format!("user-{segment_index}"),
                client_id: None,
                content: vec![codex_protocol::user_input::UserInput::Text {
                    text: format!("segment {segment_index}"),
                    text_elements: Vec::new(),
                }],
            })];
            match segment_index {
                0 => turn_items.push(collab_item(
                    "retained-old-spawn",
                    CollabAgentTool::SpawnAgent,
                    thread_id,
                    vec![recent_agent_id],
                )),
                1 => turn_items.extend([
                    subagent_activity_item(STALE_ACTIVITY_ID, stale_agent_id, "stale"),
                    collab_item(
                        STALE_SPAWN_ID,
                        CollabAgentTool::SpawnAgent,
                        thread_id,
                        vec![stale_agent_id],
                    ),
                    collab_item(
                        MIXED_SPAWN_ID,
                        CollabAgentTool::SpawnAgent,
                        thread_id,
                        vec![stale_agent_id, recent_agent_id],
                    ),
                ]),
                2 => turn_items.extend([
                    subagent_activity_item(BOUNDARY_ACTIVITY_ID, boundary_agent_id, "boundary"),
                    collab_item(
                        BOUNDARY_SPAWN_ID,
                        CollabAgentTool::SpawnAgent,
                        thread_id,
                        vec![boundary_agent_id],
                    ),
                ]),
                6 => turn_items.push(collab_item(
                    RECENT_INTERACTION_ID,
                    CollabAgentTool::SendInput,
                    thread_id,
                    vec![recent_agent_id],
                )),
                _ => {}
            }
            let mut rollout_items = Vec::with_capacity(turn_items.len() + 2);
            rollout_items.push(turn_started(&turn_id));
            rollout_items.extend(turn_items.into_iter().map(|item| {
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    content_token_count: None,
                    thread_id,
                    turn_id: turn_id.clone(),
                    item,
                    started_at_ms: Some(0),
                    completed_at_ms: 1,
                }))
            }));
            if history_mode == ThreadHistoryMode::Legacy {
                match segment_index {
                    1 => rollout_items.push(subagent_activity_event(
                        STALE_ACTIVITY_ID,
                        stale_agent_id,
                        "stale",
                    )),
                    2 => rollout_items.push(subagent_activity_event(
                        BOUNDARY_ACTIVITY_ID,
                        boundary_agent_id,
                        "boundary",
                    )),
                    _ => {}
                }
            }
            rollout_items.push(turn_completed(&turn_id));
            if active_checkpoint && segment_index + 1 == segment_count {
                for index in segment_count - 5..segment_count - 1 {
                    let turn_id = format!("turn-{index}");
                    rollout_items.push(turn_started(&turn_id));
                    rollout_items.push(RolloutItem::EventMsg(EventMsg::ItemCompleted(
                        ItemCompletedEvent {
                            content_token_count: None,
                            thread_id,
                            turn_id: turn_id.clone(),
                            item: CoreTurnItem::UserMessage(UserMessageItem {
                                id: format!("user-{index}"),
                                client_id: None,
                                content: vec![codex_protocol::user_input::UserInput::Text {
                                    text: format!("segment {index}"),
                                    text_elements: Vec::new(),
                                }],
                            }),
                            started_at_ms: Some(0),
                            completed_at_ms: 1,
                        },
                    )));
                    rollout_items.push(turn_completed(&turn_id));
                }
                // An item can arrive after its turn completed and after another turn started.
                rollout_items.push(turn_started("next-turn"));
                rollout_items.push(RolloutItem::EventMsg(EventMsg::ItemCompleted(
                    ItemCompletedEvent {
                        content_token_count: None,
                        thread_id,
                        turn_id: format!("turn-{}", segment_count - 5),
                        item: CoreTurnItem::UserMessage(UserMessageItem {
                            id: "late-343".to_string(),
                            client_id: None,
                            content: vec![codex_protocol::user_input::UserInput::Text {
                                text: "late item".to_string(),
                                text_elements: Vec::new(),
                            }],
                        }),
                        started_at_ms: Some(0),
                        completed_at_ms: 1,
                    },
                )));
            }
            store
                .append_items(AppendThreadItemsParams {
                    thread_id,
                    items: rollout_items,
                })
                .await?;
            if segment_index + 1 < segment_count {
                let params = if active_checkpoint && segment_index + 2 == segment_count {
                    // This checkpoint defines the synthetic post-compaction model context.
                    // Without one, restoring model context may need older segments.
                    FreezeRolloutSegmentParams::rotate_checkpoint(
                        super::thread_fork::certified_indexed_fork_checkpoint(codex_home.path()),
                    )
                } else {
                    FreezeRolloutSegmentParams::rotate(Vec::new())
                };
                store.freeze_thread_segment(thread_id, params).await?;
            }
        }
        let active_rollout_path = store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await?
            .rollout_path
            .expect("persisted thread rollout path");
        store.shutdown_thread(thread_id).await?;
        if build_certificate {
            assert!(
                store
                    .rebuild_history_projection_with_certificate(thread_id)
                    .await?
            );
        }

        Ok(Self {
            codex_home,
            thread_id,
            active_rollout_path,
            history_mode,
            boundary_agent_id,
        })
    }

    async fn app_server(&self) -> Result<TestAppServer> {
        TestAppServer::builder()
            .with_codex_home(self.codex_home.path())
            .without_auto_env()
            .build_initialized()
            .await
    }

    async fn assert_stored_history_is_unfiltered(&self) -> Result<()> {
        let items = codex_rollout::materialize_rollout_items(
            self.codex_home.path(),
            self.active_rollout_path.as_path(),
        )
        .await?;
        let stored_item_ids = items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => Some(event.item.id()),
                RolloutItem::EventMsg(EventMsg::SubAgentActivity(event)) => {
                    Some(event.event_id.clone())
                }
                _ => None,
            })
            .collect::<std::collections::HashSet<_>>();
        assert!(stored_item_ids.contains(STALE_ACTIVITY_ID));
        if self.history_mode == ThreadHistoryMode::Paginated {
            assert!(stored_item_ids.contains(STALE_SPAWN_ID));
            assert!(stored_item_ids.contains(MIXED_SPAWN_ID));
        }
        Ok(())
    }
}

async fn read_thread_turns(
    app_server: &mut TestAppServer,
    thread_id: ThreadId,
) -> Result<Vec<Turn>> {
    let request_id = app_server
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse { thread, .. } =
        timeout(READ_TIMEOUT, app_server.read_response(request_id)).await??;
    Ok(thread.turns)
}

async fn read_all_turn_pages(
    app_server: &mut TestAppServer,
    thread_id: ThreadId,
) -> Result<Vec<Turn>> {
    let mut cursor = None;
    let mut turns = Vec::new();
    loop {
        let request_id = app_server
            .send_thread_turns_list_request(ThreadTurnsListParams {
                thread_id: thread_id.to_string(),
                cursor: cursor.clone(),
                limit: Some(1),
                sort_direction: Some(SortDirection::Asc),
                items_view: Some(TurnItemsView::Full),
            })
            .await?;
        let response: JSONRPCResponse = timeout(
            READ_TIMEOUT,
            app_server.read_stream_until_response_message(RequestId::Integer(request_id)),
        )
        .await??;
        let page = to_response::<ThreadTurnsListResponse>(response)?;
        turns.extend(page.data);
        let Some(next_cursor) = page.next_cursor else {
            return Ok(turns);
        };
        assert_ne!(cursor.as_ref(), Some(&next_cursor));
        cursor = Some(next_cursor);
    }
}

async fn read_resume_turns(
    app_server: &mut TestAppServer,
    thread_id: ThreadId,
) -> Result<Vec<Turn>> {
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(7),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        initial_turns_page,
        ..
    } = timeout(READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert!(thread.turns.is_empty());
    Ok(initial_turns_page
        .expect("resume returns the requested full turns page")
        .data)
}

fn assert_projection_wiring(turns: &[Turn], fixture: &SubagentHistoryFixture) {
    let items = turns
        .iter()
        .flat_map(|turn| turn.items.iter())
        .collect::<Vec<_>>();
    assert!(!items.iter().any(|item| item.id() == STALE_ACTIVITY_ID));
    assert!(items.iter().any(|item| item.id() == BOUNDARY_ACTIVITY_ID));

    let boundary_activity = items
        .iter()
        .find(|item| item.id() == BOUNDARY_ACTIVITY_ID)
        .expect("fifth-newest activity remains");
    assert!(matches!(
        &boundary_activity.payload,
        ThreadItemPayload::SubAgentActivity { agent_thread_id, .. }
            if agent_thread_id == &fixture.boundary_agent_id.to_string()
    ));
}

fn collab_item(
    id: &str,
    tool: CollabAgentTool,
    sender_thread_id: ThreadId,
    receiver_thread_ids: Vec<ThreadId>,
) -> CoreTurnItem {
    let agents_states = receiver_thread_ids
        .iter()
        .copied()
        .map(|thread_id| (thread_id, AgentStatus::Running))
        .collect::<HashMap<_, _>>();
    CoreTurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
        id: id.to_string(),
        tool,
        status: CollabAgentToolCallStatus::Completed,
        sender_thread_id,
        receiver_thread_ids,
        receiver_agents: Vec::new(),
        prompt: Some(id.to_string()),
        model: None,
        reasoning_effort: None,
        agents_states,
    })
}

fn subagent_activity_item(id: &str, agent_thread_id: ThreadId, path: &str) -> CoreTurnItem {
    CoreTurnItem::SubAgentActivity(SubAgentActivityItem {
        id: id.to_string(),
        model: None,
        reasoning_effort: None,
        kind: SubAgentActivityKind::Started,
        agent_thread_id,
        agent_path: AgentPath::try_from(format!("/root/{path}")).expect("valid test agent path"),
    })
}

fn subagent_activity_event(id: &str, agent_thread_id: ThreadId, path: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::SubAgentActivity(SubAgentActivityEvent {
        event_id: id.to_string(),
        model: None,
        reasoning_effort: None,
        occurred_at_ms: 1,
        agent_thread_id,
        agent_path: AgentPath::try_from(format!("/root/{path}")).expect("valid test agent path"),
        kind: SubAgentActivityKind::Started,
    }))
}

fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_attribution: None,
        turn_id: turn_id.to_string(),
        root_turn_id: None,
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_completed(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        root_turn_id: None,
        last_agent_message: None,
        error: None,
        started_at: Some(10),
        completed_at: Some(20),
        duration_ms: Some(10_000),
        time_to_first_token_ms: None,
    }))
}
