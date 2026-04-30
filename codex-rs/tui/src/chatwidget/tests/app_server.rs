use super::*;
use codex_app_server_protocol::RawResponseItemCompletedNotification;
use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_protocol::AgentPath;
use codex_protocol::protocol::CollabAgentSpawnEndEvent;
use codex_protocol::protocol::CollabCloseEndEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::RolloutItem;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn collab_spawn_end_shows_requested_model_and_effort() {
    let (mut chat, mut rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id = ThreadId::new();
    let spawned_thread_id = ThreadId::new();
    chat.set_collab_agent_metadata(
        spawned_thread_id,
        Some("Robie".to_string()),
        Some("explorer".to_string()),
    );

    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "call-spawn".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::InProgress,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: Vec::new(),
                prompt: Some("Explore the repo".to_string()),
                model: Some("gpt-5".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::High),
                agents_states: HashMap::new(),
            },
        }),
        /*replay_kind*/ None,
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "call-spawn".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![spawned_thread_id.to_string()],
                prompt: Some("Explore the repo".to_string()),
                model: None,
                reasoning_effort: None,
                agents_states: HashMap::from([(
                    spawned_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::PendingInit,
                        message: None,
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    let rendered = cells
        .iter()
        .map(|lines| lines_to_single_string(lines))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        rendered.contains("Spawned Robie [explorer] (gpt-5 high)"),
        "expected spawn line to include agent metadata and requested model, got {rendered:?}"
    );
}

#[tokio::test]
async fn live_app_server_raw_inter_agent_message_renders_agent_message_cell() {
    let (mut chat, mut rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/watchdog").expect("valid agent path"),
        AgentPath::root(),
        Vec::new(),
        "ping 21 (21)".to_string(),
        /*trigger_turn*/ true,
    );

    chat.handle_server_notification(
        ServerNotification::RawResponseItemCompleted(RawResponseItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: communication.to_response_input_item().into(),
        }),
        /*replay_kind*/ None,
    );

    let rendered = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");

    assert_chatwidget_snapshot!(
        "live_app_server_raw_inter_agent_message_renders_agent_message_cell",
        rendered
    );
}

#[tokio::test]
async fn live_app_server_subagent_notification_renders_status_message_cell() {
    let (mut chat, mut rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    let status = AgentStatus::Completed(Some(
        "The watchdog closed itself. I did not close it.".to_string(),
    ));
    let notification = format!(
        "<subagent_notification>\n{}\n</subagent_notification>",
        serde_json::json!({
            "agent_path": "/root/factorial_sum_agent",
            "status": status,
        })
    );
    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/factorial_sum_agent").expect("valid agent path"),
        AgentPath::root(),
        Vec::new(),
        notification,
        /*trigger_turn*/ false,
    );

    chat.handle_server_notification(
        ServerNotification::RawResponseItemCompleted(RawResponseItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: communication.to_response_input_item().into(),
        }),
        /*replay_kind*/ None,
    );

    let rendered = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");

    assert_chatwidget_snapshot!(
        "live_app_server_subagent_notification_renders_status_message_cell",
        rendered
    );
}

#[tokio::test]
async fn live_app_server_user_message_item_completed_does_not_duplicate_rendered_prompt() {
    let (mut chat, mut rx, mut op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.thread_id = Some(ThreadId::new());

    chat.bottom_pane
        .set_composer_text("Hi, are you there?".to_string(), Vec::new(), Vec::new());
    chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    match next_submit_op(&mut op_rx) {
        Op::UserTurn { .. } => {}
        other => panic!("expected Op::UserTurn, got {other:?}"),
    }

    let inserted = drain_insert_history(&mut rx);
    assert_eq!(inserted.len(), 1);
    assert!(lines_to_single_string(&inserted[0]).contains("Hi, are you there?"));

    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::UserMessage {
                id: "user-1".to_string(),
                content: vec![AppServerUserInput::Text {
                    text: "Hi, are you there?".to_string(),
                    text_elements: Vec::new(),
                }],
            },
        }),
        /*replay_kind*/ None,
    );

    assert!(drain_insert_history(&mut rx).is_empty());
}

#[tokio::test]
async fn live_app_server_turn_completed_clears_working_status_after_answer_item() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::InProgress,
                error: None,
                started_at: Some(0),
                completed_at: None,
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );

    assert!(chat.bottom_pane.is_task_running());
    let status = chat
        .bottom_pane
        .status_widget()
        .expect("status indicator should be visible");
    assert_eq!(status.header(), "Working");

    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::AgentMessage {
                id: "msg-1".to_string(),
                text: "Yes. What do you need?".to_string(),
                phase: Some(MessagePhase::FinalAnswer),
                memory_citation: None,
            },
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1);
    assert!(lines_to_single_string(&cells[0]).contains("Yes. What do you need?"));
    assert!(chat.bottom_pane.is_task_running());

    chat.handle_server_notification(
        ServerNotification::TurnCompleted(TurnCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::Completed,
                error: None,
                started_at: None,
                completed_at: Some(0),
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );

    assert!(!chat.bottom_pane.is_task_running());
    assert!(chat.bottom_pane.status_widget().is_none());
}

#[tokio::test]
async fn live_app_server_turn_started_sets_feedback_turn_id() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::InProgress,
                error: None,
                started_at: Some(0),
                completed_at: None,
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );

    chat.open_feedback_note(
        crate::app_event::FeedbackCategory::Bug,
        /*include_logs*/ false,
    );
    chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert_matches!(
        rx.try_recv(),
        Ok(AppEvent::SubmitFeedback {
            category: crate::app_event::FeedbackCategory::Bug,
            reason: None,
            turn_id: Some(turn_id),
            include_logs: false,
        }) if turn_id == "turn-1"
    );
}

#[tokio::test]
async fn live_app_server_warning_notification_renders_message() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::Warning(WarningNotification {
            thread_id: None,
            message: "Exceeded skills context budget of 2%. All skill descriptions were removed and 2 additional skills were not included in the model-visible skills list.".to_string(),
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1, "expected one warning history cell");
    let rendered = lines_to_single_string(&cells[0]);
    let normalized = rendered.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized.contains("Exceeded skills context budget of 2%."),
        "expected warning notification message, got {rendered}"
    );
    assert!(
        normalized.contains(
            "All skill descriptions were removed and 2 additional skills were not included in the model-visible skills list."
        ),
        "expected warning guidance, got {rendered}"
    );
}

#[tokio::test]
async fn live_app_server_guardian_warning_notification_renders_message() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::GuardianWarning(GuardianWarningNotification {
            thread_id: "thread-1".to_string(),
            message: "Automatic approval review denied the requested action.".to_string(),
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1, "expected one warning history cell");
    let rendered = lines_to_single_string(&cells[0]);
    assert!(
        rendered.contains("Automatic approval review denied the requested action."),
        "expected guardian warning notification message, got {rendered}"
    );
}

#[tokio::test]
async fn live_app_server_config_warning_prefixes_summary() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::ConfigWarning(ConfigWarningNotification {
            summary: "Invalid configuration; using defaults.".to_string(),
            details: None,
            path: None,
            range: None,
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1, "expected one warning history cell");
    let rendered = lines_to_single_string(&cells[0]);
    assert!(
        rendered.contains("Invalid configuration; using defaults."),
        "expected config warning summary, got {rendered}"
    );
}

#[tokio::test]
async fn live_app_server_file_change_item_started_preserves_changes() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::FileChange {
                id: "patch-1".to_string(),
                changes: vec![FileUpdateChange {
                    path: "foo.txt".to_string(),
                    kind: PatchChangeKind::Add,
                    diff: "hello\n".to_string(),
                }],
                status: AppServerPatchApplyStatus::InProgress,
            },
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert!(!cells.is_empty(), "expected patch history to be rendered");
    let transcript = lines_to_single_string(cells.last().expect("patch cell"));
    assert!(
        transcript.contains("Added foo.txt") || transcript.contains("Edited foo.txt"),
        "expected patch summary to include foo.txt, got: {transcript}"
    );
}

#[tokio::test]
async fn live_app_server_command_execution_strips_shell_wrapper() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let script = r#"python3 -c 'print("Hello, world!")'"#;
    let command =
        shlex::try_join(["/bin/zsh", "-lc", script]).expect("round-trippable shell wrapper");

    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CommandExecution {
                id: "cmd-1".to_string(),
                command: command.clone(),
                cwd: test_path_buf("/tmp").abs(),
                process_id: None,
                source: AppServerCommandExecutionSource::UserShell,
                status: AppServerCommandExecutionStatus::InProgress,
                command_actions: vec![AppServerCommandAction::Unknown {
                    command: script.to_string(),
                }],
                aggregated_output: None,
                exit_code: None,
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CommandExecution {
                id: "cmd-1".to_string(),
                command,
                cwd: test_path_buf("/tmp").abs(),
                process_id: None,
                source: AppServerCommandExecutionSource::UserShell,
                status: AppServerCommandExecutionStatus::Completed,
                command_actions: vec![AppServerCommandAction::Unknown {
                    command: script.to_string(),
                }],
                aggregated_output: Some("Hello, world!\n".to_string()),
                exit_code: Some(0),
                duration_ms: Some(5),
            },
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(
        cells.len(),
        1,
        "expected one completed command history cell"
    );
    let blob = lines_to_single_string(cells.first().expect("command cell"));
    assert_chatwidget_snapshot!(
        "live_app_server_command_execution_strips_shell_wrapper",
        blob
    );
}

#[tokio::test]
async fn live_app_server_collab_wait_items_render_history() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000000001").expect("valid thread id");
    let receiver_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b958ce5dc1cc").expect("valid thread id");
    let other_receiver_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b96db334332d").expect("valid thread id");
    chat.set_collab_agent_metadata(
        receiver_thread_id,
        Some("Robie".to_string()),
        Some("explorer".to_string()),
    );
    chat.set_collab_agent_metadata(
        other_receiver_thread_id,
        Some("Ada".to_string()),
        Some("reviewer".to_string()),
    );

    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "wait-1".to_string(),
                tool: AppServerCollabAgentTool::Wait,
                status: AppServerCollabAgentToolCallStatus::InProgress,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![
                    receiver_thread_id.to_string(),
                    other_receiver_thread_id.to_string(),
                ],
                prompt: None,
                model: None,
                reasoning_effort: None,
                agents_states: HashMap::new(),
            },
        }),
        /*replay_kind*/ None,
    );

    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "wait-1".to_string(),
                tool: AppServerCollabAgentTool::Wait,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![
                    receiver_thread_id.to_string(),
                    other_receiver_thread_id.to_string(),
                ],
                prompt: None,
                model: None,
                reasoning_effort: None,
                agents_states: HashMap::from([
                    (
                        receiver_thread_id.to_string(),
                        AppServerCollabAgentState {
                            status: AppServerCollabAgentStatus::Completed,
                            message: Some("Done".to_string()),
                        },
                    ),
                    (
                        other_receiver_thread_id.to_string(),
                        AppServerCollabAgentState {
                            status: AppServerCollabAgentStatus::Running,
                            message: None,
                        },
                    ),
                ]),
            },
        }),
        /*replay_kind*/ None,
    );

    let combined = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");
    assert_chatwidget_snapshot!("app_server_collab_wait_items_render_history", combined);
}

#[tokio::test]
async fn live_app_server_collab_spawn_completed_renders_requested_model_and_effort() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000000002").expect("valid thread id");
    let spawned_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b91781b41a8e").expect("valid thread id");

    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "spawn-1".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::InProgress,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: Vec::new(),
                prompt: Some("Explore the repo".to_string()),
                model: Some("gpt-5".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::High),
                agents_states: HashMap::new(),
            },
        }),
        /*replay_kind*/ None,
    );

    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "spawn-1".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![spawned_thread_id.to_string()],
                prompt: Some("Explore the repo".to_string()),
                model: Some("gpt-5".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::High),
                agents_states: HashMap::from([(
                    spawned_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::PendingInit,
                        message: None,
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    let combined = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");
    assert_chatwidget_snapshot!(
        "app_server_collab_spawn_completed_renders_requested_model_and_effort",
        combined
    );
}

#[tokio::test]
async fn subagent_panel_mounts_watchdog_spawn() {
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001002").expect("valid thread id");
    let watchdog_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001003").expect("valid thread id");

    chat.set_collab_agent_metadata(
        watchdog_thread_id,
        Some("watch-buildpando-rpc-context-tracing-spike".to_string()),
        Some("watchdog".to_string()),
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "spawn-watchdog".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![watchdog_thread_id.to_string()],
                prompt: Some(
                    "Watch /build/pando-rpc-context-tracing-spike work on Pando RPC accounting tracing spike. Goal: end-to-end no-explicit...".to_string(),
                ),
                model: Some("gpt-5.4".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::High),
                agents_states: HashMap::from([(
                    watchdog_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::PendingInit,
                        message: None,
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    let width = 140;
    let height = chat.desired_height(width);
    let mut terminal =
        ratatui::Terminal::new(VT100Backend::new(width, height)).expect("create terminal");
    terminal.set_viewport_area(ratatui::prelude::Rect::new(0, 0, width, height));
    terminal
        .draw(|f| chat.render(f.area(), f.buffer_mut()))
        .expect("render chat widget");
    let screen = normalized_backend_snapshot(terminal.backend());

    assert_chatwidget_snapshot!("subagent_panel_mounts_watchdog_spawn", screen);
}

#[tokio::test]
async fn subagent_panel_renders_subagent_and_watchdog_rows() {
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001002").expect("valid thread id");
    let worker_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001003").expect("valid thread id");
    let watchdog_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001004").expect("valid thread id");

    chat.set_collab_agent_metadata(
        worker_thread_id,
        Some("Explorer".to_string()),
        Some("worker".to_string()),
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "spawn-worker".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![worker_thread_id.to_string()],
                prompt: Some(
                    "Audit the TUI app-server event flow and report visible regressions."
                        .to_string(),
                ),
                model: Some("gpt-5.4".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::High),
                agents_states: HashMap::from([(
                    worker_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::Running,
                        message: None,
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    chat.set_collab_agent_metadata(
        watchdog_thread_id,
        Some("Watcher".to_string()),
        Some("watchdog".to_string()),
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "spawn-watchdog".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![watchdog_thread_id.to_string()],
                prompt: Some("Watch the worker for stalled progress.".to_string()),
                model: Some("gpt-5.4".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::Low),
                agents_states: HashMap::from([(
                    watchdog_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::PendingInit,
                        message: None,
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    let width = 140;
    let height = chat.desired_height(width);
    let mut terminal =
        ratatui::Terminal::new(VT100Backend::new(width, height)).expect("create terminal");
    terminal.set_viewport_area(ratatui::prelude::Rect::new(0, 0, width, height));
    terminal
        .draw(|f| chat.render(f.area(), f.buffer_mut()))
        .expect("render chat widget");
    let screen = normalized_backend_snapshot(terminal.backend());

    assert_chatwidget_snapshot!("subagent_panel_renders_subagent_and_watchdog_rows", screen);
}

#[tokio::test]
async fn subagent_notification_completion_hides_subagent_panel_row() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001002").expect("valid thread id");
    let worker_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001003").expect("valid thread id");

    chat.set_collab_agent_metadata(
        worker_thread_id,
        Some("Calculator".to_string()),
        Some("worker".to_string()),
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "spawn-worker".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![worker_thread_id.to_string()],
                prompt: Some("Compute the answer.".to_string()),
                model: Some("gpt-5.4".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::Low),
                agents_states: HashMap::from([(
                    worker_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::Running,
                        message: None,
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    let notification = format!(
        "<subagent_notification>\n{}\n</subagent_notification>",
        serde_json::json!({
            "agent_path": worker_thread_id.to_string(),
            "status": AgentStatus::Completed(Some("4037913".to_string())),
        })
    );
    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/calculator").expect("valid agent path"),
        AgentPath::root(),
        Vec::new(),
        notification,
        /*trigger_turn*/ false,
    );
    chat.handle_server_notification(
        ServerNotification::RawResponseItemCompleted(RawResponseItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: communication.to_response_input_item().into(),
        }),
        /*replay_kind*/ None,
    );

    let width = 140;
    let height = chat.desired_height(width);
    let mut terminal =
        ratatui::Terminal::new(VT100Backend::new(width, height)).expect("create terminal");
    terminal.set_viewport_area(ratatui::prelude::Rect::new(0, 0, width, height));
    terminal
        .draw(|f| chat.render(f.area(), f.buffer_mut()))
        .expect("render chat widget");
    let screen = normalized_backend_snapshot(terminal.backend());

    assert!(!screen.contains("Subagents"));
    assert!(!screen.contains("Calculator"));
    assert!(!screen.contains("<subagent_notification>"));
    let inserted = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(inserted.contains("Agent message: 4037913"));
    assert!(!inserted.contains("<subagent_notification>"));
    assert_chatwidget_snapshot!(
        "subagent_notification_completion_hides_subagent_panel_row",
        screen
    );
}

#[tokio::test]
async fn watchdog_goodbye_message_closes_subagent_panel_row() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001002").expect("valid thread id");
    let watchdog_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001003").expect("valid thread id");

    chat.set_collab_agent_metadata(
        watchdog_thread_id,
        Some("Boyle".to_string()),
        Some("watchdog".to_string()),
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "spawn-watchdog".to_string(),
                tool: AppServerCollabAgentTool::SpawnAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![watchdog_thread_id.to_string()],
                prompt: Some(
                    "Every time you start, respond with exactly `ping $RANDOM ($SUM)`.".to_string(),
                ),
                model: Some("arcanine 1m".to_string()),
                reasoning_effort: Some(ReasoningEffortConfig::Low),
                agents_states: HashMap::from([(
                    watchdog_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::PendingInit,
                        message: None,
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/watchdog").expect("valid agent path"),
        AgentPath::root(),
        Vec::new(),
        "goodbye".to_string(),
        /*trigger_turn*/ true,
    );
    chat.handle_server_notification(
        ServerNotification::RawResponseItemCompleted(RawResponseItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: communication.to_response_input_item().into(),
        }),
        /*replay_kind*/ None,
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: AppServerThreadItem::CollabAgentToolCall {
                id: "watchdog-close".to_string(),
                tool: AppServerCollabAgentTool::CloseAgent,
                status: AppServerCollabAgentToolCallStatus::Completed,
                sender_thread_id: sender_thread_id.to_string(),
                receiver_thread_ids: vec![watchdog_thread_id.to_string()],
                prompt: None,
                model: None,
                reasoning_effort: None,
                agents_states: HashMap::from([(
                    watchdog_thread_id.to_string(),
                    AppServerCollabAgentState {
                        status: AppServerCollabAgentStatus::Completed,
                        message: Some("goodbye".to_string()),
                    },
                )]),
            },
        }),
        /*replay_kind*/ None,
    );

    let inserted = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");
    assert_chatwidget_snapshot!("watchdog_goodbye_message_inserts_close_history", inserted);

    let width = 140;
    let height = chat.desired_height(width);
    let mut terminal =
        ratatui::Terminal::new(VT100Backend::new(width, height)).expect("create terminal");
    terminal.set_viewport_area(ratatui::prelude::Rect::new(0, 0, width, height));
    terminal
        .draw(|f| chat.render(f.area(), f.buffer_mut()))
        .expect("render chat widget");
    let screen = normalized_backend_snapshot(terminal.backend());

    assert_chatwidget_snapshot!("watchdog_goodbye_message_closes_subagent_panel_row", screen);
}

#[tokio::test]
async fn resume_replay_does_not_resurrect_closed_watchdog_panel_row() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001002").expect("valid thread id");
    let watchdog_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001003").expect("valid thread id");

    chat.set_collab_agent_metadata(
        watchdog_thread_id,
        Some("Boyle".to_string()),
        Some("watchdog".to_string()),
    );
    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/watchdog").expect("valid agent path"),
        AgentPath::root(),
        Vec::new(),
        "goodbye".to_string(),
        /*trigger_turn*/ true,
    );
    let turns = build_turns_from_rollout_items(&[
        RolloutItem::EventMsg(EventMsg::CollabAgentSpawnEnd(CollabAgentSpawnEndEvent {
            call_id: "spawn-watchdog".to_string(),
            sender_thread_id,
            new_thread_id: Some(watchdog_thread_id),
            new_agent_nickname: Some("Boyle".to_string()),
            new_agent_role: Some("watchdog".to_string()),
            prompt: "Every time you start, respond with goodbye.".to_string(),
            model: "arcanine 1m".to_string(),
            reasoning_effort: ReasoningEffortConfig::Low,
            status: AgentStatus::PendingInit,
        })),
        RolloutItem::ResponseItem(communication.to_response_input_item().into()),
        RolloutItem::EventMsg(EventMsg::CollabCloseEnd(CollabCloseEndEvent {
            call_id: "watchdog-close".to_string(),
            sender_thread_id,
            receiver_thread_id: watchdog_thread_id,
            receiver_agent_nickname: Some("Boyle".to_string()),
            receiver_agent_role: Some("watchdog".to_string()),
            status: AgentStatus::Completed(Some("goodbye".to_string())),
        })),
    ]);
    chat.replay_thread_turns(turns, ReplayKind::ResumeInitialMessages);

    let replayed_history = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");
    assert_chatwidget_snapshot!(
        "resume_replay_closed_watchdog_history_cells",
        replayed_history
    );

    let width = 140;
    let height = chat.desired_height(width);
    let mut terminal =
        ratatui::Terminal::new(VT100Backend::new(width, height)).expect("create terminal");
    terminal.set_viewport_area(ratatui::prelude::Rect::new(0, 0, width, height));
    terminal
        .draw(|f| chat.render(f.area(), f.buffer_mut()))
        .expect("render chat widget");
    let screen = normalized_backend_snapshot(terminal.backend());

    assert_chatwidget_snapshot!(
        "resume_replay_does_not_resurrect_closed_watchdog_panel_row",
        screen
    );
}

#[tokio::test]
async fn resume_replay_does_not_resurrect_open_subagent_panel_row() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let sender_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001012").expect("valid thread id");
    let subagent_thread_id =
        ThreadId::from_string("019cff70-2599-75e2-af72-b90000001013").expect("valid thread id");

    chat.set_collab_agent_metadata(
        subagent_thread_id,
        Some("Avicenna".to_string()),
        Some("fast-worker".to_string()),
    );
    let turns = build_turns_from_rollout_items(&[RolloutItem::EventMsg(
        EventMsg::CollabAgentSpawnEnd(CollabAgentSpawnEndEvent {
            call_id: "spawn-subagent".to_string(),
            sender_thread_id,
            new_thread_id: Some(subagent_thread_id),
            new_agent_nickname: Some("Avicenna".to_string()),
            new_agent_role: Some("fast-worker".to_string()),
            prompt: "Compute the exact value.".to_string(),
            model: "arcanine 1m".to_string(),
            reasoning_effort: ReasoningEffortConfig::Medium,
            status: AgentStatus::Running,
        }),
    )]);
    chat.replay_thread_turns(turns, ReplayKind::ResumeInitialMessages);

    let replayed_history = drain_insert_history(&mut rx)
        .into_iter()
        .map(|lines| lines_to_single_string(&lines))
        .collect::<Vec<_>>()
        .join("\n");
    assert_chatwidget_snapshot!(
        "resume_replay_open_subagent_history_cells",
        replayed_history
    );

    let width = 140;
    let height = chat.desired_height(width);
    let mut terminal =
        ratatui::Terminal::new(VT100Backend::new(width, height)).expect("create terminal");
    terminal.set_viewport_area(ratatui::prelude::Rect::new(0, 0, width, height));
    terminal
        .draw(|f| chat.render(f.area(), f.buffer_mut()))
        .expect("render chat widget");
    let screen = normalized_backend_snapshot(terminal.backend());

    assert_chatwidget_snapshot!(
        "resume_replay_does_not_resurrect_open_subagent_panel_row",
        screen
    );
}

#[tokio::test]
async fn live_app_server_failed_turn_does_not_duplicate_error_history() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::InProgress,
                error: None,
                started_at: Some(0),
                completed_at: None,
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );

    chat.handle_server_notification(
        ServerNotification::Error(ErrorNotification {
            error: AppServerTurnError {
                message: "permission denied".to_string(),
                codex_error_info: None,
                additional_details: None,
            },
            will_retry: false,
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
        }),
        /*replay_kind*/ None,
    );

    let first_cells = drain_insert_history(&mut rx);
    assert_eq!(first_cells.len(), 1);
    assert!(lines_to_single_string(&first_cells[0]).contains("permission denied"));

    chat.handle_server_notification(
        ServerNotification::TurnCompleted(TurnCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::Failed,
                error: Some(AppServerTurnError {
                    message: "permission denied".to_string(),
                    codex_error_info: None,
                    additional_details: None,
                }),
                started_at: None,
                completed_at: Some(0),
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );

    assert!(drain_insert_history(&mut rx).is_empty());
    assert!(!chat.bottom_pane.is_task_running());
}

#[tokio::test]
async fn live_app_server_stream_recovery_restores_previous_status_header() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::InProgress,
                error: None,
                started_at: Some(0),
                completed_at: None,
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );
    drain_insert_history(&mut rx);

    chat.handle_server_notification(
        ServerNotification::Error(ErrorNotification {
            error: AppServerTurnError {
                message: "Reconnecting... 1/5".to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
                additional_details: None,
            },
            will_retry: true,
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
        }),
        /*replay_kind*/ None,
    );
    drain_insert_history(&mut rx);

    chat.handle_server_notification(
        ServerNotification::AgentMessageDelta(
            codex_app_server_protocol::AgentMessageDeltaNotification {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "item-1".to_string(),
                delta: "hello".to_string(),
            },
        ),
        /*replay_kind*/ None,
    );

    let status = chat
        .bottom_pane
        .status_widget()
        .expect("status indicator should be visible");
    assert_eq!(status.header(), "Working");
    assert_eq!(status.details(), None);
    assert!(chat.retry_status_header.is_none());
}

#[tokio::test]
async fn live_app_server_server_overloaded_error_renders_warning() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::InProgress,
                error: None,
                started_at: Some(0),
                completed_at: None,
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );
    drain_insert_history(&mut rx);

    chat.handle_server_notification(
        ServerNotification::Error(ErrorNotification {
            error: AppServerTurnError {
                message: "server overloaded".to_string(),
                codex_error_info: Some(CodexErrorInfo::ServerOverloaded),
                additional_details: None,
            },
            will_retry: false,
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1);
    assert_eq!(lines_to_single_string(&cells[0]), "⚠ server overloaded\n");
    assert!(!chat.bottom_pane.is_task_running());
}

#[tokio::test]
async fn live_app_server_cyber_policy_error_renders_dedicated_notice() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "thread-1".to_string(),
            turn: AppServerTurn {
                id: "turn-1".to_string(),
                items: Vec::new(),
                status: AppServerTurnStatus::InProgress,
                error: None,
                started_at: Some(0),
                completed_at: None,
                duration_ms: None,
            },
        }),
        /*replay_kind*/ None,
    );
    drain_insert_history(&mut rx);

    chat.handle_server_notification(
        ServerNotification::Error(ErrorNotification {
            error: AppServerTurnError {
                message: "server fallback message".to_string(),
                codex_error_info: Some(CodexErrorInfo::CyberPolicy),
                additional_details: None,
            },
            will_retry: false,
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1);
    let rendered = lines_to_single_string(&cells[0]);
    assert!(rendered.contains("This chat was flagged for possible cybersecurity risk"));
    assert!(rendered.contains("Trusted Access for Cyber"));
    assert!(!rendered.contains("server fallback message"));
    assert!(!chat.bottom_pane.is_task_running());
}

#[tokio::test]
async fn live_app_server_model_verification_renders_warning() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::ModelVerification(ModelVerificationNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            verifications: vec![AppServerModelVerification::TrustedAccessForCyber],
        }),
        /*replay_kind*/ None,
    );

    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1);
    let rendered = lines_to_single_string(&cells[0]);
    assert!(rendered.contains("multiple flags for possible cybersecurity risk"));
    assert!(rendered.contains("extra safety checks are on"));
    assert!(rendered.contains("Trusted Access for Cyber"));
    assert!(rendered.contains("https://chatgpt.com/cyber"));
}

#[tokio::test]
async fn live_app_server_invalid_thread_name_update_is_ignored() {
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let thread_id = ThreadId::new();
    chat.thread_id = Some(thread_id);
    chat.thread_name = Some("original name".to_string());

    chat.handle_server_notification(
        ServerNotification::ThreadNameUpdated(
            codex_app_server_protocol::ThreadNameUpdatedNotification {
                thread_id: "not-a-thread-id".to_string(),
                thread_name: Some("bad update".to_string()),
            },
        ),
        /*replay_kind*/ None,
    );

    assert_eq!(chat.thread_id, Some(thread_id));
    assert_eq!(chat.thread_name, Some("original name".to_string()));
}

#[tokio::test]
async fn live_app_server_thread_name_update_shows_resume_hint() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let thread_id = ThreadId::new();
    chat.thread_id = Some(thread_id);

    chat.handle_server_notification(
        ServerNotification::ThreadNameUpdated(
            codex_app_server_protocol::ThreadNameUpdatedNotification {
                thread_id: thread_id.to_string(),
                thread_name: Some("review-fix".to_string()),
            },
        ),
        /*replay_kind*/ None,
    );

    assert_eq!(chat.thread_name, Some("review-fix".to_string()));
    let cells = drain_insert_history(&mut rx);
    assert_eq!(cells.len(), 1);
    let rendered = lines_to_single_string(&cells[0]);
    assert!(rendered.contains("Thread renamed to review-fix"));
    assert!(rendered.contains("codex resume review-fix"));
}

#[tokio::test]
async fn live_app_server_thread_closed_requests_immediate_exit() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;

    chat.handle_server_notification(
        ServerNotification::ThreadClosed(ThreadClosedNotification {
            thread_id: "thread-1".to_string(),
        }),
        /*replay_kind*/ None,
    );

    assert_matches!(rx.try_recv(), Ok(AppEvent::Exit(ExitMode::Immediate)));
}
