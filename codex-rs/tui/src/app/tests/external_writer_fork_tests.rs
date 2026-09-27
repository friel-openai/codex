//! Locked-thread fork shortcuts reuse the normal fork flow without taking the source lease.

use super::session_lifecycle_requests::recorded_params;
use super::session_lifecycle_requests::start_recording_app_server;
use super::*;
use pretty_assertions::assert_eq;

async fn native_external_fork_fixture(
    config: &Config,
    filename_timestamp: &str,
    message: &str,
    turn_id: &str,
    history_base: Option<codex_protocol::protocol::HistoryPosition>,
) -> Result<(ThreadId, PathBuf)> {
    let id = app_test_support::create_fake_paginated_rollout(
        &config.codex_home,
        filename_timestamp,
        "2026-01-01T00:00:00Z",
        message,
        Some(&config.model_provider_id),
        None,
    )
    .map_err(color_eyre::eyre::Report::msg)?;
    let thread_id = ThreadId::from_string(&id)?;
    let path = app_test_support::rollout_path(&config.codex_home, filename_timestamp, &id);
    app_test_support::append_fake_paginated_user_message(&path, &id, message)
        .await
        .map_err(color_eyre::eyre::Report::msg)?;
    codex_rollout::append_rollout_item_to_path(
        &path,
        &codex_rollout::RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: "saved-turn".to_string(),
                last_agent_message: None,
                error: None,
                started_at: Some(0),
                completed_at: Some(1),
                duration_ms: Some(1),
                time_to_first_token_ms: None,
            },
        )),
    )
    .await?;
    let (mut lines, _, errors) = codex_rollout::RolloutRecorder::load_rollout_lines(&path).await?;
    assert_eq!(errors, 0);
    for line in &mut lines {
        if let Some(base) = history_base {
            line.ordinal = line
                .ordinal
                .map(|ordinal| ordinal + base.end_ordinal_exclusive);
        }
        match &mut line.item {
            codex_rollout::RolloutItem::SessionMeta(meta) => meta.meta.history_base = history_base,
            codex_rollout::RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                event.turn_id = turn_id.to_string();
            }
            codex_rollout::RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => {
                event.turn_id = turn_id.to_string();
            }
            codex_rollout::RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                event.turn_id = turn_id.to_string();
            }
            _ => {}
        }
    }
    let encoded = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .join("\n");
    tokio::fs::write(&path, format!("{encoded}\n")).await?;
    Ok((thread_id, path))
}

fn external_fork_visible_messages(thread: &codex_app_server_protocol::Thread) -> Vec<String> {
    thread
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter_map(|item| {
            let codex_app_server_protocol::ThreadItem::UserMessage { content, .. } = item else {
                return None;
            };
            Some(
                content
                    .iter()
                    .filter_map(|input| match input {
                        codex_app_server_protocol::UserInput::Text { text, .. } => {
                            Some(text.as_str())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(""),
            )
        })
        .collect()
}

#[tokio::test]
async fn external_writer_paginated_fork_rpc_preserves_history_cutoff_and_source_writer()
-> Result<()> {
    for exclude_turns in [false, true] {
        let (mut app, _events, _operations) = make_test_app_with_channels().await;
        let codex_home = tempdir()?;
        app.config.codex_home = codex_home.path().to_path_buf().abs();
        app.config.sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
        let (ancestor_id, ancestor_path) = native_external_fork_fixture(
            &app.config,
            "2026-01-01T00-00-00",
            "Inherited visible message",
            "ancestor-turn",
            None,
        )
        .await?;
        let (ancestor_lines, _, _) =
            codex_rollout::RolloutRecorder::load_rollout_lines(&ancestor_path).await?;
        let history_base = codex_protocol::protocol::HistoryPosition {
            thread_id: ancestor_id,
            end_ordinal_exclusive: ancestor_lines
                .last()
                .and_then(|line| line.ordinal)
                .expect("ancestor ordinal")
                + 1,
            end_byte_offset: tokio::fs::metadata(&ancestor_path).await?.len(),
        };
        let (source_id, source_path) = native_external_fork_fixture(
            &app.config,
            "2026-01-02T00-00-00",
            "Active visible message",
            "active-turn",
            Some(history_base),
        )
        .await?;
        // This record exists before capture but lies outside the inherited native byte cutoff.
        codex_rollout::append_rollout_item_to_path(
            &ancestor_path,
            &codex_rollout::RolloutItem::ResponseItem(serde_json::from_value::<codex_protocol::models::ResponseItem>(serde_json::json!({
                "type": "message", "role": "user", "content": [{"type": "input_text", "text": "Ancestor beyond cutoff"}]
            }))?.into()),
        ).await?;
        let mut owner = crate::start_embedded_app_server_for_picker(&app.config).await?;
        owner
            .resume_thread(
                &app.local_settings,
                app.config.clone(),
                source_id,
                app.resume_model_settings(),
            )
            .await?;
        let source_before = owner.thread_read(source_id, false).await?;
        assert_eq!(
            source_before.history_mode,
            codex_app_server_protocol::ThreadHistoryMode::Paginated
        );
        assert_eq!(source_before.path.as_deref(), Some(source_path.as_path()));
        let source_bytes = tokio::fs::read(&source_path).await?;
        let ancestor_bytes = tokio::fs::read(&ancestor_path).await?;
        let mut server = crate::start_embedded_app_server_for_picker(&app.config).await?;

        // These are real endpoint requests, independent of the TUI shortcut and bootstrap adapter.
        for (last_turn_id, before_turn_id) in [
            (Some("active-turn".to_string()), None),
            (None, Some("active-turn".to_string())),
        ] {
            let request_id = server.next_request_id();
            let error = server
                .request_handle()
                .request_typed::<codex_app_server_protocol::ThreadForkResponse>(
                    codex_app_server_protocol::ClientRequest::ThreadFork {
                        request_id,
                        params: codex_app_server_protocol::ThreadForkParams {
                            thread_id: source_id.to_string(),
                            last_turn_id,
                            before_turn_id,
                            exclude_turns,
                            ..Default::default()
                        },
                    },
                )
                .await
                .expect_err("explicit boundaries retain writer-conflict rejection");
            assert!(
                error
                    .to_string()
                    .contains(&format!("thread {source_id} already has an active writer"))
            );
        }
        let request_id = server.next_request_id();
        let missing = ThreadId::new();
        let error = server
            .request_handle()
            .request_typed::<codex_app_server_protocol::ThreadForkResponse>(
                codex_app_server_protocol::ClientRequest::ThreadFork {
                    request_id,
                    params: codex_app_server_protocol::ThreadForkParams {
                        thread_id: missing.to_string(),
                        ..Default::default()
                    },
                },
            )
            .await
            .expect_err("unrelated source lookup errors do not select external capture");
        assert!(error.to_string().contains("no rollout found"));

        let request_id = server.next_request_id();
        let response = server
            .request_handle()
            .request_typed::<codex_app_server_protocol::ThreadForkResponse>(
                codex_app_server_protocol::ClientRequest::ThreadFork {
                    request_id,
                    params: codex_app_server_protocol::ThreadForkParams {
                        thread_id: source_id.to_string(),
                        exclude_turns,
                        ..Default::default()
                    },
                },
            )
            .await?;
        let child_id = ThreadId::from_string(&response.thread.id)?;
        assert_ne!(child_id, source_id);
        assert_eq!(
            response.thread.history_mode,
            codex_app_server_protocol::ThreadHistoryMode::Paginated
        );
        let expected = vec![
            "Inherited visible message".to_string(),
            "Active visible message".to_string(),
        ];
        if exclude_turns {
            assert!(response.thread.turns.is_empty());
        } else {
            assert_eq!(external_fork_visible_messages(&response.thread), expected);
        }
        let child = server.thread_read(child_id, true).await?;
        assert_eq!(external_fork_visible_messages(&child), expected);
        let child_path = child.path.expect("durable copied child rollout");
        let (child_lines, _, errors) =
            codex_rollout::RolloutRecorder::load_rollout_lines(&child_path).await?;
        assert_eq!(errors, 0);
        assert!(
            child_lines
                .iter()
                .all(|line| !matches!(line.item, codex_rollout::RolloutItem::RolloutReference(_)))
        );
        let Some(codex_rollout::RolloutLine {
            item: codex_rollout::RolloutItem::SessionMeta(meta),
            ..
        }) = child_lines.first()
        else {
            panic!("copied child has session metadata");
        };
        assert!(meta.meta.history_base.is_none());
        let persisted = serde_json::to_string(&child_lines)?;
        assert!(persisted.contains("Inherited visible message"));
        assert!(persisted.contains("Active visible message"));
        assert!(!persisted.contains("Ancestor beyond cutoff"));
        assert_eq!(tokio::fs::read(&source_path).await?, source_bytes);
        assert_eq!(tokio::fs::read(&ancestor_path).await?, ancestor_bytes);
        assert_eq!(
            owner.thread_read(source_id, false).await?.path,
            source_before.path
        );
        let error = server
            .resume_thread(
                &app.local_settings,
                app.config.clone(),
                source_id,
                app.resume_model_settings(),
            )
            .await
            .expect_err("source remains owned by its original server");
        assert!(crate::app_server_session::is_active_writer_error(&error));

        owner.thread_inject_items(source_id, vec![serde_json::from_value(serde_json::json!({
            "type": "message", "role": "user", "content": [{"type": "input_text", "text": "Parent appended after capture"}]
        }))?]).await?;
        assert!(
            String::from_utf8(tokio::fs::read(&source_path).await?)?
                .contains("Parent appended after capture")
        );
        assert_eq!(
            tokio::fs::read(&child_path).await?,
            persisted_rollout_bytes(&child_lines)?
        );
        server.thread_inject_items(child_id, vec![serde_json::from_value(serde_json::json!({
            "type": "message", "role": "user", "content": [{"type": "input_text", "text": "Editable copied child"}]
        }))?]).await?;
        server.shutdown().await?;
        let mut reader = crate::start_embedded_app_server_for_picker(&app.config).await?;
        reader
            .resume_thread(
                &app.local_settings,
                app.config.clone(),
                child_id,
                app.resume_model_settings(),
            )
            .await?;
        let cold = reader.thread_read(child_id, true).await?;
        let cold_text = serde_json::to_string(&cold)?;
        for message in &expected {
            assert!(cold_text.contains(message));
        }
        assert!(!cold_text.contains("Parent appended after capture"));
        let copied_bytes = String::from_utf8(tokio::fs::read(&child_path).await?)?;
        assert!(copied_bytes.contains("Editable copied child"));
        assert!(!copied_bytes.contains("Parent appended after capture"));
        reader.shutdown().await?;

        // The owning server still freezes shared ancestry rather than copying source records.
        let request_id = owner.next_request_id();
        let shared = owner
            .request_handle()
            .request_typed::<codex_app_server_protocol::ThreadForkResponse>(
                codex_app_server_protocol::ClientRequest::ThreadFork {
                    request_id,
                    params: codex_app_server_protocol::ThreadForkParams {
                        thread_id: source_id.to_string(),
                        exclude_turns,
                        ..Default::default()
                    },
                },
            )
            .await?;
        let shared_path = shared.thread.path.expect("durable shared-history child");
        let (shared_lines, _, errors) =
            codex_rollout::RolloutRecorder::load_rollout_lines(&shared_path).await?;
        assert_eq!(errors, 0);
        assert!(shared_lines.iter().any(|line| match &line.item {
            codex_rollout::RolloutItem::SessionMeta(meta) => meta.meta.history_base.is_some(),
            codex_rollout::RolloutItem::RolloutReference(_) => true,
            _ => false,
        }));
        // The injected user response has no later turn completion. The shared fork adds only
        // its interruption boundary; parent activity remains behind the inherited history.
        let activity = shared_lines
            .iter()
            .map(|line| &line.item)
            .filter(|item| {
                matches!(
                    item,
                    codex_rollout::RolloutItem::ResponseItem(_)
                        | codex_rollout::RolloutItem::EventMsg(
                            EventMsg::ItemCompleted(_) | EventMsg::TurnAborted(_)
                        )
                )
            })
            .collect::<Vec<_>>();
        let [
            codex_rollout::RolloutItem::ResponseItem(marker),
            codex_rollout::RolloutItem::EventMsg(EventMsg::TurnAborted(aborted)),
        ] = activity.as_slice()
        else {
            panic!("expected only the child interruption boundary, got {activity:#?}");
        };
        let codex_protocol::models::ResponseItem::Message { role, content, .. } = &marker.item
        else {
            panic!("expected an interruption message, got {marker:#?}");
        };
        let codex_rollout::RolloutItem::SessionMeta(shared_meta) = &shared_lines[0].item else {
            panic!("shared child must begin with session metadata");
        };
        // These strings are the marker contract in core/src/context/turn_aborted.rs.
        let (expected_role, guidance) = match shared_meta
            .meta
            .multi_agent_version
            .expect("persisted child runtime version")
        {
            codex_protocol::protocol::MultiAgentVersion::V2 => (
                "developer",
                "The previous turn was interrupted on purpose. Any running unified exec processes may still be running in the background. If any tools/commands were aborted, they may have partially executed.",
            ),
            codex_protocol::protocol::MultiAgentVersion::V1
            | codex_protocol::protocol::MultiAgentVersion::Disabled => (
                "user",
                "The user interrupted the previous turn on purpose. Any running unified exec processes may still be running in the background. If any tools/commands were aborted, they may have partially executed.",
            ),
        };
        assert_eq!(
            (role.as_str(), content.as_slice()),
            (
                expected_role,
                [codex_protocol::models::ContentItem::InputText {
                    text: format!("<turn_aborted>\n{guidance}\n</turn_aborted>"),
                }]
                .as_slice()
            )
        );
        assert_eq!(
            serde_json::to_value(aborted)?,
            serde_json::to_value(codex_protocol::protocol::TurnAbortedEvent {
                turn_id: None,
                reason: codex_protocol::protocol::TurnAbortReason::Interrupted,
                started_at: None,
                completed_at: None,
                duration_ms: None,
            })?
        );
        owner.shutdown().await?;
    }
    Ok(())
}

fn persisted_rollout_bytes(lines: &[codex_rollout::RolloutLine]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for line in lines {
        serde_json::to_writer(&mut bytes, line)?;
        bytes.push(b'\n');
    }
    Ok(bytes)
}

#[tokio::test]
async fn external_writer_fork_shortcut_respects_input_ownership() -> Result<()> {
    let (mut app, mut events, _operations) = make_test_app_with_channels().await;
    app.enqueue_primary_thread_session(
        test_thread_session(ThreadId::new(), app.config.cwd.to_path_buf()),
        Vec::new(),
    )
    .await?;
    let mut server = crate::start_embedded_app_server_for_picker(&app.config).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    let fork_key = KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE);
    app.handle_tui_event(&mut tui, &mut server, TuiEvent::Key(fork_key))
        .await?;
    // A navigation key flushes the composer's pending single-character paste burst.
    app.handle_tui_event(
        &mut tui,
        &mut server,
        TuiEvent::Key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE)),
    )
    .await?;
    assert_eq!(app.chat_widget.composer_text_with_pending(), "f");
    assert!(
        !std::iter::from_fn(|| events.try_recv().ok())
            .any(|event| matches!(event, AppEvent::ForkCurrentSession { .. }))
    );

    app.chat_widget.show_external_writer_thread();
    for (key, expected) in [
        (fork_key, true),
        (KeyEvent::new(KeyCode::Char('F'), KeyModifiers::SHIFT), true),
        (KeyEvent::new(KeyCode::Char('F'), KeyModifiers::NONE), true),
        (KeyEvent::new(KeyCode::Char('f'), KeyModifiers::ALT), false),
        (
            KeyEvent::new(KeyCode::Char('f'), KeyModifiers::CONTROL),
            false,
        ),
        (
            KeyEvent::new(KeyCode::Char('f'), KeyModifiers::SUPER),
            false,
        ),
        (
            KeyEvent::new_with_kind(KeyCode::Char('f'), KeyModifiers::NONE, KeyEventKind::Repeat),
            false,
        ),
        (
            KeyEvent::new_with_kind(
                KeyCode::Char('f'),
                KeyModifiers::NONE,
                KeyEventKind::Release,
            ),
            false,
        ),
    ] {
        while events.try_recv().is_ok() {}
        app.handle_tui_event(&mut tui, &mut server, TuiEvent::Key(key))
            .await?;
        let forks = std::iter::from_fn(|| events.try_recv().ok())
            .filter(|event| {
                matches!(
                    event,
                    AppEvent::ForkCurrentSession {
                        name: None,
                        placement: None
                    }
                )
            })
            .count();
        assert_eq!(forks, usize::from(expected), "{key:?}");
        assert_eq!(app.chat_widget.composer_text_with_pending(), "f");
        if expected {
            for code in [KeyCode::Char('f'), KeyCode::Char('r')] {
                app.handle_tui_event(
                    &mut tui,
                    &mut server,
                    TuiEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)),
                )
                .await?;
            }
            assert!(events.try_recv().is_err());
            // The event handler clears the guard when it consumes the fork request.
            app.chat_widget.fork_in_progress = false;
        }
    }

    app.handle_tui_event(
        &mut tui,
        &mut server,
        TuiEvent::Key(KeyEvent::new(KeyCode::F(3), KeyModifiers::NONE)),
    )
    .await?;
    assert!(app.overlay.is_some());
    while events.try_recv().is_ok() {}
    app.handle_tui_event(&mut tui, &mut server, TuiEvent::Key(fork_key))
        .await?;
    assert!(
        !std::iter::from_fn(|| events.try_recv().ok())
            .any(|event| matches!(event, AppEvent::ForkCurrentSession { .. }))
    );

    app.overlay = None;
    app.chat_widget.show_selection_view(SelectionViewParams {
        items: vec![SelectionItem {
            name: "Keep this popup open".into(),
            ..Default::default()
        }],
        ..SelectionViewParams::picker()
    });
    app.handle_tui_event(&mut tui, &mut server, TuiEvent::Key(fork_key))
        .await?;
    assert!(
        !std::iter::from_fn(|| events.try_recv().ok())
            .any(|event| matches!(event, AppEvent::ForkCurrentSession { .. }))
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn external_writer_fork_opens_editable_thread_without_taking_source_lease() -> Result<()> {
    let (mut app, mut events, _operations) = make_test_app_with_channels().await;
    let codex_home = tempdir()?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config.sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let (thread_id, _) = native_external_fork_fixture(
        &app.config,
        "2026-01-01T00-00-00",
        "Saved user message",
        "saved-turn",
        /*history_base*/ None,
    )
    .await?;
    let mut owner = crate::start_embedded_app_server_for_picker(&app.config).await?;
    owner
        .resume_thread(
            &app.local_settings,
            app.config.clone(),
            thread_id,
            app.resume_model_settings(),
        )
        .await?;
    assert_eq!(
        owner.thread_read(thread_id, false).await?.history_mode,
        codex_app_server_protocol::ThreadHistoryMode::Paginated
    );
    let (mut server, requests, proxy) = start_recording_app_server(
        &app.config,
        /*blocked_thread_list*/ None,
        /*failed_thread_name*/ None,
    )
    .await?;
    let (view, _notice) = server
        .read_thread_for_viewing(&app.config, &app.local_settings, thread_id)
        .await?;
    app.enqueue_primary_thread_session(view.session, view.turns)
        .await?;
    app.ensure_thread_channel(thread_id).mark_external_writer();
    app.chat_widget
        .set_queue_autosend_suppressed(/*suppressed*/ true);
    app.chat_widget.insert_str("Retained queued prompt");
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert_eq!(
        app.chat_widget.queued_user_message_texts(),
        vec!["Retained queued prompt".to_string()]
    );
    app.chat_widget.insert_str("Retained draft");
    app.chat_widget.show_external_writer_thread();
    let retained_input = app.chat_widget.capture_thread_input_state();
    while events.try_recv().is_ok() {}
    requests.lock().expect("request recorder lock").clear();
    let mut tui = crate::tui::test_support::make_test_tui()?;

    app.handle_tui_event(
        &mut tui,
        &mut server,
        TuiEvent::Key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE)),
    )
    .await?;
    let event = events.try_recv()?;
    assert!(matches!(
        event,
        AppEvent::ForkCurrentSession {
            name: None,
            placement: None
        }
    ));
    Box::pin(app.handle_event(&mut tui, &mut server, event)).await?;

    assert_eq!(app.chat_widget.capture_thread_input_state(), retained_input);
    let fork_diagnostics = if app.chat_widget.thread_id() == Some(thread_id) {
        std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event {
                AppEvent::InsertHistoryCell(cell) => {
                    Some(lines_to_single_string(&cell.display_lines(/*width*/ 100)))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        String::new()
    };
    assert_ne!(
        app.chat_widget.thread_id(),
        Some(thread_id),
        "queued fork messages: {fork_diagnostics}"
    );
    assert!(!app.chat_widget.is_external_writer_view());
    assert!(!app.chat_widget.fork_in_progress);
    let messages = std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            AppEvent::InsertHistoryCell(cell) => {
                let text = lines_to_single_string(&cell.display_lines(/*width*/ 100));
                text.contains("Fork created.").then_some(text)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    insta::assert_snapshot!("fork_completion", messages.join("\n"));
    assert_eq!(
        recorded_params(&requests, "thread/fork")
            .into_iter()
            .map(|params| params["threadId"].clone())
            .collect::<Vec<_>>(),
        vec![serde_json::json!(thread_id.to_string())]
    );
    for method in ["thread/resume", "turn/start", "turn/interrupt"] {
        assert!(recorded_params(&requests, method).is_empty(), "{method}");
    }
    app.handle_tui_event(
        &mut tui,
        &mut server,
        TuiEvent::Paste("Editable fork".into()),
    )
    .await?;
    assert_eq!(
        app.chat_widget.composer_text_with_pending(),
        "Retained draftEditable fork"
    );
    let error = server
        .resume_thread(
            &app.local_settings,
            app.config.clone(),
            thread_id,
            app.resume_model_settings(),
        )
        .await
        .expect_err("source still has its original writer");
    assert!(crate::app_server_session::is_active_writer_error(&error));
    owner.shutdown().await?;
    server.shutdown().await?;
    proxy.await??;
    Ok(())
}

#[tokio::test]
async fn external_writer_fork_failure_keeps_the_locked_view_and_draft() -> Result<()> {
    let (mut app, mut events, _operations) = make_test_app_with_channels().await;
    let thread_id = ThreadId::new();
    app.chat_widget
        .handle_thread_session(test_thread_session(thread_id, app.config.cwd.to_path_buf()));
    app.chat_widget.insert_str("Retained draft");
    app.chat_widget.show_external_writer_thread();
    let mut server = crate::start_embedded_app_server_for_picker(&app.config).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    while events.try_recv().is_ok() {}
    app.handle_tui_event(
        &mut tui,
        &mut server,
        TuiEvent::Key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE)),
    )
    .await?;
    let event = events.try_recv()?;
    assert!(matches!(
        event,
        AppEvent::ForkCurrentSession {
            name: None,
            placement: None
        }
    ));
    Box::pin(app.handle_event(&mut tui, &mut server, event)).await?;
    assert_eq!(app.chat_widget.thread_id(), Some(thread_id));
    assert!(app.chat_widget.is_external_writer_view());
    assert!(!app.chat_widget.fork_in_progress);
    // The input drain can consume a resize; the next draw must sample the backend again.
    tui.terminal.last_known_screen_size = Size::new(/*width*/ 120, /*height*/ 40);
    assert_eq!(
        tui.screen_size_for_event(&TuiEvent::Draw)?,
        tui.terminal.size()?,
    );
    assert_eq!(
        app.chat_widget.composer_text_with_pending(),
        "Retained draft"
    );
    let messages = std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            AppEvent::InsertHistoryCell(cell) => {
                Some(lines_to_single_string(&cell.display_lines(/*width*/ 100)))
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(messages.contains("Failed to fork current session through the app server:"));
    server.shutdown().await?;
    Ok(())
}
