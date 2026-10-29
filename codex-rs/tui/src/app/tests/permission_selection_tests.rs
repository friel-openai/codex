//! Permission selection through the existing app-server settings API.

use super::session_lifecycle_requests::HistoryCapabilities;
use super::session_lifecycle_requests::recorded_params;
use super::session_lifecycle_requests::start_recording_app_server;
use super::session_lifecycle_requests::start_recording_app_server_with_history;
use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn builtin_permission_selection_adopts_server_settings() -> Result<()> {
    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
    let mut server = start_config_write_test_app_server(&app).await?;
    let started = server.start_thread(&app.config).await?;
    let thread_id = started.session.thread_id;
    app.enqueue_primary_thread_session(started.session, started.turns)
        .await?;
    // A previously selected custom profile may have left a profile-specific proxy cached.
    let home = tempdir()?;
    std::fs::write(
        home.path().join("config.toml"),
        r#"default_permissions = "proxied"
[features]
network_proxy = true
[permissions.proxied]
extends = ":workspace"
[permissions.proxied.network]
enabled = true
proxy_url = "http://127.0.0.1:43128"
"#,
    )?;
    let proxied = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .loader_overrides(codex_config::LoaderOverrides::without_managed_config_for_tests())
        .build()
        .await?;
    assert!(proxied.permissions.network.is_some());
    app.chat_widget
        .set_permission_network(proxied.permissions.network);
    app.chat_widget.handle_server_notification(
        turn_started_notification(thread_id, "turn-1"),
        /*replay_kind*/ None,
    );
    assert!(app.chat_widget.is_user_turn_pending_or_running());
    app.runtime_approvals_reviewer_override = Some(ApprovalsReviewer::AutoReview);
    let before = RuntimePermissionProfileOverride::from_config(app.chat_widget.config_ref());
    let profile_id = if before
        .active_permission_profile
        .as_ref()
        .is_some_and(|profile| profile.id == ":workspace")
    {
        ":read-only"
    } else {
        ":workspace"
    };
    while events.try_recv().is_ok() {}
    app.select_permission_profile(
        &mut server,
        PermissionProfileSelection {
            profile_id: profile_id.into(),
            approval_policy: Some(AskForApproval::OnRequest),
            approvals_reviewer: Some(ApprovalsReviewer::User),
            display_label: profile_id.into(),
        },
    )
    .await;
    assert_eq!(
        RuntimePermissionProfileOverride::from_config(app.chat_widget.config_ref()),
        before,
    );
    assert!(
        app.agents_overview
            .requested_permission_profiles
            .contains_key(&thread_id)
    );
    insta::assert_snapshot!(next_history_message(&mut events).replace(profile_id, "<PROFILE>"), @"• Permission selection requested: <PROFILE>");
    // Another permissions menu and prompt are queued as this selection closes.
    app.chat_widget
        .set_queue_autosend_suppressed(/*suppressed*/ true);
    for text in ["/permissions", "after permissions"] {
        app.chat_widget
            .restore_user_message_to_composer(text.into());
        app.chat_widget.handle_key_event(KeyCode::Tab.into());
    }
    app.chat_widget.handle_server_notification(
        turn_completed_notification(thread_id, "turn-1", TurnStatus::Completed),
        /*replay_kind*/ None,
    );
    app.app_event_tx.send(AppEvent::SettingsSelectionClosed);
    let settings = next_thread_settings_updated(&mut server, thread_id).await;
    app.enqueue_thread_notification(
        thread_id,
        ServerNotification::ThreadSettingsUpdated(settings),
    )
    .await?;
    assert!(!app.pending_server_profiles.contains_key(&thread_id));
    assert_eq!(
        (
            app.chat_widget
                .config_ref()
                .permissions
                .active_permission_profile(),
            app.config.approvals_reviewer
        ),
        (
            Some(ActivePermissionProfile::new(profile_id)),
            ApprovalsReviewer::User
        ),
    );
    assert_eq!(app.chat_widget.config_ref().permissions.network, None);
    assert_eq!(app.config.permissions.network, None);
    assert_eq!(
        app.runtime_permission_profile_override,
        Some(RuntimePermissionProfileOverride::from_config(&app.config))
    );
    let mut tui = crate::tui::test_support::make_test_tui()?;
    loop {
        let event = events.try_recv().expect("queued permissions menu");
        if matches!(event, AppEvent::OpenPermissionsPopup) {
            // Neither the old picker's close nor its confirmation may release this prompt.
            assert!(app.chat_widget.has_queued_follow_up_messages());
            break;
        }
        app.handle_event(&mut tui, &mut server, event).await?;
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn matching_builtin_permission_selection_submits_pending_prompt() -> Result<()> {
    let (mut app, mut events, mut ops) = make_test_app_with_channels().await;
    let mut server = start_config_write_test_app_server(&app).await?;
    let started = server.start_thread(&app.config).await?;
    app.enqueue_primary_thread_session(started.session, started.turns)
        .await?;
    app.chat_widget.initial_user_message =
        create_initial_user_message(Some("review this".to_string()), Vec::new(), Vec::new());
    let config = app.chat_widget.config_ref();
    let selection = PermissionProfileSelection {
        profile_id: config.permissions.active_permission_profile().unwrap().id,
        approval_policy: Some(config.permissions.approval_policy.value().into()),
        approvals_reviewer: Some(config.approvals_reviewer),
        display_label: "Current permissions".to_string(),
    };
    while events.try_recv().is_ok() {}
    app.select_permission_profile(&mut server, selection).await;
    assert!(app.chat_widget.initial_user_message.is_none());
    assert!(app.pending_server_profiles.is_empty());
    let Op::UserTurn { items, .. } = next_user_turn_op(&mut ops) else {
        panic!("expected initial user turn");
    };
    assert_eq!(
        items,
        vec![AppServerUserInput::Text {
            text: "review this".to_string(),
            text_elements: Vec::new(),
        }]
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn inline_server_permission_selection_holds_queued_prompt_until_confirmation() -> Result<()> {
    let (mut app, mut events, mut ops) = make_test_app_with_channels().await;
    let home = tempdir()?;
    std::fs::write(
        home.path().join("config.toml"),
        "default_permissions = \":workspace\"\n[permissions.server-only]\nextends = \":read-only\"\n",
    )?;
    let server_config = ConfigBuilder::default()
        .codex_home(home.path().into())
        .build()
        .await?;
    let mut server = crate::start_embedded_app_server_for_picker(&server_config).await?;
    let started = server.start_thread(&app.config).await?;
    let thread_id = started.session.thread_id;
    app.enqueue_primary_thread_session(started.session, started.turns)
        .await?;
    while events.try_recv().is_ok() {}
    while ops.try_recv().is_ok() {}

    app.chat_widget.select_permission_profile("server-only");
    assert!(app.chat_widget.queue_user_message_with_options(
        crate::chatwidget::UserMessage::from("use the selected profile"),
        crate::bottom_pane::QueuedInputAction::Plain,
        Vec::new(),
    ));
    let mut tui = crate::tui::test_support::make_test_tui()?;
    let mut settings_settled = false;
    for _ in 0..20 {
        if app
            .agents_overview
            .requested_permission_profiles
            .contains_key(&thread_id)
        {
            break;
        }
        let event = tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 2), events.recv())
            .await?
            .expect("app event channel should remain open");
        app.handle_event(&mut tui, &mut server, event).await?;
    }
    assert!(
        app.agents_overview
            .requested_permission_profiles
            .contains_key(&thread_id)
    );
    while let Ok(event) = events.try_recv() {
        app.handle_event(&mut tui, &mut server, event).await?;
    }
    assert!(ops.try_recv().is_err());

    let settings = next_thread_settings_updated(&mut server, thread_id).await;
    app.enqueue_thread_notification(
        thread_id,
        ServerNotification::ThreadSettingsUpdated(settings),
    )
    .await?;
    for _ in 0..20 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 2), events.recv())
            .await?
            .expect("app event channel should remain open");
        let settled = matches!(event, AppEvent::SettingsSelectionSettled);
        app.handle_event(&mut tui, &mut server, event).await?;
        if settled {
            settings_settled = true;
            break;
        }
    }
    assert!(settings_settled);

    let Op::UserTurn { items, .. } = next_user_turn_op(&mut ops) else {
        panic!("expected queued user turn");
    };
    assert_eq!(
        items,
        vec![AppServerUserInput::Text {
            text: "use the selected profile".to_string(),
            text_elements: Vec::new(),
        }]
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn custom_permission_selection_uses_server_definition_and_preserves_state_on_rejection()
-> Result<()> {
    boxed_future!(custom_permission_selection_scenario()).await
}

async fn custom_permission_selection_scenario() -> Result<()> {
    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
    app.config.ephemeral = false;
    let home = tempdir()?;
    std::fs::write(
        home.path().join("config.toml"),
        "default_permissions = \":workspace\"\n[permissions.shared]\nextends = \":workspace\"\n",
    )?;
    let server_config = ConfigBuilder::default()
        .codex_home(home.path().into())
        .build()
        .await?;
    let mut server = crate::start_embedded_app_server_for_picker(&server_config).await?;
    let started = server.start_thread(&app.config).await?;
    let thread_id = started.session.thread_id;
    app.enqueue_primary_thread_session(started.session, started.turns)
        .await?;
    let client_home = tempdir()?;
    std::fs::write(
        client_home.path().join("config.toml"),
        "default_permissions = \":workspace\"\n[permissions.shared]\nextends = \":danger-full-access\"\n",
    )?;
    let client_config = ConfigBuilder::default()
        .codex_home(client_home.path().into())
        .build()
        .await?;
    app.config.codex_home = client_config.codex_home;
    app.config.custom_permission_profiles = client_config.custom_permission_profiles;
    let selection = PermissionProfileSelection {
        profile_id: "shared".into(),
        approval_policy: Some(AskForApproval::Never),
        approvals_reviewer: None,
        display_label: "shared".into(),
    };
    app.chat_widget.handle_server_notification(
        turn_started_notification(thread_id, "turn-1"),
        /*replay_kind*/ None,
    );
    assert!(app.chat_widget.is_user_turn_pending_or_running());
    let before = RuntimePermissionProfileOverride::from_config(app.chat_widget.config_ref());
    app.select_permission_profile(&mut server, selection.clone())
        .await;
    assert_eq!(
        RuntimePermissionProfileOverride::from_config(app.chat_widget.config_ref()),
        before
    );
    let settings = next_thread_settings_updated(&mut server, thread_id).await;
    let expected = PermissionProfile::from_legacy_sandbox_policy_for_cwd(
        &settings.thread_settings.sandbox_policy.to_core(),
        settings.thread_settings.cwd.as_path(),
    );
    assert!(matches!(
        settings.thread_settings.sandbox_policy,
        codex_app_server_protocol::SandboxPolicy::WorkspaceWrite { .. }
    ));
    app.enqueue_thread_notification(
        thread_id,
        ServerNotification::ThreadSettingsUpdated(settings),
    )
    .await?;
    assert_eq!(
        (
            app.chat_widget
                .config_ref()
                .permissions
                .permission_profile(),
            app.chat_widget
                .config_ref()
                .permissions
                .approval_policy
                .value()
        ),
        (&expected, AskForApproval::Never.to_core())
    );
    std::fs::write(
        home.path().join("config.toml"),
        "default_permissions = \":workspace\"\n[permissions.shared]\nextends = \":read-only\"\n[permissions.other]\nextends = \":read-only\"\n",
    )?;
    app.select_permission_profile(
        &mut server,
        PermissionProfileSelection {
            approval_policy: None,
            ..selection.clone()
        },
    )
    .await;
    let settings = next_thread_settings_updated(&mut server, thread_id).await;
    assert!(matches!(
        settings.thread_settings.sandbox_policy,
        codex_app_server_protocol::SandboxPolicy::ReadOnly { .. }
    ));
    app.enqueue_thread_notification(
        thread_id,
        ServerNotification::ThreadSettingsUpdated(settings),
    )
    .await?;
    app.chat_widget.handle_server_notification(
        turn_completed_notification(thread_id, "turn-1", TurnStatus::Completed),
        /*replay_kind*/ None,
    );
    // Named profiles are re-read by the server, so re-selection still needs confirmation.
    app.select_permission_profile(&mut server, selection.clone())
        .await;
    assert!(app.pending_server_profiles.contains_key(&thread_id));
    let settings = next_thread_settings_updated(&mut server, thread_id).await;
    app.enqueue_thread_notification(
        thread_id,
        ServerNotification::ThreadSettingsUpdated(settings),
    )
    .await?;
    assert!(!app.reject_pending_permission_change());
    assert!(app.agents_overview.requested_permission_profiles.is_empty());
    // Another client's policy change must not leave a no-op request's old policy behind.
    server
        .thread_settings_update(ThreadSettingsUpdateParams {
            thread_id: thread_id.to_string(),
            approval_policy: Some(AskForApproval::OnRequest),
            ..Default::default()
        })
        .await?;
    let settings = next_thread_settings_updated(&mut server, thread_id).await;
    app.enqueue_thread_notification(
        thread_id,
        ServerNotification::ThreadSettingsUpdated(settings),
    )
    .await?;
    app.select_permission_profile(
        &mut server,
        PermissionProfileSelection {
            profile_id: "other".into(),
            approval_policy: None,
            approvals_reviewer: None,
            display_label: "other".into(),
        },
    )
    .await;
    assert_eq!(
        app.agents_overview.requested_permission_profiles[&thread_id].approval_policy,
        Some(AskForApproval::OnRequest)
    );
    let settings = next_thread_settings_updated(&mut server, thread_id).await;
    app.enqueue_thread_notification(
        thread_id,
        ServerNotification::ThreadSettingsUpdated(settings),
    )
    .await?;
    assert!(app.agents_overview.requested_permission_profiles.is_empty());
    let confirmed = RuntimePermissionProfileOverride::from_config(app.chat_widget.config_ref());
    let confirmed_config = app.config.clone();
    app.select_permission_profile(
        &mut server,
        PermissionProfileSelection {
            profile_id: "missing-on-server".into(),
            ..selection
        },
    )
    .await;
    assert_eq!(
        RuntimePermissionProfileOverride::from_config(app.chat_widget.config_ref()),
        confirmed
    );
    assert!(!app.pending_server_profiles.contains_key(&thread_id));
    assert_eq!(app.config, confirmed_config);
    assert_eq!(
        app.agents_overview
            .selected_permission_profiles
            .get(&thread_id)
            .map(String::as_str),
        Some("other")
    );
    // Materialize the task so /fork exercises persisted history with the confirmed profile.
    server.thread_inject_items(thread_id, vec![serde_json::from_value(serde_json::json!({
        "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "saved history"}]
    }))?]).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    while events.try_recv().is_ok() {}
    app.change_working_directory(&mut tui, &mut server, client_home.path().abs())
        .await;
    assert_eq!(app.chat_widget.thread_id(), Some(thread_id));
    insta::assert_snapshot!(next_history_message(&mut events), @"■ Permission profile has different settings.");
    let side = Box::pin(
        server.fork_side_thread(
            &app.local_settings,
            app.config.clone(),
            thread_id,
            app.agents_overview
                .requested_permission_profiles
                .get(&thread_id),
        ),
    )
    .await?;
    assert_eq!(
        (
            side.session.permission_profile,
            side.session.approval_policy
        ),
        (PermissionProfile::read_only(), AskForApproval::OnRequest)
    );
    app.cli_kv_overrides.push((
        "permissions.other.extends".into(),
        TomlValue::String(":danger-full-access".into()),
    ));
    // Both the accepted request and its confirmed descendant remain server-owned.
    for _ in 0..2 {
        let previous_thread_id = app.chat_widget.thread_id();
        Box::pin(app.handle_event(
            &mut tui,
            &mut server,
            AppEvent::ForkCurrentSession {
                name: None,
                placement: None,
            },
        ))
        .await?;
        assert_ne!(app.chat_widget.thread_id(), previous_thread_id);
        assert_eq!(
            RuntimePermissionProfileOverride::from_config(app.chat_widget.config_ref()),
            confirmed
        );
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn accepted_permissions_survive_directory_change_before_notification() -> Result<()> {
    for preserve_history in [false, true] {
        let (mut app, mut events, _ops) = make_test_app_with_channels().await;
        app.config.ephemeral = false;
        app.config
            .permissions
            .set_permission_profile(PermissionProfile::Disabled)?;
        let destination = app.config.codex_home.join("destination");
        std::fs::create_dir(&destination)?;
        crate::legacy_core::config::set_project_trust_level(
            app.config.codex_home.as_path(),
            &destination,
            codex_protocol::config_types::TrustLevel::Trusted,
        )
        .map_err(|err| color_eyre::eyre::eyre!(err.to_string()))?;
        let mut server = crate::start_embedded_app_server_for_picker(&app.config).await?;
        let started = server.start_thread(&app.config).await?;
        let source = started.session.thread_id;
        app.enqueue_primary_thread_session(started.session, started.turns)
            .await?;
        app.runtime_permission_profile_override =
            Some(RuntimePermissionProfileOverride::from_config(&app.config));
        if preserve_history {
            server.thread_inject_items(source, vec![serde_json::from_value(serde_json::json!({
                "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "saved history"}]
            }))?]).await?;
        }
        app.select_permission_profile(
            &mut server,
            PermissionProfileSelection {
                profile_id: ":read-only".into(),
                approval_policy: Some(AskForApproval::OnRequest),
                approvals_reviewer: Some(ApprovalsReviewer::User),
                display_label: "Read Only".into(),
            },
        )
        .await;
        assert!(
            app.agents_overview
                .requested_permission_profiles
                .contains_key(&source),
            "selection must be unconfirmed"
        );
        assert!(app.pending_server_profiles.contains_key(&source));
        // Destination constraints belong to the client; the server's result is authoritative.
        let requirements = app.config.codex_home.join("client-requirements.toml");
        std::fs::write(&requirements, "allowed_approval_policies = [\"never\"]")?;
        app.loader_overrides.system_requirements_path = Some(requirements.to_path_buf());
        assert_eq!(
            app.chat_widget
                .rollout_path()
                .as_deref()
                .is_some_and(rollout_path_is_resumable),
            preserve_history,
            "preserve_history={preserve_history}",
        );
        let mut tui = crate::tui::test_support::make_test_tui()?;
        app.change_working_directory(&mut tui, &mut server, destination.clone())
            .await;
        let mut fixture_errors = || {
            std::iter::from_fn(|| events.try_recv().ok())
                .take(64)
                .filter_map(|event| match event {
                    AppEvent::InsertHistoryCell(cell) => {
                        let lines = cell.display_lines(/*width*/ 500);
                        Some(
                            lines
                                .iter()
                                .flat_map(|line| {
                                    line.spans
                                        .iter()
                                        .flat_map(|span| span.content.chars())
                                        .chain(std::iter::once('\n'))
                                })
                                .take(2048)
                                .collect::<String>(),
                        )
                    }
                    _ => None,
                })
                .filter(|message| message.starts_with('\u{25a0}'))
                .take(8)
                .collect::<Vec<_>>()
        };
        assert_ne!(
            app.chat_widget.thread_id(),
            Some(source),
            "preserve_history={preserve_history}, errors={:?}",
            fixture_errors(),
        );
        let returned = app
            .primary_session_configured
            .as_ref()
            .expect("replacement session response");
        assert_eq!(
            Some(returned.thread_id),
            app.chat_widget.thread_id(),
            "preserve_history={preserve_history}",
        );
        assert_eq!(
            returned.runtime_workspace_roots,
            vec![destination.clone()],
            "preserve_history={preserve_history}, errors={:?}",
            fixture_errors(),
        );
        assert_eq!(app.config.workspace_roots, returned.runtime_workspace_roots);
        assert_eq!(
            (&app.config.cwd, app.config.permissions.permission_profile()),
            (&destination, &PermissionProfile::read_only()),
            "preserve_history={preserve_history}, errors={:?}",
            fixture_errors(),
        );
        assert_eq!(
            app.config.permissions.approval_policy.value(),
            AskForApproval::OnRequest.to_core(),
            "preserve_history={preserve_history}, errors={:?}",
            fixture_errors(),
        );
        assert_eq!(
            app.runtime_permission_profile_override,
            Some(RuntimePermissionProfileOverride::from_config(&app.config)),
            "preserve_history={preserve_history}, errors={:?}",
            fixture_errors(),
        );
        server.shutdown().await?;
    }
    Ok(())
}

fn read_only_permission_selection() -> PermissionProfileSelection {
    PermissionProfileSelection {
        profile_id: ":read-only".into(),
        approval_policy: Some(AskForApproval::OnRequest),
        approvals_reviewer: Some(ApprovalsReviewer::User),
        display_label: "Read Only".into(),
    }
}

fn assert_permission_selection_eq(
    actual: &PermissionProfileSelection,
    expected: &PermissionProfileSelection,
) {
    assert_eq!(
        (
            &actual.profile_id,
            actual.approval_policy,
            actual.approvals_reviewer,
            &actual.display_label,
        ),
        (
            &expected.profile_id,
            expected.approval_policy,
            expected.approvals_reviewer,
            &expected.display_label,
        ),
    );
}

#[tokio::test]
async fn directory_change_rejects_unconfirmed_permission_mismatches() -> Result<()> {
    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
    app.config.ephemeral = false;
    let destination = app.config.codex_home.join("destination");
    std::fs::create_dir(&destination)?;
    let (mut server, requests, proxy) = start_recording_app_server(
        &app.config,
        /*blocked_thread_list*/ None,
        /*failed_thread_name*/ None,
    )
    .await?;
    let started = server.start_thread(&app.config).await?;
    let source = started.session.thread_id;
    app.enqueue_primary_thread_session(started.session, started.turns)
        .await?;
    let source_cwd = app.config.cwd.clone();
    let mut tui = crate::tui::test_support::make_test_tui()?;

    for case in [
        "missing_request",
        "named_profile",
        "unknown_builtin",
        "profile_mismatch",
        "approval_mismatch",
        "reviewer_mismatch",
        "missing_approval",
        "missing_reviewer",
    ] {
        let mut pending = read_only_permission_selection();
        let mut requested = Some(pending.clone());
        match case {
            "missing_request" => requested = None,
            "named_profile" | "unknown_builtin" => {
                pending.profile_id = if case == "named_profile" {
                    "custom-profile"
                } else {
                    ":unknown-profile"
                }
                .to_string();
                requested = Some(pending.clone());
            }
            "profile_mismatch" => {
                requested.as_mut().unwrap().profile_id = ":workspace".into();
            }
            "approval_mismatch" => {
                requested.as_mut().unwrap().approval_policy = Some(AskForApproval::Never);
            }
            "reviewer_mismatch" => {
                requested.as_mut().unwrap().approvals_reviewer =
                    Some(ApprovalsReviewer::AutoReview);
            }
            "missing_approval" => {
                pending.approval_policy = None;
                requested = Some(pending.clone());
            }
            "missing_reviewer" => {
                pending.approvals_reviewer = None;
                requested = Some(pending.clone());
            }
            _ => unreachable!("known pending selection case"),
        }
        app.pending_server_profiles.insert(source, pending.clone());
        app.agents_overview
            .requested_permission_profiles
            .remove(&source);
        if let Some(requested) = &requested {
            app.agents_overview
                .requested_permission_profiles
                .insert(source, requested.clone());
        }
        requests.lock().expect("request recorder lock").clear();
        while events.try_recv().is_ok() {}

        app.change_working_directory(&mut tui, &mut server, destination.clone())
            .await;

        assert_eq!(app.chat_widget.thread_id(), Some(source), "{case}");
        assert_eq!(app.config.cwd, source_cwd, "{case}");
        assert!(app.runtime_working_directory_override.is_none(), "{case}");
        assert!(
            requests.lock().expect("request recorder lock").is_empty(),
            "{case} must be rejected before sending destination requests"
        );
        assert!(
            next_history_message(&mut events)
                .contains("Wait for permissions to update before changing directories."),
            "{case}"
        );
        assert_permission_selection_eq(&app.pending_server_profiles[&source], &pending);
        match requested {
            Some(requested) => assert_permission_selection_eq(
                &app.agents_overview.requested_permission_profiles[&source],
                &requested,
            ),
            None => assert!(
                !app.agents_overview
                    .requested_permission_profiles
                    .contains_key(&source)
            ),
        }
    }
    server.shutdown().await?;
    proxy.await??;
    Ok(())
}

#[tokio::test]
async fn accepted_builtin_permission_selection_still_blocks_reselection() -> Result<()> {
    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
    app.config
        .permissions
        .set_permission_profile(PermissionProfile::Disabled)?;
    let (mut server, requests, proxy) = start_recording_app_server(
        &app.config,
        /*blocked_thread_list*/ None,
        /*failed_thread_name*/ None,
    )
    .await?;
    let started = server.start_thread(&app.config).await?;
    let source = started.session.thread_id;
    app.enqueue_primary_thread_session(started.session, started.turns)
        .await?;
    let selection = read_only_permission_selection();
    app.select_permission_profile(&mut server, selection.clone())
        .await;
    let accepted_requests = recorded_params(&requests, "thread/settings/update");
    assert_eq!(accepted_requests.len(), 1);

    for profile_id in [":read-only", ":workspace"] {
        while events.try_recv().is_ok() {}
        app.select_permission_profile(
            &mut server,
            PermissionProfileSelection {
                profile_id: profile_id.into(),
                ..selection.clone()
            },
        )
        .await;

        assert_eq!(
            recorded_params(&requests, "thread/settings/update"),
            accepted_requests,
        );
        assert_permission_selection_eq(&app.pending_server_profiles[&source], &selection);
        assert_permission_selection_eq(
            &app.agents_overview.requested_permission_profiles[&source],
            &selection,
        );
        assert!(
            next_history_message(&mut events)
                .contains("Wait for permissions to update before changing permissions.")
        );
    }
    server.shutdown().await?;
    proxy.await??;
    Ok(())
}

#[tokio::test]
async fn accepted_permissions_directory_change_rejects_changed_server_policy() -> Result<()> {
    for preserve_history in [false, true] {
        assert_accepted_permissions_directory_change_rejected(
            preserve_history,
            /*invalid_requirements*/ false,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn accepted_permissions_directory_change_rejects_invalid_server_requirements() -> Result<()> {
    for preserve_history in [false, true] {
        assert_accepted_permissions_directory_change_rejected(
            preserve_history,
            /*invalid_requirements*/ true,
        )
        .await?;
    }
    Ok(())
}

async fn assert_accepted_permissions_directory_change_rejected(
    preserve_history: bool,
    invalid_requirements: bool,
) -> Result<()> {
    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
    app.config.ephemeral = false;
    app.config
        .permissions
        .set_permission_profile(PermissionProfile::Disabled)?;
    let destination = app.config.codex_home.join("destination");
    std::fs::create_dir(&destination)?;
    crate::legacy_core::config::set_project_trust_level(
        app.config.codex_home.as_path(),
        &destination,
        codex_protocol::config_types::TrustLevel::Trusted,
    )
    .map_err(|err| color_eyre::eyre::eyre!(err.to_string()))?;
    let requirements = app.config.codex_home.join("server-requirements.toml");
    std::fs::write(&requirements, "")?;
    let loader_overrides = LoaderOverrides {
        system_requirements_path: Some(requirements.to_path_buf()),
        ..LoaderOverrides::without_managed_config_for_tests()
    };
    let (mut server, requests, proxy) = start_recording_app_server_with_history(
        &app.config,
        HistoryCapabilities::Current,
        /*blocked_thread_list*/ None,
        /*failed_thread_name*/ None,
        crate::app_server_session::ThreadParamsMode::Embedded,
        loader_overrides,
    )
    .await?;
    let started = server.start_thread(&app.config).await?;
    let source = started.session.thread_id;
    app.enqueue_primary_thread_session(started.session, started.turns)
        .await?;
    app.runtime_permission_profile_override =
        Some(RuntimePermissionProfileOverride::from_config(&app.config));
    if preserve_history {
        server
            .thread_inject_items(
                source,
                vec![serde_json::from_value(serde_json::json!({
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "saved history"}],
                }))?],
            )
            .await?;
    }
    let selection = read_only_permission_selection();
    app.select_permission_profile(&mut server, selection.clone())
        .await;
    assert_permission_selection_eq(&app.pending_server_profiles[&source], &selection);
    assert_permission_selection_eq(
        &app.agents_overview.requested_permission_profiles[&source],
        &selection,
    );
    let source_cwd = app.config.cwd.clone();
    let source_permissions = RuntimePermissionProfileOverride::from_config(&app.config);
    assert_eq!(
        app.chat_widget
            .rollout_path()
            .as_deref()
            .is_some_and(rollout_path_is_resumable),
        preserve_history,
        "the fixture must exercise both new-thread and fork destination validation",
    );
    std::fs::write(
        &requirements,
        if invalid_requirements {
            "allowed_approval_policies = []"
        } else {
            "allowed_approval_policies = [\"never\"]"
        },
    )?;
    requests.lock().expect("request recorder lock").clear();
    while events.try_recv().is_ok() {}
    let mut tui = crate::tui::test_support::make_test_tui()?;

    app.change_working_directory(&mut tui, &mut server, destination)
        .await;

    assert_eq!(app.chat_widget.thread_id(), Some(source));
    assert_eq!(app.config.cwd, source_cwd);
    assert_eq!(
        RuntimePermissionProfileOverride::from_config(&app.config),
        source_permissions,
    );
    assert_eq!(
        app.runtime_permission_profile_override,
        Some(source_permissions),
    );
    assert!(app.runtime_working_directory_override.is_none());
    assert_permission_selection_eq(&app.pending_server_profiles[&source], &selection);
    assert_permission_selection_eq(
        &app.agents_overview.requested_permission_profiles[&source],
        &selection,
    );

    let starts = recorded_params(&requests, "thread/start");
    let forks = recorded_params(&requests, "thread/fork");
    let unsubscribes = recorded_params(&requests, "thread/unsubscribe");
    let archives = recorded_params(&requests, "thread/archive");
    let message = next_history_message(&mut events);
    if invalid_requirements {
        assert!(message.contains("allowed_approval_policies"), "{message}");
        if preserve_history {
            assert!(message.contains("Failed to change:"), "{message}");
            assert_eq!(forks.len(), 1);
        } else {
            assert!(message.contains("Cannot read defaults:"), "{message}");
            assert!(!recorded_params(&requests, "config/read").is_empty());
            assert!(forks.is_empty());
        }
        assert!(starts.is_empty());
        assert!(unsubscribes.is_empty());
        assert!(archives.is_empty());
    } else {
        assert!(
            message.contains("Requested directory or permissions not applied."),
            "{message}"
        );
        let transition = if preserve_history {
            assert!(starts.is_empty());
            assert_eq!(forks.len(), 1);
            &forks[0]
        } else {
            assert!(forks.is_empty());
            assert_eq!(starts.len(), 1);
            &starts[0]
        };
        assert_eq!(transition["permissions"], ":read-only");
        assert_eq!(transition["approvalPolicy"], "on-request");
        assert_eq!(transition["approvalsReviewer"], "user");
        assert_eq!(unsubscribes.len(), 1);
        assert_ne!(unsubscribes[0]["threadId"], source.to_string());
        if preserve_history {
            assert_eq!(archives.len(), 1);
            assert_eq!(archives[0]["threadId"], unsubscribes[0]["threadId"]);
        } else {
            assert!(archives.is_empty());
        }
    }
    server.shutdown().await?;
    proxy.await??;
    Ok(())
}
