use super::super::ForkGoalContinuation;
use super::super::ForkPermissionMode;
use super::super::ResumeModelSettings;
use super::super::ThreadParamsMode;
use crate::legacy_core::config::Config;
use crate::legacy_core::config::ConfigBuilder;
use app_test_support::create_fake_paginated_rollout;
use app_test_support::create_fake_rollout;
use app_test_support::rollout_path;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_features::Feature;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::models::ResponseItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use color_eyre::eyre::Result;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tempfile::TempDir;

async fn build_config(temp_dir: &TempDir) -> Config {
    ConfigBuilder::default()
        .codex_home(temp_dir.path().to_path_buf())
        .build()
        .await
        .expect("config should build")
}

fn poison_legacy_rollout_for_goal_supervisor_repair(
    codex_home: &std::path::Path,
    filename_ts: &str,
    thread_id: ThreadId,
) -> Result<()> {
    let path = rollout_path(codex_home, filename_ts, thread_id.to_string().as_str());
    let mut lines = std::fs::read_to_string(path.as_path())?
        .lines()
        .map(codex_rollout::parse_rollout_line)
        .collect::<Result<Vec<_>, _>>()?;
    let RolloutItem::SessionMeta(session_meta) = &mut lines[0].item else {
        panic!("expected session metadata");
    };
    session_meta.meta.segment_id = Some(SegmentId::new());
    session_meta.meta.cli_version = "0.148.0-alpha.6+frodex.0".to_string();
    for (ordinal, line) in lines.iter_mut().enumerate() {
        line.ordinal = Some(ordinal as u64);
    }
    let delivery_ordinal = lines.len() as u64;
    lines.push(RolloutLine {
        timestamp: "2026-08-17T08:00:01Z".to_string(),
        ordinal: Some(delivery_ordinal),
        item: RolloutItem::InterAgentCommunicationMetadata { trigger_turn: true },
    });
    lines.push(RolloutLine {
        timestamp: "2026-08-17T08:00:02Z".to_string(),
        ordinal: Some(delivery_ordinal + 1),
        item: RolloutItem::ResponseItem(
            ResponseItem::AgentMessage {
                id: Some(codex_protocol::ResponseItemId::from_server(
                    "amsg_01900000-0000-7000-8000-000000000017".to_string(),
                )),
                author: "/root/goal_supervisor".to_string(),
                recipient: "/root".to_string(),
                content: vec![
                    AgentMessageInputContent::InputText {
                        text: "Message Type: NEW_TASK\nTask name: /root\nSender: /root/goal_supervisor\nPayload:\n"
                            .to_string(),
                    },
                    AgentMessageInputContent::EncryptedContent {
                        encrypted_content: "synthetic poisoned supervisor instruction".to_string(),
                    },
                ],
                internal_chat_message_metadata_passthrough: Some(
                    InternalChatMessageMetadataPassthrough {
                        turn_id: Some(
                            "01900000-0000-7000-8000-000000000018".to_string(),
                        ),
                        ..Default::default()
                    },
                ),
            }
            .into(),
        ),
    });
    let mut bytes = Vec::new();
    for line in lines {
        serde_json::to_writer(&mut bytes, &line)?;
        bytes.push(b'\n');
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

#[tokio::test]
async fn remote_resume_restores_saved_server_profile_without_permission_overrides() -> Result<()> {
    let home = tempfile::tempdir()?;
    std::fs::write(
        home.path().join("config.toml"),
        "default_permissions = \":workspace\"\n[permissions.server-only]\nextends = \":read-only\"\n",
    )?;
    let config = build_config(&home).await;
    let client_config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .cli_overrides(vec![(
            "default_permissions".to_string(),
            ":workspace".into(),
        )])
        .harness_overrides(crate::legacy_core::config::ConfigOverrides {
            default_permissions: Some(":read-only".into()),
            ..Default::default()
        })
        .build()
        .await?;
    let thread_id = ThreadId::from_string(
        &create_fake_rollout(
            home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    let mut server = crate::start_embedded_app_server_for_picker(&config).await?;
    server.thread_params_mode = ThreadParamsMode::Remote;
    let local_settings = crate::local_settings::LocalSettings::from(&client_config);
    let initial = server
        .resume_thread(
            &local_settings,
            client_config.clone(),
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    assert_eq!(
        initial
            .session
            .active_permission_profile
            .map(|profile| profile.id),
        Some(":workspace".to_string())
    );
    server
        .thread_settings_update(codex_app_server_protocol::ThreadSettingsUpdateParams {
            thread_id: thread_id.to_string(),
            permissions: Some("server-only".into()),
            approval_policy: Some(codex_app_server_protocol::AskForApproval::Never),
            approvals_reviewer: Some(codex_app_server_protocol::ApprovalsReviewer::AutoReview),
            ..Default::default()
        })
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(codex_app_server_client::AppServerEvent::ServerNotification(notification)) =
                server.next_event().await
                && let codex_app_server_protocol::ServerNotification::ThreadSettingsUpdated(
                    settings,
                ) = *notification
                && settings.thread_id == thread_id.to_string()
                && settings
                    .thread_settings
                    .active_permission_profile
                    .as_ref()
                    .is_some_and(|profile| profile.id == "server-only")
            {
                break;
            }
        }
    })
    .await?;
    server.shutdown().await?;

    let mut server = crate::start_embedded_app_server_for_picker(&config).await?;
    server.thread_params_mode = ThreadParamsMode::Remote;
    let resumed = server
        .resume_thread(
            &local_settings,
            client_config.clone(),
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    assert_eq!(
        resumed
            .session
            .active_permission_profile
            .as_ref()
            .map(|profile| profile.id.as_str()),
        Some("server-only")
    );
    assert_eq!(
        resumed.session.approval_policy,
        codex_app_server_protocol::AskForApproval::Never
    );
    assert_eq!(
        resumed.session.approvals_reviewer,
        codex_protocol::config_types::ApprovalsReviewer::AutoReview
    );
    // A locally remembered profile may have been removed since selection.
    let stale_selection = crate::app_event::PermissionProfileSelection {
        profile_id: "removed-profile".into(),
        approval_policy: None,
        approvals_reviewer: None,
        display_label: "removed-profile".into(),
    };
    let forked = server
        .fork_thread_at(
            &local_settings,
            client_config.clone(),
            thread_id,
            /*last_turn_id*/ None,
            /*before_turn_id*/ None,
            ForkGoalContinuation::StartIfIdle,
            Some(&stale_selection),
        )
        .await?;
    assert_eq!(
        forked
            .session
            .active_permission_profile
            .as_ref()
            .map(|profile| profile.id.as_str()),
        Some("server-only")
    );
    assert_eq!(
        forked.session.approval_policy,
        codex_app_server_protocol::AskForApproval::Never
    );
    assert_eq!(
        forked.session.approvals_reviewer,
        codex_protocol::config_types::ApprovalsReviewer::AutoReview
    );
    let side = server
        .fork_side_thread(&local_settings, client_config, thread_id)
        .await?;
    assert_eq!(
        side.session.active_permission_profile.unwrap().id,
        "server-only"
    );
    let explicit_config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .harness_overrides(crate::legacy_core::config::ConfigOverrides {
            sandbox_mode: Some(codex_protocol::config_types::SandboxMode::ReadOnly),
            approval_policy: Some(codex_protocol::protocol::AskForApproval::OnRequest),
            ..Default::default()
        })
        .build()
        .await?;
    let explicit_fork = server
        .fork_thread_with_permission_mode(
            &local_settings,
            explicit_config,
            thread_id,
            ForkPermissionMode::OverrideFromCurrentConfig,
        )
        .await?;
    assert_eq!(
        explicit_fork.session.approval_policy,
        codex_app_server_protocol::AskForApproval::OnRequest
    );
    assert_eq!(
        explicit_fork.session.permission_profile,
        codex_protocol::models::PermissionProfile::read_only()
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn viewing_thread_reads_history_without_resuming_it() -> Result<()> {
    let codex_home = tempfile::tempdir()?;
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    let next_request_id = app_server.next_request_id;
    let (viewed, notice) = app_server
        .read_thread_for_viewing(
            &config,
            &crate::local_settings::LocalSettings::from(&config),
            thread_id,
        )
        .await?;

    assert_eq!(notice, None);
    assert_eq!(viewed.session.thread_id, thread_id);
    assert!(!viewed.turns.is_empty());
    assert!(!viewed.blocks_direct_input);
    assert!(app_server.next_request_id > next_request_id);
    app_server.shutdown().await?;
    Ok(())
}

#[test]
fn only_active_writer_failures_offer_read_only_view() {
    let conflict = color_eyre::eyre::eyre!("thread already has an active writer (code -32600)");
    let unrelated = color_eyre::eyre::eyre!("thread not found (code -32600)");
    assert!(crate::app_server_session::is_active_writer_error(&conflict));
    assert!(!crate::app_server_session::is_active_writer_error(
        &unrelated
    ));
}

#[test]
fn migrated_resume_preserves_history_mode_after_picker_server_replacement() -> Result<()> {
    const TEST_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;

    std::thread::Builder::new()
        .name("migrated-resume-history-mode".to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(
                    migrated_resume_preserves_history_mode_after_picker_server_replacement_inner(),
                )
        })?
        .join()
        .map_err(|_| color_eyre::eyre::eyre!("migrated resume test thread panicked"))?
}

async fn migrated_resume_preserves_history_mode_after_picker_server_replacement_inner() -> Result<()>
{
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    let mut picker_app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    let history_mode = picker_app_server
        .thread_read(thread_id, /*include_turns*/ false)
        .await?
        .history_mode;
    assert_eq!(history_mode, ThreadHistoryMode::Paginated);
    picker_app_server.shutdown().await?;

    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, history_mode);
    let next_request_id = app_server.next_request_id;
    let resumed = app_server
        .resume_thread(
            &crate::local_settings::LocalSettings::from(&config),
            config,
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;

    assert_eq!(app_server.next_request_id, next_request_id + 3);
    assert!(!resumed.turns.is_empty());
    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn embedded_legacy_resume_does_not_reenter_rollout_maintenance_lock() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let filename_ts = "2026-08-17T08-00-00";
    let thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            filename_ts,
            "2026-08-17T08:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    poison_legacy_rollout_for_goal_supervisor_repair(codex_home.path(), filename_ts, thread_id)?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
    let local_settings = crate::local_settings::LocalSettings::from(&config);

    let resumed = tokio::time::timeout(
        Duration::from_secs(10),
        app_server.resume_thread(
            &local_settings,
            config,
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        ),
    )
    .await
    .expect("embedded resume must not wait for its own maintenance lock")?;

    assert_eq!(resumed.session.thread_id, thread_id);
    assert!(!resumed.turns.is_empty());
    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn background_migration_disables_cached_legacy_resume_shortcut() -> Result<()> {
    for (startup_enabled, workspace_enabled) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let codex_home = tempfile::tempdir().expect("tempdir");
        // Keep the large setup futures off the test thread's stack.
        let mut config = Box::pin(build_config(&codex_home)).await;
        // The four policy combinations must not inherit the enabled product default.
        config
            .features
            .disable(Feature::BackgroundPaginatedRolloutMigration)?;
        let legacy_thread_id = ThreadId::from_string(
            &create_fake_rollout(
                codex_home.path(),
                "2025-01-05T12-00-00",
                "2025-01-05T12:00:00Z",
                "Saved legacy user message",
                Some(config.model_provider_id.as_str()),
                /*git_info*/ None,
            )
            .expect("create legacy rollout"),
        )?;
        let mut startup_config = config.clone();
        if startup_enabled {
            startup_config
                .features
                .enable(Feature::BackgroundPaginatedRolloutMigration)?;
        }
        let mut resume_config = config.clone();
        if workspace_enabled {
            resume_config
                .features
                .enable(Feature::BackgroundPaginatedRolloutMigration)?;
        }
        let server_config = if workspace_enabled {
            &resume_config
        } else {
            &startup_config
        };
        let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(server_config))
            .await?
            .with_startup_config(&startup_config);
        if startup_enabled || workspace_enabled {
            // Finish a real migration before installing the stale picker hint. Merely
            // marking legacy response records as paginated produces no visible turns.
            assert_eq!(
                app_server
                    .thread_read(legacy_thread_id, /*include_turns*/ false)
                    .await?
                    .history_mode,
                ThreadHistoryMode::Paginated
            );
        }
        app_server.remember_thread_history_mode(legacy_thread_id, ThreadHistoryMode::Legacy);
        let local_settings = crate::local_settings::LocalSettings::from(&resume_config);
        let next_request_id = app_server.next_request_id;
        // Keep the large resume future off the Windows test thread's stack.
        let legacy = Box::pin(app_server.resume_thread(
            &local_settings,
            resume_config.clone(),
            legacy_thread_id,
            ResumeModelSettings::RestoreFromThread,
        ))
        .await?;
        let expected_requests = if startup_enabled || workspace_enabled {
            3
        } else {
            2
        };
        assert_eq!(
            app_server.next_request_id,
            next_request_id + expected_requests,
            "startup_enabled={startup_enabled}, workspace_enabled={workspace_enabled}"
        );
        assert!(!legacy.turns.is_empty());
        assert_eq!(
            app_server.history_pagination[&legacy_thread_id].history_mode,
            if startup_enabled || workspace_enabled {
                ThreadHistoryMode::Paginated
            } else {
                ThreadHistoryMode::Legacy
            }
        );
        app_server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn rollout_maintenance_contention_disables_cached_legacy_resume_shortcut() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
    let maintenance_guard = codex_rollout::try_acquire_rollout_maintenance_lock(codex_home.path())?
        .expect("acquire rollout maintenance lock");
    let release_maintenance = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(maintenance_guard);
    });
    let next_request_id = app_server.next_request_id;

    let resumed = app_server
        .resume_thread(
            &crate::local_settings::LocalSettings::from(&config),
            config,
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    release_maintenance.await.expect("release maintenance lock");

    assert_eq!(app_server.next_request_id, next_request_id + 3);
    assert_eq!(resumed.session.thread_id, thread_id);
    assert_eq!(
        app_server
            .history_pagination
            .get(&thread_id)
            .map(|state| state.history_mode),
        Some(ThreadHistoryMode::Paginated)
    );

    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stale_legacy_history_mode_is_revalidated_before_resume() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let mut config = build_config(&codex_home).await;
    config
        .features
        .disable(Feature::BackgroundPaginatedRolloutMigration)?;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
    let next_request_id = app_server.next_request_id;

    let resumed = app_server
        .resume_thread(
            &crate::local_settings::LocalSettings::from(&config),
            config.clone(),
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    assert_eq!(resumed.session.thread_id, thread_id);
    assert!(app_server.next_request_id >= next_request_id + 4);
    assert_eq!(
        app_server
            .history_pagination
            .get(&thread_id)
            .map(|state| state.history_mode),
        Some(ThreadHistoryMode::Paginated)
    );

    let missing_thread_id = ThreadId::new();
    app_server.remember_thread_history_mode(missing_thread_id, ThreadHistoryMode::Legacy);
    let next_request_id = app_server.next_request_id;
    assert!(
        app_server
            .resume_thread(
                &crate::local_settings::LocalSettings::from(&config),
                config,
                missing_thread_id,
                ResumeModelSettings::RestoreFromThread
            )
            .await
            .is_err()
    );
    assert_eq!(app_server.next_request_id, next_request_id + 2);

    app_server.shutdown().await?;
    Ok(())
}
