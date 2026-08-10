use super::*;
use pretty_assertions::assert_eq;
use serde_json::Value;

#[tokio::test]
async fn thread_resume_uses_legacy_checkpoint_without_hydrating_missing_predecessor() -> Result<()>
{
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    mock_responses_config(&server.uri())
        .enable_feature(Feature::FastMode)
        .write(codex_home.path())?;
    let thread_id = create_fake_rollout(
        codex_home.path(),
        "2025-01-05T11-59-00",
        "2025-01-05T11:59:00Z",
        "historical predecessor",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let thread_id_parsed = ThreadId::from_string(&thread_id)?;
    let active_path = rollout_path(codex_home.path(), "2025-01-05T11-59-00", &thread_id);
    let predecessor_path = codex_home.path().join("deleted-predecessor.jsonl");
    std::fs::rename(&active_path, &predecessor_path)?;
    let mut session_meta = read_session_meta_line(&predecessor_path).await?;
    let predecessor_segment_id = SegmentId::new();
    session_meta.meta.segment_id = Some(SegmentId::new());
    let head = RolloutLine {
        timestamp: "2025-01-05T12:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::SessionMeta(session_meta),
    };
    std::fs::write(&active_path, format!("{}\n", serde_json::to_string(&head)?))?;
    append_rollout_item_to_path(
        &active_path,
        &RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: predecessor_path.clone(),
            thread_id: Some(thread_id_parsed),
            rollout_timestamp: None,
            segment_id: Some(predecessor_segment_id),
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    )
    .await?;
    let window_id = Uuid::now_v7().to_string();
    let mut checkpoint_settings = test_thread_settings_snapshot();
    let persisted_cwd = AbsolutePathBuf::try_from(codex_home.path().join("persisted-cwd"))?;
    let persisted_workspace_root =
        AbsolutePathBuf::try_from(codex_home.path().join("persisted-workspace"))?;
    let persisted_environment_cwd =
        AbsolutePathBuf::try_from(codex_home.path().join("persisted-environment"))?;
    let persisted_profile_workspace_root =
        AbsolutePathBuf::try_from(codex_home.path().join("persisted-profile-workspace"))?;
    std::fs::create_dir_all(persisted_cwd.as_path())?;
    std::fs::create_dir_all(persisted_workspace_root.as_path())?;
    std::fs::create_dir_all(persisted_environment_cwd.as_path())?;
    std::fs::create_dir_all(persisted_profile_workspace_root.as_path())?;
    checkpoint_settings.model = "gpt-5.6-sol".to_string();
    checkpoint_settings.service_tier = Some("priority".to_string());
    checkpoint_settings.approval_policy = codex_protocol::protocol::AskForApproval::OnRequest;
    checkpoint_settings.approvals_reviewer = ProtocolApprovalsReviewer::AutoReview;
    checkpoint_settings.permission_profile = codex_protocol::models::PermissionProfile::read_only();
    let persisted_active_permission_profile = codex_protocol::models::ActivePermissionProfile {
        id: "persisted-profile".to_string(),
        extends: None,
    };
    let expected_active_permission_profile = codex_app_server_protocol::ActivePermissionProfile {
        id: persisted_active_permission_profile.id.clone(),
        extends: persisted_active_permission_profile.extends.clone(),
    };
    checkpoint_settings.active_permission_profile =
        Some(persisted_active_permission_profile.clone());
    checkpoint_settings.cwd = persisted_cwd.clone();
    checkpoint_settings.workspace_roots = Some(vec![persisted_workspace_root.clone()]);
    checkpoint_settings.environments =
        Some(codex_protocol::protocol::TurnEnvironmentSelections::new(
            persisted_cwd.clone(),
            vec![codex_protocol::protocol::TurnEnvironmentSelection {
                environment_id: codex_exec_server::LOCAL_ENVIRONMENT_ID.to_string(),
                cwd: codex_utils_path_uri::PathUri::from_abs_path(&persisted_environment_cwd),
                workspace_roots: vec![codex_utils_path_uri::PathUri::from_abs_path(
                    &persisted_environment_cwd,
                )],
            }],
        ));
    checkpoint_settings.profile_workspace_roots = Some(vec![persisted_profile_workspace_root]);
    checkpoint_settings.windows_sandbox_level =
        Some(codex_protocol::config_types::WindowsSandboxLevel::RestrictedToken);
    checkpoint_settings.reasoning_effort = Some(ReasoningEffort::High);
    checkpoint_settings.reasoning_summary =
        Some(codex_protocol::config_types::ReasoningSummary::Auto);
    checkpoint_settings.personality = Some(Personality::Friendly);
    checkpoint_settings.collaboration_mode = CollaborationMode {
        mode: ModeKind::Plan,
        settings: Settings {
            model: checkpoint_settings.model.clone(),
            reasoning_effort: checkpoint_settings.reasoning_effort.clone(),
            developer_instructions: Some("persisted plan instructions".to_string()),
        },
    };
    let checkpoint_token_usage = TokenUsageInfo {
        total_token_usage: TokenUsage {
            input_tokens: 120,
            cached_input_tokens: 20,
            cache_write_input_tokens: 0,
            output_tokens: 30,
            reasoning_output_tokens: 10,
            total_tokens: 150,
            codex_rollout_budget_units: None,
        },
        last_token_usage: TokenUsage {
            input_tokens: 70,
            cached_input_tokens: 10,
            cache_write_input_tokens: 0,
            output_tokens: 20,
            reasoning_output_tokens: 5,
            total_tokens: 90,
            codex_rollout_budget_units: None,
        },
        model_context_window: Some(200_000),
    };
    let checkpoint = test_segment_state_checkpoint_with_current_state(
        CompactedItem {
            message: "checkpoint-local model history".to_string(),
            replacement_history: Some(vec![ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "checkpoint-only current message".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }]),
            window_number: Some(2),
            first_window_id: Some(window_id.clone()),
            previous_window_id: None,
            window_id: Some(window_id),
            segment_state_checkpoint: None,
        },
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        ThreadSettingsAppliedEvent {
            thread_settings: checkpoint_settings.clone(),
        },
        TokenCountEvent {
            info: Some(checkpoint_token_usage.clone()),
            rate_limits: None,
        },
    )?;
    for item in checkpoint.into_items() {
        append_rollout_item_to_path(&active_path, &item).await?;
    }
    std::fs::remove_file(predecessor_path)?;

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let hydrated_resume_id = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.clone(),
            exclude_turns: false,
            ..Default::default()
        })
        .await?;
    let hydrated_error: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(hydrated_resume_id)),
    )
    .await??;
    assert!(
        hydrated_error.error.message.contains("deleted-predecessor"),
        "unexpected full-history error: {}",
        hydrated_error.error.message
    );

    let checkpoint_resume_id = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        model,
        model_provider,
        service_tier,
        cwd,
        runtime_workspace_roots,
        approval_policy,
        approvals_reviewer,
        sandbox,
        active_permission_profile,
        reasoning_effort,
        ..
    } = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_response(checkpoint_resume_id),
    )
    .await??;
    assert_eq!(thread.id, thread_id);
    assert!(thread.turns.is_empty());
    assert_eq!(
        (
            model,
            model_provider,
            service_tier,
            cwd,
            runtime_workspace_roots,
            approval_policy,
            approvals_reviewer,
            sandbox,
            active_permission_profile,
            reasoning_effort,
        ),
        (
            checkpoint_settings.model,
            checkpoint_settings.model_provider_id,
            checkpoint_settings.service_tier,
            persisted_cwd,
            vec![persisted_environment_cwd.clone()],
            AskForApproval::OnRequest,
            ApprovalsReviewer::AutoReview,
            codex_app_server_protocol::SandboxPolicy::ReadOnly {
                network_access: false,
            },
            Some(expected_active_permission_profile),
            Some(ReasoningEffort::High),
        )
    );

    let note = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_notification_message("thread/tokenUsage/updated"),
    )
    .await??;
    let parsed: ServerNotification = note.try_into()?;
    let ServerNotification::ThreadTokenUsageUpdated(notification) = parsed else {
        panic!("expected thread/tokenUsage/updated notification");
    };
    assert_eq!(notification.thread_id, thread.id);
    assert_eq!(notification.token_usage.total.total_tokens, 150);
    assert_eq!(notification.token_usage.last.total_tokens, 90);
    assert_eq!(notification.token_usage.model_context_window, Some(200_000));

    timeout(DEFAULT_READ_TIMEOUT, app.shutdown_gracefully()).await??;
    let explicit_cwd = AbsolutePathBuf::try_from(codex_home.path().join("explicit-cwd"))?;
    let explicit_workspace_root =
        AbsolutePathBuf::try_from(codex_home.path().join("explicit-workspace"))?;
    std::fs::create_dir_all(explicit_cwd.as_path())?;
    std::fs::create_dir_all(explicit_workspace_root.as_path())?;
    let mut explicit_app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let explicit_resume_id = explicit_app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.clone(),
            cwd: Some(explicit_cwd.as_path().to_string_lossy().into_owned()),
            runtime_workspace_roots: Some(vec![explicit_workspace_root.clone()]),
            developer_instructions: Some("explicit developer instructions".to_string()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread: explicit_thread,
        cwd,
        runtime_workspace_roots,
        ..
    } = timeout(
        DEFAULT_READ_TIMEOUT,
        explicit_app.read_response(explicit_resume_id),
    )
    .await??;
    assert_eq!(
        (cwd, runtime_workspace_roots),
        (explicit_cwd.clone(), vec![explicit_workspace_root.clone()])
    );
    timeout(
        DEFAULT_READ_TIMEOUT,
        explicit_app.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: explicit_thread.id,
            input: vec![UserInput::Text {
                text: "verify explicit checkpoint overrides".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    let model_requests = server
        .received_requests()
        .await
        .expect("model requests after explicit resume");
    let request = model_requests
        .iter()
        .rev()
        .find(|request| request.url.path().ends_with("/responses"))
        .expect("explicit resume model request");
    let request_text = serde_json::to_string(&request.body_json::<Value>()?)?;
    assert!(request_text.contains("explicit developer instructions"));
    assert!(request_text.contains(&explicit_cwd.as_path().to_string_lossy().to_string()));
    assert!(
        request_text.contains(
            &explicit_workspace_root
                .as_path()
                .to_string_lossy()
                .to_string()
        )
    );
    assert!(
        !request_text.contains(
            &persisted_environment_cwd
                .as_path()
                .to_string_lossy()
                .to_string()
        )
    );

    Ok(())
}
