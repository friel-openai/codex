use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn reference_backed_fork_cold_resumes_from_its_local_checkpoint() -> Result<()> {
    const LIST_CALL_ID: &str = "list-current-fork-agents";
    let server = create_mock_responses_server_sequence_unchecked(vec![
        responses::sse(vec![
            responses::ev_response_created("list-current-fork-agents-response"),
            responses::ev_function_call_with_namespace(
                LIST_CALL_ID,
                "collaboration",
                "list_agents",
                "{}",
            ),
            responses::ev_completed("list-current-fork-agents-response"),
        ]),
        responses::sse(vec![
            responses::ev_response_created("finish-fork-turn"),
            responses::ev_assistant_message("finish-fork-message", "Done"),
            responses::ev_completed("finish-fork-turn"),
        ]),
    ])
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::MultiAgentV2)
        .write(codex_home.path())?;
    let source_thread_id = create_fake_paginated_rollout(
        codex_home.path(),
        "2025-01-05T11-58-00",
        "2025-01-05T11:58:00Z",
        "source history retained by the checkpoint",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let source_path = rollout_path(
        codex_home.path(),
        "2025-01-05T11-58-00",
        source_thread_id.as_str(),
    );
    let source_contents = std::fs::read_to_string(&source_path)?;
    let mut source_lines = source_contents
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<serde_json::Result<Vec<_>>>()?;
    source_lines[0]["payload"]["multi_agent_version"] =
        serde_json::to_value(MultiAgentVersion::V2)?;
    std::fs::write(
        &source_path,
        format!(
            "{}\n",
            source_lines
                .into_iter()
                .map(|line| line.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        ),
    )?;
    append_rollout_item_to_path(
        &source_path,
        &RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: "source-checkpoint-turn".to_string(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })),
    )
    .await?;
    append_rollout_item_to_path(
        &source_path,
        &RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: "source checkpoint turn".to_string(),
            ..Default::default()
        })),
    )
    .await?;
    append_rollout_item_to_path(
        &source_path,
        &RolloutItem::TurnContext(TurnContextItem {
            turn_id: Some("source-checkpoint-turn".to_string()),
            cwd: codex_home.path().abs(),
            workspace_roots: None,
            current_date: None,
            timezone: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: None,
            sandbox_policy: SandboxPolicy::new_read_only_policy(),
            permission_profile: None,
            network: None,
            file_system_sandbox_policy: None,
            model: "gpt-5.6-sol".to_string(),
            comp_hash: None,
            personality: None,
            collaboration_mode: None,
            multi_agent_version: Some(MultiAgentVersion::V2),
            multi_agent_mode: None,
            realtime_active: None,
            effort: None,
            service_tier: None,
            model_profile: None,
            summary: ReasoningSummary::Auto,
        }),
    )
    .await?;
    append_rollout_item_to_path(
        &source_path,
        &RolloutItem::WorldState(WorldStateItem::full(json!({
            "environments": {
                "environments": {},
                "current_date": null,
                "timezone": null,
                "network": null,
                "filesystem": null,
                "subagents": "- /root/stale: working"
            }
        }))),
    )
    .await?;
    let source_token_usage = TokenUsageInfo {
        total_token_usage: TokenUsage {
            total_tokens: 123,
            ..TokenUsage::default()
        },
        last_token_usage: TokenUsage {
            total_tokens: 45,
            ..TokenUsage::default()
        },
        model_context_window: Some(128_000),
    };
    let source_rate_limits = RateLimitSnapshot {
        limit_id: Some("codex".to_string()),
        limit_name: Some("Codex".to_string()),
        primary: Some(RateLimitWindow {
            used_percent: 25.0,
            window_minutes: Some(300),
            resets_at: Some(1_700),
        }),
        secondary: Some(RateLimitWindow {
            used_percent: 12.5,
            window_minutes: Some(10_080),
            resets_at: Some(2_300),
        }),
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    };
    append_rollout_item_to_path(
        &source_path,
        &RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: Some(source_token_usage.clone()),
            rate_limits: Some(source_rate_limits.clone()),
        })),
    )
    .await?;
    append_rollout_item_to_path(
        &source_path,
        &RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: "source-checkpoint-turn".to_string(),
            last_agent_message: None,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        })),
    )
    .await?;
    let mut source_thread_settings = test_thread_settings_snapshot();
    let persisted_thread_cwd = codex_home.path().join("persisted-fork-thread").abs();
    let persisted_environment_cwd = codex_home.path().join("persisted-fork-environment").abs();
    let persisted_profile_workspace_root =
        codex_home.path().join("persisted-profile-workspace").abs();
    std::fs::create_dir_all(persisted_thread_cwd.as_path())?;
    std::fs::create_dir_all(persisted_environment_cwd.as_path())?;
    std::fs::create_dir_all(persisted_profile_workspace_root.as_path())?;
    let persisted_environment = codex_protocol::protocol::TurnEnvironmentSelection {
        environment_id: codex_exec_server::LOCAL_ENVIRONMENT_ID.to_string(),
        cwd: codex_utils_path_uri::PathUri::from_abs_path(&persisted_environment_cwd),
        workspace_roots: vec![codex_utils_path_uri::PathUri::from_abs_path(
            &persisted_environment_cwd,
        )],
    };
    let persisted_environments = codex_protocol::protocol::TurnEnvironmentSelections::new(
        persisted_thread_cwd.clone(),
        vec![persisted_environment],
    );
    source_thread_settings.cwd = persisted_thread_cwd.clone();
    source_thread_settings.workspace_roots = Some(vec![persisted_thread_cwd]);
    source_thread_settings.active_permission_profile =
        Some(codex_protocol::models::ActivePermissionProfile {
            id: "persisted-profile".to_string(),
            extends: None,
        });
    source_thread_settings.profile_workspace_roots =
        Some(vec![persisted_profile_workspace_root.clone()]);
    source_thread_settings.windows_sandbox_level =
        Some(codex_protocol::config_types::WindowsSandboxLevel::RestrictedToken);
    source_thread_settings.environments = Some(persisted_environments.clone());
    source_thread_settings.approvals_reviewer = ProtocolApprovalsReviewer::AutoReview;
    append_rollout_item_to_path(
        &source_path,
        &RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
            ThreadSettingsAppliedEvent {
                thread_settings: source_thread_settings,
            },
        )),
    )
    .await?;
    let mut primary = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let fork_id = primary
        .send_thread_fork_request(ThreadForkParams {
            thread_id: source_thread_id.clone(),
            last_turn_id: Some("source-checkpoint-turn".to_string()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadForkResponse {
        thread: forked_thread,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, primary.read_response(fork_id)).await??;
    let fork_path = forked_thread.path.expect("durable fork rollout path");
    let fork_items = RolloutRecorder::load_rollout_items(&fork_path).await?.0;
    let fork_meta = fork_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::SessionMeta(meta) => Some(&meta.meta),
            _ => None,
        })
        .expect("fork session metadata");
    assert_eq!(fork_meta.multi_agent_version, Some(MultiAgentVersion::V2));
    let fork_reference = fork_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::RolloutReference(reference) => Some(reference.clone()),
            _ => None,
        })
        .expect("fork should retain a compact source reference");
    let checkpoint_descriptor = fork_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Compacted(compacted) => compacted.segment_state_checkpoint.as_ref(),
            _ => None,
        })
        .expect("fork-local segment checkpoint descriptor");
    assert_eq!(
        checkpoint_descriptor.reference_context,
        SegmentStateCheckpointDisposition::Established
    );
    assert_eq!(
        checkpoint_descriptor.world_state,
        SegmentStateCheckpointDisposition::Established
    );
    let fork_thread_settings = fork_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) => {
                Some(&event.thread_settings)
            }
            _ => None,
        })
        .expect("fork-local checkpoint thread settings");
    assert_eq!(
        (
            fork_thread_settings.approvals_reviewer,
            fork_thread_settings.environments.as_ref(),
            fork_thread_settings.profile_workspace_roots.as_ref(),
            fork_thread_settings.windows_sandbox_level,
        ),
        (
            ProtocolApprovalsReviewer::AutoReview,
            Some(&persisted_environments),
            Some(&vec![persisted_profile_workspace_root]),
            Some(codex_protocol::config_types::WindowsSandboxLevel::RestrictedToken),
        )
    );
    assert!(fork_items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TokenCount(event))
                if event.info.as_ref() == Some(&source_token_usage)
                    && event.rate_limits.as_ref() == Some(&source_rate_limits)
        )
    }));
    assert!(fork_items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::WorldState(WorldStateItem { full: true, state })
                if state["environments"]["subagents"] == "- /root/stale: working"
        )
    }));

    let explicit_cwd = codex_home.path().join("explicit-fork-cwd").abs();
    let explicit_workspace_root = codex_home.path().join("explicit-fork-workspace").abs();
    std::fs::create_dir_all(explicit_cwd.as_path())?;
    std::fs::create_dir_all(explicit_workspace_root.as_path())?;
    let explicit_fork_id = primary
        .send_thread_fork_request(ThreadForkParams {
            thread_id: source_thread_id,
            last_turn_id: Some("source-checkpoint-turn".to_string()),
            cwd: Some(explicit_cwd.as_path().to_string_lossy().into_owned()),
            runtime_workspace_roots: Some(vec![explicit_workspace_root.clone()]),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadForkResponse {
        cwd,
        runtime_workspace_roots,
        ..
    } = timeout(
        DEFAULT_READ_TIMEOUT,
        primary.read_response(explicit_fork_id),
    )
    .await??;
    assert_eq!(
        (cwd, runtime_workspace_roots),
        (explicit_cwd, vec![explicit_workspace_root])
    );

    timeout(DEFAULT_READ_TIMEOUT, primary.shutdown_gracefully()).await??;
    let missing_source = fork_reference.rollout_path.with_extension("jsonl.missing");
    std::fs::rename(&fork_reference.rollout_path, missing_source)?;

    let mut resumed_app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let resume_id = resumed_app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: forked_thread.id.clone(),
            path: Some(fork_path),
            model: Some("gpt-5.6-sol".to_string()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        approvals_reviewer,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, resumed_app.read_response(resume_id)).await??;
    assert_eq!(thread.id, forked_thread.id);
    assert!(thread.turns.is_empty());
    assert_eq!(approvals_reviewer, ApprovalsReviewer::AutoReview);
    let usage_notification = timeout(
        DEFAULT_READ_TIMEOUT,
        resumed_app.read_stream_until_notification_message("thread/tokenUsage/updated"),
    )
    .await??;
    let ServerNotification::ThreadTokenUsageUpdated(usage_notification) =
        usage_notification.try_into()?
    else {
        panic!("expected thread/tokenUsage/updated notification");
    };
    assert_eq!(usage_notification.token_usage.total.total_tokens, 123);
    assert_eq!(usage_notification.token_usage.last.total_tokens, 45);

    timeout(
        DEFAULT_READ_TIMEOUT,
        resumed_app.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: forked_thread.id.clone(),
            input: vec![UserInput::Text {
                text: "confirm local fork state".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;

    let response_requests = server
        .received_requests()
        .await
        .expect("response requests")
        .into_iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .collect::<Vec<_>>();
    assert_eq!(response_requests.len(), 2);
    let first_request = response_requests[0].body_json::<Value>()?;
    let first_input = serde_json::to_string(&first_request["input"])?;
    assert!(first_input.contains("source history retained by the checkpoint"));
    assert!(
        first_input.contains(
            &persisted_environment_cwd
                .as_path()
                .to_string_lossy()
                .to_string()
        )
    );
    assert!(
        first_input.contains("<subagents />"),
        "first resumed fork input did not clear inherited membership: {first_input}"
    );
    let second_request = response_requests[1].body_json::<Value>()?;
    let list_output = second_request["input"]
        .as_array()
        .expect("response input array")
        .iter()
        .find(|item| item["type"] == "function_call_output" && item["call_id"] == LIST_CALL_ID)
        .and_then(|item| item["output"].as_str())
        .expect("list_agents output");
    assert_eq!(
        serde_json::from_str::<Value>(list_output)?,
        json!({
            "agents": [],
            "next_cursor": null,
            "total_count": 0
        })
    );

    Ok(())
}
