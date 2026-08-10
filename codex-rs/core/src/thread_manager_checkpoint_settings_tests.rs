use super::*;
use crate::config::test_config;
use crate::rollout::RolloutRecorder;
use codex_protocol::SegmentId;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::Settings;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::CompactedItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RateLimitSnapshot;
use codex_protocol::protocol::RateLimitWindow;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TokenUsage;
use codex_protocol::protocol::TokenUsageInfo;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::TurnStartedEvent;
use codex_thread_store::FrozenRolloutSegment;
use codex_thread_store::PreparedFork;
use codex_utils_path_uri::PathUri;
use core_test_support::PathBufExt;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tempfile::tempdir;

struct DeepUnmarkedContext {
    root_items: Vec<RolloutItem>,
    oldest_path: PathBuf,
    token_usage: TokenUsageInfo,
    rate_limits: RateLimitSnapshot,
}

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn settings_item(
    config: &Config,
    fallback_cwd: AbsolutePathBuf,
    environment: TurnEnvironmentSelection,
) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
        ThreadSettingsAppliedEvent {
            thread_settings: ThreadSettingsSnapshot {
                model: "test-model".to_string(),
                model_provider_id: config.model_provider_id.clone(),
                service_tier: config.service_tier.clone(),
                approval_policy: AskForApproval::Never,
                approvals_reviewer: ApprovalsReviewer::User,
                permission_profile: PermissionProfile::workspace_write(),
                active_permission_profile: None,
                cwd: fallback_cwd.clone(),
                environments: Some(TurnEnvironmentSelections::new(
                    fallback_cwd.clone(),
                    vec![environment],
                )),
                workspace_roots: Some(vec![fallback_cwd]),
                profile_workspace_roots: Some(Vec::new()),
                windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
                reasoning_effort: config.model_reasoning_effort.clone(),
                reasoning_summary: config.model_reasoning_summary,
                personality: config.personality,
                collaboration_mode: config.initial_collaboration_mode.clone().unwrap_or_else(
                    || CollaborationMode {
                        mode: ModeKind::Default,
                        settings: Settings {
                            model: "test-model".to_string(),
                            reasoning_effort: config.model_reasoning_effort.clone(),
                            developer_instructions: None,
                        },
                    },
                ),
            },
        },
    ))
}

fn write_rollout_lines(path: &std::path::Path, lines: &[RolloutLine]) {
    std::fs::create_dir_all(path.parent().expect("rollout parent")).expect("create rollout parent");
    let mut contents = lines
        .iter()
        .map(|line| serde_json::to_string(line).expect("serialize rollout line"))
        .collect::<Vec<_>>()
        .join("\n");
    contents.push('\n');
    std::fs::write(path, contents).expect("write rollout lines");
}

fn deep_unmarked_context(config: &Config, thread_id: ThreadId) -> DeepUnmarkedContext {
    let environment_cwd = config.codex_home.join("deep-environment");
    std::fs::create_dir_all(environment_cwd.as_path()).expect("create deep environment");
    let environment = TurnEnvironmentSelection {
        environment_id: codex_exec_server::LOCAL_ENVIRONMENT_ID.to_string(),
        cwd: PathUri::from_abs_path(&environment_cwd),
        workspace_roots: vec![PathUri::from_abs_path(&environment_cwd)],
    };
    let token_usage = TokenUsageInfo {
        total_token_usage: TokenUsage {
            total_tokens: 4_321,
            ..TokenUsage::default()
        },
        last_token_usage: TokenUsage {
            total_tokens: 321,
            ..TokenUsage::default()
        },
        model_context_window: Some(128_000),
    };
    let rate_limits = RateLimitSnapshot {
        limit_id: Some("deep-limit".to_string()),
        limit_name: Some("Deep limit".to_string()),
        primary: Some(RateLimitWindow {
            used_percent: 37.5,
            window_minutes: Some(300),
            resets_at: Some(2_000),
        }),
        secondary: None,
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    };
    let timestamp = "2026-08-09T00:00:00Z".to_string();
    let meta = |segment_id| {
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id: Some(segment_id),
                timestamp: timestamp.clone(),
                cwd: config.cwd.to_path_buf(),
                originator: "deep-model-context-test".to_string(),
                cli_version: "test".to_string(),
                source: SessionSource::Exec,
                model_provider: Some(config.model_provider_id.clone()),
                base_instructions: Some(BaseInstructions::default()),
                history_mode: ThreadHistoryMode::Paginated,
                ..SessionMeta::default()
            },
            git: None,
        })
    };
    let line = |ordinal, item| RolloutLine {
        timestamp: timestamp.clone(),
        ordinal: Some(ordinal),
        item,
    };

    let mut previous = None;
    let mut oldest_path = None;
    for index in 0_u64..4 {
        let segment_id = SegmentId::new();
        let path = config
            .codex_home
            .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join("segment.jsonl")
            .to_path_buf();
        let mut lines = vec![line(index * 10, meta(segment_id))];
        if let Some((previous_path, previous_segment_id)) = previous {
            lines.push(line(
                index * 10 + 1,
                RolloutItem::RolloutReference(RolloutReferenceItem {
                    rollout_path: previous_path,
                    thread_id: Some(thread_id),
                    rollout_timestamp: None,
                    segment_id: Some(previous_segment_id),
                    max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                    nth_user_message: None,
                    compacted_replacement_history_filter_texts: None,
                }),
            ));
        } else {
            oldest_path = Some(path.clone());
            lines.extend([
                line(
                    index * 10 + 1,
                    settings_item(config, environment_cwd.clone(), environment.clone()),
                ),
                line(
                    index * 10 + 2,
                    RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
                        info: Some(token_usage.clone()),
                        rate_limits: Some(rate_limits.clone()),
                    })),
                ),
                line(
                    index * 10 + 3,
                    RolloutItem::ResponseItem(user_message("obsolete oldest response")),
                ),
            ]);
        }
        write_rollout_lines(path.as_path(), &lines);
        previous = Some((path, segment_id));
    }

    let (latest_path, latest_segment_id) = previous.expect("latest immutable segment");
    let root_segment_id = SegmentId::new();
    let window_id = uuid::Uuid::now_v7();
    let turn_id = "post-compaction-turn".to_string();
    let root_items = vec![
        meta(root_segment_id),
        RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: latest_path,
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(latest_segment_id),
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(vec![user_message("replacement model history")]),
            window_number: Some(7),
            first_window_id: Some(window_id.to_string()),
            previous_window_id: None,
            window_id: Some(window_id.to_string()),
            segment_state_checkpoint: None,
        }),
        RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_id.clone(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })),
        RolloutItem::TurnContext(TurnContextItem {
            turn_id: Some(turn_id.clone()),
            cwd: config.cwd.clone(),
            workspace_roots: None,
            current_date: None,
            timezone: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: None,
            sandbox_policy: SandboxPolicy::new_read_only_policy(),
            permission_profile: None,
            network: None,
            file_system_sandbox_policy: None,
            model: config
                .model
                .clone()
                .unwrap_or_else(|| "test-model".to_string()),
            comp_hash: None,
            personality: None,
            collaboration_mode: None,
            multi_agent_version: None,
            multi_agent_mode: None,
            realtime_active: None,
            effort: config.model_reasoning_effort.clone(),
            service_tier: None,
            model_profile: None,
            summary: ReasoningSummary::Auto,
        }),
        RolloutItem::ResponseItem(user_message("post-compaction user")),
        RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id,
            last_agent_message: None,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        })),
    ];

    DeepUnmarkedContext {
        root_items,
        oldest_path: oldest_path.expect("oldest immutable segment"),
        token_usage,
        rate_limits,
    }
}

fn assert_restored_token_state(
    items: &[RolloutItem],
    token_usage: &TokenUsageInfo,
    rate_limits: &RateLimitSnapshot,
) {
    assert!(items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TokenCount(event))
                if event.info.as_ref() == Some(token_usage)
                    && event.rate_limits.as_ref() == Some(rate_limits)
        )
    }));
}

#[tokio::test]
async fn prepared_historical_fork_uses_latest_settings_and_restores_rate_limits_without_source_runtime()
 {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config().await;
    config.codex_home = temp_dir.path().join("codex-home").abs();
    config.cwd = config.codex_home.clone();
    std::fs::create_dir_all(config.codex_home.as_path()).expect("create codex home");
    let boundary_cwd = config.codex_home.join("boundary-environment");
    let latest_cwd = config.codex_home.join("latest-environment");
    std::fs::create_dir_all(boundary_cwd.as_path()).expect("create boundary environment");
    std::fs::create_dir_all(latest_cwd.as_path()).expect("create latest environment");
    let boundary_environment = TurnEnvironmentSelection {
        environment_id: codex_exec_server::LOCAL_ENVIRONMENT_ID.to_string(),
        cwd: PathUri::from_abs_path(&boundary_cwd),
        workspace_roots: vec![PathUri::from_abs_path(&boundary_cwd)],
    };
    let latest_environment = TurnEnvironmentSelection {
        environment_id: codex_exec_server::LOCAL_ENVIRONMENT_ID.to_string(),
        cwd: PathUri::from_abs_path(&latest_cwd),
        workspace_roots: vec![PathUri::from_abs_path(&latest_cwd)],
    };
    let source_usage = TokenUsageInfo {
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
        secondary: None,
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    };
    let boundary_response = user_message("boundary response history");
    let boundary_context = vec![
        RolloutItem::ResponseItem(boundary_response.clone()),
        settings_item(&config, boundary_cwd, boundary_environment),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: Some(source_usage.clone()),
            rate_limits: Some(source_rate_limits.clone()),
        })),
    ];
    let latest_context = vec![
        RolloutItem::ResponseItem(user_message("newer response excluded by boundary")),
        settings_item(&config, latest_cwd, latest_environment.clone()),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: Some(source_usage.clone()),
            rate_limits: Some(source_rate_limits.clone()),
        })),
    ];
    let source_thread_id = ThreadId::new();
    let source_segment_id = SegmentId::new();
    let source_path = config.codex_home.join("missing-source.jsonl").to_path_buf();
    let source_session_meta = SessionMetaLine {
        meta: SessionMeta {
            session_id: source_thread_id.into(),
            id: source_thread_id,
            segment_id: Some(source_segment_id),
            cwd: config.cwd.to_path_buf(),
            model_provider: Some(config.model_provider_id.clone()),
            base_instructions: Some(BaseInstructions::default()),
            history_mode: ThreadHistoryMode::Paginated,
            ..SessionMeta::default()
        },
        git: None,
    };
    tokio::fs::write(
        &source_path,
        format!(
            "{}\n",
            serde_json::to_string(&RolloutLine {
                timestamp: "2026-08-09T00:00:00Z".to_string(),
                ordinal: Some(0),
                item: RolloutItem::SessionMeta(source_session_meta.clone()),
            })
            .expect("serialize source segment")
        ),
    )
    .await
    .expect("write immutable source segment");
    let frozen_segment = FrozenRolloutSegment {
        reference: RolloutReferenceItem {
            rollout_path: source_path,
            thread_id: Some(source_thread_id),
            rollout_timestamp: None,
            segment_id: Some(source_segment_id),
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
        source_session_meta,
        history_mode: ThreadHistoryMode::Paginated,
        next_rollout_ordinal: Some(3),
    };
    let mut prepared = PreparedFork::new(
        source_thread_id,
        /*history_base*/ None,
        frozen_segment,
        Arc::new(boundary_context.clone()),
        Arc::new(latest_context),
        Arc::new(boundary_context),
        /*interrupt_if_open*/ false,
        (),
    );
    prepared.shared_model_response_items = Some(Arc::new(vec![boundary_response.clone()]));

    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let (forked, response_history) = manager
        .fork_prepared_thread(
            config,
            prepared,
            /*thread_source*/ None,
            /*parent_trace*/ None,
            ClientMcpExtensions::default(),
        )
        .await
        .expect("fork prepared source without a loaded source runtime");

    assert_eq!(
        forked.thread.environment_selections().await,
        vec![latest_environment]
    );
    assert!(response_history.iter().any(|item| {
        matches!(
            item,
            RolloutItem::ResponseItem(ResponseItem::Message { content, .. })
                if content.iter().any(|content| matches!(
                    content,
                    ContentItem::OutputText { text } if text == "boundary response history"
                ))
        )
    }));
    assert!(response_history.iter().all(|item| {
        !matches!(
            item,
            RolloutItem::ResponseItem(ResponseItem::Message { content, .. })
                if content.iter().any(|content| matches!(
                    content,
                    codex_protocol::models::ContentItem::OutputText { text }
                        if text == "newer response excluded by boundary"
                ))
        )
    }));
    assert_eq!(forked.thread.token_usage_info().await, Some(source_usage));
    let fork_items = RolloutRecorder::load_rollout_items(
        &forked
            .thread
            .rollout_path()
            .expect("materialized fork rollout"),
    )
    .await
    .expect("read fork rollout")
    .0;
    assert!(fork_items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TokenCount(event))
                if event.rate_limits.as_ref() == Some(&source_rate_limits)
        )
    }));

    forked
        .thread
        .shutdown_and_wait()
        .await
        .expect("shutdown forked thread");
}

#[tokio::test]
async fn paginated_fork_startup_recovers_deep_unmarked_token_state() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config().await;
    config.codex_home = temp_dir.path().join("codex-home").abs();
    config.cwd = config.codex_home.clone();
    std::fs::create_dir_all(config.codex_home.as_path()).expect("create codex home");
    let source_thread_id = ThreadId::new();
    let fixture = deep_unmarked_context(&config, source_thread_id);
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let auth_manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());

    let forked = manager
        .resume_thread_with_history(
            config.clone(),
            InitialHistory::Forked(fixture.root_items.clone()),
            auth_manager.clone(),
            /*parent_trace*/ None,
            ClientMcpExtensions::default(),
        )
        .await
        .expect("fork deep unmarked Paginated source");
    assert_eq!(
        forked.thread.token_usage_info().await,
        Some(fixture.token_usage.clone())
    );
    let fork_items = RolloutRecorder::load_rollout_items(
        &forked
            .thread
            .rollout_path()
            .expect("materialized fork rollout"),
    )
    .await
    .expect("read fork rollout")
    .0;
    assert_restored_token_state(&fork_items, &fixture.token_usage, &fixture.rate_limits);

    forked
        .thread
        .shutdown_and_wait()
        .await
        .expect("shutdown forked thread");
    std::fs::remove_file(fixture.oldest_path).expect("remove oldest predecessor");
    let error = match manager
        .resume_thread_with_history(
            config,
            InitialHistory::Forked(fixture.root_items),
            auth_manager,
            /*parent_trace*/ None,
            ClientMcpExtensions::default(),
        )
        .await
    {
        Ok(_) => panic!("missing deep compatibility state must reject fork startup"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("No such file")
            || error.to_string().contains("not found")
            || error.to_string().contains("could not be resolved"),
        "unexpected missing predecessor error: {error}"
    );
}

#[tokio::test]
async fn spawn_subagent_recovers_deep_unmarked_token_state() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config().await;
    config.codex_home = temp_dir.path().join("codex-home").abs();
    config.cwd = config.codex_home.clone();
    std::fs::create_dir_all(config.codex_home.as_path()).expect("create codex home");
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let source = manager
        .start_thread(StartThreadOptions {
            history_mode: Some(ThreadHistoryMode::Paginated),
            ..StartThreadOptions::new(config.clone())
        })
        .await
        .expect("start Paginated source");
    source.thread.ensure_rollout_materialized().await;
    let fixture = deep_unmarked_context(&config, source.thread_id);
    source
        .thread
        .append_rollout_items(&fixture.root_items[1..])
        .await
        .expect("append deep unmarked source context");
    source
        .thread
        .flush_rollout()
        .await
        .expect("flush source context");

    let child = manager
        .spawn_subagent(source.thread_id, StartThreadOptions::new(config.clone()))
        .await
        .expect("spawn from deep unmarked source");
    assert_eq!(
        child.thread.token_usage_info().await,
        Some(fixture.token_usage.clone())
    );
    let child_items = RolloutRecorder::load_rollout_items(
        &child
            .thread
            .rollout_path()
            .expect("materialized child rollout"),
    )
    .await
    .expect("read child rollout")
    .0;
    assert_restored_token_state(&child_items, &fixture.token_usage, &fixture.rate_limits);

    child
        .thread
        .shutdown_and_wait()
        .await
        .expect("shutdown child");
    std::fs::remove_file(fixture.oldest_path).expect("remove oldest predecessor");
    let error = match manager
        .spawn_subagent(source.thread_id, StartThreadOptions::new(config))
        .await
    {
        Ok(_) => panic!("missing deep compatibility state must reject subagent spawn"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("No such file")
            || error.to_string().contains("not found")
            || error.to_string().contains("could not be resolved"),
        "unexpected missing predecessor error: {error}"
    );

    source
        .thread
        .shutdown_and_wait()
        .await
        .expect("shutdown source");
}
