use super::*;
use codex_models_manager::CustomModelConfig;
use codex_models_manager::bundled_models_response;
use codex_protocol::openai_models::ModelTokenBudgetConfig;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::start_websocket_server;
use pretty_assertions::assert_eq;

#[test]
fn goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume",
        goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume_inner(),
    )
}

async fn goal_supervisor_alias_guidance_survives_fork_and_segmented_cold_resume_inner()
-> anyhow::Result<()> {
    const ALIAS: &str = "frontier-local";
    const OLD_BACKING: &str = "gpt-5.2-preview";
    const NEW_BACKING: &str = "gpt-5.4-preview";
    const OLD_GUIDANCE: &str = "Preserve the original alias-backed context.";
    const NEW_GUIDANCE: &str = "Preserve the updated alias-backed context.";

    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-parent"),
                ev_completed("resp-parent"),
            ]),
            sse(vec![
                ev_response_created("resp-supervisor"),
                ev_completed("resp-supervisor"),
            ]),
            sse(vec![
                ev_response_created("resp-resumed"),
                ev_completed("resp-resumed"),
            ]),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let mut model_catalog = bundled_models_response()?;
    for (slug, guidance) in [("gpt-5.2", OLD_GUIDANCE), ("gpt-5.4", NEW_GUIDANCE)] {
        model_catalog
            .models
            .iter_mut()
            .find(|model| model.slug == slug)
            .and_then(|model| model.model_messages.as_mut())
            .expect("backing model should expose model messages")
            .token_budget = Some(ModelTokenBudgetConfig {
            enabled: false,
            use_history_notes_extension: false,
            reminder_threshold_tokens: 6_144,
            reminder_message_template: "Alias reminder: {n_remaining} tokens remain.".to_string(),
            guidance_message: guidance.to_string(),
            auto_compact_fallback_prompt: "Save state before rollover.".to_string(),
            auto_compact_fallback_buffer_tokens: 16_384,
        });
    }
    let _ = config.features.enable(Feature::AgentPromptInjection);
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let _ = config.features.enable(Feature::TokenBudget);
    config.model = Some(ALIAS.to_string());
    config.model_catalog = Some(model_catalog);
    config.custom_models.insert(
        ALIAS.to_string(),
        CustomModelConfig {
            model: OLD_BACKING.to_string(),
            routing_profile: None,
            trust_candidate_constraints: false,
            model_context_window: Some(128_000),
            model_auto_compact_token_limit: Some(100_000),
        },
    );
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let auth_manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
    let manager = ThreadManager::new(
        &config,
        Arc::clone(&auth_manager),
        crate::thread_manager::build_models_manager(&config, auth_manager),
        crate::CodexAppsToolsCache::default(),
        SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        empty_extension_registry(),
        Arc::new(crate::test_support::EmptyUserInstructionsProvider),
        /*analytics_events_client*/ None,
        crate::thread_manager::thread_store_from_config(&config, Some(state_db.clone())),
        crate::thread_manager::local_agent_graph_store_from_state_db(Some(&state_db)),
        uuid::Uuid::new_v4().to_string(),
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    );
    let control = manager.agent_control();
    let harness = AgentControlHarness {
        _home: home,
        config: config.clone(),
        state_db: Some(state_db),
        manager,
        control,
    };
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let parent_prompt_cache_key = parent_thread.session.prompt_cache_key();
    parent_thread
        .start_or_steer_turn(TurnInputRequest::user_input(text_input(
            "seed alias-backed guidance",
        )))
        .await?;
    wait_for_turn_complete(parent_thread.as_ref()).await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let (_goal_id, _goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Keep the alias-backed parent progressing.",
    )
    .await?;
    let child_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(
            AgentPath::root()
                .join(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
                .expect("goal supervisor path should be valid"),
        ),
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            config.clone(),
            text_input("inspect the parent goal"),
            Some(child_source.clone()),
            SpawnAgentOptions {
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await?
        .thread_id;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("goal supervisor helper should be registered");
    let child_rollout_path = child_thread
        .rollout_path()
        .expect("segmented goal supervisor rollout should exist");
    wait_for_turn_complete(child_thread.as_ref()).await;
    child_thread.session.flush_rollout().await?;

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let parent_input = requests[0].input();
    let child_input = requests[1].input();
    assert_eq!(requests[0].body_json()["model"], OLD_BACKING);
    assert_eq!(requests[1].body_json()["model"], OLD_BACKING);
    assert_eq!(
        &child_input[..parent_input.len()],
        parent_input,
        "goal supervisor full-history forks should preserve the exact parent request prefix"
    );
    assert_eq!(
        requests[1].body_json()["prompt_cache_key"],
        requests[0].body_json()["prompt_cache_key"]
    );
    assert_eq!(
        child_thread.session.prompt_cache_key(),
        parent_prompt_cache_key
    );
    assert_eq!(
        requests[0]
            .message_input_texts("developer")
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "custom aliases should inherit their backing model's token-budget guidance"
    );
    assert_eq!(
        requests[1]
            .message_input_texts("developer")
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "the supervisor fork should not duplicate inherited guidance"
    );

    let _ = harness.control.shutdown_live_agent(child_thread_id).await?;
    let mut resumed_config = config;
    resumed_config.custom_models.insert(
        ALIAS.to_string(),
        CustomModelConfig {
            model: NEW_BACKING.to_string(),
            routing_profile: None,
            trust_candidate_constraints: false,
            model_context_window: Some(128_000),
            model_auto_compact_token_limit: Some(100_000),
        },
    );
    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(resumed_config, child_thread_id, child_source)
        .await?;
    assert_eq!(resumed_thread_id, child_thread_id);
    let resumed_thread = harness
        .manager
        .get_thread(resumed_thread_id)
        .await
        .expect("cold-resumed supervisor should be registered");
    // Helpers are normally ephemeral; this fixture also checks durable cold-resume state.
    resumed_thread
        .session
        .try_ensure_rollout_materialized(PersistContext::Standard)
        .await?;
    resumed_thread
        .start_or_steer_turn(TurnInputRequest::user_input(text_input(
            "inspect resumed alias guidance",
        )))
        .await?;
    wait_for_turn_complete(resumed_thread.as_ref()).await;
    resumed_thread.session.flush_rollout().await?;

    let requests = request_log.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].body_json()["model"], NEW_BACKING);
    let resumed_developer_texts = requests[2].message_input_texts("developer");
    assert_eq!(
        resumed_developer_texts
            .iter()
            .filter(|text| text.contains(OLD_GUIDANCE))
            .count(),
        1,
        "cold resume should retain the original guidance once"
    );
    assert_eq!(
        resumed_developer_texts
            .iter()
            .filter(|text| text.contains(NEW_GUIDANCE))
            .count(),
        1,
        "cold resume should append the updated backing model guidance once"
    );

    let resumed_rollout_path = resumed_thread
        .rollout_path()
        .expect("materialized resumed helper");
    let original_items = RolloutRecorder::load_rollout_items(child_rollout_path.as_path())
        .await?
        .0;
    assert!(
        original_items
            .iter()
            .any(|item| matches!(item, RolloutItem::RolloutReference(_)))
    );
    let physical_items = RolloutRecorder::load_rollout_items(resumed_rollout_path.as_path())
        .await?
        .0;
    let world_states = physical_items
        .iter()
        .filter(|item| matches!(item, RolloutItem::WorldState(_)))
        .collect::<Vec<_>>();
    let [
        RolloutItem::WorldState(checkpoint_baseline),
        RolloutItem::WorldState(usage_hint_tombstone),
        RolloutItem::WorldState(context_guidance),
    ] = world_states.as_slice()
    else {
        panic!(
            "expected checkpoint baseline, usage-hint tombstone, and updated context guidance: {world_states:#?}"
        );
    };
    assert!(checkpoint_baseline.full);
    assert_eq!(
        checkpoint_baseline
            .state
            .get("context_window_guidance")
            .and_then(serde_json::Value::as_str),
        Some(OLD_GUIDANCE)
    );
    assert!(!usage_hint_tombstone.full);
    assert_eq!(
        serde_json::Value::Object(usage_hint_tombstone.state.clone()),
        serde_json::json!({
            "context_window": "/root/goal_supervisor",
            "multi_agent_mode": {"usage_hint_hash": null},
            "multi_agent_usage_hint": null,
        })
    );
    assert!(!context_guidance.full);
    assert_eq!(
        serde_json::Value::Object(context_guidance.state.clone()),
        serde_json::json!({"context_window_guidance": NEW_GUIDANCE})
    );

    let _ = parent_thread.submit(Op::Shutdown {}).await?;
    Ok(())
}

#[test]
fn goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id()
-> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id",
        goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id_inner(),
    )
}

async fn goal_supervisor_helper_websocket_request_reuses_parent_prompt_cache_key_without_parent_previous_response_id_inner()
-> anyhow::Result<()> {
    let server = start_websocket_server(vec![
        vec![
            vec![
                ev_response_created("warm-parent"),
                ev_completed("warm-parent"),
            ],
            vec![
                ev_response_created("resp-parent"),
                ev_assistant_message("msg-parent", "parent done"),
                ev_completed("resp-parent"),
            ],
        ],
        vec![
            vec![
                ev_response_created("warm-supervisor"),
                ev_completed("warm-supervisor"),
            ],
            vec![
                ev_response_created("resp-supervisor"),
                ev_completed("resp-supervisor"),
            ],
        ],
    ])
    .await;
    let (_home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = true;

    let state_db = init_state_db(&config)
        .await
        .expect("state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let parent = manager
        .start_thread(StartThreadOptions::new(config))
        .await?;
    let parent_thread_id = parent.thread_id;
    let parent_prompt_cache_key = parent.thread.session.prompt_cache_key();
    parent
        .thread
        .start_or_steer_turn(TurnInputRequest::user_input(text_input("parent seed")))
        .await?;
    wait_for_turn_complete(parent.thread.as_ref()).await;
    parent
        .thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent.thread.session.flush_rollout().await?;
    let before_thread_ids = manager.list_thread_ids().await;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        &state_db,
        parent_thread_id,
        &parent.thread.session,
        "Supervise the parent with prompt cache inheritance.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent.thread.session, &goal_id, &goal)
        .await?;
    let child_thread_id = spawned_thread_id_after(&manager, &before_thread_ids).await;
    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    wait_for_turn_complete(child_thread.as_ref()).await;

    let connections = server.connections();
    let supervisor_connection = connections
        .get(1)
        .expect("supervisor helper should use its own websocket connection");
    let supervisor_generated_request = supervisor_connection
        .iter()
        .map(core_test_support::responses::WebSocketRequest::body_json)
        .find(|body| body["generate"].as_bool() != Some(false))
        .unwrap_or_else(|| {
            panic!(
                "goal supervisor helper should send a generated websocket request after warmup; supervisor requests={supervisor_connection:#?}"
            )
        });
    assert_ne!(
        supervisor_generated_request["previous_response_id"].as_str(),
        Some("resp-parent"),
        "goal supervisor helpers must not reuse a parent websocket previous_response_id on their own websocket connection"
    );
    assert_eq!(
        supervisor_generated_request["prompt_cache_key"].as_str(),
        Some(parent_prompt_cache_key.to_string().as_str()),
        "goal supervisor helpers must keep the parent prompt cache key across websocket connections"
    );
    assert!(
        request_tool_signatures(&supervisor_generated_request).contains("supervisor.close_self"),
        "goal supervisor helper websocket requests must retain the supervisor tool namespace"
    );

    server.shutdown().await;
    Ok(())
}
