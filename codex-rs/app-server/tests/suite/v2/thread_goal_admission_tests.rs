use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn permanently_closed_thread_rejects_resume_and_goal_mutations() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    mock_responses_config(&server.uri())
        .enable_feature(Feature::Goals)
        .write(codex_home.path())?;
    let closed_thread_id = ThreadId::from_string(&create_fake_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Permanently closed agent",
        Some("mock_provider"),
        /*git_info*/ None,
    )?)?;
    let owner_thread_id = ThreadId::new();
    let state_db = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".into(),
    )
    .await?;
    state_db
        .upsert_thread_spawn_edge(
            owner_thread_id,
            closed_thread_id,
            codex_state::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await?;
    let expected_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            closed_thread_id,
            "closed Goal remains auditable",
            codex_state::ThreadGoalStatus::Paused,
            /*token_budget*/ None,
        )
        .await?;

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_managed_config()
        .build_initialized()
        .await?;

    let resume_request = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: closed_thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let resume_error: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(resume_request)),
    )
    .await??;
    assert!(resume_error.error.message.contains("permanently closed"));

    for (method, params) in [
        (
            "thread/goal/set",
            json!({
                "threadId": closed_thread_id.to_string(),
                "objective": "must not reactivate",
                "status": "active",
            }),
        ),
        (
            "thread/goal/clear",
            json!({"threadId": closed_thread_id.to_string()}),
        ),
    ] {
        let request_id = app.send_raw_request(method, Some(params)).await?;
        let error: JSONRPCError = timeout(
            DEFAULT_READ_TIMEOUT,
            app.read_stream_until_error_message(RequestId::Integer(request_id)),
        )
        .await??;
        assert!(
            error.error.message.contains("permanently closed"),
            "unexpected {method} error: {}",
            error.error.message
        );
    }

    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal(closed_thread_id)
            .await?,
        Some(expected_goal)
    );
    Ok(())
}

#[tokio::test]
async fn path_selected_running_thread_rechecks_permanent_close() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    mock_responses_config(&server.uri()).write(codex_home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let start_request = app
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let ThreadStartResponse { thread, .. } = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_response::<ThreadStartResponse>(start_request),
    )
    .await??;
    let turn_request = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![UserInput::Text {
                text: "materialize the running thread".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_response_message(RequestId::Integer(turn_request)),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    let rollout_path = codex_rollout::find_thread_path_by_id_str(
        codex_home.path(),
        thread.id.as_str(),
        /*state_db*/ None,
    )
    .await?
    .expect("materialized rollout path");
    let running_thread_id = ThreadId::from_string(&thread.id)?;
    let state_db = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".into(),
    )
    .await?;
    state_db
        .upsert_thread_spawn_edge(
            ThreadId::new(),
            running_thread_id,
            codex_state::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await?;

    let resume_request = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: ThreadId::new().to_string(),
            path: Some(rollout_path),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let resume_error: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(resume_request)),
    )
    .await??;
    assert!(resume_error.error.message.contains("permanently closed"));
    Ok(())
}

#[tokio::test]
async fn active_goal_mutation_releases_admission_before_supervisor_startup() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    mock_responses_config(&server.uri()).write(codex_home.path())?;
    let config_path = codex_home.path().join("config.toml");
    let config = std::fs::read_to_string(&config_path)?;
    std::fs::write(
        &config_path,
        config.replace(
            "personality = true\n",
            "personality = true\ngoals = true\ngoal_supervisor = true\n",
        ),
    )?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let start_request = app
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let ThreadStartResponse { thread, .. } = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_response::<ThreadStartResponse>(start_request),
    )
    .await??;
    let turn_request = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![UserInput::Text {
                text: "materialize before activating the Goal".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_response_message(RequestId::Integer(turn_request)),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let goal_request = app
        .send_raw_request(
            "thread/goal/set",
            Some(json!({
                "threadId": thread.id,
                "objective": "start a supervisor without a lock cycle",
                "status": "active",
            })),
        )
        .await?;
    let _: ThreadGoalSetResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(goal_request)).await??;
    wait_for_responses_request_count(&server, /*expected_count*/ 2).await?;
    Ok(())
}
