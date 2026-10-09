use super::*;
use pretty_assertions::assert_eq;

fn resume_tool(
    tools: &[Arc<dyn for<'call> ToolExecutor<ToolCall<'call>>>],
) -> &Arc<dyn for<'call> ToolExecutor<ToolCall<'call>>> {
    tools
        .iter()
        .find(|tool| {
            tool.tool_name() == codex_extension_api::ToolName::namespaced("frodex", "resume_goal")
        })
        .expect("persistent root should have resume_goal")
}

fn resume_call(arguments: serde_json::Value) -> ToolCall<'static> {
    let mut call = tool_call("resume_goal", "call-resume", arguments);
    call.tool_name = codex_extension_api::ToolName::namespaced("frodex", "resume_goal");
    call
}

#[tokio::test]
async fn resume_goal_tool_preserves_goal_and_emits_update() -> anyhow::Result<()> {
    let runtime = test_runtime().await?;
    for status in [
        codex_state::ThreadGoalStatus::Paused,
        codex_state::ThreadGoalStatus::Blocked,
        codex_state::ThreadGoalStatus::UsageLimited,
    ] {
        let thread_id = ThreadId::new();
        let harness = GoalExtensionHarness::new(runtime.clone(), thread_id).await?;
        let original = runtime
            .thread_goals()
            .replace_thread_goal(thread_id, "finish the task", status, Some(1000))
            .await?;
        let codex_state::GoalAccountingOutcome::Updated(original) = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 3,
                /*token_delta*/ 50,
                codex_state::GoalAccountingMode::ActiveOrStopped,
                Some(&original.goal_id),
            )
            .await?
        else {
            anyhow::bail!("goal accounting should succeed");
        };
        harness.start_turn("turn-1", &TokenUsage::default()).await;
        let tools = harness.tools();
        let invocation = resume_call(json!({}));
        let result = resume_tool(&tools)
            .handle(invocation.clone())
            .await?
            .code_mode_result(&invocation.payload);
        let resumed = runtime
            .thread_goals()
            .get_thread_goal(thread_id)
            .await?
            .expect("goal should exist");
        let mut expected = original;
        expected.status = codex_state::ThreadGoalStatus::Active;
        expected.updated_at = resumed.updated_at;
        assert_eq!(resumed, expected);
        assert_eq!(result["goal"]["status"], "active");
        assert_eq!(result["goal"]["objective"], "finish the task");
        assert_eq!(
            harness.sink.goal_events(),
            vec![CapturedGoalEvent {
                event_id: "call-resume".to_string(),
                turn_id: Some("turn-1".to_string()),
                status: ThreadGoalStatus::Active,
                tokens_used: 50,
            }]
        );
        // Resumption attaches accounting to this existing turn, rather than
        // resetting usage or waiting for a future turn to start accounting.
        harness
            .record_token_usage("turn-1", &input_token_usage(20))
            .await;
        harness
            .notify_tool_finish("turn-1", "call-after-resume", "shell")
            .await;
        let accounted = runtime
            .thread_goals()
            .get_thread_goal(thread_id)
            .await?
            .expect("goal should exist");
        assert_eq!(accounted.tokens_used, 70);
    }
    Ok(())
}

#[tokio::test]
async fn resume_goal_tool_rejects_invalid_status_missing_goal_and_editing() -> anyhow::Result<()> {
    let runtime = test_runtime().await?;
    let thread_id = ThreadId::new();
    let tools = installed_tools(runtime.clone(), thread_id).await;
    let tool = resume_tool(&tools);
    assert!(matches!(
        tool.handle(resume_call(json!({}))).await,
        Err(FunctionCallError::RespondToModel(_))
    ));
    for status in [
        codex_state::ThreadGoalStatus::Active,
        codex_state::ThreadGoalStatus::Complete,
        codex_state::ThreadGoalStatus::BudgetLimited,
    ] {
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(thread_id, "finish task", status, /*token_budget*/ None)
            .await?;
        assert!(matches!(
            tool.handle(resume_call(json!({}))).await,
            Err(FunctionCallError::RespondToModel(_))
        ));
        assert_eq!(
            runtime.thread_goals().get_thread_goal(thread_id).await?,
            Some(goal)
        );
    }
    let goal = runtime
        .thread_goals()
        .replace_thread_goal(
            thread_id,
            "finish task",
            codex_state::ThreadGoalStatus::Paused,
            /*token_budget*/ None,
        )
        .await?;
    for args in [
        json!({"objective": "different"}),
        json!({"thread_id": ThreadId::new()}),
        json!({"status": "active"}),
        json!({"token_budget": 100}),
    ] {
        assert!(matches!(
            tool.handle(resume_call(args)).await,
            Err(FunctionCallError::RespondToModel(_))
        ));
        assert_eq!(
            runtime.thread_goals().get_thread_goal(thread_id).await?,
            Some(goal.clone())
        );
    }
    // Generic status mutation must not gain resumption as a side effect.
    assert!(matches!(
        tool_by_name(&tools, "update_goal")
            .handle(tool_call(
                "update_goal",
                "bad-resume",
                json!({"status":"active"})
            ))
            .await,
        Err(FunctionCallError::RespondToModel(_))
    ));
    assert_eq!(
        runtime.thread_goals().get_thread_goal(thread_id).await?,
        Some(goal)
    );
    Ok(())
}

#[tokio::test]
async fn resume_goal_tool_only_available_to_persistent_roots() -> anyhow::Result<()> {
    let runtime = test_runtime().await?;
    for source in [
        SessionSource::SubAgent(SubAgentSource::Review),
        SessionSource::SubAgent(SubAgentSource::Other("goal_supervisor".to_string())),
        SessionSource::Internal(codex_protocol::protocol::InternalSessionSource::Guardian),
        SessionSource::Internal(
            codex_protocol::protocol::InternalSessionSource::MemoryConsolidation,
        ),
    ] {
        for persistent in [true, false] {
            let tools = installed_tools_with_start(
                runtime.clone(),
                ThreadId::new(),
                source.clone(),
                persistent,
            )
            .await;
            assert!(
                !tools
                    .iter()
                    .any(|tool| tool.tool_name().name == "resume_goal")
            );
        }
    }
    let tools = installed_tools_with_start(
        runtime.clone(),
        ThreadId::new(),
        SessionSource::Cli,
        /*persistent_thread_state_available*/ false,
    )
    .await;
    assert!(
        !tools
            .iter()
            .any(|tool| tool.tool_name().name == "resume_goal")
    );
    let tools = installed_tools(runtime, ThreadId::new()).await;
    let _ = resume_tool(&tools);
    assert!(
        !tools
            .iter()
            .any(|tool| tool.tool_name().name == "edit_active_goal")
    );
    Ok(())
}
