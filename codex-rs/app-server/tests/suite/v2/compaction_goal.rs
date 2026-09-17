//! Mid-turn compaction must not hand an unfinished root turn to its goal supervisor.

use super::*;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::oneshot;

#[test_case::test_case(0, ThreadHistoryMode::Paginated; "active_paginated")]
#[test_case::test_case(3_600, ThreadHistoryMode::Paginated; "snoozed_paginated")]
#[test_case::test_case(0, ThreadHistoryMode::Legacy; "active_legacy")]
#[test_case::test_case(3_600, ThreadHistoryMode::Legacy; "snoozed_legacy")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_compaction_root_completes_before_goal_supervisor(
    snooze_seconds: i64,
    history_mode: ThreadHistoryMode,
) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let (release_first, first_gate) = oneshot::channel();
    let (release_root, root_gate) = oneshot::channel();
    let (server, _completions) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: Some(first_gate),
            body: responses::sse(vec![
                responses::ev_response_created("root-tool"),
                responses::ev_function_call("read-goal", "get_goal", "{}"),
                responses::ev_completed_with_tokens("root-tool", /*total_tokens*/ 70_000),
            ]),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![
                responses::ev_assistant_message("summary", "RETAINED_COMPACTION_SUMMARY"),
                responses::ev_completed_with_tokens("summary", /*total_tokens*/ 200),
            ]),
        }],
        vec![StreamingSseChunk {
            gate: Some(root_gate),
            body: responses::sse(vec![
                responses::ev_assistant_message("root-final", "ROOT_FINISHED"),
                responses::ev_completed_with_tokens("root-final", /*total_tokens*/ 120),
            ]),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![
                responses::ev_response_created("supervisor"),
                responses::ev_function_call_with_namespace(
                    "snooze-call",
                    "supervisor",
                    "snooze",
                    r#"{"delay_seconds":3600}"#,
                ),
                responses::ev_completed("supervisor"),
            ]),
        }],
    ])
    .await;
    let codex_home = TempDir::new()?;
    compaction_config(server.uri(), /*auto_compact_limit*/ 50_000)
        .enable_feature(Feature::Goals)
        .enable_feature(Feature::GoalSupervisor)
        .disable_feature(Feature::RemoteCompactionV2)
        .write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_managed_config()
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;
    let thread_id = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            history_mode: Some(history_mode),
            ..Default::default()
        })
        .await?
        .thread
        .id;
    let request_id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![V2UserInput::Text {
                text: "check the goal, then finish this turn".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let started: TurnStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(request_id)).await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        server.wait_for_request_count(/*count*/ 1),
    )
    .await?;

    // Seed only while the root is busy, so idle scheduling cannot precede the user turn.
    let state = StateRuntime::init(
        SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".into(),
    )
    .await?;
    let root_id = ThreadId::from_string(&thread_id)?;
    let goal = state
        .thread_goals()
        .replace_thread_goal(
            root_id,
            "finish the current turn before checking progress",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    let snoozed_until =
        (snooze_seconds > 0).then(|| Utc::now().timestamp_millis() + snooze_seconds * 1_000);
    state
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(root_id, &goal.goal_id, snoozed_until)
        .await?;
    release_first.send(()).expect("first root response is held");
    timeout(
        DEFAULT_READ_TIMEOUT,
        server.wait_for_request_count(/*count*/ 3),
    )
    .await?;

    let compacted = wait_for_context_compaction_completed(&mut mcp).await?;
    assert_eq!(compacted.thread_id, thread_id);
    assert_eq!(compacted.turn_id, started.turn.id);
    let requests = server.requests().await;
    assert_eq!(
        requests.len(),
        3,
        "only root, compaction, and root continuation"
    );
    let continuation: Value = serde_json::from_slice(&requests[2])?;
    assert_eq!(continuation["client_metadata"]["turn_id"], started.turn.id);
    responses::assert_root_turn(&continuation, Some(&started.turn.id))?;
    assert!(
        continuation["input"]
            .to_string()
            .contains("RETAINED_COMPACTION_SUMMARY")
    );
    assert!(
        timeout(
            Duration::from_millis(100),
            mcp.read_notification::<TurnCompletedNotification>("turn/completed")
        )
        .await
        .is_err(),
        "compaction must not complete the unfinished root turn"
    );
    assert_eq!(
        server.requests().await.len(),
        3,
        "no supervisor before root completion"
    );
    release_root
        .send(())
        .expect("root continuation is still running");
    let completed: TurnCompletedNotification = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_notification("turn/completed"),
    )
    .await??;
    assert_eq!(completed.thread_id, thread_id);
    assert_eq!(completed.turn.id, started.turn.id);
    assert_eq!(
        completed.turn.status,
        codex_app_server_protocol::TurnStatus::Completed
    );
    if let Some(deadline) = snoozed_until {
        assert!(
            timeout(
                Duration::from_millis(200),
                server.wait_for_request_count(/*count*/ 4)
            )
            .await
            .is_err()
        );
        assert_eq!(
            state
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(root_id, &goal.goal_id)
                .await?,
            Some(deadline),
            "compaction must preserve the supervisor's future deadline"
        );
    } else {
        timeout(
            DEFAULT_READ_TIMEOUT,
            server.wait_for_request_count(/*count*/ 4),
        )
        .await?;
        let requests = server.requests().await;
        let supervisor: Value = serde_json::from_slice(&requests[3])?;
        assert_ne!(supervisor["client_metadata"]["turn_id"], started.turn.id);
        assert!(supervisor["input"].to_string().contains("ROOT_FINISHED"));
        assert!(
            supervisor["input"]
                .to_string()
                .contains("RETAINED_COMPACTION_SUMMARY")
        );
    }
    timeout(DEFAULT_READ_TIMEOUT, mcp.shutdown_gracefully()).await??;
    server.shutdown().await;
    Ok(())
}
