use crate::GoalAccountingMode;
use crate::GoalAccountingOutcome;
use crate::GoalUpdate;
use crate::StateRuntime;
use crate::ThreadGoalStatus;
use crate::runtime::test_support::unique_temp_dir;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;

async fn test_runtime() -> anyhow::Result<std::sync::Arc<StateRuntime>> {
    StateRuntime::init(
        crate::SqliteConfig::new_for_testing(unique_temp_dir().as_path().abs()),
        "test-provider".to_string(),
    )
    .await
}

#[tokio::test]
async fn resume_preserves_stopped_goal_and_accounts_exactly_once() -> anyhow::Result<()> {
    let runtime = test_runtime().await?;
    for status in [
        ThreadGoalStatus::Paused,
        ThreadGoalStatus::Blocked,
        ThreadGoalStatus::UsageLimited,
    ] {
        let thread_id = ThreadId::new();
        let original = runtime
            .thread_goals()
            .replace_thread_goal(thread_id, "finish release", status, Some(10_000))
            .await?;
        let GoalAccountingOutcome::Updated(original) = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 7,
                /*token_delta*/ 300,
                GoalAccountingMode::ActiveOrStopped,
                Some(&original.goal_id),
            )
            .await?
        else {
            panic!("stopped goal usage should be recorded");
        };
        let (first, second) = tokio::join!(
            runtime.thread_goals().resume_thread_goal(&original),
            runtime.thread_goals().resume_thread_goal(&original),
        );
        let first = first?;
        let second = second?;
        assert_ne!(first.is_some(), second.is_some());
        let resumed = first.or(second).expect("one resume should succeed");
        let mut expected = original.clone();
        expected.status = ThreadGoalStatus::Active;
        expected.updated_at = resumed.updated_at;
        assert_eq!(resumed, expected);
        assert!(resumed.updated_at > original.updated_at);
    }
    Ok(())
}

#[tokio::test]
async fn resume_rejects_changed_replaced_and_deleted_goals() -> anyhow::Result<()> {
    let runtime = test_runtime().await?;
    let thread_id = ThreadId::new();
    let original = runtime
        .thread_goals()
        .replace_thread_goal(
            thread_id,
            "finish release",
            ThreadGoalStatus::Paused,
            /*token_budget*/ None,
        )
        .await?;
    let edited = runtime
        .thread_goals()
        .update_thread_goal(
            thread_id,
            GoalUpdate {
                objective: Some("finish release and announce it".to_string()),
                status: None,
                token_budget: None,
                expected_goal_id: Some(original.goal_id.clone()),
            },
        )
        .await?
        .expect("goal should exist");
    assert_eq!(
        runtime.thread_goals().resume_thread_goal(&original).await?,
        None
    );
    assert_eq!(
        runtime.thread_goals().get_thread_goal(thread_id).await?,
        Some(edited.clone())
    );

    let replacement = runtime
        .thread_goals()
        .replace_thread_goal(
            thread_id,
            &edited.objective,
            edited.status,
            edited.token_budget,
        )
        .await?;
    assert_eq!(
        runtime.thread_goals().resume_thread_goal(&edited).await?,
        None
    );
    assert_eq!(
        runtime.thread_goals().get_thread_goal(thread_id).await?,
        Some(replacement.clone())
    );
    runtime.thread_goals().delete_thread_goal(thread_id).await?;
    assert_eq!(
        runtime
            .thread_goals()
            .resume_thread_goal(&replacement)
            .await?,
        None
    );
    assert_eq!(
        runtime.thread_goals().get_thread_goal(thread_id).await?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn resume_rejects_every_stale_snapshot_field() -> anyhow::Result<()> {
    let runtime = test_runtime().await?;
    let original = runtime
        .thread_goals()
        .replace_thread_goal(
            ThreadId::new(),
            "finish release",
            ThreadGoalStatus::Paused,
            Some(100),
        )
        .await?;
    let mut stale = vec![original.clone(); 9];
    stale[0].goal_id.push('x');
    stale[1].objective.push('x');
    stale[2].status = ThreadGoalStatus::Blocked;
    stale[3].token_budget = None;
    stale[4].tokens_used += 1;
    stale[5].time_used_seconds += 1;
    stale[6].created_at += chrono::Duration::milliseconds(1);
    stale[7].updated_at += chrono::Duration::milliseconds(1);
    stale[8].thread_id = ThreadId::new();
    for snapshot in stale {
        assert_eq!(
            runtime.thread_goals().resume_thread_goal(&snapshot).await?,
            None
        );
    }
    assert_eq!(
        runtime
            .thread_goals()
            .get_thread_goal(original.thread_id)
            .await?,
        Some(original)
    );
    Ok(())
}

#[tokio::test]
async fn resume_rejects_active_complete_and_exhausted_goals() -> anyhow::Result<()> {
    let runtime = test_runtime().await?;
    for status in [
        ThreadGoalStatus::Active,
        ThreadGoalStatus::BudgetLimited,
        ThreadGoalStatus::Complete,
    ] {
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                ThreadId::new(),
                "finish release",
                status,
                /*token_budget*/ None,
            )
            .await?;
        assert_eq!(
            runtime.thread_goals().resume_thread_goal(&goal).await?,
            None
        );
        assert_eq!(
            runtime
                .thread_goals()
                .get_thread_goal(goal.thread_id)
                .await?,
            Some(goal)
        );
    }
    // Exercise the budget predicate even if a legacy saved row remains paused.
    let goal = runtime
        .thread_goals()
        .replace_thread_goal(
            ThreadId::new(),
            "finish release",
            ThreadGoalStatus::Paused,
            Some(100),
        )
        .await?;
    sqlx::query("UPDATE thread_goals SET tokens_used = 100 WHERE thread_id = ?")
        .bind(goal.thread_id.to_string())
        .execute(runtime.thread_goals().pool.as_ref())
        .await?;
    let exhausted = runtime
        .thread_goals()
        .get_thread_goal(goal.thread_id)
        .await?
        .expect("goal should exist");
    assert_eq!(exhausted.status, ThreadGoalStatus::Paused);
    assert_eq!(
        runtime
            .thread_goals()
            .resume_thread_goal(&exhausted)
            .await?,
        None
    );
    assert_eq!(
        runtime
            .thread_goals()
            .get_thread_goal(goal.thread_id)
            .await?,
        Some(exhausted)
    );
    Ok(())
}
