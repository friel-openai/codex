use super::*;
use crate::app::test_support::make_test_app;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn placed_side_rejects_parent_after_active_thread_changes() -> color_eyre::Result<()> {
    let mut app = make_test_app().await;
    let original_parent = ThreadId::new();
    app.active_thread_id = Some(ThreadId::new());
    assert!(!app.placed_side_parent_is_current(original_parent));
    let mut tui = crate::tui::test_support::make_test_tui()?;
    assert!(matches!(
        app.handle_start_placed_side(&mut tui, original_parent, ForkPanePlacement::Right)
            .await?,
        AppRunControl::Continue
    ));
    Ok(())
}

#[tokio::test]
async fn placed_side_launch_config_projects_live_thread_settings() {
    let mut app = make_test_app().await;
    app.config.model = Some("disk-model".to_string());
    app.config.model_reasoning_effort = Some(ReasoningEffortConfig::Low);
    app.config.service_tier = Some("disk-tier".to_string());
    app.config
        .permissions
        .approval_policy
        .set(codex_protocol::protocol::AskForApproval::Never)
        .expect("app approval policy");
    app.chat_widget.set_model("live-model");
    app.chat_widget
        .set_reasoning_effort(Some(ReasoningEffortConfig::High));
    app.chat_widget
        .set_service_tier(Some("priority".to_string()));
    app.chat_widget
        .set_approval_policy(AskForApproval::OnRequest);

    let launch_config = app.placed_side_launch_config();

    assert_eq!(
        (
            launch_config.model,
            launch_config.model_reasoning_effort,
            launch_config.service_tier,
            launch_config.permissions.approval_policy.value(),
        ),
        (
            Some("live-model".to_string()),
            Some(ReasoningEffortConfig::High),
            Some("priority".to_string()),
            codex_protocol::protocol::AskForApproval::OnRequest,
        )
    );
}

#[test]
fn placed_side_failure_messages_snapshot() {
    insta::assert_snapshot!(
        "placed_side_failure_messages",
        [
            SIDE_NO_STARTED_CONVERSATION_MESSAGE.to_string(),
            REMOTE_SIDE_PANE_UNAVAILABLE_MESSAGE.to_string(),
            SIDE_PLACEMENT_REQUIRES_PANE_HOST_MESSAGE.to_string(),
            placed_side_spawn_failure_message("tmux exited with status 1"),
        ]
        .join("\n")
    );
}
