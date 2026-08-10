use super::*;
use pretty_assertions::assert_eq;

fn complete_checkpoint_settings_config(mut config: Config, suffix: &str) -> Config {
    let target_cwd = config.codex_home.join(format!("checkpoint-{suffix}-cwd"));
    std::fs::create_dir_all(target_cwd.as_path()).expect("create checkpoint cwd");
    config.cwd = target_cwd;
    let extra_workspace_root = config.cwd.join("checkpoint-extra-root");
    config.workspace_roots_explicit = true;
    config.workspace_roots = vec![config.cwd.clone(), extra_workspace_root];
    config
        .permissions
        .set_workspace_roots(config.workspace_roots.clone());
    let profile_workspace_root = config.cwd.join("checkpoint-profile-root");
    config
        .permissions
        .set_permission_profile_from_session_snapshot(
            crate::config::PermissionProfileSnapshot::active_with_profile_workspace_roots(
                codex_protocol::models::PermissionProfile::read_only(),
                codex_protocol::models::ActivePermissionProfile::read_only(),
                vec![profile_workspace_root],
            ),
        )
        .expect("install checkpoint permission profile");
    config.approvals_reviewer = ApprovalsReviewer::AutoReview;
    config.service_tier = Some("flex".to_string());
    config.model_reasoning_effort = Some(codex_protocol::openai_models::ReasoningEffort::High);
    config.model_reasoning_summary = Some(codex_protocol::config_types::ReasoningSummary::Auto);
    config.personality = Some(codex_protocol::config_types::Personality::Friendly);
    config.windows_sandbox_level =
        codex_protocol::config_types::WindowsSandboxLevel::RestrictedToken;
    config.initial_collaboration_mode = Some(codex_protocol::config_types::CollaborationMode {
        mode: codex_protocol::config_types::ModeKind::Plan,
        settings: codex_protocol::config_types::Settings {
            model: config
                .model
                .clone()
                .expect("test config should select a model"),
            reasoning_effort: config.model_reasoning_effort.clone(),
            developer_instructions: Some("persisted checkpoint instructions".to_string()),
        },
    });
    config
}

#[tokio::test]
async fn cold_adoption_restores_checkpoint_workspace_roots_without_predecessor() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let target_config = complete_checkpoint_settings_config(harness.config.clone(), "adopted");
    let target = harness
        .manager
        .start_thread(StartThreadOptions::new(target_config))
        .await
        .expect("start adoptable root");
    let expected_settings = target
        .thread
        .config_snapshot()
        .await
        .into_thread_settings_snapshot();
    target.thread.ensure_rollout_materialized().await;
    let turn_context = target.thread.session.new_default_turn().await;
    let prepared_window_advance = target
        .thread
        .session
        .prepare_auto_compact_window_advance()
        .await;
    target
        .thread
        .session
        .replace_compacted_history(
            &turn_context,
            vec![assistant_message("checkpoint-local adoption history", None)],
            /*reference_context_item*/ None,
            /*world_state_baseline*/ None,
            CompactedHistoryMetadata {
                message: "checkpoint adoption".to_string(),
                prepared_window_advance,
            },
        )
        .await
        .expect("adoption checkpoint should persist");
    target
        .thread
        .flush_rollout()
        .await
        .expect("flush adoptable root checkpoint");
    let active_path = target
        .thread
        .rollout_path()
        .expect("adoptable root rollout path");
    let (active_items, _, parse_errors) = RolloutRecorder::load_rollout_items(&active_path)
        .await
        .expect("load adoptable root checkpoint");
    assert_eq!(parse_errors, 0);
    let predecessor_path = active_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::RolloutReference(reference) => Some(reference.rollout_path.clone()),
            _ => None,
        })
        .expect("checkpoint predecessor");
    std::fs::remove_file(predecessor_path).expect("remove checkpoint predecessor");
    let state = harness.control.upgrade().expect("manager should be live");
    harness
        .control
        .unload_agent_thread(&state, target.thread_id)
        .await
        .expect("unload adoptable root");
    let agent_path = AgentPath::root()
        .join("adopted_worker")
        .expect("adopted worker path");
    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        agent_path.clone(),
        Vec::new(),
        "continue the existing thread".to_string(),
        /*trigger_turn*/ false,
    );
    let context = AgentCommunicationContext::new(AgentCommunicationKind::Spawn, parent_thread_id);
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(agent_path),
        agent_nickname: None,
        agent_role: None,
    });

    harness
        .control
        .adopt_agent_with_communication(
            harness.config.clone(),
            target.thread_id,
            communication,
            context,
            source,
            /*parent_turn_id*/ None,
        )
        .await
        .expect("cold adoption should use the active checkpoint");
    let adopted = harness
        .manager
        .get_thread(target.thread_id)
        .await
        .expect("adopted root should load");
    let restored = adopted.config_snapshot().await;
    assert_eq!(restored.into_thread_settings_snapshot(), expected_settings);
}

#[tokio::test]
async fn cold_registered_agent_restores_complete_checkpoint_settings_without_predecessor() {
    let harness = AgentControlHarness::new().await;
    let target_config =
        complete_checkpoint_settings_config(harness.config.clone(), "registered-agent");
    let target = harness
        .manager
        .start_thread(StartThreadOptions::new(target_config))
        .await
        .expect("start registered agent fixture");
    harness
        .control
        .register_session_root(target.thread_id, /*current_parent_thread_id*/ None);
    let expected_settings = target
        .thread
        .config_snapshot()
        .await
        .into_thread_settings_snapshot();
    target.thread.ensure_rollout_materialized().await;
    let turn_context = target.thread.session.new_default_turn().await;
    let prepared_window_advance = target
        .thread
        .session
        .prepare_auto_compact_window_advance()
        .await;
    target
        .thread
        .session
        .replace_compacted_history(
            &turn_context,
            vec![assistant_message("registered checkpoint history", None)],
            /*reference_context_item*/ None,
            /*world_state_baseline*/ None,
            CompactedHistoryMetadata {
                message: "registered agent checkpoint".to_string(),
                prepared_window_advance,
            },
        )
        .await
        .expect("registered agent checkpoint should persist");
    target
        .thread
        .flush_rollout()
        .await
        .expect("flush registered agent checkpoint");
    let active_path = target
        .thread
        .rollout_path()
        .expect("registered agent rollout path");
    let (active_items, _, parse_errors) = RolloutRecorder::load_rollout_items(&active_path)
        .await
        .expect("load registered agent checkpoint");
    assert_eq!(parse_errors, 0);
    let predecessor_path = active_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::RolloutReference(reference) => Some(reference.rollout_path.clone()),
            _ => None,
        })
        .expect("registered agent checkpoint predecessor");
    std::fs::remove_file(predecessor_path).expect("remove registered agent predecessor");
    let external_rollout_dir = harness
        .config
        .codex_home
        .join("external-checkpoint-rollout");
    std::fs::create_dir_all(external_rollout_dir.as_path())
        .expect("create external rollout directory");
    let external_rollout_path = external_rollout_dir.join(
        active_path
            .file_name()
            .expect("active rollout should have a file name"),
    );
    std::fs::copy(&active_path, &external_rollout_path)
        .expect("copy certified checkpoint to selected external path");
    let state = harness.control.upgrade().expect("manager should be live");
    harness
        .control
        .unload_agent_thread(&state, target.thread_id)
        .await
        .expect("unload registered agent");
    let state_db = harness
        .state_db
        .as_ref()
        .expect("state database should be available");
    let mut stale_metadata = state_db
        .get_thread(target.thread_id)
        .await
        .expect("read registered agent metadata")
        .expect("registered agent metadata should exist");
    stale_metadata.rollout_path = external_rollout_path.to_path_buf();
    stale_metadata.model_provider = "stale-unavailable-provider".to_string();
    stale_metadata.model = Some("stale-indexed-model".to_string());
    stale_metadata.approval_mode =
        serde_json::to_string(&AskForApproval::Never).expect("approval mode should serialize");
    stale_metadata.sandbox_policy = serde_json::to_string(&PermissionProfile::workspace_write())
        .expect("permission profile should serialize");
    state_db
        .upsert_thread(&stale_metadata)
        .await
        .expect("install stale indexed metadata after checkpoint commit");
    std::fs::write(&active_path, b"not the selected rollout\n")
        .expect("replace the canonical-path decoy after unload");

    harness
        .control
        .ensure_agent_loaded(harness.config.clone(), target.thread_id)
        .await
        .expect("registered agent should reload from the active checkpoint");
    let restored = harness
        .manager
        .get_thread(target.thread_id)
        .await
        .expect("registered agent should be loaded")
        .config_snapshot()
        .await;
    assert_eq!(restored.into_thread_settings_snapshot(), expected_settings);
}
