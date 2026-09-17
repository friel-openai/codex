use super::*;
use crate::config::test_config;
use codex_features::Feature;
use codex_history::ResumedHistory;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::user_input::UserInput;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn indexed_session_root_lookup_stops_at_closed_edges_and_rejects_cycles() {
    let home = tempfile::tempdir().expect("isolated home");
    let mut config = test_config().await;
    config.codex_home = AbsolutePathBuf::try_from(home.path()).expect("absolute home");
    let state_db = codex_rollout::state_db::try_init(&config)
        .await
        .expect("state database");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        home.path().to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db),
    );
    let graph = manager.state.agent_graph_store().expect("indexed graph");
    let root = ThreadId::new();
    let branch = ThreadId::new();
    let leaf = ThreadId::new();
    graph
        .upsert_thread_spawn_edge(
            root,
            branch,
            codex_agent_graph_store::ThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("root edge");
    graph
        .upsert_thread_spawn_edge(
            branch,
            leaf,
            codex_agent_graph_store::ThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("leaf edge");
    assert_eq!(manager.agent_session_root_thread_id(leaf).await, Some(root));
    graph
        .set_thread_spawn_edge_status(
            branch,
            codex_agent_graph_store::ThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("promotion boundary");
    assert_eq!(
        manager.agent_session_root_thread_id(leaf).await,
        Some(branch)
    );
    graph
        .upsert_thread_spawn_edge(
            leaf,
            branch,
            codex_agent_graph_store::ThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("malformed cycle");
    assert_eq!(manager.agent_session_root_thread_id(leaf).await, None);
    assert!(manager.state.threads.read().await.is_empty());
    assert!(!home.path().join("sessions").exists());
}

#[tokio::test]
async fn retained_membership_ignores_an_independent_legacy_registry_with_the_same_session_id() {
    let home = tempfile::tempdir().expect("isolated home");
    let mut config = test_config().await;
    config.codex_home =
        codex_utils_absolute_path::AbsolutePathBuf::try_from(home.path()).expect("absolute home");
    config.cwd = config.codex_home.clone();
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("V1 fixture");
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("root");
    let control = root.thread.session.services.agent_control.clone();
    control.register_session_root(root.thread_id, /*current_parent_thread_id*/ None);
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });
    let child_id = control
        .spawn_agent(
            config.clone(),
            vec![UserInput::Text {
                text: "wait".to_string(),
                text_elements: Vec::new(),
            }],
            Some(source.clone()),
        )
        .await
        .expect("owned child");

    // Persist an independently resumed legacy child that sorts first but has
    // no current descendants. Its session ID alone must not confer ownership.
    let legacy_id = ThreadId::from_string("00000000-0000-0000-0000-000000000000").expect("nil ID");
    let metadata = SessionMetaLine {
        meta: SessionMeta {
            id: legacy_id,
            session_id: root.thread_id.into(),
            parent_thread_id: Some(root.thread_id),
            source,
            cwd: config.cwd.to_path_buf(),
            multi_agent_version: Some(MultiAgentVersion::V1),
            ..SessionMeta::default()
        },
        git: None,
    };
    let rollout_path = home.path().join("legacy.jsonl");
    std::fs::write(
        &rollout_path,
        format!(
            "{}\n",
            serde_json::to_string(&codex_rollout::RolloutLine {
                timestamp: "2026-09-04T00:00:00Z".to_string(),
                ordinal: None,
                item: RolloutItem::SessionMeta(metadata.clone()),
            })
            .expect("metadata JSON")
        ),
    )
    .expect("legacy rollout");
    let legacy = manager
        .start_thread(StartThreadOptions {
            initial_history: InitialHistory::Resumed(ResumedHistory {
                conversation_id: legacy_id,
                history: Arc::new(vec![RolloutItem::SessionMeta(metadata)]),
                rollout_path: Some(rollout_path),
            }),
            ..StartThreadOptions::new(config)
        })
        .await
        .expect("independent legacy resume");
    assert_eq!(
        legacy
            .thread
            .session
            .services
            .agent_control
            .current_membership_root_thread_id(),
        root.thread_id
    );
    assert!(
        legacy
            .thread
            .session
            .services
            .agent_control
            .get_agent_metadata(root.thread_id)
            .is_none()
    );
    let loaded = manager
        .current_agent_members(root.thread_id)
        .await
        .expect("loaded root membership");
    assert_eq!(
        loaded
            .iter()
            .map(|member| member.thread_id)
            .collect::<Vec<_>>(),
        vec![child_id]
    );

    let eviction = manager
        .prepare_current_agent_membership_eviction(root.thread_id)
        .await
        .expect("capture members");
    for id in [child_id, root.thread_id] {
        assert!(
            eviction
                .unload_candidate_runtime_preserving_identity(id)
                .await
                .expect("unload runtime")
        );
    }
    // No archive succeeded: retain the cold identity rather than closing it.
    eviction
        .evict_exact(&[])
        .await
        .expect("retain unsuccessful members");
    assert!(manager.get_thread(legacy_id).await.is_ok());
    assert!(manager.get_thread(root.thread_id).await.is_err());
    let cold = manager
        .current_agent_members(root.thread_id)
        .await
        .expect("retained root membership");
    assert_eq!(
        cold.iter()
            .map(|member| member.thread_id)
            .collect::<Vec<_>>(),
        vec![child_id]
    );
    assert_eq!(cold[0].parent_thread_id, root.thread_id);
    legacy
        .thread
        .shutdown_and_wait()
        .await
        .expect("shutdown legacy runtime");
}
