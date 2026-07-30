use super::is_resident_session_source;
use crate::StartThreadOptions;
use crate::ThreadManager;
use crate::agent::AgentControl;
use crate::agent::control::AgentInputDelivery;
use crate::agent::registry::AgentMetadata;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::config::test_config;
use crate::context::ContextualUserFragment;
use crate::context::SubagentNotification;
use crate::thread_manager::ThreadManagerState;
use codex_features::Feature;
use codex_login::CodexAuth;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::timeout;

#[test]
fn goal_supervisor_helper_is_not_an_agent_resident() {
    let parent_thread_id = ThreadId::new();
    let worker_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });
    let supervisor_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });

    assert!(is_resident_session_source(&worker_source));
    assert!(!is_resident_session_source(&supervisor_source));
}

#[tokio::test]
async fn residency_slot_reservation_unloads_oldest_idle_v2_agent() {
    assert_residency_slot_unloads_oldest_idle_agent(MultiAgentVersion::V2).await;
}

#[tokio::test]
async fn residency_slot_reservation_unloads_oldest_idle_v1_agent() {
    assert_residency_slot_unloads_oldest_idle_agent(MultiAgentVersion::V1).await;
}

#[tokio::test]
async fn completion_parent_gets_one_temporary_slot_above_equal_capacity() {
    let mut config = test_config().await;
    let _ = config.features.disable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Collab);
    config.agent_max_threads = Some(1);
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");
    let child_thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();

    let child_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V1,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    child_slot.commit(child_thread_id);
    let ordinary_err = match control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V1,
            Some(child_thread_id),
        )
        .await
    {
        Ok(_) => panic!("ordinary reload must remain bounded by execution capacity"),
        Err(err) => err,
    };
    assert!(matches!(
        ordinary_err.details(),
        CodexErrorDetails::AgentLimitReached { max_threads: 1 }
    ));

    let parent_slot = control
        .reserve_agent_residency_slot_for_completion_parent(
            &state,
            &config,
            MultiAgentVersion::V1,
            parent_thread_id,
            child_thread_id,
        )
        .await
        .expect("completion delivery needs one temporary slot beyond equal capacity");
    parent_slot.commit(parent_thread_id);
    assert_eq!(control.agent_residency.resident_count(), 2);
}

#[tokio::test]
async fn idle_residency_is_bounded_below_v2_execution_capacity() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 33;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");
    let mut first_thread_id = None;

    for index in 0..9 {
        let slot = control
            .reserve_agent_residency_slot(
                &state,
                &config,
                MultiAgentVersion::V2,
                /*protected_thread_id*/ None,
            )
            .await
            .expect("idle residents must not consume execution capacity");
        let thread = spawn_subagent(
            &control,
            &state,
            config.clone(),
            root.thread_id,
            &format!("worker-{index}"),
        )
        .await;
        first_thread_id.get_or_insert(thread.thread_id);
        slot.commit(thread.thread_id);
        mark_thread_completed(thread.thread.as_ref()).await;
    }

    assert!(
        manager
            .get_thread(first_thread_id.expect("first child exists"))
            .await
            .is_err(),
        "the oldest idle child should be evicted before reaching the 32-agent execution limit"
    );
}

#[tokio::test]
async fn active_agents_can_exceed_idle_residency_limit() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 33;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");
    let mut thread_ids = Vec::new();

    for index in 0..9 {
        let slot = control
            .reserve_agent_residency_slot(
                &state,
                &config,
                MultiAgentVersion::V2,
                /*protected_thread_id*/ None,
            )
            .await
            .expect("active children may exceed the idle-resident target");
        let thread = spawn_subagent(
            &control,
            &state,
            config.clone(),
            root.thread_id,
            &format!("active-worker-{index}"),
        )
        .await;
        slot.commit(thread.thread_id);
        thread_ids.push(thread.thread_id);
    }

    for thread_id in thread_ids {
        assert!(
            manager.get_thread(thread_id).await.is_ok(),
            "nonterminal children must not be evicted to enforce the idle-resident target"
        );
    }
}

#[tokio::test]
async fn completed_v2_agents_are_trimmed_after_parallel_work() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 33;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");
    let mut agents = Vec::new();

    for index in 0..12 {
        let slot = control
            .reserve_agent_residency_slot(
                &state,
                &config,
                MultiAgentVersion::V2,
                /*protected_thread_id*/ None,
            )
            .await
            .expect("active children may exceed the idle-resident target");
        let thread = spawn_subagent(
            &control,
            &state,
            config.clone(),
            root.thread_id,
            &format!("parallel-worker-{index}"),
        )
        .await;
        let agent_path = AgentPath::root()
            .join(&format!("parallel_worker_{index}"))
            .expect("create child agent path");
        let metadata_slot = control
            .state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("reserve child metadata");
        metadata_slot.commit(AgentMetadata {
            agent_id: Some(thread.thread_id),
            parent_thread_id: Some(root.thread_id),
            agent_path: Some(agent_path),
            ..Default::default()
        });
        slot.commit(thread.thread_id);
        agents.push(thread);
    }

    let first_thread_id = agents[0].thread_id;
    for agent in &agents {
        mark_thread_completed(agent.thread.as_ref()).await;
        control.schedule_agent_residency_trim(
            &config,
            MultiAgentVersion::V2,
            &agent.thread.session_source,
        );
    }

    timeout(Duration::from_secs(5), async {
        loop {
            let loaded_children = manager
                .list_thread_ids()
                .await
                .into_iter()
                .filter(|thread_id| *thread_id != root.thread_id)
                .count();
            if loaded_children <= 8 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completed v2 children should be trimmed without another spawn");

    assert_eq!(
        control.get_status(first_thread_id).await,
        crate::agent::AgentStatus::Completed(Some("done".to_string()))
    );
    let status_rx = control
        .subscribe_status(first_thread_id)
        .await
        .expect("wait_agent must receive the completed cold status");
    assert_eq!(
        status_rx.borrow().clone(),
        crate::agent::AgentStatus::Completed(Some("done".to_string()))
    );
    let listed = control
        .list_agents(&SessionSource::Cli, /*path_prefix*/ None)
        .await
        .expect("list cold agents");
    assert!(listed.iter().any(|agent| {
        agent.agent_name == "/root/parallel_worker_0"
            && agent.agent_status == crate::agent::AgentStatus::Completed(Some("done".to_string()))
    }));
}

async fn assert_residency_slot_unloads_oldest_idle_agent(multi_agent_version: MultiAgentVersion) {
    let mut config = test_config().await;
    match multi_agent_version {
        MultiAgentVersion::V1 => {
            let _ = config.features.disable(Feature::MultiAgentV2);
            let _ = config.features.enable(Feature::Collab);
            config.agent_max_threads = Some(1);
        }
        MultiAgentVersion::V2 => {
            let _ = config.features.enable(Feature::MultiAgentV2);
            config.multi_agent_v2.max_concurrent_threads_per_session = 2;
        }
        MultiAgentVersion::Disabled => panic!("residency requires multi-agent support"),
    }
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");

    let first_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            multi_agent_version,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first = spawn_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_completed(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            multi_agent_version,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second = spawn_subagent(&control, &state, config, root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
}

#[tokio::test]
async fn interrupted_v2_agent_is_lost_after_residency_eviction() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");

    let first_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first = spawn_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_interrupted(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first interrupted idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second = spawn_subagent(&control, &state, config.clone(), root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);
    mark_thread_completed(second.thread.as_ref()).await;

    let err = control
        .ensure_agent_loaded(config, first.thread_id)
        .await
        .expect_err("evicted interrupted agent should stay lost");
    match err.details() {
        CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
        _ => panic!("expected ThreadNotFound, got {err:?}"),
    }

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
}

#[tokio::test]
async fn interrupted_v2_agent_remains_addressable_after_residency_eviction() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");

    let first_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first = spawn_subagent(
        &control,
        &state,
        config.clone(),
        root.thread_id,
        "interruptible",
    )
    .await;
    let agent_path = AgentPath::root()
        .join("interruptible")
        .expect("create child path");
    let metadata_slot = control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("reserve interrupted child metadata");
    metadata_slot.commit(AgentMetadata {
        agent_id: Some(first.thread_id),
        parent_thread_id: Some(root.thread_id),
        agent_path: Some(agent_path),
        ..Default::default()
    });
    first_slot.commit(first.thread_id);
    mark_thread_interrupted(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second child should evict the interrupted resident");
    let second = spawn_subagent(
        &control,
        &state,
        config.clone(),
        root.thread_id,
        "replacement",
    )
    .await;
    second_slot.commit(second.thread_id);
    mark_thread_completed(second.thread.as_ref()).await;

    assert_eq!(
        control.get_status(first.thread_id).await,
        crate::agent::AgentStatus::Interrupted
    );
    let listed = control
        .list_agents(&SessionSource::Cli, /*path_prefix*/ None)
        .await
        .expect("list interrupted cold agent");
    assert!(listed.iter().any(|agent| {
        agent.agent_name == "/root/interruptible"
            && agent.agent_status == crate::agent::AgentStatus::Interrupted
    }));
    control
        .ensure_agent_loaded(config.clone(), first.thread_id)
        .await
        .expect("interrupted cold agent must remain reusable");
    assert!(manager.get_thread(first.thread_id).await.is_ok());
    control
        .deliver_inter_agent_communication_to_agent(
            config,
            first.thread_id,
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root()
                    .join("interruptible")
                    .expect("create recipient path"),
                Vec::new(),
                "follow-up after interrupt".to_string(),
                /*trigger_turn*/ false,
            ),
            AgentCommunicationContext::new(AgentCommunicationKind::Followup, root.thread_id),
            AgentInputDelivery::Queue,
            /*parent_turn_id*/ None,
        )
        .await
        .expect("interrupted cold agent must accept a follow-up message");
}

#[tokio::test]
async fn pathless_v2_interrupted_watcher_does_not_block_residency_eviction() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = root.thread.session.services.agent_control.clone();
    let state = control.upgrade().expect("thread manager should be live");
    let source = pathless_thread_spawn_source(root.thread_id);
    let first_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first = state
        .spawn_new_thread_with_source(
            config.clone(),
            control.clone(),
            source.clone(),
            /*history_mode*/ None,
            Some(root.thread_id),
            /*forked_from_thread_id*/ None,
            Some(ThreadSource::Subagent),
            /*metrics_service_name*/ None,
            /*inherited_environments*/ None,
            /*inherited_exec_policy*/ None,
            Default::default(),
            /*environments*/ None,
        )
        .await
        .expect("spawn first pathless v2 agent");
    let registry_slot = control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("reserve first registry slot");
    registry_slot.commit(AgentMetadata {
        agent_id: Some(first.thread_id),
        ..Default::default()
    });
    first_slot.commit(first.thread_id);
    state.notify_thread_created(first.thread_id);
    assert!(control.maybe_start_completion_watcher(
        first.thread_id,
        Some(source),
        first.thread_id.to_string(),
        /*child_agent_path*/ None,
        MultiAgentVersion::V2,
    ));
    mark_thread_interrupted(first.thread.as_ref()).await;
    timeout(Duration::from_secs(5), async {
        while control.get_status(first.thread_id).await != crate::agent::AgentStatus::Interrupted {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first pathless v2 agent should become interrupted");

    let second_slot = timeout(
        Duration::from_secs(5),
        control.reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        ),
    )
    .await
    .expect("interrupted completion watcher should release residency eviction")
    .expect("second resident slot should evict the interrupted agent");
    drop(second_slot);

    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let history = root.thread.session.clone_history().await;
    assert_eq!(subagent_notification_count(history.raw_items()), 1);
}

fn pathless_thread_spawn_source(parent_thread_id: ThreadId) -> SessionSource {
    SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: Some("explorer".to_string()),
    })
}

fn subagent_notification_count(history_items: &[ResponseItem]) -> usize {
    history_items
        .iter()
        .filter(|item| {
            let ResponseItem::Message { role, content, .. } = item else {
                return false;
            };
            role == "user"
                && content.iter().any(|content_item| match content_item {
                    ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                        SubagentNotification::matches_text(text)
                    }
                    ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
                })
        })
        .count()
}

async fn spawn_subagent(
    control: &AgentControl,
    state: &Arc<ThreadManagerState>,
    config: Config,
    parent_thread_id: ThreadId,
    label: &str,
) -> crate::thread_manager::NewThread {
    state
        .spawn_new_thread_with_source(
            config,
            control.clone(),
            SessionSource::SubAgent(SubAgentSource::Other(label.to_string())),
            /*history_mode*/ None,
            Some(parent_thread_id),
            /*forked_from_thread_id*/ None,
            Some(ThreadSource::Subagent),
            /*metrics_service_name*/ None,
            /*inherited_environments*/ None,
            /*inherited_exec_policy*/ None,
            Default::default(),
            /*environments*/ None,
        )
        .await
        .expect("spawn subagent")
}

async fn mark_thread_completed(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn mark_thread_interrupted(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(turn.sub_id.clone()),
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                completed_at: None,
                duration_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn clear_active_turn(thread: &CodexThread) {
    // The fixture has no task runner to clear the turn after the terminal event.
    *thread.session.active_turn.lock().await = None;
}
