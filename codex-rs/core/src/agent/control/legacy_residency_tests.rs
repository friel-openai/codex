use super::*;
use codex_protocol::models::MessagePhase;
use codex_state::DirectionalThreadSpawnEdgeStatus;

const LEGACY_TEST_MAX_THREADS: usize = 2;

#[derive(Clone, Copy, Debug)]
enum LegacyTerminalStatus {
    Completed,
    Errored,
    Interrupted,
    Shutdown,
}

impl LegacyTerminalStatus {
    fn event(self, turn_id: &str, child_index: usize) -> EventMsg {
        match self {
            Self::Completed => EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn_id.to_string(),
                last_agent_message: Some(format!("completed child {child_index}")),
                error: None,
                started_at: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
            Self::Errored => EventMsg::Error(ErrorEvent {
                message: format!("errored child {child_index}"),
                codex_error_info: None,
            }),
            Self::Interrupted => EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(turn_id.to_string()),
                reason: TurnAbortReason::Interrupted,
                started_at: None,
                completed_at: None,
                duration_ms: None,
            }),
            Self::Shutdown => EventMsg::ShutdownComplete,
        }
    }

    fn agent_status(self, child_index: usize) -> AgentStatus {
        match self {
            Self::Completed => {
                AgentStatus::Completed(Some(format!("completed child {child_index}")))
            }
            Self::Errored => AgentStatus::Errored(format!("errored child {child_index}")),
            Self::Interrupted => AgentStatus::Interrupted,
            Self::Shutdown => AgentStatus::Shutdown,
        }
    }
}

async fn legacy_harness(network_proxy_enabled: bool) -> AgentControlHarness {
    let home = TempDir::new().expect("create benchmark Codex home");
    let mut cli_overrides = if network_proxy_enabled {
        std::fs::write(
            home.path().join("config.toml"),
            r#"
sandbox_mode = "workspace-write"

[sandbox_workspace_write]
network_access = true
"#,
        )
        .expect("write benchmark config");
        vec![(
            "features.network_proxy.enabled".to_string(),
            TomlValue::Boolean(true),
        )]
    } else {
        Vec::new()
    };
    cli_overrides.push((
        "model".to_string(),
        TomlValue::String("gpt-5.5".to_string()),
    ));
    let mut config = ConfigBuilder::without_managed_config_for_tests()
        .codex_home(home.path().to_path_buf())
        .cli_overrides(cli_overrides)
        .build()
        .await
        .expect("load benchmark config");
    let _ = config.features.disable(Feature::MultiAgentV2);
    config.agent_max_threads = Some(LEGACY_TEST_MAX_THREADS);
    AgentControlHarness::new_with_config(home, config).await
}

async fn spawn_completed_legacy_child(
    harness: &AgentControlHarness,
    parent_thread_id: ThreadId,
    child_index: usize,
    history_bytes: usize,
) -> ThreadId {
    spawn_terminal_legacy_child(
        harness,
        parent_thread_id,
        child_index,
        history_bytes,
        LegacyTerminalStatus::Completed,
    )
    .await
}

async fn spawn_terminal_legacy_child(
    harness: &AgentControlHarness,
    parent_thread_id: ThreadId,
    child_index: usize,
    history_bytes: usize,
    terminal_status: LegacyTerminalStatus,
) -> ThreadId {
    let (child_thread_id, child_thread, turn) =
        spawn_quiescent_legacy_child(harness, parent_thread_id, child_index, history_bytes).await;
    child_thread
        .session
        .send_event(
            turn.as_ref(),
            terminal_status.event(&turn.sub_id, child_index),
        )
        .await;
    child_thread_id
}

async fn spawn_quiescent_legacy_child(
    harness: &AgentControlHarness,
    parent_thread_id: ThreadId,
    child_index: usize,
    history_bytes: usize,
) -> (ThreadId, Arc<CodexThread>, Arc<crate::TurnContext>) {
    let child_thread_id = harness
        .control
        .spawn_agent_with_communication(
            harness.config.clone(),
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                format!("synthetic child {child_index}"),
                /*trigger_turn*/ false,
            ),
            AgentCommunicationContext::new(AgentCommunicationKind::Spawn, parent_thread_id),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("spawn synthetic legacy child")
        .thread_id;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("synthetic child should be loaded");
    timeout(Duration::from_secs(5), async {
        while !child_thread
            .session
            .input_queue
            .has_pending_mailbox_items()
            .await
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("synthetic initial mailbox input should arrive");
    let _ = child_thread
        .session
        .input_queue
        .drain_mailbox_input_items()
        .await;
    let turn = child_thread.session.new_default_turn().await;
    pretty_assertions::assert_eq!(
        child_thread.multi_agent_version(),
        Some(MultiAgentVersion::V1),
        "legacy residency tests must exercise a V1 child"
    );
    let unique_prefix = format!("child-{child_index:08}-");
    let payload = format!(
        "{unique_prefix}{}",
        "x".repeat(history_bytes.saturating_sub(unique_prefix.len()))
    );
    child_thread
        .session
        .record_conversation_items(
            turn.as_ref(),
            std::slice::from_ref(&assistant_message(
                &payload,
                Some(MessagePhase::FinalAnswer),
            )),
        )
        .await;
    child_thread.session.ensure_rollout_materialized().await;
    child_thread
        .session
        .flush_rollout()
        .await
        .expect("flush synthetic child history");
    (child_thread_id, child_thread, turn)
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

async fn wait_for_notification_count(parent_thread: &Arc<CodexThread>, expected: usize) {
    timeout(Duration::from_secs(30), async {
        loop {
            let history = parent_thread.session.clone_history().await;
            if subagent_notification_count(history.raw_items()) >= expected {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completion notifications should quiesce");
}

async fn wait_for_loaded_child_count_at_most(
    harness: &AgentControlHarness,
    parent_thread_id: ThreadId,
    maximum: usize,
    wait: Duration,
) -> bool {
    timeout(wait, async {
        loop {
            let loaded_children = harness
                .manager
                .list_thread_ids()
                .await
                .into_iter()
                .filter(|thread_id| *thread_id != parent_thread_id)
                .count();
            if loaded_children <= maximum {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

#[path = "legacy_residency_tests/benchmark.rs"]
mod benchmark;

#[path = "legacy_residency_tests/lifecycle.rs"]
mod lifecycle;
