use super::*;
use pretty_assertions::assert_eq;

async fn make_body_after_prefix_session() -> (
    Arc<Session>,
    Arc<TurnContext>,
    async_channel::Receiver<Event>,
) {
    make_session_and_context_with_auth_and_config_and_rx(
        CodexAuth::from_api_key("Test API Key"),
        Vec::new(),
        |config| {
            config.model_auto_compact_token_limit_scope =
                codex_protocol::config_types::AutoCompactTokenLimitScope::BodyAfterPrefix;
        },
    )
    .await
}

#[derive(Debug, PartialEq)]
struct PersistedCheckpointState {
    history: Vec<ResponseItem>,
    token_state: (Option<TokenUsageInfo>, Option<RateLimitSnapshot>),
    previous_turn_settings: Option<PreviousTurnSettings>,
    reference_context: Option<TurnContextItem>,
    world_state: Option<crate::context::world_state::WorldStateSnapshot>,
    window_number: u64,
    window_ids: AutoCompactWindowIds,
    window_snapshot: AutoCompactWindowSnapshot,
}

async fn persisted_checkpoint_state(session: &Session) -> PersistedCheckpointState {
    let state = session.state.lock().await;
    PersistedCheckpointState {
        history: state.clone_history().raw_items().to_vec(),
        token_state: state.token_info_and_rate_limits(),
        previous_turn_settings: state.previous_turn_settings(),
        reference_context: state.reference_context_item(),
        world_state: state.history.world_state_baseline(),
        window_number: state.auto_compact_window_number(),
        window_ids: state.auto_compact_window_ids(),
        window_snapshot: state.auto_compact_window_snapshot(),
    }
}

async fn load_and_resume_current_state(session: &Session) -> PersistedCheckpointState {
    let stored = session
        .services
        .thread_store
        .load_latest_model_context(codex_thread_store::LoadThreadHistoryParams {
            thread_id: session.thread_id,
            rollout_path: None,
            include_archived: false,
        })
        .await
        .expect("load persisted context after storage recovery");
    let (resumed, _turn_context, _events) = make_session_and_context_with_rx().await;
    resumed
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: session.thread_id,
            history: Arc::new(stored.items),
            rollout_path: None,
        }))
        .await
        .expect("cold resume persisted context");
    persisted_checkpoint_state(&resumed).await
}

async fn install_nondefault_checkpoint_state(session: &Session, turn_context: &TurnContext) {
    let mut state = session.state.lock().await;
    let reference_context = turn_context.to_turn_context_item();
    state.set_previous_turn_settings(Some(PreviousTurnSettings {
        model: reference_context.model.clone(),
        comp_hash: reference_context.comp_hash.clone(),
        realtime_active: reference_context.realtime_active,
    }));
    state.set_reference_context_item(Some(reference_context));
    state.set_rate_limits(RateLimitSnapshot {
        limit_id: Some("codex".to_string()),
        limit_name: Some("Codex".to_string()),
        primary: Some(RateLimitWindow {
            used_percent: 41.0,
            window_minutes: Some(300),
            resets_at: Some(1_700),
        }),
        secondary: None,
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    });
    state.request_new_context_window();
    assert!(state.claim_token_budget_reminder());
    assert!(state.claim_auto_compact_fallback());
}

#[tokio::test]
async fn compaction_checkpoint_uses_replacement_token_state_without_predecessor() {
    let (mut session, turn_context, _events) = make_body_after_prefix_session().await;
    let stable_path = attach_thread_persistence(
        Arc::get_mut(&mut session).expect("session should have one owner"),
    )
    .await;
    session
        .record_conversation_items(
            turn_context.as_ref(),
            &[user_message("old context ".repeat(5_000).as_str())],
        )
        .await;
    session.flush_rollout().await.expect("flush old context");
    let base_instructions = session.get_base_instructions().await;
    let old_estimate = session
        .clone_history()
        .await
        .estimate_token_count_with_base_instructions(&base_instructions)
        .expect("estimate old context");

    let prepared_window_advance = session.prepare_auto_compact_window_advance().await;
    session
        .replace_compacted_history(
            &turn_context,
            vec![user_message("small replacement")],
            /*reference_context_item*/ None,
            /*world_state_baseline*/ None,
            CompactedHistoryMetadata {
                message: "small replacement".to_string(),
                prepared_window_advance,
            },
        )
        .await
        .expect("replacement checkpoint should persist");
    session
        .flush_rollout()
        .await
        .expect("flush replacement checkpoint");

    let live_state = Arc::clone(&session.state).lock_owned().await;
    let expected_history = live_state.clone_history().raw_items().to_vec();
    let expected_token_state = live_state.token_info_and_rate_limits();
    let expected_window = (
        live_state.auto_compact_window_number(),
        live_state.auto_compact_window_ids(),
        live_state.auto_compact_window_snapshot(),
    );
    let replacement_estimate = expected_token_state
        .0
        .as_ref()
        .expect("replacement token info")
        .last_token_usage
        .total_tokens;
    assert_ne!(replacement_estimate, old_estimate);
    assert_eq!(
        expected_window.2.prefill_input_tokens,
        Some(replacement_estimate)
    );
    drop(live_state);

    let (active_items, _, parse_errors) = RolloutRecorder::load_rollout_items(&stable_path)
        .await
        .expect("load replacement checkpoint");
    assert_eq!(parse_errors, 0);
    let persisted_token_count = active_items.iter().rev().find_map(|item| match item {
        RolloutItem::EventMsg(EventMsg::TokenCount(event)) => Some(event.clone()),
        _ => None,
    });
    assert_eq!(
        persisted_token_count.map(|event| (event.info, event.rate_limits)),
        Some(expected_token_state.clone())
    );
    let predecessor_path = active_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::RolloutReference(reference) => Some(reference.rollout_path.clone()),
            _ => None,
        })
        .expect("replacement checkpoint predecessor");
    std::fs::remove_file(predecessor_path).expect("remove replacement predecessor");

    let latest = session
        .services
        .thread_store
        .load_latest_model_context(codex_thread_store::LoadThreadHistoryParams {
            thread_id: session.thread_id,
            rollout_path: None,
            include_archived: false,
        })
        .await
        .expect("load replacement checkpoint without predecessor");
    let (resumed, _resumed_turn_context, _resumed_events) = make_body_after_prefix_session().await;
    resumed
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: session.thread_id,
            history: Arc::new(latest.items),
            rollout_path: Some(stable_path),
        }))
        .await
        .expect("cold resume replacement checkpoint");
    let resumed_state = resumed.state.lock().await;
    assert_eq!(resumed_state.clone_history().raw_items(), expected_history);
    assert_eq!(
        resumed_state.token_info_and_rate_limits(),
        expected_token_state
    );
    assert_eq!(
        (
            resumed_state.auto_compact_window_number(),
            resumed_state.auto_compact_window_ids(),
            resumed_state.auto_compact_window_snapshot(),
        ),
        expected_window
    );
}

#[tokio::test]
async fn failed_compaction_restores_state_and_allows_persisted_continuation() {
    let (mut session, turn_context, _events) = make_session_and_context_with_rx().await;
    attach_in_memory_thread_store(
        Arc::get_mut(&mut session).expect("session should have one owner"),
    )
    .await;
    session
        .record_conversation_items(
            turn_context.as_ref(),
            &[user_message("history before failed compaction")],
        )
        .await;
    session
        .flush_rollout()
        .await
        .expect("flush initial history");
    install_nondefault_checkpoint_state(&session, &turn_context).await;
    session.rotate_current_segment_state_checkpoint().await;
    session
        .flush_rollout()
        .await
        .expect("flush pre-failure checkpoint");
    let state_before_compaction = session.state.lock().await.checkpoint_mutation_snapshot();

    let failure =
        super::super::checkpoint_rotation_test_support::CheckpointPersistenceFailure::install(
            session.thread_id,
        );
    let prepared_window_advance = session.prepare_auto_compact_window_advance().await;
    session
        .replace_compacted_history(
            &turn_context,
            vec![user_message("replacement that must be restored")],
            /*reference_context_item*/ None,
            /*world_state_baseline*/ None,
            CompactedHistoryMetadata {
                message: "failed compaction".to_string(),
                prepared_window_advance,
            },
        )
        .await
        .expect("failed compaction persistence should restore live state");
    assert!(
        session
            .state
            .lock()
            .await
            .has_same_checkpoint_mutation_state(&state_before_compaction)
    );
    drop(failure);

    session
        .record_conversation_items(
            turn_context.as_ref(),
            &[user_message("continued after failed compaction")],
        )
        .await;
    session
        .flush_rollout()
        .await
        .expect("flush continuation after storage recovery");
    assert_eq!(
        load_and_resume_current_state(&session).await,
        persisted_checkpoint_state(&session).await
    );
}

#[tokio::test]
async fn failed_rollback_restores_state_and_allows_persisted_continuation() {
    let (mut session, turn_context, _events) = make_session_and_context_with_rx().await;
    attach_in_memory_thread_store(
        Arc::get_mut(&mut session).expect("session should have one owner"),
    )
    .await;
    session
        .record_conversation_items(
            turn_context.as_ref(),
            &[
                user_message("turn one"),
                assistant_message("turn one answer"),
                user_message("turn two"),
                assistant_message("turn two answer"),
            ],
        )
        .await;
    session
        .flush_rollout()
        .await
        .expect("flush rollback history");
    install_nondefault_checkpoint_state(&session, &turn_context).await;
    session.rotate_current_segment_state_checkpoint().await;
    session
        .flush_rollout()
        .await
        .expect("flush pre-failure checkpoint");
    let state_before_rollback = session.state.lock().await.checkpoint_mutation_snapshot();

    let failure =
        super::super::checkpoint_rotation_test_support::CheckpointPersistenceFailure::install(
            session.thread_id,
        );
    handlers::thread_rollback(
        &session,
        "failed-rollback-recovery".to_string(),
        /*num_turns*/ 1,
    )
    .await;
    assert!(
        session
            .state
            .lock()
            .await
            .has_same_checkpoint_mutation_state(&state_before_rollback)
    );
    drop(failure);

    session
        .record_conversation_items(
            turn_context.as_ref(),
            &[user_message("continued after failed rollback")],
        )
        .await;
    session
        .flush_rollout()
        .await
        .expect("flush continuation after rollback storage recovery");
    assert_eq!(
        load_and_resume_current_state(&session).await,
        persisted_checkpoint_state(&session).await
    );
}

#[tokio::test]
async fn indeterminate_compaction_requires_restart_and_refuses_later_persistence() {
    let (mut session, turn_context, events) = make_session_and_context_with_rx().await;
    let store = attach_in_memory_thread_store(
        Arc::get_mut(&mut session).expect("session should have one owner"),
    )
    .await;
    session
        .record_conversation_items(
            turn_context.as_ref(),
            &[user_message("history before indeterminate compaction")],
        )
        .await;
    session
        .flush_rollout()
        .await
        .expect("flush initial history");
    while session.mcp_refresh.claim() {}
    let (submission_sender, submission_receiver) = async_channel::bounded(4);
    let submission_loop = {
        let session = Arc::clone(&session);
        let config = session.get_config().await;
        tokio::spawn(async move {
            handlers::submission_loop(session, config, submission_receiver).await;
        })
    };
    let calls_before = store.calls().await;
    let indeterminate =
        super::super::checkpoint_rotation_test_support::CheckpointPersistenceIndeterminate::install(
            session.thread_id,
        );
    let pause = super::super::checkpoint_rotation_test_support::CheckpointCapturePause::install(
        session.thread_id,
    );
    let service_tier_before = session.thread_config_snapshot().await.service_tier;
    let runtime_config_before = session.get_config().await;

    let prepared_window_advance = session.prepare_auto_compact_window_advance().await;
    let replacement = {
        let session = Arc::clone(&session);
        let turn_context = Arc::clone(&turn_context);
        tokio::spawn(async move {
            session
                .replace_compacted_history(
                    &turn_context,
                    vec![user_message("indeterminate replacement")],
                    /*reference_context_item*/ None,
                    /*world_state_baseline*/ None,
                    CompactedHistoryMetadata {
                        message: "indeterminate compaction".to_string(),
                        prepared_window_advance,
                    },
                )
                .await
        })
    };
    pause.wait_until_reached().await;
    let queued_user_input = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            handlers::user_input_or_turn_inner(
                &session,
                "queued-user-input".to_string(),
                Op::UserInput {
                    items: vec![UserInput::Text {
                        text: "must not start after indeterminate persistence".to_string(),
                        text_elements: Vec::new(),
                    }],
                    final_output_json_schema: None,
                    responsesapi_client_metadata: None,
                    additional_context: Default::default(),
                    thread_settings: Default::default(),
                },
                /*client_user_message_id*/ None,
                /*parent_turn_id*/ None,
            )
            .await
        })
    };
    let queued_compaction = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            handlers::compact(&session, "queued-compact".to_string()).await;
        })
    };
    let settings_update = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .update_settings(SessionSettingsUpdate {
                    service_tier: Some(Some("must-not-apply".to_string())),
                    ..Default::default()
                })
                .await
        })
    };
    let runtime_config_refresh = {
        let session = Arc::clone(&session);
        let mut next_config = runtime_config_before.as_ref().clone();
        next_config.mcp_oauth_credentials_store_mode =
            match runtime_config_before.mcp_oauth_credentials_store_mode {
                codex_config::types::OAuthCredentialsStoreMode::File => {
                    codex_config::types::OAuthCredentialsStoreMode::Keyring
                }
                codex_config::types::OAuthCredentialsStoreMode::Auto
                | codex_config::types::OAuthCredentialsStoreMode::Keyring => {
                    codex_config::types::OAuthCredentialsStoreMode::File
                }
            };
        tokio::spawn(async move {
            session.refresh_runtime_config(next_config).await;
        })
    };
    submission_sender
        .send(Submission {
            id: "queued-mcp-refresh".to_string(),
            op: Op::RefreshMcpServers,
            client_user_message_id: None,
            trace: None,
            parent_turn_id: None,
        })
        .await
        .expect("queue MCP refresh behind checkpoint admission");
    let admitted_communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        "must not deliver admitted mail after indeterminate persistence".to_string(),
        /*trigger_turn*/ false,
    );
    let admitted_acknowledgement = session.register_checkpoint_submission_acknowledgement(
        "queued-admitted-communication".to_string(),
    );
    submission_sender
        .send(Submission {
            id: "queued-admitted-communication".to_string(),
            op: Op::InterAgentCommunication {
                communication: admitted_communication,
            },
            client_user_message_id: None,
            trace: None,
            parent_turn_id: None,
        })
        .await
        .expect("queue admitted communication behind checkpoint admission");
    let admitted_submission = tokio::spawn(async move {
        admitted_acknowledgement
            .await
            .expect("submission loop should answer admitted communication")
    });
    let memory_mode_update = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .set_thread_memory_mode(ThreadMemoryMode::Disabled)
                .await
        })
    };
    let metadata_update = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .update_thread_metadata(
                    ThreadMetadataPatch {
                        name: Some(Some("must-not-apply".to_string())),
                        ..Default::default()
                    },
                    /*include_archived*/ false,
                )
                .await
        })
    };
    let rollout_append = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .append_rollout_items(&[RolloutItem::ResponseItem(user_message(
                    "must not append after indeterminate persistence",
                ))])
                .await
        })
    };
    let active_turn = ActiveTurn::default();
    let active_turn_state = Arc::clone(&active_turn.turn_state);
    *session.active_turn.lock().await = Some(active_turn);
    let queued_active_injection = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .inject_no_new_turn(
                    vec![user_message("must not inject into an active fenced turn")],
                    /*current_turn_context*/ None,
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!queued_user_input.is_finished());
    assert!(!queued_compaction.is_finished());
    assert!(!settings_update.is_finished());
    assert!(!runtime_config_refresh.is_finished());
    assert!(!session.mcp_refresh.is_pending());
    assert!(!memory_mode_update.is_finished());
    assert!(!metadata_update.is_finished());
    assert!(!rollout_append.is_finished());
    assert!(!queued_active_injection.is_finished());
    assert!(!admitted_submission.is_finished());
    pause.release();

    let result = replacement.await.expect("checkpoint owner task");
    assert!(
        result.is_err(),
        "indeterminate persistence must stop the turn"
    );
    queued_user_input
        .await
        .expect("queued user input task")
        .expect_err("queued user input must not cross an indeterminate checkpoint fence");
    queued_compaction.await.expect("queued compaction task");
    let settings_error = settings_update
        .await
        .expect("settings task")
        .expect_err("queued settings must not cross an indeterminate checkpoint fence");
    runtime_config_refresh
        .await
        .expect("runtime config refresh task");
    let rejected_refresh = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = events.recv().await.expect("session event channel");
            if event.id == "queued-mcp-refresh" {
                break event;
            }
        }
    })
    .await
    .expect("checkpoint-rejected MCP refresh event");
    assert!(matches!(rejected_refresh.msg, EventMsg::Error(_)));
    assert!(!session.mcp_refresh.is_pending());
    admitted_submission
        .await
        .expect("admitted communication acknowledgement task")
        .expect_err("admitted communication must be rejected by the restart fence");
    assert!(
        !session.input_queue.has_pending_mailbox_items().await,
        "a rejected admitted communication must not mutate the target mailbox"
    );
    memory_mode_update
        .await
        .expect("memory-mode task")
        .expect_err("queued memory-mode update must not cross the checkpoint fence");
    metadata_update
        .await
        .expect("metadata task")
        .expect_err("queued metadata update must not cross the checkpoint fence");
    rollout_append
        .await
        .expect("rollout append task")
        .expect_err("queued rollout append must not cross the checkpoint fence");
    queued_active_injection
        .await
        .expect("active injection task")
        .expect_err("active injection must not cross an indeterminate checkpoint fence");
    assert!(
        session
            .input_queue
            .take_pending_input_for_turn_state(active_turn_state.as_ref())
            .await
            .is_empty()
    );
    *session.active_turn.lock().await = None;
    assert!(
        settings_error
            .to_string()
            .contains("thread_settings_restart_required")
    );
    drop(indeterminate);
    assert!(session.persistence_restart_required());
    assert_eq!(
        session.thread_config_snapshot().await.service_tier,
        service_tier_before
    );
    assert_eq!(
        session.get_config().await.mcp_oauth_credentials_store_mode,
        runtime_config_before.mcp_oauth_credentials_store_mode
    );
    assert!(session.active_turn.lock().await.is_none());
    assert!(
        !session
            .clone_history()
            .await
            .raw_items()
            .contains(&user_message(
                "must not start after indeterminate persistence"
            ))
    );
    let history_at_fence = session.clone_history().await.raw_items().to_vec();
    session
        .inject_no_new_turn(
            vec![user_message(
                "must not inject after indeterminate persistence",
            )],
            /*current_turn_context*/ None,
        )
        .await
        .expect_err("no-turn injection must obey the restart fence");
    assert_eq!(
        session.clone_history().await.raw_items(),
        history_at_fence.as_slice()
    );
    session
        .record_inter_agent_communication(
            turn_context.as_ref(),
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                "must not record child mail after indeterminate persistence".to_string(),
                /*trigger_turn*/ false,
            ),
        )
        .await;
    assert_eq!(
        session.clone_history().await.raw_items(),
        history_at_fence.as_slice()
    );
    handlers::inter_agent_communication(
        &session,
        "fenced-mailbox".to_string(),
        InterAgentCommunication::new(
            AgentPath::root(),
            AgentPath::root(),
            Vec::new(),
            "must not wake after indeterminate persistence".to_string(),
            /*trigger_turn*/ true,
        ),
        /*parent_turn_id*/ None,
    )
    .await;
    assert!(
        !session.input_queue.has_pending_mailbox_items().await,
        "mailbox mutations must obey the restart fence"
    );
    session
        .maybe_start_turn_for_pending_work_with_sub_id("queued-mailbox-wake".to_string())
        .await;
    assert!(session.active_turn.lock().await.is_none());

    session
        .persist_rollout_items(&[RolloutItem::ResponseItem(user_message(
            "must not persist after indeterminate checkpoint",
        ))])
        .await;
    assert!(
        session
            .live_thread_for_mutation("append rollout items")
            .is_err(),
        "direct thread mutations must use the restart fence"
    );
    let mut expected_calls = calls_before;
    expected_calls.discard_thread += 1;
    assert_eq!(store.calls().await, expected_calls);
    submission_sender
        .send(Submission {
            id: "checkpoint-test-shutdown".to_string(),
            op: Op::Shutdown,
            client_user_message_id: None,
            trace: None,
            parent_turn_id: None,
        })
        .await
        .expect("stop test submission loop");
    submission_loop.await.expect("test submission loop");
}

#[tokio::test]
async fn elicitation_response_bypasses_checkpoint_admission_held_by_its_request_owner() {
    let (session, _turn_context, _events) = make_session_and_context_with_rx().await;
    let checkpoint_admission = Arc::clone(&session.checkpoint_admission_lock)
        .lock_owned()
        .await;
    let (submission_sender, submission_receiver) = async_channel::bounded(2);
    let ordinary_observer = submission_receiver.clone();
    let (control_sender, control_receiver) = async_channel::bounded(2);
    let submission_loop = {
        let session = Arc::clone(&session);
        let config = session.get_config().await;
        tokio::spawn(async move {
            handlers::submission_loop_with_control(
                session,
                config,
                submission_receiver,
                control_receiver,
            )
            .await;
        })
    };
    submission_sender
        .send(Submission {
            id: "ordinary-head-under-checkpoint-admission".to_string(),
            op: Op::Interrupt,
            client_user_message_id: None,
            trace: None,
            parent_turn_id: None,
        })
        .await
        .expect("queue ordinary submission ahead of elicitation response");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !ordinary_observer.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("submission loop must receive the ordinary head");
    control_sender
        .send(Submission {
            id: "resolve-elicitation-under-checkpoint-admission".to_string(),
            op: Op::ResolveElicitation {
                server_name: "test-server".to_string(),
                request_id: codex_protocol::mcp::RequestId::String("request-1".to_string()),
                decision: codex_protocol::approvals::ElicitationAction::Decline,
                content: None,
                meta: None,
            },
            client_user_message_id: None,
            trace: None,
            parent_turn_id: None,
        })
        .await
        .expect("queue elicitation response");
    control_sender
        .send(Submission {
            id: "shutdown-after-elicitation-response".to_string(),
            op: Op::Shutdown,
            client_user_message_id: None,
            trace: None,
            parent_turn_id: None,
        })
        .await
        .expect("queue shutdown after elicitation response");

    tokio::time::timeout(std::time::Duration::from_secs(5), submission_loop)
        .await
        .expect("elicitation response must not wait for checkpoint admission")
        .expect("submission loop task");
    drop(checkpoint_admission);
}

#[tokio::test]
async fn superseded_task_start_preserves_the_replacement_active_turn() {
    struct BlockingTurnStart {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl codex_extension_api::TurnLifecycleContributor for BlockingTurnStart {
        fn on_turn_start<'a>(
            &'a self,
            _input: codex_extension_api::TurnStartInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
            })
        }
    }

    let (mut session, _turn_context) = make_session_and_context().await;
    let blocker = Arc::new(BlockingTurnStart {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_lifecycle_contributor(blocker.clone());
    session.services.extensions = Arc::new(extensions.build());
    let session = Arc::new(session);
    let reserved_turn = ActiveTurn::default();
    let reserved_turn_state = Arc::clone(&reserved_turn.turn_state);
    *session.active_turn.lock().await = Some(reserved_turn);
    let turn_context = session
        .new_default_turn_with_sub_id("superseded-task-start".to_string())
        .await
        .expect("turn context");
    let starting = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .start_task(
                    turn_context,
                    Vec::new(),
                    crate::tasks::RegularTask::new(),
                    /*input_persisted*/ None,
                    crate::tasks::MailboxParentProvenance::Ignore,
                )
                .await
        })
    };
    blocker.entered.notified().await;
    let replacement_turn = ActiveTurn::default();
    let replacement_turn_state = Arc::clone(&replacement_turn.turn_state);
    *session.active_turn.lock().await = Some(replacement_turn);
    blocker.release.notify_one();

    assert_eq!(
        starting.await.expect("task start task"),
        crate::tasks::TaskStartOutcome::Superseded
    );
    let active_turn = session.active_turn.lock().await;
    let active_turn = active_turn.as_ref().expect("replacement active turn");
    assert!(active_turn.task.is_none());
    assert!(Arc::ptr_eq(
        &active_turn.turn_state,
        &replacement_turn_state
    ));
    assert!(!Arc::ptr_eq(&active_turn.turn_state, &reserved_turn_state));
}

#[tokio::test]
async fn rollback_orders_direct_injection_after_its_checkpoint() {
    let (mut session, turn_context, _events) = make_session_and_context_with_rx().await;
    attach_in_memory_thread_store(
        Arc::get_mut(&mut session).expect("session should have one owner"),
    )
    .await;
    let turn_context_item = turn_context.to_turn_context_item();
    let response_history = vec![
        user_message("turn 1 user"),
        assistant_message("turn 1 assistant"),
        user_message("turn 2 user"),
        assistant_message("turn 2 assistant"),
    ];
    session
        .replace_history(
            response_history.clone(),
            /*reference_context_item*/ None,
        )
        .await;
    assert!(session.reference_context_item().await.is_none());
    let persisted_turn = |turn_id: &str, user: &str, assistant: &str| {
        vec![
            RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: turn_id.to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            })),
            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                client_id: None,
                message: user.to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            })),
            RolloutItem::TurnContext(turn_context_item.clone()),
            RolloutItem::ResponseItem(user_message(user)),
            RolloutItem::ResponseItem(assistant_message(assistant)),
            RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn_id.to_string(),
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            })),
        ]
    };
    let persisted_history = persisted_turn("turn-1", "turn 1 user", "turn 1 assistant")
        .into_iter()
        .chain(persisted_turn("turn-2", "turn 2 user", "turn 2 assistant"))
        .collect::<Vec<_>>();
    session.persist_rollout_items(&persisted_history).await;
    session.flush_rollout().await.expect("flush initial turns");

    let pause = super::super::checkpoint_rotation_test_support::CheckpointCapturePause::install(
        session.thread_id,
    );
    let rollback = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            handlers::thread_rollback(
                &session,
                "concurrent-rollback".to_string(),
                /*num_turns*/ 1,
            )
            .await;
        })
    };
    pause.wait_until_reached().await;
    let injected_item = user_message("direct injection after rollback");
    let injection = {
        let session = Arc::clone(&session);
        let injected_item = injected_item.clone();
        tokio::spawn(async move { session.inject_response_items(vec![injected_item]).await })
    };
    tokio::task::yield_now().await;
    assert!(!injection.is_finished());
    pause.release();
    rollback.await.expect("rollback task");
    injection
        .await
        .expect("direct injection task")
        .expect("direct injection after rollback");
    session
        .flush_rollout()
        .await
        .expect("flush direct injection");

    let live_history = strip_response_item_ids(&strip_metadata_from_items(
        session.clone_history().await.raw_items(),
    ));
    assert!(live_history.contains(&user_message("turn 1 user")));
    assert!(live_history.contains(&assistant_message("turn 1 assistant")));
    assert!(!live_history.contains(&user_message("turn 2 user")));
    assert!(!live_history.contains(&assistant_message("turn 2 assistant")));
    assert_eq!(live_history.last(), Some(&injected_item));
    assert_eq!(
        load_and_resume_current_state(&session).await,
        persisted_checkpoint_state(&session).await
    );
}

#[tokio::test]
async fn panicking_checkpoint_owner_fences_queued_settings_before_releasing_state() {
    let (mut session, turn_context, _events) = make_session_and_context_with_rx().await;
    let _store = attach_in_memory_thread_store(
        Arc::get_mut(&mut session).expect("session should have one owner"),
    )
    .await;
    let panic_guard = super::super::checkpoint_rotation_test_support::CheckpointOwnerPanic::install(
        session.thread_id,
    );
    let pause = super::super::checkpoint_rotation_test_support::CheckpointCapturePause::install(
        session.thread_id,
    );
    let service_tier_before = session.thread_config_snapshot().await.service_tier;
    let prepared_window_advance = session.prepare_auto_compact_window_advance().await;
    let replacement = {
        let session = Arc::clone(&session);
        let turn_context = Arc::clone(&turn_context);
        tokio::spawn(async move {
            session
                .replace_compacted_history(
                    &turn_context,
                    vec![user_message("replacement owned by panicking checkpoint")],
                    /*reference_context_item*/ None,
                    /*world_state_baseline*/ None,
                    CompactedHistoryMetadata {
                        message: "panicking checkpoint owner".to_string(),
                        prepared_window_advance,
                    },
                )
                .await
        })
    };
    pause.wait_until_reached().await;
    let settings_update = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .update_settings(SessionSettingsUpdate {
                    service_tier: Some(Some("must-not-cross-panic".to_string())),
                    ..Default::default()
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!settings_update.is_finished());
    pause.release();

    assert!(
        replacement.await.expect("replacement task").is_err(),
        "checkpoint owner panic must stop compaction"
    );
    let settings_error = settings_update
        .await
        .expect("settings task")
        .expect_err("queued settings must not cross a checkpoint-owner panic");
    assert!(
        settings_error
            .to_string()
            .contains("thread_settings_restart_required")
    );
    drop(panic_guard);
    assert!(session.persistence_restart_required());
    assert_eq!(
        session.thread_config_snapshot().await.service_tier,
        service_tier_before
    );
}
