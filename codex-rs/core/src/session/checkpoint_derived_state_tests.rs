use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn record_initial_history_seeds_token_info_and_rate_limits_from_rollout() {
    let (session, turn_context) = make_session_and_context().await;
    let (mut rollout_items, _expected) = sample_rollout(&session, &turn_context).await;

    let info1 = TokenUsageInfo {
        total_token_usage: TokenUsage {
            input_tokens: 10,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 20,
            reasoning_output_tokens: 0,
            total_tokens: 30,
            codex_rollout_budget_units: None,
        },
        last_token_usage: TokenUsage {
            input_tokens: 3,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 4,
            reasoning_output_tokens: 0,
            total_tokens: 7,
            codex_rollout_budget_units: None,
        },
        model_context_window: Some(1_000),
    };
    let info2 = TokenUsageInfo {
        total_token_usage: TokenUsage {
            input_tokens: 100,
            cached_input_tokens: 50,
            cache_write_input_tokens: 0,
            output_tokens: 200,
            reasoning_output_tokens: 25,
            total_tokens: 375,
            codex_rollout_budget_units: None,
        },
        last_token_usage: TokenUsage {
            input_tokens: 10,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 20,
            reasoning_output_tokens: 5,
            total_tokens: 35,
            codex_rollout_budget_units: None,
        },
        model_context_window: Some(2_000),
    };

    rollout_items.push(RolloutItem::EventMsg(EventMsg::TokenCount(
        TokenCountEvent {
            info: Some(info1),
            rate_limits: None,
        },
    )));
    rollout_items.push(RolloutItem::EventMsg(EventMsg::TokenCount(
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )));
    rollout_items.push(RolloutItem::EventMsg(EventMsg::TokenCount(
        TokenCountEvent {
            info: Some(info2.clone()),
            rate_limits: None,
        },
    )));
    let rate_limits = RateLimitSnapshot {
        limit_id: Some("codex".to_string()),
        limit_name: Some("Codex".to_string()),
        primary: Some(RateLimitWindow {
            used_percent: 42.0,
            window_minutes: Some(300),
            resets_at: Some(1_700),
        }),
        secondary: None,
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    };
    rollout_items.push(RolloutItem::EventMsg(EventMsg::TokenCount(
        TokenCountEvent {
            info: None,
            rate_limits: Some(rate_limits.clone()),
        },
    )));
    rollout_items.push(RolloutItem::EventMsg(EventMsg::TokenCount(
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )));

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    let actual = session.state.lock().await.token_info_and_rate_limits();
    assert_eq!(actual, (Some(info2), Some(rate_limits)));
}

#[tokio::test]
async fn cleared_compaction_checkpoint_persists_effective_settings_and_token_state() {
    let (mut session, turn_context, _) = make_session_and_context_with_rx().await;
    let stable_path = {
        let session = Arc::get_mut(&mut session).expect("session should have one owner");
        attach_thread_persistence(session).await
    };
    let expected_rate_limits = RateLimitSnapshot {
        limit_id: Some("codex".to_string()),
        limit_name: Some("Codex".to_string()),
        primary: Some(RateLimitWindow {
            used_percent: 35.0,
            window_minutes: Some(300),
            resets_at: Some(1_700),
        }),
        secondary: None,
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    };
    {
        let mut state = session.state.lock().await;
        state.session_configuration.approvals_reviewer =
            codex_protocol::config_types::ApprovalsReviewer::AutoReview;
        state
            .session_configuration
            .environments
            .environments
            .clear();
        state.set_rate_limits(expected_rate_limits.clone());
    }
    let expected_settings = session
        .thread_config_snapshot()
        .await
        .into_thread_settings_snapshot();
    assert_eq!(expected_settings.workspace_roots, Some(Vec::new()));
    let prepared_window_advance = session.prepare_auto_compact_window_advance().await;
    session
        .replace_compacted_history(
            &turn_context,
            vec![user_message("cleared checkpoint history")],
            /*reference_context_item*/ None,
            /*world_state_baseline*/ None,
            CompactedHistoryMetadata {
                message: "manual compact".to_string(),
                prepared_window_advance,
            },
        )
        .await
        .expect("cleared compaction checkpoint should persist");

    let (items, _, parse_errors) = RolloutRecorder::load_rollout_items(stable_path.as_path())
        .await
        .expect("load cleared compaction checkpoint");
    assert_eq!(parse_errors, 0);
    let checkpoint_start = items
        .iter()
        .position(|item| matches!(item, RolloutItem::Compacted(_)))
        .expect("checkpoint compaction");
    let [
        RolloutItem::Compacted(compacted),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(settings)),
        RolloutItem::EventMsg(EventMsg::TokenCount(token_count)),
    ] = &items[checkpoint_start..]
    else {
        panic!("cleared checkpoint should be a complete three-record unit");
    };
    let descriptor = compacted
        .segment_state_checkpoint
        .as_ref()
        .expect("checkpoint descriptor");
    assert_eq!(
        (
            descriptor.world_state,
            descriptor.reference_context,
            settings.thread_settings.clone(),
            token_count.rate_limits.clone(),
        ),
        (
            codex_protocol::protocol::SegmentStateCheckpointDisposition::Cleared,
            codex_protocol::protocol::SegmentStateCheckpointDisposition::Cleared,
            expected_settings,
            Some(expected_rate_limits),
        )
    );
    assert!(token_count.info.is_some());
}

#[tokio::test]
async fn cancelled_caller_keeps_checkpoint_rotation_and_settings_ordered() {
    let (mut session, turn_context, rx) = make_session_and_context_with_rx().await;
    while session.take_pending_session_start_source().await.is_some() {}
    let stable_path = {
        let session = Arc::get_mut(&mut session).expect("session should have one owner");
        let stable_path = attach_thread_persistence(session).await;
        session
            .persist_rollout_items(&[RolloutItem::ResponseItem(user_message(
                "history before settings race",
            ))])
            .await;
        session
            .flush_rollout()
            .await
            .expect("flush pre-checkpoint history");
        stable_path
    };
    let pause = super::super::checkpoint_rotation_test_support::CheckpointCapturePause::install(
        session.thread_id,
    );
    let compact_session = Arc::clone(&session);
    let compact_turn_context = Arc::clone(&turn_context);
    let compaction = tokio::spawn(async move {
        let prepared_window_advance = compact_session.prepare_auto_compact_window_advance().await;
        compact_session
            .replace_compacted_history(
                &compact_turn_context,
                vec![user_message("checkpoint replacement history")],
                /*reference_context_item*/ None,
                /*world_state_baseline*/ None,
                CompactedHistoryMetadata {
                    message: "settings race checkpoint".to_string(),
                    prepared_window_advance,
                },
            )
            .await
            .expect("cancelled caller checkpoint should persist in detached owner");
    });
    pause.wait_until_reached().await;
    compaction.abort();
    assert!(
        compaction
            .await
            .expect_err("compaction caller should be cancelled")
            .is_cancelled()
    );

    let update_session = Arc::clone(&session);
    let mut settings_update = tokio::spawn(async move {
        handlers::update_thread_settings(
            &update_session,
            "settings-after-checkpoint-capture".to_string(),
            ThreadSettingsOverrides {
                approvals_reviewer: Some(
                    codex_protocol::config_types::ApprovalsReviewer::AutoReview,
                ),
                service_tier: Some(Some("flex".to_string())),
                ..Default::default()
            },
        )
        .await;
    });
    assert!(
        tokio::time::timeout(StdDuration::from_millis(50), &mut settings_update)
            .await
            .is_err(),
        "settings update must wait while checkpoint capture owns Session.state"
    );

    pause.release();
    settings_update.await.expect("settings update task");
    session
        .flush_rollout()
        .await
        .expect("flush first post-checkpoint settings");
    let (first_active_items, _, first_parse_errors) =
        RolloutRecorder::load_rollout_items(&stable_path)
            .await
            .expect("load active segment after first rotation");
    assert_eq!(first_parse_errors, 0);
    let first_rotation_predecessor = first_active_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::RolloutReference(reference) => Some(reference.rollout_path.clone()),
            _ => None,
        })
        .expect("first checkpoint should reference its predecessor");

    let rollback_pause =
        super::super::checkpoint_rotation_test_support::CheckpointCapturePause::install(
            session.thread_id,
        );
    let rollback_session = Arc::clone(&session);
    let rollback_checkpoint = tokio::spawn(async move {
        rollback_session
            .rotate_current_segment_state_checkpoint()
            .await;
    });
    rollback_pause.wait_until_reached().await;
    rollback_checkpoint.abort();
    assert!(
        rollback_checkpoint
            .await
            .expect_err("rollback checkpoint caller should be cancelled")
            .is_cancelled()
    );
    let rollback_update_session = Arc::clone(&session);
    let mut rollback_settings_update = tokio::spawn(async move {
        handlers::update_thread_settings(
            &rollback_update_session,
            "settings-after-rollback-checkpoint-capture".to_string(),
            ThreadSettingsOverrides {
                approvals_reviewer: Some(codex_protocol::config_types::ApprovalsReviewer::User),
                service_tier: Some(Some("priority".to_string())),
                ..Default::default()
            },
        )
        .await;
    });
    assert!(
        tokio::time::timeout(StdDuration::from_millis(50), &mut rollback_settings_update,)
            .await
            .is_err(),
        "settings update must wait while rollback checkpoint owns Session.state"
    );
    rollback_pause.release();
    rollback_settings_update
        .await
        .expect("rollback settings update task");
    session
        .flush_rollout()
        .await
        .expect("flush post-checkpoint settings");
    let expected_settings = session
        .thread_config_snapshot()
        .await
        .into_thread_settings_snapshot();

    let (active_items, _, parse_errors) = RolloutRecorder::load_rollout_items(&stable_path)
        .await
        .expect("load active checkpoint segment");
    assert_eq!(parse_errors, 0);
    let predecessor_path = active_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::RolloutReference(reference) => Some(reference.rollout_path.clone()),
            _ => None,
        })
        .expect("checkpoint should reference predecessor");
    assert_ne!(
        predecessor_path, first_rotation_predecessor,
        "cancelled rollback caller must not cancel the detached second rotation"
    );
    let missing_predecessor = predecessor_path.with_extension("jsonl.missing");
    std::fs::rename(&predecessor_path, &missing_predecessor).expect("make predecessor unavailable");

    let latest_context = codex_thread_store::ThreadStore::load_latest_model_context(
        session.services.thread_store.as_ref(),
        codex_thread_store::LoadThreadHistoryParams {
            thread_id: session.thread_id,
            rollout_path: None,
            include_archived: false,
        },
    )
    .await
    .expect("load latest context without predecessor");
    let latest_settings = latest_context
        .items
        .iter()
        .rev()
        .find_map(|item| match item {
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) => {
                Some(event.thread_settings.clone())
            }
            _ => None,
        });
    assert_eq!(latest_settings, Some(expected_settings));
    let mut token_count_events = 0;
    while let Ok(event) = rx.try_recv() {
        if matches!(event.msg, EventMsg::TokenCount(_)) {
            token_count_events += 1;
        }
    }
    assert_eq!(token_count_events, 1);
    assert!(matches!(
        session.take_pending_session_start_source().await,
        Some(codex_hooks::SessionStartSource::Compact)
    ));
    assert!(session.take_pending_session_start_source().await.is_none());
}

#[tokio::test]
async fn precommit_rotation_failure_appends_checkpoint_to_active_rollout() {
    let (mut session, turn_context, _) = make_session_and_context_with_rx().await;
    let stable_path = {
        let session = Arc::get_mut(&mut session).expect("session should have one owner");
        let stable_path = attach_thread_persistence(session).await;
        session
            .persist_rollout_items(&[RolloutItem::ResponseItem(user_message(
                "history before failed rotation",
            ))])
            .await;
        session
            .flush_rollout()
            .await
            .expect("flush pre-checkpoint history");
        stable_path
    };
    let codex_home = session.get_config().await.codex_home.clone();
    let immutable_thread_path = codex_home
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(session.thread_id.to_string());
    std::fs::create_dir_all(
        immutable_thread_path
            .parent()
            .expect("immutable thread path parent"),
    )
    .expect("create immutable segment root");
    std::fs::write(&immutable_thread_path, b"block immutable segment directory")
        .expect("install deterministic rotation blocker");
    let pause = super::super::checkpoint_rotation_test_support::CheckpointCapturePause::install(
        session.thread_id,
    );
    let compact_session = Arc::clone(&session);
    let compact_turn_context = Arc::clone(&turn_context);
    let compaction = tokio::spawn(async move {
        let prepared_window_advance = compact_session.prepare_auto_compact_window_advance().await;
        compact_session
            .replace_compacted_history(
                &compact_turn_context,
                vec![user_message("checkpoint after failed rotation")],
                /*reference_context_item*/ None,
                /*world_state_baseline*/ None,
                CompactedHistoryMetadata {
                    message: "failed rotation checkpoint".to_string(),
                    prepared_window_advance,
                },
            )
            .await
            .expect("failed rotation should append checkpoint to active rollout");
    });
    pause.wait_until_reached().await;
    compaction.abort();
    assert!(
        compaction
            .await
            .expect_err("compaction caller should be cancelled")
            .is_cancelled()
    );
    pause.release();
    // The detached persistence task owns Session.state until rotation or fallback completes.
    // Acquiring the state lock therefore waits for the fallback append after caller cancellation.
    drop(session.state.lock().await);
    session
        .flush_rollout()
        .await
        .expect("flush fallback checkpoint append");

    let (active_items, _, parse_errors) = RolloutRecorder::load_rollout_items(&stable_path)
        .await
        .expect("load active rollout after failed rotation");
    assert_eq!(parse_errors, 0);
    assert!(
        !active_items
            .iter()
            .any(|item| matches!(item, RolloutItem::RolloutReference(_))),
        "precommit rotation failure must leave the original active rollout in place"
    );
    let checkpoint_start = active_items
        .iter()
        .rposition(|item| matches!(item, RolloutItem::Compacted(_)))
        .expect("fallback compacted checkpoint");
    let RolloutItem::Compacted(compacted) = &active_items[checkpoint_start] else {
        unreachable!("checkpoint_start must identify a Compacted item");
    };
    assert!(
        codex_rollout::validated_segment_state_checkpoint(
            compacted,
            &active_items[checkpoint_start + 1..],
        )
        .is_some()
    );
}

#[tokio::test]
async fn checkpoint_persistence_failure_skips_committed_post_effects() {
    let (mut session, turn_context, rx) = make_session_and_context_with_rx().await;
    while session.take_pending_session_start_source().await.is_some() {}
    let store = attach_in_memory_thread_store(
        Arc::get_mut(&mut session).expect("session should have one owner"),
    )
    .await;
    let source_item = RolloutItem::ResponseItem(user_message("history before store failure"));
    session.persist_rollout_items(&[source_item]).await;
    session.flush_rollout().await.expect("flush source history");
    let state_before_compaction = session.state.lock().await.checkpoint_mutation_snapshot();
    store.fail_appends().await;
    let prepared_window_advance = session.prepare_auto_compact_window_advance().await;
    session
        .replace_compacted_history(
            &turn_context,
            vec![user_message("in-memory replacement after store failure")],
            /*reference_context_item*/ None,
            /*world_state_baseline*/ None,
            CompactedHistoryMetadata {
                message: "failed checkpoint persistence".to_string(),
                prepared_window_advance,
            },
        )
        .await
        .expect("failed checkpoint persistence should restore live state");

    let events = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| {
        matches!(
            &event.msg,
            EventMsg::Warning(WarningEvent { message })
                if message.contains("Failed to persist the compacted current-state checkpoint")
        )
    }));
    assert!(
        !events
            .iter()
            .any(|event| matches!(&event.msg, EventMsg::TokenCount(_)))
    );
    assert!(session.take_pending_session_start_source().await.is_none());
    assert!(
        session
            .state
            .lock()
            .await
            .has_same_checkpoint_mutation_state(&state_before_compaction),
        "failed compaction persistence must restore the exact pre-compaction model state"
    );
}
