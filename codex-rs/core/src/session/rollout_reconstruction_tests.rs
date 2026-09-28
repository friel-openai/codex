use super::*;

use super::tests::build_world_state_from_turn_context;
use super::tests::make_session_and_context;
use super::tests::raw_history_items;
use crate::context::CompactionSummary;
use crate::context::ContextualUserFragment;
use crate::context::GuardianContextMode;
use codex_history::CompactedItem;
use codex_history::InitialHistory;
use codex_history::ResponseItemEnvelope;
use codex_history::ResumedHistory;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::SessionContextWindow;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::WorldStateItem;
use codex_protocol::security_risk::SecurityRiskScore;
use codex_rollout::CertifiedSegmentStateCheckpoint;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
use core_test_support::responses::strip_metadata_from_items;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use test_case::test_case;
use uuid::Uuid;

#[tokio::test]
async fn recorded_questions_share_queued_input_order_across_resume() {
    let (session, turn) = make_session_and_context().await;
    let question = |call_id: &str| {
        serde_json::from_value::<ResponseItem>(json!({
            "type": "function_call", "call_id": call_id,
            "namespace": "mcp__codex_apps", "name": "user_messaging_send_message",
            "arguments": "{\"text\":\"Continue?\"}"
        }))
        .unwrap()
    };
    session
        .record_conversation_items(&turn, turn.model_info(), &[question("first")])
        .await;
    let reply_order = session.reserve_user_input_order().await;
    session
        .record_conversation_items(&turn, turn.model_info(), &[question("second")])
        .await;
    // The accepted reply is recorded after a newer question, and the first send's
    // result arrives last. Neither delay should move the first question's position.
    session
        .record_annotated_conversation_items(
            &turn,
            turn.model_info(),
            vec![
                ResponseItemEnvelope {
                    item: user_message("Yes."),
                    metadata: Some(codex_history::CodexHarnessMetadata {
                        user_input_order: Some(reply_order),
                        ..Default::default()
                    }),
                },
                ResponseItemEnvelope {
                    item: serde_json::from_value(json!({
                        "type": "function_call_output", "call_id": "first", "output": "Sent."
                    }))
                    .unwrap(),
                    metadata: Some(codex_history::CodexHarnessMetadata {
                        delivered_assistant_message: Some("Continue?".to_owned()),
                        ..Default::default()
                    }),
                },
            ],
        )
        .await;
    let sources = |items: &[ResponseItemEnvelope]| {
        items
            .iter()
            .map(|item| {
                item.metadata
                    .as_ref()
                    .and_then(|metadata| metadata.retained_source.clone())
            })
            .collect::<Vec<_>>()
    };
    let original_sources = sources(session.clone_history().await.annotated_items());
    assert!(original_sources.iter().any(Option::is_some));
    let saved = session
        .clone_history()
        .await
        .into_annotated_items()
        .into_iter()
        .map(RolloutItem::ResponseItem)
        .collect::<Vec<_>>();
    let saved = serde_json::from_value(serde_json::to_value(saved).unwrap()).unwrap();
    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: session.thread_id,
            history: Arc::new(saved),
            rollout_path: None,
        }))
        .await
        .expect("record initial history");
    let history = session.clone_history().await;
    assert_eq!(sources(history.annotated_items()), original_sources);
    assert_eq!(
        history
            .annotated_items()
            .iter()
            .map(|item| {
                item.metadata
                    .as_ref()
                    .and_then(|metadata| metadata.user_input_order)
            })
            .collect::<Vec<_>>(),
        vec![Some(0), Some(2), Some(1), None]
    );
    assert_eq!(session.reserve_user_input_order().await, 3);
}

#[tokio::test]
async fn sender_context_follows_its_delivery_through_checkpoint_and_rollback() {
    let (session, turn_context) = make_session_and_context().await;

    let mut live = ContextManager::for_session(&SessionSource::default());
    let mut items = Vec::new();
    let mut snapshots = Vec::new();
    for index in 0..2 {
        // A and then B are accepted before either is consumed.
        let acceptance_order = Some(live.reserve_input_order());
        let steer_order = Some(live.reserve_input_order());
        let snapshot = codex_history::SenderUserMessages {
            receiver_turn_id: format!("turn-{index}"),
            receiver_message_id: format!("delivery-{index}"),
            text: format!("Sender context {index}"),
        };
        let mut input = [
            ResponseItemEnvelope {
                item: serde_json::from_value::<ResponseItem>(json!({
                    "type": "function_call_output", "id": snapshot.receiver_message_id,
                    "name": "send_message_to_thread", "namespace": "codex_app", "output": "Inspect."
                }))
                .unwrap(),
                metadata: Some(codex_history::CodexHarnessMetadata {
                    user_input_order: acceptance_order,
                    sender_user_messages: Some(Box::new(snapshot.clone())),
                    ..Default::default()
                }),
            },
            ResponseItemEnvelope {
                item: user_message("Later user steer B."),
                metadata: Some(codex_history::CodexHarnessMetadata {
                    user_input_order: steer_order,
                    ..Default::default()
                }),
            },
        ];
        live.record_annotated_items(
            &mut input,
            turn_context.model_info().truncation_policy.into(),
        );
        items.extend(input.into_iter().map(RolloutItem::ResponseItem));
        snapshots.push(snapshot);
    }
    let mut checkpoint: CompactedItem =
        serde_json::from_value(json!({"message": "compacted"})).unwrap();
    checkpoint.replacement_history = Some(live.annotated_items().to_vec());
    checkpoint.retained_context = Some(live.retained_context().clone());
    let mut checkpointed = items.clone();
    checkpointed.push(RolloutItem::Compacted(checkpoint));
    for (num_turns, expected) in [(1, &snapshots[1]), (2, &snapshots[0])] {
        let mut rolled_back = live.clone();
        rolled_back.drop_last_n_user_turns(num_turns);
        assert_eq!(
            rolled_back.retained_context().sender_user_messages(),
            Some(expected)
        );
        for history in [items.clone(), checkpointed.clone()] {
            let mut history: Vec<RolloutItem> =
                serde_json::from_value(serde_json::to_value(history).unwrap()).unwrap();
            history.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
                ThreadRolledBackEvent { num_turns },
            )));
            let replayed = session
                .reconstruct_history_from_rollout(&turn_context, &history)
                .await;
            assert_eq!(replayed.retained_context, *rolled_back.retained_context());
            assert_eq!(replayed.history, rolled_back.annotated_items());
        }
    }
}

macro_rules! object {
    ($value:tt) => {
        serde_json::from_value(json!($value)).unwrap()
    };
}

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn assistant_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn annotated(items: Vec<ResponseItem>) -> Vec<ResponseItemEnvelope> {
    items.into_iter().map(ResponseItemEnvelope::new).collect()
}

fn retained_user_message(turn_id: &str, text: &str, order: u64) -> ResponseItemEnvelope {
    ResponseItemEnvelope {
        item: ResponseItem::Message {
            id: Some(codex_protocol::ResponseItemId::with_suffix("msg", turn_id)),
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: Some(
                codex_protocol::models::InternalChatMessageMetadataPassthrough {
                    turn_id: Some(turn_id.to_string()),
                    content_item_kinds: Some(vec![codex_protocol::models::ContentItemKind(
                        "user.text".to_string(),
                    )]),
                    ..Default::default()
                },
            ),
        },
        metadata: Some(codex_history::CodexHarnessMetadata {
            user_input_order: Some(order),
            ..Default::default()
        }),
    }
}

fn retained_answer(turn_id: &str, order: u64) -> codex_history::RetainedContextEvent {
    codex_history::RetainedContextEvent::VerifiedAnswer {
        answer: codex_history::VerifiedAnswer {
            turn_id: turn_id.to_string(),
            call_id: format!("ask-{turn_id}"),
            questions: vec![codex_history::VerifiedQuestionAnswer {
                question: format!("Publish {turn_id}?"),
                answer: format!("Only to the private {turn_id} workspace."),
            }],
        },
        acceptance_order: Some(order),
    }
}

fn inter_agent_assistant_message(text: &str) -> ResponseItem {
    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root().join("worker").unwrap(),
        Vec::new(),
        text.to_string(),
        /*trigger_turn*/ true,
    );
    ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: serde_json::to_string(&communication).unwrap(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn completed_user_turn_rollout(
    turn_context_item: TurnContextItem,
    items: Vec<RolloutItem>,
) -> Vec<RolloutItem> {
    let turn_id = turn_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let mut rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(turn_context_item),
    ];
    rollout_items.extend(items);
    rollout_items.push(RolloutItem::EventMsg(EventMsg::TurnComplete(
        codex_protocol::protocol::TurnCompleteEvent {
            turn_id,
            last_agent_message: None,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        },
    )));
    rollout_items
}

fn checkpoint_compacted(history: Vec<ResponseItem>) -> CompactedItem {
    let window_id = Uuid::now_v7();
    CompactedItem {
        message: String::new(),
        replacement_history: Some(annotated(history)),
        retained_context: None,
        retained_context_replay: None,
        guardian_history: None,
        mcp_resource_origins: None,
        compaction_response_id: None,
        latest_token_usage_record: None,
        resume_metadata: None,
        window_number: Some(8),
        first_window_id: Some(window_id.to_string()),
        previous_window_id: None,
        window_id: Some(window_id.to_string()),
        segment_state_checkpoint: None,
    }
}

fn complete_thread_settings() -> ThreadSettingsAppliedEvent {
    ThreadSettingsAppliedEvent {
        thread_id: None,
        thread_settings: ThreadSettingsSnapshot {
            model: "test-model".to_string(),
            model_provider_id: "test-provider".to_string(),
            service_tier: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: codex_protocol::config_types::ApprovalsReviewer::User,
            permission_profile: PermissionProfile::workspace_write(),
            active_permission_profile: None,
            cwd: serde_json::from_value(json!("/tmp")).expect("absolute test cwd"),
            runtime_workspace_roots: Some(Vec::new()),
            environments: Some(
                TurnEnvironmentSelections::new(
                    serde_json::from_value(json!("/tmp")).expect("absolute test cwd"),
                    Vec::new(),
                )
                .into(),
            ),
            workspace_roots: Some(Vec::new()),
            profile_workspace_roots: Some(Vec::new()),
            windows_sandbox_level: Some(
                codex_protocol::config_types::WindowsSandboxLevel::Disabled,
            ),
            reasoning_effort: None,
            reasoning_summary: None,
            personality: None,
            collaboration_mode: codex_protocol::config_types::CollaborationMode {
                mode: ModeKind::Default,
                settings: codex_protocol::config_types::Settings {
                    model: "test-model".to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            },
            disabled_plugin_ids: Vec::new(),
        },
    }
}

#[test_case(false; "legacy certified metadata")]
#[test_case(true; "modern certified metadata")]
#[tokio::test]
async fn reconstruct_history_uses_established_segment_state_checkpoint(modern: bool) {
    let (session, turn_context) = make_session_and_context().await;
    let turn_context = Arc::new(turn_context);
    let replacement_history = vec![assistant_message("checkpoint history")];
    let reference_context = turn_context.to_turn_context_item();
    let world_state = build_world_state_from_turn_context(&session, &turn_context).await;
    let world_state_snapshot = world_state.snapshot();
    let mut compacted = checkpoint_compacted(replacement_history.clone());
    compacted.resume_metadata = modern.then(|| codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: Some("checkpoint-turn".to_string()),
        previous_turn_settings: Some(PreviousTurnSettings {
            model: "previous-model".to_string(),
            comp_hash: Some("previous-hash".to_string()),
            realtime_active: Some(false),
        }),
    });
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted,
        Some(SegmentPreviousTurnSettings {
            model: "previous-model".to_string(),
            comp_hash: Some("previous-hash".to_string()),
            realtime_active: Some(false),
        }),
        Some(WorldStateItem::full(
            world_state_snapshot.clone().into_object(),
        )),
        Some(reference_context.clone()),
        complete_thread_settings(),
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid established checkpoint");

    let reconstructed = session
        .reconstruct_history_from_rollout(turn_context.as_ref(), checkpoint.items())
        .await;

    assert_eq!(reconstructed.history, annotated(replacement_history));
    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(PreviousTurnSettings {
            model: "previous-model".to_string(),
            comp_hash: Some("previous-hash".to_string()),
            realtime_active: Some(false),
        })
    );
    assert_eq!(
        reconstructed.reference_context_item,
        Some(reference_context)
    );
    assert_eq!(
        reconstructed.world_state_baseline,
        Some(world_state_snapshot)
    );
    assert_eq!(reconstructed.window_number, 8);
    assert_eq!(
        reconstructed.last_started_turn_id.as_deref(),
        modern.then_some("checkpoint-turn")
    );
}

#[tokio::test]
async fn completed_suffix_keeps_certified_world_state_before_applying_patch() {
    let (session, turn_context) = make_session_and_context().await;
    let mut compacted = checkpoint_compacted(vec![user_message("checkpoint")]);
    compacted.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: Some("checkpoint-turn".to_string()),
        previous_turn_settings: None,
    });
    let mut reference = turn_context.to_turn_context_item();
    reference.turn_id = Some("checkpoint-turn".to_string());
    let mut bounded_items = CertifiedSegmentStateCheckpoint::new(
        compacted,
        None,
        Some(WorldStateItem::full(object!({
            "environment": {"retained": true, "removed": true}
        }))),
        Some(reference),
        complete_thread_settings(),
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid modern checkpoint")
    .into_items();
    let mut newer = turn_context.to_turn_context_item();
    newer.turn_id = Some("newer-turn".to_string());
    bounded_items.extend(completed_user_turn_rollout(
        newer,
        vec![
            RolloutItem::ResponseItem(user_message("newer task").into()),
            RolloutItem::WorldState(WorldStateItem::patch(object!({
                "environment": {"removed": null, "added": true}
            }))),
        ],
    ));
    let mut full_items = completed_user_turn_rollout(
        turn_context.to_turn_context_item(),
        vec![RolloutItem::WorldState(WorldStateItem::full(object!({
            "obsolete": true
        })))],
    );
    full_items.extend(bounded_items.clone());

    let bounded = session
        .reconstruct_history_from_rollout(&turn_context, &bounded_items)
        .await;
    let full = session
        .reconstruct_history_from_rollout(&turn_context, &full_items)
        .await;
    assert_eq!(bounded, full);
    let expected_world_state = WorldStateItem::full(object!({
        "environment": {"retained": true, "added": true}
    }));
    assert_eq!(
        bounded.world_state_baseline,
        Some(crate::context::world_state::WorldStateSnapshot::from(
            &expected_world_state.state,
        ))
    );
}

#[test_case(false; "matching Guardian boundary")]
#[test_case(true; "missing Guardian boundary clears evidence")]
#[tokio::test]
async fn checkpoint_only_rollback_preserves_model_history_without_removed_authorization(
    missing_guardian_boundary: bool,
) {
    let (session, turn_context) = make_session_and_context().await;
    let retained = vec![user_message("keep"), assistant_message("kept")];
    let mut replacement = retained.clone();
    replacement.extend([user_message("remove"), assistant_message("removed")]);
    let mut guardian = retained.clone();
    if !missing_guardian_boundary {
        guardian.push(user_message("remove"));
    }
    guardian.push(assistant_message("authorization belonging to removed turn"));
    let mut compacted = checkpoint_compacted(replacement);
    compacted.guardian_history = Some(codex_history::GuardianHistoryCheckpoint(guardian));
    let mut items = CertifiedSegmentStateCheckpoint::new(
        compacted,
        None,
        None,
        None,
        complete_thread_settings(),
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid cleared checkpoint")
    .into_items();
    items.insert(
        0,
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                history_mode: ThreadHistoryMode::Paginated,
                ..Default::default()
            },
            git: None,
        }),
    );
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
    )));

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(reconstructed.history, annotated(retained.clone()));
    assert_eq!(
        reconstructed.guardian_history,
        Some(codex_history::GuardianHistoryCheckpoint(
            if missing_guardian_boundary {
                Vec::new()
            } else {
                retained
            }
        ))
    );
}

#[tokio::test]
async fn complete_history_rollback_does_not_restore_removed_checkpoint_authorization() {
    let (session, turn_context) = make_session_and_context().await;
    let mut retained = annotated(vec![
        user_message("keep original"),
        assistant_message("original answer"),
    ]);
    let mut captured = ContextManager::for_session(&turn_context.session_source);
    captured.record_annotated_items(
        &mut retained,
        turn_context.model_info().truncation_policy.into(),
    );
    let mut items = completed_user_turn_rollout(
        turn_context.to_turn_context_item(),
        retained
            .iter()
            .cloned()
            .map(RolloutItem::ResponseItem)
            .collect(),
    );
    let mut context = turn_context.to_turn_context_item();
    context.turn_id = Some("removed-turn".to_string());
    let mut compacted = checkpoint_compacted(vec![
        user_message("rewritten retained turn"),
        assistant_message("rewritten answer"),
        user_message("remove"),
        assistant_message("removed"),
    ]);
    compacted.guardian_history = Some(codex_history::GuardianHistoryCheckpoint(vec![
        assistant_message("authorization belonging only to removed checkpoint"),
    ]));
    let mut removed_items = vec![RolloutItem::ResponseItem(user_message("remove").into())];
    removed_items.extend(
        CertifiedSegmentStateCheckpoint::new(
            compacted,
            None,
            None,
            None,
            complete_thread_settings(),
            TokenCountEvent {
                info: None,
                rate_limits: None,
            },
        )
        .expect("valid cleared checkpoint")
        .into_items(),
    );
    items.extend(completed_user_turn_rollout(context, removed_items));
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
    )));

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(reconstructed.history, retained);
    assert_eq!(reconstructed.guardian_history, None);
}

#[test_case(false; "matching retained message identity")]
#[test_case(true; "missing retained message identity marks evidence incomplete")]
#[tokio::test]
async fn checkpoint_only_rollback_preserves_retained_authorization(
    missing_retained_boundary: bool,
) {
    let (session, turn_context) = make_session_and_context().await;
    let mut kept = vec![
        retained_user_message("keep", "Keep this private.", 0),
        assistant_message("kept").into(),
    ];
    let mut removed = retained_user_message("remove", "Publish the removed result.", 2);
    let mut captured = ContextManager::for_session(&turn_context.session_source);
    captured.record_annotated_items(
        &mut kept,
        turn_context.model_info().truncation_policy.into(),
    );
    captured.record_retained_context(&retained_answer("keep", 1));
    let mut expected = captured.retained_context().clone();
    if missing_retained_boundary {
        captured.reserve_input_order();
        expected.mark_user_messages_incomplete();
    } else {
        captured.record_annotated_items(
            std::slice::from_mut(&mut removed),
            turn_context.model_info().truncation_policy.into(),
        );
    }
    captured.record_retained_context(&retained_answer("remove", 3));
    expected.reserve_order();
    expected.reserve_order();
    // Older checkpoints can retain message identity without acceptance-order metadata.
    removed.metadata = None;
    let mut replacement = kept.clone();
    replacement.extend([removed, assistant_message("removed").into()]);
    let mut compacted = checkpoint_compacted(Vec::new());
    compacted.replacement_history = Some(replacement);
    compacted.retained_context = Some(captured.retained_context().clone());
    let mut items = CertifiedSegmentStateCheckpoint::new(
        compacted,
        None,
        None,
        None,
        complete_thread_settings(),
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid retained authorization checkpoint")
    .into_items();
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(reconstructed.history, kept);
    assert_eq!(reconstructed.guardian_history, None);
    assert_eq!(reconstructed.retained_context, expected);
    assert_eq!(
        reconstructed.retained_context.user_messages_complete(),
        !missing_retained_boundary
    );
    assert!(reconstructed.retained_context.verified_answers_complete());
}

#[test_case(false, GuardianContextMode::Legacy; "rollback checkpoint turn")]
#[test_case(true, GuardianContextMode::Legacy; "rollback newer completed turn")]
#[test_case(false, GuardianContextMode::ThreadOwned; "retained authorization rollback checkpoint turn")]
#[test_case(true, GuardianContextMode::ThreadOwned; "retained authorization rollback newer completed turn")]
#[tokio::test]
async fn modern_certified_checkpoint_rollback_preserves_authoritative_metadata(
    rollback_newer_turn: bool,
    guardian_mode: GuardianContextMode,
) {
    let (session, turn_context) = make_session_and_context().await;
    let kept_user = match guardian_mode {
        GuardianContextMode::Legacy => user_message("keep").into(),
        GuardianContextMode::ThreadOwned => retained_user_message("keep", "keep", 0),
    };
    let removed_user = match guardian_mode {
        GuardianContextMode::Legacy => user_message("remove").into(),
        GuardianContextMode::ThreadOwned => retained_user_message("remove", "remove", 2),
    };
    let mut kept = vec![kept_user, assistant_message("kept").into()];
    let mut captured = ContextManager::for_session(&turn_context.session_source);
    if guardian_mode == GuardianContextMode::ThreadOwned {
        captured.record_annotated_items(
            &mut kept,
            turn_context.model_info().truncation_policy.into(),
        );
        captured.record_retained_context(&retained_answer("keep", 1));
    }
    let mut expected_retained = captured.retained_context().clone();
    let mut replacement = kept.clone();
    replacement.extend([removed_user, assistant_message("removed").into()]);
    if guardian_mode == GuardianContextMode::ThreadOwned {
        captured.record_annotated_items(
            &mut replacement[kept.len()..],
            turn_context.model_info().truncation_policy.into(),
        );
        captured.record_retained_context(&retained_answer("remove", 3));
        assert!(captured.retained_context().user_messages_complete());
        assert!(captured.retained_context().verified_answers_complete());
        if rollback_newer_turn {
            expected_retained = captured.retained_context().clone();
        }
        // Rolled-back inputs must not make their acceptance counters reusable.
        expected_retained.reserve_order();
        expected_retained.reserve_order();
    }
    let settings = PreviousTurnSettings {
        model: "checkpoint-model".to_string(),
        comp_hash: Some("checkpoint-hash".to_string()),
        realtime_active: Some(false),
    };
    let mut compacted = checkpoint_compacted(Vec::new());
    compacted.replacement_history = Some(replacement.clone());
    compacted.retained_context = (guardian_mode == GuardianContextMode::ThreadOwned)
        .then(|| captured.retained_context().clone());
    compacted.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: Some(codex_protocol::protocol::MultiAgentVersion::V2),
        last_started_turn_id: Some("checkpoint-turn".to_string()),
        previous_turn_settings: Some(settings.clone()),
    });
    let window_id = compacted.window_id.clone();
    compacted.guardian_history = (guardian_mode == GuardianContextMode::Legacy).then(|| {
        codex_history::GuardianHistoryCheckpoint(
            replacement.iter().map(|item| item.item.clone()).collect(),
        )
    });
    let mut reference = turn_context.to_turn_context_item();
    reference.turn_id = Some("checkpoint-turn".to_string());
    reference.model = "checkpoint-reference-model".to_string();
    let world_state = WorldStateItem::full(object!({"environment": {"checkpoint": true}}));
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted,
        Some(SegmentPreviousTurnSettings {
            model: settings.model.clone(),
            comp_hash: settings.comp_hash.clone(),
            realtime_active: settings.realtime_active,
        }),
        Some(world_state.clone()),
        Some(reference.clone()),
        complete_thread_settings(),
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid modern checkpoint");
    let mut bounded_items = checkpoint.into_items();
    if rollback_newer_turn {
        let mut newer = reference.clone();
        newer.turn_id = Some("newer-turn".to_string());
        newer.model = "rolled-back-model".to_string();
        let newer_user = match guardian_mode {
            GuardianContextMode::Legacy => user_message("newer task").into(),
            GuardianContextMode::ThreadOwned => {
                retained_user_message("newer-turn", "newer task", 4)
            }
        };
        let mut newer_items = vec![
            RolloutItem::ResponseItem(newer_user),
            RolloutItem::ResponseItem(assistant_message("newer answer").into()),
        ];
        if guardian_mode == GuardianContextMode::ThreadOwned {
            newer_items.push(RolloutItem::RetainedContext(retained_answer(
                "newer-turn",
                5,
            )));
        }
        bounded_items.extend(completed_user_turn_rollout(newer, newer_items));
    }
    bounded_items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));
    let mut full_items = completed_user_turn_rollout(
        turn_context.to_turn_context_item(),
        vec![RolloutItem::ResponseItem(
            user_message("obsolete original history").into(),
        )],
    );
    full_items.extend(bounded_items.clone());
    let bounded = session
        .reconstruct_history_from_rollout(&turn_context, &bounded_items)
        .await;
    let full = session
        .reconstruct_history_from_rollout(&turn_context, &full_items)
        .await;
    assert_eq!(bounded, full);
    let expected_history = if rollback_newer_turn {
        replacement
    } else {
        kept
    };
    assert_eq!(bounded.history, expected_history);
    assert_eq!(
        bounded.guardian_history,
        (guardian_mode == GuardianContextMode::Legacy).then(|| {
            codex_history::GuardianHistoryCheckpoint(
                bounded
                    .history
                    .iter()
                    .map(|item| item.item.clone())
                    .collect(),
            )
        })
    );
    if guardian_mode == GuardianContextMode::ThreadOwned {
        assert_eq!(bounded.retained_context, expected_retained);
        assert!(bounded.retained_context.user_messages_complete());
        assert!(bounded.retained_context.verified_answers_complete());
    }
    assert_eq!(bounded.previous_turn_settings, Some(settings));
    assert_eq!(
        bounded.last_started_turn_id.as_deref(),
        Some(if rollback_newer_turn {
            "newer-turn"
        } else {
            "checkpoint-turn"
        })
    );
    assert_eq!(bounded.window_number, 8);
    assert_eq!(bounded.window_id.map(|id| id.to_string()), window_id);
    assert_eq!(
        bounded.reference_context_item,
        rollback_newer_turn.then_some(reference)
    );
    assert_eq!(
        bounded.world_state_baseline,
        rollback_newer_turn
            .then(|| crate::context::world_state::WorldStateSnapshot::from(&world_state.state))
    );
}

#[test_case(false; "legacy certified metadata")]
#[test_case(true; "modern certified metadata")]
#[tokio::test]
async fn cleared_segment_state_checkpoint_blocks_older_resume_metadata(modern: bool) {
    let (session, turn_context) = make_session_and_context().await;
    let mut rollout_items = completed_user_turn_rollout(
        turn_context.to_turn_context_item(),
        vec![RolloutItem::ResponseItem(ResponseItemEnvelope::new(
            user_message("older turn"),
        ))],
    );
    let replacement_history = vec![assistant_message("cleared checkpoint")];
    let mut compacted = checkpoint_compacted(replacement_history.clone());
    compacted.resume_metadata = modern.then(|| object!({}));
    rollout_items.extend(
        CertifiedSegmentStateCheckpoint::new(
            compacted,
            /*previous_turn_settings*/ None,
            /*world_state*/ None,
            /*reference_context*/ None,
            complete_thread_settings(),
            TokenCountEvent {
                info: None,
                rate_limits: None,
            },
        )
        .expect("valid cleared checkpoint")
        .into_items(),
    );

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, rollout_items.as_slice())
        .await;

    assert_eq!(reconstructed.history, annotated(replacement_history));
    assert_eq!(reconstructed.previous_turn_settings, None);
    assert_eq!(reconstructed.reference_context_item, None);
    assert_eq!(reconstructed.world_state_baseline, None);
    assert_eq!(reconstructed.last_started_turn_id, None);
}

#[test_case(false; "legacy certified metadata")]
#[test_case(true; "modern certified metadata")]
#[tokio::test]
async fn newer_turn_context_overrides_cleared_segment_state_checkpoint(modern: bool) {
    let (session, turn_context) = make_session_and_context().await;
    let replacement_history = vec![assistant_message("cleared checkpoint")];
    let mut compacted = checkpoint_compacted(replacement_history.clone());
    compacted.resume_metadata = modern.then(|| object!({}));
    let mut rollout_items = CertifiedSegmentStateCheckpoint::new(
        compacted,
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        complete_thread_settings(),
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid cleared checkpoint")
    .into_items();
    let newer_reference_context = turn_context.to_turn_context_item();
    rollout_items.push(RolloutItem::TurnContext(newer_reference_context.clone()));

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, rollout_items.as_slice())
        .await;

    assert_eq!(reconstructed.history, annotated(replacement_history));
    assert_eq!(
        reconstructed.reference_context_item,
        Some(newer_reference_context)
    );
}

#[tokio::test]
async fn record_initial_history_reconstructs_typed_inter_agent_message() {
    let (session, _turn_context) = make_session_and_context().await;
    let communication = InterAgentCommunication::new(
        AgentPath::root().join("worker").expect("worker path"),
        AgentPath::root(),
        Vec::new(),
        "child done".to_string(),
        /*trigger_turn*/ false,
    );

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(vec![RolloutItem::InterAgentCommunication(
                communication.clone(),
            )]),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        raw_history_items(&session.state.lock().await.clone_history()),
        vec![communication.to_model_input_item()]
    );
}

#[tokio::test]
async fn record_initial_history_ignores_security_risk_scores() {
    let (session, _turn_context) = make_session_and_context().await;
    let user_item = user_message("visible user input");
    let security_risk = SecurityRiskScore {
        scores: BTreeMap::from([("credential_access".to_string(), 0.92)]),
        call_id: None,
        action: None,
        sampled_at: None,
    };

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(vec![
                RolloutItem::ResponseItem(ResponseItemEnvelope::new(user_item.clone())),
                RolloutItem::SecurityRiskScore(security_risk),
            ]),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        strip_metadata_from_items(&raw_history_items(
            &session.state.lock().await.clone_history()
        )),
        vec![user_item]
    );
}

#[derive(Clone, Copy)]
enum BaselineTurnInput {
    UserMessage,
    RemovedByFork,
}

#[test_case(BaselineTurnInput::UserMessage; "user turn")]
#[test_case(BaselineTurnInput::RemovedByFork; "fork removed the task message")]
#[tokio::test]
async fn record_initial_history_restores_world_state_baseline(input: BaselineTurnInput) {
    let (session, turn_context) = make_session_and_context().await;
    let turn_context = Arc::new(turn_context);
    let world_state = build_world_state_from_turn_context(&session, &turn_context).await;
    let expected_history = world_state
        .render_full()
        .into_iter()
        .map(ContextualUserFragment::into_boxed_response_item)
        .collect::<Vec<_>>();
    let mut world_state_items = expected_history
        .iter()
        .cloned()
        .map(ResponseItemEnvelope::new)
        .map(RolloutItem::ResponseItem)
        .collect::<Vec<_>>();
    world_state_items.push(RolloutItem::WorldState(WorldStateItem::full(
        world_state.snapshot().into_object(),
    )));
    let context_item = turn_context.to_turn_context_item();
    let rollout_items = match input {
        BaselineTurnInput::UserMessage => {
            completed_user_turn_rollout(context_item.clone(), world_state_items)
        }
        BaselineTurnInput::RemovedByFork => {
            world_state_items.push(RolloutItem::TurnContext(context_item.clone()));
            world_state_items
        }
    };
    // Exercise the persisted representation, not just an in-memory fork.
    let rollout_items =
        serde_json::from_value(serde_json::to_value(rollout_items).unwrap()).unwrap();

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");
    assert_eq!(
        (
            session.previous_turn_settings().await,
            serde_json::to_value(session.reference_context_item().await).unwrap(),
        ),
        (
            Some(PreviousTurnSettings {
                model: context_item.model.clone(),
                comp_hash: context_item.comp_hash.clone(),
                realtime_active: context_item.realtime_active,
            }),
            serde_json::to_value(Some(context_item)).unwrap(),
        )
    );
    let step_context = StepContext::for_test(Arc::clone(&turn_context));
    session
        .record_context_updates_and_set_reference_context_item(&step_context)
        .await
        .expect("world state should build");

    assert_eq!(
        raw_history_items(&session.clone_history().await),
        expected_history,
    );
}

#[tokio::test]
async fn record_initial_history_resumed_bare_turn_context_does_not_hydrate_previous_turn_settings()
{
    let (session, turn_context) = make_session_and_context().await;
    let previous_model = "previous-rollout-model";
    let previous_context_item = TurnContextItem {
        turn_id: Some(turn_context.sub_id.clone()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        #[allow(deprecated)]
        cwd: turn_context.cwd.clone(),
        workspace_roots: None,
        current_date: turn_context.current_date.clone(),
        timezone: turn_context.timezone.clone(),
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        personality: turn_context.personality(),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    };
    let rollout_items = vec![RolloutItem::TurnContext(previous_context_item)];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;
    assert_eq!(reconstructed.world_state_baseline, None);

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(session.previous_turn_settings().await, None);
    assert!(session.reference_context_item().await.is_none());
}

#[tokio::test]
async fn record_initial_history_resumed_hydrates_previous_turn_settings_from_lifecycle_turn_with_missing_turn_context_id()
 {
    let (session, turn_context) = make_session_and_context().await;
    let previous_model = "previous-rollout-model";
    let mut previous_context_item = TurnContextItem {
        turn_id: Some(turn_context.sub_id.clone()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        #[allow(deprecated)]
        cwd: turn_context.cwd.clone(),
        workspace_roots: None,
        current_date: turn_context.current_date.clone(),
        timezone: turn_context.timezone.clone(),
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: Some("comp-hash-a".to_string()),
        personality: turn_context.personality(),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    };
    let turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    previous_context_item.turn_id = None;

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id,
                last_agent_message: None,
                error: None,
                started_at: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: previous_model.to_string(),
            comp_hash: Some("comp-hash-a".to_string()),
            realtime_active: Some(turn_context.realtime_active),
        })
    );
}

#[tokio::test]
async fn reconstruct_history_rollback_keeps_history_and_metadata_in_sync_for_completed_turns() {
    let (session, turn_context) = make_session_and_context().await;
    let mut first_context_item = turn_context.to_turn_context_item();
    first_context_item.service_tier = Some("priority".to_string());
    first_context_item.model_profile = Some("balanced".to_string());
    let first_turn_id = first_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let mut rolled_back_context_item = first_context_item.clone();
    rolled_back_context_item.turn_id = Some("rolled-back-turn".to_string());
    rolled_back_context_item.model = "rolled-back-model".to_string();
    rolled_back_context_item.service_tier = Some("standard".to_string());
    rolled_back_context_item.model_profile = Some("alternate".to_string());
    let rolled_back_turn_id = rolled_back_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let turn_one_user = user_message("turn 1 user");
    let turn_one_assistant = assistant_message("turn 1 assistant");
    let turn_two_user = user_message("turn 2 user");
    let turn_two_assistant = assistant_message("turn 2 assistant");

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: first_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "turn 1 user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(first_context_item.clone()),
        RolloutItem::WorldState(WorldStateItem::full(object!({
            "test": {"environment": "first"}
        }))),
        RolloutItem::ResponseItem(turn_one_user.clone().into()),
        RolloutItem::ResponseItem(turn_one_assistant.clone().into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: first_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: rolled_back_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "turn 2 user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(rolled_back_context_item),
        RolloutItem::WorldState(WorldStateItem::patch(object!({
            "test": {"environment": "rolled-back"}
        }))),
        RolloutItem::ResponseItem(turn_two_user.into()),
        RolloutItem::ResponseItem(turn_two_assistant.into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: rolled_back_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
        )),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        reconstructed.history,
        annotated(vec![turn_one_user, turn_one_assistant])
    );
    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(PreviousTurnSettings {
            model: turn_context.model_info().slug.clone(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    let retained_context_item = reconstructed
        .reference_context_item
        .as_ref()
        .expect("rollback should retain the previous turn context");
    assert_eq!(
        retained_context_item.service_tier.as_deref(),
        Some("priority")
    );
    assert_eq!(
        retained_context_item.model_profile.as_deref(),
        Some("balanced")
    );
    assert_eq!(
        serde_json::to_value(&reconstructed.reference_context_item)
            .expect("serialize reconstructed reference context item"),
        serde_json::to_value(Some(first_context_item))
            .expect("serialize expected reference context item")
    );
    assert_eq!(
        serde_json::to_value(reconstructed.world_state_baseline)
            .expect("serialize reconstructed world state"),
        json!({"test": {"environment": "first"}})
    );
}

#[tokio::test]
async fn reconstruct_history_rollback_keeps_history_and_metadata_in_sync_for_incomplete_turn() {
    let (session, turn_context) = make_session_and_context().await;
    let first_context_item = turn_context.to_turn_context_item();
    let first_turn_id = first_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let incomplete_turn_id = "incomplete-rolled-back-turn".to_string();
    let turn_one_user = user_message("turn 1 user");
    let turn_one_assistant = assistant_message("turn 1 assistant");
    let turn_two_user = user_message("turn 2 user");

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: first_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "turn 1 user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(first_context_item.clone()),
        RolloutItem::ResponseItem(turn_one_user.clone().into()),
        RolloutItem::ResponseItem(turn_one_assistant.clone().into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: first_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: incomplete_turn_id,
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "turn 2 user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::ResponseItem(turn_two_user.into()),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
        )),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        reconstructed.history,
        annotated(vec![turn_one_user, turn_one_assistant])
    );
    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(PreviousTurnSettings {
            model: turn_context.model_info().slug.clone(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert_eq!(
        serde_json::to_value(reconstructed.reference_context_item)
            .expect("serialize reconstructed reference context item"),
        serde_json::to_value(Some(first_context_item))
            .expect("serialize expected reference context item")
    );
}

#[tokio::test]
async fn reconstruct_history_rollback_skips_non_user_turns_for_history_and_metadata() {
    let (session, turn_context) = make_session_and_context().await;
    let first_context_item = turn_context.to_turn_context_item();
    let first_turn_id = first_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let second_turn_id = "rolled-back-user-turn".to_string();
    let standalone_turn_id = "standalone-turn".to_string();
    let turn_one_user = user_message("turn 1 user");
    let turn_one_assistant = assistant_message("turn 1 assistant");
    let turn_two_user = user_message("turn 2 user");
    let turn_two_assistant = assistant_message("turn 2 assistant");
    let standalone_assistant = assistant_message("standalone assistant");

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: first_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "turn 1 user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(first_context_item.clone()),
        RolloutItem::ResponseItem(turn_one_user.clone().into()),
        RolloutItem::ResponseItem(turn_one_assistant.clone().into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: first_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: second_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "turn 2 user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::ResponseItem(turn_two_user.into()),
        RolloutItem::ResponseItem(turn_two_assistant.into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: second_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: standalone_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::ResponseItem(standalone_assistant.into()),
        RolloutItem::WorldState(WorldStateItem::full(object!({}))),
        RolloutItem::TurnContext(TurnContextItem {
            turn_id: Some(standalone_turn_id.clone()),
            ..first_context_item.clone()
        }),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: standalone_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
        )),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        reconstructed.history,
        annotated(vec![turn_one_user, turn_one_assistant])
    );
    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(PreviousTurnSettings {
            model: turn_context.model_info().slug.clone(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert_eq!(
        serde_json::to_value(reconstructed.reference_context_item)
            .expect("serialize reconstructed reference context item"),
        serde_json::to_value(Some(first_context_item))
            .expect("serialize expected reference context item")
    );
}

#[tokio::test]
async fn reconstruct_history_rollback_counts_inter_agent_assistant_turns() {
    let (session, turn_context) = make_session_and_context().await;
    let first_context_item = turn_context.to_turn_context_item();
    let first_turn_id = first_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let assistant_turn_id = "assistant-instruction-turn".to_string();
    let assistant_turn_context = TurnContextItem {
        turn_id: Some(assistant_turn_id.clone()),
        ..first_context_item.clone()
    };
    let assistant_instruction = inter_agent_assistant_message("continue");
    let assistant_reply = assistant_message("worker reply");

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: first_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "turn 1 user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(first_context_item.clone()),
        RolloutItem::ResponseItem(user_message("turn 1 user").into()),
        RolloutItem::ResponseItem(assistant_message("turn 1 assistant").into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: first_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: assistant_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::TurnContext(assistant_turn_context),
        RolloutItem::ResponseItem(assistant_instruction.into()),
        RolloutItem::ResponseItem(assistant_reply.into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: assistant_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
        )),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        reconstructed.history,
        annotated(vec![
            user_message("turn 1 user"),
            assistant_message("turn 1 assistant")
        ])
    );
    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(PreviousTurnSettings {
            model: turn_context.model_info().slug.clone(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert_eq!(
        serde_json::to_value(reconstructed.reference_context_item)
            .expect("serialize reconstructed reference context item"),
        serde_json::to_value(Some(first_context_item))
            .expect("serialize expected reference context item")
    );
}

#[tokio::test]
async fn reconstruct_history_rollback_clears_history_and_metadata_when_exceeding_user_turns() {
    let (session, turn_context) = make_session_and_context().await;
    let only_context_item = turn_context.to_turn_context_item();
    let only_turn_id = only_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: only_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "only user".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(only_context_item),
        RolloutItem::ResponseItem(user_message("only user").into()),
        RolloutItem::ResponseItem(assistant_message("only assistant").into()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: only_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 99 },
        )),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(reconstructed.history, Vec::new());
    assert_eq!(reconstructed.previous_turn_settings, None);
    assert!(reconstructed.reference_context_item.is_none());
}

#[tokio::test]
async fn record_initial_history_resumed_rollback_skips_only_user_turns() {
    let (session, turn_context) = make_session_and_context().await;
    let previous_context_item = turn_context.to_turn_context_item();
    let user_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let standalone_turn_id = "standalone-task-turn".to_string();
    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: user_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: user_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        // Standalone task turn (no UserMessage) should not consume rollback skips.
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: standalone_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: standalone_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
        )),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(session.previous_turn_settings().await, None);
    assert!(session.reference_context_item().await.is_none());
}

#[tokio::test]
async fn record_initial_history_resumed_rollback_drops_incomplete_user_turn_compaction_metadata() {
    let (session, turn_context) = make_session_and_context().await;
    let previous_context_item = turn_context.to_turn_context_item();
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let incomplete_turn_id = "incomplete-compacted-user-turn".to_string();

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: previous_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(previous_context_item.clone()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: previous_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: incomplete_turn_id,
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "rolled back".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
        )),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: turn_context.model_info().slug.clone(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert_eq!(
        serde_json::to_value(session.reference_context_item().await)
            .expect("serialize seeded reference context item"),
        serde_json::to_value(Some(previous_context_item))
            .expect("serialize expected reference context item")
    );
}

#[derive(Clone, Copy)]
enum MissingContextBaseline {
    BareTurnContext,
    WorldStatePatch,
    CompactedSnapshot,
}

#[test_case(MissingContextBaseline::BareTurnContext; "bare turn context")]
#[test_case(MissingContextBaseline::WorldStatePatch; "patch without a full snapshot")]
#[test_case(MissingContextBaseline::CompactedSnapshot; "full snapshot before compaction")]
#[tokio::test]
async fn record_initial_history_requires_surviving_full_snapshot_without_user_turn(
    baseline: MissingContextBaseline,
) {
    let (session, turn_context) = make_session_and_context().await;
    let mut rollout_items = match baseline {
        MissingContextBaseline::BareTurnContext => Vec::new(),
        MissingContextBaseline::WorldStatePatch => {
            vec![RolloutItem::WorldState(WorldStateItem::patch(object!({})))]
        }
        MissingContextBaseline::CompactedSnapshot => vec![
            RolloutItem::WorldState(WorldStateItem::full(object!({}))),
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(Vec::new()),
                retained_context: None,
                retained_context_replay: None,
                guardian_history: None,
                mcp_resource_origins: None,
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
                compaction_response_id: None,
                latest_token_usage_record: None,
                resume_metadata: None,
                segment_state_checkpoint: None,
            }),
        ],
    };
    rollout_items.push(RolloutItem::TurnContext(
        turn_context.to_turn_context_item(),
    ));

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert!(session.reference_context_item().await.is_none());
}

#[tokio::test]
async fn record_initial_history_resumed_does_not_seed_reference_context_item_after_compaction() {
    let (session, turn_context) = make_session_and_context().await;
    let previous_context_item = turn_context.to_turn_context_item();
    let rollout_items = vec![
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(session.previous_turn_settings().await, None);
    assert!(session.reference_context_item().await.is_none());
}

#[tokio::test]
async fn reconstruct_history_restores_initial_window_from_session_meta() {
    let (session, turn_context) = make_session_and_context().await;
    let thread_id = ThreadId::default();
    let initial_window_id = Uuid::now_v7();
    let rollout_items = vec![RolloutItem::SessionMeta(SessionMetaLine {
        meta: SessionMeta {
            session_id: thread_id.into(),
            id: thread_id,
            context_window: Some(SessionContextWindow {
                window_id: initial_window_id.to_string(),
            }),
            ..SessionMeta::default()
        },
        git: None,
    })];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(reconstructed.window_number, 0);
    assert_eq!(reconstructed.first_window_id, Some(initial_window_id));
    assert_eq!(reconstructed.previous_window_id, None);
    assert_eq!(reconstructed.window_id, Some(initial_window_id));
}

#[tokio::test]
async fn reconstruct_history_prefers_compacted_window_over_session_meta() {
    let (session, turn_context) = make_session_and_context().await;
    let thread_id = ThreadId::default();
    let initial_window_id = Uuid::now_v7();
    let compacted_first_window_id = Uuid::now_v7();
    let compacted_previous_window_id = Uuid::now_v7();
    let compacted_window_id = Uuid::now_v7();
    let rollout_items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                context_window: Some(SessionContextWindow {
                    window_id: initial_window_id.to_string(),
                }),
                ..SessionMeta::default()
            },
            git: None,
        }),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: Some(2),
            first_window_id: Some(compacted_first_window_id.to_string()),
            previous_window_id: Some(compacted_previous_window_id.to_string()),
            window_id: Some(compacted_window_id.to_string()),
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(reconstructed.window_number, 2);
    assert_eq!(
        reconstructed.first_window_id,
        Some(compacted_first_window_id)
    );
    assert_eq!(
        reconstructed.previous_window_id,
        Some(compacted_previous_window_id)
    );
    assert_eq!(reconstructed.window_id, Some(compacted_window_id));
}

#[tokio::test]
async fn reconstruct_history_replays_world_state_from_latest_compaction_window() {
    let (session, turn_context) = make_session_and_context().await;
    let rollout_items = completed_user_turn_rollout(
        turn_context.to_turn_context_item(),
        vec![
            RolloutItem::WorldState(WorldStateItem::full(object!({
                "environment": {"status": "old"}
            }))),
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(Vec::new()),
                retained_context: None,
                retained_context_replay: None,
                guardian_history: None,
                mcp_resource_origins: None,
                window_number: Some(1),
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
                compaction_response_id: None,
                latest_token_usage_record: None,
                resume_metadata: None,
                segment_state_checkpoint: None,
            }),
            RolloutItem::WorldState(WorldStateItem::full(object!({
                "environment": {"status": "starting", "cwd": "/workspace"}
            }))),
            RolloutItem::WorldState(WorldStateItem::patch(object!({
                "environment": {"status": "ready"}
            }))),
        ],
    );

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        serde_json::to_value(reconstructed.world_state_baseline)
            .expect("serialize reconstructed world state"),
        json!({
            "environment": {"status": "ready", "cwd": "/workspace"}
        })
    );
}

#[derive(Clone, Copy)]
enum CompactionFormat {
    Current,
    Legacy,
}

#[test_case(CompactionFormat::Current; "current compactions")]
#[test_case(CompactionFormat::Legacy; "legacy compactions")]
#[tokio::test]
async fn bounded_replay_matches_full_replay_after_empty_turn_compactions(
    compaction_format: CompactionFormat,
) {
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let session_meta = SessionMetaLine {
        meta: SessionMeta {
            history_mode: ThreadHistoryMode::Paginated,
            ..Default::default()
        },
        git: None,
    };
    let initial_context = turn_context.to_turn_context_item();
    let mut rollout_items = vec![RolloutItem::SessionMeta(session_meta.clone())];
    let settings = complete_thread_settings();
    let token_info = codex_protocol::protocol::TokenUsageInfo {
        total_token_usage: codex_protocol::protocol::TokenUsage {
            total_tokens: 420,
            ..Default::default()
        },
        last_token_usage: Default::default(),
        model_context_window: Some(100_000),
    };
    let rate_limits = codex_protocol::protocol::RateLimitSnapshot {
        limit_id: Some("preserved-limit".to_string()),
        limit_name: None,
        normal_model_slug: None,
        primary: Some(codex_protocol::protocol::RateLimitWindow {
            used_percent: 12.5,
            window_minutes: Some(60),
            resets_at: Some(2_000_000_000),
        }),
        secondary: None,
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        plan_type: None,
        rate_limit_reached_type: None,
    };
    let usage_record =
        |response_id: &str, total_tokens| codex_protocol::protocol::TokenUsageRecord {
            thread_id: session_meta.meta.id,
            session_id: session_meta.meta.id.into(),
            turn_id: response_id.to_string(),
            root_turn_id: response_id.to_string(),
            response_id: response_id.to_string(),
            usage: codex_protocol::protocol::TokenUsage {
                total_tokens,
                ..Default::default()
            },
            turn_token_usage: Default::default(),
            thread_token_usage: Default::default(),
        };
    rollout_items.extend([
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(settings.clone())),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: Some(token_info.clone()),
            rate_limits: None,
        })),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: Some(rate_limits.clone()),
        })),
        RolloutItem::TokenUsageRecord(usage_record("old-response", 10)),
    ]);
    rollout_items.extend(completed_user_turn_rollout(
        initial_context.clone(),
        vec![RolloutItem::ResponseItem(
            user_message("original task").into(),
        )],
    ));
    let window_ids = [Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()];
    let current = matches!(compaction_format, CompactionFormat::Current);
    let mut latest_wake_start = 0;
    for window_number in 1..=2 {
        let mut context = initial_context.clone();
        context.turn_id = Some(format!("wake-{window_number}"));
        context.model = format!("model-{window_number}");
        context.comp_hash = Some(format!("hash-{window_number}"));
        context.realtime_active = Some(window_number == 2);
        let mut wake_items = completed_user_turn_rollout(
            context.clone(),
            vec![
                RolloutItem::Compacted(CompactedItem {
                    message: String::new(),
                    replacement_history: Some(annotated(vec![object!({
                        "type": "compaction",
                        "id": format!("checkpoint-{window_number}"),
                        "encrypted_content": format!("summary-{window_number}"),
                    })])),
                    retained_context: None,
                    retained_context_replay: None,
                    guardian_history: Some(codex_history::GuardianHistoryCheckpoint(vec![
                        user_message("original task"),
                    ])),
                    mcp_resource_origins: None,
                    window_number: Some(window_number as u64),
                    first_window_id: Some(window_ids[0].to_string()),
                    previous_window_id: Some(window_ids[window_number - 1].to_string()),
                    window_id: Some(window_ids[window_number].to_string()),
                    compaction_response_id: None,
                    latest_token_usage_record: Some(usage_record("checkpoint-response", 20)),
                    resume_metadata: current.then(|| codex_history::CompactionResumeMetadata {
                        multi_agent_version: None,
                        last_started_turn_id: Some(format!("wake-{window_number}")),
                        previous_turn_settings: Some(PreviousTurnSettings {
                            model: format!("metadata-model-{window_number}"),
                            comp_hash: Some(format!("metadata-hash-{window_number}")),
                            realtime_active: Some(false),
                        }),
                    }),
                    segment_state_checkpoint: None,
                }),
                RolloutItem::WorldState(WorldStateItem::full(object!({
                    "environment": {"window": window_number, "status": "starting"}
                }))),
                RolloutItem::TurnContext(context),
                RolloutItem::ResponseItem(assistant_message("continued working").into()),
                RolloutItem::WorldState(WorldStateItem::patch(object!({
                    "environment": {"status": "ready"}
                }))),
            ],
        );
        // Clock wakes have no user-message boundary, and the next wait interrupts the turn.
        wake_items.retain(|item| {
            !matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(_) | EventMsg::TurnComplete(_))
            )
        });
        wake_items.push(RolloutItem::EventMsg(EventMsg::TurnAborted(
            codex_protocol::protocol::TurnAbortedEvent {
                turn_id: Some(format!("wake-{window_number}")),
                reason: TurnAbortReason::Interrupted,
                started_at: None,
                completed_at: None,
                duration_ms: None,
            },
        )));
        latest_wake_start = rollout_items.len();
        rollout_items.extend(wake_items);
    }
    let latest_usage = usage_record("latest-response", 30);
    rollout_items.push(RolloutItem::TokenUsageRecord(latest_usage.clone()));

    let mut scan = ModelContextScan::default();
    let cutoff = rollout_items
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, item)| {
            matches!(scan.push(item.clone()), ModelContextScanProgress::Complete).then_some(index)
        });
    // Unmarked compactions still scan older records for sticky settings and usage, while
    // discarding older model items once the replacement history has a complete baseline.
    assert_eq!(cutoff, None);
    let mut bounded_items = scan.finish();
    bounded_items.retain(|item| !matches!(item, RolloutItem::SessionMeta(_)));
    bounded_items.insert(0, RolloutItem::SessionMeta(session_meta));
    assert!(bounded_items.len() <= rollout_items.len() - latest_wake_start + 4);
    let full = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;
    let bounded = session
        .reconstruct_history_from_rollout(&turn_context, &bounded_items)
        .await;
    assert_eq!(
        bounded.guardian_history.as_ref(),
        Some(&codex_history::GuardianHistoryCheckpoint(vec![
            user_message("original task"),
            assistant_message("continued working"),
        ])),
    );
    if current {
        assert_eq!(
            bounded
                .previous_turn_settings
                .as_ref()
                .map(|settings| settings.model.as_str()),
            Some("metadata-model-2")
        );
    }
    assert_eq!(bounded.last_started_turn_id.as_deref(), Some("wake-2"));
    assert_eq!(bounded, full);
    let expected_token_count = Some(TokenCountEvent {
        info: Some(token_info),
        rate_limits: Some(rate_limits),
    });
    for items in [&rollout_items, &bounded_items] {
        assert_eq!(
            serde_json::to_value(Session::restored_token_count_from_rollout(items))
                .expect("serialize restored token count"),
            serde_json::to_value(&expected_token_count).expect("serialize expected token count")
        );
        assert_eq!(
            Session::last_token_usage_record_from_rollout(items),
            Some(latest_usage.clone())
        );
        assert_eq!(
            items.iter().rev().find_map(|item| match item {
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) => Some(event),
                _ => None,
            }),
            Some(&settings)
        );
    }
}

#[tokio::test]
async fn paginated_compaction_does_not_restore_missing_companions_from_older_history() {
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;

    for resume_metadata in [None, Some(object!({}))] {
        let mut compacted: CompactedItem = object!({
            "message": "summary",
            "replacement_history": [assistant_message("summary")],
            "window_number": 1
        });
        compacted.resume_metadata = resume_metadata;
        let rollout_items = vec![
            RolloutItem::TurnContext(turn_context.to_turn_context_item()),
            RolloutItem::WorldState(WorldStateItem::full(object!({"older": true}))),
            RolloutItem::Compacted(compacted),
        ];

        let reconstructed = session
            .reconstruct_history_from_rollout(&turn_context, &rollout_items)
            .await;

        assert_eq!(reconstructed.previous_turn_settings, None);
        assert_eq!(reconstructed.reference_context_item, None);
        assert_eq!(reconstructed.world_state_baseline, None);
    }
}

#[tokio::test]
async fn completed_turn_suffix_after_compaction_overrides_resume_metadata() {
    let (session, turn_context) = make_session_and_context().await;
    let mut newer_context = turn_context.to_turn_context_item();
    newer_context.turn_id = Some("newer-turn".to_string());
    newer_context.model = "newer-model".to_string();
    newer_context.comp_hash = Some("newer-hash".to_string());
    let expected_settings = PreviousTurnSettings {
        model: newer_context.model.clone(),
        comp_hash: newer_context.comp_hash.clone(),
        realtime_active: newer_context.realtime_active,
    };
    let mut rollout_items = vec![
        RolloutItem::Compacted(object!({
            "message": "summary",
            "replacement_history": [user_message("seed"), assistant_message("summary")],
            "window_number": 1,
            "resume_metadata": {
                "previous_turn_settings": {
                    "model": "metadata-model",
                    "comp_hash": "metadata-hash"
                }
            }
        })),
        RolloutItem::WorldState(WorldStateItem::full(object!({}))),
    ];
    // The bounded suffix starts at the compaction, so the turn start and user input are already in
    // replacement history. Its context and completion still make its settings authoritative.
    rollout_items.extend(
        completed_user_turn_rollout(newer_context.clone(), Vec::new())
            .into_iter()
            .skip(2),
    );

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(expected_settings)
    );
    assert_eq!(reconstructed.reference_context_item, Some(newer_context));
}

#[tokio::test]
async fn reconstruct_history_preserves_legacy_compaction_count_with_session_meta_window() {
    let (session, turn_context) = make_session_and_context().await;
    let thread_id = ThreadId::default();
    let initial_window_id = Uuid::now_v7();
    let rollout_items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                context_window: Some(SessionContextWindow {
                    window_id: initial_window_id.to_string(),
                }),
                ..SessionMeta::default()
            },
            git: None,
        }),
        RolloutItem::Compacted(CompactedItem {
            message: "legacy summary".to_string(),
            replacement_history: None,
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(reconstructed.window_number, 1);
    assert_eq!(reconstructed.first_window_id, None);
    assert_eq!(reconstructed.previous_window_id, None);
    assert_eq!(reconstructed.window_id, None);
}

#[tokio::test]
async fn reconstruct_history_legacy_compaction_without_replacement_history_does_not_inject_current_initial_context()
 {
    let (session, turn_context) = make_session_and_context().await;
    let answer = codex_history::RetainedContextEvent::VerifiedAnswer {
        answer: codex_history::VerifiedAnswer {
            turn_id: "legacy-turn".to_owned(),
            call_id: "ask-1".to_owned(),
            questions: vec![codex_history::VerifiedQuestionAnswer {
                question: "Upload?".to_owned(),
                answer: "Only privately.".to_owned(),
            }],
        },
        acceptance_order: None,
    };
    let mut retained = codex_history::RetainedContext::default();
    retained.record_user_message(
        codex_history::RetainedUserMessage {
            phase: None,
            origin: codex_history::UserInputOrigin::User,
            turn_id: String::new(),
            message_id: None,
            text: "before compact".to_owned(),
            complete: false,
        },
        codex_history::RetainedInputSource::Local(None),
    );
    retained.record(&answer);
    let rollout_items = vec![
        RolloutItem::ResponseItem(user_message("before compact").into()),
        RolloutItem::ResponseItem(assistant_message("assistant reply").into()),
        RolloutItem::RetainedContext(answer),
        RolloutItem::Compacted(CompactedItem {
            message: "legacy summary".to_string(),
            replacement_history: None,
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        reconstructed.history,
        annotated(vec![
            user_message("before compact"),
            ContextualUserFragment::into(CompactionSummary::new("legacy summary")),
        ])
    );
    assert!(reconstructed.reference_context_item.is_none());
    assert_eq!(reconstructed.retained_context, retained);
}

#[tokio::test]
async fn reconstruct_history_legacy_compaction_without_replacement_history_clears_later_reference_context_item()
 {
    let (session, turn_context) = make_session_and_context().await;
    let current_context_item = turn_context.to_turn_context_item();
    let current_turn_id = current_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let rollout_items = vec![
        RolloutItem::ResponseItem(user_message("before compact").into()),
        RolloutItem::Compacted(CompactedItem {
            message: "legacy summary".to_string(),
            replacement_history: None,
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: current_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "after legacy compact".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(current_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: current_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert!(reconstructed.reference_context_item.is_none());
}

#[tokio::test]
async fn record_initial_history_resumed_turn_context_after_compaction_reestablishes_reference_context_item()
 {
    let (session, turn_context) = make_session_and_context().await;
    let previous_model = "previous-rollout-model";
    let previous_context_item = TurnContextItem {
        turn_id: Some(turn_context.sub_id.clone()),
        root_turn_id: Some("root-turn".to_string()),
        disabled_plugin_ids: None,
        #[allow(deprecated)]
        cwd: turn_context.cwd.clone(),
        workspace_roots: None,
        current_date: turn_context.current_date.clone(),
        timezone: turn_context.timezone.clone(),
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        personality: turn_context.personality(),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    };
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: previous_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        // Compaction clears baseline until a later TurnContextItem re-establishes it.
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: previous_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: previous_model.to_string(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert_eq!(
        serde_json::to_value(session.reference_context_item().await)
            .expect("serialize seeded reference context item"),
        serde_json::to_value(Some(TurnContextItem {
            turn_id: Some(turn_context.sub_id.clone()),
            root_turn_id: Some("root-turn".to_string()),
            disabled_plugin_ids: None,
            #[allow(deprecated)]
            cwd: turn_context.cwd.clone(),
            workspace_roots: None,
            current_date: turn_context.current_date.clone(),
            timezone: turn_context.timezone.clone(),
            approval_policy: turn_context.approval_policy(),
            approvals_reviewer: None,
            sandbox_policy: turn_context.sandbox_policy(),
            permission_profile: None,
            active_permission_profile: None,
            network: None,
            file_system_sandbox_policy: None,
            model: previous_model.to_string(),
            comp_hash: None,
            personality: turn_context.personality(),
            collaboration_mode: Some(turn_context.collaboration_mode()),
            multi_agent_version: None,
            multi_agent_mode: None,
            realtime_active: Some(turn_context.realtime_active),
            cyber_access_program: None,
            effort: turn_context.reasoning_effort().cloned(),
            service_tier: None,
            model_profile: None,
            summary: codex_protocol::config_types::ReasoningSummary::Auto,
        }))
        .expect("serialize expected reference context item")
    );
}

#[tokio::test]
async fn record_initial_history_resumed_aborted_turn_without_id_clears_active_turn_for_compaction_accounting()
 {
    let (session, turn_context) = make_session_and_context().await;
    let previous_model = "previous-rollout-model";
    let previous_context_item = TurnContextItem {
        turn_id: Some(turn_context.sub_id.clone()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        #[allow(deprecated)]
        cwd: turn_context.cwd.clone(),
        workspace_roots: None,
        current_date: turn_context.current_date.clone(),
        timezone: turn_context.timezone.clone(),
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        personality: turn_context.personality(),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    };
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let aborted_turn_id = "aborted-turn-without-id".to_string();

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: previous_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: previous_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: aborted_turn_id,
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "aborted".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnAborted(
            codex_protocol::protocol::TurnAbortedEvent {
                turn_id: None,
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                completed_at: None,
                duration_ms: None,
            },
        )),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: previous_model.to_string(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert!(session.reference_context_item().await.is_none());
}

#[tokio::test]
async fn record_initial_history_resumed_unmatched_abort_preserves_active_turn_for_later_turn_context()
 {
    let (session, turn_context) = make_session_and_context().await;
    let previous_context_item = turn_context.to_turn_context_item();
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let current_model = "current-rollout-model";
    let current_turn_id = "current-turn".to_string();
    let unmatched_abort_turn_id = "other-turn".to_string();
    let current_context_item = TurnContextItem {
        turn_id: Some(current_turn_id.clone()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        #[allow(deprecated)]
        cwd: turn_context.cwd.clone(),
        workspace_roots: None,
        current_date: turn_context.current_date.clone(),
        timezone: turn_context.timezone.clone(),
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: current_model.to_string(),
        comp_hash: None,
        personality: turn_context.personality(),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    };

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: previous_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: previous_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: current_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "current".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnAborted(
            codex_protocol::protocol::TurnAbortedEvent {
                turn_id: Some(unmatched_abort_turn_id),
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                completed_at: None,
                duration_ms: None,
            },
        )),
        RolloutItem::TurnContext(current_context_item.clone()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: current_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: current_model.to_string(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert_eq!(
        serde_json::to_value(session.reference_context_item().await)
            .expect("serialize seeded reference context item"),
        serde_json::to_value(Some(current_context_item))
            .expect("serialize expected reference context item")
    );
}

#[tokio::test]
async fn record_initial_history_resumed_trailing_incomplete_turn_compaction_clears_reference_context_item()
 {
    let (session, turn_context) = make_session_and_context().await;
    let previous_model = "previous-rollout-model";
    let previous_context_item = TurnContextItem {
        turn_id: Some(turn_context.sub_id.clone()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        #[allow(deprecated)]
        cwd: turn_context.cwd.clone(),
        workspace_roots: None,
        current_date: turn_context.current_date.clone(),
        timezone: turn_context.timezone.clone(),
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        personality: turn_context.personality(),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    };
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let incomplete_turn_id = "trailing-incomplete-turn".to_string();

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: previous_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: previous_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: incomplete_turn_id,
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "incomplete".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: previous_model.to_string(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert!(session.reference_context_item().await.is_none());
}

#[tokio::test]
async fn record_initial_history_resumed_trailing_incomplete_turn_preserves_turn_context_item() {
    let (session, turn_context) = make_session_and_context().await;
    let current_context_item = turn_context.to_turn_context_item();
    let current_turn_id = current_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: current_turn_id,
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "incomplete".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(current_context_item.clone()),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: turn_context.model_info().slug.clone(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert_eq!(
        serde_json::to_value(session.reference_context_item().await)
            .expect("serialize seeded reference context item"),
        serde_json::to_value(Some(current_context_item))
            .expect("serialize expected reference context item")
    );
}

#[tokio::test]
async fn record_initial_history_resumed_replaced_incomplete_compacted_turn_clears_reference_context_item()
 {
    let (session, turn_context) = make_session_and_context().await;
    let previous_model = "previous-rollout-model";
    let previous_context_item = TurnContextItem {
        turn_id: Some(turn_context.sub_id.clone()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        #[allow(deprecated)]
        cwd: turn_context.cwd.clone(),
        workspace_roots: None,
        current_date: turn_context.current_date.clone(),
        timezone: turn_context.timezone.clone(),
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        personality: turn_context.personality(),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    };
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let compacted_incomplete_turn_id = "compacted-incomplete-turn".to_string();
    let replacing_turn_id = "replacing-turn".to_string();

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: previous_turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "seed".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(previous_context_item),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: previous_turn_id,
                started_at: None,
                last_agent_message: None,
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        )),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: compacted_incomplete_turn_id,
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: "compacted".to_string(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }),
        // A newer TurnStarted replaces the incomplete compacted turn without a matching
        // completion/abort for the old one.
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: replacing_turn_id,
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
    ];

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            conversation_id: ThreadId::default(),
            history: Arc::new(rollout_items),
            rollout_path: Some(PathBuf::from("/tmp/resume.jsonl")),
        }))
        .await
        .expect("record initial history");

    assert_eq!(
        session.previous_turn_settings().await,
        Some(PreviousTurnSettings {
            model: previous_model.to_string(),
            comp_hash: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
    assert!(session.reference_context_item().await.is_none());
}

#[tokio::test]
async fn explicit_empty_resume_metadata_blocks_older_turn_state() {
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let mut older_context = turn_context.to_turn_context_item();
    older_context.turn_id = Some("older-completed-turn".to_string());
    older_context.model = "older-model".to_string();
    older_context.comp_hash = Some("older-settings".to_string());
    let mut items = completed_user_turn_rollout(
        older_context,
        vec![RolloutItem::ResponseItem(
            user_message("older request").into(),
        )],
    );
    let older = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(
        older.last_started_turn_id.as_deref(),
        Some("older-completed-turn")
    );
    assert_eq!(
        older
            .previous_turn_settings
            .as_ref()
            .map(|settings| settings.model.as_str()),
        Some("older-model")
    );

    let replacement = vec![assistant_message("modern summary")];
    let mut compacted = checkpoint_compacted(replacement.clone());
    compacted.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    items.push(RolloutItem::Compacted(compacted));

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(reconstructed.history, annotated(replacement));
    assert_eq!(reconstructed.last_started_turn_id, None);
    assert_eq!(reconstructed.previous_turn_settings, None);
}

#[tokio::test]
async fn incomplete_turn_after_modern_checkpoint_updates_only_last_started_turn_id() {
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let previous_settings = PreviousTurnSettings {
        model: "checkpoint-model".to_string(),
        comp_hash: Some("checkpoint-settings".to_string()),
        realtime_active: Some(true),
    };
    let replacement = vec![assistant_message("modern summary")];
    let mut compacted = checkpoint_compacted(replacement.clone());
    compacted.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: Some("checkpoint-turn".to_string()),
        previous_turn_settings: Some(previous_settings.clone()),
    });
    let mut newer_context = turn_context.to_turn_context_item();
    newer_context.turn_id = Some("newer-incomplete-turn".to_string());
    newer_context.model = "newer-incomplete-model".to_string();
    newer_context.comp_hash = Some("newer-incomplete-settings".to_string());
    let items = vec![
        RolloutItem::Compacted(compacted),
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: "newer-incomplete-turn".to_string(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::TurnContext(newer_context),
    ];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(reconstructed.history, annotated(replacement));
    assert_eq!(
        reconstructed.last_started_turn_id.as_deref(),
        Some("newer-incomplete-turn")
    );
    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(previous_settings)
    );
}

// The adjacent raw/event pair describes one user boundary to migration and replay.
fn migration_user_turn_rollout(
    context: TurnContextItem,
    user: ResponseItemEnvelope,
    remaining: Vec<RolloutItem>,
) -> Vec<RolloutItem> {
    let turn_id = context.turn_id.clone().expect("fixture turn ID");
    let Some(codex_protocol::items::TurnItem::UserMessage(user_message)) =
        crate::event_mapping::parse_turn_item(&user.item)
    else {
        panic!("migration fixture requires a user message");
    };
    let mut items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: turn_id.clone(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        )),
        RolloutItem::ResponseItem(user),
        RolloutItem::EventMsg(EventMsg::UserMessage(
            codex_protocol::protocol::UserMessageEvent {
                client_id: None,
                message: user_message.message(),
                images: None,
                local_images: Vec::new(),
                text_elements: Vec::new(),
                ..Default::default()
            },
        )),
        RolloutItem::TurnContext(context),
    ];
    items.extend(remaining);
    items.push(RolloutItem::EventMsg(EventMsg::TurnComplete(
        codex_protocol::protocol::TurnCompleteEvent {
            turn_id,
            last_agent_message: None,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        },
    )));
    items
}

// Uses the actual migration publication and readers, rather than reproducing their rewrite.
async fn migrate_checkpoint_fixture(
    home: &std::path::Path,
    thread_id: ThreadId,
    items: &[RolloutItem],
) -> (
    codex_thread_store::LocalThreadStore,
    Vec<RolloutItem>,
    Vec<RolloutItem>,
) {
    use codex_thread_store::ThreadStore;
    use std::io::Write;

    let directory = home.join("sessions/2025/01/03");
    std::fs::create_dir_all(&directory).expect("create isolated session directory");
    let path = directory.join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    {
        let mut file = std::fs::File::create(&path).expect("create legacy rollout");
        for item in items {
            let line = codex_rollout::RolloutLine {
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                ordinal: None,
                item: item.clone(),
            };
            writeln!(
                file,
                "{}",
                serde_json::to_string(&line).expect("serialize rollout")
            )
            .expect("write rollout");
        }
    }
    let sqlite = codex_state::SqliteConfig::new_for_testing(
        codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path(home)
            .expect("absolute isolated CODEX_HOME"),
    );
    let rollout_config = codex_rollout::RolloutConfig {
        codex_home: home.to_path_buf(),
        sqlite: sqlite.clone(),
        cwd: home.to_path_buf(),
        model_provider_id: "test-provider".to_string(),
        generate_memories: false,
    };
    let state_db = codex_rollout::state_db::try_init(&rollout_config)
        .await
        .expect("backfill isolated legacy session");
    let store = codex_thread_store::LocalThreadStore::new(
        codex_thread_store::LocalThreadStoreConfig {
            codex_home: home.to_path_buf(),
            sqlite,
            default_model_provider_id: "test-provider".to_string(),
        },
        Some(state_db),
    );
    let report = store
        .migrate_rollouts(codex_thread_store::RolloutMigrationOptions {
            mode: codex_thread_store::RolloutMigrationMode::Apply,
            thread_ids: vec![thread_id],
            max_mib_per_second: None,
        })
        .await
        .expect("migrate isolated session");
    assert_eq!(report.outcomes.len(), 1, "{report:?}");
    assert_eq!(
        report.outcomes[0].status,
        codex_thread_store::RolloutMigrationStatus::Migrated,
        "{report:?}"
    );
    let mut full = store
        .load_history(codex_thread_store::LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("read migrated full history")
        .items;
    let mut bounded = store
        .load_latest_model_context(codex_thread_store::LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("read migrated model context")
        .items;
    super::rollout_reconstruction::resolve_review_input_for_reconstruction(home, &mut full)
        .await
        .expect("resolve selected full-history review input");
    super::rollout_reconstruction::resolve_review_input_for_reconstruction(home, &mut bounded)
        .await
        .expect("resolve selected bounded review input");
    assert_eq!(
        InitialHistory::Forked(full.clone()).get_history_mode(ThreadHistoryMode::Legacy),
        ThreadHistoryMode::Paginated
    );
    (store, full, bounded)
}

#[tokio::test]
async fn migration_preserves_assistant_delivery_and_source_revisions_after_rollback() {
    use codex_history::RetainedContextEntry;
    use codex_history::RetainedContextOrder;

    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let worker_source =
        SessionSource::SubAgent(codex_protocol::protocol::SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: None,
        });
    let assistant = |id: &str, turn_id: &str, text: &str, order: u64| ResponseItemEnvelope {
        item: serde_json::from_value(json!({
            "type": "message", "role": "assistant", "id": id,
            "content": [{"type": "output_text", "text": text}],
            "phase": "commentary",
            "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
        }))
        .expect("ordered assistant fixture"),
        metadata: Some(codex_history::CodexHarnessMetadata {
            user_input_order: Some(order),
            ..Default::default()
        }),
    };
    let mut inherited = assistant("parent-reply", "parent", "Parent context.", 90);
    inherited.metadata.as_mut().unwrap().inherited_user_message = true;
    let mut inherited_source = codex_history::record_retained_message(
        &mut codex_history::RetainedContext::default(),
        &inherited.item,
        inherited.metadata.as_ref(),
        codex_history::RetainedMessageSource::Original,
    )
    .expect("inherited assistant source");
    inherited_source.complete = false;
    inherited.metadata.as_mut().unwrap().retained_source = Some(inherited_source.clone());
    let mut kept = vec![
        inherited,
        retained_user_message("A", "Keep this private.", 0),
        assistant("reply-A", "A", "I will keep it private.", 1),
        ResponseItemEnvelope {
            item: serde_json::from_value(json!({
                "type": "function_call", "id": "call-A", "call_id": "send-A",
                "name": "send_message", "arguments": "{}",
                "internal_chat_message_metadata_passthrough": {"turn_id": "A"},
            }))
            .expect("original delivery call"),
            metadata: Some(codex_history::CodexHarnessMetadata {
                user_input_order: Some(2),
                ..Default::default()
            }),
        },
    ];
    let mut unsequenced = assistant("unsequenced", "A", "Legacy assistant context.", 0);
    unsequenced.metadata.as_mut().unwrap().user_input_order = None;
    kept.push(unsequenced);
    // These records were written before B, but were accepted after B's queued input.
    let mut queued = vec![
        assistant("queued-reply", "A", "Accepted after the steer.", 4),
        ResponseItemEnvelope {
            item: serde_json::from_value(json!({
                "type": "function_call", "id": "queued-call", "call_id": "send-queued",
                "name": "send_message", "arguments": "{}",
                "internal_chat_message_metadata_passthrough": {"turn_id": "A"},
            }))
            .expect("call accepted after the steer"),
            metadata: Some(codex_history::CodexHarnessMetadata {
                user_input_order: Some(5),
                ..Default::default()
            }),
        },
    ];
    let mut removed = vec![
        retained_user_message("B", "Temporary instruction.", 3),
        ResponseItemEnvelope {
            item: serde_json::from_value(json!({
                "type": "function_call_output", "call_id": "send-A", "output": "Sent.",
            }))
            .expect("delayed delivery output"),
            metadata: Some(codex_history::CodexHarnessMetadata {
                delivered_assistant_message: Some("Confirmed private reply.".to_owned()),
                ..Default::default()
            }),
        },
        assistant("reply-B", "B", "Temporary answer.", 6),
    ];
    let mut captured = ContextManager::for_session(&worker_source);
    let policy = turn_context.model_info().truncation_policy.into();
    captured.record_annotated_items(&mut kept, policy);
    captured.record_annotated_items(&mut queued, policy);
    captured.record_annotated_items(&mut removed, policy);
    let saved_sources = captured
        .retained_context()
        .ordered_entries()
        .filter_map(|(order, entry)| {
            matches!(order, RetainedContextOrder::Local(0..=2))
                .then(|| captured.retained_context().source(entry))
                .flatten()
        })
        .collect::<Vec<_>>();
    assert_eq!(saved_sources.len(), 3);
    assert_eq!(saved_sources[2].id.message_id, "call-A");
    let mut checkpoint = checkpoint_compacted(Vec::new());
    checkpoint.replacement_history = Some([kept.clone(), queued.clone(), removed.clone()].concat());
    // Confirmed deliveries are saved in retained_context, not on the output envelope.
    // Starting from this checkpoint checks their original revision as well as their order.
    checkpoint.retained_context = Some(captured.retained_context().clone());
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: Some("B".to_owned()),
        previous_turn_settings: None,
    });
    let mut items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: session.thread_id,
                session_id: session.thread_id.into(),
                timestamp: "2025-01-03T12:00:00Z".to_owned(),
                cwd: home.path().to_path_buf(),
                source: worker_source.clone(),
                history_mode: ThreadHistoryMode::Legacy,
                ..Default::default()
            },
            git: None,
        }),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(complete_thread_settings())),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: None,
        })),
        RolloutItem::ResponseItem(kept[0].clone()),
    ];
    let mut a_context = turn_context.to_turn_context_item();
    a_context.turn_id = Some("A".to_owned());
    items.extend(migration_user_turn_rollout(
        a_context,
        kept[1].clone(),
        kept[2..]
            .iter()
            .chain(&queued)
            .cloned()
            .map(RolloutItem::ResponseItem)
            .collect(),
    ));
    let mut b_context = turn_context.to_turn_context_item();
    b_context.turn_id = Some("B".to_owned());
    let mut b_suffix = removed[1..]
        .iter()
        .cloned()
        .map(RolloutItem::ResponseItem)
        .collect::<Vec<_>>();
    b_suffix.push(RolloutItem::Compacted(checkpoint));
    items.extend(migration_user_turn_rollout(
        b_context,
        removed[0].clone(),
        b_suffix,
    ));
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));
    let mut c_context = turn_context.to_turn_context_item();
    c_context.turn_id = Some("C".to_owned());
    items.extend(migration_user_turn_rollout(
        c_context,
        retained_user_message("C", "Later temporary instruction.", 7),
        vec![RolloutItem::ResponseItem(ResponseItemEnvelope {
            item: serde_json::from_value(json!({
                "type": "function_call_output", "call_id": "send-queued", "output": "Sent.",
            }))
            .expect("stale delivery after rollback"),
            metadata: Some(codex_history::CodexHarnessMetadata {
                delivered_assistant_message: Some(
                    "Must not resurrect removed evidence.".to_owned(),
                ),
                ..Default::default()
            }),
        })],
    ));
    // A stale call lookup would resurrect order 5 and survive the order-7 rollback.
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));
    let (_, full, bounded) =
        migrate_checkpoint_fixture(home.path(), session.thread_id, &items).await;
    for source in [worker_source, SessionSource::Cli] {
        let is_root = !source.is_non_root_agent();
        turn_context.session_source = source;
        let before = session
            .reconstruct_history_from_rollout(&turn_context, &items)
            .await;
        assert_eq!(before.history, kept);
        let retained = before
            .retained_context
            .ordered_entries()
            .map(|(order, entry)| {
                let (role, message) = match entry {
                    RetainedContextEntry::UserMessage(message) => ("user", message),
                    RetainedContextEntry::AssistantMessage(message) => ("assistant", message),
                    RetainedContextEntry::VerifiedAnswer(_) => panic!("unexpected answer"),
                };
                (
                    order,
                    role,
                    message.message_id.as_deref().unwrap(),
                    message.text.as_str(),
                )
            })
            .collect::<Vec<_>>();
        let mut expected = vec![
            (
                RetainedContextOrder::Local(0),
                "user",
                kept[1].item.id().unwrap().as_str(),
                "Keep this private.",
            ),
            (
                RetainedContextOrder::Local(1),
                "assistant",
                "reply-A",
                "I will keep it private.",
            ),
            (
                RetainedContextOrder::Local(2),
                "assistant",
                "call-A",
                "Confirmed private reply.",
            ),
        ];
        let mut expected_sources = saved_sources.clone();
        if is_root {
            expected.insert(
                0,
                (
                    RetainedContextOrder::Inherited(0),
                    "assistant",
                    "parent-reply",
                    "Parent context.",
                ),
            );
            expected_sources.insert(0, inherited_source.clone());
        }
        assert_eq!(retained, expected);
        assert_eq!(
            before
                .retained_context
                .ordered_entries()
                .filter_map(|(_, entry)| before.retained_context.source(entry))
                .collect::<Vec<_>>(),
            expected_sources,
        );
        for migrated in [&full, &bounded] {
            assert_eq!(
                session
                    .reconstruct_history_from_rollout(&turn_context, migrated)
                    .await,
                before,
                "migration changed assistant evidence for {:?}",
                turn_context.session_source,
            );
        }
    }
}

#[test_case(false, false, false; "populated checkpoint with one rollback")]
#[test_case(true, false, false; "empty checkpoint with one rollback")]
#[test_case(false, true, false; "populated checkpoint with repeated rollback")]
#[test_case(true, true, false; "empty checkpoint with repeated rollback")]
#[test_case(false, true, true; "populated checkpoint with newer C runtime")]
#[test_case(true, true, true; "empty checkpoint with newer C runtime")]
#[tokio::test]
async fn migration_preserves_modern_checkpoint_reconstruction_after_rollback(
    empty_resume_metadata: bool,
    append_and_rollback_c: bool,
    c_selects_v1: bool,
) {
    for identified_messages in [false, true] {
        assert_migration_checkpoint_reconstruction_after_rollback(
            empty_resume_metadata,
            append_and_rollback_c,
            c_selects_v1,
            identified_messages,
        )
        .await;
    }
}

async fn assert_migration_checkpoint_reconstruction_after_rollback(
    empty_resume_metadata: bool,
    append_and_rollback_c: bool,
    c_selects_v1: bool,
    identified_messages: bool,
) {
    use codex_thread_store::ThreadStore;

    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let thread_id = session.thread_id;
    let initial_window_id = Uuid::now_v7();
    let previous_window_id = Uuid::now_v7();
    let checkpoint_window_id = Uuid::now_v7();
    let meta = SessionMetaLine {
        meta: SessionMeta {
            id: thread_id,
            session_id: thread_id.into(),
            timestamp: "2025-01-03T12:00:00Z".to_string(),
            cwd: home.path().to_path_buf(),
            originator: "checkpoint-migration-test".to_string(),
            cli_version: "0.0.0".to_string(),
            source: SessionSource::Cli,
            model_provider: Some("test-provider".to_string()),
            history_mode: ThreadHistoryMode::Legacy,
            multi_agent_version: None,
            ..SessionMeta::default()
        },
        git: None,
    };
    let previous_settings = (!empty_resume_metadata).then(|| PreviousTurnSettings {
        model: "checkpoint-model".to_string(),
        comp_hash: Some("checkpoint-comparison-hash".to_string()),
        realtime_active: Some(true),
    });
    let mut expected_runtime =
        (!empty_resume_metadata).then_some(codex_protocol::protocol::MultiAgentVersion::V2);
    let a_user = if identified_messages {
        retained_user_message("A", "A", 0)
    } else {
        user_message("A").into()
    };
    let b_user = if identified_messages {
        retained_user_message("B", "B", 2)
    } else {
        user_message("B").into()
    };
    let mut kept = vec![
        a_user.clone(),
        ResponseItemEnvelope {
            item: assistant_message("answer A"),
            metadata: identified_messages.then(codex_history::CodexHarnessMetadata::default),
        },
    ];
    let mut replacement = kept.clone();
    replacement.extend([
        b_user.clone(),
        ResponseItemEnvelope {
            item: assistant_message("answer B"),
            metadata: identified_messages.then(codex_history::CodexHarnessMetadata::default),
        },
    ]);
    let mut checkpoint = checkpoint_compacted(Vec::new());
    let mut expected_retained = codex_history::RetainedContext::default();
    if identified_messages {
        let mut captured = ContextManager::for_session(&turn_context.session_source);
        captured.record_annotated_items(
            &mut kept,
            turn_context.model_info().truncation_policy.into(),
        );
        replacement[..kept.len()].clone_from_slice(&kept);
        captured.record_retained_context(&retained_answer("A", 1));
        expected_retained = captured.retained_context().clone();
        captured.record_annotated_items(
            &mut replacement[kept.len()..],
            turn_context.model_info().truncation_policy.into(),
        );
        captured.record_retained_context(&retained_answer("B", 3));
        assert!(captured.retained_context().user_messages_complete());
        assert!(captured.retained_context().verified_answers_complete());
        checkpoint.retained_context = Some(captured.retained_context().clone());
        expected_retained.reserve_order();
        expected_retained.reserve_order();
        if append_and_rollback_c {
            expected_retained.reserve_order();
        }
    } else {
        checkpoint.guardian_history = Some(codex_history::GuardianHistoryCheckpoint(
            replacement.iter().map(|item| item.item.clone()).collect(),
        ));
        expected_retained.restore(None, &kept);
        if append_and_rollback_c {
            // Accepted historical messages can lack every rollback identity. Preserve
            // their incomplete evidence exactly instead of manufacturing new identities.
            expected_retained.record_user_message(
                codex_history::RetainedUserMessage {
                    turn_id: String::new(),
                    message_id: None,
                    text: "C".to_string(),
                    complete: false,
                    origin: Default::default(),
                    phase: None,
                },
                codex_history::RetainedInputSource::Local(None),
            );
        }
    }
    checkpoint.replacement_history = Some(replacement.clone());
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: expected_runtime,
        last_started_turn_id: (!empty_resume_metadata).then(|| "B".to_string()),
        previous_turn_settings: previous_settings.clone(),
    });
    checkpoint.window_number = Some(8);
    checkpoint.first_window_id = Some(initial_window_id.to_string());
    checkpoint.previous_window_id = Some(previous_window_id.to_string());
    checkpoint.window_id = Some(checkpoint_window_id.to_string());
    let mut usage = codex_protocol::protocol::TokenUsageRecord {
        thread_id,
        session_id: thread_id.into(),
        turn_id: "B".to_string(),
        root_turn_id: "B".to_string(),
        response_id: "checkpoint-response".to_string(),
        usage: codex_protocol::protocol::TokenUsage {
            total_tokens: 420,
            ..Default::default()
        },
        turn_token_usage: Default::default(),
        thread_token_usage: Default::default(),
    };
    checkpoint.compaction_response_id = Some(usage.response_id.clone());
    checkpoint.latest_token_usage_record = Some(usage.clone());
    let mut settings = complete_thread_settings();
    settings.thread_settings.model = "sticky-before-A-420".to_string();
    settings.thread_settings.collaboration_mode.settings.model =
        settings.thread_settings.model.clone();
    let mut token_count = TokenCountEvent {
        info: Some(codex_protocol::protocol::TokenUsageInfo {
            total_token_usage: codex_protocol::protocol::TokenUsage {
                total_tokens: 420,
                ..Default::default()
            },
            last_token_usage: Default::default(),
            model_context_window: Some(100_000),
        }),
        rate_limits: None,
    };
    let mut items = vec![
        RolloutItem::SessionMeta(meta),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(settings.clone())),
        RolloutItem::EventMsg(EventMsg::TokenCount(token_count.clone())),
    ];
    let mut a_context = turn_context.to_turn_context_item();
    a_context.turn_id = Some("A".to_string());
    a_context.model = "older-model-must-not-be-restored".to_string();
    a_context.comp_hash = Some("older-settings-must-not-be-restored".to_string());
    a_context.multi_agent_version = Some(codex_protocol::protocol::MultiAgentVersion::V1);
    items.extend(migration_user_turn_rollout(
        a_context,
        kept[0].clone(),
        vec![RolloutItem::ResponseItem(
            assistant_message("answer A").into(),
        )],
    ));
    let mut b_context = turn_context.to_turn_context_item();
    b_context.turn_id = Some("B".to_string());
    b_context.model = "pre-compaction-B-model".to_string();
    b_context.multi_agent_version = Some(codex_protocol::protocol::MultiAgentVersion::V1);
    settings.thread_settings.model = "sticky-post-checkpoint-B-840".to_string();
    settings.thread_settings.collaboration_mode.settings.model =
        settings.thread_settings.model.clone();
    token_count
        .info
        .as_mut()
        .expect("token info")
        .total_token_usage
        .total_tokens = 840;
    usage.response_id = "post-checkpoint-B-response".to_string();
    usage.usage.total_tokens = 840;
    items.extend(migration_user_turn_rollout(
        b_context,
        replacement[kept.len()].clone(),
        vec![
            RolloutItem::Compacted(checkpoint),
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(settings.clone())),
            RolloutItem::EventMsg(EventMsg::TokenCount(token_count.clone())),
            RolloutItem::TokenUsageRecord(usage.clone()),
        ],
    ));
    let rollback = RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
        num_turns: 1,
    }));
    items.push(rollback.clone());
    if append_and_rollback_c {
        let mut c_context = turn_context.to_turn_context_item();
        c_context.turn_id = Some("C".to_string());
        c_context.model = "rolled-back-C-model".to_string();
        // Runtime selection has an independent reducer; rolling back C does not undo its V1.
        c_context.multi_agent_version =
            c_selects_v1.then_some(codex_protocol::protocol::MultiAgentVersion::V1);
        if c_selects_v1 {
            expected_runtime = Some(codex_protocol::protocol::MultiAgentVersion::V1);
        }
        settings.thread_settings.model = "sticky-rolled-back-C-1260".to_string();
        settings.thread_settings.collaboration_mode.settings.model =
            settings.thread_settings.model.clone();
        token_count
            .info
            .as_mut()
            .expect("token info")
            .total_token_usage
            .total_tokens = 1260;
        usage.turn_id = "C".to_string();
        usage.root_turn_id = "C".to_string();
        usage.response_id = "rolled-back-C-response".to_string();
        usage.usage.total_tokens = 1260;
        items.extend(migration_user_turn_rollout(
            c_context,
            if identified_messages {
                retained_user_message("C", "C", 4)
            } else {
                user_message("C").into()
            },
            vec![
                RolloutItem::ResponseItem(assistant_message("answer C").into()),
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(settings.clone())),
                RolloutItem::EventMsg(EventMsg::TokenCount(token_count.clone())),
                RolloutItem::TokenUsageRecord(usage.clone()),
            ],
        ));
        items.push(rollback);
    }

    let before = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(
        before.guardian_history,
        (!identified_messages).then(|| codex_history::GuardianHistoryCheckpoint(
            kept.iter().map(|item| item.item.clone()).collect(),
        )),
        "a root retains the backup when checkpoint metadata omitted user evidence",
    );
    assert_eq!(before.retained_context, expected_retained);
    assert_eq!(before.history, kept);
    assert_eq!(before.previous_turn_settings, previous_settings);
    assert_eq!(
        before.last_started_turn_id.as_deref(),
        if append_and_rollback_c {
            Some("C")
        } else if empty_resume_metadata {
            None
        } else {
            Some("B")
        }
    );
    assert_eq!(before.reference_context_item, None);
    assert_eq!(before.world_state_baseline, None);
    assert_eq!(before.window_number, 8);
    assert_eq!(before.first_window_id, Some(initial_window_id));
    assert_eq!(before.previous_window_id, Some(previous_window_id));
    assert_eq!(before.window_id, Some(checkpoint_window_id));
    assert_eq!(
        InitialHistory::Forked(items.clone()).get_multi_agent_version(),
        expected_runtime
    );
    assert_eq!(
        serde_json::to_value(Session::restored_token_count_from_rollout(&items))
            .expect("serialize restored token count"),
        serde_json::to_value(Some(&token_count)).expect("serialize expected token count")
    );
    assert_eq!(
        Session::last_token_usage_record_from_rollout(&items),
        Some(usage.clone())
    );
    assert_eq!(
        items.iter().rev().find_map(|item| match item {
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(value)) => Some(value),
            _ => None,
        }),
        Some(&settings)
    );

    let (store, full, bounded) = migrate_checkpoint_fixture(home.path(), thread_id, &items).await;
    let repeated = store
        .migrate_rollouts(codex_thread_store::RolloutMigrationOptions {
            mode: codex_thread_store::RolloutMigrationMode::Apply,
            thread_ids: vec![thread_id],
            max_mib_per_second: None,
        })
        .await
        .expect("repeat migration of synthetic checkpoints");
    assert_eq!(repeated.outcomes.len(), 1);
    assert_eq!(
        repeated.outcomes[0].status,
        codex_thread_store::RolloutMigrationStatus::AlreadyPaginated
    );
    // Compare persisted records; `full` also contains a derived selected-review cache.
    assert_eq!(
        serde_json::to_value(
            store
                .load_history(codex_thread_store::LoadThreadHistoryParams {
                    thread_id,
                    include_archived: false,
                })
                .await
                .expect("read checkpoints after repeated migration")
                .items,
        )
        .expect("serialize checkpoints after repeated migration"),
        serde_json::to_value(&full).expect("serialize first migration")
    );
    let state_checkpoints = full
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Compacted(compacted)
                if compacted.message.is_empty()
                    && compacted.resume_metadata.is_some()
                    && compacted.compaction_response_id.is_none()
                    && compacted.segment_state_checkpoint.is_none() =>
            {
                Some(compacted)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        state_checkpoints.len(),
        if append_and_rollback_c { 2 } else { 1 },
        "each accepted rollback must emit one state-only checkpoint at its source position"
    );
    let rollback_indices = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            matches!(item, RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_))).then_some(index)
        })
        .collect::<Vec<_>>();
    let checkpoint_indices = full
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            matches!(item, RolloutItem::Compacted(compacted)
            if compacted.retained_context_replay.is_some())
            .then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(rollback_indices.len(), checkpoint_indices.len());
    for (original_end, migrated_end) in rollback_indices.into_iter().zip(checkpoint_indices) {
        let mut migrated_prefix = full[..=migrated_end].to_vec();
        super::rollout_reconstruction::resolve_review_input_for_reconstruction(
            home.path(),
            &mut migrated_prefix,
        )
        .await
        .expect("resolve the finite input prefix for this checkpoint");
        let original = session
            .reconstruct_history_from_rollout(&turn_context, &items[..=original_end])
            .await;
        let migrated = session
            .reconstruct_history_from_rollout(&turn_context, &migrated_prefix)
            .await;
        assert_eq!(
            migrated, original,
            "finite checkpoint changed reconstruction"
        );
    }
    let turns = store
        .list_turns(codex_thread_store::ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 25,
            sort_direction: codex_thread_store::SortDirection::Asc,
            items_view: codex_thread_store::StoredTurnItemsView::Summary,
        })
        .await
        .expect("read migrated visible turns");
    assert!(turns.next_cursor.is_none());
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["A"],
        "state-only rollback checkpoints must not add visible turns"
    );
    for (reader, migrated) in [("full history", full), ("bounded model context", bounded)] {
        let after = session
            .reconstruct_history_from_rollout(&turn_context, &migrated)
            .await;
        assert_eq!(
            after, before,
            "{reader}: migration changed core reconstruction"
        );
        assert_eq!(
            InitialHistory::Forked(migrated.clone()).get_multi_agent_version(),
            expected_runtime,
            "{reader}: migration changed the selected runtime"
        );
        assert_eq!(
            serde_json::to_value(Session::restored_token_count_from_rollout(&migrated))
                .expect("serialize restored token count"),
            serde_json::to_value(Some(&token_count)).expect("serialize expected token count"),
            "{reader}"
        );
        assert_eq!(
            Session::last_token_usage_record_from_rollout(&migrated),
            Some(usage.clone()),
            "{reader}"
        );
        assert_eq!(
            migrated.iter().rev().find_map(|item| match item {
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(value)) => Some(value),
                _ => None,
            }),
            Some(&settings),
            "{reader}: migration changed sticky thread settings"
        );
    }
}

#[tokio::test]
async fn migration_preserves_retained_eviction_before_rollback() {
    assert_migration_preserves_retained_eviction_before_rollback(None).await;
}

#[test_case(false; "initially complete")]
#[test_case(true; "initially incomplete")]
#[tokio::test]
async fn migration_preserves_original_root_transcript_decision_after_retained_eviction(
    initially_incomplete: bool,
) {
    assert_migration_preserves_retained_eviction_before_rollback(Some(initially_incomplete)).await;
}

async fn assert_migration_preserves_retained_eviction_before_rollback(
    initially_incomplete: Option<bool>,
) {
    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let mut replacement = (0..8)
        .map(|index| retained_user_message(&format!("kept-{index}"), "Keep this private.", index))
        .collect::<Vec<_>>();
    let mut captured = ContextManager::for_session(&turn_context.session_source);
    captured.record_annotated_items(
        &mut replacement,
        turn_context.model_info().truncation_policy.into(),
    );
    let mut surviving = ContextManager::for_session(&turn_context.session_source);
    for envelope in &replacement[2..] {
        surviving
            .replay_annotated_item(envelope, turn_context.model_info().truncation_policy.into());
    }
    let mut expected = surviving.retained_context().clone();
    expected.reserve_order();
    expected.reserve_order();
    expected.mark_user_messages_incomplete();
    let mut checkpoint = checkpoint_compacted(Vec::new());
    checkpoint.replacement_history = Some(replacement.clone());
    let mut retained = captured.retained_context().clone();
    if initially_incomplete == Some(true) {
        retained.mark_user_messages_incomplete();
    }
    checkpoint.retained_context = Some(retained);
    let guardian_baseline = initially_incomplete.map(|_| {
        // Both cases end with the same retained messages and missing flag. Only
        // the original missing flag decides whether this unmatched instruction survives.
        let mut messages = vec![user_message("Keep the earlier project confidential.")];
        messages.extend(replacement.iter().map(|envelope| envelope.item.clone()));
        codex_history::GuardianHistoryCheckpoint(messages)
    });
    checkpoint.guardian_history = guardian_baseline.clone();
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: Some("kept-7".to_string()),
        previous_turn_settings: None,
    });
    let mut items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: session.thread_id,
                session_id: session.thread_id.into(),
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                cwd: home.path().to_path_buf(),
                source: SessionSource::Cli,
                history_mode: ThreadHistoryMode::Legacy,
                ..Default::default()
            },
            git: None,
        }),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(complete_thread_settings())),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: None,
        })),
    ];
    for (index, user) in replacement.iter().enumerate() {
        let mut context = turn_context.to_turn_context_item();
        context.turn_id = Some(format!("kept-{index}"));
        items.extend(migration_user_turn_rollout(
            context,
            user.clone(),
            Vec::new(),
        ));
    }
    items.push(RolloutItem::Compacted(checkpoint));
    for (turn_id, order) in [("C", 8), ("D", 9)] {
        let mut context = turn_context.to_turn_context_item();
        context.turn_id = Some(turn_id.to_string());
        items.extend(migration_user_turn_rollout(
            context,
            retained_user_message(turn_id, turn_id, order),
            Vec::new(),
        ));
    }
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 2 },
    )));

    let (_, full, bounded) =
        migrate_checkpoint_fixture(home.path(), session.thread_id, &items).await;
    let worker_source =
        SessionSource::SubAgent(codex_protocol::protocol::SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: None,
        });
    for source in [turn_context.session_source.clone(), worker_source] {
        let is_root = !source.is_non_root_agent();
        turn_context.session_source = source;
        let before = session
            .reconstruct_history_from_rollout(&turn_context, &items)
            .await;
        assert_eq!(before.history, replacement);
        assert_eq!(before.retained_context, expected);
        assert!(!before.retained_context.user_messages_complete());
        assert_eq!(
            before.guardian_history,
            if is_root && initially_incomplete == Some(true) {
                guardian_baseline.clone()
            } else {
                None
            }
        );
        for migrated in [&full, &bounded] {
            if let Some(initially_incomplete) = initially_incomplete {
                let baseline_decision = migrated.iter().find_map(|item| {
                    let RolloutItem::Compacted(checkpoint) = item else {
                        return None;
                    };
                    checkpoint
                        .retained_context_replay
                        .as_ref()?
                        .resolved_review_input
                        .as_ref()?
                        .iter()
                        .find_map(|record| match record {
                            codex_history::ReviewInputRecord::Baseline {
                                root_retains_legacy_transcript,
                                ..
                            } => Some(*root_retains_legacy_transcript),
                            _ => None,
                        })
                });
                assert_eq!(baseline_decision, Some(Some(initially_incomplete)));
            }
            let after = session
                .reconstruct_history_from_rollout(&turn_context, migrated)
                .await;
            assert_eq!(after, before);
        }
    }
}

#[tokio::test]
async fn migration_preserves_post_completion_checkpoint_reconstruction() {
    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let worker_source =
        SessionSource::SubAgent(codex_protocol::protocol::SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: None,
        });
    let mut inherited = ResponseItemEnvelope::new(user_message("Keep the project private."));
    inherited.item.set_turn_id_if_missing("parent-turn");
    inherited.metadata = Some(codex_history::CodexHarnessMetadata {
        inherited_user_message: true,
        ..Default::default()
    });
    let mut temporary = ResponseItemEnvelope::new(user_message("Temporary local instruction."));
    temporary.item.set_turn_id_if_missing("temporary-turn");
    temporary.metadata = Some(codex_history::CodexHarnessMetadata {
        user_input_order: Some(0),
        ..Default::default()
    });
    let mut checkpoint = checkpoint_compacted(Vec::new());
    checkpoint.message = "checkpoint".to_string();
    checkpoint.replacement_history = Some(vec![inherited.clone()]);
    checkpoint.retained_context = Some(Default::default());
    checkpoint.window_number = Some(1);
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    assert!(checkpoint.segment_state_checkpoint.is_none());
    let started = |turn_id: &str| {
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_id: turn_id.to_string(),
                root_turn_id: None,
                trace_id: None,
                started_at: None,
                model_context_window: Some(128_000),
                collaboration_mode_kind: ModeKind::Default,
            },
        ))
    };
    let completed = |turn_id: &str| {
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                turn_id: turn_id.to_string(),
                last_agent_message: None,
                error: None,
                started_at: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            },
        ))
    };
    let items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: session.thread_id,
                session_id: session.thread_id.into(),
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                cwd: home.path().to_path_buf(),
                source: worker_source.clone(),
                history_mode: ThreadHistoryMode::Legacy,
                ..Default::default()
            },
            git: None,
        }),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(complete_thread_settings())),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: None,
        })),
        started("parent-turn"),
        RolloutItem::ResponseItem(inherited.clone()),
        completed("parent-turn"),
        RolloutItem::Compacted(checkpoint),
        started("temporary-turn"),
        RolloutItem::ResponseItem(temporary),
        completed("temporary-turn"),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
            num_turns: 1,
        })),
    ];
    let (_, full, bounded) =
        migrate_checkpoint_fixture(home.path(), session.thread_id, &items).await;
    for source in [worker_source, SessionSource::Cli] {
        turn_context.session_source = source;
        let original = session
            .reconstruct_history_from_rollout(&turn_context, &items)
            .await;
        assert_eq!(original.history, vec![inherited.clone()]);
        assert_eq!(
            original.last_started_turn_id.as_deref(),
            Some("temporary-turn")
        );
        assert_eq!(original.previous_turn_settings, None);
        assert_eq!(original.reference_context_item, None);
        assert_eq!(original.guardian_history, None);
        assert_eq!(original.window_number, 1);
        for migrated in [&full, &bounded] {
            assert_eq!(
                session
                    .reconstruct_history_from_rollout(&turn_context, migrated)
                    .await,
                original,
                "post-completion checkpoint changed reconstruction for {:?}",
                turn_context.session_source,
            );
        }
    }
}

#[test_case(8, 1; "preserves partial inherited eviction")]
#[test_case(1, 8; "does not readopt fully evicted inherited prefix")]
#[tokio::test]
async fn migration_preserves_worker_checkpoint_after_root_fork(
    inherited_count: u64,
    local_count: u64,
) {
    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let worker_path = AgentPath::root().join("worker").expect("worker path");
    let worker_source =
        SessionSource::SubAgent(codex_protocol::protocol::SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: Some(worker_path.clone()),
            agent_nickname: None,
            agent_role: None,
        });
    let inherited = (0..inherited_count)
        .map(|index| {
            let mut message = retained_user_message(
                &format!("kept-{index}"),
                &format!("Keep restriction {index}."),
                index,
            );
            let metadata = message.metadata.as_mut().expect("input metadata");
            metadata.inherited_user_message = true;
            // This checkpoint already retained an incomplete parent copy. Its saved
            // revision must survive adoption without claiming original completeness.
            metadata.retained_source = Some(codex_history::RetainedSource {
                id: codex_history::RetainedSourceId {
                    message_id: message.item.id().expect("inherited identity").to_string(),
                    turn_id: format!("kept-{index}"),
                    role: codex_history::RetainedSourceRole::User,
                },
                revision: codex_protocol::ResponseItemId::with_suffix(
                    "retained",
                    format!("kept-{index}"),
                ),
                complete: false,
            });
            message
        })
        .collect::<Vec<_>>();
    let assignment = InterAgentCommunication::new(
        AgentPath::root(),
        worker_path,
        Vec::new(),
        "Perform the delegated task.".to_string(),
        /*trigger_turn*/ true,
    );
    let mut replacement = inherited.clone();
    replacement.push(ResponseItemEnvelope {
        item: assignment.to_model_input_item(),
        metadata: Some(codex_history::CodexHarnessMetadata::default()),
    });
    let mut worker_retained = codex_history::RetainedContext::default();
    worker_retained.reserve_order();
    let mut checkpoint = checkpoint_compacted(Vec::new());
    checkpoint.replacement_history = Some(replacement.clone());
    checkpoint.retained_context = Some(worker_retained);
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: Some("worker-task".to_string()),
        previous_turn_settings: None,
    });
    let mut items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: session.thread_id,
                session_id: session.thread_id.into(),
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                cwd: home.path().to_path_buf(),
                source: worker_source.clone(),
                history_mode: ThreadHistoryMode::Legacy,
                ..Default::default()
            },
            git: None,
        }),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(complete_thread_settings())),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: None,
        })),
    ];
    for (index, user) in inherited.iter().enumerate() {
        let mut context = turn_context.to_turn_context_item();
        context.turn_id = Some(format!("kept-{index}"));
        items.extend(migration_user_turn_rollout(
            context,
            user.clone(),
            Vec::new(),
        ));
    }
    items.extend([
        RolloutItem::InterAgentCommunicationMetadata { trigger_turn: true },
        RolloutItem::ResponseItem(assignment.to_model_input_item().into()),
    ]);
    // The worker's own compaction follows its assignment. No later task delivery
    // prevents this ordinary user suffix from entering modern rollback migration.
    items.push(RolloutItem::Compacted(checkpoint));
    for index in 0..local_count {
        let turn_id = format!("local-{index}");
        let mut context = turn_context.to_turn_context_item();
        context.turn_id = Some(turn_id.clone());
        items.extend(migration_user_turn_rollout(
            context,
            retained_user_message(&turn_id, "Temporary worker input.", index + 1),
            Vec::new(),
        ));
    }
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent {
            num_turns: u32::try_from(local_count).expect("small rollback count"),
        },
    )));
    let (_, full, bounded) =
        migrate_checkpoint_fixture(home.path(), session.thread_id, &items).await;
    for source in [worker_source.clone(), SessionSource::Cli] {
        let root_thread_owned = !source.is_non_root_agent();
        turn_context.session_source = source;
        let before = session
            .reconstruct_history_from_rollout(&turn_context, &items)
            .await;
        let expected_messages = if root_thread_owned {
            inherited
                    .iter()
                    .enumerate()
                    .skip(usize::try_from(local_count).expect("small local input count"))
                    .map(|(index, envelope)| {
                        json!({
                            "inherited": true, "order": index,
                            "turn_id": format!("kept-{index}"),
                            "message_id": envelope.item.id().expect("inherited identity"),
                            "revision": envelope.metadata.as_ref().unwrap().retained_source.as_ref().unwrap().revision,
                            "text": format!("Keep restriction {index}."),
                            "complete": false,
                        })
                    })
                    .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        assert_eq!(before.history, replacement);
        assert_eq!(
            serde_json::to_value(&before.retained_context).expect("serialize retained state"),
            json!({
                "verified_answers": [], "incomplete": root_thread_owned,
                "user_messages": expected_messages,
                "user_messages_incomplete": root_thread_owned,
                "assistant_messages": [], "assistant_messages_incomplete": false,
                "next_order": local_count + 1,
            }),
        );
        for migrated in [&full, &bounded] {
            let after = session
                .reconstruct_history_from_rollout(&turn_context, migrated)
                .await;
            assert_eq!(after, before);

            let (resumed_session, _) = make_session_and_context().await;
            {
                let mut state = resumed_session.state.lock().await;
                state.session_configuration.session_source = turn_context.session_source.clone();
                state.history = ContextManager::for_session(&turn_context.session_source);
            }
            resumed_session
                .record_initial_history(InitialHistory::Resumed(ResumedHistory {
                    conversation_id: resumed_session.thread_id,
                    history: Arc::new(migrated.clone()),
                    rollout_path: None,
                }))
                .await
                .expect("install the migrated retained state");
            assert_eq!(
                resumed_session.clone_history().await.retained_context(),
                &before.retained_context,
            );
        }
    }
}

#[test_case(None; "unknown producer hash")]
#[test_case(Some("producer-hash"); "known producer hash")]
#[tokio::test]
async fn migration_preserves_opaque_checkpoint_reviewer_fallback(producer_hash: Option<&str>) {
    use codex_thread_store::ThreadStore;

    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let opaque = ResponseItem::Compaction {
        id: Some(codex_protocol::ResponseItemId::with_suffix("cmp", "opaque")),
        encrypted_content: "opaque".to_string(),
        internal_chat_message_metadata_passthrough: None,
    };
    let replacement = vec![ResponseItemEnvelope {
        item: opaque.clone(),
        metadata: producer_hash.map(|hash| codex_history::CodexHarnessMetadata {
            compaction_model_hash: Some(hash.to_string()),
            ..Default::default()
        }),
    }];
    let mut checkpoint = checkpoint_compacted(Vec::new());
    checkpoint.replacement_history = Some(replacement.clone());
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    let mut items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: session.thread_id,
                session_id: session.thread_id.into(),
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                cwd: home.path().to_path_buf(),
                source: SessionSource::Cli,
                history_mode: ThreadHistoryMode::Legacy,
                ..Default::default()
            },
            git: None,
        }),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(complete_thread_settings())),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: None,
        })),
        RolloutItem::Compacted(checkpoint),
    ];
    let mut context = turn_context.to_turn_context_item();
    context.turn_id = Some("C".to_string());
    items.extend(migration_user_turn_rollout(
        context,
        user_message("C").into(),
        Vec::new(),
    ));
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));
    let (store, full, bounded) =
        migrate_checkpoint_fixture(home.path(), session.thread_id, &items).await;
    let report = store
        .migrate_rollouts(codex_thread_store::RolloutMigrationOptions {
            mode: codex_thread_store::RolloutMigrationMode::Apply,
            thread_ids: vec![session.thread_id],
            max_mib_per_second: None,
        })
        .await
        .expect("repeat migration");
    assert_eq!(
        report.outcomes[0].status,
        codex_thread_store::RolloutMigrationStatus::AlreadyPaginated,
    );
    let mut repeated = store
        .load_history(codex_thread_store::LoadThreadHistoryParams {
            thread_id: session.thread_id,
            include_archived: false,
        })
        .await
        .expect("read repeated migration")
        .items;
    super::rollout_reconstruction::resolve_review_input_for_reconstruction(
        home.path(),
        &mut repeated,
    )
    .await
    .expect("resolve repeated migration review input");
    assert_eq!(
        serde_json::to_value(&repeated).expect("serialize repeated migration"),
        serde_json::to_value(&full).expect("serialize first migration"),
    );
    let fallback = codex_history::GuardianHistoryCheckpoint(vec![opaque]);
    let before = session
        .reconstruct_history_from_rollout(&turn_context, &items)
        .await;
    assert_eq!(before.history, replacement);
    assert_eq!(before.guardian_history, Some(fallback.clone()),);
    assert_eq!(
        serde_json::to_value(&before.retained_context).expect("serialize retained context"),
        json!({
            "verified_answers": [], "incomplete": false,
            "user_messages": [{
                "order": 0, "turn_id": "", "message_id": null,
                "text": "C", "complete": false,
            }],
            "user_messages_incomplete": true,
            "assistant_messages": [], "assistant_messages_incomplete": false,
            "next_order": 1,
        }),
    );
    for reviewer_hash in [None, Some("producer-hash"), Some("other-hash")] {
        let uses_parent_context = producer_hash.is_some() && producer_hash == reviewer_hash;
        for source_items in [&items, &full, &bounded, &repeated] {
            let reconstructed = session
                .reconstruct_history_from_rollout(&turn_context, source_items)
                .await;
            assert_eq!(reconstructed, before);
            let (installed, _) = make_session_and_context().await;
            let mut state = installed.state.lock().await;
            state.history = ContextManager::for_session(&turn_context.session_source);
            assert_eq!(
                Session::install_rollout_reconstruction_in_state(
                    &mut state,
                    reconstructed,
                    /*shared_model_response_items*/ None,
                    /*shared_model_state*/ None,
                    reviewer_hash,
                ),
                None,
            );
            assert_eq!(state.history.annotated_items(), &replacement);
            assert_eq!(state.history.retained_context(), &before.retained_context);
            assert_eq!(
                state
                    .history
                    .conversation_history_snapshot()
                    .uses_parent_context_for_review(),
                uses_parent_context,
            );
            assert_eq!(
                state.history.guardian_history_checkpoint(),
                (!uses_parent_context).then(|| fallback.clone()),
            );
        }
    }
}

#[tokio::test]
async fn reconstruction_preserves_legacy_only_review_input_applicability() {
    use std::io::Write;

    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    let baseline = user_message("A");
    let removed = user_message("C");
    let opaque = ResponseItem::Compaction {
        id: Some(codex_protocol::ResponseItemId::with_suffix("cmp", "later")),
        encrypted_content: "later opaque response".to_string(),
        internal_chat_message_metadata_passthrough: None,
    };
    let mut checkpoint = checkpoint_compacted(vec![baseline.clone()]);
    checkpoint.guardian_history = Some(codex_history::GuardianHistoryCheckpoint(vec![
        baseline.clone(),
    ]));
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    let mut items = vec![
        RolloutItem::Compacted(checkpoint.clone()),
        RolloutItem::ResponseItem(opaque.clone().into()),
    ];
    let mut context = turn_context.to_turn_context_item();
    context.turn_id = Some("C".to_string());
    items.extend(migration_user_turn_rollout(
        context,
        removed.clone().into(),
        Vec::new(),
    ));
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));

    let records = vec![
        codex_history::ReviewInputRecord::Baseline {
            applicability: codex_history::ReviewTranscriptApplicability::Legacy,
            root_retains_legacy_transcript: Some(true),
            history: codex_history::GuardianHistoryCheckpoint(vec![baseline.clone()]),
        },
        codex_history::ReviewInputRecord::ResponseItem {
            response: opaque.clone().into(),
        },
        codex_history::ReviewInputRecord::ResponseItem {
            response: removed.clone().into(),
        },
        codex_history::ReviewInputRecord::Rollback { boundary: removed },
    ];
    let segment_id = ThreadId::new();
    let segment_path = codex_rollout::review_input_segment_path(home.path(), segment_id);
    std::fs::create_dir_all(segment_path.parent().expect("review segment directory"))
        .expect("create review segment directory");
    let mut segment = std::fs::File::create(segment_path).expect("create review segment");
    let header = codex_rollout::RolloutLine {
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        ordinal: Some(0),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: segment_id,
                session_id: segment_id.into(),
                segment_id: Some(
                    codex_protocol::SegmentId::from_string(&segment_id.to_string())
                        .expect("review segment ID"),
                ),
                history_mode: ThreadHistoryMode::Paginated,
                ..Default::default()
            },
            git: None,
        }),
    };
    serde_json::to_writer(&mut segment, &header).expect("serialize review segment header");
    segment
        .write_all(b"\n")
        .expect("terminate review segment header");
    for (index, record) in records.iter().enumerate() {
        serde_json::to_writer(
            &mut segment,
            &json!({"ordinal": index + 1, "record": record}),
        )
        .expect("serialize review input");
        segment.write_all(b"\n").expect("terminate review input");
    }
    let position = codex_protocol::protocol::HistoryPosition {
        thread_id: segment_id,
        end_ordinal_exclusive: u64::try_from(records.len() + 1).expect("review endpoint ordinal"),
        end_byte_offset: segment.metadata().expect("review segment metadata").len(),
    };
    drop(segment);

    let legacy: codex_history::RetainedContext = serde_json::from_value(json!({
        "verified_answers": [], "incomplete": false,
        "user_messages": [], "user_messages_incomplete": true, "next_order": 0,
    }))
    .expect("Legacy retained state");
    let thread_owned: codex_history::RetainedContext = serde_json::from_value(json!({
        "verified_answers": [], "incomplete": false,
        "user_messages": [{"order": 0, "turn_id": "", "message_id": null,
            "text": "C", "complete": false}],
        "user_messages_incomplete": true, "next_order": 1,
    }))
    .expect("ThreadOwned retained state");
    let expected_history = vec![baseline, opaque];
    let expected_guardian = codex_history::GuardianHistoryCheckpoint(expected_history.clone());
    checkpoint.replacement_history = Some(annotated(expected_history.clone()));
    checkpoint.guardian_history = Some(expected_guardian.clone());
    checkpoint.retained_context = Some(thread_owned.clone());
    checkpoint
        .resume_metadata
        .as_mut()
        .unwrap()
        .last_started_turn_id = Some("C".to_string());
    checkpoint.retained_context_replay = Some(codex_history::RetainedContextReplay {
        legacy: legacy.clone(),
        thread_owned_worker: thread_owned.clone(),
        thread_owned_root: thread_owned.clone(),
        review_input: Some(position),
        resolved_review_input: None,
    });
    let mut inferred = ContextManager::for_session(&turn_context.session_source);
    inferred.replace_annotated(checkpoint.replacement_history.clone().unwrap());
    inferred.restore_replayed_review_context(&thread_owned, Some(&expected_guardian), None);
    // Without explicitly restoring None, the final opaque model activates this fallback.
    assert_eq!(
        inferred.guardian_history_checkpoint(),
        Some(expected_guardian.clone())
    );

    let mut replayed_items = vec![RolloutItem::Compacted(checkpoint)];
    let wire = serde_json::to_value(&replayed_items).expect("serialize unresolved checkpoint");
    super::rollout_reconstruction::resolve_review_input_for_reconstruction(
        home.path(),
        &mut replayed_items,
    )
    .await
    .expect("resolve selected checkpoint review input");
    assert_eq!(serde_json::to_value(&replayed_items).unwrap(), wire);
    let RolloutItem::Compacted(checkpoint) = &replayed_items[0] else {
        unreachable!()
    };
    assert_eq!(
        checkpoint
            .retained_context_replay
            .as_ref()
            .unwrap()
            .resolved_review_input
            .as_ref()
            .unwrap()
            .as_ref(),
        &records
    );
    let worker_source =
        SessionSource::SubAgent(codex_protocol::protocol::SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: None,
        });
    for source in [turn_context.session_source.clone(), worker_source] {
        let is_root = !source.is_non_root_agent();
        turn_context.session_source = source;
        let original = session
            .reconstruct_history_from_rollout(&turn_context, &items)
            .await;
        assert_eq!(
            original
                .history
                .iter()
                .map(|item| item.item.clone())
                .collect::<Vec<_>>(),
            expected_history
        );
        assert_eq!(
            original.guardian_history,
            is_root.then(|| expected_guardian.clone())
        );
        assert_eq!(original.last_started_turn_id.as_deref(), Some("C"));
        assert_eq!(original.retained_context, thread_owned);
        assert_eq!(
            session
                .reconstruct_history_from_rollout(&turn_context, &replayed_items)
                .await,
            original,
            "legacy review input must preserve missing root instructions without adopting them for workers",
        );
    }

    let RolloutItem::Compacted(checkpoint) = &mut replayed_items[0] else {
        unreachable!()
    };
    let records = Arc::make_mut(
        checkpoint
            .retained_context_replay
            .as_mut()
            .unwrap()
            .resolved_review_input
            .as_mut()
            .unwrap(),
    );
    let codex_history::ReviewInputRecord::Baseline {
        root_retains_legacy_transcript,
        ..
    } = &mut records[0]
    else {
        unreachable!()
    };
    *root_retains_legacy_transcript = None;
    turn_context.session_source = SessionSource::Cli;
    let predecessor = session
        .reconstruct_history_from_rollout(&turn_context, &replayed_items)
        .await;
    assert_eq!(predecessor.guardian_history, None);
    assert_eq!(predecessor.retained_context, thread_owned);
}

#[test_case(false; "thread-owned fallback")]
#[test_case(true; "ordinary guardian backup")]
#[tokio::test]
async fn migration_preserves_review_eviction_under_current_model_policy(has_guardian_backup: bool) {
    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, mut turn_context) = make_session_and_context().await;
    turn_context.history_mode = ThreadHistoryMode::Paginated;
    let opaque = ResponseItem::Compaction {
        id: Some(codex_protocol::ResponseItemId::with_suffix("cmp", "opaque")),
        encrypted_content: "opaque".to_string(),
        internal_chat_message_metadata_passthrough: None,
    };
    let baseline_output: ResponseItem = serde_json::from_value(json!({
        "type": "function_call_output", "call_id": "baseline",
        "output": "x".repeat(4 * 1024 * 1024 - 64 * 1024),
    }))
    .expect("baseline tool output");
    let baseline = vec![opaque, baseline_output];
    let mut checkpoint = checkpoint_compacted(baseline.clone());
    checkpoint.guardian_history =
        has_guardian_backup.then(|| codex_history::GuardianHistoryCheckpoint(baseline.clone()));
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    let mut items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: session.thread_id,
                session_id: session.thread_id.into(),
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                cwd: home.path().to_path_buf(),
                source: SessionSource::Cli,
                history_mode: ThreadHistoryMode::Legacy,
                ..Default::default()
            },
            git: None,
        }),
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(complete_thread_settings())),
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: None,
        })),
        RolloutItem::Compacted(checkpoint),
    ];
    let removed_output: ResponseItem = serde_json::from_value(json!({
        "type": "function_call_output", "call_id": "removed",
        "output": "y".repeat(1024 * 1024),
    }))
    .expect("historical output without a saved truncation budget");
    let mut context = turn_context.to_turn_context_item();
    context.turn_id = Some("C".to_string());
    items.extend(migration_user_turn_rollout(
        context,
        user_message("C").into(),
        vec![RolloutItem::ResponseItem(removed_output.into())],
    ));
    items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));
    let (_, full, bounded) =
        migrate_checkpoint_fixture(home.path(), session.thread_id, &items).await;
    for byte_budget in [1024, 256 * 1024] {
        Arc::make_mut(&mut Arc::make_mut(&mut turn_context.initial_settings).model_info)
            .truncation_policy =
            codex_protocol::openai_models::TruncationPolicyConfig::bytes(byte_budget);
        let before = session
            .reconstruct_history_from_rollout(&turn_context, &items)
            .await;
        let expected_guardian = Some({
            codex_history::GuardianHistoryCheckpoint(if byte_budget == 1024 {
                baseline.clone()
            } else {
                Vec::new()
            })
        });
        // Equality remains exact without printing several MiB of fixture text on failure.
        assert!(
            before.guardian_history == expected_guardian,
            "unexpected original transcript for budget {byte_budget}",
        );
        assert_eq!(
            serde_json::to_value(&before.retained_context).expect("serialize retained context"),
            json!({
                "verified_answers": [], "incomplete": false,
                "user_messages": [{
                    "order": 0, "turn_id": "", "message_id": null,
                    "text": "C", "complete": false,
                }],
                "user_messages_incomplete": true,
                "assistant_messages": [], "assistant_messages_incomplete": false,
                "next_order": 1,
            }),
        );
        for migrated in [&full, &bounded] {
            let after = session
                .reconstruct_history_from_rollout(&turn_context, migrated)
                .await;
            assert!(
                after == before,
                "migration changed reconstruction for budget {byte_budget}",
            );
        }
    }
}

#[tokio::test]
async fn review_input_resolution_follows_reconstruction_checkpoint() {
    let home = tempfile::tempdir().expect("isolated CODEX_HOME");
    let (session, _) = make_session_and_context().await;
    let mut selected = checkpoint_compacted(vec![user_message("A")]);
    selected.segment_state_checkpoint = None;
    selected.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    selected.retained_context_replay = Some(codex_history::RetainedContextReplay {
        legacy: codex_history::RetainedContext::default(),
        thread_owned_worker: codex_history::RetainedContext::default(),
        thread_owned_root: codex_history::RetainedContext::default(),
        review_input: Some(codex_protocol::protocol::HistoryPosition {
            thread_id: ThreadId::new(),
            end_ordinal_exclusive: 3,
            end_byte_offset: 4096,
        }),
        resolved_review_input: Some(Arc::new(vec![
            codex_history::ReviewInputRecord::Baseline {
                applicability: codex_history::ReviewTranscriptApplicability::Both,
                root_retains_legacy_transcript: None,
                history: codex_history::GuardianHistoryCheckpoint(vec![user_message("A")]),
            },
            codex_history::ReviewInputRecord::Rollback {
                boundary: user_message("removed"),
            },
        ])),
    });
    let mut incomplete = selected.clone();
    incomplete.replacement_history = None;
    incomplete.resume_metadata = None;
    incomplete
        .retained_context_replay
        .as_mut()
        .unwrap()
        .resolved_review_input = None;
    let mut items = vec![
        RolloutItem::Compacted(selected),
        RolloutItem::Compacted(incomplete),
    ];
    let wire_before = serde_json::to_value(&items).expect("serialize unresolved history");
    super::rollout_reconstruction::resolve_review_input_for_reconstruction(home.path(), &mut items)
        .await
        .expect("reuse selected cache without opening newer incomplete reference");
    assert_eq!(serde_json::to_value(&items).unwrap(), wire_before);
    let RolloutItem::Compacted(incomplete) = &items[1] else {
        unreachable!()
    };
    assert!(
        incomplete
            .retained_context_replay
            .as_ref()
            .unwrap()
            .resolved_review_input
            .is_none()
    );

    let RolloutItem::Compacted(selected) = &mut items[0] else {
        unreachable!()
    };
    selected
        .retained_context_replay
        .as_mut()
        .unwrap()
        .resolved_review_input = None;
    let error = super::rollout_reconstruction::resolve_review_input_for_reconstruction(
        home.path(),
        &mut items,
    )
    .await
    .expect_err("selected missing input must not fall back to an empty transcript");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert!(
        session
            .materialize_forked_history(&items, ForkedHistoryMaterialization::Recent)
            .await
            .is_err(),
        "review-only references must resolve without ordinary rollout references"
    );

    let RolloutItem::Compacted(mut newest) = items[0].clone() else {
        unreachable!()
    };
    newest.retained_context_replay = None;
    items.push(RolloutItem::Compacted(newest));
    super::rollout_reconstruction::resolve_review_input_for_reconstruction(home.path(), &mut items)
        .await
        .expect("a selected checkpoint without review input must not open older references");
}

#[test]
fn modern_state_only_checkpoint_is_invisible_without_hiding_real_compaction() {
    let mut checkpoint = checkpoint_compacted(vec![user_message("A")]);
    checkpoint.message.clear();
    checkpoint.segment_state_checkpoint = None;
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    checkpoint.compaction_response_id = None;
    assert!(
        codex_app_server_protocol::build_turns_from_rollout_items(&[RolloutItem::Compacted(
            checkpoint.clone(),
        )])
        .is_empty()
    );

    checkpoint.compaction_response_id = Some("real-compaction-response".to_string());
    let real =
        codex_app_server_protocol::build_turns_from_rollout_items(&[RolloutItem::Compacted(
            checkpoint,
        )]);
    assert_eq!(
        real.len(),
        1,
        "a real compaction must remain visible even with an empty summary"
    );
}

#[test_case(false; "legacy summary compaction")]
#[test_case(true; "modern summary compaction")]
fn nonempty_summary_compaction_remains_visible(modern: bool) {
    let mut checkpoint = checkpoint_compacted(vec![user_message("A")]);
    checkpoint.message = "A real compaction summary".to_string();
    checkpoint.segment_state_checkpoint = None;
    checkpoint.compaction_response_id = None;
    checkpoint.resume_metadata = modern.then_some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    let turns =
        codex_app_server_protocol::build_turns_from_rollout_items(&[RolloutItem::Compacted(
            checkpoint,
        )]);
    assert_eq!(
        turns.len(),
        1,
        "a nonempty compaction summary must remain visible"
    );
}

#[test]
fn incomplete_modern_checkpoint_without_window_remains_visible() {
    let mut checkpoint = checkpoint_compacted(vec![user_message("A")]);
    checkpoint.message.clear();
    checkpoint.segment_state_checkpoint = None;
    checkpoint.window_number = None;
    checkpoint.compaction_response_id = None;
    checkpoint.resume_metadata = Some(codex_history::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    let turns =
        codex_app_server_protocol::build_turns_from_rollout_items(&[RolloutItem::Compacted(
            checkpoint,
        )]);
    assert_eq!(
        turns.len(),
        1,
        "incomplete modern metadata must remain visible"
    );
}
