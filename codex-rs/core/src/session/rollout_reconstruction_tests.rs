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
use codex_protocol::turn_input::CyberAccessProgram;
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
            history_revision: None,
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

    let mut live = ContextManager::for_session(
        &SessionSource::default(),
        &crate::config::ManagedFeatures::from(codex_features::Features::with_defaults()),
    );
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
                turn_attribution: None,
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
            root_turn_id: None,
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
        turn_attribution: None,
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
            cyber_access_program: None,
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
        turn_attribution: None,
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
        turn_attribution: None,
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
            cyber_access_program: settings.cyber_access_program,
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
            history_revision: None,
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
            history_revision: None,
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
        .render_full_fragments()
        .1
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
        world_state.render_full().0.into_object(),
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
            history_revision: None,
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
                cyber_access_program: None,
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
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
    };
    let rollout_items = vec![RolloutItem::TurnContext(previous_context_item)];

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;
    assert_eq!(reconstructed.world_state_baseline, None);

    session
        .record_initial_history(InitialHistory::Resumed(ResumedHistory {
            history_revision: None,
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
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: Some("comp-hash-a".to_string()),
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
    };
    let turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    previous_context_item.turn_id = None;

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_attribution: None,
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
                root_turn_id: None,
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
            history_revision: None,
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
            cyber_access_program: None,
            realtime_active: Some(turn_context.realtime_active),
        })
    );
}

#[tokio::test]
async fn reconstruct_history_rollback_keeps_history_and_metadata_in_sync_for_completed_turns() {
    let (session, turn_context) = make_session_and_context().await;
    let mut first_context_item = turn_context.to_turn_context_item();
    first_context_item.cyber_access_program = Some(CyberAccessProgram::DaybreakBlue);
    first_context_item.root_turn_id = Some("root-turn".to_owned());
    first_context_item.service_tier = Some("priority".to_string());
    first_context_item.model_profile = Some("balanced".to_string());
    let first_turn_id = first_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let first_attribution: codex_history::TurnAttribution = object!({
        "turn_id": first_turn_id,
        "turn_trigger": "composer",
        "parent_turn_id": "parent-turn",
        "root_turn_id": "root-turn"
    });
    let mut rolled_back_context_item = first_context_item.clone();
    rolled_back_context_item.turn_id = Some("rolled-back-turn".to_string());
    rolled_back_context_item.model = "rolled-back-model".to_string();
    rolled_back_context_item.cyber_access_program = Some(CyberAccessProgram::DaybreakRed);
    rolled_back_context_item.root_turn_id = Some("rolled-back-root".to_owned());
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
                turn_attribution: Some(first_attribution.clone()),
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
                root_turn_id: None,
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
                turn_attribution: Some(object!({
                    "turn_id": rolled_back_turn_id,
                    "turn_trigger": "automation",
                    "parent_turn_id": "rolled-back-parent",
                    "root_turn_id": "rolled-back-root"
                })),
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
                root_turn_id: None,
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
        serde_json::to_value(reconstructed.previous_turn_settings)
            .expect("serialize previous settings"),
        json!({
            "model": turn_context.model_info().slug,
            "comp_hash": null,
            "realtime_active": turn_context.realtime_active,
            "cyber_access_program": "daybreak_blue",
        })
    );
    assert_eq!(reconstructed.turn_attribution, Some(first_attribution));
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
    let mut first_context_item = turn_context.to_turn_context_item();
    first_context_item.cyber_access_program = Some(CyberAccessProgram::DaybreakBlue);
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
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
        serde_json::to_value(reconstructed.previous_turn_settings)
            .expect("serialize previous settings"),
        json!({
            "model": turn_context.model_info().slug,
            "comp_hash": null,
            "realtime_active": turn_context.realtime_active,
            "cyber_access_program": "daybreak_blue",
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
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
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
            cyber_access_program: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
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
            cyber_access_program: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
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
    assert!(reconstructed.turn_attribution.is_none());
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
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
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
            history_revision: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
            history_revision: None,
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
            cyber_access_program: None,
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
            history_revision: None,
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
            history_revision: None,
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
                    guardian_history: Some(codex_history::GuardianHistoryCheckpoint(vec![
                        user_message("original task").into(),
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
                        turn_attribution: None,
                        previous_turn_settings: Some(PreviousTurnSettings {
                            model: format!("metadata-model-{window_number}"),
                            comp_hash: Some(format!("metadata-hash-{window_number}")),
                            cyber_access_program: None,
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
                root_turn_id: None,
                turn_id: Some(format!("wake-{window_number}")),
                reason: TurnAbortReason::Interrupted,
                error: None,
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
            user_message("original task").into(),
            assistant_message("continued working").into()
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
        assert_eq!(reconstructed.turn_attribution, None);
        assert_eq!(reconstructed.world_state_baseline, None);
    }
}

#[derive(Clone, Copy)]
enum AttributionEvidence {
    StartAndContext,
    StartOnly,
}

#[test_case(AttributionEvidence::StartAndContext; "late completion")]
#[test_case(AttributionEvidence::StartOnly; "late abort before current context")]
#[tokio::test]
async fn reconstruct_attribution_ignores_previous_turn_terminal(evidence: AttributionEvidence) {
    let (session, turn_context) = make_session_and_context().await;
    let older_attribution = turn_context.attribution();
    let expected: codex_history::TurnAttribution = object!({
        "turn_id": "current-turn",
        "turn_trigger": "automation",
        "parent_turn_id": "calling-turn",
        "root_turn_id": "root-turn",
        "initiating_agent_path": "/root/requester"
    });
    let mut current_context = turn_context.to_turn_context_item();
    current_context.turn_id = Some(expected.turn_id.clone());
    current_context.root_turn_id = expected.root_turn_id.clone();
    let mut rollout = completed_user_turn_rollout(turn_context.to_turn_context_item(), Vec::new());
    let RolloutItem::EventMsg(EventMsg::TurnStarted(start)) = &mut rollout[0] else {
        panic!("expected previous turn start");
    };
    start.turn_attribution = Some(older_attribution.clone());
    let mut late_terminal = rollout.pop().expect("previous turn completion");
    let mut current = completed_user_turn_rollout(current_context, Vec::new());
    current.pop().expect("current turn completion");
    let RolloutItem::EventMsg(EventMsg::TurnStarted(start)) = &mut current[0] else {
        panic!("expected current turn start");
    };
    start.turn_attribution = Some(expected.clone());
    if matches!(evidence, AttributionEvidence::StartOnly) {
        current.truncate(1);
        late_terminal = RolloutItem::EventMsg(EventMsg::TurnAborted(object!({
            "turn_id": older_attribution.turn_id,
            "reason": "interrupted"
        })));
    }
    rollout.extend(current);
    rollout.push(late_terminal);

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout)
        .await;
    assert_eq!(reconstructed.turn_attribution, Some(expected));

    if matches!(evidence, AttributionEvidence::StartAndContext) {
        rollout.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            ThreadRolledBackEvent { num_turns: 1 },
        )));
        let rolled_back = session
            .reconstruct_history_from_rollout(&turn_context, &rollout)
            .await;
        assert_eq!(rolled_back.turn_attribution, Some(older_attribution));
    }
}

#[test_case(Some("current-turn"); "continuation marker retained")]
#[test_case(None; "continuation marker cleared")]
#[tokio::test]
async fn reconstruct_attribution_ignores_previous_terminal_after_checkpoint(
    last_started_turn_id: Option<&str>,
) {
    let (session, turn_context) = make_session_and_context().await;
    let expected: codex_history::TurnAttribution = object!({
        "turn_id": "current-turn",
        "turn_trigger": "automation",
        "parent_turn_id": "calling-turn",
        "root_turn_id": "root-turn"
    });
    let mut context = turn_context.to_turn_context_item();
    context.turn_id = Some(expected.turn_id.clone());
    context.root_turn_id = expected.root_turn_id.clone();
    let settings: PreviousTurnSettings = object!({"model": "checkpoint-model"});
    let late_terminal =
        completed_user_turn_rollout(turn_context.to_turn_context_item(), Vec::new())
            .pop()
            .expect("previous turn completion");
    let mut rollout = vec![
        RolloutItem::Compacted(object!({
            "message": "summary",
            "replacement_history": [user_message("original work")],
            "window_number": 1,
            "resume_metadata": {
                "last_started_turn_id": last_started_turn_id,
                "turn_attribution": expected,
                "previous_turn_settings": settings
            }
        })),
        RolloutItem::ResponseItem(user_message("continued work").into()),
        RolloutItem::TurnContext(context.clone()),
        late_terminal,
    ];
    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout)
        .await;
    assert_eq!(reconstructed.turn_attribution, Some(expected));
    assert_eq!(reconstructed.reference_context_item, Some(context));
    assert_eq!(reconstructed.previous_turn_settings, Some(settings));
    assert_eq!(
        reconstructed.last_started_turn_id.as_deref(),
        last_started_turn_id
    );

    rollout.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        ThreadRolledBackEvent { num_turns: 1 },
    )));
    let rolled_back = session
        .reconstruct_history_from_rollout(&turn_context, &rollout)
        .await;
    assert_eq!(rolled_back.turn_attribution, None);
}

#[test_case(false, false; "completion before next turn")]
#[test_case(true, false; "completion after next turn starts")]
#[test_case(true, true; "late completion without checkpoint turn identity")]
#[tokio::test]
async fn completed_turn_suffix_after_compaction_overrides_resume_metadata(
    late_completion: bool,
    legacy_checkpoint: bool,
) {
    let (session, turn_context) = make_session_and_context().await;
    let mut newer_context = turn_context.to_turn_context_item();
    newer_context.turn_id = Some("newer-turn".to_string());
    newer_context.model = "newer-model".to_string();
    newer_context.comp_hash = Some("newer-hash".to_string());
    let checkpoint_attribution = (!legacy_checkpoint).then(|| codex_history::TurnAttribution {
        turn_id: "newer-turn".to_string(),
        turn_trigger: Some("automation".to_string()),
        parent_turn_id: None,
        initiating_agent_path: None,
        root_turn_id: None,
    });
    let expected_settings = PreviousTurnSettings {
        model: newer_context.model.clone(),
        comp_hash: newer_context.comp_hash.clone(),
        cyber_access_program: None,
        realtime_active: newer_context.realtime_active,
    };
    let mut rollout_items = vec![
        RolloutItem::Compacted(object!({
            "message": "summary",
            "replacement_history": [user_message("seed"), assistant_message("summary")],
            "window_number": 1,
            "resume_metadata": {
                "last_started_turn_id": checkpoint_attribution.as_ref().map(|value| value.turn_id.as_str()),
                "turn_attribution": checkpoint_attribution,
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
    let next_attribution: Option<codex_history::TurnAttribution> =
        late_completion.then(|| object!({"turn_id": "next-turn", "turn_trigger": "user"}));
    if let Some(attribution) = &next_attribution {
        rollout_items.insert(
            rollout_items.len() - 1,
            RolloutItem::EventMsg(EventMsg::TurnStarted(object!({
                "turn_id": attribution.turn_id,
                "turn_attribution": attribution
            }))),
        );
    }

    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;

    assert_eq!(
        reconstructed.previous_turn_settings,
        Some(expected_settings)
    );
    assert_eq!(
        reconstructed.turn_attribution,
        next_attribution.or(checkpoint_attribution)
    );
    assert_eq!(
        reconstructed.reference_context_item,
        Some(newer_context.clone())
    );

    // A rollback of a newer user turn restores the checkpoint's older regular turn.
    let RolloutItem::Compacted(checkpoint) = &mut rollout_items[0] else {
        panic!("expected checkpoint");
    };
    let metadata = checkpoint
        .resume_metadata
        .as_mut()
        .expect("resume metadata");
    let older_attribution: codex_history::TurnAttribution =
        object!({"turn_id": "older-turn", "turn_trigger": "automation"});
    metadata.turn_attribution = Some(older_attribution.clone());
    rollout_items.truncate(1);
    rollout_items.extend(completed_user_turn_rollout(newer_context, Vec::new()));
    rollout_items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
        codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
    )));
    let rolled_back = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;
    assert_eq!(rolled_back.turn_attribution, Some(older_attribution));
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
                turn_attribution: None,
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
                root_turn_id: None,
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
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
    };
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_attribution: None,
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
                root_turn_id: None,
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
            history_revision: None,
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
            cyber_access_program: None,
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
            approval_policy: turn_context.approval_policy(),
            approvals_reviewer: None,
            sandbox_policy: turn_context.sandbox_policy(),
            permission_profile: None,
            active_permission_profile: None,
            file_system_sandbox_policy: None,
            model: previous_model.to_string(),
            comp_hash: None,
            collaboration_mode: Some(turn_context.collaboration_mode()),
            multi_agent_version: None,
            realtime_active: Some(turn_context.realtime_active),
            cyber_access_program: None,
            effort: turn_context.reasoning_effort().cloned(),
            service_tier: None,
            model_profile: None,
            summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
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
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
    };
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let aborted_turn_id = "aborted-turn-without-id".to_string();

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
                turn_id: None,
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                error: None,
                completed_at: None,
                duration_ms: None,
            },
        )),
        RolloutItem::Compacted(CompactedItem {
            message: String::new(),
            replacement_history: Some(Vec::new()),
            retained_context: None,
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
            history_revision: None,
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
            cyber_access_program: None,
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
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        file_system_sandbox_policy: None,
        model: current_model.to_string(),
        comp_hash: None,
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
    };

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
                root_turn_id: None,
                turn_id: Some(unmatched_abort_turn_id),
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                error: None,
                completed_at: None,
                duration_ms: None,
            },
        )),
        RolloutItem::TurnContext(current_context_item.clone()),
        RolloutItem::EventMsg(EventMsg::TurnComplete(
            codex_protocol::protocol::TurnCompleteEvent {
                root_turn_id: None,
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
            history_revision: None,
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
            cyber_access_program: None,
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
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
    };
    let previous_turn_id = previous_context_item
        .turn_id
        .clone()
        .expect("turn context should have turn_id");
    let incomplete_turn_id = "trailing-incomplete-turn".to_string();

    let rollout_items = vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(
            codex_protocol::protocol::TurnStartedEvent {
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
            history_revision: None,
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
            cyber_access_program: None,
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
                turn_attribution: None,
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
            history_revision: None,
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
            cyber_access_program: None,
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
        approval_policy: turn_context.approval_policy(),
        approvals_reviewer: None,
        sandbox_policy: turn_context.sandbox_policy(),
        permission_profile: None,
        active_permission_profile: None,
        file_system_sandbox_policy: None,
        model: previous_model.to_string(),
        comp_hash: None,
        collaboration_mode: Some(turn_context.collaboration_mode()),
        multi_agent_version: None,
        realtime_active: Some(turn_context.realtime_active),
        cyber_access_program: None,
        effort: turn_context.reasoning_effort().cloned(),
        service_tier: None,
        model_profile: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
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
                turn_attribution: None,
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
                root_turn_id: None,
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
                turn_attribution: None,
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
                turn_attribution: None,
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
            history_revision: None,
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
            cyber_access_program: None,
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
        turn_attribution: None,
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
