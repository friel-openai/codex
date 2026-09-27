use super::*;
use codex_history::CodexHarnessMetadata;
use codex_history::SenderUserMessages;
use pretty_assertions::assert_eq;
use serde_json::json;

fn checkpoint() -> CompactedItem {
    serde_json::from_value(json!({
        "message": "checkpoint",
        "replacement_history": [],
        "window_number": 1,
        "resume_metadata": {},
        "retained_context": {
            "verified_answers": [], "incomplete": false,
            "user_messages": [], "user_messages_incomplete": false, "next_order": 0,
        },
    }))
    .expect("modern checkpoint fixture")
}

fn user(id: &str, metadata: CodexHarnessMetadata) -> ResponseItemEnvelope {
    ResponseItemEnvelope {
        item: serde_json::from_value(json!({
            "type": "message", "role": "user", "id": id,
            "content": [{"type": "input_text", "text": id}],
            "internal_chat_message_metadata_passthrough": {
                "turn_id": id,
                "content_item_kinds": ["user.text"],
            },
        }))
        .expect("user response fixture"),
        metadata: Some(metadata),
    }
}

fn sender_messages(id: &str) -> Box<SenderUserMessages> {
    Box::new(SenderUserMessages {
        receiver_turn_id: id.to_string(),
        receiver_message_id: format!("delivery-{id}"),
        text: format!("restriction-{id}"),
    })
}

#[test]
fn inherited_user_eviction_preserves_each_review_policy_across_remigration() {
    let mut base = checkpoint();
    base.replacement_history = Some(
        ["A", "B", "C", "D", "E", "F", "G", "H"]
            .into_iter()
            .map(|id| {
                user(
                    id,
                    CodexHarnessMetadata {
                        inherited_user_message: true,
                        ..Default::default()
                    },
                )
            })
            .collect(),
    );
    let surviving_inherited_users = ["B", "C", "D", "E", "F", "G", "H"]
        .into_iter()
        .enumerate()
        .map(|(index, id)| {
            json!({
                "inherited": true, "order": index + 1,
                "turn_id": id, "message_id": id, "text": id, "complete": false,
            })
        })
        .collect::<Vec<_>>();
    let expected: RetainedContextReplay = serde_json::from_value(json!({
        "legacy": {
            "verified_answers": [], "incomplete": false, "user_messages": [],
            "user_messages_incomplete": true, "next_order": 0,
        },
        "thread_owned_worker": {
            "verified_answers": [], "incomplete": false, "user_messages": [],
            "user_messages_incomplete": false, "next_order": 1,
        },
        "thread_owned_root": {
            "verified_answers": [], "incomplete": true,
            "user_messages": surviving_inherited_users,
            "user_messages_incomplete": true, "next_order": 1,
        },
    }))
    .expect("expected policy states");

    for source_is_non_root_agent in [false, true] {
        let mut planner = ModernRollbackPlanner::new();
        planner.source_is_non_root_agent = source_is_non_root_agent;
        planner.observe(0, &RolloutItem::Compacted(base.clone()));
        planner.observe(
            1,
            &RolloutItem::ResponseItem(user(
                "Q",
                CodexHarnessMetadata {
                    user_input_order: Some(0),
                    ..Default::default()
                },
            )),
        );
        planner.observe_rollback(2, 1, 0, |_| false);
        let synthetic = planner
            .finish()
            .expect("representable rollback")
            .compacted_at(2)
            .expect("synthetic checkpoint");
        assert_eq!(synthetic.retained_context_replay.as_ref(), Some(&expected));
        let selected = if source_is_non_root_agent {
            &expected.thread_owned_worker
        } else {
            &expected.thread_owned_root
        };
        assert_eq!(synthetic.retained_context.as_ref(), Some(selected));

        let synthetic: CompactedItem =
            serde_json::from_value(serde_json::to_value(&synthetic).unwrap()).unwrap();
        assert_eq!(RetainedReplay::new(&synthetic).states, expected);
    }
}

#[test]
fn evicted_inherited_users_and_sender_deliveries_are_not_restored_from_model_history() {
    let mut base = checkpoint();
    base.replacement_history = Some(vec![user(
        "A",
        CodexHarnessMetadata {
            inherited_user_message: true,
            user_input_order: Some(0),
            sender_user_messages: Some(sender_messages("A")),
            ..Default::default()
        },
    )]);
    let expected: RetainedContextReplay = serde_json::from_value(json!({
        "legacy": {
            "verified_answers": [], "incomplete": false, "user_messages": [],
            "user_messages_incomplete": true, "next_order": 1,
            "sender_deliveries": [{
                "order": 0, "receiver_turn_id": "A", "receiver_message_id": "delivery-A",
                "text": "restriction-A",
            }],
        },
        "thread_owned_worker": {
            "verified_answers": [], "incomplete": false, "user_messages": [],
            "user_messages_incomplete": false, "next_order": 9,
        },
        "thread_owned_root": {
            "verified_answers": [], "incomplete": true, "user_messages": [],
            "user_messages_incomplete": true, "next_order": 9,
        },
    }))
    .expect("expected policy states");

    for source_is_non_root_agent in [false, true] {
        let mut planner = ModernRollbackPlanner::new();
        planner.source_is_non_root_agent = source_is_non_root_agent;
        planner.observe(0, &RolloutItem::Compacted(base.clone()));
        for order in 1..=8 {
            let id = format!("local-{order}");
            planner.observe(
                order,
                &RolloutItem::ResponseItem(user(
                    &id,
                    CodexHarnessMetadata {
                        user_input_order: Some(order as u64),
                        sender_user_messages: Some(sender_messages(&id)),
                        ..Default::default()
                    },
                )),
            );
        }
        planner.observe_rollback(9, 8, 0, |_| false);
        let synthetic = planner
            .finish()
            .expect("representable rollback")
            .compacted_at(9)
            .expect("synthetic checkpoint");
        assert_eq!(synthetic.retained_context_replay.as_ref(), Some(&expected));
        let selected = if source_is_non_root_agent {
            &expected.thread_owned_worker
        } else {
            &expected.thread_owned_root
        };
        assert_eq!(synthetic.retained_context.as_ref(), Some(selected));

        let synthetic: CompactedItem =
            serde_json::from_value(serde_json::to_value(&synthetic).unwrap()).unwrap();
        assert_eq!(RetainedReplay::new(&synthetic).states, expected);
    }
}

#[test]
fn opaque_checkpoint_fallback_survives_retained_admission_and_remigration() {
    let opaque: ResponseItem = serde_json::from_value(json!({
        "type": "compaction", "id": "cmp_base", "encrypted_content": "opaque",
    }))
    .unwrap();
    let mut base = checkpoint();
    base.replacement_history = Some(vec![ResponseItemEnvelope::new(opaque.clone())]);
    let user: ResponseItem = serde_json::from_value(json!({
        "type": "message", "role": "user",
        "content": [{"type": "input_text", "text": "C"}],
    }))
    .unwrap();
    let mut planner = ModernRollbackPlanner::new();
    planner.observe(0, &RolloutItem::Compacted(base.clone()));
    planner.observe(
        1,
        &RolloutItem::ResponseItem(ResponseItemEnvelope::new(user.clone())),
    );
    planner.observe_rollback(2, 1, 0, |_| false);
    let plan = planner.finish().unwrap();
    let mut synthetic = plan.compacted_at(2).unwrap();
    let thread_owned = json!({
        "verified_answers": [], "incomplete": false,
        "user_messages": [{
            "order": 0, "turn_id": "", "message_id": null,
            "text": "C", "complete": false,
        }],
        "user_messages_incomplete": false, "next_order": 1,
    });
    let expected: RetainedContextReplay = serde_json::from_value(json!({
        "legacy": {
            "verified_answers": [], "incomplete": false, "user_messages": [],
            "user_messages_incomplete": true, "next_order": 0,
        },
        "thread_owned_worker": thread_owned,
        "thread_owned_root": thread_owned,
    }))
    .unwrap();
    assert_eq!(synthetic.guardian_history, None);
    assert_eq!(synthetic.retained_context_replay.as_ref(), Some(&expected));
    assert_eq!(plan.snapshots[&2].review_input_end, Some(3));
    assert_eq!(plan.review_input_base, None);
    assert_eq!(
        plan.review_inputs,
        vec![
            ReviewInputRecord::Baseline {
                applicability: ReviewTranscriptApplicability::ThreadOwned,
                history: GuardianHistoryCheckpoint(vec![opaque.clone()]),
            },
            ReviewInputRecord::ResponseItem {
                response: ResponseItemEnvelope::new(user.clone())
            },
            ReviewInputRecord::Rollback {
                boundary: user.clone()
            },
        ]
    );
    let position: HistoryPosition = serde_json::from_value(json!({
        "thread_id": "00000000-0000-7000-8000-000000000123",
        "end_ordinal_exclusive": 4, "end_byte_offset": 456,
    }))
    .unwrap();
    synthetic
        .retained_context_replay
        .as_mut()
        .unwrap()
        .review_input = Some(position);
    let synthetic: CompactedItem =
        serde_json::from_value(serde_json::to_value(&synthetic).unwrap()).unwrap();
    let replay = RetainedReplay::new(&synthetic);
    assert_eq!(replay.states.review_input, Some(position));
    assert_eq!(replay.review_inputs, Some(Vec::new()));
    let mut planner = ModernRollbackPlanner::new();
    planner.observe(0, &RolloutItem::Compacted(synthetic));
    planner.observe(
        1,
        &RolloutItem::ResponseItem(ResponseItemEnvelope::new(user.clone())),
    );
    planner.observe_rollback(2, 1, 0, |_| false);
    let remigrated = planner.finish().unwrap();
    assert_eq!(remigrated.review_input_base, Some(position));
    assert_eq!(remigrated.snapshots[&2].review_input_end, Some(2));
    assert_eq!(
        remigrated.review_inputs,
        vec![
            ReviewInputRecord::ResponseItem {
                response: ResponseItemEnvelope::new(user.clone())
            },
            ReviewInputRecord::Rollback { boundary: user },
        ]
    );

    base.guardian_history = Some(GuardianHistoryCheckpoint(vec![opaque.clone()]));
    assert_eq!(
        RetainedReplay::new(&base).review_inputs,
        Some(vec![ReviewInputRecord::Baseline {
            applicability: ReviewTranscriptApplicability::Both,
            history: GuardianHistoryCheckpoint(vec![opaque]),
        },])
    );
}

#[test]
fn fallback_transcript_rollback_keeps_pre_turn_context_and_remigrates() {
    let first = user("A", CodexHarnessMetadata::default());
    let developer: ResponseItem = serde_json::from_value(json!({
        "type": "message", "role": "developer",
        "content": [{"type": "input_text", "text": "<persistent_mode>active</persistent_mode>"}],
    }))
    .unwrap();
    let opaque: ResponseItem = serde_json::from_value(json!({
        "type": "compaction", "id": "cmp_base", "encrypted_content": "opaque",
    }))
    .unwrap();
    let second = user("B", CodexHarnessMetadata::default());
    let mut base = checkpoint();
    base.replacement_history = Some(vec![
        first.clone(),
        ResponseItemEnvelope::new(developer.clone()),
        second.clone(),
        ResponseItemEnvelope::new(opaque),
    ]);
    let mut planner = ModernRollbackPlanner::new();
    planner.observe(0, &RolloutItem::Compacted(base));
    planner.observe_rollback(1, 1, 0, |_| false);
    let plan = planner.finish().unwrap();
    let mut synthetic = plan.compacted_at(1).unwrap();
    super::super::rollback_checkpoint::apply_compaction_edits(0, &mut synthetic, &[], &[1], false)
        .unwrap();
    assert_eq!(synthetic.replacement_history, Some(vec![first.clone()]));
    assert_eq!(plan.snapshots[&1].review_input_end, Some(2));
    let mut history = TranscriptHistory::new(0);
    for input in &plan.review_inputs {
        match input {
            ReviewInputRecord::Baseline {
                history: baseline,
                applicability,
            } => {
                assert_eq!(*applicability, ReviewTranscriptApplicability::ThreadOwned);
                history.reset(baseline.0.iter());
            }
            ReviewInputRecord::Rollback { boundary } => {
                assert_eq!(*boundary, second.item);
                history.truncate_before(boundary);
            }
            ReviewInputRecord::ResponseItem { .. } => panic!("no raw suffix"),
        }
    }
    assert_eq!(
        history.items().cloned().collect::<Vec<_>>(),
        vec![first.item, developer]
    );
    let position: HistoryPosition = serde_json::from_value(json!({
        "thread_id": "00000000-0000-7000-8000-000000000123",
        "end_ordinal_exclusive": 3, "end_byte_offset": 456,
    }))
    .unwrap();
    synthetic
        .retained_context_replay
        .as_mut()
        .unwrap()
        .review_input = Some(position);
    let synthetic: CompactedItem =
        serde_json::from_value(serde_json::to_value(&synthetic).unwrap()).unwrap();
    let remigrated = RetainedReplay::new(&synthetic);
    assert_eq!(remigrated.states.review_input, Some(position));
    assert_eq!(remigrated.review_inputs, Some(Vec::new()));
}

#[test]
fn review_input_snapshots_share_one_log_and_exclude_surviving_suffix() {
    let original = user("original", CodexHarnessMetadata::default());
    let mut base = checkpoint();
    base.guardian_history = Some(GuardianHistoryCheckpoint(vec![original.item.clone()]));
    let first = user("C", CodexHarnessMetadata::default());
    let second = user("D", CodexHarnessMetadata::default());
    let output = ResponseItemEnvelope::new(
        serde_json::from_value(json!({
            "type": "function_call_output", "call_id": "call-1", "output": "original output",
        }))
        .unwrap(),
    );
    let mut planner = ModernRollbackPlanner::new();
    planner.observe(0, &RolloutItem::Compacted(base));
    planner.observe(1, &RolloutItem::ResponseItem(first.clone()));
    planner.observe(2, &RolloutItem::ResponseItem(output.clone()));
    planner.observe_rollback(3, 1, 0, |_| false);
    planner.observe(4, &RolloutItem::ResponseItem(second.clone()));
    planner.observe(5, &RolloutItem::ResponseItem(output.clone()));
    planner.observe_rollback(6, 1, 0, |_| false);
    planner.observe(
        7,
        &RolloutItem::ResponseItem(user("surviving", CodexHarnessMetadata::default())),
    );
    let plan = planner.finish().unwrap();
    assert_eq!(plan.snapshots[&3].review_input_end, Some(4));
    assert_eq!(plan.snapshots[&6].review_input_end, Some(7));
    assert_eq!(plan.review_input_base, None);
    assert_eq!(
        plan.review_inputs,
        vec![
            ReviewInputRecord::Baseline {
                applicability: ReviewTranscriptApplicability::Legacy,
                history: GuardianHistoryCheckpoint(vec![original.item]),
            },
            ReviewInputRecord::ResponseItem {
                response: first.clone()
            },
            ReviewInputRecord::ResponseItem {
                response: output.clone()
            },
            ReviewInputRecord::Rollback {
                boundary: first.item
            },
            ReviewInputRecord::ResponseItem {
                response: second.clone()
            },
            ReviewInputRecord::ResponseItem { response: output },
            ReviewInputRecord::Rollback {
                boundary: second.item
            },
        ]
    );
}
