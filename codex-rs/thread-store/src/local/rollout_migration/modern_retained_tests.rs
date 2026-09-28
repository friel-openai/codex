use super::*;
use codex_history::CodexHarnessMetadata;
use codex_history::RetainedContextEntry;
use codex_history::RetainedContextOrder;
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

fn assistant(id: &str, order: u64) -> ResponseItemEnvelope {
    let mut response = user(
        id,
        CodexHarnessMetadata {
            user_input_order: Some(order),
            ..Default::default()
        },
    );
    let ResponseItem::Message { role, phase, .. } = &mut response.item else {
        unreachable!();
    };
    *role = "assistant".to_owned();
    *phase = Some(codex_protocol::models::MessagePhase::Commentary);
    response
}

fn call(id: &str, turn: &str, metadata: CodexHarnessMetadata) -> ResponseItemEnvelope {
    ResponseItemEnvelope {
        item: serde_json::from_value(json!({
            "type": "function_call", "name": "send_message", "arguments": "{}",
            "call_id": id, "id": id,
            "internal_chat_message_metadata_passthrough": { "turn_id": turn },
        }))
        .unwrap(),
        metadata: Some(metadata),
    }
}

fn delivery(id: &str) -> ResponseItemEnvelope {
    ResponseItemEnvelope {
        item: serde_json::from_value(json!({
            "type": "function_call_output", "call_id": id, "output": "post-hook output",
        }))
        .unwrap(),
        metadata: Some(CodexHarnessMetadata {
            delivered_assistant_message: Some(format!("confirmed-{id}")),
            ..Default::default()
        }),
    }
}

fn persist_source(response: &mut ResponseItemEnvelope) -> codex_history::RetainedSource {
    let source = record_retained_message(
        &mut RetainedContext::default(),
        &response.item,
        response.metadata.as_ref(),
        RetainedMessageSource::Original,
    )
    .expect("original fixture has a retained source");
    response.metadata.get_or_insert_default().retained_source = Some(source.clone());
    source
}

#[test]
fn assistant_delivery_keeps_call_order_and_saved_revisions_after_rollback() {
    let mut first = user(
        "first",
        CodexHarnessMetadata {
            user_input_order: Some(0),
            ..Default::default()
        },
    );
    let first_source = persist_source(&mut first);
    let mut reply = assistant("reply", 1);
    let reply_source = persist_source(&mut reply);
    let original_call = call(
        "call-before-steer",
        "first",
        CodexHarnessMetadata {
            user_input_order: Some(2),
            ..Default::default()
        },
    );
    let steer = user(
        "steer",
        CodexHarnessMetadata {
            user_input_order: Some(3),
            ..Default::default()
        },
    );
    let mut inherited = assistant("inherited", 99);
    inherited.metadata.as_mut().unwrap().inherited_user_message = true;
    let inherited_source = persist_source(&mut inherited);
    let mut summary = assistant("summary", 6);
    summary.metadata.as_mut().unwrap().compaction_output = true;
    let mut unsequenced = assistant("unsequenced", 7);
    unsequenced.metadata.as_mut().unwrap().user_input_order = None;
    let queued_call = call(
        "queued-call",
        "first",
        CodexHarnessMetadata {
            user_input_order: Some(5),
            ..Default::default()
        },
    );
    let mut replay = RetainedReplay::new(&checkpoint());
    for response in [
        first,
        reply.clone(),
        original_call,
        unsequenced,
        assistant("queued-assistant", 4),
        queued_call,
        steer,
        delivery("call-before-steer"),
        inherited.clone(),
        summary,
    ] {
        replay.observe(&response);
    }
    let delivered_source = replay
        .states
        .thread_owned_root
        .ordered_entries()
        .find_map(|(_, entry)| {
            let RetainedContextEntry::AssistantMessage(message) = entry else {
                return None;
            };
            (message.message_id.as_deref() == Some("call-before-steer"))
                .then(|| replay.states.thread_owned_root.source(entry).unwrap())
        })
        .unwrap();
    replay.rollback(1);
    let expected_items = vec![
        ("first", RetainedInputSource::Local(Some(0))),
        ("reply", RetainedInputSource::Local(Some(1))),
        ("call-before-steer", RetainedInputSource::Local(Some(2))),
        ("unsequenced", RetainedInputSource::Local(None)),
    ];
    assert_eq!(
        replay
            .items
            .iter()
            .map(|item| (
                item.message_id.as_ref().unwrap().as_str(),
                item.input_source,
            ))
            .collect::<Vec<_>>(),
        expected_items,
    );
    replay.observe(&user(
        "later",
        CodexHarnessMetadata {
            user_input_order: Some(6),
            ..Default::default()
        },
    ));
    // A stale call lookup would restore order 5 and survive the order-6 rollback.
    replay.observe(&delivery("queued-call"));
    replay.rollback(1);
    assert_eq!(
        replay
            .items
            .iter()
            .map(|item| (
                item.message_id.as_ref().unwrap().as_str(),
                item.input_source,
            ))
            .collect::<Vec<_>>(),
        expected_items,
    );
    for (context, include_inherited) in [
        (&replay.states.thread_owned_root, true),
        (&replay.states.thread_owned_worker, false),
    ] {
        assert_eq!(context.verified_answers_complete(), !include_inherited);
        let retained = context
            .ordered_entries()
            .map(|(order, entry)| {
                let (role, message) = match entry {
                    RetainedContextEntry::UserMessage(message) => ("user", message),
                    RetainedContextEntry::AssistantMessage(message) => ("assistant", message),
                    RetainedContextEntry::VerifiedAnswer(_) => panic!("unexpected verified answer"),
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
            (RetainedContextOrder::Local(0), "user", "first", "first"),
            (
                RetainedContextOrder::Local(1),
                "assistant",
                "reply",
                "reply",
            ),
            (
                RetainedContextOrder::Local(2),
                "assistant",
                "call-before-steer",
                "confirmed-call-before-steer",
            ),
        ];
        if include_inherited {
            expected.insert(
                0,
                (
                    RetainedContextOrder::Inherited(0),
                    "assistant",
                    "inherited",
                    "inherited",
                ),
            );
        }
        assert_eq!(retained, expected);
        let sources = context
            .ordered_entries()
            .filter_map(|(_, entry)| context.source(entry))
            .collect::<Vec<_>>();
        assert_eq!(
            sources[usize::from(include_inherited)..2 + usize::from(include_inherited)],
            [first_source.clone(), reply_source.clone()]
        );
        if include_inherited {
            assert_eq!(sources[0], inherited_source);
            assert_eq!(sources[3], delivered_source);
        }
        let reply = context
            .ordered_entries()
            .find_map(|(_, entry)| match entry {
                RetainedContextEntry::AssistantMessage(message)
                    if message.message_id.as_deref() == Some("reply") =>
                {
                    Some(message)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            reply,
            &RetainedUserMessage {
                turn_id: "reply".to_owned(),
                message_id: Some("reply".to_owned()),
                text: "reply".to_owned(),
                complete: true,
                phase: Some(codex_protocol::models::MessagePhase::Commentary),
                origin: codex_history::UserInputOrigin::User,
            }
        );
    }
    assert!(replay.states.legacy.ordered_entries().next().is_none());

    let mut migrated = checkpoint();
    migrated.retained_context_replay = Some(replay.states.clone());
    // Surviving model records must not reintroduce evidence discarded by migration rollback.
    migrated.replacement_history = Some(vec![reply, inherited, assistant("rolled-back-reply", 5)]);
    assert_eq!(RetainedReplay::new(&migrated).states, replay.states);
}

#[test]
fn checkpoint_recovers_confirmed_deliveries_before_inherited_adoption() {
    let inherited_metadata = CodexHarnessMetadata {
        inherited_user_message: true,
        user_input_order: Some(42),
        ..Default::default()
    };
    let mut inherited = assistant("inherited", 40);
    inherited.metadata.as_mut().unwrap().inherited_user_message = true;
    let mut inherited_source = persist_source(&mut inherited);
    inherited_source.complete = false;
    inherited.metadata.as_mut().unwrap().retained_source = Some(inherited_source.clone());
    let mut base = checkpoint();
    base.replacement_history = Some(vec![
        inherited,
        call("inherited-call", "parent-turn", inherited_metadata),
        delivery("inherited-call"),
    ]);
    let replay = RetainedReplay::new(&base);
    assert!(
        replay
            .states
            .thread_owned_worker
            .ordered_entries()
            .next()
            .is_none()
    );
    assert!(replay.states.legacy.ordered_entries().next().is_none());
    let entries = replay
        .states
        .thread_owned_root
        .ordered_entries()
        .map(|(order, entry)| {
            let RetainedContextEntry::AssistantMessage(message) = entry else {
                panic!("expected assistant evidence");
            };
            (
                order,
                message.message_id.as_deref().unwrap(),
                message.turn_id.as_str(),
                message.text.as_str(),
                message.complete,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        entries,
        vec![
            (
                RetainedContextOrder::Inherited(0),
                "inherited-call",
                "parent-turn",
                "confirmed-inherited-call",
                true
            ),
            (
                RetainedContextOrder::Inherited(1),
                "inherited",
                "inherited",
                "inherited",
                false
            ),
        ]
    );
    let (_, adopted) = replay
        .states
        .thread_owned_root
        .ordered_entries()
        .next_back()
        .unwrap();
    assert_eq!(
        replay.states.thread_owned_root.source(adopted),
        Some(inherited_source)
    );
    base.retained_context_replay = Some(replay.states.clone());
    assert_eq!(RetainedReplay::new(&base).states, replay.states);
}

#[test]
fn replay_restores_revisions_only_for_matching_source_identity_and_completeness() {
    for mismatch in ["message", "turn", "role", "completeness"] {
        let mut response = assistant("original", 0);
        let mut saved = persist_source(&mut response);
        match mismatch {
            "message" => saved.id.message_id = "different".to_owned(),
            "turn" => saved.id.turn_id = "different".to_owned(),
            "role" => saved.id.role = codex_history::RetainedSourceRole::User,
            "completeness" => {
                let ResponseItem::Message { content, .. } = &mut response.item else {
                    unreachable!();
                };
                *content = vec![ContentItem::OutputText {
                    text: "oversized ".repeat(2_000),
                }];
            }
            _ => unreachable!(),
        }
        response.metadata.as_mut().unwrap().retained_source = Some(saved.clone());
        let mut replay = RetainedReplay::new(&checkpoint());
        replay.observe(&response);
        for context in [
            &replay.states.thread_owned_root,
            &replay.states.thread_owned_worker,
        ] {
            let (_, entry) = context
                .ordered_entries()
                .next()
                .expect("retained assistant");
            let restored = context.source(entry).unwrap();
            assert_ne!(restored.revision, saved.revision, "{mismatch}");
            assert_eq!(
                restored.id,
                codex_history::RetainedSourceId {
                    message_id: "original".to_owned(),
                    turn_id: "original".to_owned(),
                    role: codex_history::RetainedSourceRole::Assistant,
                }
            );
            assert_eq!(restored.complete, mismatch != "completeness");
        }
    }
}

#[test]
fn guardian_context_exclusion_keeps_assistants_developers_and_explicit_goals() {
    for (role, kind, expected) in [
        ("user", "guardian.retained_instructions", true),
        ("user", "user.goal", false),
        ("user", "user.goal.omitted", false),
        ("assistant", "guardian.retained_instructions", false),
        ("developer", "guardian.retained_instructions", false),
    ] {
        let item = serde_json::from_value(json!({
            "type": "message", "role": role,
            "content": [{"type": "input_text", "text": "<codex_internal_context>context</codex_internal_context>"}],
            "internal_chat_message_metadata_passthrough": {"content_item_kinds": [kind]},
        })).unwrap();
        assert_eq!(
            is_guardian_context_message(&item),
            expected,
            "{role}: {kind}"
        );
    }
}

#[test]
fn legacy_instruction_candidates_preserve_runtime_exclusions() {
    for (text, kind, expected) in [
        (format!("{SUMMARY_PREFIX}\nsummary"), "user.text", false),
        (SUMMARY_PREFIX.to_owned(), "user.text", true),
        (format!(" {SUMMARY_PREFIX}\nsummary"), "user.text", true),
        (
            " <user_action>tool action</user_action>".to_owned(),
            "user.text",
            false,
        ),
        (
            "<goal_context>runtime context</goal_context>".to_owned(),
            "user.text",
            false,
        ),
        (
            "Hidden runtime instruction.".to_owned(),
            "context.environment",
            false,
        ),
        (
            "Keep this original restriction.".to_owned(),
            "user.text",
            true,
        ),
    ] {
        let item = serde_json::from_value(json!({
            "type": "message", "role": "user", "id": "legacy-source",
            "content": [{"type": "input_text", "text": text}],
            "internal_chat_message_metadata_passthrough": {
                "turn_id": "legacy-turn", "content_item_kinds": [kind],
            },
        }))
        .unwrap();
        assert_eq!(
            legacy_user_message(&item).is_some(),
            expected,
            "{kind}: {text}"
        );
    }
    let oversized = user("x".repeat(4_000).as_str(), CodexHarnessMetadata::default());
    let retained = legacy_user_message(&oversized.item).expect("bounded original instruction");
    assert!(!retained.complete);
    assert_eq!(
        retained.text,
        truncate_text(&"x".repeat(4_000), MAX_RETAINED_USER_MESSAGE_TOKENS)
    );
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
        let mut expected = expected.clone();
        let adopted = &planner
            .candidate
            .as_ref()
            .unwrap()
            .retained_replay
            .states
            .thread_owned_root;
        for (_, entry) in adopted.ordered_entries() {
            let Some(source) = adopted.source(entry) else {
                panic!("adopted inherited message has a source");
            };
            if source.id.message_id != "A" {
                assert!(expected.thread_owned_root.restore_source_revision(&source));
            }
        }
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
                root_retains_legacy_transcript: Some(false),
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
            root_retains_legacy_transcript: Some(false),
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
                root_retains_legacy_transcript,
            } => {
                assert_eq!(*applicability, ReviewTranscriptApplicability::ThreadOwned);
                assert_eq!(*root_retains_legacy_transcript, Some(false));
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
                root_retains_legacy_transcript: Some(false),
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
