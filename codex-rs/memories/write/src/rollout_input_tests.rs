use super::*;
use pretty_assertions::assert_eq;

#[test]
fn tiered_memory_input_preserves_messages_around_rollout_references() {
    let message = ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: "Keep the inherited decision.".to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    let reply = ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: "And keep the new result.".to_string(),
        }],
        phase: Some(MessagePhase::FinalAnswer),
        internal_chat_message_metadata_passthrough: None,
    };
    let reference = RolloutItem::RolloutReference(codex_protocol::protocol::RolloutReferenceItem {
        rollout_path: "structural-reference-not-memory-evidence.jsonl".into(),
        thread_id: None,
        rollout_id: None,
        rollout_timestamp: None,
        segment_id: None,
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    });
    let expected = format!(
        "[human user]\n{}\n[assistant final]\n{}\n",
        serde_json::to_string(&message).unwrap(),
        serde_json::to_string(&reply).unwrap(),
    );
    let items = [
        reference.clone(),
        RolloutItem::ResponseItem(message.into()),
        reference,
        RolloutItem::ResponseItem(reply.into()),
    ];

    assert_eq!(serialize_tiered_input(&items, 1_000).unwrap(), expected);
}

#[test]
fn extraction_chunks_preserve_unicode_evidence_with_bounded_messages() {
    let evidence = "User correction: 🐈\n".repeat(2_000);
    let mut reconstructed = String::new();
    for message in extraction_messages(&evidence) {
        let ResponseItem::Message { role, content, .. } = message else {
            panic!("message")
        };
        assert_eq!(role, "user");
        let [ContentItem::InputText { text }] = content.as_slice() else {
            panic!("text")
        };
        assert!(text.len() < 9_000);
        reconstructed.push_str(text);
    }
    assert_eq!(reconstructed, evidence);
}

#[test]
fn classifies_memory_excluded_fragments() {
    let cases = [
        (
            "# AGENTS.md instructions for /tmp\n\n<INSTRUCTIONS>\nbody\n</INSTRUCTIONS>",
            true,
        ),
        (
            "# AGENTS.md instructions\n\n<INSTRUCTIONS>\nbody\n</INSTRUCTIONS>",
            true,
        ),
        (
            "<skill>\n<name>demo</name>\n<path>skills/demo/SKILL.md</path>\nbody\n</skill>",
            true,
        ),
        (
            "<environment_context>\n<cwd>/tmp</cwd>\n</environment_context>",
            false,
        ),
        (
            "<subagent_notification>{\"agent_id\":\"a\",\"status\":\"completed\"}</subagent_notification>",
            false,
        ),
    ];

    for (text, expected) in cases {
        assert_eq!(
            is_memory_excluded_contextual_user_fragment(&ContentItem::InputText {
                text: text.to_string(),
            }),
            expected,
            "{text}",
        );
    }
}
