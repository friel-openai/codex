use super::*;
use crate::RetainedContextEntry;
use crate::RetainedContextOrder;
use crate::RetainedSourceId;
use crate::RetainedSourceRole;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ContentItemKind;
use codex_protocol::models::ImageReference;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::models::MessagePhase;
use pretty_assertions::assert_eq;
use serde_json::json;

fn user_message(
    id: &str,
    content: Vec<ContentItem>,
    content_item_kinds: Option<Vec<&str>>,
) -> ResponseItem {
    ResponseItem::Message {
        id: Some(ResponseItemId::with_suffix("msg", id)),
        role: "user".to_owned(),
        content,
        phase: None,
        internal_chat_message_metadata_passthrough: Some(InternalChatMessageMetadataPassthrough {
            turn_id: Some("turn-1".to_owned()),
            content_item_kinds: content_item_kinds.map(|kinds| {
                kinds
                    .into_iter()
                    .map(|kind| ContentItemKind(kind.to_owned()))
                    .collect()
            }),
            ..Default::default()
        }),
    }
}

fn assert_context_matches(context: &RetainedContext, mut expected: RetainedContext) {
    // Fresh revisions are opaque; retain full checkpoint comparisons without pinning UUIDs.
    for (_, entry) in context.ordered_entries() {
        let source = context.source(entry).expect("captured source revision");
        assert!(expected.restore_source_revision(&source));
    }
    assert_eq!(context, &expected);
}

#[test]
fn mixed_media_user_authorization_preserves_text_and_marks_incomplete() {
    for kinds in [
        None,
        Some(vec!["user.text", "user.image", "user.text", "user.audio"]),
    ] {
        let item = user_message(
            "mixed-media",
            vec![
                ContentItem::InputText {
                    text: "Read the image.".to_owned(),
                },
                ContentItem::InputImage {
                    image: ImageReference::File {
                        file_id: "image-1".to_owned(),
                    },
                    detail: None,
                },
                ContentItem::OutputText {
                    text: "Do not publish the recording.".to_owned(),
                },
                ContentItem::InputAudio {
                    audio_url: "data:audio/wav;base64,AA==".to_owned(),
                },
            ],
            kinds,
        );
        let mut context = RetainedContext::default();
        assert!(record_user_authorization(
            &mut context,
            &item,
            /*metadata*/ None,
            UserMessageSource::Original,
        ));
        let expected: RetainedContext = serde_json::from_value(json!({
            "verified_answers": [],
            "incomplete": false,
            "user_messages": [{
                "order": 0,
                "turn_id": "turn-1",
                "message_id": "msg_mixed-media",
                "text": "Read the image.\nDo not publish the recording.",
                "complete": false
            }],
            "user_messages_incomplete": false,
            "next_order": 1
        }))
        .unwrap();
        assert_context_matches(&context, expected);
    }
}

#[test]
fn user_authorization_completeness_requires_original_user_annotations() {
    for (kinds, source, admitted, complete) in [
        (None, UserMessageSource::Original, true, false),
        (Some(vec![]), UserMessageSource::Original, true, false),
        (
            Some(vec!["user.text"]),
            UserMessageSource::Original,
            true,
            true,
        ),
        (
            Some(vec!["unknown"]),
            UserMessageSource::Original,
            true,
            false,
        ),
        (
            Some(vec!["user.text", "user.text"]),
            UserMessageSource::Original,
            true,
            false,
        ),
        (
            Some(vec!["user.text"]),
            UserMessageSource::Checkpoint,
            true,
            false,
        ),
        (
            Some(vec!["user.goal.omitted"]),
            UserMessageSource::Original,
            true,
            false,
        ),
        (
            Some(vec!["context.environment"]),
            UserMessageSource::Original,
            false,
            false,
        ),
    ] {
        let item = user_message(
            "restriction",
            vec![ContentItem::InputText {
                text: "Do not publish.".to_owned(),
            }],
            kinds,
        );
        let metadata = CodexHarnessMetadata {
            user_input_order: Some(7),
            ..Default::default()
        };
        let mut context = RetainedContext::default();
        assert_eq!(
            record_user_authorization(&mut context, &item, Some(&metadata), source),
            admitted,
        );
        let messages = if admitted {
            vec![json!({
                "order": 7,
                "turn_id": "turn-1",
                "message_id": "msg_restriction",
                "text": "Do not publish.",
                "complete": complete
            })]
        } else {
            vec![]
        };
        let expected: RetainedContext = serde_json::from_value(json!({
            "verified_answers": [],
            "incomplete": false,
            "user_messages": messages,
            "user_messages_incomplete": false,
            "next_order": if admitted { 8 } else { 0 }
        }))
        .unwrap();
        assert_context_matches(&context, expected);
    }
}

#[test]
fn inherited_user_authorization_preserves_local_acceptance_order() {
    let mut context = RetainedContext::default();
    for (id, order, inherited, source) in [
        ("local-first", Some(7), false, UserMessageSource::Original),
        (
            "parent-first",
            Some(100),
            true,
            UserMessageSource::Checkpoint,
        ),
        (
            "parent-second",
            Some(90),
            true,
            UserMessageSource::Checkpoint,
        ),
        ("local-second", None, false, UserMessageSource::Original),
    ] {
        let item = user_message(
            id,
            vec![ContentItem::InputText {
                text: "Keep this private.".to_owned(),
            }],
            Some(vec!["user.text"]),
        );
        let metadata = CodexHarnessMetadata {
            user_input_order: order,
            inherited_user_message: inherited,
            ..Default::default()
        };
        assert!(record_user_authorization(
            &mut context,
            &item,
            Some(&metadata),
            source,
        ));
    }
    let expected: RetainedContext = serde_json::from_value(json!({
        "verified_answers": [],
        "incomplete": true,
        "user_messages": [
            {
                "inherited": true, "order": 0,
                "turn_id": "turn-1", "message_id": "msg_parent-first",
                "text": "Keep this private.", "complete": false
            },
            {
                "inherited": true, "order": 1,
                "turn_id": "turn-1", "message_id": "msg_parent-second",
                "text": "Keep this private.", "complete": false
            },
            {
                "order": 7,
                "turn_id": "turn-1", "message_id": "msg_local-first",
                "text": "Keep this private.", "complete": true
            },
            {
                "order": 8,
                "turn_id": "turn-1", "message_id": "msg_local-second",
                "text": "Keep this private.", "complete": true
            }
        ],
        "user_messages_incomplete": false,
        "next_order": 9
    }))
    .unwrap();
    assert_context_matches(&context, expected);
}

#[test]
fn retained_user_authorization_truncates_at_900_tokens() {
    let at_budget = format!("{}{}", "a".repeat(/*n*/ 1_800), "b".repeat(/*n*/ 1_800));
    let truncated = format!(
        "{}<truncated omitted_approx_tokens=\"1\" />{}",
        "a".repeat(/*n*/ 1_780),
        "b".repeat(/*n*/ 1_781),
    );
    for (text, expected_text, complete) in [
        (at_budget.clone(), at_budget.clone(), true),
        (format!("{at_budget}b"), truncated, false),
    ] {
        let item = user_message(
            "bounded-message",
            vec![ContentItem::InputText { text }],
            Some(vec!["user.text"]),
        );
        let mut context = RetainedContext::default();
        assert!(record_user_authorization(
            &mut context,
            &item,
            /*metadata*/ None,
            UserMessageSource::Original,
        ));
        let expected: RetainedContext = serde_json::from_value(json!({
            "verified_answers": [],
            "incomplete": false,
            "user_messages": [{
                "order": 0,
                "turn_id": "turn-1",
                "message_id": "msg_bounded-message",
                "text": expected_text,
                "complete": complete
            }],
            "user_messages_incomplete": false,
            "next_order": 1
        }))
        .unwrap();
        assert_context_matches(&context, expected);
    }
}

#[test]
fn retained_messages_preserve_phase_origin_and_source() {
    for (role, expected_role, origin) in [
        ("user", RetainedSourceRole::User, UserInputOrigin::Heartbeat),
        (
            "assistant",
            RetainedSourceRole::Assistant,
            UserInputOrigin::User,
        ),
    ] {
        let mut item = user_message(
            "retained",
            vec![ContentItem::OutputText {
                text: "Keep this context.".to_owned(),
            }],
            Some(vec!["user.heartbeat"]),
        );
        if let ResponseItem::Message {
            role: item_role,
            phase,
            ..
        } = &mut item
        {
            *item_role = role.to_owned();
            *phase = Some(MessagePhase::Commentary);
        }
        let metadata = CodexHarnessMetadata {
            user_input_order: Some(7),
            ..Default::default()
        };
        let mut context = RetainedContext::default();
        let captured = record_retained_message(
            &mut context,
            &item,
            Some(&metadata),
            RetainedMessageSource::Original,
        )
        .expect("captured ordinary message");
        assert_eq!(
            captured.id,
            RetainedSourceId {
                message_id: "msg_retained".to_owned(),
                turn_id: "turn-1".to_owned(),
                role: expected_role,
            }
        );
        assert!(captured.complete);
        let entries = context.ordered_entries().collect::<Vec<_>>();
        assert_eq!(entries.len(), 1);
        let (order, entry) = entries[0];
        assert_eq!(order, RetainedContextOrder::Local(7));
        assert_eq!(context.source(entry), Some(captured));
        let message = match (role, entry) {
            ("user", RetainedContextEntry::UserMessage(message))
            | ("assistant", RetainedContextEntry::AssistantMessage(message)) => message,
            _ => panic!("retained source has the wrong role"),
        };
        assert_eq!(
            message,
            &RetainedUserMessage {
                turn_id: "turn-1".to_owned(),
                message_id: Some("msg_retained".to_owned()),
                text: "Keep this context.".to_owned(),
                complete: true,
                origin,
                phase: Some(MessagePhase::Commentary),
            }
        );
    }
}

#[test]
fn retained_messages_preserve_incompleteness_and_exclude_compaction_output() {
    for role in ["user", "assistant"] {
        for (source, compaction_output, saved_complete, expected_complete) in [
            (RetainedMessageSource::Original, false, None, Some(true)),
            (RetainedMessageSource::Original, true, None, None),
            (
                RetainedMessageSource::Checkpoint,
                false,
                Some(true),
                Some(false),
            ),
            (
                RetainedMessageSource::Original,
                false,
                Some(false),
                Some(false),
            ),
        ] {
            let mut item = user_message(
                "retained",
                vec![ContentItem::InputText {
                    text: "Keep this context.".to_owned(),
                }],
                Some(vec!["user.text"]),
            );
            if let ResponseItem::Message {
                role: item_role, ..
            } = &mut item
            {
                *item_role = role.to_owned();
            }
            let metadata = CodexHarnessMetadata {
                compaction_output,
                user_input_order: Some(7),
                retained_source: saved_complete.map(|complete| RetainedSource {
                    id: RetainedSourceId {
                        message_id: "msg_retained".to_owned(),
                        turn_id: "turn-1".to_owned(),
                        role: if role == "user" {
                            RetainedSourceRole::User
                        } else {
                            RetainedSourceRole::Assistant
                        },
                    },
                    revision: ResponseItemId::with_suffix("rev", "saved"),
                    complete,
                }),
                ..Default::default()
            };
            let mut context = RetainedContext::default();
            let captured = record_retained_message(&mut context, &item, Some(&metadata), source);
            assert_eq!(captured.map(|source| source.complete), expected_complete);
            assert_eq!(
                context.ordered_entries().count(),
                usize::from(expected_complete.is_some())
            );
        }
    }
}

#[test]
fn retained_assistant_message_bounds_text_and_marks_media_incomplete() {
    for content in [
        vec![ContentItem::OutputText {
            text: "a".repeat(/*n*/ 3_601),
        }],
        vec![
            ContentItem::OutputText {
                text: "Look at this image.".to_owned(),
            },
            ContentItem::InputImage {
                image: ImageReference::File {
                    file_id: "image-1".to_owned(),
                },
                detail: None,
            },
        ],
    ] {
        let expected_text = match &content[0] {
            ContentItem::OutputText { text } => {
                truncate_text(text, MAX_RETAINED_USER_MESSAGE_TOKENS)
            }
            _ => unreachable!(),
        };
        let item = ResponseItem::Message {
            id: Some(ResponseItemId::with_suffix("msg", "assistant")),
            role: "assistant".to_owned(),
            content,
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        };
        let mut context = RetainedContext::default();
        assert_eq!(
            record_retained_message(
                &mut context,
                &item,
                /*metadata*/ None,
                RetainedMessageSource::Original,
            ),
            None,
        );
        assert_eq!(context, RetainedContext::default());
        let metadata = CodexHarnessMetadata {
            user_input_order: Some(7),
            ..Default::default()
        };
        let captured = record_retained_message(
            &mut context,
            &item,
            Some(&metadata),
            RetainedMessageSource::Original,
        )
        .expect("captured bounded assistant message");
        assert!(!captured.complete);
        let (_, RetainedContextEntry::AssistantMessage(message)) =
            context.ordered_entries().next().unwrap()
        else {
            panic!("missing assistant message");
        };
        assert_eq!(message.text, expected_text);
        assert!(!message.complete);
    }
}

#[test]
fn checkpoint_matching_requires_original_role_identity_text_and_guardian_visibility() {
    for role in ["user", "assistant"] {
        let mut item = user_message(
            "checkpoint",
            vec![ContentItem::InputText {
                text: "Keep this context.".to_owned(),
            }],
            Some(vec!["user.text"]),
        );
        if let ResponseItem::Message {
            role: item_role, ..
        } = &mut item
        {
            *item_role = role.to_owned();
        }
        let mut context = RetainedContext::default();
        let metadata = CodexHarnessMetadata {
            user_input_order: Some(7),
            ..Default::default()
        };
        record_retained_message(
            &mut context,
            &item,
            Some(&metadata),
            RetainedMessageSource::Original,
        )
        .expect("captured checkpoint evidence");
        let envelope = crate::ResponseItemEnvelope {
            item,
            metadata: Some(metadata),
        };
        assert!(!crate::checkpoint_requires_parent_context(
            Some(&context),
            /*checkpoint*/ None,
            std::slice::from_ref(&envelope),
            |_| false,
        ));
        assert!(crate::checkpoint_requires_parent_context(
            Some(&context),
            /*checkpoint*/ None,
            std::slice::from_ref(&envelope),
            |_| true,
        ));
        assert!(crate::checkpoint_requires_parent_context(
            Some(&context),
            /*checkpoint*/ None,
            &[],
            |_| false,
        ));
        for field in ["role", "id", "turn_id", "text"] {
            let mut mismatched = envelope.clone();
            if let ResponseItem::Message {
                id,
                role,
                content,
                internal_chat_message_metadata_passthrough,
                ..
            } = &mut mismatched.item
            {
                match field {
                    "role" => *role = "developer".to_owned(),
                    "id" => *id = Some(ResponseItemId::with_suffix("msg", "other")),
                    "turn_id" => {
                        internal_chat_message_metadata_passthrough
                            .as_mut()
                            .unwrap()
                            .turn_id = Some("turn-other".to_owned())
                    }
                    "text" => {
                        *content = vec![ContentItem::InputText {
                            text: "Changed context.".to_owned(),
                        }]
                    }
                    _ => unreachable!(),
                }
            }
            assert!(
                crate::checkpoint_requires_parent_context(
                    Some(&context),
                    /*checkpoint*/ None,
                    &[mismatched],
                    |_| false,
                ),
                "mismatch in {field} must require parent context"
            );
        }
    }
}
