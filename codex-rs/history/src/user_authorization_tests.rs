use super::*;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ContentItemKind;
use codex_protocol::models::ImageReference;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
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
        assert_eq!(context, expected);
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
        assert_eq!(context, expected);
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
    assert_eq!(context, expected);
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
        assert_eq!(context, expected);
    }
}
