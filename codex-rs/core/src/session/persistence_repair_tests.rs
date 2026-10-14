use super::*;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::FunctionCallOutputPayload;
use pretty_assertions::assert_eq;

#[test]
fn recovery_presentation_preserves_distinct_user_ids_and_assistant_revisions() {
    let user = ResponseItem::Message {
        id: None,
        role: "user".to_owned(),
        content: vec![ContentItem::InputText {
            text: "same text".to_owned(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    let first = recovery_turn_item(&user, "user-first".to_owned(), &HashMap::new()).unwrap();
    let second = recovery_turn_item(&user, "user-second".to_owned(), &HashMap::new()).unwrap();
    assert_eq!(
        (first.id(), second.id()),
        ("user-first".to_owned(), "user-second".to_owned())
    );

    let mut assistant = ResponseItem::Message {
        id: Some(ResponseItemId::with_suffix("msg", "updated")),
        role: "assistant".to_owned(),
        content: vec![ContentItem::OutputText {
            text: "old text".to_owned(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    let saved_key = recovery_item_key(&assistant).unwrap();
    if let ResponseItem::Message { content, .. } = &mut assistant {
        *content = vec![ContentItem::OutputText {
            text: "new text".to_owned(),
        }];
    }
    assert_ne!(saved_key, recovery_item_key(&assistant).unwrap());
    let item = recovery_turn_item(&assistant, "msg_updated".to_owned(), &HashMap::new()).unwrap();
    let expected = serde_json::json!({
        "type": "AgentMessage", "id": "msg_updated",
        "content": [{"type": "Text", "text": "new text"}]
    });
    assert_eq!(serde_json::to_value(item).unwrap(), expected);

    let hook = codex_protocol::items::build_hook_prompt_message(&[
        codex_protocol::items::HookPromptFragment::from_single_hook("Retained hook text", "hook-1"),
    ])
    .unwrap();
    let first = recovery_turn_item(&hook, "recovered-hook".to_owned(), &HashMap::new()).unwrap();
    let second = recovery_turn_item(&hook, "recovered-hook".to_owned(), &HashMap::new()).unwrap();
    assert_eq!(first.id(), "recovered-hook".to_owned());
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
}

#[test]
fn recovery_tool_output_keeps_payload_without_inventing_execution_status() {
    let response = ResponseItem::FunctionCallOutput {
        id: None,
        call_id: Some("call-1".to_owned()),
        name: Some("read_file".to_owned()),
        namespace: None,
        output: FunctionCallOutputPayload::from_text("retained output".to_owned()),
        internal_chat_message_metadata_passthrough: None,
    };
    let actual = recovery_turn_item(&response, "output-1".to_owned(), &HashMap::new()).unwrap();
    let expected = TurnItem::FunctionCallOutput(FunctionCallOutputItem {
        id: "output-1".to_owned(),
        name: "read_file".to_owned(),
        namespace: None,
        output: FunctionCallOutputBody::Text("retained output".to_owned()),
    });
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}
