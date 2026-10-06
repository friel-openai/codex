use super::*;
use codex_history::CodexHarnessMetadata;

#[test]
fn independent_thread_input_is_not_replaced_by_truncated_tool_output() {
    let envelope = ResponseItemEnvelope::new(ResponseItem::FunctionCallOutput {
        id: Some(codex_protocol::ResponseItemId::new("delivery")),
        call_id: None,
        name: Some("send_message_to_thread".to_string()),
        namespace: Some("frodex".to_string()),
        output: FunctionCallOutputPayload::from_text(
            serde_json::json!({
                "source_thread_id": "00000000-0000-7000-8000-000000000001",
                "input": "bounded request".repeat(100),
            })
            .to_string(),
        ),
        internal_chat_message_metadata_passthrough: None,
    });
    assert!(rewritten_output_for_context_window(&envelope).is_none());
    let processed = crate::context_manager::ContextManager::process_response_item_for_history(
        &envelope.item,
        None,
        codex_utils_output_truncation::TruncationPolicy::Tokens(1),
    );
    assert_eq!(processed, envelope.item);
}

#[test]
fn rewritten_output_preserves_harness_metadata() {
    let envelope = ResponseItemEnvelope {
        item: ResponseItem::FunctionCallOutput {
            id: None,
            call_id: Some("call-1".to_string()),
            name: None,
            namespace: None,
            output: FunctionCallOutputPayload {
                body: FunctionCallOutputBody::Text("large output".repeat(100)),
                success: Some(true),
            },
            internal_chat_message_metadata_passthrough: None,
        },
        metadata: Some(CodexHarnessMetadata::default()),
    };

    let rewritten = rewritten_output_for_context_window(&envelope)
        .expect("function output should be rewritten");

    assert_eq!(rewritten.metadata, envelope.metadata);
    assert_ne!(rewritten.item, envelope.item);
}
