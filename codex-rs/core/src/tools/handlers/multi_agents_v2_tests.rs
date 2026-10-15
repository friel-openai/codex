use super::*;
use crate::agent::types::MessageDeliveryMode;
use crate::context::ContextualUserFragment;
use crate::context::InterAgentMessage;
use crate::context::InterAgentMessageType;
use crate::tools::context::ToolCallSource;
use codex_protocol::AgentPath;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::ResponseItem;
use pretty_assertions::assert_eq;

#[test]
fn code_mode_followup_rejects_unencrypted_message_argument() {
    let message = "Start the next canonical cycle immediately.";
    let error = match agent_message_from_tool(
        message.to_string(),
        &ToolCallSource::CodeMode {
            cell_id: "cell-1".to_string(),
            runtime_tool_call_id: "followup-parent-1".to_string(),
        },
    ) {
        Ok(_) => panic!("code mode cannot supply encrypted arguments"),
        Err(error) => error,
    };

    assert_eq!(
        error,
        FunctionCallError::RespondToModel(
            "collaboration tools with encrypted message arguments cannot be called from code mode; call the tool directly instead"
                .to_string(),
        )
    );
}

#[test]
fn direct_plaintext_followup_produces_plaintext_model_message() {
    let author = AgentPath::try_from("/root/goal_supervisor").expect("valid author path");
    let recipient = AgentPath::root();
    let message = "Start the next canonical cycle immediately.";
    let communication =
        agent_message_from_tool(message.to_string(), &ToolCallSource::DirectPlaintextMessage)
            .expect("the Responses backend marked the message as plaintext")
            .into_communication(
                author.clone(),
                recipient.clone(),
                MessageDeliveryMode::TriggerTurn,
            );
    let expected_text = InterAgentMessage::new(
        InterAgentMessageType::NewTask,
        recipient.clone(),
        author.clone(),
        message.to_string(),
    )
    .render();

    assert_eq!(
        communication.to_model_input_item(),
        ResponseItem::AgentMessage {
            id: None,
            author: author.to_string(),
            recipient: recipient.to_string(),
            content: vec![AgentMessageInputContent::InputText {
                text: expected_text,
            }],
            internal_chat_message_metadata_passthrough: None,
        }
    );
}

#[test]
fn direct_followup_preserves_encrypted_model_message() {
    let author = AgentPath::try_from("/root/worker").expect("valid author path");
    let recipient = AgentPath::root();
    let encrypted_message = "gAAAA-encrypted-message";
    let communication =
        agent_message_from_tool(encrypted_message.to_string(), &ToolCallSource::Direct)
            .expect("direct model calls carry encrypted arguments")
            .into_communication(
                author.clone(),
                recipient.clone(),
                MessageDeliveryMode::TriggerTurn,
            );

    assert_eq!(
        communication.to_model_input_item(),
        ResponseItem::AgentMessage {
            id: None,
            author: author.to_string(),
            recipient: recipient.to_string(),
            content: vec![
                AgentMessageInputContent::InputText {
                    text: format!(
                        "Message Type: NEW_TASK\nTask name: {recipient}\nSender: {author}\nPayload:\n"
                    ),
                },
                AgentMessageInputContent::EncryptedContent {
                    encrypted_content: encrypted_message.to_string(),
                },
            ],
            internal_chat_message_metadata_passthrough: None,
        }
    );
}
