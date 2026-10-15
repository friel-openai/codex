use super::*;
use codex_protocol::AgentPath;
use pretty_assertions::assert_eq;

#[test]
fn direct_plaintext_and_code_mode_build_plaintext_followups() {
    for source in [
        ToolCallSource::DirectPlaintextMessage,
        ToolCallSource::CodeMode {
            cell_id: "cell-1".to_string(),
            runtime_tool_call_id: "runtime-1".to_string(),
        },
    ] {
        let message = plaintext_followup_message("continue the active goal".to_string(), &source)
            .expect("plaintext sources should be accepted");
        let communication = message.into_communication(
            AgentPath::try_from("/root/goal_supervisor").expect("valid helper path"),
            AgentPath::root(),
            MessageDeliveryMode::TriggerTurn,
        );

        assert!(communication.encrypted_content.is_none());
        assert!(communication.content.contains("continue the active goal"));
        assert!(communication.trigger_turn);
    }
}

#[test]
fn encrypted_direct_source_is_rejected_before_communication() {
    let error =
        plaintext_followup_message("opaque direct value".to_string(), &ToolCallSource::Direct)
            .err()
            .expect("encrypted direct source should be rejected before constructing a message");

    assert_eq!(
        error,
        FunctionCallError::RespondToModel(
            "supervisor.followup_parent does not accept encrypted direct arguments.".to_string()
        )
    );
}
