use super::*;
use crate::TurnStartOptions;
use crate::agent::api::AgentInput;
use crate::agent::api::AgentTarget;
use crate::agent::api::SendRequest;
use crate::agent::child_config::build_agent_resume_config;
use crate::agent::types::AgentMessage;
use crate::agent::types::MessageDeliveryMode;
use crate::tools::context::ToolCallSource;
use crate::tools::handlers::multi_agents_spec::create_supervisor_followup_parent_tool;
use crate::tools::handlers::multi_agents_spec::create_supervisor_tools_namespace;
use codex_tools::ToolSpec;

/// Delivers a Goal Supervisor result without extending `collaboration.*`.
pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "followup_parent")
    }

    fn spec(&self) -> ToolSpec {
        create_supervisor_tools_namespace(vec![create_supervisor_followup_parent_tool()])
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            handle_followup_parent(invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

async fn handle_followup_parent(
    invocation: ToolInvocation,
) -> Result<SupervisorFollowupParentResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        source,
        ..
    } = invocation;
    let arguments = function_arguments(payload)?;
    let args: SupervisorFollowupParentArgs = parse_arguments(&arguments)?;
    if args.message.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "Empty message can't be sent to the supervised parent".to_string(),
        ));
    }
    let message = plaintext_followup_message(args.message, &source)?;
    let Some(parent_thread_id) = session
        .services
        .agent_control
        .goal_supervisor_parent_for_helper(session.thread_id)
        .await
    else {
        return Err(FunctionCallError::RespondToModel(
            "supervisor.followup_parent is only available in goal supervisor check-in threads."
                .to_string(),
        ));
    };
    let resume_config =
        build_agent_resume_config(&turn).map_err(FunctionCallError::RespondToModel)?;
    session
        .services
        .agent_control
        .send_goal_supervisor_parent(SendRequest {
            caller: session.thread_id,
            target: AgentTarget::Id(parent_thread_id),
            resume_config,
            input: AgentInput::Message {
                message,
                mode: MessageDeliveryMode::TriggerTurn,
            },
            start_options: TurnStartOptions {
                parent_turn_id: Some(turn.sub_id.clone()),
                root_turn_id: turn.turn_metadata_state.root_turn_id(),
                turn_trigger: turn.turn_metadata_state.current_turn_trigger(),
                cyber_access_program: turn.cyber_access_program,
                ..Default::default()
            },
        })
        .await
        .map_err(|err| collab_agent_error(parent_thread_id, err))?;
    // The authenticated send records the accepted follow-up before the helper can retire.
    let delivered = session
        .services
        .agent_control
        .finish_goal_supervisor_helper_after_followup(session.thread_id)
        .await;
    if !delivered {
        return Err(FunctionCallError::RespondToModel(
            "The parent received the follow-up, but the goal supervisor check-in did not finish."
                .to_string(),
        ));
    }

    Ok(SupervisorFollowupParentResult { delivered })
}

/// Reject encrypted direct arguments before the controller can construct or deliver a message.
fn plaintext_followup_message(
    message: String,
    source: &ToolCallSource,
) -> Result<AgentMessage, FunctionCallError> {
    match source {
        ToolCallSource::DirectPlaintextMessage | ToolCallSource::CodeMode { .. } => {}
        ToolCallSource::Direct => {
            return Err(FunctionCallError::RespondToModel(
                "supervisor.followup_parent does not accept encrypted direct arguments."
                    .to_string(),
            ));
        }
    }
    Ok(AgentMessage::Plaintext(message))
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

/// Plaintext follow-up supplied by a goal supervisor for its authenticated direct parent.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorFollowupParentArgs {
    message: String,
}

/// Confirms accepted delivery and successful retirement of the supervisor helper.
#[derive(Debug, Serialize)]
struct SupervisorFollowupParentResult {
    delivered: bool,
}

impl ToolOutput for SupervisorFollowupParentResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "followup_parent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn terminal_no_response(&self) -> bool {
        self.delivered
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "followup_parent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "followup_parent")
    }
}

#[cfg(test)]
#[path = "supervisor_followup_parent_tests.rs"]
mod tests;
