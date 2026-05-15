use super::*;

pub(crate) struct Handler;

impl ToolHandler for Handler {
    type Output = SupervisorSnoozeResult;

    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "snooze")
    }

    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<Self::Output, FunctionCallError> {
        let ToolInvocation {
            session, payload, ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: SupervisorSnoozeArgs = parse_arguments(&arguments)?;
        let Some(delay_seconds) = session
            .services
            .agent_control
            .snooze_goal_supervisor_helper(session.conversation_id, args.delay_seconds)
            .await
        else {
            return Err(FunctionCallError::RespondToModel(
                "supervisor.snooze is only available in goal supervisor check-in threads."
                    .to_string(),
            ));
        };
        let _ = args.reason;
        Ok(SupervisorSnoozeResult { delay_seconds })
    }
}

#[derive(Debug, Deserialize)]
struct SupervisorSnoozeArgs {
    delay_seconds: Option<u64>,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SupervisorSnoozeResult {
    delay_seconds: u64,
}

impl ToolOutput for SupervisorSnoozeResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "snooze")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn terminal_no_response(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "snooze")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "snooze")
    }
}
