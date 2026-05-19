use super::*;
use crate::tools::handlers::multi_agents_spec::create_supervisor_snooze_tool;
use crate::tools::handlers::multi_agents_spec::create_supervisor_tools_namespace;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

#[async_trait::async_trait]
impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "snooze")
    }

    fn spec(&self) -> Option<ToolSpec> {
        Some(create_supervisor_tools_namespace(vec![
            create_supervisor_snooze_tool(),
        ]))
    }

    async fn handle(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        handle_snooze(invocation).await.map(boxed_tool_output)
    }
}

async fn handle_snooze(
    invocation: ToolInvocation,
) -> Result<SupervisorSnoozeResult, FunctionCallError> {
    let ToolInvocation {
        session, payload, ..
    } = invocation;
    let arguments = function_arguments(payload)?;
    let args: SupervisorSnoozeArgs = parse_arguments(&arguments)?;
    let parent_thread_id = session
        .services
        .agent_control
        .goal_supervisor_parent_for_helper(session.conversation_id)
        .await;
    let Some(delay_seconds) = session
        .services
        .agent_control
        .snooze_goal_supervisor_helper(session.conversation_id, args.delay_seconds)
        .await
    else {
        return Err(FunctionCallError::RespondToModel(
            "supervisor.snooze is only available in goal supervisor check-in threads.".to_string(),
        ));
    };
    let _ = args.reason;
    if let Some(parent_thread_id) = parent_thread_id
        && let Err(err) = session
            .services
            .agent_control
            .send_goal_supervisor_snooze_event(parent_thread_id, delay_seconds)
            .await
    {
        tracing::warn!("failed to publish goal supervisor snooze event: {err}");
    }
    Ok(SupervisorSnoozeResult { delay_seconds })
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
struct SupervisorSnoozeArgs {
    delay_seconds: u64,
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
