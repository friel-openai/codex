use super::*;
use codex_protocol::AgentPath;
use codex_protocol::protocol::InterAgentCommunication;

pub(crate) struct Handler;

impl ToolHandler for Handler {
    type Output = SupervisorSelfCloseResult;

    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "close_self")
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
        let args: SupervisorSelfCloseArgs = parse_arguments(&arguments)?;
        let Some(parent_thread_id) = session
            .services
            .agent_control
            .goal_supervisor_parent_for_helper(session.conversation_id)
            .await
        else {
            return Err(FunctionCallError::RespondToModel(
                "supervisor.close_self is only available in goal supervisor check-in threads."
                    .to_string(),
            ));
        };
        let state = session
            .services
            .agent_control
            .upgrade_for_tools()
            .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?;
        let parent_thread = state
            .get_thread(parent_thread_id)
            .await
            .map_err(|err| collab_agent_error(parent_thread_id, err))?;
        let message = args.message.filter(|message| !message.trim().is_empty());
        let agent_paths = if message.is_some() {
            let receiver_agent = session
                .services
                .agent_control
                .get_agent_metadata(parent_thread_id)
                .unwrap_or_default();
            let receiver_agent_path = receiver_agent.agent_path.unwrap_or_else(AgentPath::root);
            let sender_agent_path = session
                .services
                .agent_control
                .get_agent_config_snapshot(session.conversation_id)
                .await
                .and_then(|snapshot| snapshot.session_source.get_agent_path())
                .unwrap_or_else(AgentPath::root);
            Some((sender_agent_path, receiver_agent_path))
        } else {
            None
        };
        let goal = crate::goal_supervisor::complete_supervised_goal(
            &parent_thread.codex.session,
            session.conversation_id,
        )
        .await
        .map_err(|err| FunctionCallError::RespondToModel(format!("close_self failed: {err}")))?;
        if goal.is_some()
            && let Some(message) = message
            && let Some((sender_agent_path, receiver_agent_path)) = agent_paths
        {
            let communication = InterAgentCommunication::new(
                sender_agent_path,
                receiver_agent_path,
                Vec::new(),
                message,
                /*trigger_turn*/ true,
            );
            session
                .services
                .agent_control
                .send_inter_agent_communication(parent_thread_id, communication)
                .await
                .map_err(|err| collab_agent_error(parent_thread_id, err))?;
        }
        Ok(SupervisorSelfCloseResult {
            completed: goal.is_some(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct SupervisorSelfCloseArgs {
    message: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SupervisorSelfCloseResult {
    completed: bool,
}

impl ToolOutput for SupervisorSelfCloseResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "close_self")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn terminal_no_response(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "close_self")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "close_self")
    }
}
