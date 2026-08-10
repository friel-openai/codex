use super::*;
use crate::tools::handlers::multi_agents_spec::create_close_agent_tool_v1;
use codex_protocol::AgentPath;
use codex_protocol::error::CodexErrorDetails;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced(MULTI_AGENT_V1_NAMESPACE, "close_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_close_agent_tool_v1()
    }

    fn search_info(&self) -> Option<ToolSearchInfo> {
        multi_agent_tool_search_info(
            "close_agent close shutdown stop agent subagent thread status target",
            self.spec(),
        )
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move { handle_close_agent(invocation).await.map(boxed_tool_output) })
    }
}

async fn handle_close_agent(
    invocation: ToolInvocation,
) -> Result<CloseAgentResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        call_id,
        ..
    } = invocation;
    let arguments = function_arguments(payload)?;
    let args: CloseAgentArgs = parse_arguments(&arguments)?;
    let agent_id = parse_agent_id_target(&args.target)?;
    let receiver_agent = session.services.agent_control.get_agent_metadata(agent_id);
    let receiver_agent = receiver_agent.unwrap_or_default();
    if agent_id == session.thread_id {
        return Err(FunctionCallError::RespondToModel(
            "an agent cannot close itself".to_string(),
        ));
    }
    if receiver_agent
        .agent_path
        .as_ref()
        .is_some_and(AgentPath::is_root)
    {
        return Err(FunctionCallError::RespondToModel(
            "root is not a spawned agent".to_string(),
        ));
    }
    session
        .emit_turn_item_started(
            &turn,
            &TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                id: call_id.clone(),
                tool: CollabAgentTool::CloseAgent,
                status: CollabAgentToolCallStatus::InProgress,
                sender_thread_id: session.thread_id,
                receiver_thread_ids: vec![agent_id],
                receiver_agents: Vec::new(),
                prompt: None,
                model: None,
                reasoning_effort: None,
                agents_states: Default::default(),
            }),
        )
        .await;
    let status = match session
        .services
        .agent_control
        .subscribe_status(agent_id)
        .await
    {
        Ok(mut status_rx) => status_rx.borrow_and_update().clone(),
        Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {
            session.services.agent_control.get_status(agent_id).await
        }
        Err(_) => session.services.agent_control.get_status(agent_id).await,
    };
    let result = Box::pin(
        session
            .services
            .agent_control
            .close_agent_subtree(session.thread_id, agent_id),
    )
    .await
    .map_err(|err| collab_agent_error(agent_id, err));
    session
        .emit_turn_item_completed(
            &turn,
            TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                id: call_id,
                tool: CollabAgentTool::CloseAgent,
                status: collab_tool_call_status(&status, Some(agent_id)),
                sender_thread_id: session.thread_id,
                receiver_thread_ids: vec![agent_id],
                receiver_agents: vec![CollabAgentRef {
                    thread_id: agent_id,
                    agent_nickname: receiver_agent.agent_nickname,
                    agent_role: receiver_agent.agent_role,
                }],
                prompt: None,
                model: None,
                reasoning_effort: None,
                agents_states: [(agent_id, status.clone())].into_iter().collect(),
            }),
        )
        .await;
    let report = result?;

    Ok(CloseAgentResult {
        previous_status: status,
        closed_agents: report.closed_agents,
        closed_edges: report.closed_edges,
        newly_closed_edges: report.newly_closed_edges,
        stopped_runtimes: report.stopped_runtimes,
        paused_goals: report.paused_goals,
        cleared_queued_items: report.cleared_queued_items,
        evicted_identities: report.evicted_identities,
    })
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CloseAgentResult {
    pub(crate) previous_status: AgentStatus,
    pub(crate) closed_agents: usize,
    pub(crate) closed_edges: usize,
    pub(crate) newly_closed_edges: usize,
    pub(crate) stopped_runtimes: usize,
    pub(crate) paused_goals: usize,
    pub(crate) cleared_queued_items: usize,
    pub(crate) evicted_identities: usize,
}

impl ToolOutput for CloseAgentResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "close_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "close_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "close_agent")
    }
}

#[derive(Debug, Deserialize)]
struct CloseAgentArgs {
    target: String,
}
