use super::*;
use crate::agent::SupervisorParentCompactionResult;

pub(crate) struct Handler;

impl ToolHandler for Handler {
    type Output = CompactParentContextResult;

    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "compact_parent_context")
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
        let args: CompactParentContextArgs = parse_arguments(&arguments)?;
        let _ = (args.reason, args.evidence);
        let result = session
            .services
            .agent_control
            .compact_parent_for_goal_supervisor_helper(session.conversation_id)
            .await
            .map_err(|err| {
                FunctionCallError::RespondToModel(format!("compact_parent_context failed: {err}"))
            })?;
        if !matches!(
            &result,
            SupervisorParentCompactionResult::NotSupervisorHelper
        ) {
            let _ = session
                .services
                .agent_control
                .finish_goal_supervisor_helper(session.conversation_id)
                .await;
        }
        Ok(CompactParentContextResult::from(result))
    }
}

#[derive(Debug, Deserialize)]
struct CompactParentContextArgs {
    reason: Option<String>,
    evidence: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CompactParentContextResult {
    kind: &'static str,
    parent_thread_id: Option<String>,
    submission_id: Option<String>,
}

impl From<SupervisorParentCompactionResult> for CompactParentContextResult {
    fn from(value: SupervisorParentCompactionResult) -> Self {
        match value {
            SupervisorParentCompactionResult::NotSupervisorHelper => Self {
                kind: "not_supervisor_helper",
                parent_thread_id: None,
                submission_id: None,
            },
            SupervisorParentCompactionResult::ParentBusy { parent_thread_id } => Self {
                kind: "parent_busy",
                parent_thread_id: Some(parent_thread_id.to_string()),
                submission_id: None,
            },
            SupervisorParentCompactionResult::Submitted {
                parent_thread_id,
                submission_id,
            } => Self {
                kind: "submitted",
                parent_thread_id: Some(parent_thread_id.to_string()),
                submission_id: Some(submission_id),
            },
        }
    }
}

impl ToolOutput for CompactParentContextResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "compact_parent_context")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn terminal_no_response(&self) -> bool {
        self.kind != "not_supervisor_helper"
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "compact_parent_context")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "compact_parent_context")
    }
}
