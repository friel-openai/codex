//! Shared argument parsing and dispatch for the v2 agent messaging tools.
//!
//! `send_message` and `followup_task` share the same submission path and differ only in whether the
//! resulting `InterAgentCommunication` should wake the target immediately.

use super::analytics::ToolCallAnalytics;
use super::*;
use crate::TurnStartOptions;
use crate::agent::api::AgentInput;
use crate::agent::api::AgentTarget;
use crate::agent::api::SendRequest;
use crate::agent::child_config::build_agent_resume_config;
use crate::agent::types::MessageDeliveryMode;
use crate::tools::context::FunctionToolOutput;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Input for the MultiAgentV2 `send_message` tool.
pub(crate) struct SendMessageArgs {
    pub(crate) target: String,
    pub(crate) message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Input for the MultiAgentV2 `followup_task` tool.
pub(crate) struct FollowupTaskArgs {
    pub(crate) target: String,
    pub(crate) message: String,
}

pub(super) fn message_content(message: String) -> Result<String, FunctionCallError> {
    if message.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "Empty message can't be sent to an agent".to_string(),
        ));
    }
    Ok(message)
}

/// Handles the shared MultiAgentV2 message flow for both `send_message` and `followup_task`.
pub(super) async fn handle_message_string_tool(
    invocation: ToolInvocation,
    mode: MessageDeliveryMode,
    target: String,
    message: String,
    analytics: &mut ToolCallAnalytics,
) -> Result<FunctionToolOutput, FunctionCallError> {
    let message = message_content(message)?;
    let ToolInvocation {
        session,
        turn,
        call_id,
        source,
        ..
    } = invocation;
    let message = agent_message_from_tool(message, &source)?;
    let direct_parent_thread_id = direct_parent_thread_id(&turn.session_source);
    // Persisted pre-validation paths can exceed current AgentPath limits. Resolve the target first
    // so an owned child named `parent` remains addressable, then use the direct parent ID only as a
    // compatibility fallback for the reserved alias.
    let receiver_thread_id = match resolve_agent_target(&session, &turn, &target).await {
        Ok(receiver_thread_id) => receiver_thread_id,
        Err(_) if target == "parent" => direct_parent_thread_id
            .ok_or_else(|| FunctionCallError::RespondToModel("agent not found".to_string()))?,
        Err(err) => return Err(err),
    };
    analytics.set_receiver(receiver_thread_id);
    let resume_config =
        build_agent_resume_config(&turn).map_err(FunctionCallError::RespondToModel)?;
    let request = SendRequest {
        caller: session.thread_id,
        target: AgentTarget::Id(receiver_thread_id),
        resume_config,
        input: AgentInput::Message { message, mode },
        start_options: TurnStartOptions {
            parent_turn_id: (mode == MessageDeliveryMode::TriggerTurn).then(|| turn.sub_id.clone()),
            root_turn_id: turn.turn_metadata_state.root_turn_id(),
            turn_trigger: turn.turn_metadata_state.current_turn_trigger(),
            cyber_access_program: turn.cyber_access_program,
            ..Default::default()
        },
    };
    let receipt = session
        .services
        .agent_control
        .send(request)
        .await
        .map_err(|err| collab_v2_agent_error(receiver_thread_id, err))?;
    let receiver_agent_path = receipt.metadata.agent_path.ok_or_else(|| {
        FunctionCallError::RespondToModel("target agent is missing an agent_path".to_string())
    })?;
    emit_sub_agent_activity(
        &session,
        &turn,
        SubAgentActivityItem {
            id: call_id,
            agent_thread_id: receiver_thread_id,
            agent_path: receiver_agent_path,
            kind: SubAgentActivityKind::Interacted,
        },
    )
    .await;
    Ok(FunctionToolOutput::from_text(String::new(), Some(true)))
}

fn direct_parent_thread_id(session_source: &SessionSource) -> Option<ThreadId> {
    match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        }) => Some(*parent_thread_id),
        SessionSource::Cli
        | SessionSource::VSCode
        | SessionSource::Exec
        | SessionSource::Mcp
        | SessionSource::Custom(_)
        | SessionSource::Internal(_)
        | SessionSource::SubAgent(SubAgentSource::Review)
        | SessionSource::SubAgent(SubAgentSource::Compact)
        | SessionSource::SubAgent(SubAgentSource::MemoryConsolidation)
        | SessionSource::SubAgent(SubAgentSource::Other(_))
        | SessionSource::Unknown => None,
    }
}
