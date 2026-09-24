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
    let target_is_parent = target == "parent";
    let direct_parent_thread_id = direct_parent_thread_id(&turn.session_source);
    let receiver_thread_id = if target_is_parent {
        direct_parent_thread_id.ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "target `parent` is only available from a spawned agent.".to_string(),
            )
        })?
    } else {
        resolve_agent_target(&session, &turn, &target).await?
    };
    analytics.set_receiver(receiver_thread_id);
    let is_goal_supervisor_parent = session
        .services
        .agent_control
        .goal_supervisor_parent_for_helper(session.thread_id)
        .await
        == Some(receiver_thread_id);
    if mode == MessageDeliveryMode::QueueOnly && is_goal_supervisor_parent {
        return Err(FunctionCallError::RespondToModel(
            "supervisor check-in threads must use followup_task with target `parent` to message their parent."
                .to_string(),
        ));
    }
    if mode == MessageDeliveryMode::TriggerTurn && target_is_parent && !is_goal_supervisor_parent {
        return Err(FunctionCallError::RespondToModel(
            "Only supervisor check-in threads can use followup_task with target `parent`; use send_message for parent updates."
                .to_string(),
        ));
    }
    let resume_config =
        build_agent_resume_config(&turn).map_err(FunctionCallError::RespondToModel)?;
    let request = SendRequest {
        caller: session.thread_id,
        target: AgentTarget::Id(receiver_thread_id),
        resume_config,
        input: AgentInput::Message {
            message: agent_message_from_tool(message, &source),
            mode,
        },
        start_options: TurnStartOptions {
            parent_turn_id: (mode == MessageDeliveryMode::TriggerTurn).then(|| turn.sub_id.clone()),
            root_turn_id: turn.turn_metadata_state.root_turn_id(),
            turn_trigger: turn.turn_metadata_state.current_turn_trigger(),
            cyber_access_program: turn.cyber_access_program,
            ..Default::default()
        },
    };
    let receipt = if is_goal_supervisor_parent {
        session
            .services
            .agent_control
            .send_goal_supervisor_parent(request)
            .await
    } else {
        session.services.agent_control.send(request).await
    }
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
    if mode == MessageDeliveryMode::TriggerTurn && is_goal_supervisor_parent {
        let _ = session
            .services
            .agent_control
            .finish_goal_supervisor_helper_after_followup(session.thread_id)
            .await;
    }

    let output = FunctionToolOutput::from_text(String::new(), Some(true));
    if mode == MessageDeliveryMode::TriggerTurn && is_goal_supervisor_parent {
        Ok(output.into_terminal_no_response())
    } else {
        Ok(output)
    }
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
