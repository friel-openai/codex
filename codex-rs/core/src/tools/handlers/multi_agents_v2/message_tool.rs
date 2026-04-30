//! Shared argument parsing and dispatch for the v2 text-only agent messaging tools.
//!
//! `send_message` and `followup_task` share the same submission path and differ only in whether the
//! resulting `InterAgentCommunication` should wake the target immediately.

use super::*;
use crate::session::turn_context::TurnContext;
use crate::tools::context::FunctionToolOutput;
use codex_protocol::ThreadId;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MessageDeliveryMode {
    QueueOnly,
    TriggerTurn,
}

impl MessageDeliveryMode {
    /// Returns whether the produced communication should start a turn immediately.
    fn apply(self, communication: InterAgentCommunication) -> InterAgentCommunication {
        match self {
            Self::QueueOnly => InterAgentCommunication {
                trigger_turn: false,
                ..communication
            },
            Self::TriggerTurn => InterAgentCommunication {
                trigger_turn: true,
                ..communication
            },
        }
    }
}

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
    #[serde(default)]
    pub(crate) interrupt: bool,
}

fn message_content(message: String) -> Result<String, FunctionCallError> {
    if message.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "Empty message can't be sent to an agent".to_string(),
        ));
    }
    Ok(message)
}

/// Handles the shared MultiAgentV2 plain-text message flow for both `send_message` and `followup_task`.
pub(crate) async fn handle_message_string_tool(
    invocation: ToolInvocation,
    mode: MessageDeliveryMode,
    target: String,
    message: String,
    interrupt: bool,
) -> Result<FunctionToolOutput, FunctionCallError> {
    handle_message_submission(
        invocation,
        mode,
        target,
        message_content(message)?,
        interrupt,
    )
    .await
}

async fn handle_message_submission(
    invocation: ToolInvocation,
    mode: MessageDeliveryMode,
    target: String,
    prompt: String,
    interrupt: bool,
) -> Result<FunctionToolOutput, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        call_id,
        ..
    } = invocation;
    let target_is_parent = target == "parent";
    let receiver_thread_id = resolve_message_target(&session, &turn, &target).await?;
    let direct_parent_thread_id = direct_parent_thread_id(&turn.session_source);
    let is_direct_parent = direct_parent_thread_id == Some(receiver_thread_id);
    let watchdog_owner_thread_id = session
        .services
        .agent_control
        .watchdog_owner_for_active_helper(session.conversation_id)
        .await;
    let is_watchdog_parent = watchdog_owner_thread_id == Some(receiver_thread_id);
    let receiver_agent = session
        .services
        .agent_control
        .get_agent_metadata(receiver_thread_id)
        .unwrap_or_default();
    if session
        .services
        .agent_control
        .is_watchdog_handle(receiver_thread_id)
        .await
    {
        return Err(FunctionCallError::RespondToModel(
            "watchdog handles can't receive send_message or followup_task; watchdog check-ins run on the idle timer. Use close_agent to stop a watchdog."
                .to_string(),
        ));
    }
    if mode == MessageDeliveryMode::QueueOnly && is_watchdog_parent {
        return Err(FunctionCallError::RespondToModel(
            "watchdog check-in threads must use followup_task with target `parent` to message their parent."
                .to_string(),
        ));
    }
    if mode == MessageDeliveryMode::TriggerTurn
        && is_direct_parent
        && !is_watchdog_parent
        && target_is_parent
    {
        return Err(FunctionCallError::RespondToModel(
            "Only watchdog check-in threads can use followup_task with target `parent`; use send_message for parent updates."
                .to_string(),
        ));
    }
    if mode == MessageDeliveryMode::TriggerTurn
        && receiver_agent
            .agent_path
            .as_ref()
            .is_some_and(AgentPath::is_root)
        && !is_watchdog_parent
    {
        return Err(FunctionCallError::RespondToModel(
            "Tasks can't be assigned to the root agent".to_string(),
        ));
    }
    if mode == MessageDeliveryMode::TriggerTurn && is_direct_parent && !is_watchdog_parent {
        return Err(FunctionCallError::RespondToModel(
            "Only watchdog check-in threads can use followup_task with target `parent`; use send_message for parent updates."
                .to_string(),
        ));
    }
    if interrupt {
        session
            .services
            .agent_control
            .interrupt_agent(receiver_thread_id)
            .await
            .map_err(|err| collab_agent_error(receiver_thread_id, err))?;
    }
    session
        .send_event(
            &turn,
            CollabAgentInteractionBeginEvent {
                call_id: call_id.clone(),
                sender_thread_id: session.conversation_id,
                receiver_thread_id,
                prompt: prompt.clone(),
            }
            .into(),
        )
        .await;
    let receiver_agent_path = receiver_agent
        .agent_path
        .clone()
        .or_else(|| {
            is_direct_parent
                .then(|| direct_parent_path(&turn.session_source))
                .flatten()
        })
        .ok_or_else(|| {
            FunctionCallError::RespondToModel("target agent is missing an agent_path".to_string())
        })?;
    let communication = InterAgentCommunication::new(
        turn.session_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root),
        receiver_agent_path,
        Vec::new(),
        prompt.clone(),
        /*trigger_turn*/ true,
    );
    let result = session
        .services
        .agent_control
        .send_inter_agent_communication(receiver_thread_id, mode.apply(communication))
        .await
        .map_err(|err| collab_agent_error(receiver_thread_id, err));
    let status = session
        .services
        .agent_control
        .get_status(receiver_thread_id)
        .await;
    session
        .send_event(
            &turn,
            CollabAgentInteractionEndEvent {
                call_id,
                sender_thread_id: session.conversation_id,
                receiver_thread_id,
                receiver_agent_nickname: receiver_agent.agent_nickname,
                receiver_agent_role: receiver_agent.agent_role,
                prompt,
                status,
            }
            .into(),
        )
        .await;
    result?;
    if mode == MessageDeliveryMode::TriggerTurn && is_watchdog_parent {
        let _ = session
            .services
            .agent_control
            .finish_watchdog_helper(session.conversation_id)
            .await;
        session
            .services
            .agent_control
            .finish_watchdog_helper_thread(session.conversation_id)
            .await
            .map_err(|err| {
                FunctionCallError::RespondToModel(format!(
                    "failed to finish watchdog helper after followup_task: {err}"
                ))
            })?;
    }

    Ok(FunctionToolOutput::from_text(String::new(), Some(true)))
}

async fn resolve_message_target(
    session: &Arc<crate::session::session::Session>,
    turn: &Arc<TurnContext>,
    target: &str,
) -> Result<ThreadId, FunctionCallError> {
    if target == "parent" {
        return direct_parent_thread_id(&turn.session_source).ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "target `parent` is only available from a spawned agent.".to_string(),
            )
        });
    }
    resolve_agent_target(session, turn, target).await
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

fn direct_parent_path(session_source: &SessionSource) -> Option<AgentPath> {
    parent_path(session_source.get_agent_path()?.as_str())
}

fn parent_path(agent_path: &str) -> Option<AgentPath> {
    if agent_path == AgentPath::ROOT {
        return None;
    }
    let parent = agent_path.rsplit_once('/')?.0;
    if parent.is_empty() {
        return None;
    }
    AgentPath::try_from(parent).ok()
}
