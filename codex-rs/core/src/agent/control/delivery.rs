//! Delivers captured agent input without exposing local loading and eviction to callers.
//!
//! Target checks precede reload, and queue-only messages retain their non-waking semantics.

use super::AgentInputDelivery;
use super::LocalAgentControl;
use crate::agent::api::AgentInput;
use crate::agent::api::DeliveryReceipt;
use crate::agent::api::SendRequest;
use crate::agent::types::AgentMessage;
use crate::agent::types::AgentMetadata;
use crate::agent::types::MessageDeliveryMode;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::context::ContextualUserFragment;
use crate::context::InterAgentMessage;
use crate::context::InterAgentMessageType;
use codex_protocol::AgentPath;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::InterAgentCommunication;

impl AgentMessage {
    pub(crate) fn into_communication(
        self,
        author: AgentPath,
        recipient: AgentPath,
        mode: MessageDeliveryMode,
    ) -> InterAgentCommunication {
        let trigger_turn = mode == MessageDeliveryMode::TriggerTurn;
        match self {
            Self::Encrypted(message) => InterAgentCommunication::new_encrypted(
                author,
                recipient,
                Vec::new(),
                message,
                trigger_turn,
            ),
            Self::Plaintext(message) => {
                let message_type = match mode {
                    MessageDeliveryMode::QueueOnly => InterAgentMessageType::Message,
                    MessageDeliveryMode::TriggerTurn => InterAgentMessageType::NewTask,
                };
                let content = InterAgentMessage::new(
                    message_type,
                    recipient.clone(),
                    author.clone(),
                    message,
                )
                .render();
                InterAgentCommunication::new(author, recipient, Vec::new(), content, trigger_turn)
            }
        }
    }
}

impl LocalAgentControl {
    /// Resolves and delivers captured input, restoring an evicted runtime when necessary.
    pub(crate) async fn send(&self, request: SendRequest) -> CodexResult<DeliveryReceipt> {
        self.send_inner(request, /*supervisor_parent*/ false).await
    }

    /// Only a live goal helper can wake its direct parent, including a root parent.
    pub(crate) async fn send_goal_supervisor_parent(
        &self,
        request: SendRequest,
    ) -> CodexResult<DeliveryReceipt> {
        let state = self.upgrade()?;
        let helper = state.get_thread(request.caller).await?;
        let target = self.resolve_target(request.caller, &request.target)?;
        if !helper.is_running()
            || !std::sync::Arc::ptr_eq(&self.state, &helper.session.services.agent_control.state)
            || !crate::goal_supervisor::is_goal_supervisor_helper_source(&helper.session_source)
            || helper.session_source.parent_thread_id() != Some(target)
            || !matches!(
                &request.input,
                AgentInput::Message {
                    mode: MessageDeliveryMode::TriggerTurn,
                    ..
                }
            )
        {
            return Err(CodexErr::UnsupportedOperation(
                "Only a goal supervisor helper can follow up with its direct parent".to_string(),
            ));
        }
        self.send_inner(request, /*supervisor_parent*/ true).await
    }

    async fn send_inner(
        &self,
        request: SendRequest,
        supervisor_parent: bool,
    ) -> CodexResult<DeliveryReceipt> {
        let SendRequest {
            caller,
            target,
            resume_config,
            input,
            mut start_options,
        } = request;
        let target = self.resolve_target(caller, &target)?;
        let (metadata, submission_id) = match input {
            AgentInput::UserInput(input) => {
                let receiver = self.get_agent_metadata(target);
                let submission_id = if receiver.is_some() {
                    self.deliver_input_to_agent(
                        resume_config,
                        target,
                        input,
                        AgentInputDelivery::Queue,
                        start_options,
                    )
                    .await?
                } else {
                    self.send_input(target, input, start_options).await?
                };
                (receiver.unwrap_or_default(), submission_id)
            }
            AgentInput::Message { message, mode } => {
                let registered_receiver = self.get_agent_metadata(target);
                let receiver_is_registered = registered_receiver.is_some();
                let mut receiver = match registered_receiver {
                    Some(receiver) => receiver,
                    None if supervisor_parent => AgentMetadata {
                        agent_id: Some(target),
                        ..Default::default()
                    },
                    None => return Err(CodexErr::ThreadNotFound(target)),
                };
                if supervisor_parent && receiver.agent_path.is_none() {
                    let parent = self.upgrade()?.get_thread(target).await?;
                    receiver.agent_path = Some(
                        parent
                            .session_source
                            .get_agent_path()
                            .unwrap_or_else(AgentPath::root),
                    );
                }
                let author = self
                    .ensure_agent_known(caller)?
                    .agent_path
                    .unwrap_or_else(AgentPath::root);
                if mode == MessageDeliveryMode::TriggerTurn
                    && receiver.agent_path.as_ref().is_some_and(AgentPath::is_root)
                    && !supervisor_parent
                {
                    return Err(CodexErr::UnsupportedOperation(
                        "Follow-up tasks can't target the root agent".to_string(),
                    ));
                }
                let receiver_path = receiver.agent_path.clone().ok_or_else(|| {
                    CodexErr::UnsupportedOperation(
                        "target agent is missing an agent_path".to_string(),
                    )
                })?;
                let communication = message.into_communication(author, receiver_path, mode);
                let delivered_parent_message = supervisor_parent.then(|| communication.clone());
                let kind = match mode {
                    MessageDeliveryMode::QueueOnly => {
                        start_options.parent_turn_id = None;
                        AgentCommunicationKind::Message
                    }
                    MessageDeliveryMode::TriggerTurn => AgentCommunicationKind::Followup,
                };
                let context = AgentCommunicationContext::new(kind, caller);
                let submission_id = if receiver_is_registered {
                    self.deliver_inter_agent_communication_to_agent(
                        resume_config,
                        target,
                        communication,
                        context,
                        AgentInputDelivery::Queue,
                        start_options,
                    )
                    .await?
                } else {
                    // A scheduler-loaded root need not be registered as a child.
                    self.send_inter_agent_communication(
                        target,
                        communication,
                        context,
                        start_options,
                    )
                    .await?
                };
                if let Some(communication) = delivered_parent_message {
                    self.record_goal_supervisor_followup_action(target, &communication)
                        .await;
                }
                (receiver, submission_id)
            }
        };
        Ok(DeliveryReceipt {
            thread_id: target,
            metadata,
            submission_id,
        })
    }
}
