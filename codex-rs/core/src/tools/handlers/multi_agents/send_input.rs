use super::*;
use crate::agent::control::render_input_preview;
use codex_protocol::AgentPath;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SessionSource;

pub(crate) struct Handler;

impl ToolHandler for Handler {
    type Output = SendInputResult;

    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<Self::Output, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            call_id,
            ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: SendInputArgs = parse_arguments(&arguments)?;
        let receiver_thread_id = parse_agent_id_target(&args.target)?;
        let message = args.message.clone();
        let items = args.items.clone();
        let input_items = parse_collab_input(args.message, args.items)?;
        let prompt = render_input_preview(&input_items);
        if session
            .services
            .agent_control
            .is_watchdog_handle(receiver_thread_id)
            .await
        {
            return Err(FunctionCallError::RespondToModel(
                "watchdog handles can't receive send_input; watchdog check-ins run on the idle timer. Use close_agent to stop a watchdog."
                    .to_string(),
            ));
        }
        let receiver_agent = session
            .services
            .agent_control
            .get_agent_metadata(receiver_thread_id)
            .unwrap_or_default();
        if args.interrupt {
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
        let agent_control = session.services.agent_control.clone();
        let sender_is_subagent = matches!(&turn.session_source, SessionSource::SubAgent(_));
        let result = match (sender_is_subagent, message, items) {
            (true, Some(message), None) => {
                let sender_path = turn
                    .session_source
                    .get_agent_path()
                    .or_else(|| {
                        agent_control
                            .get_agent_metadata(session.conversation_id)
                            .and_then(|metadata| metadata.agent_path)
                    })
                    .unwrap_or_else(|| fallback_agent_path(session.conversation_id));
                let receiver_path = receiver_agent
                    .agent_path
                    .clone()
                    .unwrap_or_else(|| fallback_agent_path(receiver_thread_id));
                agent_control
                    .send_inter_agent_communication(
                        receiver_thread_id,
                        InterAgentCommunication::new(
                            sender_path,
                            receiver_path,
                            Vec::new(),
                            message,
                            /*trigger_turn*/ true,
                        ),
                    )
                    .await
            }
            _ => {
                agent_control
                    .send_input(receiver_thread_id, input_items)
                    .await
            }
        }
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
        let submission_id = result?;

        Ok(SendInputResult { submission_id })
    }
}

fn fallback_agent_path(thread_id: ThreadId) -> AgentPath {
    let name = format!("thread_{}", thread_id.to_string().replace('-', "_"));
    AgentPath::root()
        .join(&name)
        .unwrap_or_else(|_| AgentPath::root())
}

#[derive(Debug, Deserialize)]
struct SendInputArgs {
    target: String,
    message: Option<String>,
    items: Option<Vec<UserInput>>,
    #[serde(default)]
    interrupt: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct SendInputResult {
    submission_id: String,
}

impl ToolOutput for SendInputResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "send_input")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "send_input")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "send_input")
    }
}
