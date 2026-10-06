//! Attributed agent-output delivery between loaded roots in one app-server.
//! Destination admission owns the atomic idle/start or active/steer decision.

use std::collections::BTreeMap;
use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;
use codex_protocol::turn_input::TurnInput;
use codex_protocol::turn_input::TurnInputSubmission;
use codex_protocol::turn_input::TurnStartOptions;
use codex_tools::JsonSchema;
use codex_tools::JsonToolOutput;
use codex_tools::ResponsesApiNamespace;
use codex_tools::ResponsesApiNamespaceTool;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolOutput;
use codex_tools::ToolSpec;
use serde::Deserialize;
use serde::Serialize;

use super::Host;
use super::encode_input;
use super::invalid;
use super::validate_prompt;
use crate::TurnInputRequest;
use crate::function_tool::FunctionCallError;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use crate::tools::registry::ToolRegistry;

pub(super) fn register(session: &Session, turn: &TurnContext, registry: &mut ToolRegistry) {
    if !turn.session_source.is_non_root_agent()
        && let Some(host) = session.services.thread_extension_data.get::<Host>()
    {
        registry.add(Handler { host });
    }
}

/// Weak host access keeps delivery local without keeping the manager alive.
struct Handler {
    host: Arc<Host>,
}

/// Model-controlled destination and content; sender identity is never an argument.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    thread_id: String,
    prompt: String,
}

/// Input acceptance is not evidence that the recipient completed the request.
#[derive(Serialize)]
struct Receipt {
    thread_id: ThreadId,
    source_thread_id: ThreadId,
    turn_id: String,
    status: Delivery,
}

/// The destination's atomic admission decision.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Delivery {
    Started,
    Steered,
}

impl Handler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
        let ToolPayload::Function { arguments } = &invocation.payload else {
            return Err(invalid(
                "send_message_to_thread requires function arguments",
            ));
        };
        let args: Args = parse_arguments(arguments)?;
        let thread_id = ThreadId::from_string(&args.thread_id).map_err(invalid)?;
        validate_prompt(&args.prompt)?;
        if invocation
            .session
            .session_source()
            .await
            .is_non_root_agent()
        {
            return Err(invalid(
                "only root threads may send independent thread messages",
            ));
        }
        let manager = self
            .host
            .manager
            .upgrade()
            .ok_or_else(|| invalid("messaging host is shutting down"))?;
        let destination = manager.get_thread(thread_id).await.map_err(|error| invalid(format!(
            "destination must be loaded in this app-server process; resume it in the host first: {error}"
        )))?;
        if destination
            .session
            .session_source()
            .await
            .is_non_root_agent()
        {
            return Err(invalid(
                "destination must be a root thread; use collaboration tools for subagents",
            ));
        }
        let source_thread_id = invocation.session.thread_id();
        let source_thread_name = if let Ok(source) = manager.get_thread(source_thread_id).await {
            source
                .read_thread(true, false)
                .await
                .ok()
                .and_then(|thread| thread.name)
                .filter(|name| name.len() <= 256)
        } else {
            None
        };
        // A standalone tool output is agent content, not a user request or a grant
        // of the sender's authority. Runtime admission assigns its durable item ID.
        let message = ResponseItem::FunctionCallOutput {
            id: None,
            call_id: None,
            name: Some("send_message_to_thread".to_string()),
            namespace: Some("frodex".to_string()),
            output: FunctionCallOutputPayload::from_text(encode_input(
                source_thread_id,
                source_thread_name.as_deref(),
                &args.prompt,
            )?),
            internal_chat_message_metadata_passthrough: None,
        };
        let submitted = destination
            .start_or_steer_turn(
                TurnInputRequest::new(TurnInput::ResponseItem(message)).on_start(
                    TurnStartOptions {
                        turn_trigger: Some("frodex_thread_message".to_string()),
                        ..Default::default()
                    },
                ),
            )
            .await
            .map_err(invalid)?;
        let (status, turn_id) = match submitted {
            TurnInputSubmission::Started { turn_id } => (Delivery::Started, turn_id),
            TurnInputSubmission::Steered { turn_id } => (Delivery::Steered, turn_id),
            TurnInputSubmission::NotSubmitted { reason } => {
                return Err(invalid(format!("message was not submitted: {reason:?}")));
            }
        };
        Ok(boxed_tool_output(JsonToolOutput::new(
            serde_json::to_value(Receipt {
                thread_id,
                source_thread_id,
                turn_id,
                status,
            })
            .map_err(invalid)?,
        )))
    }
}

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("frodex", "send_message_to_thread")
    }

    fn spec(&self) -> ToolSpec {
        ToolSpec::Namespace(ResponsesApiNamespace {
            name: "frodex".to_string(),
            description: "Tools for spawning and managing sub-agents.".to_string(),
            tools: vec![ResponsesApiNamespaceTool::Function(ResponsesApiTool {
                name: "send_message_to_thread".to_string(),
                description: "Send attributed agent output to a root thread already loaded in this app-server process. Starts an idle thread or steers an active turn without interrupting it; preserves destination settings. Use only for user-authorized communication. The receipt confirms acceptance, not completion. Other processes and subagents are unsupported; resume unloaded threads in the host first. Do not automatically retry uncertain delivery. Keep prompts concise (maximum 10,000 UTF-8 bytes); review prompts over 1,000 bytes for unnecessary copied context.".to_string(),
                strict: false,
                defer_loading: None,
                parameters: JsonSchema::object(BTreeMap::from([
                    ("thread_id".to_string(), JsonSchema::string(Some("Loaded destination root thread ID.".to_string()))),
                    ("prompt".to_string(), JsonSchema::string(Some("Agent message. The JSON-encoded message including sender attribution must fit 10000 UTF-8 bytes.".to_string()))),
                ]), Some(vec!["thread_id".to_string(), "prompt".to_string()]), Some(false.into())),
                output_schema: None,
            })],
        })
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(self.handle_call(invocation))
    }
}

impl CoreToolRuntime for Handler {}

#[cfg(test)]
#[path = "message_tests.rs"]
mod tests;
