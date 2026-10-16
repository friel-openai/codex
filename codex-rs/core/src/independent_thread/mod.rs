//! Host-installed tools for independent root threads, outside agent-tree ownership.

mod fork;
mod message;
pub(crate) mod retention;

use std::sync::Arc;
use std::sync::Weak;

use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadStartInput;
use codex_protocol::ThreadId;
use serde::Serialize;

use crate::ThreadManager;
use crate::config::Config;
use crate::function_tool::FunctionCallError;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::tools::registry::ToolRegistry;

/// Installs process-local coordination without retaining the manager through its threads.
pub fn install(builder: &mut ExtensionRegistryBuilder<Config>, manager: Weak<ThreadManager>) {
    builder.thread_lifecycle_contributor(Arc::new(Host { manager }));
}

/// Access to the host registry; current root membership is checked at invocation.
struct Host {
    manager: Weak<ThreadManager>,
}

impl ThreadLifecycleContributor<Config> for Host {
    fn on_thread_start<'a>(
        &'a self,
        input: ThreadStartInput<'a, Config>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            // A child can later be promoted to an independent root. Store the capability,
            // but do not expose or execute either operation while it remains a child.
            input.thread_store.insert(Host {
                manager: self.manager.clone(),
            });
        })
    }
}

pub(crate) fn register(session: &Session, turn: &TurnContext, registry: &mut ToolRegistry) {
    fork::register(session, turn, registry);
    message::register(session, turn, registry);
}

/// Bound persisted agent input before creating a thread or accepting a delivery.
fn validate_prompt(prompt: &str) -> Result<(), FunctionCallError> {
    if prompt.trim().is_empty() {
        return Err(invalid("prompt must not be empty"));
    }
    if prompt.len() > 10_000 {
        return Err(invalid("prompt must not exceed 10000 UTF-8 bytes"));
    }
    Ok(())
}

/// Agent-authored input with identity supplied by the process rather than tool arguments.
#[derive(Serialize)]
struct AttributedInput<'a> {
    source_thread_id: ThreadId,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_thread_name: Option<&'a str>,
    input: &'a str,
}

/// Bound the actual stored fragment, including JSON escaping and sender attribution.
fn encode_input(
    source_thread_id: ThreadId,
    source_thread_name: Option<&str>,
    prompt: &str,
) -> Result<String, FunctionCallError> {
    validate_prompt(prompt)?;
    let text = serde_json::to_string(&AttributedInput {
        source_thread_id,
        source_thread_name,
        input: prompt,
    })
    .map_err(invalid)?;
    if text.len() > 10_000 {
        return Err(invalid(
            "JSON-encoded attribution and prompt must not exceed 10000 UTF-8 bytes",
        ));
    }
    Ok(text)
}

fn invalid(error: impl std::fmt::Display) -> FunctionCallError {
    FunctionCallError::RespondToModel(error.to_string())
}

#[cfg(test)]
#[path = "prompt_tests.rs"]
mod tests;
