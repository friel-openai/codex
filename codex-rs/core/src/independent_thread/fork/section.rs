//! Optional Desktop sidebar placement through tools advertised by the client.
//!
//! Desktop owns section membership and host routing. Native thread-store sections
//! are not a substitute for that state. Each request is bounded; a timeout leaves
//! placement unconfirmed and never cancels the independently running fork.

use std::time::Duration;

use codex_protocol::ThreadId;
use codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem;
use codex_protocol::dynamic_tools::DynamicToolNamespaceTool;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_tools::ToolName;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;

use crate::tools::context::ToolInvocation;
use crate::tools::handlers::dynamic::request_dynamic_tool;

/// Maximum wait for one optional Desktop request, independent of fork execution.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Placement is independent of creation and assignment submission.
#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum Outcome {
    /// Desktop confirmed the move to the source section.
    Inherited {
        /// Section identifier returned by Desktop.
        section_id: String,
    },
    /// Desktop confirmed the move to the explicitly named destination.
    Placed {
        /// Destination identifier returned by Desktop.
        section_id: String,
        /// Whether this invocation created the destination before moving.
        created: bool,
    },
    /// Placement was not requested or required capabilities were absent.
    Skipped {
        /// Why no move was attempted.
        reason: String,
    },
    /// A supported lookup or move did not confirm placement.
    Failed {
        /// Failure or uncertainty; a timed-out move might still complete.
        reason: String,
    },
}

/// Places a saved fork without changing its execution outcome.
///
/// An explicit name overrides inheritance, including opt-out. Creation and move
/// are separate effects: an unsuccessful move can leave a newly created section.
/// Requests are never retried because a missing receipt can hide a completed effect.
pub(super) async fn place(
    invocation: &ToolInvocation,
    fork: ThreadId,
    inherit: bool,
    name: Option<&str>,
) -> Outcome {
    if name.is_none() && !inherit {
        return Outcome::Skipped {
            reason: "inherit_section is false".to_string(),
        };
    }
    let tools = &invocation.turn.dynamic_tools;
    let (Some(list), Some(move_tool)) = (
        advertised_tool(tools, "list_threads"),
        advertised_tool(tools, "move_thread_to_sidebar_section"),
    ) else {
        return Outcome::Skipped {
            reason: "Desktop list and move capabilities are not available".to_string(),
        };
    };
    let listing: Listing = match request(invocation, list, json!({"limit": 50}), "list").await {
        Ok(listing) => listing,
        Err(reason) => return Outcome::Failed { reason },
    };
    let source_id = invocation.session.thread_id().to_string();
    let Some(source) = listing
        .threads
        .iter()
        .chain(&listing.pinned_threads)
        .find(|thread| thread.id == source_id && thread.kind == "codex")
    else {
        return Outcome::Skipped {
            reason: "Desktop did not return the caller's host information".to_string(),
        };
    };
    let Some(host_id) = source.host_id.as_ref() else {
        return Outcome::Skipped {
            reason: "Desktop did not return the caller's host information".to_string(),
        };
    };
    let (section_id, created) = if let Some(name) = name {
        match named_destination(invocation, &listing, name).await {
            Ok(destination) => destination,
            Err(reason) => return Outcome::Failed { reason },
        }
    } else {
        match listing.source_section(&source_id) {
            Ok(Some(section)) => (section.section_id.clone(), false),
            Ok(None) => {
                return Outcome::Skipped {
                    reason: "caller has no Desktop sidebar section".to_string(),
                };
            }
            Err(reason) => return Outcome::Failed { reason },
        }
    };
    let fork_id = fork.to_string();
    let receipt: MoveReceipt = match request(
        invocation,
        move_tool,
        json!({
            "threadId": fork_id,
            "hostId": host_id,
            "sectionId": section_id,
        }),
        "move",
    )
    .await
    {
        Ok(receipt) => receipt,
        Err(reason) => {
            return Outcome::Failed {
                reason: format!("{reason}; destination section {section_id} (created: {created})"),
            };
        }
    };
    if receipt.thread_id != fork_id
        || receipt.section_id != section_id
        || receipt.host_id != *host_id
    {
        return Outcome::Failed {
            reason: format!(
                "Desktop did not confirm the requested fork placement; destination section {section_id} (created: {created})"
            ),
        };
    }
    if name.is_some() {
        Outcome::Placed {
            section_id: receipt.section_id,
            created,
        }
    } else {
        Outcome::Inherited {
            section_id: receipt.section_id,
        }
    }
}

/// Resolves a unique displayed name, creating only when the listing has no match.
async fn named_destination(
    invocation: &ToolInvocation,
    listing: &Listing,
    name: &str,
) -> Result<(String, bool), String> {
    let mut matches = listing
        .sections
        .iter()
        .filter(|section| section.name == name);
    let found = matches.next();
    if matches.next().is_some() {
        return Err(format!(
            "Desktop returned multiple sections named {name:?}; no destination selected"
        ));
    }
    if let Some(section) = found {
        return Ok((section.section_id.clone(), false));
    }
    let create = advertised_tool(&invocation.turn.dynamic_tools, "create_sidebar_section")
        .ok_or_else(|| "Desktop section creation capability is not available".to_string())?;
    let section: Section = request(invocation, create, json!({"name": name}), "create").await?;
    if section.name != name || section.section_id.is_empty() {
        return Err("Desktop did not confirm the requested section creation; inspect sections before retrying".to_string());
    }
    Ok((section.section_id, true))
}

/// Resolves only the supported app tool names in the advertised dynamic catalog.
fn advertised_tool(tools: &[DynamicToolSpec], name: &str) -> Option<ToolName> {
    for tool in tools {
        match tool {
            DynamicToolSpec::Namespace(namespace) if namespace.name == "codex_app"
                && namespace.tools.iter().any(|tool| matches!(tool, DynamicToolNamespaceTool::Function(function) if function.name == name)) => {
                    return Some(ToolName::namespaced("codex_app", name));
                }
            DynamicToolSpec::Function(function) if function.name == format!("codex_app__{name}") => {
                return Some(ToolName::new(None, function.name.clone()));
            }
            _ => {}
        }
    }
    None
}

/// Calls the existing client transport, completing pending bookkeeping on timeout.
async fn request<T: DeserializeOwned>(
    invocation: &ToolInvocation,
    tool: ToolName,
    arguments: Value,
    suffix: &str,
) -> Result<T, String> {
    // Native placement calls bypass tool registration, but not the caller's policy.
    if !invocation.session.tool_policy.allows(&tool) {
        return Err(
            "Desktop placement tool is not allowed by the caller's tool policy".to_string(),
        );
    }
    let call_id = format!("{}-section-{suffix}", invocation.call_id);
    let future = request_dynamic_tool(
        &invocation.session,
        &invocation.turn,
        call_id.clone(),
        tool,
        arguments,
    );
    tokio::pin!(future);
    let response = tokio::select! {
        response = &mut future => response,
        _ = tokio::time::sleep(REQUEST_TIMEOUT) => {
            // Resolve the existing pending request so the item has a terminal
            // event. A late client receipt cannot restart or duplicate the move.
            invocation.session.notify_dynamic_tool_response(&call_id, DynamicToolResponse {
                content_items: vec![DynamicToolCallOutputContentItem::InputText {
                    text: "Desktop placement request timed out; its outcome is unconfirmed".to_string(),
                }],
                success: false,
            }).await;
            future.await
        }
    }.ok_or_else(|| "Desktop placement request was cancelled".to_string())?;
    decode(response)
}

/// Decodes the app's JSON text without treating human error text as success.
fn decode<T: DeserializeOwned>(response: DynamicToolResponse) -> Result<T, String> {
    let text = response
        .content_items
        .into_iter()
        .filter_map(|item| match item {
            DynamicToolCallOutputContentItem::InputText { text } => Some(text),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    if !response.success {
        return Err(format!("Desktop placement request failed: {text}"));
    }
    serde_json::from_str(&text)
        .map_err(|error| format!("Desktop placement response is invalid: {error}"))
}

/// Fields owned by Desktop's list tool that are needed for placement.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Listing {
    /// Thread identities include routing, unlike sidebar item keys.
    threads: Vec<Thread>,
    /// Pinned threads are excluded from the ordinary thread page.
    #[serde(default)]
    pinned_threads: Vec<Thread>,
    /// Complete sidebar sections returned alongside the thread page.
    sections: Vec<Section>,
}

impl Listing {
    /// Sidebar keys carry a source label, not the public remote-host identifier.
    fn source_section(&self, thread_id: &str) -> Result<Option<&Section>, String> {
        let suffix = format!(":{thread_id}");
        let mut matches = self.sections.iter().filter(|section| {
            section
                .item_keys
                .iter()
                .any(|key| key.starts_with("codex:thread:") && key.ends_with(&suffix))
        });
        let section = matches.next();
        if matches.next().is_some() {
            return Err("Desktop returned ambiguous section membership".to_string());
        }
        Ok(section)
    }
}

/// Public thread routing supplied by Desktop, never inferred from sidebar keys.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Thread {
    /// Saved thread identifier.
    id: String,
    /// Only Codex threads are relevant to this operation.
    kind: String,
    /// Client routing for both source and the fork created on the same host.
    host_id: Option<String>,
}

/// Desktop-owned section membership.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Section {
    /// Stable section identifier accepted by the move tool.
    section_id: String,
    /// Displayed section name used for explicit destination selection.
    name: String,
    /// Desktop's opaque sidebar identities, including the saved thread suffix.
    item_keys: Vec<String>,
}

/// Confirmation returned by the move tool.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MoveReceipt {
    /// Host on which the moved fork resides.
    host_id: String,
    /// Moved fork identifier.
    thread_id: String,
    /// Destination confirmed by Desktop.
    section_id: String,
}

#[cfg(test)]
#[path = "section_tests.rs"]
mod tests;
