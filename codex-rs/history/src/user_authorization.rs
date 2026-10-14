//! Shared bounded retention of user authorization and original assistant context.

use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::TruncationPolicy;

use crate::CodexHarnessMetadata;
use crate::RetainedContext;
use crate::RetainedSource;
use crate::RetainedUserMessage;
use crate::UserInputOrigin;
use crate::truncation::truncate_text;

/// Token budget for each retained message, matching delegated review excerpts.
pub const MAX_RETAINED_USER_MESSAGE_TOKENS: usize = 900;

/// Checkpoint items may have been shortened while preserving their original metadata.
#[derive(Clone, Copy)]
pub enum RetainedMessageSource {
    Original,
    Checkpoint,
}

/// Compatibility name for consumers that retain user authorization only.
pub type UserMessageSource = RetainedMessageSource;

/// Configuration updates require harness provenance; raw system messages are never retained.
pub fn is_api_message(message: &ResponseItem, metadata: Option<&CodexHarnessMetadata>) -> bool {
    match message {
        ResponseItem::Message { role, .. } => role.as_str() != "system",
        ResponseItem::ConfigurationUpdate { .. } => {
            metadata.is_some_and(|metadata| metadata.harness_authored_configuration)
        }
        ResponseItem::AdditionalTools { .. }
        | ResponseItem::AgentMessage { .. }
        | ResponseItem::FunctionCallOutput { .. }
        | ResponseItem::FunctionCall { .. }
        | ResponseItem::ToolSearchCall { .. }
        | ResponseItem::ToolSearchOutput { .. }
        | ResponseItem::CustomToolCall { .. }
        | ResponseItem::CustomToolCallOutput { .. }
        | ResponseItem::LocalShellCall { .. }
        | ResponseItem::Reasoning { .. }
        | ResponseItem::WebSearchCall { .. }
        | ResponseItem::ImageGenerationCall { .. }
        | ResponseItem::Compaction { .. }
        | ResponseItem::ContextCompaction { .. } => true,
        ResponseItem::CompactionTrigger { .. } => false,
        ResponseItem::Other => false,
    }
}

/// Uses host annotations rather than text markers to identify user authorization changes.
pub fn is_user_authorization_message(item: &ResponseItem) -> bool {
    let ResponseItem::Message {
        role,
        content,
        internal_chat_message_metadata_passthrough,
        ..
    } = item
    else {
        return false;
    };
    role == "user"
        && internal_chat_message_metadata_passthrough
            .as_ref()
            .and_then(|metadata| metadata.content_item_kinds.as_ref())
            .is_none_or(|kinds| {
                // Unknown, incomplete, and legacy messages remain conservative.
                kinds.is_empty()
                    || kinds.len() != content.len()
                    || kinds.iter().any(|kind| {
                        kind.0.starts_with("user.")
                            || matches!(
                                kind.0.as_str(),
                                "" | "unknown"
                                    // Media preparation can replace real user input.
                                    | "images.preparation_error"
                                    | "images.unsupported"
                                    | "audio.unsupported"
                            )
                    })
            })
}

/// Records admitted user authorization with its host acceptance order.
/// Returns whether the item is user authorization, even when retention deduplicates it.
pub fn record_user_authorization(
    context: &mut RetainedContext,
    item: &ResponseItem,
    metadata: Option<&CodexHarnessMetadata>,
    source: UserMessageSource,
) -> bool {
    if metadata.is_some_and(|metadata| metadata.compaction_output)
        || !is_user_authorization_message(item)
    {
        return false;
    }
    let _ = record_retained_message(context, item, metadata, source);
    true
}

/// Captures ordinary messages without deciding whether a worker adopts inherited context.
/// Checkpoint copies and previously incomplete sources cannot establish completeness.
pub fn record_retained_message(
    context: &mut RetainedContext,
    item: &ResponseItem,
    metadata: Option<&CodexHarnessMetadata>,
    source: RetainedMessageSource,
) -> Option<RetainedSource> {
    let is_assistant = matches!(item, ResponseItem::Message { role, .. } if role == "assistant");
    if metadata.is_some_and(|metadata| metadata.compaction_output)
        || (!is_assistant && !is_user_authorization_message(item))
    {
        return None;
    }
    let ResponseItem::Message {
        content,
        phase,
        internal_chat_message_metadata_passthrough,
        ..
    } = item
    else {
        return None;
    };
    let mut complete = matches!(source, RetainedMessageSource::Original)
        && (is_assistant
            || internal_chat_message_metadata_passthrough
                .as_ref()
                .and_then(|metadata| metadata.content_item_kinds.as_ref())
                .is_some_and(|kinds| {
                    kinds.len() == content.len()
                        && kinds.iter().all(|kind| {
                            kind.0.starts_with("user.") && kind.0 != "user.goal.omitted"
                        })
                }));
    complete &= metadata
        .and_then(|metadata| metadata.retained_source.as_ref())
        .is_none_or(|source| source.complete);
    let text = content
        .iter()
        .filter_map(|content| match content {
            ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                Some(text.as_str())
            }
            _ => {
                complete = false;
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    // Keep the same bounded text that child reviewers receive before
    // compaction, instead of letting storage discard a large source.
    // Local instruction sections still omit incomplete originals whole.
    complete &=
        text.len() <= TruncationPolicy::Tokens(MAX_RETAINED_USER_MESSAGE_TOKENS).byte_budget();
    let text = truncate_text(&text, MAX_RETAINED_USER_MESSAGE_TOKENS);
    let message = RetainedUserMessage {
        turn_id: item.turn_id().unwrap_or_default().to_owned(),
        message_id: item.id().map(|id| id.as_str().to_owned()),
        text,
        complete,
        phase: phase.clone(),
        origin: UserInputOrigin::from_message(item),
    };
    if is_assistant {
        context.record_assistant_message(message, metadata.into())
    } else {
        context.record_user_message(message, metadata.into())
    }
}

#[cfg(test)]
#[path = "user_authorization_tests.rs"]
mod tests;
