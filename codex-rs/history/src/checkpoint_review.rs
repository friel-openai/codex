use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;

use crate::GuardianHistoryCheckpoint;
use crate::MAX_RETAINED_USER_MESSAGE_TOKENS;
use crate::ResponseItemEnvelope;
use crate::RetainedContext;
use crate::RetainedContextEntry;
use crate::truncate_text;

/// A checkpoint without a complete transcript must not fall back to its partial model window.
/// The caller supplies its Guardian-context predicate so runtime exclusions remain authoritative.
pub fn checkpoint_requires_parent_context(
    retained_context: Option<&RetainedContext>,
    checkpoint: Option<&GuardianHistoryCheckpoint>,
    items: &[ResponseItemEnvelope],
    is_guardian_context_message: impl Fn(&ResponseItem) -> bool,
) -> bool {
    checkpoint.is_none()
        && retained_context.is_some_and(|context| {
            !context.verified_answers_complete()
                || context.ordered_entries().any(|(_, entry)| {
                    let (message, expected_role) = match entry {
                        RetainedContextEntry::VerifiedAnswer(_) => return true,
                        RetainedContextEntry::UserMessage(message) => (message, "user"),
                        RetainedContextEntry::AssistantMessage(message) => (message, "assistant"),
                    };
                    !items.iter().any(|envelope| {
                        let item = &envelope.item;
                        if item.id().map(codex_protocol::ResponseItemId::as_str)
                            != message.message_id.as_deref()
                            || item.turn_id().unwrap_or_default() != message.turn_id
                        {
                            return false;
                        }
                        let ResponseItem::Message { role, content, .. } = item else {
                            return false;
                        };
                        if role != expected_role || is_guardian_context_message(item) {
                            return false;
                        }
                        let text = content
                            .iter()
                            .filter_map(|content| match content {
                                ContentItem::InputText { text }
                                | ContentItem::OutputText { text } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join("\n");
                        truncate_text(&text, MAX_RETAINED_USER_MESSAGE_TOKENS) == message.text
                    })
                })
        })
}
