use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;

use crate::GuardianHistoryCheckpoint;
use crate::MAX_RETAINED_USER_MESSAGE_TOKENS;
use crate::ResponseItemEnvelope;
use crate::RetainedContext;
use crate::RetainedContextEntry;
use crate::truncate_text;

/// A checkpoint without a complete transcript must not fall back to its partial model window.
/// The caller supplies its contextual-message predicate so runtime registries remain authoritative.
pub fn checkpoint_requires_parent_context(
    retained_context: Option<&RetainedContext>,
    checkpoint: Option<&GuardianHistoryCheckpoint>,
    items: &[ResponseItemEnvelope],
    is_contextual_user_message: impl Fn(&ResponseItem) -> bool,
) -> bool {
    checkpoint.is_none()
        && retained_context.is_some_and(|context| {
            !context.verified_answers_complete()
                || context.ordered_entries().any(|(_, entry)| match entry {
                    RetainedContextEntry::VerifiedAnswer(_) => true,
                    RetainedContextEntry::UserMessage(message) => !items.iter().any(|envelope| {
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
                        if role != "user" || is_contextual_user_message(item) {
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
                    }),
                })
        })
}
