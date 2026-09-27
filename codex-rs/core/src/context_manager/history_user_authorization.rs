//! Captures local authorization and adopts copied instructions when a worker becomes a root.
//! Inherited history remains excluded for workers; roots also recover it from checkpoints.
//! Oversized originals retain bounded, incomplete excerpts for root review.
//! Checkpoint copies cannot establish original completeness, even when their text is short.

use std::sync::Arc;

use super::ContextManager;
use crate::context::GuardianContextMode;
use codex_history::CodexHarnessMetadata;
use codex_history::RetainedContext;
pub(super) use codex_history::UserMessageSource;
use codex_protocol::models::ResponseItem;

impl ContextManager {
    pub(crate) fn restore_retained_context(&mut self, checkpoint: Option<&RetainedContext>) {
        Arc::make_mut(&mut self.retained_context).restore(checkpoint, &self.items);
        if self.retain_inherited_user_messages
            && !self.retained_context.has_inherited_user_messages()
        {
            // Worker checkpoints intentionally omit copied instructions. A standalone
            // root adopts the surviving prefix without duplicating an adopted checkpoint.
            let items = Arc::clone(&self.items);
            for envelope in items.iter().filter(|envelope| {
                envelope
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.inherited_user_message)
            }) {
                self.record_user_authorization(
                    &envelope.item,
                    envelope.metadata.as_ref(),
                    UserMessageSource::Checkpoint,
                );
            }
        }
    }

    pub(super) fn record_user_authorization(
        &mut self,
        item: &ResponseItem,
        metadata: Option<&CodexHarnessMetadata>,
        source: UserMessageSource,
    ) {
        if !crate::context::is_user_authorization_message(item) {
            return;
        }
        let inherited = metadata.is_some_and(|metadata| metadata.inherited_user_message);
        if self.guardian_context_mode == GuardianContextMode::Legacy {
            Arc::make_mut(&mut self.retained_context).mark_user_messages_incomplete();
        } else if !inherited || self.retain_inherited_user_messages {
            codex_history::record_user_authorization(
                Arc::make_mut(&mut self.retained_context),
                item,
                metadata,
                source,
            );
        }
        self.user_message_revision = self.user_message_revision.saturating_add(1);
    }
}
