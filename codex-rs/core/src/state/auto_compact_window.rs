use codex_protocol::ContextWindowId;
use codex_protocol::protocol::TokenUsage;
use std::sync::Arc;

/// Allocates context window IDs. Clones share the same allocator.
pub(crate) type ContextWindowIdGenerator = Arc<dyn Fn() -> ContextWindowId + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AutoCompactWindowIds {
    pub(crate) first_window_id: ContextWindowId,
    pub(crate) previous_window_id: Option<ContextWindowId>,
    pub(crate) window_id: ContextWindowId,
}

impl AutoCompactWindowIds {
    pub(crate) fn new_initial(window_id: ContextWindowId) -> Self {
        Self {
            first_window_id: window_id.clone(),
            previous_window_id: None,
            window_id,
        }
    }
}

/// A proposed auto-compaction window transition that has not mutated session state.
///
/// Compaction renders initial context before it acquires the checkpoint transaction. Keeping the
/// prior and next identities together lets publication verify that rendered context still matches
/// the state it is about to commit.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "the prepared transition must be committed with the rendered compaction history"]
pub(crate) struct PreparedAutoCompactWindowAdvance {
    prior_window_number: u64,
    prior_ids: AutoCompactWindowIds,
    window_number: u64,
    ids: AutoCompactWindowIds,
    clear_prefill: bool,
}

impl PreparedAutoCompactWindowAdvance {
    pub(crate) fn ids(&self) -> AutoCompactWindowIds {
        self.ids.clone()
    }

    pub(crate) fn clear_prefill_on_commit(&mut self) {
        self.clear_prefill = true;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AutoCompactWindowSnapshot {
    pub(crate) prefill_input_tokens: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoCompactWindowPrefill {
    ServerObserved(i64),
    Estimated(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AutoCompactWindow {
    window_number: u64,
    ids: AutoCompactWindowIds,
    new_context_window_requested: bool,
    /// Absolute input-token baseline for the current compaction window.
    ///
    /// `body_after_prefix` subtracts this from later active-context usage. It is
    /// not the growth itself; server-observed usage replaces estimated
    /// resume/recompute baselines when available.
    prefill_input_tokens: Option<AutoCompactWindowPrefill>,
    token_budget_reminder_delivered: bool,
    auto_compact_fallback_delivered: bool,
}

impl AutoCompactWindow {
    pub(super) fn new_with_ids(ids: AutoCompactWindowIds) -> Self {
        Self {
            window_number: 0,
            ids,
            new_context_window_requested: false,
            prefill_input_tokens: None,
            token_budget_reminder_delivered: false,
            auto_compact_fallback_delivered: false,
        }
    }

    pub(super) fn clear_prefill(&mut self) {
        self.prefill_input_tokens = None;
    }

    pub(super) fn window_number(&self) -> u64 {
        self.window_number
    }

    pub(super) fn ids(&self) -> &AutoCompactWindowIds {
        &self.ids
    }

    pub(super) fn restore(&mut self, window_number: u64, ids: AutoCompactWindowIds) {
        self.window_number = window_number;
        self.ids = ids;
    }

    #[cfg(test)]
    pub(super) fn advance(&mut self, window_id: ContextWindowId) -> (u64, AutoCompactWindowIds) {
        let prepared = self.prepare_advance(window_id);
        self.commit_prepared_advance(prepared)
            .expect("freshly prepared auto-compaction window transition must commit")
    }

    pub(super) fn prepare_advance(
        &self,
        window_id: ContextWindowId,
    ) -> PreparedAutoCompactWindowAdvance {
        let mut ids = self.ids.clone();
        ids.previous_window_id = Some(ids.window_id);
        ids.window_id = window_id;
        PreparedAutoCompactWindowAdvance {
            prior_window_number: self.window_number,
            prior_ids: self.ids.clone(),
            window_number: self.window_number.saturating_add(1),
            ids,
            clear_prefill: false,
        }
    }

    pub(super) fn commit_prepared_advance(
        &mut self,
        prepared: PreparedAutoCompactWindowAdvance,
    ) -> Option<(u64, AutoCompactWindowIds)> {
        if self.window_number != prepared.prior_window_number || self.ids != prepared.prior_ids {
            return None;
        }

        self.window_number = prepared.window_number;
        self.ids = prepared.ids;
        self.new_context_window_requested = false;
        self.token_budget_reminder_delivered = false;
        self.auto_compact_fallback_delivered = false;
        if prepared.clear_prefill {
            self.clear_prefill();
        }
        Some((self.window_number, self.ids.clone()))
    }

    pub(super) fn claim_token_budget_reminder(&mut self) -> bool {
        !std::mem::replace(&mut self.token_budget_reminder_delivered, true)
    }

    pub(super) fn claim_auto_compact_fallback(&mut self) -> bool {
        !std::mem::replace(&mut self.auto_compact_fallback_delivered, true)
    }

    pub(super) fn request_new_context_window(&mut self) {
        self.new_context_window_requested = true;
    }

    pub(super) fn take_new_context_window_request(&mut self) -> bool {
        let requested = self.new_context_window_requested;
        self.new_context_window_requested = false;
        requested
    }

    /// Records the request-input side of the first server usage sample. The
    /// sampled output from that response is body growth and should remain
    /// counted against the scoped auto-compact budget.
    pub(super) fn ensure_server_observed_prefill_from_usage(&mut self, usage: &TokenUsage) {
        if matches!(
            self.prefill_input_tokens,
            Some(AutoCompactWindowPrefill::ServerObserved(_))
        ) {
            return;
        }

        self.prefill_input_tokens = Some(AutoCompactWindowPrefill::ServerObserved(
            usage.input_tokens.max(0),
        ));
    }

    pub(super) fn set_estimated_prefill(&mut self, tokens: i64) {
        if matches!(
            self.prefill_input_tokens,
            Some(AutoCompactWindowPrefill::ServerObserved(_))
        ) {
            return;
        }

        self.prefill_input_tokens = Some(AutoCompactWindowPrefill::Estimated(tokens.max(0)));
    }

    pub(super) fn snapshot(&self) -> AutoCompactWindowSnapshot {
        let prefill_input_tokens = match self.prefill_input_tokens {
            Some(AutoCompactWindowPrefill::ServerObserved(tokens))
            | Some(AutoCompactWindowPrefill::Estimated(tokens)) => Some(tokens),
            None => None,
        };
        AutoCompactWindowSnapshot {
            prefill_input_tokens,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn prepared_window_clears_prefill_only_when_committed() {
        let mut window = AutoCompactWindow::new_with_ids(AutoCompactWindowIds::new_initial(
            ContextWindowId::new_uuid_v7(),
        ));
        window.set_estimated_prefill(150);
        let mut stale = window.prepare_advance(ContextWindowId::new_uuid_v7());
        stale.clear_prefill_on_commit();
        let accepted = window.prepare_advance(ContextWindowId::new_uuid_v7());
        assert!(window.commit_prepared_advance(accepted).is_some());
        assert!(window.commit_prepared_advance(stale).is_none());
        assert_eq!(window.snapshot().prefill_input_tokens, Some(150));
        let mut clearing = window.prepare_advance(ContextWindowId::new_uuid_v7());
        clearing.clear_prefill_on_commit();
        assert!(window.commit_prepared_advance(clearing).is_some());
        assert_eq!(window.snapshot().prefill_input_tokens, None);
    }

    #[test]
    fn tracks_prefill_and_window_boundaries() {
        let mut window = AutoCompactWindow::new_with_ids(AutoCompactWindowIds::new_initial(
            ContextWindowId::new_uuid_v7(),
        ));

        assert_eq!(window.window_number(), 0);
        let initial_window_id = window.ids().window_id.clone();
        assert_eq!(
            uuid::Uuid::parse_str(&initial_window_id.to_string())
                .unwrap()
                .get_version_num(),
            7
        );
        assert_eq!(
            window.ids(),
            &AutoCompactWindowIds {
                first_window_id: initial_window_id.clone(),
                previous_window_id: None,
                window_id: initial_window_id.clone(),
            }
        );
        let first_window_id = initial_window_id;
        let restored_window_id = ContextWindowId::new_uuid_v7();
        let restored_previous_window_id = ContextWindowId::new_uuid_v7();
        window.restore(
            /*window_number*/ 3,
            AutoCompactWindowIds {
                first_window_id: first_window_id.clone(),
                previous_window_id: Some(restored_previous_window_id),
                window_id: restored_window_id.clone(),
            },
        );
        assert_eq!(window.window_number(), 3);
        assert_eq!(window.ids().window_id, restored_window_id);
        assert!(window.claim_token_budget_reminder());
        assert!(!window.claim_token_budget_reminder());
        assert!(window.claim_auto_compact_fallback());
        assert!(!window.claim_auto_compact_fallback());
        window.request_new_context_window();
        assert!(window.take_new_context_window_request());
        assert!(!window.take_new_context_window_request());
        window.request_new_context_window();
        let next_window_id = ContextWindowId::new_uuid_v7();
        let (window_number, ids) = window.advance(next_window_id.clone());
        assert_eq!(window_number, 4);
        assert_eq!(window.window_number(), 4);
        assert_eq!(window.ids(), &ids);
        assert_eq!(ids.first_window_id, first_window_id);
        assert_eq!(ids.previous_window_id.as_ref(), Some(&restored_window_id));
        assert_eq!(ids.window_id, next_window_id);
        assert_ne!(ids.window_id, restored_window_id);
        assert!(!window.take_new_context_window_request());
        assert!(window.claim_token_budget_reminder());
        assert!(window.claim_auto_compact_fallback());

        assert_eq!(
            window.snapshot(),
            AutoCompactWindowSnapshot {
                prefill_input_tokens: None,
            }
        );

        window.set_estimated_prefill(/*tokens*/ 150);
        assert_eq!(
            window.snapshot(),
            AutoCompactWindowSnapshot {
                prefill_input_tokens: Some(150),
            }
        );

        window.ensure_server_observed_prefill_from_usage(&TokenUsage {
            input_tokens: 120,
            total_tokens: 170,
            ..Default::default()
        });
        assert_eq!(
            window.snapshot(),
            AutoCompactWindowSnapshot {
                prefill_input_tokens: Some(120),
            }
        );

        window.ensure_server_observed_prefill_from_usage(&TokenUsage {
            input_tokens: 130,
            total_tokens: 180,
            ..Default::default()
        });
        window.set_estimated_prefill(/*tokens*/ 90);
        assert_eq!(
            window.snapshot(),
            AutoCompactWindowSnapshot {
                prefill_input_tokens: Some(120),
            }
        );
    }
}
