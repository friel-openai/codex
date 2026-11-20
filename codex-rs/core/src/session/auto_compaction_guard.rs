use std::sync::Arc;

use codex_protocol::config_types::AutoCompactTokenLimitScope;
use codex_tools::ToolSpec;

use super::context_window::ContextWindowTokenStatus;

/// Identifies the budget and model-visible tools used after a compaction.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct CompactionBudgetKey {
    pub(super) model: String,
    pub(super) compaction_hash: Option<String>,
    pub(super) provider: String,
    pub(super) scope: AutoCompactTokenLimitScope,
    pub(super) auto_limit: Option<i64>,
    pub(super) full_limit: Option<i64>,
    pub(super) fallback_buffer: i64,
    pub(super) post_turn_percent: u8,
    pub(super) window_number: u64,
    pub(super) tools: Arc<[ToolSpec]>,
}

/// The estimated retained history immediately after a successful compaction.
pub(super) struct CompactionBaseline {
    pub(super) key: CompactionBudgetKey,
    pub(super) estimated_history_tokens: i64,
    pub(super) required_reduction: i64,
}

/// Tokens that must be removed to get below the currently active thresholds.
pub(super) fn required_reduction(
    status: &ContextWindowTokenStatus,
    fallback_buffer: i64,
    post_turn_percent: u8,
    post_turn: bool,
) -> i64 {
    let auto = status.auto_compact_scope_limit.map_or(0, |limit| {
        overage(
            status.auto_compact_scope_tokens,
            limit.saturating_add(fallback_buffer),
        )
    });
    let full = status
        .full_context_window_limit
        .map_or(0, |limit| overage(status.active_context_tokens, limit));
    let post = if post_turn && post_turn_percent > 0 {
        status.full_context_window_limit.map_or(0, |limit| {
            let threshold = (i128::from(limit) * i128::from(post_turn_percent) + 99) / 100;
            overage(
                status.active_context_tokens,
                i64::try_from(threshold).unwrap_or(i64::MAX),
            )
        })
    } else {
        0
    };
    auto.max(full).max(post)
}

fn overage(used: i64, limit: i64) -> i64 {
    if used >= limit {
        used.saturating_sub(limit).saturating_add(1)
    } else {
        0
    }
}

/// Fail closed when new history is too small to account for the required reduction.
pub(super) fn no_compactable_progress(
    after: i64,
    current: i64,
    previous_required: i64,
    current_required: i64,
) -> bool {
    let required = previous_required.min(current_required);
    required > 0 && current.saturating_sub(after).max(0) < required
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use codex_protocol::config_types::AutoCompactTokenLimitScope;
    use codex_tools::ToolSpec;

    use super::CompactionBudgetKey;
    use super::no_compactable_progress;

    #[test]
    fn detects_unchanged_overhead_but_allows_sufficient_new_history() {
        assert!(no_compactable_progress(8_600, 8_615, 10_028, 10_028));
        assert!(no_compactable_progress(8_600, 8_599, 10_028, 10_028));
        assert!(!no_compactable_progress(8_600, 18_628, 10_028, 20_056));
        assert!(!no_compactable_progress(8_600, 8_615, 10_028, 0));
        assert!(!no_compactable_progress(8_600, 8_615, 0, 10_028));
    }

    #[test]
    fn model_window_and_tool_changes_invalidate_baseline() {
        let key = CompactionBudgetKey {
            model: "model-a".into(),
            compaction_hash: None,
            provider: "provider".into(),
            scope: AutoCompactTokenLimitScope::Total,
            auto_limit: Some(20_000),
            full_limit: Some(95_000),
            fallback_buffer: 0,
            post_turn_percent: 0,
            window_number: 1,
            tools: Arc::from([]),
        };
        let mut changed = key.clone();
        changed.model = "model-b".into();
        assert_ne!(key, changed);
        let mut changed = key.clone();
        changed.full_limit = Some(190_000);
        assert_ne!(key, changed);
        let mut changed = key.clone();
        changed.auto_limit = Some(40_000);
        assert_ne!(key, changed);
        let mut changed = key.clone();
        changed.tools = Arc::from([ToolSpec::WebSearch {
            external_web_access: None,
            indexed_web_access: None,
            filters: None,
            user_location: None,
            search_context_size: None,
            search_content_types: None,
        }]);
        assert_ne!(key, changed);
    }
}
