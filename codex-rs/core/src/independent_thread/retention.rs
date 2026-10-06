//! Bounded preservation of independent-thread assignments without user authority.

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;

use crate::compact::content_items_to_text;
use crate::compact::is_summary_message;
use codex_history::ResponseItemEnvelope;
use codex_protocol::models::ResponseItem;

/// Recognizes admitted independent-thread input without treating it as user authorization.
pub(crate) fn is_independent_thread_message(item: &ResponseItem) -> bool {
    let ResponseItem::FunctionCallOutput {
        id: Some(_),
        call_id: None,
        name: Some(name),
        namespace: Some(namespace),
        output,
        ..
    } = item
    else {
        return false;
    };
    if namespace != "frodex" || !matches!(name.as_str(), "fork_thread" | "send_message_to_thread") {
        return false;
    }
    let Some(text) = output.body.to_text() else {
        return false;
    };
    // Names alone must not exempt arbitrary output from history budgets. Native
    // deliveries have a bounded prompt and a runtime-supplied source thread ID.
    if text.len() > 10_000 {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
        return false;
    };
    value
        .get("input")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|input| input.len() <= 10_000)
        && value
            .get("source_thread_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|source| codex_protocol::ThreadId::from_string(source).is_ok())
}

/// Preserve the current fork assignment and three recent peer messages as agent output.
/// The per-call input limit bounds retained content. Existing user-message order is unchanged,
/// and retained messages are inserted before their next surviving history item when possible.
pub(crate) fn retain_independent_thread_messages(
    previous_history: &[ResponseItemEnvelope],
    compacted_history: &mut Vec<ResponseItemEnvelope>,
) {
    let assignment = previous_history.iter().rposition(|item| {
        is_independent_thread_message(&item.item)
            && matches!(&item.item, ResponseItem::FunctionCallOutput { name: Some(name), .. } if name == "fork_thread")
    });
    let mut indices = previous_history.iter().enumerate().rev()
        .filter(|(_, item)| is_independent_thread_message(&item.item)
            && matches!(&item.item, ResponseItem::FunctionCallOutput { name: Some(name), .. } if name == "send_message_to_thread"))
        .take(3).map(|(index, _)| index).collect::<Vec<_>>();
    if let Some(assignment) = assignment {
        indices.push(assignment);
    }
    indices.sort_unstable();
    indices.dedup();
    if indices.is_empty() {
        return;
    }
    compacted_history.retain(|item| !is_independent_thread_message(&item.item));
    for index in indices {
        let insertion = previous_history[index + 1..].iter().find_map(|following| {
            compacted_history.iter().position(|item| item.item == following.item)
        }).unwrap_or_else(|| {
            compacted_history.last().filter(|item| {
                matches!(&item.item, ResponseItem::Compaction { .. } | ResponseItem::ContextCompaction { .. })
                    || matches!(&item.item, ResponseItem::Message { role, content, .. }
                        if role == "user" && content_items_to_text(content).is_some_and(|text| is_summary_message(&text)))
            }).map_or(compacted_history.len(), |_| compacted_history.len() - 1)
        });
        compacted_history.insert(insertion, previous_history[index].clone());
    }
}
