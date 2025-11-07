use std::sync::Arc;

use crate::Prompt;
use crate::codex::Session;
use crate::codex::TurnContext;
use crate::error::Result as CodexResult;
use crate::protocol::CompactedItem;
use crate::protocol::ContextCompactedEvent;
use crate::protocol::EventMsg;
use crate::protocol::RolloutItem;
use crate::protocol::TaskStartedEvent;
use crate::user_instructions::USER_INSTRUCTIONS_OPEN_TAG_LEGACY;
use crate::user_instructions::USER_INSTRUCTIONS_PREFIX;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::ENVIRONMENT_CONTEXT_OPEN_TAG;

fn is_initial_context_item(item: &ResponseItem) -> bool {
    match item {
        ResponseItem::Message { role, content, .. } => {
            if role == "developer" {
                true
            } else if role == "user" {
                if let [ContentItem::InputText { text }] = content.as_slice() {
                    text.starts_with(USER_INSTRUCTIONS_PREFIX)
                        || text.starts_with(USER_INSTRUCTIONS_OPEN_TAG_LEGACY)
                        || text.starts_with(ENVIRONMENT_CONTEXT_OPEN_TAG)
                } else {
                    false
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

pub(crate) async fn run_inline_remote_auto_compact_task(
    sess: Arc<Session>,
    turn_context: Arc<TurnContext>,
) {
    run_remote_compact_task_inner(&sess, &turn_context).await;
}

pub(crate) async fn run_remote_compact_task(sess: Arc<Session>, turn_context: Arc<TurnContext>) {
    let start_event = EventMsg::TaskStarted(TaskStartedEvent {
        model_context_window: turn_context.client.get_model_context_window(),
    });
    sess.send_event(&turn_context, start_event).await;

    run_remote_compact_task_inner(&sess, &turn_context).await;
}

async fn run_remote_compact_task_inner(sess: &Arc<Session>, turn_context: &Arc<TurnContext>) {
    if let Err(err) = run_remote_compact_task_inner_impl(sess, turn_context).await {
        let event = EventMsg::Error(
            err.to_error_event(Some("Error running remote compact task".to_string())),
        );
        sess.send_event(turn_context, event).await;
    }
}

async fn run_remote_compact_task_inner_impl(
    sess: &Arc<Session>,
    turn_context: &Arc<TurnContext>,
) -> CodexResult<()> {
    let mut history = sess.clone_history().await;
    let prompt = Prompt {
        input: history.get_history_for_prompt(),
        tools: vec![],
        parallel_tool_calls: false,
        base_instructions_override: turn_context.base_instructions.clone(),
        output_schema: None,
    };

    let new_history = turn_context
        .client
        .compact_conversation_history(&prompt)
        .await?;
    // Required to keep `/undo` available after compaction
    let ghost_snapshots: Vec<ResponseItem> = history
        .get_history()
        .iter()
        .filter(|item| matches!(item, ResponseItem::GhostSnapshot { .. }))
        .cloned()
        .collect();

    // Re-apply the initial context (developer + AGENTS + environment) because the
    // remote compact service may omit it, and replay/resume uses the replacement
    // history verbatim.
    let mut rebuilt_history = sess.build_initial_context(turn_context);
    let compacted_without_context: Vec<ResponseItem> = new_history
        .into_iter()
        .skip_while(is_initial_context_item)
        .collect();
    rebuilt_history.extend(compacted_without_context);

    if !ghost_snapshots.is_empty() {
        rebuilt_history.extend(ghost_snapshots);
    }

    sess.replace_history(rebuilt_history.clone()).await;
    sess.recompute_token_usage(turn_context).await;

    let compacted_item = CompactedItem {
        message: String::new(),
        replacement_history: Some(rebuilt_history),
    };
    sess.persist_rollout_items(&[RolloutItem::Compacted(compacted_item)])
        .await;

    let event = EventMsg::ContextCompacted(ContextCompactedEvent {});
    sess.send_event(turn_context, event).await;

    Ok(())
}
