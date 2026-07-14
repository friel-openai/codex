use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::DynamicToolCallItem;
use codex_protocol::items::DynamicToolCallStatus;
use codex_protocol::items::ReasoningItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::CursorScope;
use super::HistoryCursor;
use crate::ListItemsParams;
use crate::SortDirection;

#[tokio::test]
async fn concatenated_referenced_item_pages_equal_full_logical_projection() {
    let home = TempDir::new().expect("temp dir");
    let compacted_id = ThreadId::default();
    let compacted_segment_id = SegmentId::new();
    let compacted_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(compacted_id.to_string())
        .join(compacted_segment_id.to_string())
        .join(format!("rollout-compacted-{compacted_id}.jsonl"));
    write_lines(
        compacted_path.as_path(),
        vec![
            line(0, session_meta(compacted_id, compacted_segment_id)),
            line(1, turn_started("turn-1")),
            line(2, completed_user(compacted_id, "turn-1", "user-1")),
            line(
                3,
                completed_reasoning(compacted_id, "turn-1", "reasoning-1"),
            ),
            line(4, completed_dynamic_tool(compacted_id, "turn-1", "tool-1")),
            line(5, completed_agent(compacted_id, "turn-1", "agent-1")),
            line(6, turn_completed("turn-1")),
        ],
    )
    .await;

    let parent_id = ThreadId::default();
    let parent_segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent_id.to_string())
        .join(parent_segment_id.to_string())
        .join(format!("rollout-parent-{parent_id}.jsonl"));
    write_lines(
        parent_path.as_path(),
        vec![
            line(7, session_meta(parent_id, parent_segment_id)),
            line(
                8,
                rollout_reference(compacted_path, compacted_id, compacted_segment_id),
            ),
            line(9, turn_started("turn-2")),
            line(10, completed_user(parent_id, "turn-2", "user-2")),
            line(11, completed_agent(parent_id, "turn-2", "agent-2")),
            line(12, turn_completed("turn-2")),
        ],
    )
    .await;

    let child_id = ThreadId::default();
    let child_path = home.path().join(format!("rollout-child-{child_id}.jsonl"));
    write_lines(
        child_path.as_path(),
        vec![
            line(13, session_meta(child_id, SegmentId::new())),
            line(
                14,
                rollout_reference(parent_path, parent_id, parent_segment_id),
            ),
            line(15, turn_started("turn-3")),
            line(16, completed_user(child_id, "turn-3", "user-3")),
            line(17, completed_agent(child_id, "turn-3", "agent-3")),
            line(18, turn_completed("turn-3")),
        ],
    )
    .await;

    let logical_lines = codex_rollout::materialize_rollout_lines(home.path(), child_path.as_path())
        .await
        .expect("materialize referenced rollout");
    let full_page = super::list_items(
        logical_lines.clone(),
        item_params(child_id, /*page_size*/ 10, /*cursor*/ None),
        item_scope(),
        /*cursor*/ None,
    )
    .expect("read full logical page");
    let expected_ids = full_page
        .items
        .iter()
        .map(|item| item.item_id.clone())
        .collect::<Vec<_>>();

    let mut actual_ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = super::list_items(
            logical_lines.clone(),
            item_params(child_id, /*page_size*/ 1, cursor.clone()),
            item_scope(),
            cursor
                .as_deref()
                .map(serde_json::from_str::<HistoryCursor>)
                .transpose()
                .expect("parse cursor"),
        )
        .expect("read logical page");
        actual_ids.extend(page.items.into_iter().map(|item| item.item_id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    assert_eq!(actual_ids, expected_ids);
    assert_eq!(
        expected_ids,
        vec![
            "user-1",
            "reasoning-1",
            "tool-1",
            "agent-1",
            "user-2",
            "agent-2",
            "user-3",
            "agent-3",
        ]
    );
}

fn item_params(thread_id: ThreadId, page_size: usize, cursor: Option<String>) -> ListItemsParams {
    ListItemsParams {
        thread_id,
        turn_id: None,
        include_archived: false,
        cursor,
        page_size,
        sort_direction: SortDirection::Asc,
    }
}

fn item_scope() -> CursorScope {
    CursorScope::Items
}

async fn write_lines(path: &std::path::Path, lines: Vec<RolloutLine>) {
    tokio::fs::create_dir_all(path.parent().expect("parent"))
        .await
        .expect("create rollout parent");
    let contents = lines
        .into_iter()
        .map(|line| serde_json::to_string(&line).expect("serialize rollout line"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    tokio::fs::write(path, contents)
        .await
        .expect("write rollout");
}

fn line(ordinal: u64, item: RolloutItem) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:00.000Z".to_string(),
        ordinal: Some(ordinal),
        item,
    }
}

fn session_meta(thread_id: ThreadId, segment_id: SegmentId) -> RolloutItem {
    RolloutItem::SessionMeta(SessionMetaLine {
        meta: SessionMeta {
            id: thread_id,
            session_id: thread_id.into(),
            segment_id: Some(segment_id),
            history_mode: ThreadHistoryMode::Paginated,
            timestamp: "2026-07-13T00:00:00.000Z".to_string(),
            ..Default::default()
        },
        git: None,
    })
}

fn rollout_reference(
    rollout_path: std::path::PathBuf,
    thread_id: ThreadId,
    segment_id: SegmentId,
) -> RolloutItem {
    RolloutItem::RolloutReference(RolloutReferenceItem {
        rollout_path,
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    })
}

fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_completed(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(10),
        completed_at: Some(20),
        duration_ms: Some(10_000),
        time_to_first_token_ms: None,
    }))
}

fn completed_user(thread_id: ThreadId, turn_id: &str, item_id: &str) -> RolloutItem {
    completed_item(
        thread_id,
        turn_id,
        TurnItem::UserMessage(UserMessageItem {
            id: item_id.to_string(),
            client_id: None,
            content: Vec::new(),
        }),
    )
}

fn completed_agent(thread_id: ThreadId, turn_id: &str, item_id: &str) -> RolloutItem {
    completed_item(
        thread_id,
        turn_id,
        TurnItem::AgentMessage(AgentMessageItem {
            id: item_id.to_string(),
            content: vec![AgentMessageContent::Text {
                text: item_id.to_string(),
            }],
            phase: None,
            memory_citation: None,
        }),
    )
}

fn completed_reasoning(thread_id: ThreadId, turn_id: &str, item_id: &str) -> RolloutItem {
    completed_item(
        thread_id,
        turn_id,
        TurnItem::Reasoning(ReasoningItem {
            id: item_id.to_string(),
            summary_text: vec!["reasoning summary".to_string()],
            raw_content: vec!["encrypted reasoning".to_string()],
        }),
    )
}

fn completed_dynamic_tool(thread_id: ThreadId, turn_id: &str, item_id: &str) -> RolloutItem {
    completed_item(
        thread_id,
        turn_id,
        TurnItem::DynamicToolCall(DynamicToolCallItem {
            id: item_id.to_string(),
            namespace: Some("functions".to_string()),
            tool: "lookup".to_string(),
            arguments: serde_json::json!({"key": "value"}),
            status: DynamicToolCallStatus::Completed,
            content_items: Some(vec![DynamicToolCallOutputContentItem::InputText {
                text: "tool output".to_string(),
            }]),
            success: Some(true),
            error: None,
            duration: None,
        }),
    )
}

fn completed_item(thread_id: ThreadId, turn_id: &str, item: TurnItem) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item,
        completed_at_ms: 1,
    }))
}
