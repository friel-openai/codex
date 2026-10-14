use super::*;
use crate::compact::collect_annotated_user_messages;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use pretty_assertions::assert_eq;
use serde_json::json;

fn independent_thread_input(name: &str, index: usize) -> ResponseItemEnvelope {
    ResponseItemEnvelope::new(ResponseItem::FunctionCallOutput {
        id: Some(ResponseItemId::with_suffix("delivery", index)),
        call_id: None,
        name: Some(name.to_string()),
        namespace: Some("frodex".to_string()),
        output: FunctionCallOutputPayload::from_text(
            json!({
                "source_thread_id": "00000000-0000-7000-8000-000000000001",
                "input": format!("assignment-{index}"),
            })
            .to_string(),
        ),
        internal_chat_message_metadata_passthrough: None,
    })
}

#[test]
fn compaction_retains_bounded_independent_messages_without_user_authority() {
    let history = (0..6)
        .map(|index| {
            independent_thread_input(
                if index == 0 {
                    "fork_thread"
                } else {
                    "send_message_to_thread"
                },
                index,
            )
        })
        .collect::<Vec<_>>();
    let mut compacted = Vec::new();
    retain_independent_thread_messages(&history, &mut compacted);
    assert_eq!(
        compacted,
        vec![
            history[0].clone(),
            history[3].clone(),
            history[4].clone(),
            history[5].clone()
        ]
    );
    assert!(
        compacted
            .iter()
            .all(|item| matches!(&item.item, ResponseItem::FunctionCallOutput { .. }))
    );
    assert!(collect_annotated_user_messages(&compacted).is_empty());
    let items = compacted
        .iter()
        .map(|envelope| &envelope.item)
        .collect::<Vec<_>>();
    let serialized = serde_json::to_string(&items).unwrap();
    let restored: Vec<ResponseItem> = serde_json::from_str(&serialized).unwrap();
    let restored = restored
        .into_iter()
        .map(ResponseItemEnvelope::new)
        .collect::<Vec<_>>();
    let mut compacted_again = Vec::new();
    retain_independent_thread_messages(&restored, &mut compacted_again);
    assert_eq!(compacted_again, compacted);
}

#[test]
fn compaction_keeps_independent_message_before_following_user_input() {
    let delivery = independent_thread_input("send_message_to_thread", 1);
    let user = ResponseItemEnvelope::new(ResponseItem::Message {
        id: Some(ResponseItemId::new("later-user")),
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: "newer user request".to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    });
    let history = vec![delivery.clone(), user.clone()];
    let mut compacted = vec![user.clone()];
    retain_independent_thread_messages(&history, &mut compacted);
    assert_eq!(compacted, vec![delivery, user]);
}

#[test]
fn nested_independent_fork_retains_current_assignment_not_ancestor() {
    let history = vec![
        independent_thread_input("fork_thread", 0),
        independent_thread_input("fork_thread", 1),
        independent_thread_input("send_message_to_thread", 2),
        independent_thread_input("send_message_to_thread", 3),
        independent_thread_input("send_message_to_thread", 4),
        independent_thread_input("send_message_to_thread", 5),
    ];
    let mut compacted = Vec::new();
    retain_independent_thread_messages(&history, &mut compacted);
    assert_eq!(
        compacted,
        vec![
            history[1].clone(),
            history[3].clone(),
            history[4].clone(),
            history[5].clone()
        ]
    );
}

#[test]
fn ancestor_assignments_do_not_consume_recent_message_slots() {
    let history = vec![
        independent_thread_input("send_message_to_thread", 0),
        independent_thread_input("send_message_to_thread", 1),
        independent_thread_input("fork_thread", 2),
        independent_thread_input("fork_thread", 3),
    ];
    let mut compacted = Vec::new();
    retain_independent_thread_messages(&history, &mut compacted);
    assert_eq!(
        compacted,
        vec![history[0].clone(), history[1].clone(), history[3].clone()]
    );
}

#[test]
fn independent_message_retention_rejects_regular_and_unbounded_outputs() {
    let mut message = independent_thread_input("send_message_to_thread", 1);
    assert!(is_independent_thread_message(&message.item));
    if let ResponseItem::FunctionCallOutput { call_id, .. } = &mut message.item {
        *call_id = Some("ordinary-tool-call".to_string());
    }
    assert!(!is_independent_thread_message(&message.item));
    if let ResponseItem::FunctionCallOutput {
        call_id, output, ..
    } = &mut message.item
    {
        *call_id = None;
        *output = FunctionCallOutputPayload::from_text(
            json!({
                "source_thread_id": "00000000-0000-7000-8000-000000000001",
                "input": "x".repeat(10_001),
            })
            .to_string(),
        );
    }
    assert!(!is_independent_thread_message(&message.item));
}
