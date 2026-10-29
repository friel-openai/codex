use codex_guardian_context::TranscriptHistory;
use codex_history::CodexHarnessMetadata;
use codex_history::GuardianHistoryCheckpoint;
use codex_history::ResponseItemEnvelope;
use codex_protocol::models::ResponseItem;
use codex_rollout::CompactedItem;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::apply_compaction_edits;

#[test]
fn checkpoint_rollback_preserves_guardian_ordering_without_changing_raw_history_rules() {
    let message =
        |role: &str, text: &str, order: Option<u64>, inherited: bool| ResponseItemEnvelope {
            item: serde_json::from_value(json!({
                "type": "message", "id": text, "role": role,
                "content": [{"type": "input_text", "text": text}],
            }))
            .unwrap(),
            metadata: Some(CodexHarnessMetadata {
                user_input_order: order,
                inherited_user_message: inherited,
                ..Default::default()
            }),
        };
    let initial = message("user", "initial", Some(0), false);
    let earlier = message("assistant", "earlier", Some(1), false);
    let later = message("assistant", "later", Some(4), false);
    let unknown = message("assistant", "unknown", None, false);
    let inherited = message("assistant", "inherited", None, true);
    let boundary = message("user", "queued", Some(2), false);
    let call = ResponseItemEnvelope::new(
        serde_json::from_value::<ResponseItem>(json!({
            "type": "function_call", "call_id": "call-1", "name": "exec", "arguments": "{}",
        }))
        .unwrap(),
    );
    let original = vec![
        initial.clone(),
        earlier.clone(),
        later,
        unknown.clone(),
        call.clone(),
        inherited.clone(),
        boundary.clone(),
    ];
    let mut transcript = TranscriptHistory::new(0);
    transcript.reset(original.iter());
    let mut checkpoint: CompactedItem =
        serde_json::from_value(json!({"message": "summary"})).unwrap();
    checkpoint.replacement_history = Some(original);
    checkpoint.guardian_history = Some(transcript.checkpoint());
    transcript.truncate_before(&boundary);

    apply_compaction_edits(0, &mut checkpoint, &[], &[1], false).unwrap();

    assert_eq!(checkpoint.guardian_history, Some(transcript.checkpoint()));
    assert_eq!(
        checkpoint.guardian_history,
        Some(GuardianHistoryCheckpoint(vec![
            initial.clone(),
            earlier.clone(),
            inherited.clone()
        ])),
    );
    assert_eq!(
        checkpoint.replacement_history,
        Some(vec![initial, earlier, unknown, call, inherited]),
    );
}
