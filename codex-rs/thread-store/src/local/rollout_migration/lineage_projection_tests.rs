use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::MAX_ROLLOUT_LINE_BYTES;
use super::RolloutMigrationRateLimiter;
use super::lineage::plan_legacy_lineage;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_projection::BULK_PROJECTION_BUDGET;
use super::lineage_projection::try_project_staged_targets;
use super::lineage_stage::stage_legacy_lineage;
use super::tests::complete_projection_rows;
use super::tests::indexed_store;
use super::tests::user_message;
use super::tests::write_rollout;
use super::thread_history;

#[tokio::test]
async fn bulk_lineage_projection_checks_phase_budget_and_authenticated_coordinates() {
    let home = tempfile::tempdir().expect("Codex home");
    let source = write_rollout(
        home.path(),
        ThreadId::new(),
        SessionSource::Cli,
        vec![user_message("retained user message")],
    );
    let plan = plan_legacy_lineage(home.path(), &source)
        .await
        .expect("plan source");
    let stage = tempfile::tempdir().expect("private stage");
    let staged = stage_legacy_lineage(&plan, stage.path())
        .await
        .expect("canonical targets");
    let store = indexed_store(home.path()).await;
    let mut journal = LineageMigrationJournal::from_plan(&plan);
    let mut limiter =
        RolloutMigrationRateLimiter::new(/*max_mib_per_second*/ None).expect("unlimited migration");
    assert!(
        try_project_staged_targets(
            &store,
            &journal,
            /*complete_root*/ None,
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .is_err()
    );
    journal
        .record_staged_targets(&staged)
        .expect("durable targets");
    journal.verify_sources().await.expect("unchanged source");
    journal
        .verify_staged_targets()
        .await
        .expect("authenticated target");
    let target = journal.targets[0].rollout_id;
    assert!(
        !try_project_staged_targets(
            &store,
            &journal,
            Some(target),
            &mut limiter,
            /*memory_budget*/ 0
        )
        .await
        .expect("bounded fallback")
    );
    assert!(
        thread_history::projection_state(&store, target)
            .await
            .expect("projection state")
            .is_none()
    );

    let mut wrong_boundary = journal.clone();
    *wrong_boundary.targets[0]
        .byte_count
        .as_mut()
        .expect("byte boundary") += 1;
    assert!(
        try_project_staged_targets(
            &store,
            &wrong_boundary,
            Some(target),
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .is_err()
    );
    assert!(
        thread_history::projection_state(&store, target)
            .await
            .expect("projection state")
            .is_none()
    );

    assert!(
        try_project_staged_targets(
            &store,
            &journal,
            Some(target),
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .expect("bulk projection")
    );
    let reference = ThreadId::new();
    thread_history::reset_projection_for_replacement(
        &store, reference, /*next_rollout_ordinal*/ 0,
    )
    .await
    .expect("reference checkpoint");
    store
        .project_rollout_in_batches(
            reference,
            &staged.targets[0].staged_path,
            /*complete_root*/ None,
            &mut limiter,
        )
        .await
        .expect("ordered SQL writer");
    assert_eq!(
        complete_projection_rows(&store, target).await,
        complete_projection_rows(&store, reference).await
    );
}

#[tokio::test]
async fn bulk_lineage_projection_accepts_expanded_canonical_compaction() {
    let home = tempfile::tempdir().expect("Codex home");
    let checkpoint: CompactedItem = serde_json::from_value(json!({
        "message": "checkpoint",
        "replacement_history": [{
            "type": "message", "role": "developer",
            "content": [{"type": "input_text", "text": ""}],
        }],
        "window_number": 1,
        "resume_metadata": {},
        "retained_context": {
            "verified_answers": [], "incomplete": false,
            "user_messages": [{
                "order": 0, "turn_id": "retained-turn", "message_id": "retained-message",
                "text": "r".repeat(2048), "complete": true,
            }],
            "user_messages_incomplete": false, "next_order": 1,
        },
    }))
    .expect("modern source checkpoint");
    let mut checkpoint_line = RolloutLine {
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::Compacted(checkpoint),
    };
    let unpadded_bytes = serde_json::to_vec(&checkpoint_line)
        .expect("measure source checkpoint")
        .len()
        + 1;
    let RolloutItem::Compacted(checkpoint) = &mut checkpoint_line.item else {
        unreachable!();
    };
    let ResponseItem::Message { content, .. } = &mut checkpoint
        .replacement_history
        .as_mut()
        .expect("replacement history")[0]
        .item
    else {
        unreachable!();
    };
    let ContentItem::InputText { text } = &mut content[0] else {
        unreachable!();
    };
    // Rollback preserves this model payload and adds three bounded retained-context copies.
    // The source stays 1 KiB below the Legacy limit; the generated checkpoint exceeds it.
    *text = "x".repeat(MAX_ROLLOUT_LINE_BYTES - 1024 - unpadded_bytes);
    assert_eq!(
        serde_json::to_vec(&checkpoint_line)
            .expect("measure padded source checkpoint")
            .len()
            + 1,
        MAX_ROLLOUT_LINE_BYTES - 1024
    );
    let removed_response: ResponseItem = serde_json::from_value(json!({
        "type": "message", "role": "user", "id": "removed-message",
        "content": [{"type": "input_text", "text": "removed instruction"}],
        "internal_chat_message_metadata_passthrough": {"turn_id": "removed-turn"},
    }))
    .expect("temporary response");
    let source = write_rollout(
        home.path(),
        ThreadId::new(),
        SessionSource::Cli,
        vec![
            user_message("before expanded checkpoint"),
            checkpoint_line.item,
            RolloutItem::ResponseItem(removed_response.into()),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("after expanded checkpoint"),
        ],
    );
    assert!(
        std::fs::read_to_string(&source)
            .expect("source records")
            .split_inclusive('\n')
            .all(|record| record.len() <= MAX_ROLLOUT_LINE_BYTES)
    );
    let plan = plan_legacy_lineage(home.path(), &source)
        .await
        .expect("plan source");
    let stage = tempfile::tempdir().expect("private stage");
    let staged = stage_legacy_lineage(&plan, stage.path())
        .await
        .expect("canonical targets");
    assert_eq!(staged.targets.len(), 1);
    let staged_text =
        std::fs::read_to_string(&staged.targets[0].staged_path).expect("staged canonical records");
    let expanded = staged_text
        .lines()
        .find(|record| record.len() > MAX_ROLLOUT_LINE_BYTES)
        .expect("canonicalization expands a valid Legacy checkpoint beyond the source limit");
    let expanded = codex_rollout::parse_rollout_line(expanded).expect("expanded canonical record");
    let RolloutItem::Compacted(checkpoint) = expanded.item else {
        panic!("expanded record must be a generated checkpoint");
    };
    assert!(checkpoint.retained_context_replay.is_some());

    let mut journal = LineageMigrationJournal::from_plan(&plan);
    journal
        .record_staged_targets(&staged)
        .expect("durable targets");
    journal.verify_sources().await.expect("unchanged source");
    journal
        .verify_staged_targets()
        .await
        .expect("authenticated expanded target");
    let store = indexed_store(home.path()).await;
    let target = journal.targets[0].rollout_id;
    let mut limiter =
        RolloutMigrationRateLimiter::new(/*max_mib_per_second*/ None).expect("unlimited migration");
    assert!(
        try_project_staged_targets(
            &store,
            &journal,
            Some(target),
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .expect("bulk projection accepts expanded canonical checkpoint")
    );
    let reference = ThreadId::new();
    thread_history::reset_projection_for_replacement(
        &store, reference, /*next_rollout_ordinal*/ 0,
    )
    .await
    .expect("reference checkpoint");
    store
        .project_rollout_in_batches(
            reference,
            &staged.targets[0].staged_path,
            /*complete_root*/ None,
            &mut limiter,
        )
        .await
        .expect("ordered SQL writer accepts expanded canonical checkpoint");
    for rollout_id in [target, reference] {
        let position = thread_history::projection_state(&store, rollout_id)
            .await
            .expect("projection state")
            .expect("completed projection");
        assert_eq!(
            Some(position.next_ordinal),
            journal.targets[0].end_ordinal_exclusive
        );
        assert_eq!(
            Some(position.next_byte_offset),
            journal.targets[0].byte_count
        );
    }
    assert_eq!(
        complete_projection_rows(&store, target).await,
        complete_projection_rows(&store, reference).await
    );
    let items: Vec<String> = sqlx::query_scalar(
        "SELECT item_json FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(target.to_string())
    .fetch_all(
        store
            .thread_history_db()
            .await
            .expect("projection database"),
    )
    .await
    .expect("neighboring visible items");
    let messages = items
        .iter()
        .map(|item| {
            let item: serde_json::Value = serde_json::from_str(item).expect("projected item");
            item["content"][0]["text"]
                .as_str()
                .expect("visible user message")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        messages,
        ["before expanded checkpoint", "after expanded checkpoint"]
    );
}
