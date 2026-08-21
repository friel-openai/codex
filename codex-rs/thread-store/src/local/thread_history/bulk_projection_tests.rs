use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::ThreadHistoryItemChange;
use serde_json::json;

use super::*;
use crate::local::test_support::test_config;
use crate::local::thread_history;
use crate::local::thread_history::RolloutProjectionStep;

fn turn(id: &str, status: TurnStatus) -> ThreadHistoryChangeSet {
    ThreadHistoryChangeSet {
        changed_turns: vec![ThreadHistoryTurnChange {
            turn_id: id.to_string(),
            status,
            error: None,
            started_at: Some(1),
            completed_at: Some(2),
            duration_ms: Some(1000),
        }],
        ..Default::default()
    }
}

fn item(turn_id: &str, id: &str, kind: &str, phase: Option<&str>) -> ThreadHistoryChangeSet {
    let value = if kind == "userMessage" {
        json!({"type":kind,"id":id,"clientId":null,"content":[]})
    } else {
        json!({"type":kind,"id":id,"text":"message","phase":phase,"memoryCitation":null})
    };
    ThreadHistoryChangeSet {
        changed_items: vec![ThreadHistoryItemChange {
            turn_id: turn_id.to_string(),
            item: serde_json::from_value(value).expect("test item"),
            started_at_ms: None,
            completed_at_ms: None,
        }],
        ..Default::default()
    }
}

async fn rows(store: &LocalThreadStore, id: ThreadId) -> Vec<Vec<String>> {
    let pool = store
        .thread_history_db()
        .await
        .expect("projection database");
    let mut result = Vec::new();
    for query in [
        "SELECT json_array(turn_id, rollout_ordinal, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id, rollout_byte_offset, rollout_end_ordinal, rollout_end_byte_offset) FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
        "SELECT json_array(turn_id, item_id, rollout_ordinal, created_at_ms, item_json, item_type, updated_at_ordinal) FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
        "SELECT json_array(next_rollout_byte_offset, next_rollout_ordinal) FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        result.push(
            sqlx::query_scalar::<_, String>(query)
                .bind(id.to_string())
                .fetch_all(pool)
                .await
                .expect("complete projection rows"),
        );
    }
    result
}

#[tokio::test]
async fn bulk_projection_matches_ordered_sql_for_late_and_duplicate_events() {
    let home = tempfile::tempdir().expect("Codex home");
    let config = test_config(home.path());
    let rollout_config = codex_rollout::RolloutConfig {
        codex_home: config.codex_home.clone(),
        sqlite: config.sqlite.clone(),
        cwd: home.path().to_path_buf(),
        model_provider_id: config.default_model_provider_id.clone(),
        generate_memories: false,
    };
    let db = codex_rollout::state_db::try_init(&rollout_config)
        .await
        .expect("state database");
    let store = LocalThreadStore::new(config, Some(db));
    let changes = vec![
        item("a", "user", "userMessage", /*phase*/ None),
        item("a", "unphased", "agentMessage", /*phase*/ None),
        turn("a", TurnStatus::InProgress),
        item("a", "final", "agentMessage", Some("final_answer")),
        turn("a", TurnStatus::Completed),
        item("a", "final", "userMessage", /*phase*/ None),
        item("a", "late", "agentMessage", Some("final_answer")),
        turn("a", TurnStatus::Failed),
        turn("b", TurnStatus::Completed),
        item("b", "late-user", "userMessage", /*phase*/ None),
        item("b", "late-final", "agentMessage", Some("final_answer")),
        turn("removed", TurnStatus::InProgress),
        item("removed", "discard", "userMessage", /*phase*/ None),
        ThreadHistoryChangeSet {
            removed_turn_ids: vec!["removed".to_string()],
            ..Default::default()
        },
        turn("removed", TurnStatus::Interrupted),
    ];
    let lines = changes
        .into_iter()
        .enumerate()
        .map(|(index, changes)| ProjectedRolloutLine {
            ordinal: index as u64,
            start_byte_offset: index as u64 * 100,
            end_byte_offset: (index as u64 + 1) * 100,
            fallback_created_at_ms: Some(index as i64),
            changes,
        })
        .collect::<Vec<_>>();
    for complete in [false, true] {
        let reference = ThreadId::new();
        let actual = ThreadId::new();
        if !complete {
            thread_history::begin_incomplete_paginated_projection(
                &store, reference, /*initial_ordinal*/ 0,
            )
            .await
            .expect("incomplete reference");
        }
        thread_history::apply_projection(
            &store,
            reference,
            /*start_offset*/ 0,
            lines.last().expect("last line").end_byte_offset,
            /*initial_ordinal*/ 0,
            lines
                .iter()
                .cloned()
                .map(RolloutProjectionStep::Line)
                .collect(),
        )
        .await
        .expect("ordered SQL reference");
        let mut bulk = BulkProjection::new(/*initial_ordinal*/ 0);
        bulk.begin_segment(/*initial_ordinal*/ 0)
            .expect("first segment");
        for line in &lines {
            bulk.apply(line).expect("reduce projection");
        }
        bulk.replace_unpublished(&store, actual, complete)
            .await
            .expect("bulk insert");
        assert_eq!(rows(&store, actual).await, rows(&store, reference).await);
    }
}
