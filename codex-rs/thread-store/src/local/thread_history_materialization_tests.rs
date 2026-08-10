use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::ThreadHistoryItemChange;
use codex_app_server_protocol::ThreadHistoryTurnChange;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::CompactedItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RateLimitSnapshot;
use codex_protocol::protocol::RateLimitWindow;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::CertifiedSegmentStateCheckpoint;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;
use codex_utils_absolute_path::test_support::PathExt;
use codex_utils_path_uri::PathUri;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::super::LocalThreadStore;
use super::super::LocalThreadStoreConfig;
use super::super::test_support::test_config;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::ForkBoundary;
use crate::FreezeRolloutSegmentParams;
use crate::ListThreadsParams;
use crate::ListTurnsParams;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ResumeThreadParams;
use crate::SortDirection;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadPersistenceMetadata;
use crate::ThreadSortKey;
use crate::ThreadStore;

#[tokio::test]
async fn paginated_history_without_state_db_does_not_initialize_sqlite() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let sqlite = config.sqlite.clone();
    let store = LocalThreadStore::new(config, /*state_db*/ None);
    let thread_id = ThreadId::default();

    assert!(!store.supports_paginated_history_lists());
    create_paginated_thread(&store, thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1")],
        })
        .await
        .expect("append paginated rollout");
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated rollout");
    store
        .flush_thread(thread_id)
        .await
        .expect("flush paginated rollout");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown paginated rollout");

    for runtime_db in sqlite.runtime_db_paths() {
        assert!(
            !runtime_db.path.exists(),
            "expected no SQLite initialization for {}",
            runtime_db.path.display()
        );
    }
}

/// Separate Codex and SQLite homes must work together across startup backfill,
/// thread listing, and projection-backed paginated history reads.
#[tokio::test]
async fn split_homes_support_backfill_listing_and_paginated_history() {
    let root = TempDir::new().expect("temp dir");
    let codex_home = root.path().join("codex");
    let sqlite_home = root.path().join("sqlite");
    let thread_id = ThreadId::new();
    let sqlite = codex_state::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let rollout_config = RolloutConfig {
        codex_home: codex_home.clone(),
        sqlite: sqlite.clone(),
        cwd: codex_home.clone(),
        model_provider_id: "test-provider".to_string(),
        generate_memories: false,
    };
    let recorder = RolloutRecorder::new(
        &rollout_config,
        RolloutRecorderParams::new(
            thread_id,
            /*forked_from_id*/ None,
            /*parent_thread_id*/ None,
            SessionSource::Exec,
            /*thread_source*/ None,
            "test-originator".to_string(),
            BaseInstructions::default(),
            Vec::new(),
        )
        .with_history_mode(ThreadHistoryMode::Paginated)
        .with_initial_window_id("window-1".to_string()),
    )
    .await
    .expect("create paginated rollout");
    recorder
        .record_canonical_items(&[RolloutItem::EventMsg(EventMsg::UserMessage(
            UserMessageEvent {
                message: "existing thread".to_string(),
                ..Default::default()
            },
        ))])
        .await
        .expect("record existing user message");
    recorder.persist().await.expect("persist paginated rollout");
    let rollout_path = recorder.rollout_path().to_path_buf();
    recorder.shutdown().await.expect("close paginated rollout");

    let runtime = codex_rollout::state_db::try_init(&rollout_config)
        .await
        .expect("backfill state from Codex home");
    assert!(
        runtime
            .get_thread(thread_id)
            .await
            .expect("read backfilled thread")
            .is_some(),
        "startup backfill should index the rollout"
    );
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig::from_config(&rollout_config),
        Some(runtime),
    );

    let threads = store
        .list_threads(ListThreadsParams {
            page_size: 10,
            cursor: None,
            sort_key: ThreadSortKey::CreatedAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: Vec::new(),
            model_providers: None,
            cwd_filters: None,
            archived: false,
            search_term: None,
            relation_filter: None,
            section: None,
            use_state_db_only: true,
        })
        .await
        .expect("list backfilled threads");
    assert_eq!(threads.items.len(), 1);
    assert_eq!(
        (
            threads.items[0].thread_id,
            threads.items[0].rollout_path.as_deref(),
            threads.items[0].history_mode,
        ),
        (
            thread_id,
            Some(rollout_path.as_path()),
            ThreadHistoryMode::Paginated,
        )
    );

    store
        .resume_thread(ResumeThreadParams {
            thread_id,
            rollout_path: Some(rollout_path),
            history: None,
            include_archived: false,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(codex_home.clone()),
                model_provider: "test-provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await
        .expect("resume backfilled thread");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "user-1".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::AgentMessage(AgentMessageItem {
                        id: "agent-1".to_string(),
                        content: vec![AgentMessageContent::Text {
                            text: "done".to_string(),
                        }],
                        phase: None,
                        memory_citation: None,
                    }),
                ),
                turn_completed("turn-1"),
            ],
        })
        .await
        .expect("append paginated history");

    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await
        .expect("list paginated history");
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| {
                (
                    turn.turn_id.as_str(),
                    turn.status,
                    turn.items
                        .iter()
                        .map(|item| item.item_id.as_str())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            "turn-1",
            StoredTurnStatus::Completed,
            vec!["user-1", "agent-1"],
        )]
    );

    let state_db_path = sqlite.state_db_path();
    let thread_history_db_path = sqlite.thread_history_db_path();
    for sqlite_path in [&state_db_path, &thread_history_db_path] {
        assert!(
            sqlite_path.exists(),
            "expected SQLite database at {}",
            sqlite_path.display()
        );
        let filename = sqlite_path.file_name().expect("SQLite database filename");
        assert!(
            !codex_home.join(filename).exists(),
            "SQLite database should not be created under Codex home"
        );
    }
}

#[tokio::test]
async fn legacy_projection_materializes_visible_messages_without_rollout_ordinals() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n{}\n{}\n{}\n",
            rollout_line(None, turn_started("turn-1")),
            rollout_line(None, legacy_user_message("first question")),
            rollout_line(None, legacy_agent_message("first answer")),
            rollout_line(None, turn_completed("turn-1")),
        ),
    );

    let mut builder = ThreadHistoryBuilder::new();
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project legacy-visible history");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let items = sqlx::query_as::<_, (String, String, i64)>(
        "SELECT item_id, turn_id, rollout_ordinal FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected legacy items");
    assert_eq!(
        items,
        vec![
            ("item-1".to_string(), "turn-1".to_string(), 2),
            ("item-2".to_string(), "turn-1".to_string(), 3),
        ]
    );
    let turn = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT status, first_user_item_id, final_agent_item_id FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read projected legacy turn");
    assert_eq!(
        turn,
        (
            "completed".to_string(),
            Some("item-1".to_string()),
            Some("item-2".to_string()),
        )
    );
    assert!(
        fs::read_to_string(rollout_path.as_path())
            .expect("read canonical legacy rollout")
            .lines()
            .map(|line| serde_json::from_str::<RolloutLine>(line)
                .expect("parse canonical legacy line"))
            .all(|line| line.ordinal.is_none()),
        "legacy projection must not add ordinals to canonical rollout records"
    );
}

#[tokio::test]
async fn legacy_projection_materializes_inter_agent_communication() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    let communication = InterAgentCommunication::new(
        AgentPath::try_from("/root/worker").expect("valid agent path"),
        AgentPath::root(),
        Vec::new(),
        "ready for review".to_string(),
        /*trigger_turn*/ true,
    );
    let policy_rejected_item = completed_item(
        thread_id,
        "turn-1",
        TurnItem::UserMessage(UserMessageItem {
            id: "must-not-project".to_string(),
            client_id: None,
            content: Vec::new(),
        }),
    );
    assert!(
        !codex_rollout::is_persisted_rollout_item(&policy_rejected_item, ThreadHistoryMode::Legacy,),
        "historical item-completed user messages are excluded from legacy history"
    );
    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n{}\n{}\n{}\n{}\n",
            rollout_line(None, turn_started("turn-1")),
            rollout_line(None, legacy_user_message("first question")),
            rollout_line(None, policy_rejected_item),
            rollout_line(
                None,
                RolloutItem::InterAgentCommunication(communication.clone()),
            ),
            rollout_line(None, turn_completed("turn-1")),
        ),
    );

    let mut builder = ThreadHistoryBuilder::new();
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project legacy inter-agent communication");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let projected_items = sqlx::query_as::<_, (String, String)>(
        "SELECT item_id, item_json FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected legacy items");
    assert_eq!(projected_items.len(), 2);
    assert_eq!(projected_items[0].0, "item-1");
    assert_eq!(projected_items[1].0, "item-2");
    let projected_communication = serde_json::from_str::<ThreadItem>(&projected_items[1].1)
        .expect("decode projected inter-agent communication");
    assert_eq!(
        projected_communication,
        ThreadItem::InterAgentCommunication {
            id: "item-2".to_string(),
            communication,
        }
    );
}

#[tokio::test]
async fn legacy_projection_preserves_item_ids_across_streaming_batches() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");

    let mut suffix = String::new();
    for index in 1..=70 {
        let turn_id = format!("turn-{index}");
        for item in [
            turn_started(&turn_id),
            legacy_user_message(&format!("question {index}")),
            legacy_agent_message(&format!("answer {index}")),
            turn_completed(&turn_id),
        ] {
            suffix.push_str(&rollout_line(None, item));
            suffix.push('\n');
        }
    }
    append_suffix(rollout_path.as_path(), &suffix);

    let mut builder = ThreadHistoryBuilder::new();
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project legacy history across streaming batches");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let counts = sqlx::query_as::<_, (i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?), (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?)",
    )
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read legacy projection counts");
    assert_eq!(counts, (70, 140));
    let final_items = sqlx::query_as::<_, (String, i64)>(
        "SELECT item_id, rollout_ordinal FROM thread_items WHERE thread_id = ? AND turn_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .bind("turn-70")
    .fetch_all(&pool)
    .await
    .expect("read final projected legacy turn");
    assert_eq!(
        final_items,
        vec![("item-139".to_string(), 278), ("item-140".to_string(), 279)]
    );
    let rollout_len = i64::try_from(
        fs::metadata(rollout_path.as_path())
            .expect("legacy rollout metadata")
            .len(),
    )
    .expect("legacy rollout length fits SQLite");
    assert_eq!(projection_state(&pool, thread_id).await, (rollout_len, 281));
}

#[tokio::test]
async fn legacy_projection_materializes_implicit_completed_turn_summary() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n{}\n",
            rollout_line(None, legacy_user_message("implicit question")),
            rollout_line(None, legacy_agent_message("implicit answer")),
        ),
    );

    let mut builder = ThreadHistoryBuilder::new();
    let session_meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
        .await
        .expect("read canonical session metadata");
    builder.handle_rollout_item_with_changes(&RolloutItem::SessionMeta(session_meta));
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project implicitly completed legacy turn");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let turn = sqlx::query_as::<_, (String, String, Option<String>, Option<String>)>(
        "SELECT turn_id, status, first_user_item_id, final_agent_item_id FROM thread_turns WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read implicitly completed legacy turn");
    assert_eq!(
        turn,
        (
            "rollout-1".to_string(),
            "completed".to_string(),
            Some("item-1".to_string()),
            Some("item-2".to_string()),
        )
    );
}

#[tokio::test]
async fn legacy_projection_materializes_implicit_commentary_without_a_final_answer() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n{}\n",
            rollout_line(None, legacy_user_message("implicit question")),
            rollout_line(
                None,
                RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                    message: "implicit commentary".to_string(),
                    phase: Some(MessagePhase::Commentary),
                    memory_citation: None,
                })),
            ),
        ),
    );

    let mut builder = ThreadHistoryBuilder::new();
    let session_meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
        .await
        .expect("read canonical session metadata");
    builder.handle_rollout_item_with_changes(&RolloutItem::SessionMeta(session_meta));
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project implicitly completed legacy commentary");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let turn = sqlx::query_as::<_, (String, String, Option<String>, Option<String>)>(
        "SELECT turn_id, status, first_user_item_id, final_agent_item_id FROM thread_turns WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read implicitly completed commentary turn");
    assert_eq!(
        turn,
        (
            "rollout-1".to_string(),
            "completed".to_string(),
            Some("item-1".to_string()),
            None,
        )
    );
    let commentary = sqlx::query_as::<_, (String, String)>(
        "SELECT item_id, json_extract(item_json, '$.phase') FROM thread_items WHERE thread_id = ? AND turn_id = ? AND item_type = 'agentMessage'",
    )
    .bind(thread_id.to_string())
    .bind("rollout-1")
    .fetch_one(&pool)
    .await
    .expect("read implicit legacy commentary");
    assert_eq!(commentary, ("item-2".to_string(), "commentary".to_string()));
}

#[tokio::test]
async fn legacy_projection_does_not_report_commentary_as_a_final_answer() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n{}\n{}\n{}\n",
            rollout_line(None, turn_started("turn-1")),
            rollout_line(None, legacy_user_message("question")),
            rollout_line(
                None,
                RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                    message: "progress update".to_string(),
                    phase: Some(MessagePhase::Commentary),
                    memory_citation: None,
                })),
            ),
            rollout_line(None, turn_completed("turn-1")),
        ),
    );

    let mut builder = ThreadHistoryBuilder::new();
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project legacy commentary");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let turn = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT status, first_user_item_id, final_agent_item_id FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read projected commentary-only turn");
    assert_eq!(
        turn,
        ("completed".to_string(), Some("item-1".to_string()), None,)
    );
    let commentary = sqlx::query_scalar::<_, String>(
        "SELECT json_extract(item_json, '$.phase') FROM thread_items WHERE thread_id = ? AND item_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("item-2")
    .fetch_one(&pool)
    .await
    .expect("read projected legacy commentary phase");
    assert_eq!(commentary, "commentary");
}

#[tokio::test]
async fn legacy_projection_skips_repeated_segment_metadata() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    let segment_metadata = fs::read_to_string(rollout_path.as_path())
        .expect("read canonical session metadata")
        .lines()
        .next()
        .expect("session metadata line")
        .to_string();
    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n{}\n{}\n",
            rollout_line(None, legacy_user_message("first segment")),
            segment_metadata,
            rollout_line(None, legacy_user_message("next segment")),
        ),
    );

    let mut builder = ThreadHistoryBuilder::new();
    let session_meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
        .await
        .expect("read canonical session metadata");
    builder.handle_rollout_item_with_changes(&RolloutItem::SessionMeta(session_meta));
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project legacy history without replaying segment metadata");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let turns = sqlx::query_scalar::<_, String>(
        "SELECT turn_id FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read legacy synthetic turn ids");
    assert_eq!(
        turns,
        vec!["rollout-1".to_string(), "rollout-2".to_string()]
    );
    let items = sqlx::query_as::<_, (String, String)>(
        "SELECT item_id, turn_id FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read legacy items across segment metadata");
    assert_eq!(
        items,
        vec![
            ("item-1".to_string(), "rollout-1".to_string()),
            ("item-2".to_string(), "rollout-2".to_string()),
        ]
    );
    assert_eq!(projection_state(&pool, thread_id).await.1, 4);
}

#[tokio::test]
async fn legacy_projection_waits_for_complete_rollout_lines() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n{}",
            rollout_line(None, turn_started("turn-1")),
            rollout_line(None, legacy_user_message("partial message")),
        ),
    );

    let mut builder = ThreadHistoryBuilder::new();
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project complete legacy lines");
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let before = projection_state(&pool, thread_id).await;
    let visible_items =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_items WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .fetch_one(&pool)
            .await
            .expect("count legacy items before line completion");
    assert_eq!(visible_items, 0);

    append_suffix(rollout_path.as_path(), "\n");
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project completed legacy line");
    let visible_items = sqlx::query_as::<_, (String, String)>(
        "SELECT item_id, turn_id FROM thread_items WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read completed legacy item");
    assert_eq!(
        visible_items,
        vec![("item-1".to_string(), "turn-1".to_string())]
    );
    let after = projection_state(&pool, thread_id).await;
    assert!(after.0 > before.0);
    assert_eq!(after.1, before.1 + 1);
}

#[tokio::test]
async fn legacy_projection_removes_rolled_back_turns_and_items_transactionally() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_legacy_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist legacy session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("legacy rollout path");
    let mut suffix = String::new();
    for (turn_id, message) in [("turn-1", "keep"), ("turn-2", "remove")] {
        for item in [
            turn_started(turn_id),
            legacy_user_message(message),
            legacy_agent_message(message),
            turn_completed(turn_id),
        ] {
            suffix.push_str(&rollout_line(None, item));
            suffix.push('\n');
        }
    }
    append_suffix(rollout_path.as_path(), &suffix);
    let mut builder = ThreadHistoryBuilder::new();
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project both legacy turns");

    append_suffix(
        rollout_path.as_path(),
        &format!(
            "{}\n",
            rollout_line(
                None,
                RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                    num_turns: 1,
                })),
            )
        ),
    );
    super::materialize_legacy_to_sqlite(&store, thread_id, rollout_path.as_path(), &mut builder)
        .await
        .expect("project legacy rollback");

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let turns = sqlx::query_scalar::<_, String>(
        "SELECT turn_id FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read remaining legacy turns");
    assert_eq!(turns, vec!["turn-1".to_string()]);
    let items = sqlx::query_as::<_, (String, String)>(
        "SELECT item_id, turn_id FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read remaining legacy items");
    assert_eq!(
        items,
        vec![
            ("item-1".to_string(), "turn-1".to_string()),
            ("item-2".to_string(), "turn-1".to_string()),
        ]
    );
    assert_eq!(projection_state(&pool, thread_id).await.1, 10);
}

#[tokio::test]
async fn removed_turn_projection_deletes_only_the_requested_thread_turn_and_items() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::new();
    let other_thread_id = ThreadId::new();

    create_paginated_thread(&store, thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "retained-user".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                turn_completed("turn-1"),
                turn_started("turn-2"),
                completed_item(
                    thread_id,
                    "turn-2",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "removed-user".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                turn_completed("turn-2"),
            ],
        })
        .await
        .expect("project retained and removed turns");

    create_paginated_thread(&store, other_thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: other_thread_id,
            items: vec![
                turn_started("turn-2"),
                completed_item(
                    other_thread_id,
                    "turn-2",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "other-user".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
            ],
        })
        .await
        .expect("project another thread with the same turn ID");

    apply_history_changes(
        &store,
        thread_id,
        ThreadHistoryChangeSet {
            removed_turn_ids: vec!["turn-2".to_string()],
            ..Default::default()
        },
    )
    .await;

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let turns = sqlx::query_as::<_, (String, String)>(
        "SELECT thread_id, turn_id FROM thread_turns ORDER BY thread_id, turn_id",
    )
    .fetch_all(&pool)
    .await
    .expect("read projected turns after rollback");
    let mut expected_turns = vec![
        (thread_id.to_string(), "turn-1".to_string()),
        (other_thread_id.to_string(), "turn-2".to_string()),
    ];
    expected_turns.sort();
    assert_eq!(turns, expected_turns);

    let items = sqlx::query_as::<_, (String, String, String)>(
        "SELECT thread_id, turn_id, item_id FROM thread_items ORDER BY thread_id, turn_id, item_id",
    )
    .fetch_all(&pool)
    .await
    .expect("read projected items after rollback");
    let mut expected_items = vec![
        (
            thread_id.to_string(),
            "turn-1".to_string(),
            "retained-user".to_string(),
        ),
        (
            other_thread_id.to_string(),
            "turn-2".to_string(),
            "other-user".to_string(),
        ),
    ];
    expected_items.sort();
    assert_eq!(items, expected_items);
}

#[tokio::test]
async fn removed_turn_projection_deletes_old_items_before_recreating_the_turn() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::new();
    create_paginated_thread(&store, thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "removed-user".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                turn_completed("turn-1"),
            ],
        })
        .await
        .expect("project original completed turn");

    apply_history_changes(
        &store,
        thread_id,
        ThreadHistoryChangeSet {
            removed_turn_ids: vec!["turn-1".to_string()],
            changed_turns: vec![ThreadHistoryTurnChange {
                turn_id: "turn-1".to_string(),
                status: TurnStatus::InProgress,
                error: None,
                started_at: Some(30),
                completed_at: None,
                duration_ms: None,
            }],
            changed_items: vec![ThreadHistoryItemChange {
                turn_id: "turn-1".to_string(),
                item: ThreadItem::UserMessage {
                    id: "replacement-user".to_string(),
                    client_id: None,
                    content: Vec::new(),
                },
                started_at_ms: None,
                completed_at_ms: None,
            }],
        },
    )
    .await;

    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open thread history db");
    let turn = sqlx::query_as::<_, (String, i64, Option<String>)>(
        "SELECT status, started_at, first_user_item_id FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read recreated turn");
    assert_eq!(
        turn,
        (
            "inProgress".to_string(),
            30,
            Some("replacement-user".to_string()),
        )
    );

    let items = sqlx::query_scalar::<_, String>(
        "SELECT item_id FROM thread_items WHERE thread_id = ? AND turn_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_all(&pool)
    .await
    .expect("read recreated turn items");
    assert_eq!(items, vec!["replacement-user".to_string()]);
}

#[tokio::test]
async fn paginated_live_append_materializes_turn_items_and_state() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "user-1".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::AgentMessage(AgentMessageItem {
                        id: "agent-1".to_string(),
                        content: vec![AgentMessageContent::Text {
                            text: "done".to_string(),
                        }],
                        phase: None,
                        memory_citation: None,
                    }),
                ),
                turn_completed("turn-1"),
            ],
        })
        .await
        .expect("append paginated items");

    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let (turn_start_byte_offset, _) =
        rollout_line_byte_offsets(rollout_path.as_path(), /*ordinal*/ 1);
    let (_, turn_end_byte_offset) =
        rollout_line_byte_offsets(rollout_path.as_path(), /*ordinal*/ 4);
    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let turn = sqlx::query_as::<
        _,
        (
            i64,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            String,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<String>,
        ),
    >(
        r#"
SELECT
    rollout_ordinal,
    rollout_byte_offset,
    rollout_end_ordinal,
    rollout_end_byte_offset,
    status,
    started_at,
    completed_at,
    duration_ms,
    first_user_item_id,
    final_agent_item_id
FROM thread_turns
WHERE thread_id = ? AND turn_id = ?
        "#,
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read projected turn");
    assert_eq!(
        turn,
        (
            1,
            Some(turn_start_byte_offset),
            Some(4),
            Some(turn_end_byte_offset),
            "completed".to_string(),
            Some(10),
            Some(20),
            Some(10_000),
            Some("user-1".to_string()),
            Some("agent-1".to_string()),
        )
    );

    let items = sqlx::query_as::<_, (String, i64)>(
        r#"
SELECT item_id, rollout_ordinal
FROM thread_items
WHERE thread_id = ?
ORDER BY rollout_ordinal
        "#,
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected items");
    assert_eq!(
        items,
        vec![("user-1".to_string(), 2), ("agent-1".to_string(), 3)]
    );

    let rollout_len = i64::try_from(fs::metadata(rollout_path).expect("rollout metadata").len())
        .expect("rollout length");
    let projection_state = sqlx::query_as::<_, (i64, i64)>(
        r#"
SELECT next_rollout_byte_offset, next_rollout_ordinal
FROM thread_history_projection_state
WHERE thread_id = ?
        "#,
    )
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read projection state");
    assert_eq!(projection_state, (rollout_len, 5));
}

#[tokio::test]
async fn referenced_paginated_rollout_projects_inherited_ordinal_range() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let source_id = ThreadId::default();
    create_paginated_thread(&store, source_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source_id,
            items: vec![
                turn_started("source-turn"),
                user_message("source message"),
                turn_completed("source-turn"),
            ],
        })
        .await
        .expect("append source history");
    let source_path = store
        .live_rollout_path(source_id)
        .await
        .expect("source rollout path");
    let (_, source_end_byte_offset) =
        rollout_line_byte_offsets(source_path.as_path(), /*ordinal*/ 3);
    let child_id = ThreadId::default();
    let history_base = HistoryPosition {
        thread_id: source_id,
        end_ordinal_exclusive: 4,
        end_byte_offset: u64::try_from(source_end_byte_offset).expect("source byte offset"),
    };
    create_paginated_subagent_thread(
        &store,
        child_id,
        Some(history_base),
        /*subagent_history_start_ordinal*/ None,
    )
    .await;
    store
        .persist_thread(child_id)
        .await
        .expect("persist child metadata");
    assert_eq!(
        prepare_paginated_fork(&store, child_id, ForkBoundary::Latest)
            .await
            .history_base,
        Some(history_base)
    );
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![
                turn_started("child-turn"),
                user_message("child message"),
                turn_completed("child-turn"),
            ],
        })
        .await
        .expect("append child history");
    let latest_history_base = prepare_paginated_fork(&store, child_id, ForkBoundary::Latest)
        .await
        .history_base;
    for (boundary, expected_base) in [
        (ForkBoundary::Latest, latest_history_base),
        (
            ForkBoundary::ThroughTurn("source-turn".to_string()),
            Some(history_base),
        ),
        (
            ForkBoundary::BeforeTurn("child-turn".to_string()),
            Some(history_base),
        ),
        (
            ForkBoundary::ThroughTurn("child-turn".to_string()),
            latest_history_base,
        ),
        (ForkBoundary::BeforeTurn("source-turn".to_string()), None),
    ] {
        let prepared = prepare_paginated_fork(&store, child_id, boundary).await;
        assert_eq!(prepared.history_base, expected_base);
        assert!(matches!(
            prepared.model_context.first(),
            Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == child_id
        ));
        assert_eq!(
            contains_user_message(&prepared.model_context, "source message"),
            expected_base.is_some()
        );
        assert_eq!(
            contains_user_message(&prepared.model_context, "child message"),
            expected_base == latest_history_base
        );
    }

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let turn_ordinal = sqlx::query_scalar::<_, i64>(
        "SELECT rollout_ordinal FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(child_id.to_string())
    .bind("child-turn")
    .fetch_one(&pool)
    .await
    .expect("read child turn ordinal");
    let next_ordinal = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(child_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read child projection ordinal");
    assert_eq!((turn_ordinal, next_ordinal), (5, 8));
}

#[tokio::test]
async fn named_fork_boundaries_reject_invisible_and_noncanonical_turns() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let source_id = ThreadId::default();
    create_paginated_thread(&store, source_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source_id,
            items: vec![turn_started("inherited-turn"), user_message("before fork")],
        })
        .await
        .expect("append inherited active turn");

    let history_base = prepare_paginated_fork(&store, source_id, ForkBoundary::Latest)
        .await
        .history_base;
    let child_id = ThreadId::default();
    create_paginated_subagent_thread(
        &store,
        child_id,
        history_base,
        /*subagent_history_start_ordinal*/ None,
    )
    .await;
    store
        .persist_thread(child_id)
        .await
        .expect("persist child metadata");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![turn_started("child-turn"), turn_completed("child-turn")],
        })
        .await
        .expect("append child turn after inherited active turn");
    let error = store
        .prepare_fork(PrepareForkParams {
            thread_id: child_id,
            boundary: ForkBoundary::ThroughTurn("inherited-turn".to_string()),
        })
        .await
        .expect_err("cannot fork through an inherited active turn");
    assert!(matches!(
        error,
        crate::ThreadStoreError::InvalidRequest { message }
            if message == "lastTurnId 'inherited-turn' identifies an in-progress turn"
    ));
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source_id,
            items: vec![
                user_message("after fork"),
                turn_completed("inherited-turn"),
                turn_started("stale-turn"),
                turn_started("replacement-turn"),
                turn_completed("replacement-turn"),
                completed_item(
                    source_id,
                    "review-turn",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "review-message".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                turn_completed("review-turn"),
            ],
        })
        .await
        .expect("append invisible, stale, and terminal-only turns");

    for (thread_id, boundary, expected_error) in [
        (
            child_id,
            ForkBoundary::ThroughTurn("inherited-turn".to_string()),
            "fork boundary exceeds inherited source history",
        ),
        (
            source_id,
            ForkBoundary::ThroughTurn("stale-turn".to_string()),
            "lastTurnId 'stale-turn' identifies an in-progress turn",
        ),
        (
            source_id,
            ForkBoundary::BeforeTurn("review-turn".to_string()),
            "turn review-turn does not have a persisted start boundary",
        ),
    ] {
        let error = store
            .prepare_fork(PrepareForkParams {
                thread_id,
                boundary,
            })
            .await
            .expect_err("reject an invalid fork boundary");
        assert!(
            matches!(
                &error,
                crate::ThreadStoreError::InvalidRequest { message } if message == expected_error
            ),
            "expected {expected_error:?}, got {error:?}"
        );
    }
}

#[tokio::test]
async fn active_turn_stores_only_its_start_position() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1")],
        })
        .await
        .expect("append active turn");

    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let (turn_start_byte_offset, _) =
        rollout_line_byte_offsets(rollout_path.as_path(), /*ordinal*/ 1);
    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let turn_position = sqlx::query_as::<_, (Option<i64>, Option<i64>, Option<i64>)>(
        "SELECT rollout_byte_offset, rollout_end_ordinal, rollout_end_byte_offset FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read active turn position");
    assert_eq!(turn_position, (Some(turn_start_byte_offset), None, None));

    let (latest_byte_offset, latest_ordinal) = projection_state(&pool, thread_id).await;
    let prepared = prepare_paginated_fork(&store, thread_id, ForkBoundary::Latest).await;
    assert_eq!(
        prepared.history_base,
        Some(HistoryPosition {
            thread_id,
            end_ordinal_exclusive: u64::try_from(latest_ordinal).expect("latest ordinal"),
            end_byte_offset: u64::try_from(latest_byte_offset).expect("latest byte offset"),
        })
    );
    assert!(prepared.model_context.iter().any(|item| {
        matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(event)) if event.turn_id == "turn-1")
    }));
    assert_eq!(
        prepare_paginated_fork(
            &store,
            thread_id,
            ForkBoundary::BeforeTurn("turn-1".to_string()),
        )
        .await
        .history_base,
        None
    );
}

#[tokio::test]
async fn latest_paginated_fork_reuses_latest_model_context() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("first-turn"),
                user_message("first message"),
                turn_completed("first-turn"),
                turn_started("second-turn"),
                user_message("second message"),
                turn_completed("second-turn"),
            ],
        })
        .await
        .expect("append source history");

    let latest = prepare_paginated_fork(&store, thread_id, ForkBoundary::Latest).await;
    assert!(
        Arc::ptr_eq(&latest.model_context, &latest.latest_model_context),
        "a latest-boundary fork should scan its frozen model context only once"
    );
    assert!(contains_user_message(
        latest.model_context.as_slice(),
        "second message"
    ));
    drop(latest);

    let historical = prepare_paginated_fork(
        &store,
        thread_id,
        ForkBoundary::BeforeTurn("second-turn".to_string()),
    )
    .await;
    assert!(
        !Arc::ptr_eq(&historical.model_context, &historical.latest_model_context),
        "a historical boundary must retain its distinct frozen model context"
    );
    assert!(contains_user_message(
        historical.model_context.as_slice(),
        "first message"
    ));
    assert!(!contains_user_message(
        historical.model_context.as_slice(),
        "second message"
    ));
    assert!(contains_user_message(
        historical.latest_model_context.as_slice(),
        "second message"
    ));
}

#[tokio::test]
async fn paginated_fork_without_response_history_reuses_bounded_model_context() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("first-turn"),
                user_message("first message"),
                turn_completed("first-turn"),
                turn_started("second-turn"),
                user_message("second message"),
                turn_completed("second-turn"),
            ],
        })
        .await
        .expect("append source history");

    let latest = store
        .prepare_fork_without_response_history(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect("prepare bounded latest fork");
    assert_eq!(latest.source_thread_id, thread_id);
    assert!(latest.interrupt_if_open);
    assert!(
        Arc::ptr_eq(&latest.response_history, &latest.model_context),
        "a fork excluding turns must not materialize full source response history"
    );
    assert!(Arc::ptr_eq(
        &latest.model_context,
        &latest.latest_model_context
    ));
    assert!(contains_user_message(
        latest.model_context.as_slice(),
        "second message"
    ));
    drop(latest);

    let historical = store
        .prepare_fork_without_response_history(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::BeforeTurn("second-turn".to_string()),
        })
        .await
        .expect("prepare bounded historical fork");
    assert_eq!(historical.source_thread_id, thread_id);
    assert!(!historical.interrupt_if_open);
    assert!(Arc::ptr_eq(
        &historical.response_history,
        &historical.model_context
    ));
    assert!(!Arc::ptr_eq(
        &historical.model_context,
        &historical.latest_model_context
    ));
    assert!(contains_user_message(
        historical.model_context.as_slice(),
        "first message"
    ));
    assert!(!contains_user_message(
        historical.model_context.as_slice(),
        "second message"
    ));
    assert!(contains_user_message(
        historical.latest_model_context.as_slice(),
        "second message"
    ));
}

#[tokio::test]
async fn indexed_latest_fork_preserves_authoritative_context_and_projected_parent_turns() {
    for segment_count in [8, 32, 128] {
        let home = TempDir::new().expect("temp dir");
        let store = projection_store(home.path()).await;
        let thread_id = ThreadId::default();
        create_paginated_thread(&store, thread_id).await;
        store
            .persist_thread(thread_id)
            .await
            .expect("persist paginated source metadata");

        let mut source_context = Vec::with_capacity(segment_count);
        let mut immutable_predecessors = Vec::with_capacity(segment_count - 1);
        for index in 0..segment_count {
            let turn_id = format!("turn-{index}");
            let message = format!("source message {index}");
            let user_item = UserMessageItem {
                id: format!("item-{index}"),
                client_id: None,
                content: Vec::new(),
            };
            let RolloutItem::ResponseItem(item) = user_message(message.as_str()) else {
                panic!("source model history must contain canonical response items");
            };
            source_context.push(item);
            store
                .append_items(AppendThreadItemsParams {
                    thread_id,
                    items: vec![
                        turn_started(turn_id.as_str()),
                        user_message(message.as_str()),
                        completed_item(
                            thread_id,
                            turn_id.as_str(),
                            TurnItem::UserMessage(user_item),
                        ),
                        turn_completed(turn_id.as_str()),
                    ],
                })
                .await
                .expect("append projected source turn");
            if index + 1 < segment_count {
                let immutable = store
                    .freeze_thread_segment(
                        thread_id,
                        FreezeRolloutSegmentParams::rotate(
                            certified_projection_checkpoint(
                                source_context.clone(),
                                u64::try_from(index + 1).expect("test window number"),
                            )
                            .into_items(),
                        ),
                    )
                    .await
                    .expect("rotate projected source segment");
                immutable_predecessors.push(immutable.reference.rollout_path);
            }
        }

        for predecessor in immutable_predecessors
            .iter()
            .take(immutable_predecessors.len() - 1)
        {
            fs::remove_file(predecessor).expect("remove obsolete projected immutable predecessor");
        }
        assert!(
            immutable_predecessors
                .last()
                .expect("immediate immutable predecessor")
                .exists()
        );

        let source_context = Arc::new(source_context);
        index_paginated_source_metadata(&store, thread_id).await;
        let expected_position = store
            .projected_history_position(thread_id)
            .await
            .expect("read projected source position")
            .expect("projected source position");
        let prepared = store
            .prepare_fork_with_model_context(
                PrepareForkParams {
                    thread_id,
                    boundary: ForkBoundary::Latest,
                },
                Arc::clone(&source_context),
                expected_position,
            )
            .await
            .expect("prepare indexed latest fork without reading older immutable predecessors");

        assert!(matches!(
            prepared.model_context.first(),
            Some(RolloutItem::SessionMeta(_))
        ));
        assert!(Arc::ptr_eq(
            prepared
                .shared_model_response_items
                .as_ref()
                .expect("preserve shared parent model history"),
            &source_context
        ));
        assert_eq!(source_context.len(), segment_count);
        assert!(Arc::ptr_eq(
            &prepared.model_context,
            &prepared.latest_model_context
        ));
        let projected_turns = prepared
            .projected_response_turns
            .as_ref()
            .expect("preserve complete parent-owned projected response turns");
        assert_eq!(projected_turns.len(), segment_count);
        for (index, turn) in projected_turns.iter().enumerate() {
            assert_eq!(turn.turn_id, format!("turn-{index}"));
            assert_eq!(turn.items.len(), 1);
            assert_eq!(turn.items[0].item_id, format!("item-{index}"));
        }
        assert_eq!(prepared.frozen_segment.reference.thread_id, Some(thread_id));

        let side = store
            .prepare_fork_without_response_history_with_model_context(
                PrepareForkParams {
                    thread_id,
                    boundary: ForkBoundary::Latest,
                },
                Arc::clone(&source_context),
                expected_position,
            )
            .await
            .expect("prepare side without reading older immutable predecessors");
        assert!(side.projected_response_turns.is_none());
        assert!(Arc::ptr_eq(
            side.shared_model_response_items
                .as_ref()
                .expect("share parent context with side"),
            &source_context
        ));

        let pool = codex_state::open_thread_history_db(
            &codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        )
        .await
        .expect("open projected parent history");
        let parent_rows = sqlx::query_as::<_, (i64, i64)>(
            r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?)
            "#,
        )
        .bind(thread_id.to_string())
        .bind(thread_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("read parent-owned projected turns and items");
        let count = i64::try_from(segment_count).expect("segment count");
        assert_eq!(parent_rows, (count, count));
    }
}

#[tokio::test]
async fn indexed_latest_fork_replays_uncompacted_model_context_once() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated source metadata");

    for index in 0..4 {
        let turn_id = format!("turn-{index}");
        let message = format!("uncompacted message {index}");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    turn_started(turn_id.as_str()),
                    user_message(message.as_str()),
                    completed_item(
                        thread_id,
                        turn_id.as_str(),
                        TurnItem::UserMessage(UserMessageItem {
                            id: format!("item-{index}"),
                            client_id: None,
                            content: Vec::new(),
                        }),
                    ),
                    turn_completed(turn_id.as_str()),
                ],
            })
            .await
            .expect("append uncompacted source turn");
        if index < 3 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate uncompacted source segment");
        }
    }
    index_paginated_source_metadata(&store, thread_id).await;

    let prepared = store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect("prepare complete uncompacted source history");

    assert!(prepared.shared_model_response_items.is_none());
    assert!(contains_user_message(
        prepared.model_context.as_slice(),
        "uncompacted message 0"
    ));
    assert!(contains_user_message(
        prepared.model_context.as_slice(),
        "uncompacted message 3"
    ));
    assert_eq!(
        prepared
            .projected_response_turns
            .as_ref()
            .expect("reuse projected parent response instead of replaying twice")
            .len(),
        4
    );
}

#[tokio::test]
async fn indexed_latest_fork_rejects_stale_authoritative_model_snapshot() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated source metadata");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("first-turn"),
                user_message("first source message"),
                turn_completed("first-turn"),
            ],
        })
        .await
        .expect("append first source turn");
    index_paginated_source_metadata(&store, thread_id).await;
    let stale_position = store
        .projected_history_position(thread_id)
        .await
        .expect("read projected source position")
        .expect("projected source position");
    let later_cwd = home.path().join("later-environment").abs();
    let later_settings = ThreadSettingsSnapshot {
        model: "later-model".to_string(),
        model_provider_id: "later-provider".to_string(),
        service_tier: None,
        approval_policy: AskForApproval::Never,
        approvals_reviewer: ApprovalsReviewer::AutoReview,
        permission_profile: PermissionProfile::workspace_write(),
        active_permission_profile: None,
        cwd: later_cwd.clone(),
        environments: Some(TurnEnvironmentSelections::new(
            later_cwd.clone(),
            vec![TurnEnvironmentSelection {
                environment_id: "later-environment".to_string(),
                cwd: PathUri::from_abs_path(&later_cwd),
                workspace_roots: vec![PathUri::from_abs_path(&later_cwd)],
            }],
        )),
        workspace_roots: Some(vec![later_cwd]),
        profile_workspace_roots: Some(Vec::new()),
        windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
        reasoning_effort: None,
        reasoning_summary: None,
        personality: None,
        collaboration_mode: CollaborationMode {
            mode: ModeKind::Default,
            settings: Settings {
                model: "later-model".to_string(),
                reasoning_effort: None,
                developer_instructions: None,
            },
        },
    };
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("later-turn"),
                user_message("later source message"),
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
                    ThreadSettingsAppliedEvent {
                        thread_settings: later_settings.clone(),
                    },
                )),
                turn_completed("later-turn"),
            ],
        })
        .await
        .expect("advance source after model snapshot");

    let stale_context = Arc::new(vec![match user_message("first source message") {
        RolloutItem::ResponseItem(item) => item,
        _ => panic!("expected canonical source response item"),
    }]);
    let prepared = store
        .prepare_fork_with_model_context(
            PrepareForkParams {
                thread_id,
                boundary: ForkBoundary::Latest,
            },
            stale_context,
            stale_position,
        )
        .await
        .expect("reload exact context after stale snapshot");

    assert!(prepared.shared_model_response_items.is_none());
    assert!(contains_user_message(
        prepared.model_context.as_slice(),
        "later source message"
    ));
    assert_eq!(
        prepared
            .latest_model_context
            .iter()
            .rev()
            .find_map(|item| match item {
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) => {
                    Some(&event.thread_settings)
                }
                _ => None,
            }),
        Some(&later_settings)
    );
}

#[tokio::test]
async fn indexed_latest_fork_replays_referenced_metadata_after_rollback() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated source metadata");

    for (turn_id, message) in [
        ("retained-turn", "retained"),
        ("rolled-back-turn", "removed"),
    ] {
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    turn_started(turn_id),
                    user_message(message),
                    completed_item(
                        thread_id,
                        turn_id,
                        TurnItem::UserMessage(UserMessageItem {
                            id: format!("{turn_id}-item"),
                            client_id: None,
                            content: Vec::new(),
                        }),
                    ),
                    turn_completed(turn_id),
                ],
            })
            .await
            .expect("append source turn");
        if turn_id == "retained-turn" {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate retained source turn");
        }
    }
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
                ThreadRolledBackEvent { num_turns: 1 },
            ))],
        })
        .await
        .expect("append rollback marker");
    index_paginated_source_metadata(&store, thread_id).await;
    let expected_position = store
        .projected_history_position(thread_id)
        .await
        .expect("read projected source position")
        .expect("projected source position");
    let retained_context = Arc::new(vec![match user_message("retained") {
        RolloutItem::ResponseItem(item) => item,
        _ => panic!("expected canonical retained response item"),
    }]);

    let prepared = store
        .prepare_fork_with_model_context(
            PrepareForkParams {
                thread_id,
                boundary: ForkBoundary::Latest,
            },
            retained_context,
            expected_position,
        )
        .await
        .expect("replay referenced metadata after rollback");

    assert!(prepared.shared_model_response_items.is_none());
    assert!(prepared.model_context.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            }))
        )
    }));
}

#[tokio::test]
async fn indexed_latest_fork_rebuilds_history_when_projection_disappears_before_persist() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated source metadata");
    let mut source_context = Vec::new();
    for index in 0..3 {
        let turn_id = format!("turn-{index}");
        let message = format!("projected message {index}");
        let RolloutItem::ResponseItem(response) = user_message(message.as_str()) else {
            panic!("expected canonical source response");
        };
        source_context.push(response);
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    turn_started(turn_id.as_str()),
                    user_message(message.as_str()),
                    completed_item(
                        thread_id,
                        turn_id.as_str(),
                        TurnItem::UserMessage(UserMessageItem {
                            id: format!("item-{index}"),
                            client_id: None,
                            content: Vec::new(),
                        }),
                    ),
                    turn_completed(turn_id.as_str()),
                ],
            })
            .await
            .expect("append projected source turn");
        if index < 2 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate projected source segment");
        }
    }
    index_paginated_source_metadata(&store, thread_id).await;
    let expected_position = store
        .projected_history_position(thread_id)
        .await
        .expect("read projected source position")
        .expect("projected source position");
    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open projected thread history");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(thread_id.to_string())
            .execute(&pool)
            .await
            .expect("remove complete projected source history");
    }

    let prepared = store
        .prepare_fork_with_model_context(
            PrepareForkParams {
                thread_id,
                boundary: ForkBoundary::Latest,
            },
            Arc::new(source_context),
            expected_position,
        )
        .await
        .expect("reload complete canonical ancestry after projection disappears");

    assert!(prepared.shared_model_response_items.is_none());
    assert!(prepared.projected_response_turns.is_none());
    assert!(contains_user_message(
        prepared.response_history.as_slice(),
        "projected message 0"
    ));
    assert!(contains_user_message(
        prepared.response_history.as_slice(),
        "projected message 2"
    ));
}

#[tokio::test]
async fn indexed_latest_fork_preserves_same_thread_nested_user_cutoff() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated source metadata");
    for index in 0..3 {
        let turn_id = format!("turn-{index}");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    turn_started(turn_id.as_str()),
                    user_message(format!("cutoff message {index}").as_str()),
                    completed_item(
                        thread_id,
                        turn_id.as_str(),
                        TurnItem::UserMessage(UserMessageItem {
                            id: format!("cutoff-item-{index}"),
                            client_id: None,
                            content: Vec::new(),
                        }),
                    ),
                    turn_completed(turn_id.as_str()),
                ],
            })
            .await
            .expect("append projected source turn");
        if index < 2 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate projected source segment");
        }
    }
    index_paginated_source_metadata(&store, thread_id).await;
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("read active rollout path");
    let source = fs::read_to_string(active_path.as_path()).expect("read canonical source rollout");
    let mut lines = source
        .lines()
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse canonical source rollout");
    match &mut lines[1].item {
        RolloutItem::RolloutReference(reference) => reference.nth_user_message = Some(1),
        _ => panic!("expected leading same-thread rollout reference"),
    }
    let encoded = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .expect("encode source rollout with inherited user cutoff")
        .join("\n")
        + "\n";
    fs::write(active_path.as_path(), encoded.as_bytes())
        .expect("replace source rollout with inherited user cutoff");
    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open projected thread history");
    sqlx::query(
        "UPDATE thread_history_projection_state SET next_rollout_byte_offset = ? WHERE thread_id = ?",
    )
    .bind(i64::try_from(encoded.len()).expect("source length"))
    .bind(thread_id.to_string())
    .execute(&pool)
    .await
    .expect("update source projection byte checkpoint");

    let prepared = store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect("honor inherited same-thread user cutoff");

    assert!(prepared.projected_response_turns.is_none());
    assert!(!contains_user_message(
        prepared.response_history.as_slice(),
        "cutoff message 1"
    ));
    assert!(contains_user_message(
        prepared.response_history.as_slice(),
        "cutoff message 2"
    ));
}

#[tokio::test]
async fn indexed_latest_fork_rejects_missing_immediate_immutable_predecessor() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated source metadata");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("first-turn"),
                user_message("first message"),
                turn_completed("first-turn"),
            ],
        })
        .await
        .expect("append first source segment");
    let predecessor = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("rotate immutable predecessor");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("latest-turn"),
                user_message("latest message"),
                turn_completed("latest-turn"),
            ],
        })
        .await
        .expect("append latest source segment");
    let expected_position = store
        .projected_history_position(thread_id)
        .await
        .expect("read projected source position")
        .expect("projected source position");
    fs::remove_file(predecessor.reference.rollout_path)
        .expect("remove immediate immutable predecessor");

    let error = store
        .prepare_fork_with_model_context(
            PrepareForkParams {
                thread_id,
                boundary: ForkBoundary::Latest,
            },
            Arc::new(Vec::new()),
            expected_position,
        )
        .await
        .expect_err("missing immediate immutable predecessor must not create a child reference");
    assert!(matches!(
        error,
        crate::ThreadStoreError::Internal { .. } | crate::ThreadStoreError::InvalidRequest { .. }
    ));
}

#[tokio::test]
async fn paginated_fork_persists_empty_source() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    assert!(!rollout_path.exists());

    let prepared = prepare_paginated_fork(&store, thread_id, ForkBoundary::Latest).await;

    assert!(rollout_path.exists());
    assert_eq!(prepared.history_base, None);
    assert!(matches!(
        prepared.model_context.as_slice(),
        [RolloutItem::SessionMeta(meta)] if meta.meta.id == thread_id
    ));
}

#[tokio::test]
async fn paginated_fork_materializes_compressed_source_and_ancestor() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let ancestor_thread_id = ThreadId::default();
    create_paginated_thread(&store, ancestor_thread_id).await;
    store
        .persist_thread(ancestor_thread_id)
        .await
        .expect("persist ancestor meta");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: ancestor_thread_id,
            items: vec![
                turn_started("ancestor-turn"),
                user_message("inherited ancestor message"),
                turn_completed("ancestor-turn"),
            ],
        })
        .await
        .expect("append ancestor turn");
    let ancestor_path = store
        .live_rollout_path(ancestor_thread_id)
        .await
        .expect("ancestor rollout path");
    let ancestor_base = prepare_paginated_fork(&store, ancestor_thread_id, ForkBoundary::Latest)
        .await
        .history_base
        .expect("ancestor prefix");
    store
        .shutdown_thread(ancestor_thread_id)
        .await
        .expect("shutdown ancestor");

    let source_thread_id = ThreadId::default();
    create_paginated_subagent_thread(
        &store,
        source_thread_id,
        Some(ancestor_base),
        /*subagent_history_start_ordinal*/ None,
    )
    .await;
    store
        .persist_thread(source_thread_id)
        .await
        .expect("persist source meta");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source_thread_id,
            items: vec![
                turn_started("source-turn"),
                user_message("inherited source message"),
                turn_completed("source-turn"),
            ],
        })
        .await
        .expect("append source turn");
    let source_path = store
        .live_rollout_path(source_thread_id)
        .await
        .expect("source rollout path");
    store
        .shutdown_thread(source_thread_id)
        .await
        .expect("shutdown source");
    let ancestor_compressed_path = ancestor_path.with_extension("jsonl.zst");
    let source_compressed_path = source_path.with_extension("jsonl.zst");
    compress_rollout(ancestor_path.as_path());
    compress_rollout(source_path.as_path());
    let ancestor_modified = fs::metadata(&ancestor_compressed_path)
        .and_then(|metadata| metadata.modified())
        .expect("read compressed ancestor timestamp");
    let source_modified = fs::metadata(&source_compressed_path)
        .and_then(|metadata| metadata.modified())
        .expect("read compressed source timestamp");

    let (first, second) = tokio::join!(
        prepare_paginated_fork(&store, source_thread_id, ForkBoundary::Latest),
        prepare_paginated_fork(&store, source_thread_id, ForkBoundary::Latest),
    );
    assert!(ancestor_path.exists());
    assert!(source_path.exists());
    assert!(!ancestor_compressed_path.exists());
    assert!(!source_compressed_path.exists());
    assert_eq!(
        fs::metadata(&ancestor_path)
            .and_then(|metadata| metadata.modified())
            .expect("read materialized ancestor timestamp"),
        ancestor_modified
    );
    assert_eq!(
        fs::metadata(&source_path)
            .and_then(|metadata| metadata.modified())
            .expect("read materialized source timestamp"),
        source_modified
    );
    for prepared in [first, second] {
        assert!(matches!(
            prepared.model_context.first(),
            Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == source_thread_id
        ));
        for message in ["inherited ancestor message", "inherited source message"] {
            assert!(contains_user_message(
                prepared.model_context.as_slice(),
                message
            ));
        }
    }
}

#[tokio::test]
async fn history_projection_requires_visible_items_for_projected_turns() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let thread_id = ThreadId::new();
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state runtime");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        home.path().join("missing-rollout.jsonl"),
        Utc::now(),
        SessionSource::Cli,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    runtime
        .upsert_thread(&metadata.build(config.default_model_provider_id.as_str()))
        .await
        .expect("seed paginated thread metadata");
    let store = LocalThreadStore::new(config.clone(), Some(runtime));
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist paginated thread");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "user-1".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                completed_item(
                    thread_id,
                    "turn-1",
                    agent_message("agent-1", MessagePhase::FinalAnswer),
                ),
                turn_completed("turn-1"),
            ],
        })
        .await
        .expect("project complete turn and visible items");

    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("check complete history projection"),
        "a projected turn with its user and assistant items must remain usable"
    );

    let pool = codex_state::open_thread_history_db(&config.sqlite)
        .await
        .expect("open existing thread history database");
    sqlx::query("DELETE FROM thread_items WHERE thread_id = ?")
        .bind(thread_id.to_string())
        .execute(&pool)
        .await
        .expect("remove only projected item rows");
    let remaining_rows = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
SELECT
    (SELECT COUNT(*) FROM thread_history_projection_state WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?)
        "#,
    )
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("inspect remaining history projection rows");
    assert_eq!(remaining_rows, (1, 1, 0));
    assert!(
        !store
            .has_history_projection(thread_id)
            .await
            .expect("check incomplete history projection"),
        "projected turns without their visible items must use canonical rollout history"
    );
}

#[tokio::test]
async fn named_fork_rebuilds_projection_across_same_thread_rotations() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist source metadata");

    for (turn_id, message) in [
        ("turn-1", "first segment"),
        ("turn-2", "second segment"),
        ("turn-3", "third segment"),
    ] {
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    turn_started(turn_id),
                    user_message(message),
                    turn_completed(turn_id),
                ],
            })
            .await
            .expect("append rotated turn");
        if turn_id != "turn-3" {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate paginated rollout");
        }
    }

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(thread_id.to_string())
            .execute(&pool)
            .await
            .expect("clear projected history");
    }

    let prepared = tokio::time::timeout(
        Duration::from_secs(10),
        store.prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::ThroughTurn("turn-2".to_string()),
        }),
    )
    .await
    .expect("same-thread rotation must not deadlock")
    .expect("prepare named fork across rotations");
    assert!(contains_user_message(
        prepared.model_context.as_slice(),
        "first segment"
    ));
    assert!(contains_user_message(
        prepared.model_context.as_slice(),
        "second segment"
    ));
    assert!(!contains_user_message(
        prepared.model_context.as_slice(),
        "third segment"
    ));
}

#[tokio::test]
async fn cancelled_fork_keeps_source_reserved_until_lineage_materialization_finishes() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let ancestor_thread_id = ThreadId::default();
    create_paginated_thread(&store, ancestor_thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: ancestor_thread_id,
            items: vec![
                turn_started("ancestor-turn"),
                turn_completed("ancestor-turn"),
            ],
        })
        .await
        .expect("append ancestor history");
    let ancestor_base = prepare_paginated_fork(&store, ancestor_thread_id, ForkBoundary::Latest)
        .await
        .history_base
        .expect("ancestor prefix");

    let source_thread_id = ThreadId::default();
    create_paginated_subagent_thread(
        &store,
        source_thread_id,
        Some(ancestor_base),
        /*subagent_history_start_ordinal*/ None,
    )
    .await;
    store
        .persist_thread(source_thread_id)
        .await
        .expect("persist source metadata");
    let source_path = store
        .live_rollout_path(source_thread_id)
        .await
        .expect("source rollout path");
    store
        .shutdown_thread(source_thread_id)
        .await
        .expect("shutdown source");
    compress_rollout(source_path.as_path());

    let ancestor_writer_guard = store.live_writer_locks.lock(ancestor_thread_id).await;
    let preparation_store = store.clone();
    let preparation = tokio::spawn(async move {
        preparation_store
            .prepare_fork(PrepareForkParams {
                thread_id: source_thread_id,
                boundary: ForkBoundary::Latest,
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !source_path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached lineage task should materialize the source");
    preparation.abort();
    assert!(
        preparation
            .await
            .expect_err("fork preparation should be cancelled")
            .is_cancelled()
    );

    let mut delete = Box::pin(store.delete_thread(DeleteThreadParams {
        thread_id: source_thread_id,
    }));
    tokio::select! {
        biased;
        result = &mut delete => {
            panic!("source deletion completed while lineage materialization was active: {result:?}")
        }
        _ = tokio::task::yield_now() => {}
    }
    drop(ancestor_writer_guard);
    delete
        .await
        .expect("delete source after lineage materialization finishes");
}

#[tokio::test]
async fn prepared_fork_reserves_source_until_child_reference_is_durable() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let source_thread_id = ThreadId::default();
    create_paginated_thread(&store, source_thread_id).await;
    store
        .persist_thread(source_thread_id)
        .await
        .expect("persist source metadata");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source_thread_id,
            items: vec![turn_started("source-turn"), turn_completed("source-turn")],
        })
        .await
        .expect("append source turn");
    let prepared = store
        .prepare_fork(PrepareForkParams {
            thread_id: source_thread_id,
            boundary: ForkBoundary::Latest,
        })
        .await
        .expect("prepare referenced fork");
    let history_base = prepared.history_base.expect("source history base");

    let mut delete = Box::pin(store.delete_thread(DeleteThreadParams {
        thread_id: source_thread_id,
    }));
    tokio::select! {
        biased;
        result = &mut delete => {
            panic!("source deletion completed before its child reference was durable: {result:?}")
        }
        _ = tokio::task::yield_now() => {}
    }
    tokio::time::timeout(
        Duration::from_secs(10),
        store.append_items(AppendThreadItemsParams {
            thread_id: source_thread_id,
            items: vec![
                turn_started("later-turn"),
                user_message("later source message"),
                turn_completed("later-turn"),
            ],
        }),
    )
    .await
    .expect("source write should not wait behind deletion")
    .expect("source writes remain available during fork preparation");
    assert_eq!(prepared.history_base, Some(history_base));
    assert!(!contains_user_message(
        prepared.model_context.as_slice(),
        "later source message"
    ));
    assert!(!contains_user_message(
        prepared.response_history.as_slice(),
        "later source message"
    ));
    let frozen_items = super::super::read_thread::load_history_items(
        home.path(),
        prepared.frozen_segment.reference.rollout_path.as_path(),
    )
    .await
    .expect("materialize the exact prepared reference");
    assert!(!contains_user_message(
        frozen_items.as_slice(),
        "later source message"
    ));

    let child_thread_id = ThreadId::default();
    create_paginated_subagent_thread(
        &store,
        child_thread_id,
        Some(history_base),
        /*subagent_history_start_ordinal*/ None,
    )
    .await;
    store
        .persist_thread(child_thread_id)
        .await
        .expect("persist child history reference");
    drop(prepared);

    let error = delete
        .await
        .expect_err("durable child reference protects its source");
    assert!(
        error
            .to_string()
            .contains("forked history still references")
    );
}

#[tokio::test]
async fn subagent_prefix_advances_projection_without_materializing_history() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_subagent_thread(
        &store,
        thread_id,
        /*history_base*/ None,
        /*subagent_history_start_ordinal*/ Some(4),
    )
    .await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("parent-turn"),
                completed_item(
                    thread_id,
                    "parent-turn",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "parent-user".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                turn_completed("parent-turn"),
                turn_started("child-turn"),
                completed_item(
                    thread_id,
                    "child-turn",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "child-user".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                turn_completed("child-turn"),
            ],
        })
        .await
        .expect("append inherited prefix and child history");

    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let (child_start_byte_offset, _) =
        rollout_line_byte_offsets(rollout_path.as_path(), /*ordinal*/ 4);
    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let turns = sqlx::query_as::<_, (String, i64, Option<i64>)>(
        "SELECT turn_id, rollout_ordinal, rollout_byte_offset FROM thread_turns WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected turns");
    assert_eq!(
        turns,
        vec![("child-turn".to_string(), 4, Some(child_start_byte_offset))]
    );
    let items = sqlx::query_as::<_, (String, i64)>(
        "SELECT item_id, rollout_ordinal FROM thread_items WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected items");
    assert_eq!(items, vec![("child-user".to_string(), 5)]);
    assert_eq!(projection_state(&pool, thread_id).await.1, 7);
}

#[tokio::test]
async fn unexpected_duplicate_item_completion_does_not_poison_projection() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "user-1".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
            ],
        })
        .await
        .expect("append completed item");
    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let first_created_at_ms = sqlx::query_scalar::<_, i64>(
        "SELECT created_at_ms FROM thread_items WHERE thread_id = ? AND turn_id = ? AND item_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .bind("user-1")
    .fetch_one(&pool)
    .await
    .expect("read first item timestamp");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![completed_item(
                thread_id,
                "turn-1",
                TurnItem::UserMessage(UserMessageItem {
                    id: "user-1".to_string(),
                    client_id: Some("updated".to_string()),
                    content: Vec::new(),
                }),
            )],
        })
        .await
        .expect("append unexpected duplicate item completion");

    let item = sqlx::query_as::<_, (i64, i64, i64, String)>(
        r#"
SELECT rollout_ordinal, updated_at_ordinal, created_at_ms, item_json
FROM thread_items
WHERE thread_id = ? AND turn_id = ? AND item_id = ?
        "#,
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .bind("user-1")
    .fetch_one(&pool)
    .await
    .expect("read projected item");
    assert_eq!((item.0, item.1, item.2), (2, 3, first_created_at_ms));
    assert_eq!(
        serde_json::from_str::<ThreadItem>(item.3.as_str()).expect("parse projected item"),
        ThreadItem::UserMessage {
            id: "user-1".to_string(),
            client_id: Some("updated".to_string()),
            content: Vec::new(),
        }
    );
}

#[tokio::test]
async fn terminal_turn_does_not_change_after_later_records() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1"), turn_completed("turn-1")],
        })
        .await
        .expect("append terminal turn");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "late-user".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
            ],
        })
        .await
        .expect("append later records");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let (turn_start_byte_offset, _) =
        rollout_line_byte_offsets(rollout_path.as_path(), /*ordinal*/ 1);
    let (_, turn_end_byte_offset) =
        rollout_line_byte_offsets(rollout_path.as_path(), /*ordinal*/ 2);
    let turn = sqlx::query_as::<
        _,
        (
            i64,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            String,
            Option<String>,
        ),
    >(
        r#"
SELECT
    rollout_ordinal,
    rollout_byte_offset,
    rollout_end_ordinal,
    rollout_end_byte_offset,
    status,
    first_user_item_id
FROM thread_turns
WHERE thread_id = ? AND turn_id = ?
        "#,
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read projected turn");
    assert_eq!(
        turn,
        (
            1,
            Some(turn_start_byte_offset),
            Some(2),
            Some(turn_end_byte_offset),
            "completed".to_string(),
            None,
        )
    );
}

#[tokio::test]
async fn summary_items_use_final_answers_and_ignore_commentary() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let thread_id = ThreadId::default();
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state runtime");
    let mut builder = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        home.path().join("missing-rollout.jsonl"),
        Utc::now(),
        SessionSource::Cli,
    );
    builder.history_mode = ThreadHistoryMode::Paginated;
    runtime
        .upsert_thread(&builder.build(config.default_model_provider_id.as_str()))
        .await
        .expect("seed thread metadata");
    let store = LocalThreadStore::new(config, Some(runtime));
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                completed_item(
                    thread_id,
                    "turn-1",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "user-1".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                completed_item(
                    thread_id,
                    "turn-1",
                    agent_message("commentary-1", MessagePhase::Commentary),
                ),
                completed_item(
                    thread_id,
                    "turn-1",
                    agent_message("final-1", MessagePhase::FinalAnswer),
                ),
            ],
        })
        .await
        .expect("append items before turn lifecycle");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1"), turn_completed("turn-1")],
        })
        .await
        .expect("append delayed turn lifecycle");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-2"),
                completed_item(
                    thread_id,
                    "turn-2",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "user-2".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                completed_item(
                    thread_id,
                    "turn-2",
                    agent_message("commentary-2", MessagePhase::Commentary),
                ),
                turn_completed("turn-2"),
            ],
        })
        .await
        .expect("append commentary-only turn");

    let summary = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 2,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await
        .expect("list turn summaries");
    assert_eq!(
        summary
            .turns
            .iter()
            .map(|turn| {
                (
                    turn.turn_id.as_str(),
                    turn.items
                        .iter()
                        .map(|item| item.item_id.as_str())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("turn-1", vec!["user-1", "final-1"]),
            ("turn-2", vec!["user-2"]),
        ]
    );
}

#[tokio::test]
async fn paginated_projection_accepts_float_rate_limits_and_later_final_answers() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1")],
        })
        .await
        .expect("append projected turn start");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let checkpoint = projection_state(&pool, thread_id).await;
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let token_count = |primary: Option<f64>, secondary: Option<f64>| {
        RolloutItem::EventMsg(EventMsg::TokenCount(TokenCountEvent {
            info: None,
            rate_limits: Some(RateLimitSnapshot {
                limit_id: None,
                limit_name: None,
                primary: primary.map(|used_percent| RateLimitWindow {
                    used_percent,
                    window_minutes: Some(60),
                    resets_at: Some(1_800_000_000),
                }),
                secondary: secondary.map(|used_percent| RateLimitWindow {
                    used_percent,
                    window_minutes: Some(10_080),
                    resets_at: Some(1_800_100_000),
                }),
                credits: None,
                individual_limit: None,
                spend_control_reached: None,
                plan_type: None,
                rate_limit_reached_type: None,
            }),
        }))
    };

    let recorder = store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .expect("live recorder")
        .recorder
        .clone();
    recorder
        .record_canonical_items(&[
            token_count(Some(0.0), Some(1.0)),
            completed_item(
                thread_id,
                "turn-1",
                agent_message("final-1", MessagePhase::FinalAnswer),
            ),
            token_count(Some(12.5), None),
            turn_completed("turn-1"),
        ])
        .await
        .expect("queue unprojected history");
    recorder.flush().await.expect("flush unprojected history");
    assert_eq!(projection_state(&pool, thread_id).await, checkpoint);
    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("project floating-point rate limits");

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-2"),
                token_count(None, Some(87.25)),
                completed_item(
                    thread_id,
                    "turn-2",
                    agent_message("final-2", MessagePhase::FinalAnswer),
                ),
                token_count(Some(1e-12), Some(1_000_000.0)),
                turn_completed("turn-2"),
            ],
        })
        .await
        .expect("append history after unprojected floating-point rate limits");

    let final_answers = sqlx::query_as::<_, (String, String, String, i64)>(
        r#"
SELECT turns.turn_id, items.item_id, turns.status, items.rollout_ordinal
FROM thread_turns AS turns
JOIN thread_items AS items
  ON items.thread_id = turns.thread_id
 AND items.turn_id = turns.turn_id
 AND items.item_id = turns.final_agent_item_id
WHERE turns.thread_id = ?
ORDER BY turns.rollout_ordinal
        "#,
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected final answers");
    assert_eq!(
        final_answers,
        vec![
            (
                "turn-1".to_string(),
                "final-1".to_string(),
                "completed".to_string(),
                3,
            ),
            (
                "turn-2".to_string(),
                "final-2".to_string(),
                "completed".to_string(),
                8,
            ),
        ]
    );

    let rollout_len = i64::try_from(
        fs::metadata(rollout_path.as_path())
            .expect("rollout metadata")
            .len(),
    )
    .expect("rollout length");
    assert_eq!(projection_state(&pool, thread_id).await, (rollout_len, 11));
}

#[tokio::test]
async fn next_write_catches_up_unprojected_durable_suffix() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let checkpoint = projection_state(&pool, thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1")],
        })
        .await
        .expect("append turn start");

    let thread_id_string = thread_id.to_string();
    sqlx::query("DELETE FROM thread_turns WHERE thread_id = ?")
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("remove projected turn");
    sqlx::query(
        r#"
UPDATE thread_history_projection_state
SET next_rollout_byte_offset = ?, next_rollout_ordinal = ?
WHERE thread_id = ?
        "#,
    )
    .bind(checkpoint.0)
    .bind(checkpoint.1)
    .bind(thread_id_string.as_str())
    .execute(&pool)
    .await
    .expect("rewind projection state");

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![completed_item(
                thread_id,
                "turn-1",
                TurnItem::UserMessage(UserMessageItem {
                    id: "user-1".to_string(),
                    client_id: None,
                    content: Vec::new(),
                }),
            )],
        })
        .await
        .expect("append after simulated projection failure");

    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
SELECT
    (SELECT status FROM thread_turns WHERE thread_id = ? AND turn_id = 'turn-1'),
    (SELECT item_id FROM thread_items WHERE thread_id = ? AND turn_id = 'turn-1')
        "#,
    )
    .bind(thread_id_string.as_str())
    .bind(thread_id_string.as_str())
    .fetch_one(&pool)
    .await
    .expect("read recovered rows");
    assert_eq!(rows, ("inProgress".to_string(), "user-1".to_string()));

    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let rollout_len = i64::try_from(fs::metadata(rollout_path).expect("rollout metadata").len())
        .expect("rollout length");
    assert_eq!(projection_state(&pool, thread_id).await, (rollout_len, 3));
}

#[tokio::test]
async fn synchronized_catch_up_does_not_replay_old_rows() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1")],
        })
        .await
        .expect("append turn start");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let before = projection_state(&pool, thread_id).await;
    sqlx::query("UPDATE thread_turns SET status = 'sentinel' WHERE thread_id = ?")
        .bind(thread_id.to_string())
        .execute(&pool)
        .await
        .expect("mark projected turn");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("catch up synchronized rollout");

    assert_eq!(projection_state(&pool, thread_id).await, before);
    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM thread_turns WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .fetch_one(&pool)
            .await
            .expect("read projected turn");
    assert_eq!(status, "sentinel");
}

#[tokio::test]
async fn catch_up_preserves_trailing_partial_line_boundaries() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let before = projection_state(&pool, thread_id).await;
    let complete_line = rollout_line(Some(1), turn_started("turn-1"));
    let partial_line = rollout_line(Some(2), turn_completed("turn-1"));
    let complete_suffix = format!("{complete_line}\n");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    append_suffix(
        rollout_path.as_path(),
        format!("{complete_suffix}{partial_line}").as_str(),
    );

    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("catch up complete suffix");

    let expected_offset =
        before.0 + i64::try_from(complete_suffix.len()).expect("complete suffix byte count");
    assert_eq!(
        projection_state(&pool, thread_id).await,
        (expected_offset, 2)
    );
    append_suffix(rollout_path.as_path(), "\n");
    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("catch up completed partial suffix");

    let rollout_len = i64::try_from(fs::metadata(rollout_path).expect("rollout metadata").len())
        .expect("rollout length");
    let turn_position = sqlx::query_as::<_, (Option<i64>, Option<i64>, Option<i64>)>(
        "SELECT rollout_byte_offset, rollout_end_ordinal, rollout_end_byte_offset FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read completed turn position");
    assert_eq!(turn_position, (Some(before.0), Some(2), Some(rollout_len)));
}

#[tokio::test]
async fn catch_up_rejects_invalid_complete_suffixes_without_advancing_state() {
    let cases = [
        (
            "missing ordinal",
            format!(
                "{}\n",
                rollout_line(/*ordinal*/ None, turn_started("turn-1"))
            ),
        ),
        (
            "duplicate ordinal",
            format!(
                "{}\n{}\n",
                rollout_line(Some(1), turn_started("turn-1")),
                rollout_line(Some(1), turn_started("turn-2")),
            ),
        ),
        (
            "out of order ordinal",
            format!("{}\n", rollout_line(Some(2), turn_started("turn-1"))),
        ),
        (
            "gap larger than rejected prefix",
            format!(
                "{{not json}}\n{}\n",
                rollout_line(Some(3), turn_started("turn-1")),
            ),
        ),
    ];
    for (name, suffix) in cases {
        let home = TempDir::new().expect("temp dir");
        let store = projection_store(home.path()).await;
        let thread_id = ThreadId::default();
        create_paginated_thread(&store, thread_id).await;
        store
            .persist_thread(thread_id)
            .await
            .expect("persist session metadata");

        let pool = codex_state::open_thread_history_db(
            &codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        )
        .await
        .expect("open thread history db");
        let before = projection_state(&pool, thread_id).await;
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("rollout path");
        append_suffix(rollout_path.as_path(), suffix.as_str());

        super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
            .await
            .expect_err(name);

        assert_eq!(
            projection_state(&pool, thread_id).await,
            before,
            "{name} should not advance projection state"
        );
        let counts = sqlx::query_as::<_, (i64, i64)>(
            r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?)
            "#,
        )
        .bind(thread_id.to_string())
        .bind(thread_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("read projected row counts");
        assert_eq!(counts, (0, 0), "{name} should not project rows");
    }
}

#[tokio::test]
async fn stateful_legacy_projection_removes_rolled_back_turns_and_items() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    let mut builder = ThreadHistoryBuilder::new();
    let items = [
        turn_started("retained-turn"),
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: "retained user".to_string(),
            ..Default::default()
        })),
        turn_completed("retained-turn"),
        turn_started("removed-turn"),
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: "removed user".to_string(),
            ..Default::default()
        })),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
            num_turns: 1,
        })),
    ];
    let projections = items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let ordinal = u64::try_from(index).expect("fixture ordinal");
            super::super::thread_history::RolloutProjectionStep::Line(
                super::super::thread_history::ProjectedRolloutLine {
                    ordinal,
                    start_byte_offset: ordinal,
                    end_byte_offset: ordinal + 1,
                    fallback_created_at_ms: Some(1),
                    changes: builder.handle_rollout_item_with_changes(item),
                },
            )
        })
        .collect();

    super::super::thread_history::apply_projection(
        &store,
        thread_id,
        /*start_offset*/ 0,
        u64::try_from(items.len()).expect("fixture length"),
        /*initial_ordinal*/ 0,
        projections,
    )
    .await
    .expect("project stateful legacy events");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let turns = sqlx::query_as::<_, (String,)>(
        "SELECT turn_id FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected turns");
    assert_eq!(turns, [("retained-turn".to_string(),)]);

    let items = sqlx::query_as::<_, (String, String)>(
        "SELECT turn_id, item_json FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected legacy items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, "retained-turn");
    assert!(items[0].1.contains("retained user"));
}

#[tokio::test]
async fn jsonl_failure_does_not_create_projection_database() {
    let home = TempDir::new().expect("temp dir");
    fs::write(home.path().join("sessions"), "not a directory").expect("block sessions dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1")],
        })
        .await
        .expect_err("JSONL append should fail");

    assert!(
        !codex_state::SqliteConfig::new_for_testing(home.path().abs())
            .thread_history_db_path()
            .exists()
    );
}

#[tokio::test]
async fn catch_up_rejects_missing_rollout_after_projection() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("close rollout");
    fs::remove_file(rollout_path.as_path()).expect("remove rollout");

    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect_err("missing projected rollout should fail");
}

#[tokio::test]
async fn sqlite_failure_does_not_fail_durable_jsonl_write() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    fs::create_dir(store.config.sqlite.thread_history_db_path())
        .expect("block thread history database");
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1")],
        })
        .await
        .expect("durable JSONL append should succeed");

    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let (items, _, _) = RolloutRecorder::load_rollout_items(rollout_path.as_path())
        .await
        .expect("load durable rollout");
    assert!(items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TurnStarted(event))
                if event.turn_id == "turn-1"
        )
    }));
}

#[tokio::test]
async fn blank_and_rejected_rollout_lines_do_not_poison_projection() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let before = projection_state(&pool, thread_id).await;
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(rollout_path.as_path())
        .expect("open rollout for rejected line");
    file.write_all(b"\n \t\r\n{not json}\n\xff\n")
        .expect("append blank and rejected lines");
    file.flush().expect("flush rejected line");
    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("leave rejected tail pending");
    assert_eq!(
        projection_state(&pool, thread_id).await,
        (before.0 + 5, before.1)
    );

    let recorder = store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .expect("live recorder")
        .recorder
        .clone();
    recorder
        .record_canonical_items(&[turn_started("turn-1")])
        .await
        .expect("queue valid retry");
    recorder.flush().await.expect("flush valid retry");

    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("project valid retry after rejected line");

    let (expected_start_byte_offset, _) =
        rollout_line_byte_offsets(rollout_path.as_path(), /*ordinal*/ 1);
    let start_byte_offset = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT rollout_byte_offset FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read projected turn byte offset");
    assert_eq!(start_byte_offset, Some(expected_start_byte_offset));
    let rollout_len = i64::try_from(fs::metadata(rollout_path).expect("rollout metadata").len())
        .expect("rollout length");
    assert_eq!(projection_state(&pool, thread_id).await, (rollout_len, 2));
}

#[tokio::test]
async fn unprojectable_rollout_lines_wait_for_later_ordinals() {
    let unknown_line = |ordinal| {
        format!(
            concat!(
                "{{\"timestamp\":\"2025-01-01T00:00:00.000Z\",\"ordinal\":{ordinal},",
                "\"type\":\"future_item\",\"payload\":{{}}}}"
            ),
            ordinal = ordinal
        )
    };
    let cases = [
        ("unknown payload", unknown_line(1), unknown_line(2)),
        (
            "structurally invalid JSON",
            "{}".to_string(),
            "null".to_string(),
        ),
    ];
    for (name, pending_line, skipped_line) in cases {
        let home = TempDir::new().expect("temp dir");
        let store = projection_store(home.path()).await;
        let thread_id = ThreadId::default();
        create_paginated_thread(&store, thread_id).await;
        store
            .persist_thread(thread_id)
            .await
            .expect("persist session metadata");

        let pool = codex_state::open_thread_history_db(
            &codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        )
        .await
        .expect("open thread history db");
        let before = projection_state(&pool, thread_id).await;
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("rollout path");
        append_suffix(rollout_path.as_path(), format!("{pending_line}\n").as_str());

        super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
            .await
            .expect("leave unprojectable tail pending");
        assert_eq!(projection_state(&pool, thread_id).await, before, "{name}");

        append_suffix(
            rollout_path.as_path(),
            format!(
                "{}\n{skipped_line}\n{}\n",
                rollout_line(Some(1), turn_started("retry-turn")),
                rollout_line(Some(3), turn_started("turn-1")),
            )
            .as_str(),
        );

        super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
            .await
            .expect(name);

        let rollout_len =
            i64::try_from(fs::metadata(rollout_path).expect("rollout metadata").len())
                .expect("rollout length");
        assert_eq!(
            projection_state(&pool, thread_id).await,
            (rollout_len, 4),
            "{name}"
        );
        let turn_ordinals = sqlx::query_as::<_, (String, i64)>(
            "SELECT turn_id, rollout_ordinal FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
        )
        .bind(thread_id.to_string())
        .fetch_all(&pool)
        .await
        .expect("read projected turns");
        assert_eq!(
            turn_ordinals,
            vec![("retry-turn".to_string(), 1), ("turn-1".to_string(), 3)],
            "{name}"
        );
    }
}

#[tokio::test]
async fn event_timestamps_allow_invalid_rollout_timestamps() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    let invalid_timestamp_line = |ordinal, item| {
        rollout_line(Some(ordinal), item).replace("2025-01-01T00:00:00.000Z", "not-a-timestamp")
    };
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    append_suffix(
        rollout_path.as_path(),
        format!(
            "{}\n{}\n",
            invalid_timestamp_line(1, turn_started("turn-1")),
            invalid_timestamp_line(
                2,
                completed_item(
                    thread_id,
                    "turn-1",
                    agent_message("agent-1", MessagePhase::FinalAnswer),
                ),
            ),
        )
        .as_str(),
    );

    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("project records with event timestamps");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let created_at_ms = sqlx::query_scalar::<_, i64>(
        "SELECT created_at_ms FROM thread_items WHERE thread_id = ? AND turn_id = ? AND item_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .bind("agent-1")
    .fetch_one(&pool)
    .await
    .expect("read projected item timestamp");
    assert_eq!(created_at_ms, 0);
    let rollout_len = i64::try_from(fs::metadata(rollout_path).expect("rollout metadata").len())
        .expect("rollout length");
    assert_eq!(projection_state(&pool, thread_id).await, (rollout_len, 3));
}

#[tokio::test]
async fn malformed_rollout_lines_skip_inferred_ordinal_gaps() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");

    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");
    append_suffix(
        rollout_path.as_path(),
        format!(
            "{{not json}}\n{}\n",
            rollout_line(Some(2), turn_started("turn-1"))
        )
        .as_str(),
    );

    super::materialize_to_sqlite(&store, thread_id, rollout_path.as_path())
        .await
        .expect("skip malformed gap and project later history");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let rollout_len = i64::try_from(fs::metadata(rollout_path).expect("rollout metadata").len())
        .expect("rollout length");
    assert_eq!(projection_state(&pool, thread_id).await, (rollout_len, 3));
    let turn_ordinal = sqlx::query_scalar::<_, i64>(
        "SELECT rollout_ordinal FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read projected turn");
    assert_eq!(turn_ordinal, 2);
}

#[tokio::test]
async fn shutdown_materializes_items_queued_without_a_flush() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    let recorder = store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .expect("live recorder")
        .recorder
        .clone();
    recorder
        .record_canonical_items(&[turn_started("turn-1")])
        .await
        .expect("queue rollout item");

    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown live thread");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let projected_turns = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
    )
    .bind(thread_id.to_string())
    .bind("turn-1")
    .fetch_one(&pool)
    .await
    .expect("read projected turns");
    assert_eq!(projected_turns, 1);
}

#[tokio::test]
async fn delete_waits_for_in_flight_projection_before_removing_rows() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    store
        .persist_thread(thread_id)
        .await
        .expect("persist session metadata");
    let write_permit = store.live_writer_locks.lock(thread_id).await;

    let append_store = store.clone();
    let append = tokio::spawn(async move {
        append_store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![turn_started("turn-1")],
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
    let delete_store = store.clone();
    let delete = tokio::spawn(async move {
        delete_store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
    });
    tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
    assert!(!delete.is_finished());

    drop(write_permit);
    append
        .await
        .expect("join append")
        .expect("finish in-flight append");
    delete.await.expect("join delete").expect("delete thread");

    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .expect("open thread history db");
    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_history_projection_state WHERE thread_id = ?)
        "#,
    )
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read history row counts");
    assert_eq!(counts, (0, 0, 0));
}

async fn projection_store(codex_home: &Path) -> LocalThreadStore {
    let config = test_config(codex_home);
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database for paginated history");
    LocalThreadStore::new(config, Some(state_db))
}

async fn index_paginated_source_metadata(store: &LocalThreadStore, thread_id: ThreadId) {
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("active paginated rollout");
    let state_db = store.state_db().await.expect("state database");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        active_path,
        Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    state_db
        .upsert_thread(&metadata.build("test-provider"))
        .await
        .expect("index projected paginated root metadata");
}

async fn create_paginated_thread(store: &LocalThreadStore, thread_id: ThreadId) {
    create_paginated_subagent_thread(
        store, thread_id, /*history_base*/ None, /*subagent_history_start_ordinal*/ None,
    )
    .await;
}

async fn prepare_paginated_fork(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    boundary: ForkBoundary,
) -> PreparedFork {
    store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary,
        })
        .await
        .expect("prepare paginated fork")
}

async fn create_legacy_thread(store: &LocalThreadStore, thread_id: ThreadId) {
    store
        .create_thread(CreateThreadParams {
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: None,
            parent_thread_id: None,
            source: SessionSource::Exec,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: ThreadHistoryMode::Legacy,
            history_base: None,
            subagent_history_start_ordinal: None,
            persistence_mode: crate::ThreadPersistenceMode::Durable,
            initial_rollout_ordinal: 0,
            initial_window_id: "window-1".to_string(),
            metadata: ThreadPersistenceMetadata {
                cwd: Some(std::env::current_dir().expect("cwd")),
                model_provider: "test-provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await
        .expect("create legacy thread");
}

async fn create_paginated_subagent_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    subagent_history_start_ordinal: Option<u64>,
) {
    store
        .create_thread(CreateThreadParams {
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: None,
            parent_thread_id: None,
            source: SessionSource::Exec,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: ThreadHistoryMode::Paginated,
            history_base,
            subagent_history_start_ordinal,
            persistence_mode: crate::ThreadPersistenceMode::Durable,
            initial_rollout_ordinal: 0,
            initial_window_id: "window-1".to_string(),
            metadata: ThreadPersistenceMetadata {
                cwd: Some(std::env::current_dir().expect("cwd")),
                model_provider: "test-provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await
        .expect("create paginated thread");
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

fn user_message(message: &str) -> RolloutItem {
    RolloutItem::ResponseItem(ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: message.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    })
}

fn certified_projection_checkpoint(
    replacement_history: Vec<ResponseItem>,
    window_number: u64,
) -> CertifiedSegmentStateCheckpoint {
    let window_id = Uuid::now_v7();
    CertifiedSegmentStateCheckpoint::new(
        CompactedItem {
            message: String::new(),
            replacement_history: Some(replacement_history),
            window_number: Some(window_number),
            first_window_id: Some(window_id.to_string()),
            previous_window_id: None,
            window_id: Some(window_id.to_string()),
            segment_state_checkpoint: None,
        },
        Some(SegmentPreviousTurnSettings {
            model: "test-model".to_string(),
            comp_hash: None,
            realtime_active: None,
        }),
        /*world_state*/ None,
        /*reference_context*/ None,
        ThreadSettingsAppliedEvent {
            thread_settings: ThreadSettingsSnapshot {
                model: "test-model".to_string(),
                model_provider_id: "test-provider".to_string(),
                service_tier: None,
                approval_policy: AskForApproval::Never,
                approvals_reviewer: ApprovalsReviewer::User,
                permission_profile: PermissionProfile::workspace_write(),
                active_permission_profile: None,
                cwd: serde_json::from_value(serde_json::json!("/tmp")).expect("absolute test cwd"),
                environments: Some(TurnEnvironmentSelections::new(
                    serde_json::from_value(serde_json::json!("/tmp")).expect("absolute test cwd"),
                    Vec::new(),
                )),
                workspace_roots: Some(Vec::new()),
                profile_workspace_roots: Some(Vec::new()),
                windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
                reasoning_effort: None,
                reasoning_summary: None,
                personality: None,
                collaboration_mode: CollaborationMode {
                    mode: ModeKind::Default,
                    settings: Settings {
                        model: "test-model".to_string(),
                        reasoning_effort: None,
                        developer_instructions: None,
                    },
                },
            },
        },
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid projected checkpoint")
}

fn contains_user_message(items: &[RolloutItem], expected: &str) -> bool {
    items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::ResponseItem(ResponseItem::Message { content, .. })
                if content.iter().any(|content| {
                    matches!(content, ContentItem::InputText { text } if text == expected)
                })
        )
    })
}

fn legacy_user_message(message: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: message.to_string(),
        ..Default::default()
    }))
}

fn legacy_agent_message(message: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
        message: message.to_string(),
        phase: Some(MessagePhase::FinalAnswer),
        memory_citation: None,
    }))
}

fn completed_item(thread_id: ThreadId, turn_id: &str, item: TurnItem) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item,
        started_at_ms: Some(0),
        completed_at_ms: 1,
    }))
}

fn agent_message(id: &str, phase: MessagePhase) -> TurnItem {
    TurnItem::AgentMessage(AgentMessageItem {
        id: id.to_string(),
        content: vec![AgentMessageContent::Text {
            text: id.to_string(),
        }],
        phase: Some(phase),
        memory_citation: None,
    })
}

fn rollout_line_byte_offsets(path: &std::path::Path, ordinal: u64) -> (i64, i64) {
    let bytes = fs::read(path).expect("read rollout");
    let mut start_byte_offset = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let end_byte_offset = start_byte_offset + line.len();
        if serde_json::from_slice::<RolloutLine>(line)
            .ok()
            .and_then(|line| line.ordinal)
            == Some(ordinal)
        {
            return (
                i64::try_from(start_byte_offset).expect("start byte offset fits i64"),
                i64::try_from(end_byte_offset).expect("end byte offset fits i64"),
            );
        }
        start_byte_offset = end_byte_offset;
    }
    panic!("missing rollout ordinal {ordinal}");
}

fn compress_rollout(path: &Path) {
    let mut compressed_path = path.as_os_str().to_os_string();
    compressed_path.push(".zst");
    let compressed = zstd::stream::encode_all(
        fs::File::open(path).expect("open rollout for compression"),
        /*level*/ 0,
    )
    .expect("compress rollout");
    fs::write(compressed_path, compressed).expect("write compressed rollout");
    fs::remove_file(path).expect("remove plain rollout");
}

async fn projection_state(pool: &sqlx::SqlitePool, thread_id: ThreadId) -> (i64, i64) {
    sqlx::query_as::<_, (i64, i64)>(
        r#"
SELECT next_rollout_byte_offset, next_rollout_ordinal
FROM thread_history_projection_state
WHERE thread_id = ?
        "#,
    )
    .bind(thread_id.to_string())
    .fetch_one(pool)
    .await
    .expect("read projection state")
}

async fn apply_history_changes(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    changes: ThreadHistoryChangeSet,
) {
    let state = super::super::thread_history::projection_state(store, thread_id)
        .await
        .expect("read existing projection state")
        .expect("history projection exists");
    let start_byte_offset = state.next_byte_offset;
    let ordinal = state.next_ordinal;
    let end_byte_offset = start_byte_offset
        .checked_add(1)
        .expect("advance projection byte offset");
    super::super::thread_history::apply_projection(
        store,
        thread_id,
        start_byte_offset,
        end_byte_offset,
        ordinal,
        vec![super::super::thread_history::RolloutProjectionStep::Line(
            super::super::thread_history::ProjectedRolloutLine {
                ordinal,
                start_byte_offset,
                end_byte_offset,
                fallback_created_at_ms: Some(1),
                changes,
            },
        )],
    )
    .await
    .expect("apply history changes");
}

fn rollout_line(ordinal: Option<u64>, item: RolloutItem) -> String {
    serde_json::to_string(&RolloutLine {
        timestamp: "2025-01-01T00:00:00.000Z".to_string(),
        ordinal,
        item,
    })
    .expect("serialize rollout line")
}

fn append_suffix(rollout_path: &std::path::Path, suffix: &str) {
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(rollout_path)
        .expect("open rollout suffix");
    file.write_all(suffix.as_bytes()).expect("append suffix");
    file.flush().expect("flush suffix");
}
