//! Attached-thread fork and discovery after restart and SQLite recovery, including pre-database parents.
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::append_fake_paginated_user_message;
use app_test_support::create_fake_paginated_rollout;
use app_test_support::create_fake_rollout;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadDeleteParams;
use codex_app_server_protocol::ThreadDeleteResponse;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadItemsListCursor;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeInitialTurnsPageParams;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSearchResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use codex_state::SqliteConfig;
use codex_thread_store::FreezeRolloutSegmentParams;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use test_case::test_case;

#[derive(Clone, Copy)]
enum Recovery {
    Restart,
    Missing,
    Corrupt,
}

#[test_case(false; "migration disabled")]
#[test_case(true; "default migration")]
#[tokio::test]
async fn cold_attached_resume_repairs_incomplete_projection_without_exposing_parent(
    background_migration: bool,
) -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let home = TempDir::new()?;
    let config = MockResponsesConfig::new(&server.uri());
    if background_migration {
        config.write(home.path())?;
    } else {
        config
            .disable_feature(Feature::BackgroundPaginatedRolloutMigration)
            .write(home.path())?;
    }
    let parent = create_fake_paginated_rollout(
        home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "parent-only context",
        Some("mock_provider"),
        None,
    )?;
    let parent_path = app_test_support::rollout_path(home.path(), "2025-01-05T12-00-00", &parent);
    append_fake_paginated_user_message(&parent_path, &parent, "parent-only context").await?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let mut children = Vec::new();
    for _ in 0..2 {
        let id = mcp
            .send_thread_fork_request(ThreadForkParams {
                thread_id: parent.clone(),
                attach_to_source: true,
                exclude_turns: true,
                ..Default::default()
            })
            .await?;
        let fork: ThreadForkResponse = mcp.read_response(id).await?;
        let path = fork.thread.path.expect("attached rollout");
        let meta = codex_rollout::read_session_meta_line_exact(&path)
            .await?
            .meta;
        assert!(meta.attached_thread);
        assert_eq!(
            meta.forked_from_id.map(|id| id.to_string()),
            Some(parent.clone())
        );
        assert!(
            meta.history_base
                .is_some_and(|base| base.thread_id != meta.id)
        );
        children.push((fork.thread.id, path));
    }
    let mut turn_ids = Vec::new();
    for text in ["first child turn"] {
        let id = mcp
            .send_turn_start_request(TurnStartParams {
                thread_id: children[0].0.clone(),
                input: vec![UserInput::Text {
                    text: text.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            })
            .await?;
        let started: TurnStartResponse = mcp.read_response(id).await?;
        turn_ids.push(started.turn.id);
        mcp.read_stream_until_notification_message("turn/completed")
            .await?;
    }
    assert!(mcp.shutdown_gracefully().await?.success());

    let sqlite = SqliteConfig::new_for_testing(home.path().abs());
    let state =
        codex_state::StateRuntime::init(sqlite.clone(), "mock_provider".to_string()).await?;
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: home.path().to_path_buf(),
            sqlite: sqlite.clone(),
            default_model_provider_id: "mock_provider".to_string(),
        },
        Some(state),
    );
    let child_id = codex_protocol::ThreadId::from_string(&children[0].0)?;
    let frozen = store
        .freeze_thread_segment(child_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await?;
    let frozen_id = frozen.reference.rollout_id.expect("physical predecessor");
    assert_ne!(frozen_id, child_id);
    let root_meta = codex_rollout::read_session_meta_line_exact(&children[0].1)
        .await?
        .meta;
    assert_eq!(
        root_meta.history_base.map(|base| base.thread_id),
        Some(frozen_id)
    );
    drop(store);
    let mut mcp = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: children[0].0.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse = mcp.read_response(id).await?;
    let id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: children[0].0.clone(),
            input: vec![UserInput::Text {
                text: "second child turn".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let started: TurnStartResponse = mcp.read_response(id).await?;
    turn_ids.push(started.turn.id);
    mcp.read_stream_until_notification_message("turn/completed")
        .await?;
    assert!(mcp.shutdown_gracefully().await?.success());

    let pool = sqlite
        .open_read_write_pool(&sqlite.thread_history_db_path())
        .await?;
    for (id, path) in &children {
        sqlx::query("DELETE FROM thread_items WHERE thread_id = ?")
            .bind(id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM thread_turns WHERE thread_id = ?")
            .bind(id)
            .execute(&pool)
            .await?;
        let offset = i64::try_from(std::fs::metadata(path)?.len())?;
        sqlx::query(
            "INSERT INTO thread_history_projection_state \
             (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, 0) \
             ON CONFLICT(thread_id) DO UPDATE SET next_rollout_byte_offset = excluded.next_rollout_byte_offset",
        )
        .bind(id)
        .bind(-1 - offset)
        .execute(&pool)
        .await?;
    }
    sqlx::query("DELETE FROM thread_items WHERE thread_id = ?")
        .bind(frozen_id.to_string())
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM thread_turns WHERE thread_id = ?")
        .bind(frozen_id.to_string())
        .execute(&pool)
        .await?;
    pool.close().await;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: children[0].0.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let resumed: ThreadResumeResponse = mcp.read_response(id).await?;
    assert!(resumed.thread.turns.is_empty());
    let mut cursor = resumed.turns_backwards_cursor;
    assert!(cursor.is_some());
    let mut item_cursor = resumed.items_backwards_cursor;
    assert!(item_cursor.is_some());
    for expected in turn_ids.iter().rev() {
        let id = mcp
            .send_thread_turns_list_request(ThreadTurnsListParams {
                thread_id: children[0].0.clone(),
                cursor,
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::NotLoaded),
            })
            .await?;
        let page: ThreadTurnsListResponse = mcp.read_response(id).await?;
        assert_eq!(page.data.len(), 1);
        assert_eq!(&page.data[0].id, expected);
        cursor = page.next_cursor;
    }
    assert!(cursor.is_none());
    let mut item_turns = Vec::new();
    loop {
        let id = mcp
            .send_thread_items_list_request(ThreadItemsListParams {
                thread_id: children[0].0.clone(),
                turn_id: None,
                cursor: item_cursor.map(ThreadItemsListCursor::Opaque),
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                defer_output: None,
            })
            .await?;
        let page: ThreadItemsListResponse = mcp.read_response(id).await?;
        item_turns.extend(page.data.into_iter().map(|item| item.turn_id));
        item_cursor = page.next_cursor;
        if item_cursor.is_none() {
            break;
        }
        assert!(item_turns.len() < 20, "item cursor must advance");
    }
    assert!(item_turns.len() >= 4);
    assert_eq!(item_turns.first(), turn_ids.last());
    assert!(item_turns.iter().all(|id| turn_ids.contains(id)));
    assert!(turn_ids.iter().all(|id| item_turns.contains(id)));

    let id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: children[1].0.clone(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::NotLoaded),
            }),
            ..Default::default()
        })
        .await?;
    let empty: ThreadResumeResponse = mcp.read_response(id).await?;
    assert!(
        empty
            .initial_turns_page
            .expect("initial page")
            .data
            .is_empty()
    );
    assert!(empty.turns_backwards_cursor.is_none());
    assert!(empty.items_backwards_cursor.is_none());
    Ok(())
}

#[test_case(Recovery::Restart; "paginated restart")]
#[test_case(Recovery::Missing; "paginated missing database")]
#[test_case(Recovery::Corrupt; "paginated corrupt database")]
#[tokio::test]
async fn saved_attached_thread_upgrade_restart_and_delete(recovery: Recovery) -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    // Both parents were persisted before this app-server (and its SQLite migration) starts.
    let parent = create_fake_paginated_rollout(
        home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "BLUE HERON 72",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let other = create_fake_paginated_rollout(
        home.path(),
        "2025-01-06T12-00-00",
        "2025-01-06T12:00:00Z",
        "unrelated",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let mut children = Vec::new();
    for _ in 0..2 {
        let id = mcp
            .send_thread_fork_request(ThreadForkParams {
                thread_id: parent.clone(),
                attach_to_source: true,
                exclude_turns: true,
                ..Default::default()
            })
            .await?;
        let fork: ThreadForkResponse = mcp.read_response(id).await?;
        assert!(!fork.thread.ephemeral);
        assert_eq!(fork.thread.forked_from_id, Some(parent.clone()));
        assert_eq!(fork.thread.attached_to_thread_id.as_ref(), Some(&parent));
        children.push(fork.thread.id);
    }
    // Newer attached threads fill the raw scan page. Listing must refill across hidden
    // results, with and without title filters and in both scan directions.
    for (params, expected) in [
        (
            json!({"limit": 1, "searchTerm": "BLUE HERON"}),
            vec![&parent],
        ),
        (
            json!({"limit": 1, "searchTerm": "BLUE HERON", "useStateDbOnly": true}),
            vec![&parent],
        ),
        (
            json!({"limit": 2, "modelProviders": ["mock_provider"]}),
            vec![&other, &parent],
        ),
        (
            json!({"limit": 2, "modelProviders": ["mock_provider"], "sortDirection": "asc"}),
            vec![&parent, &other],
        ),
        (
            json!({"limit": 2, "useStateDbOnly": true}),
            vec![&other, &parent],
        ),
        // The existing parent filter continues to mean spawned children.
        (json!({"parentThreadId": parent}), vec![]),
    ] {
        let id = mcp.send_raw_request("thread/list", Some(params)).await?;
        let ordinary: ThreadListResponse = mcp.read_response(id).await?;
        assert_eq!(
            ordinary.data.iter().map(|t| &t.id).collect::<Vec<_>>(),
            expected
        );
    }
    let sqlite = SqliteConfig::new_for_testing(home.path().canonicalize()?.as_path().abs());
    assert!(mcp.shutdown_gracefully().await?.success());
    // No child has taken a turn yet; its attachment must still survive a restart.
    match recovery {
        Recovery::Restart => {}
        Recovery::Missing => tokio::fs::remove_file(sqlite.state_db_path()).await?,
        Recovery::Corrupt => {
            tokio::fs::write(sqlite.state_db_path(), b"damaged state database").await?
        }
    }
    let mut mcp = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let id = mcp
        .send_raw_request(
            "thread/list",
            Some(json!({"useStateDbOnly": true, "limit": 2})),
        )
        .await?;
    let ordinary: ThreadListResponse = mcp.read_response(id).await?;
    assert_eq!(
        ordinary.data.iter().map(|t| &t.id).collect::<Vec<_>>(),
        vec![&other, &parent]
    );
    let mut cursor = None;
    let mut found = Vec::new();
    loop {
        let id = mcp
            .send_raw_request(
                "thread/list",
                Some(json!({
                    "attachedToThreadId": parent, "limit": 1, "cursor": cursor
                })),
            )
            .await?;
        let page: ThreadListResponse = mcp.read_response(id).await?;
        assert_eq!(page.data.len(), 1);
        found.push(page.data[0].id.clone());
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
        assert!(
            found.len() <= children.len(),
            "attached thread cursor must advance"
        );
    }
    found.sort();
    children.sort();
    assert_eq!(found, children);
    let id = mcp
        .send_raw_request("thread/read", Some(json!({"threadId": children[0]})))
        .await?;
    let cold: ThreadReadResponse = mcp.read_response(id).await?;
    assert_eq!(cold.thread.attached_to_thread_id.as_ref(), Some(&parent));
    let id = mcp
        .send_raw_request("thread/list", Some(json!({"attachedToThreadId": other})))
        .await?;
    let other_list: ThreadListResponse = mcp.read_response(id).await?;
    assert!(other_list.data.is_empty());
    let id = mcp
        .send_raw_request(
            "thread/resume",
            Some(json!({
                "threadId": children[0],
                "developerInstructions": "Follow BIRCH-849."
            })),
        )
        .await?;
    let resumed: ThreadResumeResponse = mcp.read_response(id).await?;
    assert_eq!(resumed.thread.id, children[0]);
    assert!(resumed.thread.turns.is_empty());
    // Ordinary forks do not inherit attachment.
    let id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: children[0].clone(),
            ..Default::default()
        })
        .await?;
    let independent: ThreadForkResponse = mcp.read_response(id).await?;
    assert_eq!(independent.thread.attached_to_thread_id, None);
    let id = mcp
        .send_raw_request(
            "thread/list",
            Some(json!({"attachedToThreadId": children[0]})),
        )
        .await?;
    let attached: ThreadListResponse = mcp.read_response(id).await?;
    assert!(attached.data.is_empty());
    let id = mcp
        .send_thread_delete_request(ThreadDeleteParams {
            thread_id: independent.thread.id,
        })
        .await?;
    let _: ThreadDeleteResponse = mcp.read_response(id).await?;
    let id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: children[0].clone(),
            input: vec![UserInput::Text {
                text: "continue the chartreuse calculation".into(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let started: TurnStartResponse = mcp.read_response(id).await?;
    mcp.read_stream_until_notification_message("turn/completed")
        .await?;
    let requests = server.received_requests().await.expect("response requests");
    let input = requests
        .iter()
        .find(|r| r.url.path().ends_with("/responses"))
        .expect("resumed attached thread sends a model request")
        .body_json::<serde_json::Value>()?["input"]
        .to_string();
    assert!(
        input.contains("BIRCH-849"),
        "resume applies client instructions"
    );
    assert!(
        input.contains("BLUE HERON 72"),
        "model recalls the parent after restart"
    );
    let id = mcp
        .send_raw_request(
            "thread/read",
            Some(json!({"threadId": children[0], "includeTurns": true})),
        )
        .await?;
    let read: ThreadReadResponse = mcp.read_response(id).await?;
    let id = mcp
        .send_raw_request(
            "thread/name/set",
            Some(json!({
                "threadId": children[0], "name": "chartreuse calculation"
            })),
        )
        .await?;
    let _: serde_json::Value = mcp.read_response(id).await?;
    for include in [false, true] {
        let params =
            json!({"searchTerm": "chartreuse calculation", "includeAttachedThreads": include});
        let id = mcp
            .send_raw_request("thread/list", Some(params.clone()))
            .await?;
        let by_title: ThreadListResponse = mcp.read_response(id).await?;
        let id = mcp.send_raw_request("thread/search", Some(params)).await?;
        let by_content: ThreadSearchResponse = mcp.read_response(id).await?;
        for results in [
            by_title.data,
            by_content.data.into_iter().map(|hit| hit.thread).collect(),
        ] {
            let destinations = results
                .into_iter()
                .map(|thread| (thread.id, thread.attached_to_thread_id))
                .collect::<Vec<_>>();
            assert_eq!(
                destinations,
                if include {
                    vec![(children[0].clone(), Some(parent.clone()))]
                } else {
                    vec![]
                }
            );
        }
    }
    assert_eq!(read.thread.turns.len(), 1);
    assert_eq!(read.thread.turns[0].id, started.turn.id);
    let id = mcp
        .send_raw_request("thread/resume", Some(json!({"threadId": parent})))
        .await?;
    let _: ThreadResumeResponse = mcp.read_response(id).await?;
    let id = mcp
        .send_thread_delete_request(ThreadDeleteParams {
            thread_id: parent.clone(),
        })
        .await?;
    let error = mcp
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert!(
        error
            .error
            .message
            .contains("delete the attached thread first")
    );
    let id = mcp
        .send_thread_loaded_list_request(ThreadLoadedListParams::default())
        .await?;
    let loaded: ThreadLoadedListResponse = mcp.read_response(id).await?;
    assert!(
        loaded.data.contains(&parent),
        "rejection must leave the parent loaded"
    );
    for child in children {
        let id = mcp
            .send_thread_delete_request(ThreadDeleteParams { thread_id: child })
            .await?;
        let _: ThreadDeleteResponse = mcp.read_response(id).await?;
    }
    let id = mcp
        .send_thread_delete_request(ThreadDeleteParams { thread_id: parent })
        .await?;
    let _: ThreadDeleteResponse = mcp.read_response(id).await?;
    let id = mcp.send_raw_request("thread/list", Some(json!({}))).await?;
    let ordinary: ThreadListResponse = mcp.read_response(id).await?;
    assert_eq!(
        ordinary.data.iter().map(|t| &t.id).collect::<Vec<_>>(),
        vec![&other]
    );
    Ok(())
}

#[tokio::test]
async fn saved_attached_thread_rejects_conflicting_fork_and_discovery_filters() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let parent = create_fake_rollout(
        home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "parent",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    for (method, params, message) in [
        (
            "thread/fork",
            json!({"threadId": parent, "attachToSource": true}),
            "attachToSource requires a paginated parent",
        ),
        (
            "thread/fork",
            json!({"threadId": parent, "attachToSource": true, "ephemeral": true}),
            "attachToSource cannot be combined",
        ),
        (
            "thread/list",
            json!({"attachedToThreadId": parent, "parentThreadId": parent}),
            "attachedToThreadId cannot be combined",
        ),
        (
            "thread/list",
            json!({"attachedToThreadId": parent, "ancestorThreadId": parent}),
            "attachedToThreadId cannot be combined",
        ),
        (
            "thread/fork",
            json!({"threadId": parent, "attachToSource": true, "deferGoalContinuation": true}),
            "attachToSource cannot be combined",
        ),
    ] {
        let id = mcp.send_raw_request(method, Some(params)).await?;
        let error = mcp
            .read_stream_until_error_message(RequestId::Integer(id))
            .await?;
        assert!(
            error.error.message.starts_with(message),
            "{:?}",
            error.error
        );
    }
    let id = mcp
        .send_raw_request("thread/list", Some(json!({"attachedToThreadId": parent})))
        .await?;
    let page: ThreadListResponse = mcp.read_response(id).await?;
    assert!(
        page.data.is_empty(),
        "failed forks must leave no discoverable attached thread"
    );
    Ok(())
}
