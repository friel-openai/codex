use super::session_request_support::start_recording_app_server;
use super::*;
use app_test_support::create_fake_parented_rollout_with_source;
use app_test_support::create_fake_rollout;
use app_test_support::rollout_path;
use codex_protocol::AgentPath;
use codex_state::SqliteConfig;
use pretty_assertions::assert_eq;
use std::sync::Mutex;

/// Returns and resets `(thread/loaded/list, thread/read)` request counts.
fn take_backfill_counts(requests: &Arc<Mutex<Vec<String>>>) -> (usize, usize) {
    let requests = std::mem::take(&mut *requests.lock().expect("request recorder lock"));
    (
        requests
            .iter()
            .filter(|method| *method == "thread/loaded/list")
            .count(),
        requests
            .iter()
            .filter(|method| *method == "thread/read")
            .count(),
    )
}

#[test]
fn fresh_session_applies_requested_name() -> Result<()> {
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

    std::thread::Builder::new()
        .name("tui-named-fresh-session".to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let mut app = make_test_app().await;
                let codex_home = tempdir()?;
                app.config.codex_home = codex_home.path().to_path_buf().abs();
                app.config.sqlite = SqliteConfig::new_for_testing(codex_home.path().abs());
                let (mut app_server, requests, proxy) =
                    start_recording_app_server(&app.config, /*blocked_thread_list*/ None).await?;
                let mut tui = crate::tui::test_support::make_test_tui()?;

                app.start_fresh_session_with_summary_hint(
                    &mut tui,
                    &mut app_server,
                    /*session_start_source*/ None,
                    /*initial_user_message*/ None,
                    /*new_thread_name*/ Some("Add User".to_string()),
                )
                .await;

                let thread_id = app
                    .chat_widget
                    .thread_id()
                    .expect("fresh session should have a thread id");
                assert_eq!(app.chat_widget.thread_name(), Some("Add User".to_string()));
                assert!(
                    requests
                        .lock()
                        .expect("request recorder lock")
                        .iter()
                        .any(|method| method == "thread/name/set"),
                    "fresh session should be named through the app server"
                );
                let thread = app_server
                    .thread_read(thread_id, /*include_turns*/ false)
                    .await?;
                assert_eq!(thread.name.as_deref(), Some("Add User"));

                app_server.shutdown().await?;
                proxy.await??;
                Ok(())
            })
        })?
        .join()
        .expect("named fresh session test thread")
}

#[test]
fn session_lifecycle_avoids_redundant_subagent_metadata_reads() -> Result<()> {
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

    std::thread::Builder::new()
        .name("tui-session-lifecycle-requests".to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let mut app = make_test_app().await;
                let codex_home = tempdir()?;
                app.config.codex_home = codex_home.path().to_path_buf().abs();
                app.config.sqlite =
                    codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
                let root_timestamp = "2026-01-01T00-00-00";
                let root_thread_id = ThreadId::from_string(
                    &create_fake_rollout(
                        codex_home.path(),
                        root_timestamp,
                        "2026-01-01T00:00:00Z",
                        "Saved user message",
                        Some(app.config.model_provider_id.as_str()),
                        /*git_info*/ None,
                    )
                    .expect("create root rollout"),
                )?;
                let child_thread_id = ThreadId::from_string(
                    &create_fake_parented_rollout_with_source(
                        codex_home.path(),
                        "2026-01-01T00-00-01",
                        "2026-01-01T00:00:01Z",
                        "Saved child message",
                        Some(app.config.model_provider_id.as_str()),
                        /*git_info*/ None,
                        RolloutSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                            parent_thread_id: root_thread_id,
                            depth: 1,
                            agent_path: Some(
                                AgentPath::try_from("/root/worker").expect("valid agent path"),
                            ),
                            agent_nickname: Some("worker".to_string()),
                            agent_role: Some("worker".to_string()),
                        }),
                        root_thread_id.into(),
                        root_thread_id,
                    )
                    .expect("create child rollout"),
                )?;
                let root_rollout_path = rollout_path(
                    codex_home.path(),
                    root_timestamp,
                    &root_thread_id.to_string(),
                );
                let (mut app_server, requests, proxy) =
                    start_recording_app_server(&app.config, /*blocked_thread_list*/ None).await?;
                let root = app_server
                    .resume_thread(
                        app.config.clone(),
                        root_thread_id,
                        app.resume_model_settings(),
                    )
                    .await?;
                app.enqueue_primary_thread_session(root.session, root.turns)
                    .await?;
                app_server
                    .resume_thread(
                        app.config.clone(),
                        child_thread_id,
                        app.resume_model_settings(),
                    )
                    .await?;
                let mut tui = crate::tui::test_support::make_test_tui()?;
                take_backfill_counts(&requests);

                let control = Box::pin(app.handle_event(
                    &mut tui,
                    &mut app_server,
                    AppEvent::ForkCurrentSession { placement: None },
                ))
                .await?;

                assert!(matches!(control, AppRunControl::Continue));
                assert_ne!(app.chat_widget.thread_id(), Some(root_thread_id));
                // Forking may read the source metadata once when the response includes its parent
                // id. It must not scan or backfill loaded threads for the newly created fork.
                assert!(matches!(take_backfill_counts(&requests), (0, 0) | (0, 1)));

                app.start_fresh_session_with_summary_hint(
                    &mut tui,
                    &mut app_server,
                    /*session_start_source*/ None,
                    /*initial_user_message*/ None,
                    /*new_thread_name*/ None,
                )
                .await;

                assert_ne!(app.chat_widget.thread_id(), Some(root_thread_id));
                assert_eq!(take_backfill_counts(&requests), (0, 0));

                let loaded_threads = app_server
                    .thread_loaded_list(ThreadLoadedListParams {
                        cursor: None,
                        limit: None,
                        ancestor_thread_id: Some(root_thread_id.to_string()),
                    })
                    .await?
                    .data;
                assert_eq!(loaded_threads, vec![child_thread_id.to_string()]);
                let expected_reads = loaded_threads.len();
                take_backfill_counts(&requests);
                app.harness_overrides.cwd = Some(app.config.cwd.to_path_buf());

                let control = app
                    .resume_target_session(
                        &mut tui,
                        &mut app_server,
                        crate::resume_picker::SessionTarget {
                            path: Some(root_rollout_path),
                            thread_id: root_thread_id,
                        },
                    )
                    .await?;

                assert!(matches!(control, AppRunControl::Continue));
                assert_eq!(app.chat_widget.thread_id(), Some(root_thread_id));
                assert_eq!(take_backfill_counts(&requests), (1, expected_reads));
                assert_eq!(
                    app.agent_navigation.get(&child_thread_id),
                    Some(&AgentPickerThreadEntry {
                        agent_nickname: Some("worker".to_string()),
                        agent_role: Some("worker".to_string()),
                        agent_path: Some("/root/worker".to_string()),
                        is_running: false,
                        is_closed: false,
                    })
                );

                Box::pin(app.open_agent_picker(&mut app_server)).await;

                // Opening the picker uses cached agents; its background refresh must not block on
                // a global loaded-thread scan or synchronously read thread metadata.
                assert_eq!(take_backfill_counts(&requests), (0, 0));
                app_server.shutdown().await?;
                proxy.await??;
                Ok(())
            })
        })?
        .join()
        .expect("session lifecycle request test thread")
}
