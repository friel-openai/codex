use super::*;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
use codex_core::StartThreadOptions;
use codex_protocol::ResponseItemId;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_thread_store::LiveThread;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::ThreadPersistenceMetadata;
use pretty_assertions::assert_eq;

const LIVE_MESSAGE: &str = "message accepted only by the writerless saved runtime";

fn marker(text: &str, turn_id: &str) -> ResponseItem {
    let mut item = ResponseItem::Message {
        id: Some(ResponseItemId::with_suffix("msg", text)),
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: Some(MessagePhase::FinalAnswer),
        internal_chat_message_metadata_passthrough: None,
    };
    item.set_turn_id_if_missing(turn_id);
    item
}

fn assert_live_message_once<'a>(items: impl Iterator<Item = &'a ThreadItem>) {
    assert_eq!(
        items
            .filter_map(|item| match item {
                ThreadItem::AgentMessage { text, .. } if text == LIVE_MESSAGE =>
                    Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![LIVE_MESSAGE],
    );
}

#[test]
#[serial(app_server_tracing)]
fn saved_thread_read_repairs_live_runtime_and_rejects_stale_fallback() -> Result<()> {
    run_current_thread_test_with_stack("saved-thread-api-repair", async {
        let mut harness = TracingHarness::new_with_sqlite().await?;
        // The outer transport registers the connection after writing Initialize's
        // response. Running-thread resume does not reply to a closed connection.
        harness
            .processor
            .connection_initialized(TEST_CONNECTION_ID, harness.session.request_attestation())
            .await;
        let (manager, store) = harness
            .processor
            .thread_processor
            .saved_thread_persistence_test_resources();
        let config = ConfigBuilder::default()
            .codex_home(harness._codex_home.path().to_path_buf())
            .build()
            .await?;
        let auth_manager =
            AuthManager::shared_from_config(&config, /*enable_codex_api_key_env*/ false).await?;

        for (case, block_writer) in [false, true].into_iter().enumerate() {
            let mut options = StartThreadOptions::new(config.clone());
            options.history_mode = Some(ThreadHistoryMode::Paginated);
            let saved = manager.start_thread(options).await?;
            let thread_id = saved.thread_id;
            saved
                .thread
                .inject_response_items(vec![marker("already saved", "saved-api-turn")])
                .await?;
            saved.thread.ensure_rollout_materialized().await;
            saved.thread.flush_rollout().await?;
            let rollout_path = saved.thread.rollout_path().expect("saved rollout");
            saved.thread.shutdown_and_wait().await?;
            assert!(manager.remove_thread(&thread_id).await.is_some());
            drop(saved);

            // Bypass the fixed agent loader to reproduce the old runtime's bad config.
            let mut broken_config = config.clone();
            broken_config.ephemeral = true;
            let broken = manager
                .resume_thread_from_rollout(
                    broken_config,
                    rollout_path.clone(),
                    Arc::clone(&auth_manager),
                    /*parent_trace*/ None,
                    ClientMcpExtensions::default(),
                )
                .await?
                .thread;
            assert!(!broken.has_persistence());
            let live_marker = marker(LIVE_MESSAGE, "live-api-turn");
            broken
                .inject_response_items(vec![live_marker.clone()])
                .await?;
            let stored_before = store
                .load_history(LoadThreadHistoryParams {
                    thread_id,
                    include_archived: true,
                })
                .await?;
            assert!(!stored_before.items.iter().any(|item| matches!(
                item,
                RolloutItem::ResponseItem(response)
                    if response.item.id() == live_marker.id()
            )));

            let request_id = 10 + case as i64 * 10;
            if block_writer {
                let blocker = LiveThread::resume(
                    Arc::clone(&store),
                    ThreadHistoryMode::Paginated,
                    ResumeThreadParams {
                        thread_id,
                        rollout_path: Some(rollout_path),
                        history: None,
                        include_archived: true,
                        metadata: ThreadPersistenceMetadata {
                            cwd: Some(config.cwd.to_path_buf()),
                            model_provider: config.model_provider_id.clone(),
                            memory_mode: ThreadMemoryMode::Disabled,
                        },
                    },
                )
                .await?;
                for request in [
                    ClientRequest::ThreadRead {
                        request_id: RequestId::Integer(request_id),
                        params: ThreadReadParams {
                            thread_id: thread_id.to_string(),
                            include_turns: true,
                        },
                    },
                    ClientRequest::ThreadForkPrepare {
                        request_id: RequestId::Integer(request_id + 1),
                        params: ThreadForkParams {
                            thread_id: thread_id.to_string(),
                            exclude_turns: true,
                            ..Default::default()
                        },
                    },
                ] {
                    let id = request.id().clone();
                    harness
                        .processor
                        .process_request(
                            TEST_CONNECTION_ID,
                            request_from_client_request(request),
                            &AppServerTransport::Stdio,
                            Arc::clone(&harness.session),
                        )
                        .await;
                    let error = read_repair_error(&mut harness.outgoing_rx, id).await?;
                    assert_eq!(error.code, crate::error_code::INTERNAL_ERROR_CODE);
                    assert!(error.message.contains("failed to access loaded thread"));
                    assert!(
                        error
                            .message
                            .contains("failed to restore saved thread persistence")
                    );
                    assert!(!broken.has_persistence());
                }
                blocker.shutdown().await?;
            }

            let read: ThreadReadResponse = harness
                .request(
                    ClientRequest::ThreadRead {
                        request_id: RequestId::Integer(request_id + 2),
                        params: ThreadReadParams {
                            thread_id: thread_id.to_string(),
                            include_turns: true,
                        },
                    },
                    /*trace*/ None,
                )
                .await;
            assert!(!read.thread.ephemeral);
            assert!(broken.has_persistence());
            assert_live_message_once(read.thread.turns.iter().flat_map(|turn| &turn.items));

            let turns: ThreadTurnsListResponse = harness
                .request(
                    ClientRequest::ThreadTurnsList {
                        request_id: RequestId::Integer(request_id + 3),
                        params: ThreadTurnsListParams {
                            thread_id: thread_id.to_string(),
                            cursor: None,
                            limit: Some(10),
                            sort_direction: None,
                            items_view: Some(TurnItemsView::Full),
                        },
                    },
                    /*trace*/ None,
                )
                .await;
            assert_live_message_once(turns.data.iter().flat_map(|turn| &turn.items));

            let items: ThreadItemsListResponse = harness
                .request(
                    ClientRequest::ThreadItemsList {
                        request_id: RequestId::Integer(request_id + 4),
                        params: ThreadItemsListParams {
                            thread_id: thread_id.to_string(),
                            turn_id: None,
                            cursor: None,
                            limit: Some(100),
                            sort_direction: None,
                        },
                    },
                    /*trace*/ None,
                )
                .await;
            assert_live_message_once(items.data.iter().map(|entry| &entry.item));

            let resumed: ThreadResumeResponse = harness
                .request(
                    ClientRequest::ThreadResume {
                        request_id: RequestId::Integer(request_id + 5),
                        params: ThreadResumeParams {
                            thread_id: thread_id.to_string(),
                            ..Default::default()
                        },
                    },
                    /*trace*/ None,
                )
                .await;
            assert_live_message_once(resumed.thread.turns.iter().flat_map(|turn| &turn.items));
            assert!(Arc::ptr_eq(&broken, &manager.get_thread(thread_id).await?));
            broken.shutdown_and_wait().await?;
            assert!(manager.remove_thread(&thread_id).await.is_some());
        }
        harness.shutdown().await;
        Ok(())
    })
}

async fn read_repair_error(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    request_id: RequestId,
) -> Result<codex_app_server_protocol::JSONRPCErrorError> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let envelope = outgoing_rx.recv().await.expect("outgoing channel");
            let crate::outgoing_message::OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                ..
            } = envelope
            else {
                continue;
            };
            if connection_id != TEST_CONNECTION_ID {
                continue;
            }
            match message {
                crate::outgoing_message::OutgoingMessage::Error(error)
                    if error.id == request_id =>
                {
                    return error.error;
                }
                crate::outgoing_message::OutgoingMessage::Response(response)
                    if response.id == request_id =>
                {
                    panic!("repair error returned stale data: {response:?}");
                }
                _ => {}
            }
        }
    })
    .await
    .map_err(Into::into)
}
