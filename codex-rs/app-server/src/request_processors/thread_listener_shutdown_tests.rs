#![cfg(test)]

//! Checks runtime release and notification delivery across listener shutdown and reconnect.

use super::super::thread_goal_processor::ThreadGoalRequestProcessor;
use super::super::thread_processor::ThreadRequestProcessor;
use super::super::thread_processor::ThreadResumeTarget;
use super::*;
use crate::config_manager::ConfigManager;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::thread_sample::SampleResponseRegistry;
use crate::thread_state::ConnectionCapabilities;
use crate::thread_state::PendingNetworkApproval;
use crate::thread_state::resolve_server_request_on_thread_listener;
use anyhow::Context;
use anyhow::Result;
use codex_analytics::AnalyticsEventsClient;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadUnsubscribeParams;
use codex_app_server_protocol::ThreadUnsubscribeResponse;
use codex_app_server_protocol::ThreadUnsubscribeStatus;
use codex_arg0::Arg0DispatchPaths;
use codex_core::context::ContextualUserFragment;
use codex_core::context::InternalContextSource;
use codex_core::context::InternalModelContextFragment;
use codex_goal_extension::GoalService;
use codex_login::AuthManager;
use codex_protocol::user_input::UserInput;
use codex_thread_store::PersistContext;
use core_test_support::responses;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use std::collections::HashSet;
use test_case::test_case;
use tokio::sync::mpsc;

async fn test_processor(
    test: &TestCodex,
) -> Result<(ThreadRequestProcessor, mpsc::Receiver<OutgoingEnvelope>)> {
    let config = Arc::new(test.config.clone());
    let auth =
        AuthManager::shared_from_config(config.as_ref(), /*enable_codex_api_key_env*/ false)
            .await?;
    let config_manager =
        ConfigManager::without_managed_config_for_tests(config.codex_home.to_path_buf());
    let (outgoing_tx, outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let thread_state_manager = ThreadStateManager::new();
    let thread_watch_manager = ThreadWatchManager::new_with_outgoing(outgoing.clone());
    let skills_watcher = SkillsWatcher::new(
        test.thread_manager.skills_service(),
        &config.codex_home,
        outgoing.clone(),
    );
    let thread_goal_processor = ThreadGoalRequestProcessor::new(
        test.thread_manager.clone(),
        outgoing.clone(),
        config.clone(),
        thread_state_manager.clone(),
        /*state_db*/ None,
        Arc::new(GoalService::new()),
        config_manager.clone(),
    );
    let processor = ThreadRequestProcessor::new(
        auth,
        test.thread_manager.clone(),
        outgoing,
        Arg0DispatchPaths::default(),
        config,
        config_manager,
        test.thread_store.clone(),
        Default::default(),
        thread_state_manager,
        thread_watch_manager,
        Arc::new(ThreadListState::default()),
        thread_goal_processor,
        /*state_db*/ None,
        /*log_db*/ None,
        skills_watcher,
        /*turn_cost_worker*/ None,
        Vec::new(),
        Arc::new(SampleResponseRegistry::default()),
    );
    Ok((processor, outgoing_rx))
}

#[tokio::test]
async fn loaded_resume_and_read_complete_while_an_unrelated_resume_holds_list_state() -> Result<()>
{
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    test.thread_store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    let (processor, _messages) = test_processor(&test).await?;
    let connection_id = ConnectionId(1);
    processor
        .connection_initialized(connection_id, ConnectionCapabilities::default())
        .await;
    let mut held = processor
        .thread_list_state_permit
        .acquire_resume(/*shared*/ true)
        .await
        .map_err(|error| anyhow::anyhow!("permit failed: {}", error.message))?;
    held.finish_probe();
    let resume_id = ConnectionRequestId {
        connection_id,
        request_id: RequestId::Integer(1),
    };
    let resume = tokio::time::timeout(
        Duration::from_secs(10),
        processor.thread_resume(
            ThreadResumeTarget::Client(resume_id),
            ThreadResumeParams {
                thread_id: thread_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            /*app_server_client_name*/ None,
            /*app_server_client_version*/ None,
            Default::default(),
        ),
    )
    .await
    .context("loaded resume blocked by an unrelated held list-state permit")?
    .map_err(|error| anyhow::anyhow!("resume failed: {}", error.message))?;
    assert!(resume.is_none());
    let read_id = ConnectionRequestId {
        connection_id,
        request_id: RequestId::Integer(2),
    };
    let read = tokio::time::timeout(
        Duration::from_secs(10),
        processor.thread_read(
            &read_id,
            ThreadReadParams {
                thread_id: thread_id.to_string(),
                include_turns: false,
            },
        ),
    )
    .await
    .context("thread read blocked by an unrelated held list-state permit")?
    .map_err(|error| anyhow::anyhow!("read failed: {}", error.message))?;
    assert!(read.is_some());
    drop(held);
    Ok(())
}

// The cold-resume test fixtures create futures that exceed CI's 8 MiB test stack in debug builds.
fn run_cold_daemon_test<F, Fut>(test: F) -> Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>>,
{
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(test())
        })?
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

#[test]
fn cold_daemon_resume_shares_list_permit_with_an_unrelated_resume() -> Result<()> {
    run_cold_daemon_test(cold_daemon_resume_shares_list_permit_with_an_unrelated_resume_inner)
}

async fn cold_daemon_resume_shares_list_permit_with_an_unrelated_resume_inner() -> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    test.thread_store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    test.codex.shutdown_and_wait().await?;
    test.thread_manager.remove_thread(&thread_id).await;
    let (processor, _messages) = test_processor(&test).await?;
    let mut other = processor
        .thread_list_state_permit
        .acquire_resume(/*shared*/ true)
        .await
        .map_err(|error| anyhow::anyhow!("permit failed: {}", error.message))?;
    other.finish_probe();
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        Box::pin(processor.thread_resume(
            ThreadResumeTarget::DaemonRecovery(None),
            ThreadResumeParams {
                thread_id: thread_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            None,
            None,
            Default::default(),
        )),
    )
    .await;
    drop(other);
    result
        .context("cold daemon root waited for an unrelated shared list permit")?
        .map_err(|error| anyhow::anyhow!("resume failed: {}", error.message))?;
    assert!(test.thread_manager.get_thread(thread_id).await.is_ok());
    Ok(())
}

#[test]
fn cold_daemon_resume_waits_behind_a_queued_list_writer() -> Result<()> {
    run_cold_daemon_test(cold_daemon_resume_waits_behind_a_queued_list_writer_inner)
}

async fn cold_daemon_resume_waits_behind_a_queued_list_writer_inner() -> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    test.thread_store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    test.codex.shutdown_and_wait().await?;
    test.thread_manager.remove_thread(&thread_id).await;
    let (processor, _messages) = test_processor(&test).await?;
    let mut other = processor
        .thread_list_state_permit
        .acquire_resume(/*shared*/ true)
        .await
        .map_err(|error| anyhow::anyhow!("permit failed: {}", error.message))?;
    other.finish_probe();
    processor
        .thread_list_state_permit
        .wait_for_resume_admission_attempt()
        .await;
    processor
        .thread_list_state_permit
        .wait_for_resume_list_permit_acquired()
        .await;
    let writer = processor.thread_list_state_permit.acquire_exclusive();
    tokio::pin!(writer);
    assert!(futures::poll!(&mut writer).is_pending());
    let resume_processor = processor.clone();
    let resume = tokio::spawn(async move {
        Box::pin(resume_processor.thread_resume(
            ThreadResumeTarget::DaemonRecovery(None),
            ThreadResumeParams {
                thread_id: thread_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            None,
            None,
            Default::default(),
        ))
        .await
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        processor
            .thread_list_state_permit
            .wait_for_resume_admission_attempt(),
    )
    .await
    .context("daemon did not reach shared list admission")?;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            processor
                .thread_list_state_permit
                .wait_for_resume_list_permit_acquired(),
        )
        .await
        .is_err(),
        "daemon acquired a shared list permit ahead of the queued writer"
    );
    drop(other);
    let writer = tokio::time::timeout(Duration::from_secs(10), &mut writer)
        .await
        .context("queued writer did not acquire after the earlier resume")?
        .map_err(|error| anyhow::anyhow!("writer permit failed: {}", error.message))?;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            processor
                .thread_list_state_permit
                .wait_for_resume_list_permit_acquired(),
        )
        .await
        .is_err(),
        "daemon acquired a shared list permit while the writer held it"
    );
    drop(writer);
    tokio::time::timeout(
        Duration::from_secs(15),
        processor
            .thread_list_state_permit
            .wait_for_resume_list_permit_acquired(),
    )
    .await
    .context("daemon did not acquire a shared list permit after the writer")?;
    tokio::time::timeout(Duration::from_secs(15), resume)
        .await
        .context("cold daemon resume did not proceed after the writer")?
        .context("daemon resume task failed")?
        .map_err(|error| anyhow::anyhow!("resume failed: {}", error.message))?;
    Ok(())
}

#[test]
fn cold_daemon_same_id_waiter_can_be_cancelled_without_blocking_another_root() -> Result<()> {
    run_cold_daemon_test(
        cold_daemon_same_id_waiter_can_be_cancelled_without_blocking_another_root_inner,
    )
}

async fn cold_daemon_same_id_waiter_can_be_cancelled_without_blocking_another_root_inner()
-> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let first_id = test.session_configured.thread_id;
    let second = Box::pin(
        test.thread_manager
            .start_thread(test.start_thread_options().await),
    )
    .await?;
    let second_id = second.thread_id;
    for (thread_id, thread) in [(first_id, &test.codex), (second_id, &second.thread)] {
        test.thread_store
            .persist_thread(thread_id, PersistContext::Standard)
            .await?;
        thread.shutdown_and_wait().await?;
        test.thread_manager.remove_thread(&thread_id).await;
    }
    let (processor, _messages) = test_processor(&test).await?;
    let same_id_reservation = processor.thread_state_manager.lock_resume(first_id).await;
    assert_eq!(
        processor
            .thread_state_manager
            .resume_reservation_strong_count(first_id)
            .await,
        1
    );
    let waiting_processor = processor.clone();
    let waiting = tokio::spawn(async move {
        Box::pin(waiting_processor.thread_resume(
            ThreadResumeTarget::DaemonRecovery(None),
            ThreadResumeParams {
                thread_id: first_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            None,
            None,
            Default::default(),
        ))
        .await
    });
    tokio::time::timeout(Duration::from_secs(15), async {
        while processor
            .thread_state_manager
            .resume_reservation_strong_count(first_id)
            .await
            < 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("same-ID daemon did not reach the held reservation")?;
    let other = tokio::time::timeout(
        Duration::from_secs(15),
        Box::pin(processor.thread_resume(
            ThreadResumeTarget::DaemonRecovery(None),
            ThreadResumeParams {
                thread_id: second_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            None,
            None,
            Default::default(),
        )),
    )
    .await
    .context("a same-ID waiter blocked an unrelated daemon root")?
    .map_err(|error| anyhow::anyhow!("unrelated resume failed: {}", error.message))?;
    assert!(other.is_none());
    assert!(
        !waiting.is_finished(),
        "same-ID resume bypassed its reservation"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(
        processor
            .thread_state_manager
            .resume_reservation_strong_count(first_id)
            .await,
        1,
        "cancelled waiter retained the reservation"
    );
    drop(same_id_reservation);
    tokio::time::timeout(
        Duration::from_secs(15),
        Box::pin(processor.thread_resume(
            ThreadResumeTarget::DaemonRecovery(None),
            ThreadResumeParams {
                thread_id: first_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            None,
            None,
            Default::default(),
        )),
    )
    .await
    .context("cancelled same-ID waiter left its reservation held")?
    .map_err(|error| anyhow::anyhow!("retry failed: {}", error.message))?;
    Ok(())
}

#[tokio::test]
async fn loaded_daemon_resume_keeps_exclusive_list_ordering() -> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    test.thread_store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    let (processor, _messages) = test_processor(&test).await?;
    let mut other = processor
        .thread_list_state_permit
        .acquire_resume(/*shared*/ true)
        .await
        .map_err(|error| anyhow::anyhow!("permit failed: {}", error.message))?;
    other.finish_probe();
    let resume = processor.thread_resume(
        ThreadResumeTarget::DaemonRecovery(None),
        ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        },
        None,
        None,
        Default::default(),
    );
    tokio::pin!(resume);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut resume)
            .await
            .is_err(),
        "loaded daemon recovery must retain exclusive list ordering"
    );
    drop(other);
    tokio::time::timeout(Duration::from_secs(15), &mut resume)
        .await
        .context("loaded daemon recovery did not proceed after the other permit released")?
        .map_err(|error| anyhow::anyhow!("resume failed: {}", error.message))?;
    Ok(())
}

#[tokio::test]
async fn daemon_resume_without_loaded_history_source_upgrades_before_startup() -> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    test.thread_store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    test.codex.shutdown_and_wait().await?;
    test.thread_manager.remove_thread(&thread_id).await;
    let source = test
        .thread_store
        .read_thread(codex_thread_store::ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: false,
        })
        .await?;
    assert!(source.attached_to_thread_id.is_none());
    assert!(source.parent_thread_id.is_none());
    assert!(!source.source.is_non_root_agent());
    assert!(!matches!(
        source.source,
        codex_protocol::protocol::SessionSource::Unknown
    ));

    let (processor, _messages) = test_processor(&test).await?;
    processor
        .missing_resume_history_sources
        .lock()
        .unwrap()
        .insert(thread_id);
    let mut other = processor
        .thread_list_state_permit
        .acquire_resume(/*shared*/ true)
        .await
        .map_err(|error| anyhow::anyhow!("permit failed: {}", error.message))?;
    other.finish_probe();
    let resume_processor = processor.clone();
    let request = tokio::spawn(async move {
        Box::pin(resume_processor.thread_resume(
            ThreadResumeTarget::DaemonRecovery(None),
            ThreadResumeParams {
                thread_id: thread_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            None,
            None,
            Default::default(),
        ))
        .await
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        processor.missing_resume_history_applied.notified(),
    )
    .await
    .context("daemon did not reach the real history loader")?;
    tokio::time::timeout(
        Duration::from_secs(15),
        processor
            .thread_list_state_permit
            .wait_for_resume_upgrade_attempt(),
    )
    .await
    .context("missing history source did not trigger an exclusive upgrade")?;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            processor
                .thread_list_state_permit
                .wait_for_resume_upgrade_acquired(),
        )
        .await
        .is_err(),
        "daemon acquired exclusive admission while another resume held it"
    );
    drop(other);
    tokio::time::timeout(
        Duration::from_secs(15),
        processor
            .thread_list_state_permit
            .wait_for_resume_upgrade_acquired(),
    )
    .await
    .context("daemon did not acquire exclusive admission after the other resume")?;
    request.abort();
    let _ = request.await;
    Ok(())
}

#[tokio::test]
async fn late_command_cancellation_reaches_the_current_subscriber() -> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    let (processor, mut messages) = test_processor(&test).await?;
    let thread_state_manager = &processor.thread_state_manager;
    let original = ConnectionId(1);
    let resumed = ConnectionId(2);
    for client in [original, resumed] {
        processor
            .connection_initialized(client, ConnectionCapabilities::default())
            .await;
    }
    processor
        .try_attach_thread_listener(thread_id, test.codex.clone(), vec![original])
        .await;
    let outgoing = ThreadScopedOutgoingMessageSender::new(
        processor.outgoing.clone(),
        vec![original],
        thread_id,
    );
    let approval = serde_json::from_value(serde_json::json!({
        "turn_id": "finished-turn",
        "call_id": "late-command",
        "started_at_ms": 42,
        "command": ["echo", "late"],
        "parsed_cmd": [],
        "cwd": test.config.cwd,
    }))?;
    apply_bespoke_event_handling(
        codex_protocol::protocol::Event {
            id: "finished-turn".into(),
            msg: EventMsg::ExecApprovalRequest(approval),
        },
        thread_id,
        test.codex.clone(),
        test.thread_manager.clone(),
        outgoing.clone(),
        thread_state_manager.thread_state(thread_id).await,
        processor.thread_watch_manager.clone(),
    )
    .await;

    assert!(
        thread_state_manager
            .try_add_connection_to_thread(thread_id, resumed)
            .await
    );
    assert!(
        thread_state_manager
            .unsubscribe_connection_from_thread(thread_id, original)
            .await
    );
    outgoing.abort_pending_server_requests().await;

    let recipient = tokio::time::timeout(Duration::from_secs(/*secs*/ 10), async {
        while let Some(message) = messages.recv().await {
            if let OutgoingEnvelope::ToConnection {
                connection_id,
                message: OutgoingMessage::AppServerNotification(notification),
                ..
            } = message
                && let ServerNotification::ItemCompleted(event) = notification.notification
                && event.item.id() == "late-command"
            {
                return Ok(connection_id);
            }
        }
        anyhow::bail!("missing late command completion")
    })
    .await??;
    assert_eq!(recipient, resumed);
    Ok(())
}

#[tokio::test]
async fn only_canonical_id_resumes_share_list_state() -> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    test.thread_store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    let (processor, mut outgoing) = test_processor(&test).await?;
    let connection_id = ConnectionId(1);
    processor
        .connection_initialized(connection_id, ConnectionCapabilities::default())
        .await;
    let held = processor
        .thread_list_state_permit
        .acquire_resume(/*shared*/ true)
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let resume_id = ConnectionRequestId {
        connection_id,
        request_id: RequestId::Integer(1),
    };
    let resume = tokio::time::timeout(
        Duration::from_secs(10),
        processor.thread_resume(
            ThreadResumeTarget::Client(resume_id),
            ThreadResumeParams {
                thread_id: thread_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            },
            /*app_server_client_name*/ None,
            /*app_server_client_version*/ None,
            /*client_mcp_extensions*/ Default::default(),
        ),
    )
    .await
    .context("loaded resume blocked by an unrelated held list-state permit")?
    .map_err(|error| anyhow::anyhow!("resume failed: {}", error.message))?;
    assert!(resume.is_none());
    assert!(
        std::iter::from_fn(|| outgoing.try_recv().ok()).any(|envelope| matches!(
            envelope,
            OutgoingEnvelope::ToConnection {
                message: OutgoingMessage::Response(response),
                ..
            } if response.id == RequestId::Integer(1)
        ))
    );
    // The alias uses a different request queue, so it must wait for shared resumes to finish.
    let alias_resume = processor.thread_resume(
        ThreadResumeTarget::Client(ConnectionRequestId {
            connection_id,
            request_id: RequestId::Integer(3),
        }),
        ThreadResumeParams {
            thread_id: thread_id.to_string().replace('-', ""),
            exclude_turns: true,
            ..Default::default()
        },
        /*app_server_client_name*/ None,
        /*app_server_client_version*/ None,
        /*client_mcp_extensions*/ Default::default(),
    );
    tokio::pin!(alias_resume);
    assert!(futures::poll!(&mut alias_resume).is_pending());
    drop(held);
    let alias_response = tokio::time::timeout(Duration::from_secs(10), alias_resume)
        .await
        .context("aliased resume did not finish after shared resumes")?
        .map_err(|error| anyhow::anyhow!("aliased resume failed: {}", error.message))?;
    assert!(alias_response.is_none());
    assert!(
        std::iter::from_fn(|| outgoing.try_recv().ok()).any(|envelope| matches!(
            envelope,
            OutgoingEnvelope::ToConnection { message: OutgoingMessage::Response(response), .. }
                if response.id == RequestId::Integer(3)
        ))
    );

    // A path may not exist until an earlier exclusive restore completes. Do not
    // inspect it or return a path error ahead of that operation.
    let writer = processor
        .acquire_thread_list_state_permit()
        .await
        .expect("writer permit");
    let path_resume = processor.thread_resume(
        ThreadResumeTarget::Client(ConnectionRequestId {
            connection_id,
            request_id: RequestId::Integer(2),
        }),
        ThreadResumeParams {
            thread_id: thread_id.to_string(),
            path: Some(
                test.config
                    .codex_home
                    .join("missing-rollout.jsonl")
                    .to_path_buf(),
            ),
            exclude_turns: true,
            ..Default::default()
        },
        /*app_server_client_name*/ None,
        /*app_server_client_version*/ None,
        /*client_mcp_extensions*/ Default::default(),
    );
    tokio::pin!(path_resume);
    assert!(futures::poll!(&mut path_resume).is_pending());
    drop(writer);
    let error = path_resume.await.expect_err("missing running-thread path");
    assert!(error.message.contains("stale path"));
    Ok(())
}

#[test_case(true; "listener attached before shutdown")]
#[test_case(false; "listener attached after shutdown")]
#[tokio::test]
async fn listener_shutdown_releases_runtime_and_preserves_other_subscribers(
    attach_before_shutdown: bool,
) -> Result<()> {
    let server = wiremock::MockServer::start().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    test.thread_store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    let (processor, mut outgoing_rx) = test_processor(&test).await?;
    let thread_state_manager = &processor.thread_state_manager;
    let thread_watch_manager = &processor.thread_watch_manager;
    let thread = test.codex;
    let runtime = Arc::downgrade(&thread);
    let connection_id = ConnectionId(1);
    let disconnecting_connection_id = ConnectionId(2);
    let remaining_connection_id = ConnectionId(3);
    let connection_ids = vec![
        connection_id,
        disconnecting_connection_id,
        remaining_connection_id,
    ];
    for &connection_id in &connection_ids {
        processor
            .connection_initialized(connection_id, ConnectionCapabilities::default())
            .await;
    }
    let network_request_id = RequestId::Integer(4);
    if attach_before_shutdown {
        processor
            .try_attach_thread_listener(thread_id, Arc::clone(&thread), connection_ids.clone())
            .await;
        // Leave the prompt outstanding so shutdown runs before the proxy waiter resolves it.
        let permission_guard = thread_watch_manager
            .note_permission_requested(&thread_id.to_string())
            .await;
        thread_state_manager
            .thread_state(thread_id)
            .await
            .lock()
            .await
            .network_approvals
            .insert(
                "network-approval".to_string(),
                PendingNetworkApproval {
                    request_id: network_request_id.clone(),
                    permission_guard,
                },
            );
    }
    thread.shutdown_and_wait().await?;
    let shutdown_result = tokio::time::timeout(Duration::from_secs(/*secs*/ 10), async {
        let resume = processor
            .thread_resume(
                ThreadResumeTarget::Client(ConnectionRequestId {
                    connection_id,
                    request_id: RequestId::Integer(1),
                }),
                ThreadResumeParams {
                    thread_id: thread_id.to_string(),
                    exclude_turns: true,
                    ..Default::default()
                },
                /*app_server_client_name*/ None,
                /*app_server_client_version*/ None,
                /*client_mcp_extensions*/ Default::default(),
            )
            .await;
        test.thread_manager.remove_thread(&thread_id).await;
        if !attach_before_shutdown {
            processor
                .try_attach_thread_listener(thread_id, Arc::clone(&thread), connection_ids)
                .await;
        }
        let mut network_resolved = false;
        loop {
            match outgoing_rx.recv().await.expect("outgoing channel closed") {
                OutgoingEnvelope::ToConnection {
                    connection_id: recipient,
                    message: OutgoingMessage::AppServerNotification(notification),
                    ..
                } if recipient == connection_id
                    && matches!(
                        &notification.notification,
                        ServerNotification::ServerRequestResolved(resolved)
                            if resolved.request_id == network_request_id
                    ) =>
                {
                    assert!(!network_resolved, "duplicate network prompt resolution");
                    network_resolved = true;
                }
                OutgoingEnvelope::Broadcast {
                    message: OutgoingMessage::AppServerNotification(notification),
                } if matches!(
                    &notification.notification,
                    ServerNotification::ThreadStatusChanged(status)
                        if status.status == ThreadStatus::NotLoaded
                ) =>
                {
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(network_resolved, attach_before_shutdown);
        drop(thread);
        while runtime.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
        let Some(ClientResponsePayload::ThreadUnsubscribe(unsubscribe)) = processor
            .thread_unsubscribe(
                &ConnectionRequestId {
                    connection_id,
                    request_id: RequestId::Integer(2),
                },
                ThreadUnsubscribeParams {
                    thread_id: thread_id.to_string(),
                },
            )
            .await
            .expect("unsubscribe failed")
        else {
            panic!("unsubscribe response missing");
        };
        assert_eq!(
            unsubscribe,
            ThreadUnsubscribeResponse {
                status: ThreadUnsubscribeStatus::NotLoaded,
            },
        );
        assert_eq!(
            thread_state_manager
                .subscribed_connection_ids(thread_id)
                .await
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::from([disconnecting_connection_id, remaining_connection_id]),
        );
        processor
            .connection_closed(disconnecting_connection_id)
            .await;
        assert_eq!(
            thread_state_manager
                .subscribed_connection_ids(thread_id)
                .await,
            vec![remaining_connection_id],
        );
        resolve_server_request_on_thread_listener(
            &thread_state_manager.thread_state(thread_id).await,
            RequestId::Integer(3),
        )
        .await;
        loop {
            if let OutgoingEnvelope::ToConnection {
                connection_id: recipient,
                message: OutgoingMessage::AppServerNotification(notification),
                ..
            } = outgoing_rx.recv().await.expect("outgoing channel closed")
                && recipient == remaining_connection_id
                && matches!(
                    &notification.notification,
                    ServerNotification::ServerRequestResolved(resolved)
                        if resolved.request_id == RequestId::Integer(3)
                )
            {
                break;
            }
        }
        resume
    })
    .await;
    thread_state_manager.clear_all_listeners().await;
    processor.skills_watcher.shutdown();

    let resume = shutdown_result.context("terminal listener did not release or relay")?;
    assert_eq!(
        resume.expect_err("resume accepted a terminated runtime"),
        invalid_request(format!(
            "thread {thread_id} is closing; retry thread/resume after the thread is closed"
        )),
    );
    Ok(())
}

#[tokio::test]
async fn resume_preserves_recovery_turn_before_status_publication() -> Result<()> {
    const TIMEOUT: Duration = Duration::from_secs(/*secs*/ 10);
    let server = wiremock::MockServer::start().await;
    let original_request = responses::mount_response_once(
        &server,
        responses::sse_response(responses::sse_completed("original"))
            .set_delay(Duration::from_secs(/*secs*/ 60)),
    )
    .await;
    let test = test_codex()
        .with_history_mode(codex_protocol::protocol::ThreadHistoryMode::Paginated)
        .build_with_auto_env(&server)
        .await?;
    let thread = &test.codex;
    let thread_id = test.session_configured.thread_id;
    let TurnInputSubmission::Started {
        turn_id: original_id,
        ..
    } = thread
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "Finish the work".into(),
            text_elements: Vec::new(),
        }]))
        .await?
    else {
        anyhow::bail!("original turn did not start");
    };
    tokio::time::timeout(TIMEOUT, async {
        while original_request.last_request().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    thread.submit(Op::Interrupt).await?;
    wait_for_event(thread, |event| matches!(event, EventMsg::TurnAborted(_))).await;
    let (processor, mut outgoing) = test_processor(&test).await?;
    let connection_id = ConnectionId(1);
    processor
        .connection_initialized(connection_id, ConnectionCapabilities::default())
        .await;
    processor
        .thread_watch_manager
        .upsert_thread(&thread_id.to_string())
        .await;
    let recovery_request =
        responses::mount_sse_once(&server, responses::sse_completed("recovered")).await;

    // TurnStarted is durable before it is published. Hold the paginated history writer
    // so reconnect observes the registered recovery task while both statuses are idle.
    let history = codex_state::open_thread_history_db(test.config.sqlite_config()).await?;
    let writer = history.begin_with("BEGIN IMMEDIATE").await?;
    let input = ContextualUserFragment::into(InternalModelContextFragment::new(
        InternalContextSource::from_static("daemon_recovery"),
        "Continue the interrupted work.",
    ));
    let TurnInputSubmission::Started {
        turn_id: recovery_id,
        ..
    } = thread
        .continue_turn_if_idle(
            TurnInputRequest::new(TurnInput::ResponseItem(input)).on_start(TurnStartOptions {
                turn_trigger: Some("daemon_recovery".into()),
                ..Default::default()
            }),
            original_id.clone(),
        )
        .await?
    else {
        anyhow::bail!("recovery turn did not start");
    };
    assert_ne!(recovery_id, original_id);
    assert!(!matches!(thread.agent_status().await, AgentStatus::Running));
    assert_eq!(
        processor
            .thread_watch_manager
            .loaded_status_for_thread(&thread_id.to_string())
            .await,
        ThreadStatus::Idle
    );
    let resume = processor.thread_resume(
        ThreadResumeTarget::Client(ConnectionRequestId {
            connection_id,
            request_id: RequestId::Integer(1),
        }),
        ThreadResumeParams {
            thread_id: thread_id.to_string(),
            config: Some([("model_reasoning_effort".into(), serde_json::json!("medium"))].into()),
            exclude_turns: true,
            ..Default::default()
        },
        Some("codex-tui".into()),
        /*app_server_client_version*/ None,
        Default::default(),
    );
    let (resumed, attached) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(resume, async {
            let attached = tokio::select! {
                () = processor.thread_state_manager.wait_for_thread_subscriber(thread_id) => true,
                () = async {
                    while thread.active_turn_root(&recovery_id).await.is_some() {
                        tokio::task::yield_now().await;
                    }
                } => false,
            };
            writer.rollback().await?;
            Ok::<_, sqlx::Error>(attached)
        })
    })
    .await?;
    assert!(attached?, "reconnect shut down recovery turn {recovery_id}");
    resumed.map_err(|err| anyhow::anyhow!(err.message))?;
    let completed = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let OutgoingEnvelope::ToConnection {
                connection_id: recipient,
                message: OutgoingMessage::AppServerNotification(notification),
                ..
            } = outgoing
                .recv()
                .await
                .expect("reconnecting client channel closed")
                && recipient == connection_id
                && let ServerNotification::TurnCompleted(completed) = notification.notification
            {
                return completed;
            }
        }
    })
    .await?;
    assert_eq!(completed.turn.id, recovery_id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    assert!(
        recovery_request
            .single_request()
            .has_content_kinds(&["daemon_recovery.internal_context"])
    );
    processor.thread_state_manager.clear_all_listeners().await;
    processor.skills_watcher.shutdown();
    Ok(())
}

#[tokio::test]
async fn revert_settles_interrupts_before_replacing_a_runtime() -> Result<()> {
    use super::super::turn_processor::TurnRequestProcessor;
    use crate::outgoing_message::OutgoingError;
    use codex_app_server_protocol::ThreadRevertParams;
    use codex_app_server_protocol::TurnInterruptParams;
    use codex_core::TurnInputRequest;
    use codex_core::TurnInputSubmission;
    use codex_extension_api::ExtensionFuture;
    use codex_extension_api::ExtensionRegistryBuilder;
    use codex_extension_api::TurnLifecycleContributor;
    use codex_extension_api::TurnStopInput;
    use codex_protocol::protocol::MultiAgentVersion;
    use codex_protocol::protocol::ThreadHistoryMode;
    use tokio::sync::Notify;

    #[derive(Default)]
    struct StopGate {
        entered: Notify,
        release: Notify,
    }

    impl TurnLifecycleContributor for StopGate {
        fn on_turn_stop<'a>(&'a self, _: TurnStopInput<'a>) -> ExtensionFuture<'a, ()> {
            Box::pin(async {
                self.entered.notify_one();
                self.release.notified().await;
            })
        }
    }

    let gate = Arc::new(StopGate::default());
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_lifecycle_contributor(gate.clone());
    let server = responses::start_mock_server().await;
    responses::mount_sse_once(&server, responses::sse_completed("first")).await;
    let test = test_codex()
        .with_extensions(Arc::new(extensions.build()))
        .with_history_mode(ThreadHistoryMode::Paginated)
        .with_config(|config| {
            config.agents_enabled = false;
            config
                .features
                .disable(codex_features::Feature::MultiAgentV2)
                .unwrap();
        })
        .build_with_auto_env(&server)
        .await?;
    let thread_id = test.session_configured.thread_id;
    let (processor, mut outgoing_rx) = test_processor(&test).await?;
    // Keep transport flowing, retaining every reply and turn boundary in client-visible order.
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let collector = tokio::spawn(async move {
        while let Some(envelope) = outgoing_rx.recv().await {
            let OutgoingEnvelope::ToConnection { message, .. } = envelope else {
                continue;
            };
            let relevant = match &message {
                OutgoingMessage::Response(_) | OutgoingMessage::Error(_) => true,
                OutgoingMessage::AppServerNotification(envelope) => matches!(
                    envelope.notification,
                    ServerNotification::TurnStarted(_) | ServerNotification::TurnCompleted(_)
                ),
                OutgoingMessage::Request(_) => false,
            };
            if relevant && observed_tx.send(message).is_err() {
                break;
            }
        }
    });
    let connection_id = ConnectionId(1);
    processor
        .connection_initialized(connection_id, ConnectionCapabilities::default())
        .await;
    processor
        .try_attach_thread_listener(thread_id, test.codex.clone(), vec![connection_id])
        .await;
    let turn_processor = TurnRequestProcessor::new(
        test.thread_manager.clone(),
        processor.outgoing.clone(),
        AnalyticsEventsClient::disabled(),
        processor.config.clone(),
        processor.config_manager.clone(),
        processor.pending_thread_unloads.clone(),
        processor.thread_state_manager.clone(),
        processor.thread_watch_manager.clone(),
        processor.skills_watcher.clone(),
        /*turn_cost_worker*/ None,
    );
    let request_id = |id| ConnectionRequestId {
        connection_id,
        request_id: RequestId::Integer(id),
    };
    let interrupt = async |id, turn_id: &str| {
        turn_processor
            .turn_interrupt(
                &request_id(id),
                TurnInterruptParams {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_owned(),
                },
            )
            .await
    };
    let start = async |thread: &Arc<CodexThread>, text: &str| -> Result<String> {
        let TurnInputSubmission::Started { turn_id, .. } = thread
            .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
                text: text.to_owned(),
                text_elements: vec![],
            }]))
            .await?
        else {
            anyhow::bail!("expected a new turn")
        };
        Ok(turn_id)
    };
    let first = start(&test.codex, "first").await?;
    assert_eq!(
        test.codex.multi_agent_version(),
        Some(MultiAgentVersion::Disabled)
    );
    gate.entered.notified().await;
    assert!(matches!(observed_rx.recv().await,
        Some(OutgoingMessage::AppServerNotification(n))
            if matches!(&n.notification, ServerNotification::TurnStarted(started) if started.turn.id == first)
    ));
    assert!(
        interrupt(/*id*/ 99, &first)
            .await
            .expect("interrupt accepted")
            .is_none()
    );
    let state = processor.thread_state_manager.thread_state(thread_id).await;
    assert_eq!(state.lock().await.pending_interrupts.len(), 1);
    let mut drain = state
        .lock()
        .await
        .shutdown_drain
        .clone()
        .expect("listener installed");

    let revert = processor.thread_revert(
        request_id(/*id*/ 100),
        ThreadRevertParams {
            thread_id: thread_id.to_string(),
            before_turn_id: first.clone(),
        },
        /*app_server_client_name*/ None,
        /*app_server_client_version*/ None,
    );
    tokio::pin!(revert);
    // Stop polling revert at its shutdown barrier so late requests still load the retired runtime.
    tokio::select! {
        biased;
        drained = drain.wait_for(|drained| *drained) => { drained?; }
        result = &mut revert => { anyhow::bail!("revert passed the shutdown barrier: {result:?}"); }
    }
    assert!(Arc::ptr_eq(
        &test.codex,
        &test.thread_manager.get_thread(thread_id).await?
    ));
    let Some(OutgoingMessage::Error(error)) = observed_rx.recv().await else {
        anyhow::bail!("expected the pending interrupt's shutdown error");
    };
    assert_eq!(
        error,
        OutgoingError {
            id: request_id(/*id*/ 99).request_id,
            error: internal_error("thread shut down before turn interruption completed"),
        }
    );
    assert!(state.lock().await.pending_interrupts.is_empty());
    assert_eq!(
        interrupt(/*id*/ 101, &first).await.unwrap_err(),
        invalid_request("thread runtime is no longer accepting interrupts")
    );

    gate.release.notify_one();
    wait_for_event(
        &test.codex,
        |msg| matches!(msg, EventMsg::TurnComplete(done) if done.turn_id == first),
    )
    .await;
    // Late finalization changes Core's status back from Shutdown; the listener must stay closed.
    assert!(!test.codex.has_shutdown_status());
    assert_eq!(
        interrupt(/*id*/ 102, &first).await.unwrap_err(),
        invalid_request("thread runtime is no longer accepting interrupts")
    );
    revert.await.expect("revert replaces the retired runtime");
    assert!(matches!(observed_rx.recv().await,
        Some(OutgoingMessage::Response(response)) if response.id == request_id(/*id*/ 100).request_id
    ));
    let replacement = test.thread_manager.get_thread(thread_id).await?;
    assert!(!Arc::ptr_eq(&test.codex, &replacement));

    responses::mount_sse_once(&server, responses::sse_completed("second")).await;
    let second = start(&replacement, "second").await?;
    gate.entered.notified().await;
    assert!(matches!(observed_rx.recv().await,
        Some(OutgoingMessage::AppServerNotification(n))
            if matches!(&n.notification, ServerNotification::TurnStarted(started) if started.turn.id == second)
    ));
    assert!(
        interrupt(/*id*/ 103, &second)
            .await
            .expect("replacement accepts interrupts")
            .is_none()
    );
    gate.release.notify_one();
    assert!(matches!(observed_rx.recv().await,
        Some(OutgoingMessage::Response(response)) if response.id == request_id(/*id*/ 103).request_id
    ));
    assert!(matches!(observed_rx.recv().await,
        Some(OutgoingMessage::AppServerNotification(n))
            if matches!(&n.notification, ServerNotification::TurnCompleted(done) if done.turn.id == second)
    ));
    assert!(state.lock().await.pending_interrupts.is_empty());
    replacement.shutdown_and_wait().await?;
    processor.thread_state_manager.clear_all_listeners().await;
    processor.skills_watcher.shutdown();
    collector.abort();
    Ok(())
}
