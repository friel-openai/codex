//! Accepted mail pins a worker through dispatch; cancelled eviction still owns teardown.

use super::*;
use crate::suite::settings_commits::COMMITTED_MODEL;
use crate::suite::settings_commits::PauseAfterCommit;
use codex_core::TurnInputRequest;
use codex_core::config::Config;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadStopInput;
use codex_extension_api::ToolLifecycleContributor;
use codex_extension_api::ToolLifecycleFuture;
use codex_extension_api::ToolStartInput;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::ThreadIdle;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[derive(Default)]
struct ShutdownGate {
    entered: Notify,
    release: Notify,
}

#[derive(Default)]
struct PauseShutdown {
    delivery_started: Notify,
}

impl ToolLifecycleContributor for PauseShutdown {
    fn on_tool_start<'a>(&'a self, input: ToolStartInput<'a>) -> ToolLifecycleFuture<'a> {
        Box::pin(async move {
            if input.call_id == "late-mail" {
                self.delivery_started.notify_one();
            }
        })
    }
}

impl ThreadLifecycleContributor<Config> for PauseShutdown {
    fn on_thread_stop<'a>(&'a self, input: ThreadStopInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if let Some(gate) = input.thread_store.get::<Arc<ShutdownGate>>() {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_mail_and_cancelled_eviction_keep_worker_ownership() -> Result<()> {
    const QUEUED_TASK: &str = "handle this accepted task after dispatch resumes";
    const LATE_MAIL: &str = "must not disappear behind shutdown";
    const DRAIN_TASK: &str = "process the message queued after eviction";
    let server = start_mock_server().await;
    mount_root_collaboration_call(
        &server,
        FIRST_PROMPT,
        "first-call",
        "spawn_agent",
        json!({ "message": FIRST_TASK, "task_name": "first", "fork_turns": "none" }),
    )
    .await;
    mount_completed_worker(&server, FIRST_TASK, "first-call").await;
    let delivery = mount_root_collaboration_call(
        &server,
        "queue a followup",
        "queued-mail",
        "followup_task",
        json!({ "target": "first", "message": QUEUED_TASK }),
    )
    .await;
    let worker = mount_completed_worker(&server, QUEUED_TASK, "queued-mail").await;
    let queued_message_count = || {
        worker
            .requests()
            .iter()
            .flat_map(|request| request.inputs_of_type("agent_message"))
            .filter(|item| item.to_string().contains(QUEUED_TASK))
            .count()
    };
    let blocked_spawn = mount_root_collaboration_call(
        &server,
        "reclaim the queued worker",
        "blocked-spawn",
        "spawn_agent",
        json!({ "message": SECOND_TASK, "task_name": "replacement", "fork_turns": "none" }),
    )
    .await;

    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    extensions.config_contributor(Arc::new(PauseAfterCommit {
        gate: Mutex::new(Some((entered_tx, release_rx))),
    }));
    extensions.thread_lifecycle_contributor(Arc::new(ThreadIdle));
    let lifecycle = Arc::new(PauseShutdown::default());
    extensions.thread_lifecycle_contributor(lifecycle.clone());
    extensions.tool_lifecycle_contributor(lifecycle.clone());
    let test = test_codex()
        .with_model("gpt-5.6-sol")
        .with_extensions(Arc::new(extensions.build()))
        .with_config(|config| {
            config.features.enable(Feature::Collab).unwrap();
            config.features.enable(Feature::MultiAgentV2).unwrap();
            config.multi_agent_v2.max_concurrent_threads_per_session = 2;
        })
        .build_with_auto_env(&server)
        .await
        .context("build the eviction fixture")?;
    let mut created = test.thread_manager.subscribe_thread_created();
    test.submit_turn(FIRST_PROMPT)
        .await
        .context("spawn the first worker")?;
    ThreadIdle::wait(&test.codex).await;
    let first_id = created
        .recv()
        .await
        .context("observe the first worker creation")?;
    let first = test
        .thread_manager
        .get_thread(first_id)
        .await
        .context("load the first worker")?;
    wait_for_event(&first, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    ThreadIdle::wait(&first).await;

    // Reuse the settings fixture to pause the submission loop without starting a turn.
    first
        .submit(Op::ThreadSettings {
            thread_settings: ThreadSettingsOverrides {
                model: Some(COMMITTED_MODEL.to_string()),
                ..Default::default()
            },
            reply: None,
        })
        .await
        .context("submit the settings update that pauses dispatch")?;
    timeout(Duration::from_secs(10), entered_rx)
        .await
        .context("wait for the settings-commit pause before queueing mail")?
        .context("settings-commit pause dropped its entry notification")?;
    test.submit_turn("queue a followup")
        .await
        .context("queue accepted mail while worker dispatch is paused")?;
    ThreadIdle::wait(&test.codex).await;
    assert_eq!(
        delivery.function_call_output_text("queued-mail"),
        Some(String::new()),
    );
    assert_eq!(queued_message_count(), 0);

    // The sender has finished, but accepted mail still owns its place in the queue.
    timeout(
        Duration::from_secs(10),
        test.submit_turn("reclaim the queued worker"),
    )
    .await
    .context("queued mail must prevent eviction without waiting for dispatch")?
    .context("complete the capacity-denied spawn while mail is queued")?;
    ThreadIdle::wait(&test.codex).await;
    assert_eq!(
        blocked_spawn.function_call_output_text("blocked-spawn"),
        Some("collab spawn failed: agent thread limit reached".to_string()),
    );
    assert!(test.thread_manager.get_thread(first_id).await.is_ok());

    release_tx
        .send(())
        .context("release the settings-commit pause")?;
    wait_for_event(&first, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    ThreadIdle::wait(&first).await;
    assert_eq!(queued_message_count(), 1);

    // Reaching shutdown proves dispatch released the mail's residency guard.
    let gate = Arc::new(ShutdownGate::default());
    first.thread_extension_data().insert(Arc::clone(&gate));

    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, "start eviction"),
        sse(vec![
            ev_response_created("eviction-response"),
            ev_function_call_with_namespace(
                "cancelled-spawn",
                MULTI_AGENT_V2_NAMESPACE,
                "spawn_agent",
                &json!({ "message": SECOND_TASK, "task_name": "replacement", "fork_turns": "none" }).to_string(),
            ),
            ev_completed("eviction-response"),
        ]),
    )
    .await;
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "start eviction".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .context("start eviction after queued mail has drained")?;
    timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .context("wait for worker shutdown to enter the eviction gate")?;
    test.codex
        .submit(Op::Interrupt)
        .await
        .context("cancel the spawn caller while eviction owns shutdown")?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;

    let delivery = mount_root_collaboration_call(
        &server,
        "send during eviction",
        "late-mail",
        "send_message",
        json!({ "target": "first", "message": LATE_MAIL }),
    )
    .await;
    let send = test.submit_turn("send during eviction");
    tokio::pin!(send);
    tokio::select! {
        result = &mut send => panic!("delivery finished before shutdown was released: {result:?}"),
        started = timeout(Duration::from_secs(10), lifecycle.delivery_started.notified()) => {
            started.context("wait for late-mail tool delivery during cancelled eviction")?;
        },
    }
    assert!(
        timeout(Duration::from_secs(1), &mut send).await.is_err(),
        "mail must not be accepted by the closing runtime after its eviction caller is cancelled",
    );
    gate.release.notify_one();
    timeout(Duration::from_secs(10), send)
        .await
        .context("wait for late-mail delivery after releasing worker shutdown")?
        .context("accept late mail while the evicted worker remains unloaded")?;
    assert_eq!(
        delivery.function_call_output_text("late-mail"),
        Some(String::new()),
    );
    timeout(Duration::from_secs(10), first.wait_until_terminated())
        .await
        .context("wait for the original worker runtime to terminate")?;
    let Err(error) = test.thread_manager.get_thread(first_id).await else {
        panic!("queue-only mail must not reload an evicted worker");
    };
    assert!(
        matches!(error.details(), CodexErrorDetails::ThreadNotFound(id) if *id == first_id),
        "expected ThreadNotFound for the evicted worker, got {error:?}",
    );
    assert!(
        matches!(
            created.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "queue-only mail must not publish a worker creation notification",
    );
    assert!(first.submit(Op::Interrupt).await.is_err());

    // A follow-up reloads the worker; mail accepted while unloaded must survive once.
    mount_root_collaboration_call(
        &server,
        "process queued mail",
        "drain-mail",
        "followup_task",
        json!({ "target": "first", "message": DRAIN_TASK }),
    )
    .await;
    let drained = mount_completed_worker(&server, DRAIN_TASK, "drain-mail").await;
    test.submit_turn("process queued mail")
        .await
        .context("drain mail accepted after cancelled eviction")?;
    assert_eq!(
        timeout(Duration::from_secs(10), created.recv())
            .await
            .context("wait for the reloaded worker creation notification after follow-up")?
            .context("worker creation subscription closed before reload")?,
        first_id,
    );
    let reloaded = test
        .thread_manager
        .get_thread(first_id)
        .await
        .context("load the replacement runtime for the original worker")?;
    assert!(
        !Arc::ptr_eq(&first, &reloaded),
        "follow-up must reload the worker, not reuse the terminated runtime",
    );
    wait_for_event(&reloaded, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    ThreadIdle::wait(&reloaded).await;
    assert_eq!(
        drained
            .single_request()
            .inputs_of_type("agent_message")
            .into_iter()
            .filter(|item| item.to_string().contains(LATE_MAIL))
            .count(),
        1,
    );

    // The cancelled caller leaves no reservation behind once teardown finishes.
    mount_root_collaboration_call(
        &server,
        "retry replacement",
        "replacement-call",
        "spawn_agent",
        json!({ "message": SECOND_TASK, "task_name": "replacement", "fork_turns": "none" }),
    )
    .await;
    mount_completed_worker(&server, SECOND_TASK, "replacement-call").await;
    test.submit_turn("retry replacement")
        .await
        .context("retry replacement after cancelled eviction releases its reservation")?;
    let replacement_id = created
        .recv()
        .await
        .context("observe replacement worker creation")?;
    let replacement = test
        .thread_manager
        .get_thread(replacement_id)
        .await
        .context("load the replacement worker")?;
    wait_for_event(&replacement, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    test.thread_manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}
