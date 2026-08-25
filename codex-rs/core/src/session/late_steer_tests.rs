//! Accepted steering must finish before the idle lifecycle can schedule a supervisor.

use super::*;
use pretty_assertions::assert_eq;

/// Stops immediately after the task's last empty-input check to expose finalization races.
struct OrderingTask {
    final_check: Arc<tokio::sync::Notify>,
    finish_initial: Arc<tokio::sync::Notify>,
    continuation_started: Arc<tokio::sync::Notify>,
    finish_continuation: Arc<tokio::sync::Notify>,
}

impl SessionTask for OrderingTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.pending_input_idle_ordering"
    }

    async fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        _ctx: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        _cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        assert!(
            !session
                .input_queue
                .has_pending_input(&session.active_turn)
                .await
        );
        self.final_check.notify_one();
        self.finish_initial.notified().await;
        Ok(None)
    }

    fn supports_pending_input_continuation(&self) -> bool {
        true
    }

    async fn run_pending_input_continuation(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        _cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        let (input, _) = session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await;
        assert_eq!(input.len(), 1);
        assert!(
            matches!(&input[0], TurnInput::UserInput { content, .. }
            if matches!(content.as_slice(), [UserInput::Text { text, .. }]
                if text == "late steer before supervisor"))
                || matches!(&input[0], TurnInput::InterAgentCommunication(mail)
                if mail.content == "late mailbox update")
        );
        {
            let active = session.active_turn.lock().await;
            let current = &active.as_ref().unwrap().task.as_ref().unwrap().turn_context;
            assert!(
                Arc::ptr_eq(current, &ctx),
                "continuation must use the current task context"
            );
        }
        self.continuation_started.notify_one();
        self.finish_continuation.notified().await;
        Ok(Some("continued".to_string()))
    }
}

/// Finalization must distinguish current-turn input from errors and deferred mail.
#[derive(Clone, Copy)]
enum TaskOutcome {
    Success,
    RefreshedContext,
    CurrentMailbox,
    DeferredMailbox,
    ReportedTerminalError,
    DeferredMail,
}

#[test_case::test_case(TaskOutcome::Success; "success_continues")]
#[test_case::test_case(TaskOutcome::RefreshedContext; "refreshed_context_continues")]
#[test_case::test_case(TaskOutcome::CurrentMailbox; "current_mailbox_continues")]
#[test_case::test_case(TaskOutcome::DeferredMailbox; "deferred_mailbox_stays_queued")]
#[test_case::test_case(TaskOutcome::ReportedTerminalError; "terminal_error_does_not_continue")]
#[test_case::test_case(TaskOutcome::DeferredMail; "deferred_mail_does_not_continue")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_steer_continues_before_thread_idle_lifecycle(outcome: TaskOutcome) {
    struct ThreadIdleRecorder {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        idle_tx: async_channel::Sender<()>,
    }
    impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for ThreadIdleRecorder {
        fn on_thread_idle<'a>(
            &'a self,
            _input: codex_extension_api::ThreadIdleInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.idle_tx.send(()).await.expect("idle receiver open");
            })
        }
    }
    let (mut session, turn_context) = make_session_and_context().await;
    let idle_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (idle_tx, idle_rx) = async_channel::bounded(1);
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.thread_lifecycle_contributor(Arc::new(ThreadIdleRecorder {
        calls: Arc::clone(&idle_calls),
        idle_tx,
    }));
    session.services.extensions = Arc::new(builder.build());
    let final_check = Arc::new(tokio::sync::Notify::new());
    let finish_initial = Arc::new(tokio::sync::Notify::new());
    let continuation_started = Arc::new(tokio::sync::Notify::new());
    let finish_continuation = Arc::new(tokio::sync::Notify::new());
    let session = Arc::new(session);
    let turn_context = Arc::new(turn_context);
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            OrderingTask {
                final_check: Arc::clone(&final_check),
                finish_initial: Arc::clone(&finish_initial),
                continuation_started: Arc::clone(&continuation_started),
                finish_continuation: Arc::clone(&finish_continuation),
            },
        )
        .await;
    timeout(StdDuration::from_secs(2), final_check.notified())
        .await
        .expect("task reaches final check");
    if matches!(outcome, TaskOutcome::RefreshedContext) {
        let prepared = session
            .prepare_turn_context_replacement(&turn_context)
            .await
            .expect("capture active task");
        let refreshed = session
            .refresh_active_turn_context(&turn_context, &prepared.settings)
            .await;
        assert!(!Arc::ptr_eq(&refreshed, &turn_context));
        assert!(
            session
                .try_replace_active_turn_context(&prepared, &refreshed)
                .await
                .expect("publish refreshed context")
        );
    }
    let expected_text = if matches!(
        outcome,
        TaskOutcome::CurrentMailbox | TaskOutcome::DeferredMailbox
    ) {
        session
            .input_queue
            .enqueue_mailbox_communication(
                InterAgentCommunication::new(
                    AgentPath::try_from("/root/worker").unwrap(),
                    AgentPath::root(),
                    Vec::new(),
                    "late mailbox update".to_string(),
                    /*trigger_turn*/ false,
                ),
                Default::default(),
            )
            .await;
        if matches!(outcome, TaskOutcome::DeferredMailbox) {
            session
                .input_queue
                .defer_mailbox_delivery_to_next_turn(&session.active_turn, &turn_context.sub_id)
                .await;
        }
        assert!(session.input_queue.has_pending_mailbox_items().await);
        "late mailbox update"
    } else if matches!(outcome, TaskOutcome::DeferredMail) {
        let turn_state = session
            .input_queue
            .turn_state_for_sub_id(&session.active_turn, &turn_context.sub_id)
            .await
            .expect("active turn state");
        session
            .input_queue
            .extend_pending_input_for_turn_state(
                &turn_state,
                vec![TurnInput::InterAgentCommunication(
                    InterAgentCommunication::new(
                        AgentPath::try_from("/root/worker").unwrap(),
                        AgentPath::root(),
                        Vec::new(),
                        "late queue-only update".to_string(),
                        /*trigger_turn*/ false,
                    ),
                )],
            )
            .await;
        session
            .input_queue
            .defer_mailbox_delivery_to_next_turn(&session.active_turn, &turn_context.sub_id)
            .await;
        assert!(
            !session
                .input_queue
                .has_pending_input(&session.active_turn)
                .await
        );
        "late queue-only update"
    } else {
        let submission = submit_steer_only(
            &session,
            vec![UserInput::Text {
                text: "late steer before supervisor".to_string(),
                text_elements: Vec::new(),
            }],
            &turn_context.sub_id,
        )
        .await;
        assert!(matches!(submission, TurnInputSubmission::Steered { .. }));
        "late steer before supervisor"
    };
    if matches!(outcome, TaskOutcome::ReportedTerminalError) {
        *turn_context.terminal_error.lock().await = Some(ErrorEvent {
            message: "already-reported terminal failure".to_string(),
            codex_error_info: None,
            misalignment: None,
        });
    }
    finish_initial.notify_one();
    if !matches!(
        outcome,
        TaskOutcome::Success | TaskOutcome::RefreshedContext | TaskOutcome::CurrentMailbox
    ) {
        timeout(StdDuration::from_secs(2), idle_rx.recv())
            .await
            .expect("task becomes idle without continuation")
            .expect("idle receiver open");
        assert!(continuation_started.notified().now_or_never().is_none());
        assert_eq!(idle_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let history = session.clone_history().await;
        if matches!(outcome, TaskOutcome::DeferredMailbox) {
            assert!(session.input_queue.has_pending_mailbox_items().await);
            let (items, _) = session.input_queue.drain_mailbox_input_items().await;
            assert!(
                matches!(items.as_slice(), [TurnInput::InterAgentCommunication(mail)]
                if mail.content == expected_text)
            );
            assert!(session.active_turn.lock().await.is_none());
            return;
        }
        assert_eq!(raw_history_items(&history).iter().filter(|item| {
            matches!(item, ResponseItem::Message { content, .. }
                if content.iter().any(|item|
                    matches!(item, ContentItem::InputText { text } if text.contains(expected_text))))
                || matches!(item, ResponseItem::AgentMessage { content, .. }
                    if codex_protocol::models::plaintext_agent_message_content(content).as_deref()
                        == Some(expected_text))
        }).count(), 1, "unconsumed input is persisted exactly once");
        assert!(session.active_turn.lock().await.is_none());
        return;
    }
    timeout(StdDuration::from_secs(2), async {
        tokio::select! {
            _ = continuation_started.notified() => {},
            _ = idle_rx.recv() => panic!("accepted late steer reached idle without continuation"),
        }
    })
    .await
    .expect("continuation must start");
    assert_eq!(idle_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    finish_continuation.notify_one();
    timeout(StdDuration::from_secs(2), idle_rx.recv())
        .await
        .expect("thread becomes idle")
        .expect("idle receiver open");
    assert_eq!(idle_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(session.active_turn.lock().await.is_none());
}
