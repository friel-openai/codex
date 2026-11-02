use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::elicitation::ElicitationRequestManager;
use crate::elicitation::ElicitationRequestRouter;
use crate::request_router::McpConnectionRequestRouter;
use crate::request_router::McpSessionRoute;
use codex_protocol::approvals::ElicitationRequestEvent;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_rmcp_client::Elicitation;
use codex_rmcp_client::ElicitationResponse;
use rmcp::model::ElicitationAction;
use rmcp::model::RequestId;
use tokio::sync::Notify;

fn elicitation_manager(
    approval_policy: AskForApproval,
    permission_profile: PermissionProfile,
) -> ElicitationRequestManager {
    ElicitationRequestManager::new(
        crate::mcp::tests::test_elicitation_config("test", approval_policy, permission_profile),
        /*reviewer*/ None,
        /*lifecycle*/ None,
        ElicitationRequestRouter::default(),
    )
}

fn route() -> (
    Arc<McpSessionRoute>,
    ElicitationRequestManager,
    async_channel::Receiver<Event>,
) {
    let manager = elicitation_manager(AskForApproval::OnRequest, PermissionProfile::default());
    let (tx, rx) = async_channel::unbounded();
    (
        Arc::new(McpSessionRoute::new(
            "test-submit".to_string(),
            manager.clone(),
            Some(tx),
        )),
        manager,
        rx,
    )
}

fn route_with_manager(
    submit_id: &str,
    manager: &ElicitationRequestManager,
) -> (Arc<McpSessionRoute>, async_channel::Receiver<Event>) {
    let (tx, rx) = async_channel::unbounded();
    (
        Arc::new(McpSessionRoute::new(
            submit_id.to_string(),
            manager.clone(),
            Some(tx),
        )),
        rx,
    )
}

#[tokio::test]
async fn threadless_route_declines_interactive_elicitation() -> anyhow::Result<()> {
    let manager = elicitation_manager(AskForApproval::OnRequest, PermissionProfile::default());
    let route = Arc::new(McpSessionRoute::new(
        "threadless".to_string(),
        manager,
        /*tx_event*/ None,
    ));
    let router = McpConnectionRequestRouter::default();
    router.register(&route);
    let sender = router.make_sender("test".to_string(), false);

    let response = router
        .run(route, async move {
            sender(
                RequestId::String("threadless-request".into()),
                Elicitation::OpenAiForm {
                    meta: None,
                    message: "requires a user response".to_string(),
                    requested_schema: serde_json::json!({ "type": "object" }),
                },
            )
            .await
        })
        .await??;

    assert_eq!(response.action, ElicitationAction::Decline);
    Ok(())
}

#[tokio::test]
async fn operations_are_serialized_across_session_routes() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));

    let operations = [first_route, second_route].map(|route| {
        let router = router.clone();
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        tokio::spawn(async move {
            router
                .run(route, async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                })
                .await
        })
    });
    for operation in operations {
        operation.await.expect("operation task should finish")?;
    }

    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn operations_from_one_session_route_remain_concurrent() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, _, _) = route();
    router.register(&route);
    let rendezvous = Arc::new(tokio::sync::Barrier::new(2));

    let operations = [(), ()].map(|()| {
        let router = router.clone();
        let route = Arc::clone(&route);
        let rendezvous = Arc::clone(&rendezvous);
        tokio::spawn(async move {
            router
                .run(route, async move {
                    tokio::time::timeout(Duration::from_secs(1), rendezvous.wait()).await
                })
                .await
        })
    });
    for operation in operations {
        operation.await.expect("operation task should finish")??;
    }
    Ok(())
}

#[tokio::test]
async fn reconciled_authority_views_remain_concurrent() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let manager = elicitation_manager(AskForApproval::OnRequest, PermissionProfile::default());
    let (previous, previous_events) = route_with_manager("previous", &manager);
    let (reconciled, reconciled_events) = route_with_manager("reconciled", &manager);
    assert!(!Arc::ptr_eq(&previous, &reconciled));
    router.register(&previous);
    router.register(&reconciled);
    let rendezvous = Arc::new(tokio::sync::Barrier::new(2));

    let operations = [Arc::clone(&previous), Arc::clone(&reconciled)].map(|route| {
        let router = router.clone();
        let rendezvous = Arc::clone(&rendezvous);
        tokio::spawn(async move {
            router
                .run(route, async move {
                    tokio::time::timeout(Duration::from_secs(1), rendezvous.wait()).await
                })
                .await
        })
    });
    for operation in operations {
        operation.await.expect("operation task should finish")??;
    }

    router.emit_startup_ready("test".to_string()).await;
    for (events, submit_id) in [
        (&previous_events, "previous"),
        (&reconciled_events, "reconciled"),
    ] {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
        assert_eq!(event.id, submit_id);
        let EventMsg::McpStartupUpdate(event) = event.msg else {
            anyhow::bail!("expected a recovery event for each view");
        };
        assert_eq!(event.server, "test");
        assert!(matches!(
            event.status,
            codex_protocol::protocol::McpStartupStatus::Ready
        ));
    }
    router.unregister(&previous);
    assert!(previous.is_closed());
    assert!(!reconciled.is_closed());
    let _active = router.acquire(Arc::clone(&reconciled)).await?;
    Ok(())
}

#[tokio::test]
async fn equal_route_metadata_does_not_merge_distinct_authorities() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let config = crate::mcp::tests::test_elicitation_config(
        "test",
        AskForApproval::OnRequest,
        PermissionProfile::default(),
    );
    let responders = ElicitationRequestRouter::default();
    let first_manager = ElicitationRequestManager::new(
        Arc::clone(&config),
        /*reviewer*/ None,
        /*lifecycle*/ None,
        responders.clone(),
    );
    let second_manager = ElicitationRequestManager::new(
        config, /*reviewer*/ None, /*lifecycle*/ None, responders,
    );
    let (events, _received) = async_channel::unbounded();
    let first = Arc::new(McpSessionRoute::new(
        "same-submit".to_string(),
        first_manager,
        Some(events.clone()),
    ));
    let second = Arc::new(McpSessionRoute::new(
        "same-submit".to_string(),
        second_manager,
        Some(events),
    ));
    router.register(&first);
    router.register(&second);

    let first_active = router.acquire(first).await?;
    let mut second_active = Box::pin(router.acquire(second));
    assert!(futures::poll!(&mut second_active).is_pending());
    drop(first_active);
    let _second_active = tokio::time::timeout(Duration::from_secs(1), second_active).await??;
    Ok(())
}

#[tokio::test]
async fn compatible_view_counts_preserve_exclusivity_and_fifo() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let manager = elicitation_manager(AskForApproval::OnRequest, PermissionProfile::default());
    let (previous, _previous_events) = route_with_manager("previous", &manager);
    let (reconciled, _reconciled_events) = route_with_manager("reconciled", &manager);
    let (foreign, _, _) = route();
    router.register(&previous);
    router.register(&reconciled);
    router.register(&foreign);
    let previous_first = router.acquire(Arc::clone(&previous)).await?;
    let previous_second = router.acquire(Arc::clone(&previous)).await?;
    let reconciled_first = tokio::time::timeout(
        Duration::from_secs(1),
        router.acquire(Arc::clone(&reconciled)),
    )
    .await??;
    let mut foreign_waiter = Box::pin(router.acquire(foreign));
    assert!(futures::poll!(&mut foreign_waiter).is_pending());
    let mut reconciled_waiter = Box::pin(router.acquire(Arc::clone(&reconciled)));
    assert!(futures::poll!(&mut reconciled_waiter).is_pending());
    let cancelled_calls = Arc::new(AtomicUsize::new(0));
    let cancelled_operation_calls = Arc::clone(&cancelled_calls);
    let mut cancelled = Box::pin(router.run(Arc::clone(&reconciled), async move {
        cancelled_operation_calls.fetch_add(1, Ordering::SeqCst);
    }));
    assert!(futures::poll!(&mut cancelled).is_pending());
    drop(cancelled);

    router.unregister(&previous);
    assert!(previous.is_closed());
    assert!(!reconciled.is_closed());
    drop(previous_first);
    assert!(router.current_route().is_none());
    assert!(futures::poll!(&mut foreign_waiter).is_pending());
    drop(previous_second);
    assert!(Arc::ptr_eq(
        &router
            .current_route()
            .expect("only the reconciled view remains active"),
        &reconciled,
    ));
    assert!(futures::poll!(&mut foreign_waiter).is_pending());
    assert!(futures::poll!(&mut reconciled_waiter).is_pending());
    drop(reconciled_first);

    let foreign_active = tokio::time::timeout(Duration::from_secs(1), foreign_waiter).await??;
    assert!(futures::poll!(&mut reconciled_waiter).is_pending());
    drop(foreign_active);
    let _reconciled_active =
        tokio::time::timeout(Duration::from_secs(1), reconciled_waiter).await??;
    assert_eq!(cancelled_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn ambiguous_compatible_views_do_not_borrow_event_routes() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let manager = elicitation_manager(AskForApproval::OnRequest, PermissionProfile::default());
    let (previous, previous_events) = route_with_manager("previous", &manager);
    let (reconciled, reconciled_events) = route_with_manager("reconciled", &manager);
    router.register(&previous);
    router.register(&reconciled);
    let sender = router.make_sender("test".to_string(), false);
    let request = || Elicitation::OpenAiForm {
        meta: None,
        message: "requires one initiating view".to_string(),
        requested_schema: serde_json::json!({ "type": "object" }),
    };
    assert!(
        sender(RequestId::String("idle".into()), request())
            .await
            .is_err()
    );
    let previous_active = router.acquire(Arc::clone(&previous)).await?;
    let reconciled_active = tokio::time::timeout(
        Duration::from_secs(1),
        router.acquire(Arc::clone(&reconciled)),
    )
    .await??;
    assert!(
        sender(RequestId::String("both-active".into()), request())
            .await
            .is_err()
    );
    previous.close();
    assert!(
        sender(RequestId::String("closed-but-active".into()), request())
            .await
            .is_err()
    );
    assert!(previous_events.try_recv().is_err());
    assert!(reconciled_events.try_recv().is_err());
    let mut blocked = Box::pin(router.acquire(Arc::clone(&reconciled)));
    assert!(futures::poll!(&mut blocked).is_pending());
    drop(previous_active);
    let _reconciled_second = tokio::time::timeout(Duration::from_secs(1), blocked).await??;

    let pending = tokio::spawn(sender(RequestId::String("unique-active".into()), request()));
    let event = tokio::time::timeout(Duration::from_secs(1), reconciled_events.recv()).await??;
    assert_eq!(event.id, "mcp_elicitation_request");
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected request for the remaining active view");
    };
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected string request id");
    };
    manager
        .resolve(
            event.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: None,
                meta: None,
            },
        )
        .await?;
    assert_eq!(pending.await??.action, ElicitationAction::Accept);
    assert!(previous_events.try_recv().is_err());
    assert!(reconciled_events.try_recv().is_err());
    drop(reconciled_active);
    Ok(())
}

#[tokio::test]
async fn closing_selected_view_does_not_retarget_to_compatible_view() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let manager = elicitation_manager(AskForApproval::OnRequest, PermissionProfile::default());
    let (previous, previous_events) = route_with_manager("previous", &manager);
    let (reconciled, reconciled_events) = route_with_manager("reconciled", &manager);
    router.register(&previous);
    router.register(&reconciled);
    let _previous_active = router.acquire(Arc::clone(&previous)).await?;
    let sender = router.make_sender("test".to_string(), false);
    let pending = tokio::spawn(sender(
        RequestId::String("pinned".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "remain on the selected view".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    ));
    let event = tokio::time::timeout(Duration::from_secs(1), previous_events.recv()).await??;
    assert_eq!(event.id, "mcp_elicitation_request");
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected request for the selected view");
    };
    let _reconciled_active = tokio::time::timeout(
        Duration::from_secs(1),
        router.acquire(Arc::clone(&reconciled)),
    )
    .await??;
    previous.close();
    assert!(pending.await?.is_err());
    let abandoned = previous_events.try_recv()?;
    assert_eq!(abandoned.id, "mcp_elicitation_abandoned");
    let EventMsg::ElicitationAbandoned(abandoned) = abandoned.msg else {
        anyhow::bail!("expected abandonment only on the selected view");
    };
    assert_eq!(abandoned.server_name, event.server_name);
    assert_eq!(abandoned.id, event.id);
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected string request id");
    };
    assert!(
        manager
            .resolve(
                event.server_name,
                RequestId::String(id.into()),
                ElicitationResponse {
                    action: ElicitationAction::Accept,
                    content: None,
                    meta: None,
                },
            )
            .await
            .is_err()
    );
    assert!(previous_events.try_recv().is_err());
    assert!(reconciled_events.try_recv().is_err());
    assert!(!reconciled.is_closed());
    Ok(())
}

#[tokio::test]
async fn cancelled_caller_does_not_release_route_early() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let second_started = Arc::new(Notify::new());

    let first_caller = {
        let router = router.clone();
        let first_started = Arc::clone(&first_started);
        let release_first = Arc::clone(&release_first);
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    first_started.notify_one();
                    release_first.notified().await;
                })
                .await
        })
    };
    first_started.notified().await;
    first_caller.abort();

    let second_caller = {
        let router = router.clone();
        let second_started = Arc::clone(&second_started);
        tokio::spawn(async move {
            router
                .run(second_route, async move {
                    second_started.notify_one();
                })
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_started.notified())
            .await
            .is_err()
    );
    release_first.notify_one();
    tokio::time::timeout(Duration::from_secs(1), second_started.notified()).await?;
    second_caller.await??;
    Ok(())
}

#[tokio::test]
async fn cancelling_a_queued_caller_removes_its_operation() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let second_started = Arc::new(Notify::new());

    let first = {
        let router = router.clone();
        let first_started = Arc::clone(&first_started);
        let release_first = Arc::clone(&release_first);
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    first_started.notify_one();
                    release_first.notified().await;
                })
                .await
        })
    };
    first_started.notified().await;

    let queued = {
        let router = router.clone();
        let second_started = Arc::clone(&second_started);
        tokio::spawn(async move {
            router
                .run(second_route, async move {
                    second_started.notify_one();
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    queued.abort();
    release_first.notify_one();
    first.await??;

    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_started.notified())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn waiting_routes_are_scheduled_fairly() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let active_started = Arc::new(Notify::new());
    let release_active = Arc::new(Notify::new());
    let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();

    let active = {
        let router = router.clone();
        let active_started = Arc::clone(&active_started);
        let release_active = Arc::clone(&release_active);
        let first_route = Arc::clone(&first_route);
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    active_started.notify_one();
                    release_active.notified().await;
                })
                .await
        })
    };
    active_started.notified().await;

    let second = {
        let router = router.clone();
        let order_tx = order_tx.clone();
        tokio::spawn(async move {
            router
                .run(second_route, async move {
                    let _ = order_tx.send("second");
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    let first_again = {
        let router = router.clone();
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    let _ = order_tx.send("first");
                })
                .await
        })
    };
    release_active.notify_one();

    active.await??;
    second.await??;
    first_again.await??;
    assert_eq!(order_rx.recv().await, Some("second"));
    assert_eq!(order_rx.recv().await, Some("first"));
    Ok(())
}

#[tokio::test]
async fn late_elicitation_after_session_close_is_rejected_not_misrouted() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, first_events) = route();
    let (second_route, _, second_events) = route();
    router.register(&first_route);
    router.register(&second_route);
    let operation_started = Arc::new(Notify::new());
    let send_elicitation = Arc::new(Notify::new());
    let sender = router.make_sender("test".to_string(), false);

    let caller = {
        let router = router.clone();
        let operation_route = Arc::clone(&first_route);
        let operation_started = Arc::clone(&operation_started);
        let send_elicitation = Arc::clone(&send_elicitation);
        tokio::spawn(async move {
            router
                .run(operation_route, async move {
                    operation_started.notify_one();
                    send_elicitation.notified().await;
                    sender(
                        RequestId::String("late-request".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "late request".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                })
                .await
        })
    };
    operation_started.notified().await;
    caller.abort();
    router.unregister(&first_route);
    send_elicitation.notify_one();

    tokio::time::timeout(Duration::from_secs(1), router.run(second_route, async {})).await??;
    assert!(first_events.try_recv().is_err());
    assert!(second_events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn elicitation_is_delivered_only_to_the_active_session() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, child_manager, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);
    let sender = router.make_sender("test".to_string(), false);

    let operation = {
        let router = router.clone();
        tokio::spawn(async move {
            router
                .run(child_route, async move {
                    sender(
                        RequestId::String("server-request".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "child only".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                })
                .await
        })
    };
    let event = tokio::time::timeout(Duration::from_secs(1), child_events.recv()).await??;
    let EventMsg::ElicitationRequest(ElicitationRequestEvent {
        server_name, id, ..
    }) = event.msg
    else {
        anyhow::bail!("expected an elicitation request event");
    };
    assert!(root_events.try_recv().is_err());
    let codex_protocol::mcp::RequestId::String(id) = id else {
        anyhow::bail!("expected a string elicitation request id");
    };
    child_manager
        .resolve(
            server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    let response = operation.await???;
    assert_eq!(response.action, ElicitationAction::Accept);
    Ok(())
}

#[tokio::test]
async fn closing_origin_abandons_pending_elicitation_only_to_origin() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, child_manager, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);
    let _active = router.acquire(Arc::clone(&child_route)).await?;
    let sender = router.make_sender("test".to_string(), false);
    let request = tokio::spawn(sender(
        RequestId::String("pending-request".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "child only".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    ));
    let event = tokio::time::timeout(Duration::from_secs(1), child_events.recv()).await??;
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected request");
    };
    child_route.close();
    assert!(request.await?.is_err());
    let abandoned = child_events.try_recv()?;
    let EventMsg::ElicitationAbandoned(abandoned) = abandoned.msg else {
        anyhow::bail!("expected abandonment");
    };
    assert_eq!(abandoned.server_name, event.server_name);
    assert_eq!(abandoned.id, event.id);
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected string request id");
    };
    assert!(
        child_manager
            .resolve(
                event.server_name,
                RequestId::String(id.into()),
                ElicitationResponse {
                    action: ElicitationAction::Accept,
                    content: None,
                    meta: None,
                }
            )
            .await
            .is_err()
    );
    assert!(child_events.try_recv().is_err());
    assert!(root_events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn routed_elicitation_preserves_bounded_backpressure() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let manager = elicitation_manager(AskForApproval::OnRequest, PermissionProfile::default());
    let (tx, events) = async_channel::bounded(1);
    let route = Arc::new(McpSessionRoute::new(
        "test".to_string(),
        manager.clone(),
        Some(tx),
    ));
    router.register(&route);
    router.emit_startup_ready("test".to_string()).await;
    let sender = router.make_sender("test".to_string(), false);
    let mut request = sender(
        RequestId::String("bounded-request".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "bounded".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    );
    assert!(futures::poll!(request.as_mut()).is_pending());
    assert!(matches!(
        events.recv().await?.msg,
        EventMsg::McpStartupUpdate(_)
    ));
    let request = tokio::spawn(request);
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected bounded request after capacity became available");
    };
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected string request id");
    };
    manager
        .resolve(
            event.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: None,
                meta: None,
            },
        )
        .await?;
    assert_eq!(request.await??.action, ElicitationAction::Accept);
    assert!(events.try_recv().is_err());

    router.emit_startup_ready("test".to_string()).await;
    let mut retired_request = sender(
        RequestId::String("retired-bounded-request".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "must not outlive its connection".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    );
    assert!(futures::poll!(retired_request.as_mut()).is_pending());
    router.close();
    assert!(!route.is_closed());
    assert!(matches!(
        events.recv().await?.msg,
        EventMsg::McpStartupUpdate(_)
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(1), retired_request)
            .await?
            .is_err()
    );
    assert!(events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn shared_connection_preserves_route_specific_elicitation_policy() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let never_manager = elicitation_manager(AskForApproval::Never, PermissionProfile::Disabled);
    let (never_tx, never_events) = async_channel::unbounded();
    let never_route = Arc::new(McpSessionRoute::new(
        "never".to_string(),
        never_manager,
        Some(never_tx),
    ));
    let (prompt_route, prompt_manager, prompt_events) = route();
    router.register(&never_route);
    router.register(&prompt_route);

    let never_sender = router.make_sender("test".to_string(), false);
    let declined = router
        .run(never_route, async move {
            never_sender(
                RequestId::String("never".into()),
                Elicitation::OpenAiForm {
                    meta: None,
                    message: "decline".to_string(),
                    requested_schema: serde_json::json!({ "type": "object" }),
                },
            )
            .await
        })
        .await??;
    assert_eq!(declined.action, ElicitationAction::Decline);
    assert!(never_events.try_recv().is_err());

    let prompt_sender = router.make_sender("test".to_string(), false);
    let request = {
        let router = router.clone();
        tokio::spawn(async move {
            router
                .run(prompt_route, async move {
                    prompt_sender(
                        RequestId::String("prompt".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "prompt".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                })
                .await
        })
    };
    let event = tokio::time::timeout(Duration::from_secs(1), prompt_events.recv()).await??;
    assert!(never_events.try_recv().is_err());
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected an elicitation request event");
    };
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected a string elicitation request id");
    };
    prompt_manager
        .resolve(
            event.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    assert_eq!(request.await???.action, ElicitationAction::Accept);
    Ok(())
}

#[tokio::test]
async fn unattributed_elicitation_is_rejected_instead_of_borrowing_a_session_policy() {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, _, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);
    let sender = router.make_sender("test".to_string(), false);
    let error = sender(
        RequestId::String("unattributed".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "no active request".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    )
    .await
    .expect_err("an unattributed request must fail closed");

    assert!(error.to_string().contains("no live session"));
    assert!(root_events.try_recv().is_err());
    assert!(child_events.try_recv().is_err());
}

#[tokio::test]
async fn idle_elicitation_uses_the_only_live_session_route() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, manager, events) = route();
    router.register(&route);
    let sender = router.make_sender("test".to_string(), false);
    let request = tokio::spawn(async move {
        sender(
            RequestId::String("idle".into()),
            Elicitation::OpenAiForm {
                meta: None,
                message: "idle request".to_string(),
                requested_schema: serde_json::json!({ "type": "object" }),
            },
        )
        .await
    });

    let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected an elicitation request event");
    };
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected a string elicitation request id");
    };
    manager
        .resolve(
            event.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;

    assert_eq!(request.await??.action, ElicitationAction::Accept);
    Ok(())
}

#[tokio::test]
async fn startup_recovery_event_uses_a_live_route() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, _, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);
    router.unregister(&root_route);

    router.emit_startup_ready("test-server".to_string()).await;

    assert!(root_events.try_recv().is_err());
    let event = tokio::time::timeout(Duration::from_secs(1), child_events.recv()).await??;
    assert_eq!(event.id, "test-submit");
    assert!(matches!(
        event.msg,
        EventMsg::McpStartupUpdate(codex_protocol::protocol::McpStartupUpdateEvent {
            server,
            status: codex_protocol::protocol::McpStartupStatus::Ready,
        }) if server == "test-server"
    ));
    Ok(())
}

#[tokio::test]
async fn startup_recovery_event_reaches_every_live_route() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, _, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);

    router.emit_startup_ready("test-server".to_string()).await;

    for events in [root_events, child_events] {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
        assert!(matches!(
            event.msg,
            EventMsg::McpStartupUpdate(codex_protocol::protocol::McpStartupUpdateEvent {
                server,
                status: codex_protocol::protocol::McpStartupStatus::Ready,
            }) if server == "test-server"
        ));
    }
    Ok(())
}

#[tokio::test]
async fn closed_connection_rejects_elicitation_and_suppresses_recovery_events() {
    let router = McpConnectionRequestRouter::default();
    let (session_route, _, events) = route();
    router.register(&session_route);
    let sender = router.make_sender("test".to_string(), false);
    router.close();

    let error = sender(
        RequestId::String("retired".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "retired connection".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    )
    .await
    .expect_err("a retired connection must reject server requests");
    assert!(error.to_string().contains("no live session"));
    router.emit_startup_ready("test-server".to_string()).await;
    assert!(events.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_close_linearizes_with_event_dispatch() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, _, events) = route();
    router.register(&route);
    let (dispatch_started_tx, dispatch_started_rx) = std::sync::mpsc::sync_channel(1);
    let release_dispatch = Arc::new(std::sync::Barrier::new(2));
    route.set_before_event_dispatch(Some(Arc::new({
        let release_dispatch = Arc::clone(&release_dispatch);
        move || {
            dispatch_started_tx
                .send(())
                .expect("the test should still be waiting for dispatch");
            release_dispatch.wait();
        }
    })));

    let recovery = {
        let router = router.clone();
        tokio::spawn(async move {
            router.emit_startup_ready("test-server".to_string()).await;
        })
    };
    dispatch_started_rx.recv_timeout(Duration::from_secs(1))?;
    let (close_finished_tx, close_finished_rx) = std::sync::mpsc::sync_channel(1);
    let close_route = Arc::clone(&route);
    let closer = std::thread::spawn(move || {
        close_route.close();
        close_finished_tx
            .send(())
            .expect("the test should still be waiting for close");
    });
    assert!(
        close_finished_rx
            .recv_timeout(Duration::from_millis(50))
            .is_err(),
        "close must wait for the event dispatch that already linearized"
    );

    release_dispatch.wait();
    tokio::time::timeout(Duration::from_secs(1), recovery).await??;
    close_finished_rx.recv_timeout(Duration::from_secs(1))?;
    closer.join().expect("route close thread should finish");
    route.set_before_event_dispatch(/*hook*/ None);

    let event = events.recv().await?;
    assert!(matches!(event.msg, EventMsg::McpStartupUpdate(_)));
    router.emit_startup_ready("after-close".to_string()).await;
    assert!(events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn a_closed_route_rejects_elicitation_and_recovery_events() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, _, events) = route();
    router.register(&route);
    let sender = router.make_sender("test".to_string(), false);
    route.close();

    let error = sender(
        RequestId::String("after-close".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "closed route".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    )
    .await
    .expect_err("a closed route must reject elicitation");
    assert!(error.to_string().contains("no live session"));

    router.emit_startup_ready("test-server".to_string()).await;
    assert!(events.try_recv().is_err());
    Ok(())
}

#[test]
fn a_retired_connection_rejects_an_event_after_route_selection() {
    let router = McpConnectionRequestRouter::default();
    let (route, _, events) = route();
    router.register(&route);
    let selected_route = router
        .current_route()
        .expect("the route should be selectable before retirement");
    router.close();

    let error = router
        .dispatch_event(
            &selected_route,
            Event {
                id: "retired".to_string(),
                msg: EventMsg::McpStartupUpdate(codex_protocol::protocol::McpStartupUpdateEvent {
                    server: "retired".to_string(),
                    status: codex_protocol::protocol::McpStartupStatus::Ready,
                }),
            },
        )
        .expect_err("a retired generation must reject its selected route");

    assert!(error.to_string().contains("connection closed"));
    assert!(events.try_recv().is_err());
}
