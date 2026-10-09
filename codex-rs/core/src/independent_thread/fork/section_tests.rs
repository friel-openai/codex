use super::*;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context_with_dynamic_tools_and_rx;
use crate::state::ActiveTurn;
use crate::tools::context::ToolCallSource;
use crate::tools::context::ToolPayload;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_protocol::dynamic_tools::DynamicToolFunctionSpec;
use codex_protocol::dynamic_tools::DynamicToolNamespaceSpec;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::EventMsg;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// The observed Desktop representation routes remote threads separately from keys.
#[test]
fn remote_host_is_not_inferred_from_local_sidebar_key() {
    let listing: Listing = serde_json::from_value(json!({
        "threads": [{"id": "chatgpt", "kind": "chatgpt"}],
        "pinnedThreads": [{"id": "parent", "kind": "codex", "hostId": "remote-ssh-discovered:abg-c"}],
        "sections": [{"sectionId": "section", "name": "Source", "itemKeys": ["codex:thread:local:parent"]}]
    })).unwrap();
    assert_eq!(
        listing
            .source_section("parent")
            .unwrap()
            .unwrap()
            .section_id,
        "section"
    );
    assert_eq!(
        listing.pinned_threads[0].host_id.as_deref(),
        Some("remote-ssh-discovered:abg-c")
    );
    assert!(listing.source_section("absent").unwrap().is_none());
}

/// Error text never becomes a placement confirmation.
#[test]
fn failed_tool_response_is_not_decoded_as_success() {
    let response = DynamicToolResponse {
        success: false,
        content_items: vec![DynamicToolCallOutputContentItem::InputText {
            text: "unavailable".to_string(),
        }],
    };
    assert!(decode::<Listing>(response).is_err());
}

/// Advertised capabilities alone permit the real dynamic request transport.
#[test_case::test_case(true, "inherited"; "confirmed")]
#[test_case::test_case(false, "failed"; "rejected")]
#[tokio::test]
async fn placement_uses_client_routing_and_reports_move_result(
    move_succeeds: bool,
    expected_status: &str,
) {
    let (session, turn, events) =
        make_session_and_context_with_dynamic_tools_and_rx(vec![DynamicToolSpec::Namespace(
            DynamicToolNamespaceSpec {
                name: "codex_app".to_string(),
                description: "Desktop tools".to_string(),
                tools: ["list_threads", "move_thread_to_sidebar_section"]
                    .into_iter()
                    .map(|name| {
                        DynamicToolNamespaceTool::Function(DynamicToolFunctionSpec {
                            name: name.to_string(),
                            description: name.to_string(),
                            input_schema: json!({"type": "object"}),
                            defer_loading: false,
                        })
                    })
                    .collect(),
            },
        )])
        .await;
    *session.active_turn.lock().await = Some(ActiveTurn::default());
    let fork = ThreadId::new();
    let invocation = ToolInvocation {
        session: Arc::clone(&session),
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
        call_id: "placement-test".to_string(),
        tool_name: ToolName::namespaced("frodex", "fork_thread"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
    };
    let client = async {
        let mut requests = 0;
        while requests < 2 {
            let event = events.recv().await.unwrap();
            let EventMsg::ItemStarted(event) = event.msg else {
                continue;
            };
            let TurnItem::DynamicToolCall(call) = event.item else {
                continue;
            };
            assert_eq!(call.namespace.as_deref(), Some("codex_app"));
            let (value, success) = if call.tool == "list_threads" {
                assert_eq!(call.arguments, json!({"limit": 50}));
                (
                    json!({
                        "threads": [{"id": "chatgpt", "kind": "chatgpt"}],
                        "pinnedThreads": [{"id": session.thread_id(), "kind": "codex", "hostId": "remote-ssh-discovered:abg-c"}],
                        "sections": [{"sectionId": "source-section", "name": "Source", "itemKeys": [format!("codex:thread:local:{}", session.thread_id())]}],
                    }),
                    true,
                )
            } else {
                assert_eq!(call.tool, "move_thread_to_sidebar_section");
                assert_eq!(
                    call.arguments,
                    json!({"threadId": fork, "hostId": "remote-ssh-discovered:abg-c", "sectionId": "source-section"})
                );
                (call.arguments.clone(), move_succeeds)
            };
            session
                .notify_dynamic_tool_response(
                    &call.id,
                    DynamicToolResponse {
                        content_items: vec![DynamicToolCallOutputContentItem::InputText {
                            text: value.to_string(),
                        }],
                        success,
                    },
                )
                .await;
            requests += 1;
        }
    };
    let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(place(&invocation, fork, true, None), client)
    })
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(outcome).unwrap()["status"],
        expected_status
    );
}

/// An explicit name wins over both inherited membership and opting out.
#[test_case::test_case(true; "inherit_enabled")]
#[test_case::test_case(false; "inherit_disabled")]
#[tokio::test]
async fn explicit_existing_section_overrides_inheritance(inherit: bool) {
    let (invocation, events) =
        desktop_session(&["list_threads", "move_thread_to_sidebar_section"]).await;
    let fork = ThreadId::new();
    let client = async {
        reply_to_desktop(&invocation, &events, "list_threads", json!({"limit": 50}), json!({
            "threads": [{"id": invocation.session.thread_id(), "kind": "codex", "hostId": "remote"}],
            "sections": [
                {"sectionId": "source", "name": "Source", "itemKeys": [format!("codex:thread:local:{}", invocation.session.thread_id())]},
                {"sectionId": "destination", "name": "Research", "itemKeys": []},
            ],
        }), true).await;
        let placement = json!({"threadId": fork, "hostId": "remote", "sectionId": "destination"});
        reply_to_desktop(
            &invocation,
            &events,
            "move_thread_to_sidebar_section",
            placement.clone(),
            placement,
            true,
        )
        .await;
    };
    let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(place(&invocation, fork, inherit, Some("Research")), client)
    })
    .await
    .unwrap();
    assert_eq!(
        outcome,
        Outcome::Placed {
            section_id: "destination".to_string(),
            created: false
        }
    );
}

/// A missing name creates a destination even when the caller has no section.
#[test_case::test_case(true; "move_confirmed")]
#[test_case::test_case(false; "move_rejected_after_creation")]
#[tokio::test]
async fn explicit_missing_section_is_created_before_placement(move_succeeds: bool) {
    let (invocation, events) = desktop_session(&[
        "list_threads",
        "create_sidebar_section",
        "move_thread_to_sidebar_section",
    ])
    .await;
    let fork = ThreadId::new();
    let client = async {
        reply_to_desktop(&invocation, &events, "list_threads", json!({"limit": 50}), json!({
            "threads": [{"id": invocation.session.thread_id(), "kind": "codex", "hostId": "remote"}],
            "sections": [],
        }), true).await;
        reply_to_desktop(
            &invocation,
            &events,
            "create_sidebar_section",
            json!({"name": "Research"}),
            json!({
                "sectionId": "created-section", "name": "Research", "itemKeys": [],
            }),
            true,
        )
        .await;
        let placement =
            json!({"threadId": fork, "hostId": "remote", "sectionId": "created-section"});
        reply_to_desktop(
            &invocation,
            &events,
            "move_thread_to_sidebar_section",
            placement.clone(),
            placement,
            move_succeeds,
        )
        .await;
    };
    let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(place(&invocation, fork, false, Some("Research")), client)
    })
    .await
    .unwrap();
    if move_succeeds {
        assert_eq!(
            outcome,
            Outcome::Placed {
                section_id: "created-section".to_string(),
                created: true
            }
        );
    } else {
        let Outcome::Failed { reason } = outcome else {
            panic!("unconfirmed placement must not report success")
        };
        assert!(reason.contains("created-section"));
        assert!(reason.contains("created: true"));
    }
}

/// A rejected or mismatched creation receipt cannot authorize a move.
#[test_case::test_case(false, "Research"; "creation_rejected")]
#[test_case::test_case(true, "Different"; "creation_mismatch")]
#[tokio::test]
async fn unconfirmed_creation_does_not_move(success: bool, returned_name: &str) {
    let (invocation, events) = desktop_session(&[
        "list_threads",
        "create_sidebar_section",
        "move_thread_to_sidebar_section",
    ])
    .await;
    let fork = ThreadId::new();
    let client = async {
        reply_to_desktop(&invocation, &events, "list_threads", json!({"limit": 50}), json!({
            "threads": [{"id": invocation.session.thread_id(), "kind": "codex", "hostId": "remote"}],
            "sections": [],
        }), true).await;
        reply_to_desktop(
            &invocation,
            &events,
            "create_sidebar_section",
            json!({"name": "Research"}),
            json!({"sectionId": "created", "name": returned_name, "itemKeys": []}),
            success,
        )
        .await;
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(place(&invocation, fork, false, Some("Research")), client)
    })
    .await
    .unwrap();
    assert!(matches!(result, Outcome::Failed { .. }));
}

/// Ambiguous names must not select or create another section.
#[tokio::test]
async fn duplicate_names_are_reported_without_mutation() {
    let (invocation, events) = desktop_session(&["create_sidebar_section"]).await;
    let listing: Listing = serde_json::from_value(json!({"threads": [], "sections": [
        {"sectionId": "one", "name": "Research", "itemKeys": []},
        {"sectionId": "two", "name": "Research", "itemKeys": []},
    ]}))
    .unwrap();
    let error = named_destination(&invocation, &listing, "Research")
        .await
        .unwrap_err();
    assert!(error.contains("multiple sections"));
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event.msg, EventMsg::ItemStarted(event) if matches!(event.item, TurnItem::DynamicToolCall(_)))
        );
    }
}

/// Missing creation support does not turn an explicit destination into inheritance.
#[tokio::test]
async fn missing_creation_capability_reports_failure() {
    let (invocation, events) =
        desktop_session(&["list_threads", "move_thread_to_sidebar_section"]).await;
    let fork = ThreadId::new();
    let client = reply_to_desktop(
        &invocation,
        &events,
        "list_threads",
        json!({"limit": 50}),
        json!({
            "threads": [{"id": invocation.session.thread_id(), "kind": "codex", "hostId": "remote"}],
            "sections": [{"sectionId": "source", "name": "Source", "itemKeys": [format!("codex:thread:local:{}", invocation.session.thread_id())]}],
        }),
        true,
    );
    let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(place(&invocation, fork, true, Some("Research")), client)
    })
    .await
    .unwrap();
    assert_eq!(
        outcome,
        Outcome::Failed {
            reason: "Desktop section creation capability is not available".to_string()
        }
    );
}

/// Advertised Desktop tools cannot escape the caller's startup tool restrictions.
#[test_case::test_case(false; "listing_denied")]
#[test_case::test_case(true; "move_denied_after_allowed_listing")]
#[tokio::test]
async fn restricted_root_does_not_invoke_disallowed_desktop_tool(allow_listing: bool) {
    let (mut invocation, events) =
        desktop_session(&["list_threads", "move_thread_to_sidebar_section"]).await;
    let mut allowed_tools = vec![ToolName::namespaced("frodex", "fork_thread")];
    if allow_listing {
        allowed_tools.push(ToolName::new(None, "codex_app__list_threads".to_string()));
    }
    Arc::get_mut(&mut invocation.session)
        .expect("unshared test session")
        .tool_policy = Arc::new(codex_extension_api::ToolPolicy {
        allowed_tools: Some(allowed_tools),
        ..Default::default()
    });
    let client = async {
        if allow_listing {
            reply_to_desktop(
                &invocation,
                &events,
                "list_threads",
                json!({"limit": 50}),
                json!({
                    "threads": [{"id": invocation.session.thread_id(), "kind": "codex", "hostId": "remote"}],
                    "sections": [{"sectionId": "source", "name": "Source", "itemKeys": [format!("codex:thread:local:{}", invocation.session.thread_id())]}],
                }),
                true,
            ).await;
        }
    };
    let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(place(&invocation, ThreadId::new(), true, None), client)
    })
    .await
    .unwrap();
    let Outcome::Failed { reason } = outcome else {
        panic!("restricted placement must not succeed")
    };
    assert!(reason.contains("not allowed by the caller's tool policy"));
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event.msg, EventMsg::ItemStarted(event) if matches!(event.item, TurnItem::DynamicToolCall(_)))
        );
    }
}

/// Opens an active source turn with the supplied Desktop capabilities.
async fn desktop_session(
    names: &[&str],
) -> (
    ToolInvocation,
    async_channel::Receiver<codex_protocol::protocol::Event>,
) {
    let tools = names
        .iter()
        .map(|name| {
            DynamicToolSpec::Function(DynamicToolFunctionSpec {
                name: format!("codex_app__{name}"),
                description: name.to_string(),
                input_schema: json!({"type": "object"}),
                defer_loading: false,
            })
        })
        .collect();
    let (session, turn, events) = make_session_and_context_with_dynamic_tools_and_rx(tools).await;
    *session.active_turn.lock().await = Some(ActiveTurn::default());
    (
        ToolInvocation {
            session,
            step_context: StepContext::for_test(Arc::clone(&turn)),
            turn,
            cancellation_token: CancellationToken::new(),
            tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
            call_id: "explicit-section-test".to_string(),
            tool_name: ToolName::namespaced("frodex", "fork_thread"),
            source: ToolCallSource::Direct,
            payload: ToolPayload::Function {
                arguments: "{}".to_string(),
            },
        },
        events,
    )
}

/// Checks the next Desktop request and answers through the client transport.
async fn reply_to_desktop(
    invocation: &ToolInvocation,
    events: &async_channel::Receiver<codex_protocol::protocol::Event>,
    tool: &str,
    arguments: Value,
    response: Value,
    success: bool,
) {
    loop {
        let event = events.recv().await.unwrap();
        let EventMsg::ItemStarted(event) = event.msg else {
            continue;
        };
        let TurnItem::DynamicToolCall(call) = event.item else {
            continue;
        };
        assert_eq!(call.tool, format!("codex_app__{tool}"));
        assert_eq!(call.arguments, arguments);
        invocation
            .session
            .notify_dynamic_tool_response(
                &call.id,
                DynamicToolResponse {
                    content_items: vec![DynamicToolCallOutputContentItem::InputText {
                        text: response.to_string(),
                    }],
                    success,
                },
            )
            .await;
        return;
    }
}

/// A stalled Desktop request ends at its deadline and records a terminal item.
#[tokio::test]
async fn timed_out_request_finishes_pending_dynamic_call() {
    let (session, turn, events) = crate::session::tests::make_session_and_context_with_rx().await;
    *session.active_turn.lock().await = Some(ActiveTurn::default());
    let invocation = ToolInvocation {
        session: Arc::clone(&session),
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
        call_id: "timeout-test".to_string(),
        tool_name: ToolName::namespaced("frodex", "fork_thread"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
    };
    tokio::time::pause();
    let result = request::<Listing>(
        &invocation,
        ToolName::namespaced("codex_app", "list_threads"),
        json!({}),
        "list",
    )
    .await;
    assert!(result.err().unwrap().contains("outcome is unconfirmed"));
    loop {
        let event = events.try_recv().unwrap();
        if let EventMsg::ItemCompleted(event) = event.msg
            && let TurnItem::DynamicToolCall(call) = event.item
        {
            assert_eq!(call.success, Some(false));
            assert_eq!(call.id, "timeout-test-section-list");
            break;
        }
    }
}

/// Opting out must not perform even a sidebar lookup when tools are available.
#[tokio::test]
async fn opt_out_makes_no_desktop_request() {
    let tools = ["list_threads", "move_thread_to_sidebar_section"]
        .into_iter()
        .map(|name| {
            DynamicToolSpec::Function(DynamicToolFunctionSpec {
                name: format!("codex_app__{name}"),
                description: name.to_string(),
                input_schema: json!({"type": "object"}),
                defer_loading: false,
            })
        })
        .collect();
    let (session, turn, events) = make_session_and_context_with_dynamic_tools_and_rx(tools).await;
    *session.active_turn.lock().await = Some(ActiveTurn::default());
    let invocation = ToolInvocation {
        session,
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
        call_id: "opt-out-test".to_string(),
        tool_name: ToolName::namespaced("frodex", "fork_thread"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
    };
    assert_eq!(
        place(&invocation, ThreadId::new(), false, None).await,
        Outcome::Skipped {
            reason: "inherit_section is false".to_string(),
        }
    );
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event.msg, EventMsg::ItemStarted(event) if matches!(event.item, TurnItem::DynamicToolCall(_)))
        );
    }
}

/// A call whose source turn ended must not wait on an unregistered response.
#[tokio::test]
async fn missing_active_turn_returns_without_a_client_request() {
    let (session, turn, events) = crate::session::tests::make_session_and_context_with_rx().await;
    assert!(session.active_turn.lock().await.is_none());
    let invocation = ToolInvocation {
        session,
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
        call_id: "ended-turn-test".to_string(),
        tool_name: ToolName::namespaced("frodex", "fork_thread"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
    };
    tokio::time::pause();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        request::<Listing>(
            &invocation,
            ToolName::namespaced("codex_app", "list_threads"),
            json!({}),
            "list",
        ),
    )
    .await
    .expect("ended turns must not wait for a placement timeout");
    assert_eq!(
        result.err().as_deref(),
        Some("Desktop placement request was cancelled")
    );
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event.msg, EventMsg::ItemStarted(event) if matches!(event.item, TurnItem::DynamicToolCall(_)))
        );
    }
}
