use std::sync::Arc;

use codex_core::ForkSnapshot;
use codex_core::NewThread;
use codex_core::ThreadConfigSnapshot;
use codex_core::parse_turn_item;
use codex_features::Feature;
use codex_protocol::items::TurnItem;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InitialHistory;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ResumedHistory;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SegmentStateCheckpointDisposition;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::user_input::UserInput;
use codex_rollout::materialize_rollout_items;
use codex_rollout::validate_certified_segment_state_checkpoint;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use serde_json::Value;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_clears_inherited_subagent_environment_context() {
    skip_if_no_network!();

    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-initial"),
                ev_completed("resp-initial"),
            ]),
            sse(vec![
                ev_response_created("resp-fork"),
                ev_completed("resp-fork"),
            ]),
        ],
    )
    .await;
    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("test config should allow feature update");
    });
    let initial = builder
        .build_with_auto_env(&server)
        .await
        .expect("create source conversation");
    initial
        .submit_turn("persist a source world state")
        .await
        .expect("complete source turn");
    initial.codex.ensure_rollout_materialized().await;
    initial
        .codex
        .flush_rollout()
        .await
        .expect("flush source rollout");
    let source_thread_id = initial.session_configured.thread_id;
    let rollout_path = initial.codex.rollout_path().expect("source rollout path");
    initial
        .codex
        .shutdown_and_wait()
        .await
        .expect("shutdown source before disk-backed fork");
    initial
        .thread_manager
        .remove_thread(&source_thread_id)
        .await
        .expect("remove source runtime");

    let mut rollout_lines = std::fs::read_to_string(&rollout_path)
        .expect("read source rollout")
        .lines()
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<serde_json::Result<Vec<_>>>()
        .expect("parse source rollout");
    let world_state = rollout_lines
        .iter_mut()
        .rev()
        .find_map(|line| match &mut line.item {
            RolloutItem::WorldState(world_state) => Some(world_state),
            _ => None,
        })
        .expect("source turn should persist a world state");
    let state = world_state
        .state
        .as_object_mut()
        .expect("world state should be an object");
    let environments = state
        .entry("environments")
        .or_insert_with(|| Value::Object(Default::default()))
        .as_object_mut()
        .expect("environments world state should be an object");
    environments.insert(
        "subagents".to_string(),
        Value::String("- /root/stale: working".to_string()),
    );
    let persisted = rollout_lines
        .iter()
        .map(serde_json::to_string)
        .collect::<serde_json::Result<Vec<_>>>()
        .expect("serialize source rollout")
        .join("\n");
    std::fs::write(&rollout_path, format!("{persisted}\n")).expect("write source rollout");

    let NewThread { thread: fork, .. } = initial
        .thread_manager
        .fork_thread(
            ForkSnapshot::Interrupted,
            initial.config.clone(),
            rollout_path,
            /*thread_source*/ None,
            /*parent_trace*/ None,
        )
        .await
        .expect("fork source conversation");
    fork.submit(Op::UserInput {
        items: vec![UserInput::Text {
            text: "confirm fork membership".to_string(),
            text_elements: Vec::new(),
        }],
        final_output_json_schema: None,
        responsesapi_client_metadata: None,
        additional_context: Default::default(),
        thread_settings: Default::default(),
    })
    .await
    .expect("submit fork turn");
    wait_for_event(&fork, |event| matches!(event, EventMsg::TurnComplete(_))).await;

    let requests = responses.requests();
    pretty_assertions::assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .message_input_texts("user")
            .iter()
            .any(|text| text.contains("<subagents />")),
        "the first fork request must clear inherited subagent context"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_thread_twice_drops_to_first_message() {
    skip_if_no_network!();

    // Start a mock server that completes three turns.
    let server = MockServer::start().await;
    let sse = sse(vec![ev_response_created("resp"), ev_completed("resp")]);
    let first = ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_raw(sse.clone(), "text/event-stream");

    // Expect three calls to /v1/responses – one per user input.
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(first)
        .expect(3)
        .mount(&server)
        .await;

    let mut builder = test_codex();
    let test = builder.build(&server).await.expect("create conversation");
    let codex = test.codex.clone();
    let thread_manager = test.thread_manager.clone();
    let config_for_fork = test.config.clone();

    // Send three user messages; wait for three completed turns.
    for text in ["first", "second", "third"] {
        codex
            .submit(Op::UserInput {
                items: vec![UserInput::Text {
                    text: text.to_string(),
                    text_elements: Vec::new(),
                }],
                final_output_json_schema: None,
                responsesapi_client_metadata: None,
                additional_context: Default::default(),
                thread_settings: Default::default(),
            })
            .await
            .unwrap();
        let _ = wait_for_event(&codex, |ev| matches!(ev, EventMsg::TurnComplete(_))).await;
    }

    // Request history from the base conversation to obtain rollout path.
    let base_path = codex.rollout_path().expect("rollout path");

    // GetHistory flushes before returning the path; no wait needed.

    // Compute expected prefixes after each fork by truncating base rollout
    // strictly before the nth user input (0-based).
    let base_items = read_rollout_items(&base_path);
    let find_user_input_positions = |items: &[RolloutItem]| -> Vec<usize> {
        let mut pos = Vec::new();
        for (i, it) in items.iter().enumerate() {
            if let RolloutItem::ResponseItem(response_item) = it
                && let Some(TurnItem::UserMessage(_)) = parse_turn_item(response_item)
            {
                // Consider any user message as an input boundary; recorder stores both EventMsg and ResponseItem.
                // We specifically look for input items, which are represented as ContentItem::InputText.
                pos.push(i);
            }
        }
        pos
    };
    let turn_boundary_for_user = |items: &[RolloutItem], user_position: usize| {
        items[..user_position]
            .iter()
            .rposition(|item| matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(_))))
            .unwrap_or(user_position)
    };
    let user_inputs = find_user_input_positions(&base_items);

    // After cutting at nth user input (n=1 → second user message), cut strictly before that input.
    let cut1 = user_inputs
        .get(1)
        .copied()
        .map(|position| turn_boundary_for_user(&base_items, position))
        .unwrap_or(0);
    let mut expected_after_first: Vec<RolloutItem> = base_items[..cut1].to_vec();

    // After dropping again (n=1 on fork1), compute expected relative to fork1's rollout.

    // Fork once with n=1 → drops the last user input and everything after.
    let NewThread {
        thread: codex_fork1,
        ..
    } = thread_manager
        .fork_thread(
            ForkSnapshot::TruncateBeforeNthUserMessage(1),
            config_for_fork.clone(),
            base_path.clone(),
            /*thread_source*/ None,
            /*parent_trace*/ None,
        )
        .await
        .expect("fork 1");

    let fork1_path = codex_fork1.rollout_path().expect("rollout path");
    expected_after_first.push(thread_settings_applied_item(
        codex_fork1.config_snapshot().await,
    ));

    // GetHistory on fork1 flushed; the file is ready.
    let fork1_raw_items = read_rollout_items(&fork1_path);
    assert!(
        fork1_raw_items
            .iter()
            .any(|item| matches!(item, RolloutItem::RolloutReference(_)))
    );
    assert!(
        fork1_raw_items
            .iter()
            .all(|item| !matches!(item, RolloutItem::ResponseItem(_)))
    );
    let fork1_materialized_items = without_session_meta(
        materialize_rollout_items(test.config.codex_home.as_path(), &fork1_path)
            .await
            .expect("materialize first fork"),
    );
    let expected_fork1_settings = expected_after_first
        .pop()
        .expect("expected fork settings item");
    let (fork1_items, fork1_checkpoint) =
        split_certified_segment_checkpoint(&fork1_materialized_items);
    assert_checkpoint_thread_settings(fork1_checkpoint, &expected_fork1_settings);
    pretty_assertions::assert_eq!(
        serde_json::to_value(fork1_items).unwrap(),
        serde_json::to_value(&expected_after_first).unwrap()
    );

    // Fork again with n=0 → drops the (new) last user message, leaving only the first.
    let NewThread {
        thread: codex_fork2,
        ..
    } = thread_manager
        .fork_thread(
            ForkSnapshot::TruncateBeforeNthUserMessage(0),
            config_for_fork.clone(),
            fork1_path.clone(),
            /*thread_source*/ None,
            /*parent_trace*/ None,
        )
        .await
        .expect("fork 2");

    let fork2_path = codex_fork2.rollout_path().expect("rollout path");
    // GetHistory on fork2 flushed; the file is ready.
    let fork1_user_inputs = find_user_input_positions(fork1_items);
    let cut_last_on_fork1 = fork1_user_inputs
        .get(fork1_user_inputs.len().saturating_sub(1))
        .copied()
        .map(|position| turn_boundary_for_user(fork1_items, position))
        .unwrap_or(0);
    let mut expected_after_second: Vec<RolloutItem> = fork1_items[..cut_last_on_fork1].to_vec();
    expected_after_second.push(thread_settings_applied_item(
        codex_fork2.config_snapshot().await,
    ));
    let fork2_raw_items = read_rollout_items(&fork2_path);
    assert!(
        fork2_raw_items
            .iter()
            .any(|item| matches!(item, RolloutItem::RolloutReference(_)))
    );
    assert!(
        fork2_raw_items
            .iter()
            .all(|item| !matches!(item, RolloutItem::ResponseItem(_)))
    );
    let fork2_materialized_items = without_session_meta(
        materialize_rollout_items(test.config.codex_home.as_path(), &fork2_path)
            .await
            .expect("materialize second fork"),
    );
    let expected_fork2_settings = expected_after_second
        .pop()
        .expect("expected fork settings item");
    let (fork2_items, fork2_checkpoint) =
        split_certified_segment_checkpoint(&fork2_materialized_items);
    assert_checkpoint_thread_settings(fork2_checkpoint, &expected_fork2_settings);
    pretty_assertions::assert_eq!(
        serde_json::to_value(fork2_items).unwrap(),
        serde_json::to_value(&expected_after_second).unwrap()
    );

    // Re-forking the first truncated child at its current boundary must preserve that child's
    // inherited cutoff. It must not reopen the original parent suffix excluded by the first fork.
    let NewThread {
        thread: codex_refork,
        ..
    } = thread_manager
        .fork_thread(
            ForkSnapshot::Interrupted,
            config_for_fork,
            fork1_path,
            /*thread_source*/ None,
            /*parent_trace*/ None,
        )
        .await
        .expect("re-fork truncated child");
    let refork_path = codex_refork.rollout_path().expect("re-fork rollout path");
    let expected_refork_settings =
        thread_settings_applied_item(codex_refork.config_snapshot().await);
    let refork_raw_items = without_session_meta(read_rollout_items(&refork_path));
    let (refork_physical_prefix, refork_physical_checkpoint) =
        split_certified_segment_checkpoint(&refork_raw_items);
    assert!(matches!(
        refork_physical_prefix,
        [RolloutItem::RolloutReference(_)]
    ));
    assert_checkpoint_thread_settings(refork_physical_checkpoint, &expected_refork_settings);

    let refork_materialized_items = without_session_meta(
        materialize_rollout_items(test.config.codex_home.as_path(), &refork_path)
            .await
            .expect("materialize re-forked history"),
    );
    let (refork_items, refork_checkpoint) =
        split_certified_segment_checkpoint(&refork_materialized_items);
    assert_checkpoint_thread_settings(refork_checkpoint, &expected_refork_settings);
    pretty_assertions::assert_eq!(
        serde_json::to_value(refork_items).unwrap(),
        serde_json::to_value(&fork1_materialized_items).unwrap()
    );
}

fn split_certified_segment_checkpoint(items: &[RolloutItem]) -> (&[RolloutItem], &[RolloutItem]) {
    let checkpoint_start = items
        .iter()
        .rposition(|item| {
            matches!(
                item,
                RolloutItem::Compacted(compacted)
                    if compacted.segment_state_checkpoint.is_some()
            )
        })
        .expect("fork must persist a child-local checkpoint");
    let RolloutItem::Compacted(compacted) = &items[checkpoint_start] else {
        unreachable!("checkpoint start must be compacted history");
    };
    let descriptor = compacted
        .segment_state_checkpoint
        .as_ref()
        .expect("checkpoint descriptor");
    let checkpoint_len =
        3 + usize::from(matches!(
            descriptor.world_state,
            SegmentStateCheckpointDisposition::Established
        )) + usize::from(matches!(
            descriptor.reference_context,
            SegmentStateCheckpointDisposition::Established
        ));
    let checkpoint_end = checkpoint_start + checkpoint_len;
    assert_eq!(
        checkpoint_end,
        items.len(),
        "child-local checkpoint must be the terminal physical state"
    );
    let checkpoint = &items[checkpoint_start..checkpoint_end];
    validate_certified_segment_state_checkpoint(checkpoint)
        .expect("child-local checkpoint must be certified");
    (&items[..checkpoint_start], checkpoint)
}

fn assert_checkpoint_thread_settings(checkpoint: &[RolloutItem], expected: &RolloutItem) {
    let actual = checkpoint
        .iter()
        .find(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(_))
            )
        })
        .expect("checkpoint settings");
    pretty_assertions::assert_eq!(
        serde_json::to_value(actual).expect("serialize actual settings"),
        serde_json::to_value(expected).expect("serialize expected settings")
    );
}

fn thread_settings_applied_item(snapshot: ThreadConfigSnapshot) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
        ThreadSettingsAppliedEvent {
            thread_settings: snapshot.into_thread_settings_snapshot(),
        },
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_thread_from_history_does_not_require_source_rollout_path() {
    assert_copied_fork_persists_inherited_history(ThreadHistoryMode::Legacy).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copied_paginated_fork_persists_inherited_history() {
    assert_copied_fork_persists_inherited_history(ThreadHistoryMode::Paginated).await;
}

async fn assert_copied_fork_persists_inherited_history(history_mode: ThreadHistoryMode) {
    skip_if_no_network!();

    let server = MockServer::start().await;
    let sse = sse(vec![ev_response_created("resp"), ev_completed("resp")]);
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse, "text/event-stream"),
        )
        .expect(if matches!(history_mode, ThreadHistoryMode::Paginated) {
            2
        } else {
            1
        })
        .mount(&server)
        .await;

    let mut builder = test_codex().with_history_mode(history_mode);
    let test = builder.build(&server).await.expect("create conversation");
    let codex = test.codex.clone();
    let thread_manager = test.thread_manager.clone();

    codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "fork me from stored history".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await
        .expect("submit initial user turn");
    let _ = wait_for_event(&codex, |ev| matches!(ev, EventMsg::TurnComplete(_))).await;

    codex.flush_rollout().await.expect("flush source rollout");
    let source_path = codex.rollout_path().expect("source rollout path");
    let source_items = read_rollout_items(&source_path);
    let source_meta = codex_rollout::read_session_meta_line(source_path.as_path())
        .await
        .expect("read source session metadata");
    let mut supplied_history = vec![RolloutItem::SessionMeta(source_meta)];
    supplied_history.extend(source_items.iter().cloned());
    let NewThread {
        thread: forked_thread,
        ..
    } = thread_manager
        .fork_thread_from_history(
            ForkSnapshot::Interrupted,
            test.config.clone(),
            InitialHistory::Resumed(ResumedHistory {
                conversation_id: test.session_configured.thread_id,
                history: Arc::new(supplied_history),
                rollout_path: None,
            }),
            /*thread_source*/ None,
            /*parent_trace*/ None,
            ClientMcpExtensions::default(),
        )
        .await
        .expect("fork from stored history");

    let forked_path = forked_thread.rollout_path().expect("forked rollout path");
    let forked_raw_items = read_rollout_items(&forked_path);
    assert!(
        forked_raw_items
            .iter()
            .any(|item| matches!(item, RolloutItem::RolloutReference(_)))
    );
    let forked_items = without_session_meta(
        materialize_rollout_items(test.config.codex_home.as_path(), &forked_path)
            .await
            .expect("materialize forked history"),
    );
    let forked_items = forked_items
        .iter()
        .map(|item| serde_json::to_value(item).expect("serialize forked rollout item"))
        .collect::<Vec<_>>();
    let source_items = source_items
        .iter()
        .map(|item| serde_json::to_value(item).expect("serialize source rollout item"))
        .collect::<Vec<_>>();
    assert!(
        forked_items.starts_with(&source_items),
        "forked history should start with the supplied source history"
    );

    if matches!(history_mode, ThreadHistoryMode::Paginated) {
        forked_thread
            .shutdown_and_wait()
            .await
            .expect("shutdown copied paginated fork");
        let resumed_history = codex_rollout::RolloutRecorder::get_rollout_history(&forked_path)
            .await
            .expect("load copied paginated fork history");
        let resumed = thread_manager
            .resume_thread_with_history(
                test.config.clone(),
                resumed_history,
                codex_core::test_support::auth_manager_from_auth(
                    codex_login::CodexAuth::from_api_key("dummy"),
                ),
                /*parent_trace*/ None,
                ClientMcpExtensions::default(),
            )
            .await
            .expect("resume copied paginated fork")
            .thread;
        resumed
            .submit(Op::UserInput {
                items: vec![UserInput::Text {
                    text: "continue after cold resume".to_string(),
                    text_elements: Vec::new(),
                }],
                final_output_json_schema: None,
                responsesapi_client_metadata: None,
                additional_context: Default::default(),
                thread_settings: Default::default(),
            })
            .await
            .expect("start resumed turn");
        wait_for_event(&resumed, |event| matches!(event, EventMsg::TurnComplete(_))).await;
        let requests = server.received_requests().await.expect("response requests");
        let input = serde_json::to_string(
            &requests
                .last()
                .expect("resumed model request")
                .body_json::<serde_json::Value>()
                .expect("response request body")["input"],
        )
        .expect("serialize model input");
        assert!(input.contains("fork me from stored history"));
        assert!(input.contains("continue after cold resume"));
    }
}

fn read_rollout_items(path: &std::path::Path) -> Vec<RolloutItem> {
    let read_message = format!("failed to read rollout file {}", path.display());
    let text = std::fs::read_to_string(path).expect(&read_message);
    let mut items: Vec<RolloutItem> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parse_json_message = format!("failed to parse rollout JSON line `{line}`");
        let v: serde_json::Value = serde_json::from_str(line).expect(&parse_json_message);
        let parse_line_message = format!("failed to parse rollout line `{line}`");
        let rl: RolloutLine = serde_json::from_value(v).expect(&parse_line_message);
        match rl.item {
            RolloutItem::SessionMeta(_) => {}
            other => items.push(other),
        }
    }
    items
}

fn without_session_meta(items: Vec<RolloutItem>) -> Vec<RolloutItem> {
    items
        .into_iter()
        .filter(|item| !matches!(item, RolloutItem::SessionMeta(_)))
        .collect()
}
