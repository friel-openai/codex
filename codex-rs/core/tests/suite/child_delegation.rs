use anyhow::Result;
use codex_core::StartThreadOptions;
use codex_core::TurnInputRequest;
use codex_features::Feature;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::MultiAgentMessages;
use codex_protocol::openai_models::MultiAgentModeMessages;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::namespace_child_tool;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;
use test_case::test_case;

const DIRECT_WORK: &str = "Complete your assigned work directly.";
const PROACTIVE: &str = "Proactive multi-agent delegation is active.";
const PARENT_HINT: &str = "Delegate independent work proactively.";

/// Parent mode sources that must not become a child's delegation policy.
#[derive(Clone, Copy)]
enum ParentMode {
    Bundled,
    Configured,
    EmptyConfigured,
    Catalog,
}

#[test_case(ParentMode::Bundled; "bundled root default")]
#[test_case(ParentMode::Configured; "inherited configured hint")]
#[test_case(ParentMode::EmptyConfigured; "inherited empty hint")]
#[test_case(ParentMode::Catalog; "inherited catalog hint")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_child_defaults_to_direct_work_without_disabling_tools(
    mode: ParentMode,
) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        (1..=2)
            .map(|index| {
                sse(vec![
                    ev_response_created(&format!("resp-{index}")),
                    ev_completed(&format!("resp-{index}")),
                ])
            })
            .collect(),
    )
    .await;
    let test = test_codex()
        .with_model_info_override("gpt-5.4", move |model| {
            if matches!(mode, ParentMode::Catalog) {
                model.model_messages.as_mut().unwrap().multi_agent = Some(MultiAgentMessages {
                    mode: Some(MultiAgentModeMessages {
                        hint_text: Some(PARENT_HINT.to_owned()),
                        explicit: None,
                        proactive: None,
                    }),
                    ..Default::default()
                });
            }
        })
        .with_config(move |config| {
            config.features.enable(Feature::MultiAgentV2).unwrap();
            config.multi_agent_v2.multi_agent_mode_hint_text = match mode {
                ParentMode::Configured => Some(PARENT_HINT.to_owned()),
                ParentMode::EmptyConfigured => Some(String::new()),
                ParentMode::Bundled | ParentMode::Catalog => None,
            };
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("parent work").await?;
    let child = test
        .thread_manager
        .start_thread(StartThreadOptions {
            session_source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: test.session_configured.session_id.into(),
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            ..StartThreadOptions::new(test.config.clone())
        })
        .await?;
    child
        .thread
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "assigned child work".to_owned(),
            text_elements: Vec::new(),
        }]))
        .await?;
    wait_for_event(&child.thread, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    let requests = responses.requests();
    assert_eq!(requests.len(), 2);
    let parent_texts = requests[0].message_input_texts("developer");
    let child_texts = requests[1].message_input_texts("developer");
    let contains = |texts: &[String], text: &str| texts.iter().any(|item| item.contains(text));
    assert_eq!(
        (
            contains(&parent_texts, PROACTIVE),
            contains(&parent_texts, PARENT_HINT),
            contains(&parent_texts, DIRECT_WORK),
            contains(&child_texts, DIRECT_WORK),
            contains(&child_texts, PROACTIVE),
            contains(&child_texts, PARENT_HINT),
        ),
        (
            matches!(mode, ParentMode::Bundled),
            matches!(mode, ParentMode::Configured | ParentMode::Catalog),
            false,
            true,
            false,
            false,
        )
    );
    assert!(
        namespace_child_tool(&requests[1].body_json(), "collaboration", "spawn_agent").is_some()
    );
    child.thread.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_resumed_child_supersedes_saved_proactive_mode() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        (1..=2)
            .map(|index| {
                sse(vec![
                    ev_response_created(&format!("resp-{index}")),
                    ev_completed(&format!("resp-{index}")),
                ])
            })
            .collect(),
    )
    .await;
    let mut builder = test_codex()
        .with_history_mode(ThreadHistoryMode::Legacy)
        .with_config(|config| {
            config.features.enable(Feature::MultiAgentV2).unwrap();
        });
    let initial = builder.build_with_auto_env(&server).await?;
    initial.submit_turn("saved work").await?;
    initial.codex.shutdown_and_wait().await?;
    let rollout_path = initial.session_configured.rollout_path.as_ref().unwrap();
    let mut records = std::fs::read_to_string(rollout_path)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<serde_json::Result<Vec<_>>>()?;
    // Old children used the same proactive mode and world-state baseline as roots. Change only
    // the saved identity so resume must replace that baseline, rather than starting fresh.
    let meta = records
        .iter_mut()
        .find(|item| item["type"] == "session_meta")
        .unwrap();
    meta["payload"]["source"] =
        serde_json::to_value(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: None,
        }))?;
    let contents = records
        .iter()
        .map(serde_json::to_string)
        .collect::<serde_json::Result<Vec<_>>>()?
        .join("\n")
        + "\n";
    std::fs::write(rollout_path, contents)?;
    let resumed = test_codex()
        .with_config(|config| {
            config.features.enable(Feature::MultiAgentV2).unwrap();
        })
        .resume(&server, initial.home.clone(), rollout_path.clone())
        .await?;
    resumed.submit_turn("continue saved child work").await?;
    let requests = responses.requests();
    assert_eq!(requests.len(), 2);
    let texts = requests[1].message_input_texts("developer");
    let old_mode = texts
        .iter()
        .position(|text| text.contains(PROACTIVE))
        .unwrap();
    let new_mode = texts
        .iter()
        .position(|text| text.contains(DIRECT_WORK))
        .unwrap();
    assert!(
        old_mode < new_mode,
        "child policy must supersede inherited proactive context"
    );
    assert_eq!(
        texts
            .iter()
            .filter(|text| text.contains(DIRECT_WORK))
            .count(),
        1
    );
    Ok(())
}
