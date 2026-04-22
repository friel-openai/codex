use anyhow::Result;
use codex_core::CodexThread;
use codex_core::ThreadManager;
use codex_exec_server::EnvironmentManager;
use codex_login::CodexAuth;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::built_in_model_providers;
use codex_models_manager::collaboration_mode_presets::CollaborationModesConfig;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::user_input::UserInput;
use core_test_support::load_default_config_for_test;
use core_test_support::responses::ResponsesRequest;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::wait_for_event;
use serde_json::Value;
use std::sync::Arc;
use tempfile::TempDir;

async fn start_thread_with_source(
    server: &wiremock::MockServer,
    session_source: SessionSource,
) -> Result<Arc<CodexThread>> {
    let codex_home = TempDir::new()?;
    let mut config = load_default_config_for_test(&codex_home).await;
    config.model_provider = ModelProviderInfo {
        base_url: Some(format!("{}/v1", server.uri())),
        supports_websockets: false,
        ..built_in_model_providers(/*openai_base_url*/ None)["openai"].clone()
    };
    config.developer_instructions = Some("Durable developer baseline.".to_string());

    let thread_manager = ThreadManager::new(
        &config,
        codex_core::test_support::auth_manager_from_auth(CodexAuth::from_api_key("dummy")),
        session_source,
        CollaborationModesConfig::default(),
        Arc::new(EnvironmentManager::new(/*exec_server_url*/ None)),
        /*analytics_events_client*/ None,
    );
    let new_thread = thread_manager.start_thread(config).await?;
    Ok(new_thread.thread)
}

async fn submit_text_and_wait(thread: &CodexThread, text: &str) -> Result<()> {
    thread
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: text.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
        })
        .await?;
    wait_for_event(thread, |ev| matches!(ev, EventMsg::TurnComplete(_))).await;
    Ok(())
}

fn input_message_text(item: &Value) -> Option<String> {
    let role = item.get("role").and_then(Value::as_str)?;
    let content = item.get("content").and_then(Value::as_array)?;
    let text = content
        .iter()
        .filter(|span| span.get("type").and_then(Value::as_str) == Some("input_text"))
        .filter_map(|span| span.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    Some(format!("{role}\n{text}"))
}

fn message_index(input: &[Value], role: &str, needle: &str) -> usize {
    input
        .iter()
        .position(|item| {
            item.get("type").and_then(Value::as_str) == Some("message")
                && item.get("role").and_then(Value::as_str) == Some(role)
                && input_message_text(item).is_some_and(|text| text.contains(needle))
        })
        .unwrap_or_else(|| panic!("{role} message containing {needle:?} not found"))
}

fn all_message_text_for_role(input: &[Value], role: &str) -> String {
    input
        .iter()
        .filter(|item| {
            item.get("type").and_then(Value::as_str) == Some("message")
                && item.get("role").and_then(Value::as_str) == Some(role)
        })
        .filter_map(input_message_text)
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_role_prompt_is_developer_item(
    request: &ResponsesRequest,
    prompt_needle: &str,
    task_needle: &str,
) -> String {
    let input = request.input();
    let prompt_idx = message_index(&input, "developer", prompt_needle);
    let task_idx = message_index(&input, "user", task_needle);
    assert!(prompt_idx < task_idx);
    assert!(!request.instructions_text().contains(prompt_needle));
    all_message_text_for_role(&input, "developer")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_subagent_and_watchdog_prompts_are_developer_items_in_responses_requests() -> Result<()>
{
    let server = start_mock_server().await;

    let root_mock = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-1"), ev_completed("resp-1")]),
    )
    .await;
    let root = start_thread_with_source(&server, SessionSource::Exec).await?;
    submit_text_and_wait(&root, "root task").await?;
    let root_request = root_mock.single_request();
    let root_developer_text = assert_role_prompt_is_developer_item(
        &root_request,
        "# You are the Root Agent",
        "root task",
    );
    assert!(root_developer_text.contains("## Watchdogs"));
    assert!(root_developer_text.contains("Durable developer baseline."));
    assert!(!root_developer_text.contains("More importantly, you are a **watchdog**"));
    assert!(
        !root_request
            .instructions_text()
            .contains("Durable developer baseline.")
    );

    let subagent_mock = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-2"), ev_completed("resp-2")]),
    )
    .await;
    let subagent = start_thread_with_source(
        &server,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::default(),
            depth: 1,
            agent_path: None,
            agent_nickname: Some("worker".to_string()),
            agent_role: None,
        }),
    )
    .await?;
    submit_text_and_wait(&subagent, "subagent task").await?;
    let subagent_request = subagent_mock.single_request();
    let subagent_developer_text = assert_role_prompt_is_developer_item(
        &subagent_request,
        "# You are a Subagent",
        "subagent task",
    );
    assert!(subagent_developer_text.contains("## Subagent Responsibilities"));
    assert!(!subagent_developer_text.contains("More importantly, you are a **watchdog**"));
    assert!(!subagent_developer_text.contains("watchdog.snooze"));

    let watchdog_mock = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-3"), ev_completed("resp-3")]),
    )
    .await;
    let watchdog = start_thread_with_source(
        &server,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::default(),
            depth: 1,
            agent_path: None,
            agent_nickname: Some("watchdog".to_string()),
            agent_role: Some("watchdog".to_string()),
        }),
    )
    .await?;
    submit_text_and_wait(&watchdog, "watchdog task").await?;
    let watchdog_request = watchdog_mock.single_request();
    let watchdog_developer_text = assert_role_prompt_is_developer_item(
        &watchdog_request,
        "More importantly, you are a **watchdog**",
        "watchdog task",
    );
    assert!(watchdog_developer_text.contains("watchdog.snooze"));
    assert!(!watchdog_developer_text.contains("## Subagent Responsibilities"));

    Ok(())
}
