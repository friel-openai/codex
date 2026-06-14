use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::write_models_cache;
use codex_app_server::in_process;
use codex_app_server::in_process::InProcessClientHandle;
use codex_app_server::in_process::InProcessServerEvent;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_config::NoopThreadConfigLoader;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_features::Feature;
use codex_feedback::CodexFeedback;
use codex_protocol::protocol::SessionSource as CoreSessionSource;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;
use wiremock::Mock;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path_regex;

const TIMEOUT: Duration = Duration::from_secs(20);
const ORIGINAL_RESPONSE_DELAY: Duration = Duration::from_secs(5);
const THREAD_ADOPTION_FEATURE_CONFIG: &str =
    "[features.multi_agent_v2]\nenabled = true\nenable_thread_adoption = true";
const ORIGINAL_PROMPT: &str = "finish the original root turn before adoption";
const ORIGINAL_ANSWER: &str = "the original root finished without interruption";
const ADOPT_PROMPT: &str = "adopt the existing root after its current turn completes";
const ADOPT_MESSAGE: &str = "continue the completed root as an adopted worker";
const ADOPT_ANSWER: &str = "the adopted worker retained the completed root history";
const ADOPT_CALL_ID: &str = "active-root-adoption-spawn-call";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_adoption_waits_for_active_root_without_interrupting_its_turn() -> Result<()> {
    let server = responses::start_mock_server().await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::Collab)
        .with_extra_config(THREAD_ADOPTION_FEATURE_CONFIG)
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;

    let mut client = start_isolated_app_server(codex_home.path()).await?;
    let original: ThreadStartResponse = request(
        &client,
        ClientRequest::ThreadStart {
            request_id: RequestId::Integer(1),
            params: ThreadStartParams::default(),
        },
    )
    .await?;
    let original_id = original.thread.id;
    let original_path = original
        .thread
        .path
        .context("the adoption source must have a durable rollout")?;

    let parent: ThreadStartResponse = request(
        &client,
        ClientRequest::ThreadStart {
            request_id: RequestId::Integer(3),
            params: ThreadStartParams::default(),
        },
    )
    .await?;
    let parent_id = parent.thread.id;

    let adopt_arguments = serde_json::to_string(&json!({
        "existing_thread_id": original_id,
        "task_name": "active_adopted_worker",
        "message": ADOPT_MESSAGE,
    }))?;
    let adoption_request = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| request_contains(request, ADOPT_PROMPT),
        responses::sse(vec![
            responses::ev_response_created("resp-active-adoption-tool"),
            responses::ev_function_call_with_namespace(
                ADOPT_CALL_ID,
                "frodex",
                "adopt_agent",
                &adopt_arguments,
            ),
            responses::ev_completed("resp-active-adoption-tool"),
        ]),
    )
    .await;
    let adopted_thread_id = original_id.clone();
    let adopted_request = responses::mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            request_contains(request, ADOPT_MESSAGE)
                && request_has_thread_id(request, &adopted_thread_id)
                && request_has_agent_message(request)
        },
        responses::sse(vec![
            responses::ev_response_created("resp-active-adoption-worker"),
            responses::ev_assistant_message("resp-active-adoption-worker", ADOPT_ANSWER),
            responses::ev_completed("resp-active-adoption-worker"),
        ]),
    )
    .await;
    let adoption_result = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| request_contains(request, ADOPT_CALL_ID),
        responses::sse(vec![
            responses::ev_response_created("resp-active-adoption-result"),
            responses::ev_assistant_message(
                "resp-active-adoption-result",
                "the active root was adopted after completing its original turn",
            ),
            responses::ev_completed("resp-active-adoption-result"),
        ]),
    )
    .await;

    let original_response = responses::sse(vec![
        responses::ev_response_created("resp-active-adoption-original"),
        responses::ev_assistant_message("resp-active-adoption-original", ORIGINAL_ANSWER),
        responses::ev_completed("resp-active-adoption-original"),
    ]);
    Mock::given(method("POST"))
        .and(path_regex(".*/responses$"))
        .and(header("thread-id", original_id.as_str()))
        .respond_with(responses::sse_response(original_response).set_delay(ORIGINAL_RESPONSE_DELAY))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    start_turn(
        &client,
        /*request_id*/ 2,
        &original_id,
        ORIGINAL_PROMPT,
    )
    .await?;
    wait_for_original_response_request(&server, &original_id).await?;

    start_turn(&client, /*request_id*/ 4, &parent_id, ADOPT_PROMPT).await?;
    wait_for_matching_mock_request(&adoption_request, "parent adoption request", |request| {
        response_has_thread_id(request, &parent_id) && request.body_contains_text(ADOPT_PROMPT)
    })
    .await?;
    let adoption_tool_request = adoption_request
        .requests()
        .into_iter()
        .find(|request| {
            response_has_thread_id(request, &parent_id) && request.body_contains_text(ADOPT_PROMPT)
        })
        .context("parent adoption request was not captured")?;
    let spawn_agent = adoption_tool_request
        .tool_by_name("collaboration", "spawn_agent")
        .context("canonical collaboration.spawn_agent was not declared")?;
    assert!(
        spawn_agent["parameters"]["properties"]
            .get("existing_thread_id")
            .is_none(),
        "collaboration.spawn_agent must keep the canonical parameter schema"
    );
    assert!(
        adoption_tool_request
            .tool_by_name("collaboration", "adopt_agent")
            .is_none()
    );
    assert!(
        adoption_tool_request
            .tool_by_name("collaboration", "promote_agent")
            .is_none()
    );
    assert!(
        adoption_tool_request
            .tool_by_name("collaboration", "close_agent")
            .is_none()
    );
    assert!(
        adoption_tool_request
            .tool_by_name("frodex", "adopt_agent")
            .is_some()
    );
    assert!(
        adoption_tool_request
            .tool_by_name("frodex", "close_agent")
            .is_some()
    );
    assert!(
        adoption_tool_request
            .tool_by_name("frodex", "promote_agent")
            .is_some()
    );

    let active_source: ThreadReadResponse = request(
        &client,
        ClientRequest::ThreadRead {
            request_id: RequestId::Integer(5),
            params: ThreadReadParams {
                thread_id: original_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    assert_eq!(active_source.thread.id, original_id);
    assert_eq!(active_source.thread.path.as_ref(), Some(&original_path));
    assert_eq!(active_source.thread.parent_thread_id, None);
    assert_eq!(active_source.thread.source, SessionSource::Cli);
    let early_adopted_requests = adopted_request
        .requests()
        .into_iter()
        .filter(|request| is_adopted_worker_response(request, &original_id))
        .map(|request| request.body_json())
        .collect::<Vec<_>>();
    assert!(
        early_adopted_requests.is_empty(),
        "adoption must not start a second Responses request while the source turn is active: {early_adopted_requests:?}"
    );

    wait_for_original_and_parent_completion(&mut client, &original_id, &parent_id).await?;
    wait_for_matching_mock_request(&adopted_request, "adopted worker assignment", |request| {
        is_adopted_worker_response(request, &original_id)
    })
    .await?;

    let output = adoption_result
        .function_call_output_text(ADOPT_CALL_ID)
        .context("adopt_agent did not return an adoption result")?;
    let output: Value = serde_json::from_str(&output)
        .with_context(|| format!("adopt_agent returned {output:?}"))?;
    assert_eq!(
        output.get("task_name").and_then(Value::as_str),
        Some("/root/active_adopted_worker")
    );
    let adopted = wait_for_adopted_history(&client, &original_id).await?;
    assert_eq!(adopted.thread.id, original_id);
    assert_eq!(adopted.thread.path.as_ref(), Some(&original_path));
    assert_eq!(
        adopted.thread.parent_thread_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert!(matches!(adopted.thread.source, SessionSource::SubAgent(_)));

    let agent_messages: Vec<&str> = adopted
        .thread
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter_map(|item| match item {
            ThreadItem::AgentMessage { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(agent_messages, vec![ORIGINAL_ANSWER, ADOPT_ANSWER]);

    client.shutdown().await?;
    Ok(())
}

async fn start_isolated_app_server(codex_home: &Path) -> Result<InProcessClientHandle> {
    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(codex_home.to_path_buf())
            .fallback_cwd(Some(codex_home.to_path_buf()))
            .loader_overrides(loader_overrides.clone())
            .build()
            .await?,
    );
    let state_db = codex_rollout::state_db::try_init(config.as_ref()).await?;

    Ok(in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config,
        cli_overrides: Vec::new(),
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        thread_config_loader: Arc::new(NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: Some(state_db),
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        config_warnings: Vec::new(),
        session_source: CoreSessionSource::Cli,
        enable_codex_api_key_env: false,
        initialize: InitializeParams {
            client_info: ClientInfo {
                name: "active-thread-adoption-test".to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                ..Default::default()
            }),
        },
        channel_capacity: in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await?)
}

async fn request<T: DeserializeOwned>(
    client: &InProcessClientHandle,
    request: ClientRequest,
) -> Result<T> {
    let response = client
        .request(request)
        .await?
        .map_err(|error| anyhow::anyhow!("isolated app-server request failed: {error:?}"))?;
    Ok(serde_json::from_value(response)?)
}

async fn start_turn(
    client: &InProcessClientHandle,
    request_id: i64,
    thread_id: &str,
    prompt: &str,
) -> Result<()> {
    let _: TurnStartResponse = request(
        client,
        ClientRequest::TurnStart {
            request_id: RequestId::Integer(request_id),
            params: TurnStartParams {
                thread_id: thread_id.to_string(),
                input: vec![UserInput::Text {
                    text: prompt.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    Ok(())
}

async fn wait_for_original_response_request(
    server: &wiremock::MockServer,
    thread_id: &str,
) -> Result<()> {
    timeout(TIMEOUT, async {
        loop {
            if server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .any(|request| request_has_thread_id(request, thread_id))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the original root did not start its delayed Responses request")?;
    Ok(())
}

async fn wait_for_original_and_parent_completion(
    client: &mut InProcessClientHandle,
    original_id: &str,
    parent_id: &str,
) -> Result<()> {
    timeout(TIMEOUT, async {
        let mut original_completed = false;
        loop {
            let event = client
                .next_event()
                .await
                .context("the isolated app server stopped before adoption completed")?;
            if let InProcessServerEvent::ServerNotification(notification) = event
                && let ServerNotification::TurnCompleted(completed) = notification.as_ref()
            {
                if completed.thread_id == original_id {
                    assert_eq!(completed.turn.status, TurnStatus::Completed);
                    original_completed = true;
                }
                if completed.thread_id == parent_id {
                    assert_eq!(completed.turn.status, TurnStatus::Completed);
                    assert!(
                        original_completed,
                        "adoption must complete the original root turn before the adopter turn"
                    );
                    return Ok::<(), anyhow::Error>(());
                }
            }
        }
    })
    .await
    .context("timed out waiting for the original and adopter turns")??;
    Ok(())
}

async fn wait_for_adopted_history(
    client: &InProcessClientHandle,
    thread_id: &str,
) -> Result<ThreadReadResponse> {
    timeout(TIMEOUT, async {
        let mut request_id = 1_000;
        loop {
            let response: ThreadReadResponse = request(
                client,
                ClientRequest::ThreadRead {
                    request_id: RequestId::Integer(request_id),
                    params: ThreadReadParams {
                        thread_id: thread_id.to_string(),
                        include_turns: true,
                    },
                },
            )
            .await?;
            if response
                .thread
                .turns
                .iter()
                .flat_map(|turn| &turn.items)
                .any(|item| {
                    matches!(item, ThreadItem::AgentMessage { text, .. } if text == ADOPT_ANSWER)
                })
            {
                return Ok::<ThreadReadResponse, anyhow::Error>(response);
            }
            request_id += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for adopted thread {thread_id} history"))?
}

async fn wait_for_matching_mock_request<F>(
    mock: &responses::ResponseMock,
    description: &str,
    matches: F,
) -> Result<()>
where
    F: Fn(&responses::ResponsesRequest) -> bool,
{
    timeout(TIMEOUT, async {
        while !mock.requests().iter().any(&matches) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| {
        let observed = mock
            .requests()
            .iter()
            .map(|request| {
                (
                    request.header("thread-id"),
                    request.header("x-client-request-id"),
                    request.body_contains_text(ORIGINAL_PROMPT),
                    request.body_contains_text(ADOPT_PROMPT),
                    request.body_contains_text(ADOPT_MESSAGE),
                    request
                        .input()
                        .iter()
                        .filter_map(|item| item.get("type").and_then(Value::as_str))
                        .map(str::to_string)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        format!("timed out waiting for {description}; observed {observed:?}")
    })?;
    Ok(())
}

fn request_contains(request: &wiremock::Request, expected: &str) -> bool {
    String::from_utf8_lossy(&request.body).contains(expected)
}

fn response_has_thread_id(request: &responses::ResponsesRequest, expected: &str) -> bool {
    request
        .header("thread-id")
        .or_else(|| request.header("x-client-request-id"))
        .as_deref()
        == Some(expected)
}

fn is_adopted_worker_response(request: &responses::ResponsesRequest, thread_id: &str) -> bool {
    response_has_thread_id(request, thread_id)
        && request.body_contains_text(ADOPT_MESSAGE)
        && request
            .input()
            .iter()
            .any(|item| item.get("type").and_then(Value::as_str) == Some("agent_message"))
}

fn request_has_thread_id(request: &wiremock::Request, expected: &str) -> bool {
    request
        .headers
        .get("thread-id")
        .or_else(|| request.headers.get("x-client-request-id"))
        .and_then(|value| value.to_str().ok())
        == Some(expected)
}

fn request_has_agent_message(request: &wiremock::Request) -> bool {
    serde_json::from_slice::<Value>(&request.body)
        .ok()
        .and_then(|body| body.get("input").and_then(Value::as_array).cloned())
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("type").and_then(Value::as_str) == Some("agent_message"))
        })
}
