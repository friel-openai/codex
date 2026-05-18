use anyhow::Result;
use codex_config::types::ToolSuggestDiscoverable;
use codex_config::types::ToolSuggestDiscoverableType;
use codex_core::ThreadConfigSnapshot;
use codex_core::config::AgentRoleConfig;
use codex_core_plugins::OPENAI_CURATED_MARKETPLACE_NAME;
use codex_core_plugins::startup_sync::curated_plugins_repo_path;
use codex_features::Feature;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use core_test_support::responses::ResponsesRequest;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_once_match;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::sse;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::fs;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;
use wiremock::Match;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::matchers::path_regex;

const SPAWN_CALL_ID: &str = "spawn-call-1";
const TURN_0_FORK_PROMPT: &str = "seed fork context";
const TURN_1_PROMPT: &str = "spawn a child and continue";
const TURN_2_NO_WAIT_PROMPT: &str = "follow up without wait";
const CHILD_PROMPT: &str = "child: do work";
const INHERITED_MODEL: &str = "gpt-5.3-codex";
const INHERITED_REASONING_EFFORT: ReasoningEffort = ReasoningEffort::XHigh;
const REQUESTED_MODEL: &str = "gpt-5.4";
const REQUESTED_REASONING_EFFORT: ReasoningEffort = ReasoningEffort::Low;
const ROLE_MODEL: &str = "gpt-5.4";
const ROLE_REASONING_EFFORT: ReasoningEffort = ReasoningEffort::High;

#[derive(Clone)]
struct RawRequestRecorder {
    requests: Arc<Mutex<Vec<wiremock::Request>>>,
}

impl RawRequestRecorder {
    fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn single_request(&self) -> wiremock::Request {
        let requests = match self.requests.lock() {
            Ok(requests) => requests,
            Err(err) => panic!("requests lock should not panic: {err}"),
        };
        assert_eq!(requests.len(), 1);
        let Some(request) = requests.first() else {
            panic!("request should exist");
        };
        request.clone()
    }
}

impl Match for RawRequestRecorder {
    fn matches(&self, request: &wiremock::Request) -> bool {
        match self.requests.lock() {
            Ok(mut requests) => requests.push(request.clone()),
            Err(err) => panic!("requests lock should not panic: {err}"),
        }
        true
    }
}

fn body_contains(req: &wiremock::Request, text: &str) -> bool {
    request_body_text(req).is_some_and(|body| body.contains(text))
}

fn request_body_text(req: &wiremock::Request) -> Option<String> {
    let is_zstd = req
        .headers
        .get("content-encoding")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|entry| entry.trim().eq_ignore_ascii_case("zstd"))
        });
    let bytes = if is_zstd {
        zstd::stream::decode_all(std::io::Cursor::new(&req.body)).ok()
    } else {
        Some(req.body.clone())
    };
    bytes.and_then(|body| String::from_utf8(body).ok())
}

fn has_subagent_notification(req: &ResponsesRequest) -> bool {
    req.message_input_texts("user")
        .iter()
        .any(|text| text.contains("<subagent_notification>"))
}

async fn mount_fork_marker_child_response(
    server: &MockServer,
    response_body: String,
) -> RawRequestRecorder {
    let child_request_log = RawRequestRecorder::new();
    Mock::given(method("POST"))
        .and(path_regex(".*/responses$"))
        .and(|req: &wiremock::Request| {
            let body = request_body_text(req).unwrap_or_default();
            body.contains(r#""previous_response_id":"resp-turn1-1""#)
                || (body.contains("# Subagent Assignment") && body.contains(CHILD_PROMPT))
        })
        .and(child_request_log.clone())
        .respond_with(sse_response(response_body))
        .with_priority(4)
        .up_to_n_times(1)
        .mount(server)
        .await;
    child_request_log
}

fn run_large_fork_request_test<F, Fut>(name: &'static str, test: F) -> Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    // These tests intentionally send full forked requests with parent
    // developer context and tool schemas. wiremock clones and matches that
    // large request body, so use an explicit Tokio stack size instead of
    // relying on the platform default worker stack.
    let test_thread = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()?;
            runtime.block_on(test())
        })?;
    match test_thread.join() {
        Ok(result) => result,
        Err(err) => std::panic::resume_unwind(err),
    }
}

fn tool_parameter_description(
    req: &ResponsesRequest,
    tool_name: &str,
    parameter_name: &str,
) -> Option<String> {
    req.body_json()
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .and_then(|tools| {
            tools.iter().find_map(|tool| {
                if tool.get("name").and_then(serde_json::Value::as_str) == Some(tool_name) {
                    tool.get("parameters")
                        .and_then(|parameters| parameters.get("properties"))
                        .and_then(|properties| properties.get(parameter_name))
                        .and_then(|parameter| parameter.get("description"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                } else {
                    None
                }
            })
        })
}

fn tool_names(body: &Value) -> Vec<String> {
    body.get("tools")
        .and_then(Value::as_array)
        .map(|tools| {
            tools
                .iter()
                .filter_map(|tool| {
                    tool.get("name")
                        .or_else(|| tool.get("type"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn role_block(description: &str, role_name: &str) -> Option<String> {
    let role_header = format!("{role_name}: {{");
    let mut lines = description.lines().skip_while(|line| *line != role_header);
    let first_line = lines.next()?;
    let mut block = vec![first_line];
    for line in lines {
        if line.ends_with(": {") {
            break;
        }
        block.push(line);
    }
    Some(block.join("\n"))
}

fn write_home_skill(codex_home: &Path, dir: &str, name: &str, description: &str) -> Result<()> {
    let skill_dir = codex_home.join("skills").join(dir);
    fs::create_dir_all(&skill_dir)?;
    let contents = format!("---\nname: {name}\ndescription: {description}\n---\n\n# Body\n");
    fs::write(skill_dir.join("SKILL.md"), contents)?;
    Ok(())
}

fn write_discoverable_plugin(codex_home: &Path, plugin_name: &str) -> Result<()> {
    let curated_root = curated_plugins_repo_path(codex_home);
    let plugin_root = curated_root.join("plugins").join(plugin_name);
    let marketplace_plugin = format!(
        r#"{{
      "name": "{plugin_name}",
      "source": {{
        "source": "local",
        "path": "./plugins/{plugin_name}"
      }}
    }}"#
    );
    fs::create_dir_all(curated_root.join(".agents/plugins"))?;
    fs::write(
        curated_root.join(".agents/plugins/marketplace.json"),
        format!(
            r#"{{
  "name": "{OPENAI_CURATED_MARKETPLACE_NAME}",
  "plugins": [
{marketplace_plugin}
  ]
}}"#
        ),
    )?;
    fs::create_dir_all(plugin_root.join(".codex-plugin"))?;
    fs::write(
        plugin_root.join(".codex-plugin/plugin.json"),
        format!(
            r#"{{
  "name": "{plugin_name}",
  "description": "Plugin suggested only through request_plugin_install"
}}"#
        ),
    )?;
    Ok(())
}

async fn mount_empty_apps_directory(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/connectors/directory/list"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "apps": [],
            "nextToken": null
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/connectors/directory/list_workspace"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "apps": [],
            "nextToken": null
        })))
        .mount(server)
        .await;
}

async fn wait_for_spawned_thread_id(test: &TestCodex) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let ids = test.thread_manager.list_thread_ids().await;
        if let Some(spawned_id) = ids
            .iter()
            .find(|id| **id != test.session_configured.thread_id)
        {
            return Ok(spawned_id.to_string());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for spawned thread id");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_requests(
    mock: &core_test_support::responses::ResponseMock,
) -> Result<Vec<ResponsesRequest>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let requests = mock.requests();
        if !requests.is_empty() {
            return Ok(requests);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("expected at least 1 request, got {}", requests.len());
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn setup_turn_one_with_spawned_child(
    server: &MockServer,
    child_response_delay: Option<Duration>,
) -> Result<(TestCodex, String)> {
    setup_turn_one_with_custom_spawned_child(
        server,
        json!({
            "message": CHILD_PROMPT,
        }),
        child_response_delay,
        /*wait_for_parent_notification*/ true,
        |builder| builder,
    )
    .await
}

async fn setup_turn_one_with_custom_spawned_child(
    server: &MockServer,
    spawn_args: serde_json::Value,
    child_response_delay: Option<Duration>,
    wait_for_parent_notification: bool,
    configure_test: impl FnOnce(
        core_test_support::test_codex::TestCodexBuilder,
    ) -> core_test_support::test_codex::TestCodexBuilder,
) -> Result<(TestCodex, String)> {
    let spawn_args = serde_json::to_string(&spawn_args)?;

    mount_sse_once_match(
        server,
        |req: &wiremock::Request| body_contains(req, TURN_1_PROMPT),
        sse(vec![
            ev_response_created("resp-turn1-1"),
            ev_function_call(SPAWN_CALL_ID, "spawn_agent", &spawn_args),
            ev_completed("resp-turn1-1"),
        ]),
    )
    .await;

    let child_sse = sse(vec![
        ev_response_created("resp-child-1"),
        ev_assistant_message("msg-child-1", "child done"),
        ev_completed("resp-child-1"),
    ]);
    let child_request_log = if let Some(delay) = child_response_delay {
        mount_response_once_match(
            server,
            |req: &wiremock::Request| {
                body_contains(req, CHILD_PROMPT) && !body_contains(req, SPAWN_CALL_ID)
            },
            sse_response(child_sse).set_delay(delay),
        )
        .await
    } else {
        mount_sse_once_match(
            server,
            |req: &wiremock::Request| {
                body_contains(req, CHILD_PROMPT) && !body_contains(req, SPAWN_CALL_ID)
            },
            child_sse,
        )
        .await
    };

    let _turn1_followup = mount_sse_once_match(
        server,
        |req: &wiremock::Request| body_contains(req, SPAWN_CALL_ID),
        sse(vec![
            ev_response_created("resp-turn1-2"),
            ev_assistant_message("msg-turn1-2", "parent done"),
            ev_completed("resp-turn1-2"),
        ]),
    )
    .await;

    #[allow(clippy::expect_used)]
    let mut builder = configure_test(test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::Collab)
            .expect("test config should allow feature update");
        config.model = Some(INHERITED_MODEL.to_string());
        config.model_reasoning_effort = Some(INHERITED_REASONING_EFFORT);
    }));
    let test = builder.build(server).await?;
    test.submit_turn(TURN_1_PROMPT).await?;
    if child_response_delay.is_none() && wait_for_parent_notification {
        let _ = wait_for_requests(&child_request_log).await?;
        let rollout_path = test
            .codex
            .rollout_path()
            .ok_or_else(|| anyhow::anyhow!("expected parent rollout path"))?;
        let deadline = Instant::now() + Duration::from_secs(6);
        loop {
            let has_notification = tokio::fs::read_to_string(&rollout_path)
                .await
                .is_ok_and(|rollout| rollout.contains("<subagent_notification>"));
            if has_notification {
                break;
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for parent rollout to include subagent notification"
                );
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
    let spawned_id = wait_for_spawned_thread_id(&test).await?;

    Ok((test, spawned_id))
}

async fn spawn_child_and_capture_snapshot(
    server: &MockServer,
    spawn_args: serde_json::Value,
    configure_test: impl FnOnce(
        core_test_support::test_codex::TestCodexBuilder,
    ) -> core_test_support::test_codex::TestCodexBuilder,
) -> Result<ThreadConfigSnapshot> {
    let (test, spawned_id) = setup_turn_one_with_custom_spawned_child(
        server,
        spawn_args,
        /*child_response_delay*/ None,
        /*wait_for_parent_notification*/ false,
        configure_test,
    )
    .await?;
    let thread_id = ThreadId::from_string(&spawned_id)?;
    Ok(test
        .thread_manager
        .get_thread(thread_id)
        .await?
        .config_snapshot()
        .await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subagent_notification_is_included_without_wait() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let (test, _spawned_id) =
        setup_turn_one_with_spawned_child(&server, /*child_response_delay*/ None).await?;

    let turn2 = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, TURN_2_NO_WAIT_PROMPT),
        sse(vec![
            ev_response_created("resp-turn2-1"),
            ev_assistant_message("msg-turn2-1", "no wait path"),
            ev_completed("resp-turn2-1"),
        ]),
    )
    .await;
    test.submit_turn(TURN_2_NO_WAIT_PROMPT).await?;

    let turn2_requests = wait_for_requests(&turn2).await?;
    assert!(turn2_requests.iter().any(has_subagent_notification));

    Ok(())
}

#[test]
fn spawned_child_receives_forked_parent_context() -> Result<()> {
    run_large_fork_request_test(
        "spawned_child_receives_forked_parent_context",
        spawned_child_receives_forked_parent_context_impl,
    )
}

async fn spawned_child_receives_forked_parent_context_impl() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;

    let seed_turn = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, TURN_0_FORK_PROMPT),
        sse(vec![
            ev_response_created("resp-seed-1"),
            ev_assistant_message("msg-seed-1", "seeded"),
            ev_completed("resp-seed-1"),
        ]),
    )
    .await;

    let spawn_args = serde_json::to_string(&json!({
        "task_name": "child",
        "message": CHILD_PROMPT,
        "fork_turns": "all",
    }))?;
    let spawn_turn = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, TURN_1_PROMPT),
        sse(vec![
            ev_response_created("resp-turn1-1"),
            ev_function_call(SPAWN_CALL_ID, "spawn_agent", &spawn_args),
            ev_completed("resp-turn1-1"),
        ]),
    )
    .await;

    let _turn1_followup = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, SPAWN_CALL_ID),
        sse(vec![
            ev_response_created("resp-turn1-2"),
            ev_assistant_message("msg-turn1-2", "parent done"),
            ev_completed("resp-turn1-2"),
        ]),
    )
    .await;

    let mut builder = test_codex().with_config(|config| {
        if let Err(err) = config.features.enable(Feature::Collab) {
            panic!("test config should allow feature update: {err}");
        }
    });
    let test = builder.build(&server).await?;
    let child_request_log = mount_fork_marker_child_response(
        &server,
        sse(vec![
            ev_response_created("resp-child-1"),
            ev_assistant_message("msg-child-1", "child done"),
            ev_completed("resp-child-1"),
        ]),
    )
    .await;

    test.submit_turn(TURN_0_FORK_PROMPT).await?;
    let _ = seed_turn.single_request();

    test.submit_turn(TURN_1_PROMPT).await?;
    let _ = spawn_turn.single_request();

    let child_request = child_request_log.single_request();
    let child_body = request_body_text(&child_request)
        .ok_or_else(|| anyhow::anyhow!("child request body should be text"))?;
    assert!(
        child_body.contains(TURN_0_FORK_PROMPT)
            || child_body.contains(r#""previous_response_id":"resp-turn1-1""#),
        "forked child should either inline parent context or continue from the parent response"
    );
    Ok(())
}

#[test]
fn spawned_child_inherits_parent_app_server_client_tool_filters() -> Result<()> {
    run_large_fork_request_test(
        "spawned_child_inherits_parent_app_server_client_tool_filters",
        spawned_child_inherits_parent_app_server_client_tool_filters_impl,
    )
}

async fn spawned_child_inherits_parent_app_server_client_tool_filters_impl() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    mount_empty_apps_directory(&server).await;
    let spawn_args = serde_json::to_string(&json!({
        "message": CHILD_PROMPT,
        "fork_turns": "all",
    }))?;
    let spawn_turn = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, TURN_1_PROMPT),
        sse(vec![
            ev_response_created("resp-turn1-1"),
            ev_function_call(SPAWN_CALL_ID, "spawn_agent", &spawn_args),
            ev_completed("resp-turn1-1"),
        ]),
    )
    .await;
    let child_turn = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| {
            body_contains(req, CHILD_PROMPT) && !body_contains(req, SPAWN_CALL_ID)
        },
        sse(vec![
            ev_response_created("resp-child-1"),
            ev_completed("resp-child-1"),
        ]),
    )
    .await;
    let _turn1_followup = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, SPAWN_CALL_ID),
        sse(vec![
            ev_response_created("resp-turn1-2"),
            ev_completed("resp-turn1-2"),
        ]),
    )
    .await;

    let plugin_name = "slack";
    let chatgpt_base_url = server.uri();
    let mut builder = test_codex()
        .with_pre_build_hook(move |codex_home| {
            write_discoverable_plugin(codex_home, plugin_name)
                .expect("discoverable plugin fixture should be written");
        })
        .with_config(move |config| {
            config
                .features
                .enable(Feature::Apps)
                .expect("test config should allow feature update");
            config
                .features
                .enable(Feature::Plugins)
                .expect("test config should allow feature update");
            config
                .features
                .enable(Feature::ToolSuggest)
                .expect("test config should allow feature update");
            config.chatgpt_base_url = chatgpt_base_url;
            config.tool_suggest.discoverables = vec![ToolSuggestDiscoverable {
                kind: ToolSuggestDiscoverableType::Plugin,
                id: format!("{plugin_name}@{OPENAI_CURATED_MARKETPLACE_NAME}"),
            }];
        });
    let test = builder.build(&server).await?;
    test.codex
        .set_app_server_client_info(
            Some("codex-tui".to_string()),
            Some("test-client-version".to_string()),
            false,
        )
        .await?;

    test.submit_turn(TURN_1_PROMPT).await?;
    let root_tool_names = tool_names(&spawn_turn.single_request().body_json());
    let child_requests = wait_for_requests(&child_turn).await?;
    let child_tool_names = tool_names(&child_requests[0].body_json());

    // `request_plugin_install` is controlled by client-sensitive discoverable-tool
    // filtering. Forked children must inherit the parent client metadata so the
    // backend-visible tool list stays byte-compatible with the parent request.
    assert_eq!(child_tool_names, root_tool_names);
    assert!(
        !root_tool_names
            .iter()
            .any(|name| name == "request_plugin_install"),
        "codex-tui should filter plugin install suggestions out of both requests: {root_tool_names:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_agent_requested_model_and_reasoning_override_inherited_settings_without_role()
-> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let child_snapshot = spawn_child_and_capture_snapshot(
        &server,
        json!({
            "message": CHILD_PROMPT,
            "model": REQUESTED_MODEL,
            "reasoning_effort": REQUESTED_REASONING_EFFORT,
        }),
        |builder| builder,
    )
    .await?;

    assert_eq!(child_snapshot.model, REQUESTED_MODEL);
    assert_eq!(
        child_snapshot.reasoning_effort,
        Some(REQUESTED_REASONING_EFFORT)
    );

    Ok(())
}

#[test]
fn spawned_multi_agent_v2_child_inherits_parent_developer_context() -> Result<()> {
    run_large_fork_request_test(
        "spawned_multi_agent_v2_child_inherits_parent_developer_context",
        spawned_multi_agent_v2_child_inherits_parent_developer_context_impl,
    )
}

async fn spawned_multi_agent_v2_child_inherits_parent_developer_context_impl() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let spawn_args = serde_json::to_string(&json!({
        "message": CHILD_PROMPT,
        "task_name": "worker",
    }))?;
    mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, TURN_1_PROMPT),
        sse(vec![
            ev_response_created("resp-turn1-1"),
            ev_function_call(SPAWN_CALL_ID, "spawn_agent", &spawn_args),
            ev_completed("resp-turn1-1"),
        ]),
    )
    .await;

    let mut builder = test_codex().with_config(|config| {
        if let Err(err) = config.features.enable(Feature::Collab) {
            panic!("test config should allow feature update: {err}");
        }
        if let Err(err) = config.features.enable(Feature::MultiAgentV2) {
            panic!("test config should allow feature update: {err}");
        }
        config.developer_instructions = Some("Parent developer instructions.".to_string());
    });
    let test = builder.build(&server).await?;
    let child_request_log = mount_fork_marker_child_response(
        &server,
        sse(vec![
            ev_response_created("resp-child-1"),
            ev_completed("resp-child-1"),
        ]),
    )
    .await;

    let _turn1_followup = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| {
            body_contains(req, "function_call_output") && body_contains(req, "/root/worker")
        },
        sse(vec![
            ev_response_created("resp-turn1-2"),
            ev_assistant_message("msg-turn1-2", "parent done"),
            ev_completed("resp-turn1-2"),
        ]),
    )
    .await;

    test.submit_turn(TURN_1_PROMPT).await?;

    let child_request = child_request_log.single_request();
    let child_body = request_body_text(&child_request)
        .ok_or_else(|| anyhow::anyhow!("child request body should be text"))?;
    assert!(
        child_body.contains("Parent developer instructions.")
            || child_body.contains(r#""previous_response_id":"resp-turn1-1""#),
        "forked child should either inline parent developer context or continue from the parent response"
    );
    assert!(child_body.contains(CHILD_PROMPT));

    Ok(())
}

#[test]
fn skills_toggle_skips_instructions_for_parent_and_spawned_child() -> Result<()> {
    run_large_fork_request_test(
        "skills_toggle_skips_instructions_for_parent_and_spawned_child",
        skills_toggle_skips_instructions_for_parent_and_spawned_child_impl,
    )
}

async fn skills_toggle_skips_instructions_for_parent_and_spawned_child_impl() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let spawn_args = serde_json::to_string(&json!({
        "message": CHILD_PROMPT,
        "task_name": "worker",
    }))?;
    let spawn_turn = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, TURN_1_PROMPT),
        sse(vec![
            ev_response_created("resp-turn1-1"),
            ev_function_call(SPAWN_CALL_ID, "spawn_agent", &spawn_args),
            ev_completed("resp-turn1-1"),
        ]),
    )
    .await;

    let _child_request_log = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, CHILD_PROMPT),
        sse(vec![
            ev_response_created("resp-child-1"),
            ev_completed("resp-child-1"),
        ]),
    )
    .await;

    let mut builder = test_codex()
        .with_pre_build_hook(|home| {
            if let Err(err) = write_home_skill(home, "demo", "demo-skill", "demo skill") {
                panic!("write home skill: {err}");
            }
        })
        .with_config(|config| {
            if let Err(err) = config.features.enable(Feature::Collab) {
                panic!("test config should allow feature update: {err}");
            }
            if let Err(err) = config.features.enable(Feature::MultiAgentV2) {
                panic!("test config should allow feature update: {err}");
            }
            config.include_skill_instructions = false;
        });
    let test = builder.build(&server).await?;
    let _turn1_followup = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| {
            body_contains(req, "function_call_output") && body_contains(req, "/root/worker")
        },
        sse(vec![
            ev_response_created("resp-turn1-2"),
            ev_assistant_message("msg-turn1-2", "parent done"),
            ev_completed("resp-turn1-2"),
        ]),
    )
    .await;

    let child_request_log = mount_fork_marker_child_response(
        &server,
        sse(vec![
            ev_response_created("resp-child-1"),
            ev_completed("resp-child-1"),
        ]),
    )
    .await;

    test.submit_turn(TURN_1_PROMPT).await?;
    let parent_request = spawn_turn.single_request();
    assert!(!parent_request.body_contains_text("<skills_instructions>"));
    assert!(!parent_request.body_contains_text("demo-skill"));

    let child_request = child_request_log.single_request();
    assert!(!body_contains(&child_request, "<skills_instructions>"));
    assert!(!body_contains(&child_request, "demo-skill"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_agent_role_overrides_requested_model_and_reasoning_settings() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let child_snapshot = spawn_child_and_capture_snapshot(
        &server,
        json!({
            "message": CHILD_PROMPT,
            "agent_type": "custom",
            "model": REQUESTED_MODEL,
            "reasoning_effort": REQUESTED_REASONING_EFFORT,
        }),
        |builder| {
            builder.with_config(|config| {
                let role_path = config.codex_home.join("custom-role.toml");
                std::fs::write(
                    &role_path,
                    format!(
                        "model = \"{ROLE_MODEL}\"\nmodel_reasoning_effort = \"{ROLE_REASONING_EFFORT}\"\n",
                    ),
                )
                .expect("write role config");
                config.agent_roles.insert(
                    "custom".to_string(),
                    AgentRoleConfig {
                        description: Some("Custom role".to_string()),
                        config_file: Some(role_path.to_path_buf()),
                        nickname_candidates: None,
                    },
                );
            })
        },
    )
    .await?;

    assert_eq!(child_snapshot.model, ROLE_MODEL);
    assert_eq!(child_snapshot.reasoning_effort, Some(ROLE_REASONING_EFFORT));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_agent_tool_description_mentions_role_locked_settings() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let resp_mock = mount_sse_once_match(
        &server,
        |req: &wiremock::Request| body_contains(req, TURN_1_PROMPT),
        sse(vec![
            ev_response_created("resp-turn1-1"),
            ev_assistant_message("msg-turn1-1", "done"),
            ev_completed("resp-turn1-1"),
        ]),
    )
    .await;

    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::Collab)
            .expect("test config should allow feature update");
        let role_path = config.codex_home.join("custom-role.toml");
        std::fs::write(
            &role_path,
            format!(
                "developer_instructions = \"Stay focused\"\nmodel = \"{ROLE_MODEL}\"\nmodel_reasoning_effort = \"{ROLE_REASONING_EFFORT}\"\n",
            ),
        )
        .expect("write role config");
        config.agent_roles.insert(
            "custom".to_string(),
            AgentRoleConfig {
                description: Some("Custom role".to_string()),
                config_file: Some(role_path.to_path_buf()),
                nickname_candidates: None,
            },
        );
    });
    let test = builder.build(&server).await?;

    test.submit_turn(TURN_1_PROMPT).await?;

    let request = resp_mock.single_request();
    let agent_type_description = tool_parameter_description(&request, "spawn_agent", "agent_type")
        .expect("spawn_agent agent_type description");
    let custom_role_description =
        role_block(&agent_type_description, "custom").expect("custom role description");
    assert_eq!(
        custom_role_description,
        "custom: {\nCustom role\n- This role's model is set to `gpt-5.4` and its reasoning effort is set to `high`. These settings cannot be changed.\n}"
    );

    Ok(())
}
