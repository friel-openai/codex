use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::PathBufExt;
use app_test_support::TestAppServer;
use app_test_support::create_fake_rollout;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence;
use app_test_support::rollout_path;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadClosedNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadItemPayload;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadUnsubscribeParams;
use codex_app_server_protocol::ThreadUnsubscribeResponse;
use codex_app_server_protocol::ThreadUnsubscribeStatus;
use codex_app_server_protocol::TurnEnvironmentParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput as V2UserInput;
use codex_exec_server::LOCAL_ENVIRONMENT_ID;
use codex_features::Feature;
use codex_protocol::ThreadId;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::EnvironmentConfigState;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TurnEnvironmentRequest;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;
use codex_shell_command::shell_detect::ShellType;
use codex_shell_command::shell_detect::detect_shell_type;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::LegacyAppPathString;
use codex_utils_path_uri::PathUri;
use core_test_support::responses;
use futures::SinkExt;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use super::exec_server_test_support::accept_exec_server_environment;
use super::exec_server_test_support::read_exec_server_json;

const AGENTS_INSTRUCTIONS: &str = "selected environment workspace instructions";
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
// Finish before an awaited remote instruction discovery could expire.
const LOCAL_CONVERSATION_TIMEOUT: Duration = Duration::from_secs(5);
const SAVED_USER_MESSAGE: &str = "Saved environment selection";
// Three stalled lookups share one 10-second deadline, not three sequential deadlines.
const BOUNDED_RESUME_TIMEOUT: Duration = Duration::from_secs(20);
const OPTIONAL_VERIFICATION_LOG_FILTER: &str = "warn,codex_core::environment_selection=debug";

fn write_mock_config(codex_home: &Path, server_uri: &str) -> std::io::Result<()> {
    MockResponsesConfig::new(server_uri)
        .with_root_config("compact_prompt = \"compact\"\nmodel_auto_compact_token_limit = 100000")
        .with_provider_config("supports_websockets = false")
        .write(codex_home)
}

fn text_turn_params(thread_id: String, prompt: &str) -> TurnStartParams {
    TurnStartParams {
        thread_id,
        input: vec![V2UserInput::Text {
            text: prompt.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    }
}

async fn wait_for_optional_secondary_verification(
    app: &TestAppServer,
    environment_id: &str,
    previous_events: &[serde_json::Value],
    verified: bool,
) -> Result<()> {
    let message = if verified {
        "optional secondary executor verified"
    } else {
        "optional secondary executor verification failed"
    };
    app.wait_for_matching_json_log_event(
        &format!("`{message}` for `{environment_id}`"),
        |event| {
            event["fields"]["message"] == message
                && event["fields"]["environment_id"] == environment_id
                && !previous_events.contains(event)
        },
        DEFAULT_READ_TIMEOUT,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn thread_start_reports_selected_environment_metadata() -> Result<()> {
    let server = responses::start_mock_server().await;
    let codex_home = TempDir::new()?;
    write_mock_config(codex_home.path(), &server.uri())?;
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let selected_workspace_roots = app_server
        .auto_env()?
        .selection()
        .workspace_roots
        .iter()
        .filter_map(|root| root.to_abs_path().ok())
        .collect::<Vec<_>>();

    let ThreadStartResponse {
        cwd,
        runtime_workspace_roots,
        active_permission_profile,
        ..
    } = app_server
        .start_thread(ThreadStartParams::default())
        .await?;
    let host_cwd = codex_home.path().to_path_buf().abs().canonicalize()?;
    let cwd = cwd.canonicalize()?;
    assert_eq!(
        (cwd, runtime_workspace_roots, active_permission_profile),
        (
            // TODO(anp): Return the selected environment's native cwd from thread/start.
            host_cwd,
            selected_workspace_roots,
            // TODO(anp): Report the implicit built-in permission profile instead of None.
            None,
        )
    );

    Ok(())
}

#[tokio::test]
async fn thread_start_reports_selected_environment_instruction_source() -> Result<()> {
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_assistant_message("msg-1", "done"),
            responses::ev_completed("resp-1"),
        ]),
    )
    .await;
    let codex_home = TempDir::new()?;
    write_mock_config(codex_home.path(), &server.uri())?;
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let (agents_source, environment_cwd) = {
        let auto_env = app_server.auto_env()?;
        let environment_cwd = auto_env.selection().cwd.clone();
        let agents_source = environment_cwd.join("AGENTS.md")?;
        auto_env
            .environment()
            .get_filesystem()
            .write_file(
                &agents_source,
                AGENTS_INSTRUCTIONS.as_bytes().to_vec(),
                Default::default(),
                /*sandbox*/ None,
            )
            .await?;
        (agents_source, environment_cwd)
    };

    let response = app_server
        .start_thread(ThreadStartParams::default())
        .await?;

    assert_eq!(response.instruction_sources, vec![agents_source.into()]);
    timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(text_turn_params(
            response.thread.id,
            "inspect workspace instructions",
        )),
    )
    .await??;

    let user_context = response_mock.single_request().message_input_texts("user");
    let instructions = user_context
        .iter()
        .find(|text| text.starts_with("# AGENTS.md instructions"))
        .context("selected environment instructions should be model visible")?;
    let expected_instructions = format!(
        "# AGENTS.md instructions for {}\n\n<INSTRUCTIONS>\n{AGENTS_INSTRUCTIONS}\n</INSTRUCTIONS>",
        environment_cwd.inferred_native_path_string()
    );
    assert_eq!(instructions, &expected_instructions);

    Ok(())
}

#[tokio::test]
async fn thread_start_keeps_healthy_defaults_when_secondary_instruction_discovery_stalls()
-> Result<()> {
    use codex_app_server_protocol::ThreadReadParams;
    use codex_app_server_protocol::ThreadReadResponse;
    use codex_app_server_protocol::TurnStatus;

    const THREAD_START_TIMEOUT: Duration = Duration::from_secs(20);
    const LOCAL_INSTRUCTIONS: &str = "local workspace instructions";
    const PROJECTLESS_INSTRUCTIONS: &str = "projectless workspace instructions";
    const REMAINING_INSTRUCTIONS: &str = "instructions from the unaffected secondary";
    const QUARANTINED_EXEC_CALL: &str = "exec-on-quarantined-secondary";

    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_sequence(
        &server,
        [
            "explicit-environment-echo",
            "implicit-environments",
            "projectless-composer",
            "move-back-and-verify-secondaries",
        ]
        .into_iter()
        .map(|id| {
            responses::sse(vec![
                responses::ev_response_created(id),
                responses::ev_assistant_message(id, "done"),
                responses::ev_completed(id),
            ])
        })
        .chain([
            responses::sse(vec![
                responses::ev_response_created("quarantined-exec"),
                responses::ev_exec_command_call_with_args(
                    QUARANTINED_EXEC_CALL,
                    &json!({"cmd": "echo must-not-start", "environment_id": "healthy", "login": false}),
                ),
                responses::ev_completed("quarantined-exec"),
            ]),
            create_final_assistant_message_sse_response("continued in the local workspace")?,
        ])
        .collect(),
    )
    .await;
    let codex_home = TempDir::new()?;
    let workspace_dir = TempDir::new()?;
    let workspace = workspace_dir.path().to_path_buf().abs().canonicalize()?;
    std::fs::write(workspace.join("AGENTS.md"), LOCAL_INSTRUCTIONS)?;
    let workspace_uri = PathUri::from_abs_path(&workspace);
    let local_source = workspace_uri.join("AGENTS.md")?;
    let healthy_source = workspace_uri.join("AGENTS.override.md")?;
    let remaining_source = workspace_uri.join("AGENTS.remaining.md")?;

    let healthy = TcpListener::bind("127.0.0.1:0").await?;
    let remaining = TcpListener::bind("127.0.0.1:0").await?;
    let stalled = TcpListener::bind("127.0.0.1:0").await?;
    std::fs::write(
        codex_home.path().join("environments.toml"),
        format!(
            "[[environments]]\nid = \"healthy\"\nurl = \"ws://{}\"\n\n\
             [[environments]]\nid = \"remaining\"\nurl = \"ws://{}\"\n\n\
             [[environments]]\nid = \"stalled\"\nurl = \"ws://{}\"\n",
            healthy.local_addr()?,
            remaining.local_addr()?,
            stalled.local_addr()?,
        ),
    )?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("danger-full-access")
        .with_provider_config("supports_websockets = false")
        .with_root_config("project_doc_fallback_filenames = [\"AGENTS.remaining.md\"]")
        .with_extra_config(&format!(
            "[mcp_servers.stalled_process]\ncommand = \"must-not-start\"\n\
             cwd = {}\nenvironment_id = \"stalled\"\nrequired = true\n",
            json!(workspace.to_string_lossy()),
        ))
        .write(codex_home.path())?;

    let (stop_healthy, healthy_stop) = oneshot::channel();
    let (stop_remaining, remaining_stop) = oneshot::channel();
    let (stop_stalled, stalled_stop) = oneshot::channel();
    let (fail_healthy_metadata, healthy_metadata_failure) = oneshot::channel();
    let (healthy_metadata_error, healthy_metadata_error_seen) = oneshot::channel();
    let healthy_server = tokio::spawn(serve_quarantine_executor(
        healthy,
        Some((healthy_source.clone(), AGENTS_INSTRUCTIONS)),
        Some((healthy_metadata_failure, healthy_metadata_error)),
        healthy_stop,
    ));
    let remaining_server = tokio::spawn(serve_quarantine_executor(
        remaining,
        Some((remaining_source.clone(), REMAINING_INSTRUCTIONS)),
        None,
        remaining_stop,
    ));
    let stalled_server = tokio::spawn(serve_quarantine_executor(stalled, None, None, stalled_stop));
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .with_json_logging(OPTIONAL_VERIFICATION_LOG_FILTER)
        .build_initialized()
        .await?;
    let start = ThreadStartParams {
        cwd: Some(workspace.to_string_lossy().into_owned()),
        ephemeral: Some(true),
        ..Default::default()
    };
    let denied = TurnEnvironmentParams {
        environment_id: "stalled".to_string(),
        cwd: LegacyAppPathString::from_path(workspace.as_path()),
        runtime_workspace_roots: None,
    };
    let other_workspace_dir = TempDir::new()?;
    let other_workspace = other_workspace_dir.path().canonicalize()?;
    let projectless_cwd = other_workspace.join("project");
    std::fs::create_dir(&projectless_cwd)?;
    std::fs::write(projectless_cwd.join("AGENTS.md"), PROJECTLESS_INSTRUCTIONS)?;

    let mut projectless_thread = None;
    for (index, label) in ["Desktop echo", "cwd only", "projectless composer"]
        .into_iter()
        .enumerate()
    {
        let previous_events = app_server.json_log_events()?;
        let request_id = app_server.send_thread_start_request(start.clone()).await?;
        let response: ThreadStartResponse = timeout(
            LOCAL_CONVERSATION_TIMEOUT,
            app_server.read_response(request_id),
        )
        .await??;
        let thread_id = response.thread.id;
        let mut reported = response
            .thread
            .environments
            .context("thread/start should report its selected environments")?;
        let mut reported_ids = reported
            .iter()
            .map(|environment| environment.environment_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(reported_ids.first().copied(), Some(LOCAL_ENVIRONMENT_ID));
        reported_ids.sort_unstable();
        assert_eq!(
            reported_ids,
            ["healthy", LOCAL_ENVIRONMENT_ID, "remaining", "stalled"],
        );
        assert_eq!(
            response.instruction_sources,
            [LegacyAppPathString::from(local_source.clone())]
        );
        for environment_id in ["healthy", "remaining"] {
            wait_for_optional_secondary_verification(
                &app_server,
                environment_id,
                &previous_events,
                /*verified*/ true,
            )
            .await?;
        }
        let projectless = index == 2;
        let local_cwd = if projectless {
            // The projectless composer changes only the primary local entry.
            reported[0].cwd = LegacyAppPathString::from_path(&projectless_cwd);
            reported[0].runtime_workspace_roots =
                vec![LegacyAppPathString::from_path(&other_workspace)];
            projectless_cwd.clone()
        } else {
            workspace.to_path_buf()
        };
        let echoed: Vec<_> = reported
            .iter()
            .cloned()
            .map(|environment| TurnEnvironmentParams {
                environment_id: environment.environment_id,
                cwd: environment.cwd,
                runtime_workspace_roots: Some(environment.runtime_workspace_roots),
            })
            .collect();
        let stalled_index = echoed
            .iter()
            .position(|environment| environment.environment_id == "stalled")
            .context("thread/start should report its unavailable secondary")?;
        let completed = timeout(
            DEFAULT_READ_TIMEOUT,
            app_server.start_turn_and_wait_for_completion(TurnStartParams {
                cwd: Some(local_cwd.clone()),
                environments: (index != 1).then(|| echoed.clone()),
                ..text_turn_params(thread_id.clone(), label)
            }),
        )
        .await??;
        assert_eq!(completed.turn.status, TurnStatus::Completed);
        let requests = response_mock.requests();
        assert_eq!(requests.len(), index + 1);
        let input = requests[index].message_input_texts("user");
        let local_instructions = if projectless {
            PROJECTLESS_INSTRUCTIONS
        } else {
            LOCAL_INSTRUCTIONS
        };
        for expected in [
            local_instructions,
            AGENTS_INSTRUCTIONS,
            REMAINING_INSTRUCTIONS,
        ] {
            assert!(
                input.iter().any(|text| text.contains(expected)),
                "{input:?}"
            );
        }

        if projectless {
            assert!(!input.iter().any(|text| text.contains(LOCAL_INSTRUCTIONS)));
            let request_id = app_server
                .send_thread_read_request(ThreadReadParams {
                    thread_id: thread_id.clone(),
                    include_turns: false,
                })
                .await?;
            let updated: ThreadReadResponse =
                timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
            assert_eq!(updated.thread.environments, Some(reported));

            let mut promoted = echoed.clone();
            promoted.swap(0, stalled_index);
            let mut moved = echoed.clone();
            moved[0].cwd = LegacyAppPathString::from_path(workspace.as_path());
            moved[stalled_index].cwd = LegacyAppPathString::from_path(&other_workspace);
            let mut rerooted = echoed.clone();
            rerooted[stalled_index].runtime_workspace_roots =
                Some(vec![LegacyAppPathString::from_path(&other_workspace)]);
            for (label, cwd, environments) in [
                (
                    "request only the stalled executor",
                    local_cwd.clone(),
                    vec![echoed[stalled_index].clone()],
                ),
                ("promote the stalled executor", local_cwd.clone(), promoted),
                (
                    "move the stalled executor cwd",
                    workspace.to_path_buf(),
                    moved,
                ),
                (
                    "change the stalled executor roots",
                    local_cwd.clone(),
                    rerooted,
                ),
                (
                    "change the fallback cwd",
                    other_workspace.clone(),
                    echoed.clone(),
                ),
            ] {
                let denied_id = environments
                    .iter()
                    .find(|environment| environment.environment_id != LOCAL_ENVIRONMENT_ID)
                    .context("an explicit request for an unavailable secondary")?
                    .environment_id
                    .clone();
                let request_id = app_server
                    .send_turn_start_request(TurnStartParams {
                        cwd: Some(cwd),
                        environments: Some(environments),
                        ..text_turn_params(thread_id.clone(), label)
                    })
                    .await?;
                let error = timeout(
                    DEFAULT_READ_TIMEOUT,
                    app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
                )
                .await??;
                assert_eq!(error.error.code, -32600, "{label}");
                assert_eq!(
                    error.error.message,
                    format!(
                        "invalid thread settings override: invalid value for `environments`: `{denied_id}` is not in the allowed set owner-provided environment configuration (set by <unspecified>)"
                    ),
                    "{label}"
                );
            }
            assert_eq!(response_mock.requests().len(), index + 1);
            projectless_thread = Some((thread_id, echoed));
        }
    }

    let request_id = app_server
        .send_thread_start_request(ThreadStartParams {
            environments: Some(vec![denied]),
            ..start.clone()
        })
        .await?;
    let error = timeout(
        THREAD_START_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert!(
        error.error.message.contains(
            "Environment `stalled` did not complete executor readiness and AGENTS.md discovery."
        ),
        "{error:?}",
    );

    let (thread_id, mut environments) = projectless_thread.context("projectless thread")?;
    let healthy_selection = environments
        .iter()
        .find(|environment| environment.environment_id == "healthy")
        .context("the healthy secondary should still be selected")?
        .clone();
    let original_local_path = LegacyAppPathString::from_path(workspace.as_path());
    environments[0].cwd = original_local_path.clone();
    environments[0].runtime_workspace_roots = Some(vec![original_local_path]);
    let previous_events = app_server.json_log_events()?;
    fail_healthy_metadata
        .send(())
        .expect("healthy fixture is still running");
    let primed = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(TurnStartParams {
            cwd: Some(workspace.to_path_buf()),
            environments: Some(environments.clone()),
            ..text_turn_params(
                thread_id.clone(),
                "Move back to the original local workspace",
            )
        }),
    )
    .await??;
    assert_eq!(primed.turn.status, TurnStatus::Completed);
    timeout(DEFAULT_READ_TIMEOUT, healthy_metadata_error_seen).await??;
    wait_for_optional_secondary_verification(
        &app_server,
        "remaining",
        &previous_events,
        /*verified*/ true,
    )
    .await?;
    let completed = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Try the quarantined executor, then continue locally",
        )),
    )
    .await??;
    assert_eq!(completed.turn.status, TurnStatus::Completed);

    let requests = responses::received_responses_requests(&server).await;
    assert_eq!(requests.len(), 6);
    assert!(
        requests[4].body_json()["tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "exec_command"))
    );
    let instructions = requests[4].message_input_texts("user");
    let root = workspace_uri.inferred_native_path_string();
    let expected = format!(
        "# AGENTS.md instructions\n\n<INSTRUCTIONS>\nThese AGENTS.md instructions replace all previously provided AGENTS.md instructions.\n\nfor `local` with root {root}\n\n{LOCAL_INSTRUCTIONS}\n\nfor `remaining` with root {root}\n\n{REMAINING_INSTRUCTIONS}\n</INSTRUCTIONS>",
    );
    assert_eq!(
        instructions
            .iter()
            .rfind(|text| text.starts_with("# AGENTS.md instructions")),
        Some(&expected),
    );
    let (output, success) = requests[5]
        .function_call_output_content_and_success(QUARANTINED_EXEC_CALL)
        .context("quarantined exec_command output")?;
    let output = output.context("quarantined exec_command denial text")?;
    assert_ne!(success, Some(true));
    assert_eq!(
        output,
        "No usable execution environment is available. Wait for an environment to become available before using this tool.",
    );
    let request_id = app_server
        .send_thread_read_request(ThreadReadParams {
            thread_id,
            include_turns: false,
        })
        .await?;
    let saved: ThreadReadResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    let saved = saved.thread.environments.context("saved environments")?;
    assert_eq!(
        saved
            .iter()
            .map(|selection| (
                &selection.environment_id,
                &selection.cwd,
                &selection.runtime_workspace_roots
            ))
            .collect::<Vec<_>>(),
        environments
            .iter()
            .map(|selection| (
                &selection.environment_id,
                &selection.cwd,
                selection
                    .runtime_workspace_roots
                    .as_ref()
                    .expect("explicit roots")
            ))
            .collect::<Vec<_>>(),
    );

    let request_id = app_server
        .send_thread_start_request(ThreadStartParams {
            environments: Some(vec![healthy_selection]),
            ..start
        })
        .await?;
    let error = timeout(
        THREAD_START_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert!(
        error.error.message.contains("Environment `healthy`")
            && error.error.message.contains("AGENTS.md"),
        "{error:?}",
    );
    assert_eq!(
        responses::received_responses_requests(&server).await.len(),
        6
    );

    let _ = stop_healthy.send(());
    let _ = stop_remaining.send(());
    let _ = stop_stalled.send(());
    timeout(DEFAULT_READ_TIMEOUT, healthy_server).await???;
    timeout(DEFAULT_READ_TIMEOUT, remaining_server).await???;
    let stalled_metadata_requests = timeout(DEFAULT_READ_TIMEOUT, stalled_server).await???;
    assert!(stalled_metadata_requests > 0);
    Ok(())
}

#[tokio::test]
async fn later_turn_denies_stdin_for_terminal_in_quarantined_default_secondary() -> Result<()> {
    use codex_app_server_protocol::ThreadReadParams;
    use codex_app_server_protocol::ThreadReadResponse;
    use codex_app_server_protocol::TurnStatus;

    const REMOTE_ID: &str = "quarantined";
    const OPEN_CALL: &str = "open-remote-terminal";
    const WRITE_CALL: &str = "write-to-prior-terminal";
    const LOCAL_CALL: &str = "exec-in-local-workspace";
    const LOCAL_MARKER: &str = "local-terminal-quarantine-marker";
    const LOCAL_INSTRUCTIONS: &str = "controller workspace instructions";
    const REMAINING_INSTRUCTIONS: &str = "unaffected secondary workspace instructions";

    let server = responses::start_mock_server().await;
    let warm = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("open-terminal"),
                responses::ev_exec_command_call_with_args(
                    OPEN_CALL,
                    &json!({
                        "cmd": "cat",
                        "environment_id": REMOTE_ID,
                        "login": false,
                        "tty": true,
                        "yield_time_ms": 250,
                    }),
                ),
                responses::ev_completed("open-terminal"),
            ]),
            create_final_assistant_message_sse_response("remote terminal is open")?,
        ],
    )
    .await;
    let codex_home = TempDir::new()?;
    let workspace_dir = TempDir::new()?;
    let workspace = workspace_dir.path().to_path_buf().abs().canonicalize()?;
    std::fs::write(workspace.join("AGENTS.md"), LOCAL_INSTRUCTIONS)?;
    let workspace_uri = PathUri::from_abs_path(&workspace);
    let local_source = workspace_uri.join("AGENTS.md")?;
    let remote_source = workspace_uri.join("AGENTS.override.md")?;
    let remaining_source = workspace_uri.join("AGENTS.remaining.md")?;
    let remote = TcpListener::bind("127.0.0.1:0").await?;
    let remaining = TcpListener::bind("127.0.0.1:0").await?;
    std::fs::write(
        codex_home.path().join("environments.toml"),
        format!(
            "[[environments]]\nid = \"{REMOTE_ID}\"\nurl = \"ws://{}\"\n\n\
             [[environments]]\nid = \"remaining\"\nurl = \"ws://{}\"\n",
            remote.local_addr()?,
            remaining.local_addr()?,
        ),
    )?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("danger-full-access")
        .with_provider_config("supports_websockets = false")
        .with_root_config("project_doc_fallback_filenames = [\"AGENTS.remaining.md\"]")
        // Exercise the quarantine check even when terminal input needs no approval.
        .disable_feature(Feature::WriteStdinApproval)
        .write(codex_home.path())?;
    let (stop_remote, remote_stop) = oneshot::channel();
    let (stop_remaining, remaining_stop) = oneshot::channel();
    let (enable_fault, fault) = oneshot::channel();
    let (metadata_error, metadata_error_seen) = oneshot::channel();
    let (terminal_events, mut terminal_requests) = mpsc::unbounded_channel();
    let remote_server = tokio::spawn(serve_quarantine_executor_with_terminal(
        remote,
        Some((remote_source.clone(), AGENTS_INSTRUCTIONS)),
        Some((fault, metadata_error)),
        Some((OPEN_CALL, terminal_events)),
        remote_stop,
    ));
    let remaining_server = tokio::spawn(serve_quarantine_executor(
        remaining,
        Some((remaining_source.clone(), REMAINING_INSTRUCTIONS)),
        None,
        remaining_stop,
    ));
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .with_json_logging(OPTIONAL_VERIFICATION_LOG_FILTER)
        .build_initialized()
        .await?;
    let start_id = app
        .send_thread_start_request(ThreadStartParams {
            cwd: Some(workspace.to_string_lossy().into_owned()),
            ephemeral: Some(true),
            ..Default::default()
        })
        .await?;
    let start: ThreadStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(start_id)).await??;
    let thread_id = start.thread.id;
    let saved_environments = start.thread.environments.context("default selections")?;
    let mut reported_ids = saved_environments
        .iter()
        .map(|selection| selection.environment_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(reported_ids.first().copied(), Some(LOCAL_ENVIRONMENT_ID));
    reported_ids.sort_unstable();
    assert_eq!(reported_ids, [LOCAL_ENVIRONMENT_ID, REMOTE_ID, "remaining"]);
    assert_eq!(
        start.instruction_sources,
        [LegacyAppPathString::from(local_source)]
    );
    for environment_id in [REMOTE_ID, "remaining"] {
        wait_for_optional_secondary_verification(&app, environment_id, &[], /*verified*/ true)
            .await?;
    }
    let opened = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Open a terminal on the secondary executor.",
        )),
    )
    .await??;
    assert_eq!(opened.turn.status, TurnStatus::Completed);
    let requests = warm.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .message_input_texts("user")
            .iter()
            .any(|text| text.contains(AGENTS_INSTRUCTIONS))
    );
    let (output, success) = requests[1]
        .function_call_output_content_and_success(OPEN_CALL)
        .context("remote terminal output")?;
    assert_ne!(success, Some(false));
    let session_id: i32 = output
        .context("remote terminal response text")?
        .lines()
        .find_map(|line| line.strip_prefix("Process running with session ID "))
        .context("the remote terminal must still be running")?
        .parse()?;
    let remote_start = timeout(DEFAULT_READ_TIMEOUT, terminal_requests.recv())
        .await?
        .context("remote process/start RPC")?;
    assert_eq!(remote_start["method"], "process/start");
    assert_eq!(remote_start["params"]["metadata"]["threadId"], thread_id);
    assert_eq!(remote_start["params"]["metadata"]["toolCallId"], OPEN_CALL);
    assert_eq!(remote_start["params"]["cwd"], json!(workspace_uri));
    assert_eq!(remote_start["params"]["tty"], true);
    assert!(
        remote_start["params"]["processId"]
            .as_str()
            .context("remote process ID")?
            .starts_with(&format!("{session_id}-"))
    );

    let later = responses::mount_sse_sequence(
        &server,
        vec![
            create_final_assistant_message_sse_response("checking executor availability")?,
            responses::sse(vec![
                responses::ev_response_created("write-to-terminal"),
                responses::ev_function_call(
                    WRITE_CALL,
                    "write_stdin",
                    &json!({"session_id": session_id, "chars": "must-not-write\n"}).to_string(),
                ),
                responses::ev_completed("write-to-terminal"),
            ]),
            responses::sse(vec![
                responses::ev_response_created("continue-locally"),
                responses::ev_exec_command_call_with_args(
                    LOCAL_CALL,
                    &json!({"cmd": format!("echo {LOCAL_MARKER}"), "environment_id": "local", "login": false}),
                ),
                responses::ev_completed("continue-locally"),
            ]),
            create_final_assistant_message_sse_response("continued locally")?,
        ],
    )
    .await;
    let previous_events = app.json_log_events()?;
    enable_fault
        .send(())
        .expect("remote RPC is still connected");
    let primed = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Check whether the secondary executors are still available.",
        )),
    )
    .await??;
    assert_eq!(primed.turn.status, TurnStatus::Completed);
    timeout(DEFAULT_READ_TIMEOUT, metadata_error_seen).await??;
    wait_for_optional_secondary_verification(
        &app,
        "remaining",
        &previous_events,
        /*verified*/ true,
    )
    .await?;
    let completed = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Try the old terminal, then continue in the local workspace.",
        )),
    )
    .await??;
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    let requests = later.requests();
    assert_eq!(requests.len(), 4);
    let instructions = requests[1].message_input_texts("user");
    let latest = instructions
        .iter()
        .rfind(|text| text.starts_with("# AGENTS.md instructions"))
        .context("replacement instructions")?;
    assert!(latest.contains(LOCAL_INSTRUCTIONS), "{latest}");
    assert!(latest.contains(REMAINING_INSTRUCTIONS), "{latest}");
    assert!(!latest.contains(AGENTS_INSTRUCTIONS), "{latest}");
    let (output, success) = requests[2]
        .function_call_output_content_and_success(WRITE_CALL)
        .context("write_stdin output")?;
    assert_ne!(success, Some(true));
    assert_eq!(
        output.as_deref(),
        Some("The terminal's environment `quarantined` is unavailable for this turn."),
    );
    let (output, success) = requests[3]
        .function_call_output_content_and_success(LOCAL_CALL)
        .context("local continuation output")?;
    assert_ne!(success, Some(false));
    assert!(output.context("local output text")?.contains(LOCAL_MARKER));
    let request_id = app
        .send_thread_read_request(ThreadReadParams {
            thread_id,
            include_turns: false,
        })
        .await?;
    let saved: ThreadReadResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(saved.thread.environments, Some(saved_environments));

    stop_remote.send(()).expect("remote RPC is still connected");
    stop_remaining
        .send(())
        .expect("remaining RPC is still connected");
    timeout(DEFAULT_READ_TIMEOUT, remote_server).await???;
    timeout(DEFAULT_READ_TIMEOUT, remaining_server).await???;
    assert!(
        matches!(
            terminal_requests.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ),
        "the executor must see no writes to the old terminal"
    );
    Ok(())
}

#[tokio::test]
async fn initially_unavailable_default_secondary_joins_an_ordinary_later_turn() -> Result<()> {
    check_initial_default_secondary_admission(/*replace_registration*/ false).await
}

#[tokio::test]
async fn initially_unavailable_default_secondary_rejects_replaced_registration() -> Result<()> {
    check_initial_default_secondary_admission(/*replace_registration*/ true).await
}

async fn check_initial_default_secondary_admission(replace_registration: bool) -> Result<()> {
    use codex_app_server_protocol::EnvironmentAddResponse;
    use codex_app_server_protocol::EnvironmentInfoResponse;
    use codex_app_server_protocol::EnvironmentStatusKind;
    use codex_app_server_protocol::EnvironmentStatusResponse;
    use codex_app_server_protocol::SandboxPolicy;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;
    use tokio_tungstenite::tungstenite::handshake::server::Request;
    use tokio_tungstenite::tungstenite::handshake::server::Response;
    use tokio_tungstenite::tungstenite::http::StatusCode;
    use tokio_util::task::AbortOnDropHandle;

    const REMOTE_ID: &str = "initially-unavailable";
    const EARLY_CALL: &str = "exec-before-first-attachment";
    const LATER_CALL: &str = "exec-after-first-attachment";
    const LOCAL_INSTRUCTIONS: &str = "local instructions for the initial offline turn";
    const REMOTE_INSTRUCTIONS: &str = "instructions from the newly attached default executor";
    const UNAVAILABLE: &str = "No usable execution environment is available. Wait for an environment to become available before using this tool.";

    let server = responses::start_mock_server().await;
    let call = |id: &str| {
        responses::sse(vec![
            responses::ev_response_created(id),
            responses::ev_exec_command_call_with_args(
                id,
                &json!({
                    "cmd": "cat",
                    "environment_id": REMOTE_ID,
                    "login": false,
                    "tty": true,
                    "yield_time_ms": 250,
                }),
            ),
            responses::ev_completed(id),
        ])
    };
    let mock = responses::mount_sse_sequence(
        &server,
        vec![
            call(EARLY_CALL),
            create_final_assistant_message_sse_response("continued locally")?,
            create_final_assistant_message_sse_response("checking the registered executor")?,
            call(LATER_CALL),
            create_final_assistant_message_sse_response(if replace_registration {
                "continued locally after replacement"
            } else {
                "remote terminal opened"
            })?,
        ],
    )
    .await;
    let codex_home = TempDir::new()?;
    let workspace_dir = TempDir::new()?;
    let workspace = workspace_dir.path().to_path_buf().abs().canonicalize()?;
    std::fs::write(workspace.join("AGENTS.md"), LOCAL_INSTRUCTIONS)?;
    let workspace_uri = PathUri::from_abs_path(&workspace);
    let local_source = workspace_uri.join("AGENTS.md")?;
    let remote_source = workspace_uri.join("AGENTS.override.md")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let original_addr = listener.local_addr()?;
    std::fs::write(
        codex_home.path().join("environments.toml"),
        format!("[[environments]]\nid = \"{REMOTE_ID}\"\nurl = \"ws://{original_addr}\"\n"),
    )?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("workspace-write")
        .with_approval_policy("never")
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::UnifiedExec)
        .with_extra_config(&format!(
            "[projects.{}]\ntrust_level = \"trusted\"\n",
            json!(workspace.to_string_lossy()),
        ))
        .write(codex_home.path())?;

    let (allow_attach, mut attach_allowed) = oneshot::channel();
    let original = AbortOnDropHandle::new(tokio::spawn(async move {
        // Only the same-registration case releases this listener to accept upgrades.
        loop {
            let (stream, _) = tokio::select! {
                biased;
                allow = &mut attach_allowed => {
                    allow?;
                    return Ok::<_, anyhow::Error>(listener);
                }
                connection = listener.accept() => connection?,
            };
            let rejected = accept_hdr_async(stream, |_: &Request, _: Response| {
                let mut response = ErrorResponse::new(None);
                *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                Err(response)
            })
            .await;
            assert!(rejected.is_err(), "initial WebSocket must not attach");
        }
    }));

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .with_mock_chatgpt_backend()
        .without_auto_env()
        .with_json_logging(OPTIONAL_VERIFICATION_LOG_FILTER)
        .build_initialized()
        .await?;
    let request_id = app
        .send_thread_start_request(ThreadStartParams {
            cwd: Some(workspace.to_string_lossy().into_owned()),
            ephemeral: Some(true),
            ..Default::default()
        })
        .await?;
    let started: ThreadStartResponse =
        timeout(BOUNDED_RESUME_TIMEOUT, app.read_response(request_id)).await??;
    assert!(matches!(
        started.sandbox,
        SandboxPolicy::WorkspaceWrite { .. }
    ));
    assert_eq!(
        started.instruction_sources,
        [LegacyAppPathString::from(local_source)]
    );
    let selections = started.thread.environments.context("default selections")?;
    assert_eq!(
        selections
            .iter()
            .map(|environment| environment.environment_id.as_str())
            .collect::<Vec<_>>(),
        [LOCAL_ENVIRONMENT_ID, REMOTE_ID],
    );
    let thread_id = started.thread.id;
    wait_for_optional_secondary_verification(&app, REMOTE_ID, &[], /*verified*/ false).await?;
    let previous_events = app.json_log_events()?;
    let first = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Continue while the default secondary is unavailable.",
        )),
    )
    .await??;
    assert_eq!(first.turn.status, TurnStatus::Completed);
    let requests = mock.requests();
    assert_eq!(requests.len(), 2);
    let input = requests[0].message_input_texts("user");
    assert!(input.iter().any(|text| text.contains(LOCAL_INSTRUCTIONS)));
    assert!(!input.iter().any(|text| text.contains(REMOTE_INSTRUCTIONS)));
    let (output, success) = requests[1]
        .function_call_output_content_and_success(EARLY_CALL)
        .context("denial of exec on the unattached executor")?;
    assert_ne!(success, Some(true));
    assert_eq!(output.as_deref(), Some(UNAVAILABLE));
    wait_for_optional_secondary_verification(
        &app,
        REMOTE_ID,
        &previous_events,
        /*verified*/ false,
    )
    .await?;

    let request_id = app
        .send_raw_request(
            "environment/status",
            Some(json!({"environmentId": REMOTE_ID})),
        )
        .await?;
    let status: EnvironmentStatusResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(status.status, EnvironmentStatusKind::Disconnected);
    assert!(
        status
            .error
            .as_deref()
            .is_some_and(|error| error.contains("503"))
    );
    let ready_listener = if replace_registration {
        TcpListener::bind("127.0.0.1:0").await?
    } else {
        allow_attach
            .send(())
            .expect("executor is still rejecting upgrades");
        timeout(DEFAULT_READ_TIMEOUT, original).await???
    };
    let ready_addr = ready_listener.local_addr()?;
    let (stop, stopped) = oneshot::channel();
    let (terminal_events, mut terminal_requests) = mpsc::unbounded_channel();
    let remote = AbortOnDropHandle::new(tokio::spawn(serve_quarantine_executor_with_terminal(
        ready_listener,
        Some((remote_source, REMOTE_INSTRUCTIONS)),
        /*metadata_fault*/ None,
        Some((LATER_CALL, terminal_events)),
        stopped,
    )));
    if replace_registration {
        assert_ne!(ready_addr, original_addr);
        let request_id = app
            .send_raw_request(
                "environment/add",
                Some(json!({
                    "environmentId": REMOTE_ID,
                    "execServerUrl": format!("ws://{ready_addr}"),
                })),
            )
            .await?;
        let _: EnvironmentAddResponse =
            timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    } else {
        assert_eq!(ready_addr, original_addr);
    }
    // Connect without sending any thread setting or selection update.
    let request_id = app
        .send_raw_request(
            "environment/info",
            Some(json!({"environmentId": REMOTE_ID})),
        )
        .await?;
    let info: EnvironmentInfoResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(info.shell.name, "bash");
    assert_eq!(info.shell.path, "/bin/bash");

    let primed = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Check the default executor without using it yet.",
        )),
    )
    .await??;
    assert_eq!(primed.turn.status, TurnStatus::Completed);
    if !replace_registration {
        wait_for_optional_secondary_verification(&app, REMOTE_ID, &[], /*verified*/ true).await?;
    }

    let later = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Use the recovered default secondary in the same workspace.",
        )),
    )
    .await??;
    assert_eq!(later.turn.status, TurnStatus::Completed);
    assert_ne!(first.turn.id, later.turn.id);
    let requests = mock.requests();
    assert_eq!(requests.len(), 5);
    let priming_input = requests[2].message_input_texts("user");
    assert!(
        !priming_input
            .iter()
            .any(|text| text.contains(REMOTE_INSTRUCTIONS))
    );
    let input = requests[3].message_input_texts("user");
    let instructions = input
        .iter()
        .rfind(|text| text.starts_with("# AGENTS.md instructions"))
        .context("current workspace instructions")?;
    assert!(instructions.contains(LOCAL_INSTRUCTIONS));
    if replace_registration {
        assert!(
            requests[2..]
                .iter()
                .flat_map(|request| request.message_input_texts("user"))
                .all(|text| !text.contains(REMOTE_INSTRUCTIONS)),
            "the replacement's instructions must not reach this thread"
        );
        let (output, success) = requests[4]
            .function_call_output_content_and_success(LATER_CALL)
            .context("denial of exec on the replacement registration")?;
        assert_ne!(success, Some(true));
        assert_eq!(output.as_deref(), Some(UNAVAILABLE));
        stop.send(()).expect("replacement is still connected");
        timeout(DEFAULT_READ_TIMEOUT, remote).await???;
        assert!(
            matches!(
                terminal_requests.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ),
            "the replacement must not receive process/start"
        );
        return Ok(());
    }
    assert!(instructions.contains(REMOTE_INSTRUCTIONS), "{instructions}");
    let (output, success) = requests[4]
        .function_call_output_content_and_success(LATER_CALL)
        .context("exec output from the newly attached executor")?;
    assert_ne!(success, Some(false));
    let session_id: i32 = output
        .context("remote terminal output")?
        .lines()
        .find_map(|line| line.strip_prefix("Process running with session ID "))
        .context("expected an open terminal on the newly attached executor")?
        .parse()?;
    let command = timeout(DEFAULT_READ_TIMEOUT, terminal_requests.recv())
        .await?
        .context("remote process/start RPC")?;
    assert_eq!(command["method"], "process/start");
    assert_eq!(command["params"]["metadata"]["threadId"], thread_id);
    assert_eq!(command["params"]["metadata"]["toolCallId"], LATER_CALL);
    assert_eq!(command["params"]["cwd"], json!(workspace_uri));
    assert_eq!(
        command["params"]["sandbox"]["permissions"]["type"],
        "managed"
    );
    assert!(
        command["params"]["processId"]
            .as_str()
            .context("remote process ID")?
            .starts_with(&format!("{session_id}-"))
    );
    stop.send(()).expect("executor is still connected");
    timeout(DEFAULT_READ_TIMEOUT, remote).await???;
    Ok(())
}

#[tokio::test]
async fn turn_start_fails_when_required_remote_instruction_refresh_fails() -> Result<()> {
    use codex_app_server_protocol::TurnCompletedNotification;
    use codex_app_server_protocol::TurnStartResponse;
    use codex_app_server_protocol::TurnStatus;

    const REMOTE_ID: &str = "required-remote";

    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_once(
        &server,
        create_final_assistant_message_sse_response("Read the workspace instructions")?,
    )
    .await;
    let codex_home = TempDir::new()?;
    let workspace_dir = TempDir::new()?;
    let workspace = workspace_dir.path().to_path_buf().abs().canonicalize()?;
    let instruction_source = PathUri::from_abs_path(&workspace).join("AGENTS.override.md")?;
    let additional_root = workspace.join("additional-root");
    std::fs::create_dir(&additional_root)?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    std::fs::write(
        codex_home.path().join("environments.toml"),
        format!(
            "[[environments]]\nid = \"{REMOTE_ID}\"\nurl = \"ws://{}\"\n",
            listener.local_addr()?,
        ),
    )?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("danger-full-access")
        .with_provider_config("supports_websockets = false")
        .write(codex_home.path())?;
    let (enable_fault, fault) = oneshot::channel();
    let (metadata_error, metadata_error_seen) = oneshot::channel();
    let (stop, stopped) = oneshot::channel();
    let executor = tokio::spawn(serve_quarantine_executor(
        listener,
        Some((instruction_source.clone(), AGENTS_INSTRUCTIONS)),
        Some((fault, metadata_error)),
        stopped,
    ));

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let cwd = LegacyAppPathString::from_abs_path(&workspace);
    let mut selection = TurnEnvironmentParams {
        environment_id: REMOTE_ID.to_string(),
        cwd: cwd.clone(),
        runtime_workspace_roots: Some(vec![cwd.clone()]),
    };
    let request_id = app
        .send_thread_start_request(ThreadStartParams {
            cwd: Some(workspace.to_string_lossy().into_owned()),
            environments: Some(vec![selection.clone()]),
            ephemeral: Some(true),
            ..Default::default()
        })
        .await?;
    let response: ThreadStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(
        response.instruction_source_path_uris(),
        vec![instruction_source],
    );
    let thread_id = response.thread.id;
    let first = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(text_turn_params(
            thread_id.clone(),
            "Inspect the required remote workspace.",
        )),
    )
    .await??;
    assert_eq!(first.turn.status, TurnStatus::Completed);
    assert!(
        response_mock
            .single_request()
            .message_input_texts("user")
            .iter()
            .any(|text| text.contains(AGENTS_INSTRUCTIONS))
    );

    // Changing the roots invalidates the instruction cache without changing the AGENTS.md candidate.
    selection.runtime_workspace_roots = Some(vec![
        cwd.clone(),
        LegacyAppPathString::from_path(&additional_root),
    ]);
    enable_fault.send(()).expect("executor fixture is running");
    let request_id = app
        .send_turn_start_request(TurnStartParams {
            environments: Some(vec![
                selection,
                TurnEnvironmentParams {
                    environment_id: LOCAL_ENVIRONMENT_ID.to_string(),
                    cwd: cwd.clone(),
                    runtime_workspace_roots: Some(vec![cwd]),
                },
            ]),
            ..text_turn_params(thread_id.clone(), "Use the changed workspace roots.")
        })
        .await?;
    let started: TurnStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(started.turn.status, TurnStatus::InProgress);
    assert_ne!(started.turn.id, first.turn.id);
    timeout(DEFAULT_READ_TIMEOUT, metadata_error_seen).await??;
    let completed: TurnCompletedNotification = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_notification("turn/completed"),
    )
    .await??;
    assert_eq!(completed.thread_id, thread_id);
    assert_eq!(completed.turn.id, started.turn.id);
    assert_eq!(completed.turn.status, TurnStatus::Failed);
    let error = completed.turn.error.context("required remote turn error")?;
    assert!(
        error.message.contains(&format!(
            "failed to load AGENTS.md instructions for environment `{REMOTE_ID}`"
        )) && error
            .message
            .contains("test executor cannot read AGENTS.override.md metadata"),
        "{}",
        error.message,
    );
    assert_eq!(
        responses::received_responses_requests(&server).await.len(),
        1
    );

    let _ = stop.send(());
    timeout(DEFAULT_READ_TIMEOUT, executor).await???;
    Ok(())
}

async fn serve_quarantine_executor(
    listener: TcpListener,
    instruction: Option<(PathUri, &'static str)>,
    metadata_fault: Option<(oneshot::Receiver<()>, oneshot::Sender<()>)>,
    stop: oneshot::Receiver<()>,
) -> Result<usize> {
    serve_quarantine_executor_with_terminal(
        listener,
        instruction,
        metadata_fault,
        /*terminal*/ None,
        stop,
    )
    .await
}

async fn serve_quarantine_executor_with_terminal(
    listener: TcpListener,
    instruction: Option<(PathUri, &'static str)>,
    metadata_fault: Option<(oneshot::Receiver<()>, oneshot::Sender<()>)>,
    terminal: Option<(&'static str, mpsc::UnboundedSender<serde_json::Value>)>,
    mut stop: oneshot::Receiver<()>,
) -> Result<usize> {
    let (mut enable_metadata_fault, mut metadata_error) = metadata_fault.unzip();
    let mut metadata_fault_enabled = false;
    let environment_info = json!({
        "shell": {"name": "bash", "path": "/bin/bash"},
        "capabilities": {"environmentConfigRead": true},
    });
    let mut websocket = accept_exec_server_environment(listener, environment_info.clone()).await?;
    let (instruction_source, contents) = instruction.unzip();
    let instruction_path = serde_json::to_value(&instruction_source)?;
    let mut stalled_metadata_requests = 0;
    loop {
        let request = tokio::select! {
            biased;
            request = read_exec_server_json(&mut websocket) => request?,
            _ = &mut stop => return Ok(stalled_metadata_requests),
        };
        let id = &request["id"];
        let is_instruction_path = request["params"]["path"] == instruction_path;
        let result = match request["method"].as_str() {
            Some("environment/info") => environment_info.clone(),
            Some("fs/getMetadata") if instruction_source.is_none() => {
                stalled_metadata_requests += 1;
                continue;
            }
            Some("fs/getMetadata")
                if is_instruction_path
                    && (metadata_fault_enabled
                        || enable_metadata_fault
                            .as_mut()
                            .is_some_and(|enable| enable.try_recv().is_ok())) =>
            {
                metadata_fault_enabled = true;
                let response = json!({
                    "id": id,
                    "error": {
                        "code": -32000,
                        "message": "test executor cannot read AGENTS.override.md metadata",
                    },
                });
                websocket
                    .send(Message::Text(response.to_string().into()))
                    .await?;
                if let Some(observed) = metadata_error.take() {
                    observed
                        .send(())
                        .expect("test is waiting for the AGENTS.md failure");
                }
                continue;
            }
            Some("fs/getMetadata") => json!({
                "isDirectory": false,
                "isFile": is_instruction_path,
                "isSymlink": false,
                "size": if is_instruction_path { contents.context("instruction contents")?.len() } else { 0 },
                "createdAtMs": 0,
                "modifiedAtMs": 0,
            }),
            Some("fs/readFile") if is_instruction_path => {
                json!({"dataBase64": BASE64_STANDARD.encode(contents.context("instruction contents")?)})
            }
            Some("environmentConfig/read") if instruction_source.is_none() => {
                anyhow::bail!("unexpected MCP discovery on the stalled executor: {request}")
            }
            Some("environmentConfig/read") => json!({
                "codexHomeDir": "file:///codex-home",
                "config": {"layers": [], "cloudInsertionIndex": 0},
                "requirements": {"layers": [], "cloudInsertionIndex": 0},
            }),
            Some("process/start") => {
                let (expected_call, requests) = terminal
                    .as_ref()
                    .with_context(|| format!("unexpected process/start: {request}"))?;
                assert_eq!(request["params"]["metadata"]["toolCallId"], *expected_call);
                requests.send(request.clone())?;
                json!({"processId": request["params"]["processId"], "sandboxType": "none"})
            }
            Some("process/write") if terminal.is_some() => {
                let (_, requests) = terminal.as_ref().context("terminal request recorder")?;
                requests.send(request.clone())?;
                json!({"status": "accepted"})
            }
            Some("process/read") if terminal.is_some() => json!({
                "chunks": [],
                "nextSeq": request["params"]["afterSeq"].as_u64().unwrap_or_default(),
                "exited": false,
                "closed": false,
            }),
            Some("process/terminate") if terminal.is_some() => json!({"running": false}),
            _ => {
                let error = json!({"code": -32601, "message": "not supported by test executor"});
                let response = json!({"id": id, "error": error});
                websocket
                    .send(Message::Text(response.to_string().into()))
                    .await?;
                continue;
            }
        };
        let response = json!({"id": id, "result": result});
        websocket
            .send(Message::Text(response.to_string().into()))
            .await?;
    }
}

#[tokio::test]
async fn ordinary_cold_resume_and_sends_do_not_wait_for_saved_secondaries() -> Result<()> {
    for by_path in [false, true] {
        cold_resume_and_sends_do_not_wait_for_saved_secondaries(by_path)
            .await
            .with_context(|| format!("cold resume with explicit path: {by_path}"))?;
    }
    Ok(())
}

async fn cold_resume_and_sends_do_not_wait_for_saved_secondaries(by_path: bool) -> Result<()> {
    const STALLED: &str = "stalled";
    const MISSING: &str = "retired";
    const FIRST_CALL: &str = "exec-on-stalled-secondary";
    const LATER_CALL: &str = "exec-on-retired-secondary";
    const LOCAL_INSTRUCTIONS: &str = "local instructions for the resumed thread";
    const UNAVAILABLE: &str = "No usable execution environment is available. Wait for an environment to become available before using this tool.";

    let server = responses::start_mock_server().await;
    let call = |id: &str, environment_id: &str| {
        responses::sse(vec![
            responses::ev_response_created(id),
            responses::ev_exec_command_call_with_args(
                id,
                &json!({"cmd": "echo must-not-run", "environment_id": environment_id, "login": false}),
            ),
            responses::ev_completed(id),
        ])
    };
    let mock = responses::mount_sse_sequence(
        &server,
        vec![
            call(FIRST_CALL, STALLED),
            create_final_assistant_message_sse_response("continued locally on first send")?,
            call(LATER_CALL, MISSING),
            create_final_assistant_message_sse_response("continued locally on later send")?,
            create_final_assistant_message_sse_response("continued in the new local workspace")?,
        ],
    )
    .await;
    let codex_home = TempDir::new()?;
    let workspace_dir = TempDir::new()?;
    let workspace = workspace_dir.path().to_path_buf().abs().canonicalize()?;
    std::fs::write(workspace.join("AGENTS.md"), LOCAL_INSTRUCTIONS)?;
    let workspace_uri = PathUri::from_abs_path(&workspace);
    let local_source = workspace_uri.join("AGENTS.md")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    std::fs::write(
        codex_home.path().join("environments.toml"),
        format!(
            "[[environments]]\nid = {STALLED:?}\nurl = \"ws://{}\"\n",
            listener.local_addr()?
        ),
    )?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .with_sandbox_mode("danger-full-access")
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::UnifiedExec)
        .write(codex_home.path())?;
    let (stop, stopped) = oneshot::channel();
    let requests = Arc::new(InstructionDiscoveryRequests::default());
    let executor = tokio::spawn(serve_instruction_discovery_executor(
        listener,
        InstructionDiscoveryExecutor::Stalled,
        Arc::clone(&requests),
        stopped,
    ));
    let selections = [LOCAL_ENVIRONMENT_ID, STALLED, MISSING];
    let thread_id = seed_resume_with_saved_environments(
        codex_home.path(),
        &workspace,
        &selections,
        /*implicit_selection*/ false,
        /*previously_unavailable*/ None,
    )?;
    let remote_primary_id = seed_resume_with_saved_environments(
        codex_home.path(),
        &workspace,
        &[MISSING, LOCAL_ENVIRONMENT_ID],
        /*implicit_selection*/ false,
        /*previously_unavailable*/ None,
    )?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let path = if by_path {
        let request_id = app
            .send_thread_read_request(ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: false,
            })
            .await?;
        let saved: ThreadReadResponse =
            timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
        Some(saved.thread.path.context("selected saved rollout path")?)
    } else {
        None
    };
    let resumed: ThreadResumeResponse = timeout(LOCAL_CONVERSATION_TIMEOUT, async {
        let request_id = app
            .send_thread_resume_request(ThreadResumeParams {
                thread_id: thread_id.clone(),
                path: path.clone(),
                exclude_turns: by_path,
                ..Default::default()
            })
            .await?;
        requests.stalled_metadata_seen.notified().await;
        app.read_response(request_id).await
    })
    .await
    .context("ordinary resume should return while the secondary's metadata RPC is held")??;
    assert_eq!(resumed.thread.id, thread_id);
    if let Some(path) = path {
        assert_eq!(resumed.thread.path.as_ref(), Some(&path));
    }
    assert_eq!(
        resumed.instruction_sources,
        [LegacyAppPathString::from(local_source)]
    );
    let reported = resumed
        .thread
        .environments
        .context("the resume should report the saved selections")?;
    assert_eq!(
        reported
            .iter()
            .map(|selection| selection.environment_id.as_str())
            .collect::<Vec<_>>(),
        selections,
    );
    let echoed = reported
        .into_iter()
        .map(|selection| TurnEnvironmentParams {
            environment_id: selection.environment_id,
            cwd: selection.cwd,
            runtime_workspace_roots: Some(selection.runtime_workspace_roots),
        })
        .collect::<Vec<_>>();
    let rollout = resumed.thread.path.context("resumed thread rollout")?;
    let saved_thread_id = ThreadId::from_string(&thread_id)?;
    let saved = latest_owned_settings(&rollout, saved_thread_id).await?;
    let saved_environments = saved.environments.context("owned saved environments")?;
    assert!(!saved_environments.implicit_selection);
    assert!(saved_environments.environments.iter().all(|selection| {
        !selection.startup_unavailable
            && TurnEnvironmentRequest::from(selection.clone()).config
                == EnvironmentConfigState::FromThread
            && selection.cwd == workspace_uri
            && selection.workspace_roots == [workspace_uri.clone()]
    }));

    let mut previous_turn_id = None;
    for (prompt, environments) in [
        ("Ordinary send without environments", None),
        ("Send with the saved environments", Some(echoed.clone())),
    ] {
        let completed = timeout(
            LOCAL_CONVERSATION_TIMEOUT,
            app.start_turn_and_wait_for_completion(TurnStartParams {
                environments,
                ..text_turn_params(thread_id.clone(), prompt)
            }),
        )
        .await
        .with_context(|| format!("{prompt} should finish with remote metadata still held"))??;
        assert_eq!(completed.turn.status, TurnStatus::Completed);
        assert_ne!(previous_turn_id.as_ref(), Some(&completed.turn.id));
        previous_turn_id = Some(completed.turn.id);
    }
    let model_requests = mock.requests();
    assert_eq!(model_requests.len(), 4);
    assert!(
        model_requests[0]
            .message_input_texts("user")
            .iter()
            .any(|text| text.contains(LOCAL_INSTRUCTIONS))
    );
    for (request, call_id) in [
        (&model_requests[1], FIRST_CALL),
        (&model_requests[3], LATER_CALL),
    ] {
        let (output, success) = request
            .function_call_output_content_and_success(call_id)
            .with_context(|| format!("missing denial for {call_id}"))?;
        assert_ne!(success, Some(true));
        assert_eq!(output.as_deref(), Some(UNAVAILABLE));
    }
    let mut promoted = echoed.clone();
    promoted.swap(0, 2);
    let mut rerooted = echoed.clone();
    rerooted[2].runtime_workspace_roots = Some(Vec::new());
    let mut reordered = echoed.clone();
    reordered.swap(1, 2);
    for (prompt, environments) in [
        ("Promote the retired executor", promoted),
        ("Reroot the retired executor", rerooted),
        ("Reorder the saved secondaries", reordered),
    ] {
        let request_id = app
            .send_turn_start_request(TurnStartParams {
                environments: Some(environments),
                ..text_turn_params(thread_id.clone(), prompt)
            })
            .await?;
        let error = timeout(
            DEFAULT_READ_TIMEOUT,
            app.read_stream_until_error_message(RequestId::Integer(request_id)),
        )
        .await??;
        assert_eq!(error.error.code, -32600, "{prompt}");
        assert_eq!(
            error.error.message,
            format!("unknown turn environment id `{MISSING}`"),
            "{prompt}"
        );
    }
    assert_eq!(mock.requests().len(), 4);
    let saved_after = latest_owned_settings(&rollout, saved_thread_id).await?;
    assert_eq!(saved_after.environments.as_ref(), Some(&saved_environments));

    let new_local_cwd = workspace.join("new-local-workspace");
    std::fs::create_dir(&new_local_cwd)?;
    let mut local_update = echoed;
    local_update[0].cwd = LegacyAppPathString::from_path(&new_local_cwd);
    local_update[0].runtime_workspace_roots =
        Some(vec![LegacyAppPathString::from_path(&new_local_cwd)]);
    let completed = timeout(
        LOCAL_CONVERSATION_TIMEOUT,
        app.start_turn_and_wait_for_completion(TurnStartParams {
            cwd: Some(new_local_cwd.to_path_buf()),
            environments: Some(local_update),
            ..text_turn_params(thread_id.clone(), "Change only the local workspace")
        }),
    )
    .await??;
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    assert_eq!(mock.requests().len(), 5);
    let mut expected = saved_environments;
    expected.legacy_fallback_cwd = new_local_cwd.clone();
    let new_local_cwd = PathUri::from_abs_path(&new_local_cwd);
    expected.environments[0].cwd = new_local_cwd.clone();
    expected.environments[0].workspace_roots = vec![new_local_cwd];
    let saved_after_local_update = latest_owned_settings(&rollout, saved_thread_id).await?;
    assert_eq!(saved_after_local_update.environments, Some(expected));
    assert_saved_resume_requires_environment(&mut app, &remote_primary_id, None, MISSING).await?;

    stop.send(()).expect("stalled executor is still connected");
    timeout(DEFAULT_READ_TIMEOUT, executor).await???;
    Ok(())
}

#[tokio::test]
async fn thread_resume_bounds_saved_implicit_environment_discovery_and_keeps_required_environments()
-> Result<()> {
    let server = create_mock_responses_server_sequence(vec![
        create_final_assistant_message_sse_response("repaired")?,
        responses::sse(vec![
            responses::ev_response_created("resp-2"),
            responses::ev_assistant_message("msg-2", "continued"),
            responses::ev_completed("resp-2"),
        ]),
        create_final_assistant_message_sse_response("continued on the recovered executor")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    let workspace_dir = TempDir::new()?;
    let workspace = workspace_dir.path().to_path_buf().abs().canonicalize()?;
    std::fs::write(workspace.join("AGENTS.md"), "local workspace instructions")?;
    let workspace_uri = PathUri::from_abs_path(&workspace);
    let local_source = workspace_uri.join("AGENTS.md")?;
    let healthy_source = workspace_uri.join("AGENTS.override.md")?;
    let recovered_source = workspace_uri.join("RECOVERED.md")?;
    let recovered = Arc::new(AtomicBool::new(false));
    let mut registrations = String::new();
    let mut executors = Vec::new();
    let healthy_requests = Arc::new(InstructionDiscoveryRequests::default());
    for (id, executor) in [
        (
            "healthy",
            InstructionDiscoveryExecutor::Ready {
                source: healthy_source.clone(),
                read_once: true,
            },
        ),
        (
            "stalled",
            InstructionDiscoveryExecutor::Recoverable {
                source: recovered_source.clone(),
                recovered: Arc::clone(&recovered),
            },
        ),
        ("stalled-2", InstructionDiscoveryExecutor::Stalled),
        ("stalled-3", InstructionDiscoveryExecutor::Stalled),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        registrations.push_str(&format!(
            "[[environments]]\nid = {id:?}\nurl = \"ws://{}\"\n\n",
            listener.local_addr()?
        ));
        let (stop, stop_receiver) = oneshot::channel();
        let requests = if id == "healthy" {
            Arc::clone(&healthy_requests)
        } else {
            Arc::new(InstructionDiscoveryRequests::default())
        };
        let task = tokio::spawn(serve_instruction_discovery_executor(
            listener,
            executor,
            Arc::clone(&requests),
            stop_receiver,
        ));
        executors.push((id, requests, stop, task));
    }
    let executor_request_counts = |include_healthy: bool| {
        executors
            .iter()
            .filter(|(id, ..)| include_healthy || *id != "healthy")
            .map(|(id, requests, ..)| (*id, requests.total.load(Ordering::Relaxed)))
            .collect::<Vec<_>>()
    };
    std::fs::write(codex_home.path().join("environments.toml"), registrations)?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .with_sandbox_mode("danger-full-access")
        .with_root_config(
            "project_doc_fallback_filenames = [\"RECOVERED.md\"]\nthread_unload_delay_secs = 1",
        )
        .with_provider_config("supports_websockets = false")
        .with_extra_config(&format!(
            "[mcp_servers.stalled_process]\ncommand = \"must-not-start\"\n\
             cwd = {}\nenvironment_id = \"stalled-2\"\nrequired = true\n",
            json!(workspace.to_string_lossy()),
        ))
        .write(codex_home.path())?;

    let registered_selection = [
        LOCAL_ENVIRONMENT_ID,
        "healthy",
        "stalled",
        "stalled-2",
        "stalled-3",
    ];
    let mut legacy_selection = registered_selection.to_vec();
    legacy_selection.push("retired");
    let implicit_id = seed_resume_with_saved_environments(
        codex_home.path(),
        &workspace,
        &registered_selection,
        /*implicit_selection*/ true,
        /*previously_unavailable*/ None,
    )?;
    let required_id = seed_resume_with_saved_environments(
        codex_home.path(),
        &workspace,
        &legacy_selection,
        /*implicit_selection*/ false,
        /*previously_unavailable*/ None,
    )?;
    let retry_id = seed_resume_with_saved_environments(
        codex_home.path(),
        &workspace,
        &[LOCAL_ENVIRONMENT_ID, "stalled"],
        /*implicit_selection*/ true,
        /*previously_unavailable*/ Some("stalled"),
    )?;

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .with_json_logging(OPTIONAL_VERIFICATION_LOG_FILTER)
        .build_initialized()
        .await?;
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: implicit_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let response: ThreadResumeResponse = timeout(
        LOCAL_CONVERSATION_TIMEOUT,
        app_server.read_response(request_id),
    )
    .await??;
    assert_eq!(response.thread.id, implicit_id);
    assert!(response.thread.turns.is_empty());
    let expected_instruction_sources: Vec<LegacyAppPathString> =
        vec![local_source.clone().into(), healthy_source.into()];
    assert_eq!(
        response.instruction_sources,
        [LegacyAppPathString::from(local_source.clone())]
    );
    wait_for_optional_secondary_verification(&app_server, "healthy", &[], /*verified*/ true)
        .await?;
    assert_eq!(
        healthy_requests
            .instruction_reads
            .swap(0, Ordering::Relaxed),
        1
    );
    for (id, requests, _, _) in &executors {
        if *id != "healthy" {
            timeout(
                DEFAULT_READ_TIMEOUT,
                requests.stalled_metadata_seen.notified(),
            )
            .await
            .with_context(|| format!("{id} never received an instruction metadata request"))?;
        }
    }

    assert_saved_resume_requires_environment(
        &mut app_server,
        &required_id,
        /*optional_ids*/ Some(&[]),
        "stalled",
    )
    .await?;
    assert_eq!(
        healthy_requests
            .instruction_reads
            .swap(0, Ordering::Relaxed),
        1
    );

    let before_read = executor_request_counts(/*include_healthy*/ true);
    let request_id = app_server
        .send_thread_read_request(ThreadReadParams {
            thread_id: required_id.clone(),
            include_turns: true,
        })
        .await?;
    let cold_read: ThreadReadResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(cold_read.thread.id, required_id);
    assert!(cold_read.thread.environments.is_none());
    let original_user_item = saved_user_item(&cold_read.thread)?.clone();
    let request_id = app_server
        .send_thread_loaded_list_request(ThreadLoadedListParams::default())
        .await?;
    let loaded: ThreadLoadedListResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert!(!loaded.data.contains(&required_id));
    let request_id = app_server
        .send_turn_start_request(text_turn_params(required_id.clone(), "Not loaded yet"))
        .await?;
    let not_loaded = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(not_loaded.error.code, -32600);
    assert_eq!(
        not_loaded.error.message,
        format!("thread not found: {required_id}"),
    );
    assert_eq!(executor_request_counts(true), before_read);

    assert_saved_resume_requires_environment(
        &mut app_server,
        &required_id,
        Some(&["stalled"]),
        "stalled-2",
    )
    .await?;
    assert_eq!(
        healthy_requests
            .instruction_reads
            .swap(0, Ordering::Relaxed),
        1
    );
    assert_saved_resume_requires_environment(
        &mut app_server,
        &required_id,
        Some(&["stalled", "stalled-2", "stalled-3"]),
        "retired",
    )
    .await?;
    assert_eq!(
        healthy_requests
            .instruction_reads
            .swap(0, Ordering::Relaxed),
        1
    );

    let request_id = app_server
        .send_raw_request(
            "thread/resume",
            Some(json!({
                "threadId": required_id,
                "optionalEnvironmentIds": ["stalled", "stalled-2", "stalled-3", "retired"],
            })),
        )
        .await?;
    let recovered_resume: ThreadResumeResponse =
        timeout(BOUNDED_RESUME_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(recovered_resume.thread.id, required_id);
    assert_eq!(
        recovered_resume.instruction_sources,
        expected_instruction_sources
    );
    assert_eq!(
        saved_user_item(&recovered_resume.thread)?,
        &original_user_item
    );
    assert_eq!(
        healthy_requests.instruction_reads.load(Ordering::Relaxed),
        1
    );

    // An ordinary Desktop attach reuses the loaded thread without a second discovery.
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: required_id.clone(),
            ..Default::default()
        })
        .await?;
    let warm_resume: ThreadResumeResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(warm_resume.thread.id, required_id);
    assert_eq!(
        warm_resume.instruction_sources,
        expected_instruction_sources
    );
    assert_eq!(warm_resume.thread.turns, recovered_resume.thread.turns);
    assert_eq!(
        healthy_requests
            .instruction_reads
            .swap(0, Ordering::Relaxed),
        1
    );

    let rollout = recovered_resume
        .thread
        .path
        .context("the recovered thread should retain its rollout")?;
    let saved_thread_id = ThreadId::from_string(&required_id)?;
    let saved_settings = latest_owned_settings(&rollout, saved_thread_id).await?;
    let saved_environments = saved_settings
        .environments
        .as_ref()
        .context("the owned settings checkpoint lost its saved environments")?;
    assert!(!saved_environments.implicit_selection);
    assert_eq!(
        saved_environments
            .environments
            .iter()
            .map(|selection| selection.environment_id.as_str())
            .collect::<Vec<_>>(),
        legacy_selection,
    );
    assert!(saved_environments.environments.iter().all(|selection| {
        !selection.startup_unavailable
            && TurnEnvironmentRequest::from(selection.clone()).config
                == EnvironmentConfigState::FromThread
    }));

    // A user-approved turn replaces only the saved selection, not its retained locations.
    let retained = &saved_environments.environments[..2];
    let excluded_before_repair = executor_request_counts(/*include_healthy*/ false);
    healthy_requests
        .allow_repeated_instruction_reads
        .store(true, Ordering::Relaxed);
    let mut repair = text_turn_params(required_id.clone(), "Continue on the reviewed environments");
    repair.environments = Some(
        retained
            .iter()
            .map(|selection| TurnEnvironmentParams {
                environment_id: selection.environment_id.clone(),
                cwd: selection.cwd.clone().into(),
                runtime_workspace_roots: Some(
                    selection
                        .workspace_roots
                        .iter()
                        .cloned()
                        .map(Into::into)
                        .collect(),
                ),
            })
            .collect(),
    );
    let repaired = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(repair),
    )
    .await??;
    assert_eq!(repaired.turn.status, TurnStatus::Completed);

    let request_id = app_server
        .send_thread_unsubscribe_request(ThreadUnsubscribeParams {
            thread_id: required_id.clone(),
        })
        .await?;
    let unsubscribe: ThreadUnsubscribeResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(unsubscribe.status, ThreadUnsubscribeStatus::Unsubscribed);
    let closed: ThreadClosedNotification = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_notification("thread/closed"),
    )
    .await??;
    assert_eq!(closed.thread_id, required_id);

    let repaired_settings = latest_owned_settings(&rollout, saved_thread_id).await?;
    let repaired_environments = repaired_settings
        .environments
        .as_ref()
        .context("the repair did not persist its environment selection")?;
    assert!(!repaired_environments.implicit_selection);
    assert_eq!(repaired_environments.environments.as_slice(), retained);
    assert_eq!(
        repaired_environments.legacy_fallback_cwd,
        saved_environments.legacy_fallback_cwd,
    );
    assert_eq!(
        repaired_settings.permission_profile,
        saved_settings.permission_profile,
    );
    healthy_requests
        .allow_repeated_instruction_reads
        .store(false, Ordering::Relaxed);
    healthy_requests
        .instruction_reads
        .store(0, Ordering::Relaxed);
    let previous_events = app_server.json_log_events()?;
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: required_id.clone(),
            ..Default::default()
        })
        .await?;
    let reopened: ThreadResumeResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(reopened.thread.id, required_id);
    assert_eq!(
        reopened.instruction_sources,
        [LegacyAppPathString::from(local_source.clone())]
    );
    assert_eq!(saved_user_item(&reopened.thread)?, &original_user_item);
    wait_for_optional_secondary_verification(
        &app_server,
        "healthy",
        &previous_events,
        /*verified*/ true,
    )
    .await?;
    assert_eq!(
        healthy_requests.instruction_reads.load(Ordering::Relaxed),
        1
    );
    healthy_requests
        .allow_repeated_instruction_reads
        .store(true, Ordering::Relaxed);
    let continued = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(text_turn_params(required_id, "Keep going")),
    )
    .await??;
    assert_eq!(continued.turn.status, TurnStatus::Completed);
    assert_eq!(executor_request_counts(false), excluded_before_repair);

    // Only the fake's filesystem changes; the server, registration, and websocket stay in place.
    recovered.store(true, Ordering::Relaxed);
    let previous_events = app_server.json_log_events()?;
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: retry_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let response: ThreadResumeResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(response.thread.id, retry_id);
    assert_eq!(
        response.instruction_sources,
        [LegacyAppPathString::from(local_source)],
    );
    wait_for_optional_secondary_verification(
        &app_server,
        "stalled",
        &previous_events,
        /*verified*/ true,
    )
    .await?;
    let recovered_turn = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(text_turn_params(
            retry_id,
            "Continue after recovery",
        )),
    )
    .await??;
    assert_eq!(recovered_turn.turn.status, TurnStatus::Completed);
    let requests = responses::received_responses_requests(&server).await;
    assert_eq!(requests.len(), 3);
    assert!(
        requests[2]
            .message_input_texts("user")
            .iter()
            .any(|text| text.contains(AGENTS_INSTRUCTIONS))
    );

    for (_, _, stop, task) in executors {
        let _ = stop.send(());
        timeout(DEFAULT_READ_TIMEOUT, task).await???;
    }
    Ok(())
}

fn saved_user_item(thread: &Thread) -> Result<&ThreadItem> {
    thread
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .find(|item| {
            matches!(
                &item.payload,
                ThreadItemPayload::UserMessage { content, .. }
                    if matches!(content.as_slice(), [V2UserInput::Text { text, .. }]
                        if text == SAVED_USER_MESSAGE)
            )
        })
        .context("saved user message is missing from thread history")
}

async fn latest_owned_settings(path: &Path, thread_id: ThreadId) -> Result<ThreadSettingsSnapshot> {
    let (items, _, _) = RolloutRecorder::load_rollout_items(path).await?;
    items
        .into_iter()
        .rev()
        .find_map(|item| match item {
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event))
                if event.thread_id == Some(thread_id) =>
            {
                Some(event.thread_settings)
            }
            _ => None,
        })
        .context("missing owned settings checkpoint")
}

async fn assert_saved_resume_requires_environment(
    app_server: &mut TestAppServer,
    thread_id: &str,
    optional_ids: Option<&[&str]>,
    required_id: &str,
) -> Result<()> {
    let mut params = json!({"threadId": thread_id});
    if let Some(optional_ids) = optional_ids {
        params["optionalEnvironmentIds"] = json!(optional_ids);
    }
    let request_id = app_server
        .send_raw_request("thread/resume", Some(params))
        .await?;
    let error = timeout(
        BOUNDED_RESUME_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert!(
        error
            .error
            .message
            .contains(&format!("Environment `{required_id}`"))
            && error.error.message.contains("AGENTS.md discovery"),
        "{error:?}",
    );
    Ok(())
}

fn seed_resume_with_saved_environments(
    codex_home: &Path,
    workspace: &AbsolutePathBuf,
    environment_ids: &[&str],
    implicit_selection: bool,
    previously_unavailable: Option<&str>,
) -> Result<String> {
    const FILENAME_TIMESTAMP: &str = "2025-01-05T12-00-00";
    const TIMESTAMP: &str = "2025-01-05T12:00:00Z";
    let thread_id = create_fake_rollout(
        codex_home,
        FILENAME_TIMESTAMP,
        TIMESTAMP,
        SAVED_USER_MESSAGE,
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let workspace_uri = PathUri::from_abs_path(workspace);
    let selections: Vec<_> = environment_ids
        .iter()
        .map(|id| {
            let mut selection = json!({
                "environment_id": id,
                "cwd": workspace_uri,
                "workspace_roots": [workspace_uri],
            });
            if Some(*id) == previously_unavailable {
                selection["config_source"] = json!("owner");
                selection["startup_unavailable"] = json!(true);
            }
            selection
        })
        .collect();
    let mut environments = json!({
        "legacy_fallback_cwd": workspace,
        "environments": selections,
    });
    // Write the persisted representation so the legacy fixture really omits provenance.
    if implicit_selection {
        environments["implicit_selection"] = json!(true);
    }
    let event = json!({
        "timestamp": TIMESTAMP,
        "type": "event_msg",
        "payload": {
            "type": "thread_settings_applied",
            "thread_id": thread_id,
            "thread_settings": {
                "model": "gpt-5.4",
                "model_provider_id": "mock_provider",
                "cwd": workspace,
                "runtime_workspace_roots": [workspace],
                "approval_policy": "never",
                "approvals_reviewer": "user",
                "permission_profile": PermissionProfile::Disabled,
                "collaboration_mode": {"mode": "default", "settings": {"model": "gpt-5.4"}},
                "environments": environments,
            },
        },
    });
    let _: ThreadSettingsAppliedEvent = serde_json::from_value(event["payload"].clone())?;
    let path = rollout_path(codex_home, FILENAME_TIMESTAMP, &thread_id);
    writeln!(OpenOptions::new().append(true).open(path)?, "{event}")?;
    Ok(thread_id)
}

/// Controls how a connected executor answers instruction discovery RPCs.
enum InstructionDiscoveryExecutor {
    Ready {
        source: PathUri,
        read_once: bool,
    },
    Stalled,
    Recoverable {
        source: PathUri,
        recovered: Arc<AtomicBool>,
    },
}

/// Counts discovery RPCs before the server proceeds to the next request.
#[derive(Default)]
struct InstructionDiscoveryRequests {
    total: AtomicUsize,
    stalled_metadata_seen: Notify,
    instruction_reads: AtomicUsize,
    allow_repeated_instruction_reads: AtomicBool,
}

async fn serve_instruction_discovery_executor(
    listener: TcpListener,
    executor: InstructionDiscoveryExecutor,
    requests: Arc<InstructionDiscoveryRequests>,
    mut stop: oneshot::Receiver<()>,
) -> Result<()> {
    let environment_info = json!({
        "shell": {"name": "bash", "path": "/bin/bash"},
        "capabilities": {"environmentConfigRead": true},
    });
    let mut websocket = accept_exec_server_environment(listener, environment_info.clone()).await?;
    let instruction_path = match &executor {
        InstructionDiscoveryExecutor::Ready { source, .. }
        | InstructionDiscoveryExecutor::Recoverable { source, .. } => serde_json::to_value(source)?,
        InstructionDiscoveryExecutor::Stalled => serde_json::Value::Null,
    };
    loop {
        let request = tokio::select! {
            biased;
            request = read_exec_server_json(&mut websocket) => request?,
            _ = &mut stop => return Ok(()),
        };
        requests.total.fetch_add(1, Ordering::Relaxed);
        let id = &request["id"];
        let is_instruction_path = request["params"]["path"] == instruction_path;
        let stalled = match &executor {
            InstructionDiscoveryExecutor::Ready { .. } => false,
            InstructionDiscoveryExecutor::Stalled => true,
            InstructionDiscoveryExecutor::Recoverable { recovered, .. } => {
                !recovered.load(Ordering::Relaxed)
            }
        };
        let result = match request["method"].as_str() {
            Some("environment/info") => environment_info.clone(),
            Some("fs/getMetadata") if stalled => {
                requests.stalled_metadata_seen.notify_one();
                continue;
            }
            Some("fs/getMetadata") => json!({
                "isDirectory": false,
                "isFile": is_instruction_path,
                "isSymlink": false,
                "size": if is_instruction_path { AGENTS_INSTRUCTIONS.len() } else { 0 },
                "createdAtMs": 0,
                "modifiedAtMs": 0,
            }),
            Some("fs/readFile") if is_instruction_path => {
                let previous_reads = requests.instruction_reads.fetch_add(1, Ordering::Relaxed);
                anyhow::ensure!(
                    previous_reads == 0
                        || requests
                            .allow_repeated_instruction_reads
                            .load(Ordering::Relaxed)
                        || !matches!(
                            &executor,
                            InstructionDiscoveryExecutor::Ready {
                                read_once: true,
                                ..
                            }
                        ),
                    "unexpected second read of the same AGENTS.md"
                );
                json!({"dataBase64": BASE64_STANDARD.encode(AGENTS_INSTRUCTIONS)})
            }
            Some("environmentConfig/read") if stalled => {
                anyhow::bail!("unexpected MCP discovery on the stalled executor: {request}")
            }
            Some("environmentConfig/read") => json!({
                "codexHomeDir": "file:///codex-home",
                "config": {"layers": [], "cloudInsertionIndex": 0},
                "requirements": {"layers": [], "cloudInsertionIndex": 0},
            }),
            Some("process/start") => anyhow::bail!("unexpected process/start: {request}"),
            _ => {
                let error = json!({"code": -32601, "message": "not supported by test executor"});
                let response = json!({"id": id, "error": error});
                websocket
                    .send(Message::Text(response.to_string().into()))
                    .await?;
                continue;
            }
        };
        let response = json!({"id": id, "result": result});
        websocket
            .send(Message::Text(response.to_string().into()))
            .await?;
    }
}

#[tokio::test]
async fn turn_model_context_uses_selected_environment() -> Result<()> {
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_assistant_message("msg-1", "done"),
            responses::ev_completed("resp-1"),
        ]),
    )
    .await;
    let codex_home = TempDir::new()?;
    write_mock_config(codex_home.path(), &server.uri())?;
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let (environment_cwd, environment_shell) = {
        let auto_env = app_server.auto_env()?;
        (
            auto_env.selection().cwd.clone(),
            auto_env.environment().info().await?.shell.name,
        )
    };

    let thread = app_server
        .start_thread(ThreadStartParams::default())
        .await?
        .thread;
    timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(text_turn_params(
            thread.id,
            "inspect the selected environment",
        )),
    )
    .await??;

    let user_context = response_mock.single_request().message_input_texts("user");
    let environment_context = user_context
        .iter()
        .find(|text| text.starts_with("<environment_context>"))
        .context("selected environment context should be model visible")?;
    let shell = environment_context
        .lines()
        .find(|line| line.trim_start().starts_with("<shell>"))
        .map(str::trim)
        .map(str::to_string);
    let cwd = environment_context
        .lines()
        .find(|line| line.trim_start().starts_with("<cwd>"))
        .map(str::trim)
        .map(str::to_string);
    assert_eq!(
        (shell, cwd),
        (
            Some(format!("<shell>{environment_shell}</shell>")),
            Some(format!(
                "<cwd>{}</cwd>",
                environment_cwd.inferred_native_path_string()
            )),
        )
    );
    Ok(())
}

#[tokio::test]
async fn command_execution_notifications_preserve_selected_environment_paths() -> Result<()> {
    let command_arguments = serde_json::to_string(&json!({
        "cmd": "cat main.rs",
        "yield_time_ms": 10_000,
    }))?;
    let server = create_mock_responses_server_sequence(vec![
        responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_function_call(
                "selected-environment-read",
                "exec_command",
                &command_arguments,
            ),
            responses::ev_completed("resp-1"),
        ]),
        create_final_assistant_message_sse_response("done")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_root_config("compact_prompt = \"compact\"\nmodel_auto_compact_token_limit = 100000")
        .with_provider_config("supports_websockets = false")
        .with_sandbox_mode("danger-full-access")
        .enable_feature(Feature::UnifiedExec)
        .write(codex_home.path())?;
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let (expected_path, shell) = {
        let environment = app_server.auto_env()?;
        let path = environment.selection().cwd.join("main.rs")?;
        environment
            .environment()
            .get_filesystem()
            .write_file(
                &path,
                b"fn main() {}\n".to_vec(),
                Default::default(),
                /*sandbox*/ None,
            )
            .await?;
        (
            path.inferred_native_path_string(),
            environment.environment().info().await?.shell,
        )
    };
    let thread = app_server
        .start_thread(ThreadStartParams::default())
        .await?
        .thread;
    timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(text_turn_params(thread.id, "read main.rs")),
    )
    .await??;

    let expected_actions = match shell.name.as_str() {
        // Windows shell scripts are not yet parsed into file-read command actions.
        "powershell" => {
            let command = if detect_shell_type(&shell.path) == Some(ShellType::PowerShell) {
                "cat main.rs".to_string()
            } else {
                shlex::try_join([shell.path.as_str(), "-Command", "cat main.rs"])?
            };
            json!([{
                "type": "unknown",
                "command": command,
            }])
        }
        "cmd" => {
            let command = shlex::try_join([shell.path.as_str(), "/c", "cat main.rs"])?;
            json!([{
                "type": "unknown",
                "command": command,
            }])
        }
        _ => json!([{
            "type": "read",
            "command": "cat main.rs",
            "name": "main.rs",
            "path": expected_path,
        }]),
    };

    for method in ["item/started", "item/completed"] {
        let notification = timeout(
            DEFAULT_READ_TIMEOUT,
            app_server.read_stream_until_matching_notification(method, |notification| {
                notification.method == method
                    && notification
                        .params
                        .as_ref()
                        .is_some_and(|params| params["item"]["id"] == "selected-environment-read")
            }),
        )
        .await??;
        let params = notification
            .params
            .context("command execution notification should include params")?;
        assert_eq!(params["item"]["commandActions"], expected_actions);
    }

    Ok(())
}

#[cfg_attr(windows, ignore = "uses a POSIX sandboxed command")]
#[tokio::test]
async fn later_turn_preserves_remote_instructions_and_executes_after_session_rejoin() -> Result<()>
{
    use std::process::Stdio;

    use codex_app_server_protocol::EnvironmentConnectionNotification;
    use codex_app_server_protocol::EnvironmentInfoResponse;
    use codex_app_server_protocol::SandboxPolicy;
    use codex_app_server_protocol::TurnStartResponse;
    use codex_app_server_protocol::TurnStatus;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::BufReader;
    use tokio::process::Command;
    use tokio_util::task::AbortOnDropHandle;

    const REMOTE_ID: &str = "rejoin-worker";
    const INSTRUCTIONS: &str = "Keep these remote workspace instructions after rejoin.";
    const CALL_ID: &str = "rejoined-worker-read";
    const MARKER: &str = "read on the rejoined worker";

    #[cfg(target_os = "linux")]
    {
        const PARENT_NETNS: &str = "CODEX_TEST_REJOIN_PARENT_NETNS";
        let netns = std::fs::read_link("/proc/self/ns/net")?;
        let devices = std::fs::read_to_string("/proc/net/dev")?;
        let loopback_only = devices
            .lines()
            .skip(2)
            .filter_map(|line| line.split_once(':').map(|(name, _)| name.trim()))
            .eq(["lo"])
            && std::fs::read_to_string("/proc/net/route")?.lines().count() == 1;
        if let Some(parent) = std::env::var_os(PARENT_NETNS) {
            assert_ne!(
                netns.as_os_str(),
                parent.as_os_str(),
                "rejoin reused the parent network"
            );
            assert!(loopback_only, "rejoin requires loopback-only networking");
        } else if !loopback_only {
            let (_, module) = module_path!()
                .split_once("::")
                .context("integration module")?;
            let case = format!(
                "{module}::later_turn_preserves_remote_instructions_and_executes_after_session_rejoin"
            );
            let client = std::env::current_exe()?;
            let daemon = codex_utils_cargo_bin::cargo_bin("codex-app-server")?;
            let executor = codex_utils_cargo_bin::cargo_bin("exec-server")?;
            // Keep the real managed sandbox inside the loopback-only test.
            let output = Command::new("/usr/bin/bwrap")
                .args([
                    "--unshare-net",
                    "--unshare-pid",
                    "--die-with-parent",
                    "--new-session",
                ])
                .args(["--tmpfs", "/", "--ro-bind", "/usr", "/usr"])
                .args(["--symlink", "usr/bin", "/bin"])
                .args(["--symlink", "usr/lib", "/lib"])
                .args(["--symlink", "usr/lib64", "/lib64"])
                .args(["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"])
                .arg("--ro-bind")
                .arg(client)
                .arg("/client")
                .arg("--ro-bind")
                .arg(daemon)
                .arg("/app-server")
                .arg("--ro-bind")
                .arg(executor)
                .arg("/exec-server")
                .args(["--chdir", "/", "--setenv", "HOME", "/tmp"])
                .args(["--setenv", PARENT_NETNS])
                .arg(netns)
                .args(["--", "/client", "--exact", &case, "--test-threads=1"])
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("LANG", "C.UTF-8")
                .env("RUST_MIN_STACK", "33554432")
                .env("CARGO_BIN_EXE_codex_app_server", "/app-server")
                .env("CARGO_BIN_EXE_exec_server", "/exec-server")
                .kill_on_drop(true)
                .output()
                .await
                .context("start the rejoin test in a private network")?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            anyhow::ensure!(
                output.status.success()
                    && stdout.lines().any(|line| {
                        line.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored;")
                    }),
                "isolated rejoin test did not pass ({}):\n{stdout}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
            return Ok(());
        }
    }

    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_sequence(
        &server,
        vec![
            create_final_assistant_message_sse_response("ready")?,
            create_final_assistant_message_sse_response("still selected")?,
            responses::sse(vec![
                responses::ev_exec_command_call_with_args(
                    CALL_ID,
                    &json!({"cmd": "cat rejoin-marker", "environment_id": REMOTE_ID, "login": false}),
                ),
                responses::ev_completed("rejoined-worker-tool"),
            ]),
            create_final_assistant_message_sse_response("done")?,
        ],
    )
    .await;
    let codex_home = TempDir::new()?;
    let executor_home = TempDir::new()?;
    let local_dir = TempDir::new()?;
    let remote_dir = TempDir::new()?;
    let local_cwd = local_dir.path().to_path_buf().abs().canonicalize()?;
    let remote_cwd = remote_dir.path().to_path_buf().abs().canonicalize()?;
    let remote_uri = PathUri::from_abs_path(&remote_cwd);
    std::fs::write(remote_cwd.join("AGENTS.md"), INSTRUCTIONS)?;
    std::fs::write(remote_cwd.join("rejoin-marker"), MARKER)?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("workspace-write")
        .with_approval_policy("never")
        .with_provider_config("supports_websockets = false")
        .enable_feature(Feature::UnifiedExec)
        .with_extra_config(&format!(
            "[projects.{}]\ntrust_level = \"trusted\"\n",
            json!(local_cwd.to_string_lossy()),
        ))
        .write(codex_home.path())?;

    let mut executor = Command::new(codex_utils_cargo_bin::cargo_bin("exec-server")?);
    executor
        .current_dir(remote_cwd.as_path())
        .env_clear()
        .env("HOME", executor_home.path())
        .env("CODEX_HOME", executor_home.path())
        .env("PATH", "/usr/bin:/bin")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(target_os = "linux")]
    executor.env(
        "CODEX_TEST_LINUX_SANDBOX_EXE",
        core_test_support::find_codex_linux_sandbox_exe()?,
    );
    let mut executor = executor.spawn()?;
    let mut executor_stdout =
        BufReader::new(executor.stdout.take().context("executor stdout")?).lines();
    let executor_url = timeout(DEFAULT_READ_TIMEOUT, executor_stdout.next_line())
        .await??
        .context("executor WebSocket URL")?;
    assert!(
        executor_url.starts_with("ws://127.0.0.1:"),
        "{executor_url}"
    );
    let relay_listener = TcpListener::bind("127.0.0.1:0").await?;
    std::fs::write(
        codex_home.path().join("environments.toml"),
        format!(
            "[[environments]]\nid = \"{REMOTE_ID}\"\nurl = \"ws://{}\"\n",
            relay_listener.local_addr()?,
        ),
    )?;
    let (disconnect, disconnect_rx) = oneshot::channel();
    let (dropped, dropped_rx) = oneshot::channel();
    let (allow_resume, allow_resume_rx) = oneshot::channel();
    let (finish, finish_rx) = oneshot::channel();
    let relay = AbortOnDropHandle::new(tokio::spawn(relay_executor_session(
        relay_listener,
        executor_url,
        disconnect_rx,
        dropped,
        allow_resume_rx,
        finish_rx,
        CALL_ID,
    )));

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_managed_config()
        .with_mock_chatgpt_backend()
        .without_auto_env()
        .with_env_overrides(&[("HOME", codex_home.path().to_str())])
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;
    let request_id = app
        .send_thread_start_request(ThreadStartParams {
            cwd: Some(local_cwd.to_string_lossy().into_owned()),
            environments: Some(
                [("local", &local_cwd), (REMOTE_ID, &remote_cwd)]
                    .into_iter()
                    .map(|(id, cwd)| {
                        let cwd = LegacyAppPathString::from_abs_path(cwd);
                        TurnEnvironmentParams {
                            environment_id: id.to_string(),
                            cwd: cwd.clone(),
                            runtime_workspace_roots: Some(vec![cwd]),
                        }
                    })
                    .collect(),
            ),
            ephemeral: Some(true),
            ..Default::default()
        })
        .await?;
    let response: ThreadStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(
        response.instruction_source_path_uris(),
        vec![remote_uri.join("AGENTS.md")?],
    );
    assert!(matches!(
        response.sandbox,
        SandboxPolicy::WorkspaceWrite { .. }
    ));
    let thread_id = response.thread.id;
    let expected_connection = EnvironmentConnectionNotification {
        thread_id: thread_id.clone(),
        environment_id: REMOTE_ID.to_string(),
    };
    let environments: Vec<_> = response
        .thread
        .environments
        .context("selected environments")?
        .into_iter()
        .map(|environment| TurnEnvironmentParams {
            environment_id: environment.environment_id,
            cwd: environment.cwd,
            runtime_workspace_roots: Some(environment.runtime_workspace_roots),
        })
        .collect();
    assert_eq!(
        environments
            .iter()
            .map(|environment| environment.environment_id.as_str())
            .collect::<Vec<_>>(),
        vec!["local", REMOTE_ID],
    );
    let turn = |prompt| TurnStartParams {
        cwd: Some(local_cwd.to_path_buf()),
        environments: Some(environments.clone()),
        ..text_turn_params(thread_id.clone(), prompt)
    };
    let first = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(turn("Inspect the selected workspace.")),
    )
    .await??;
    assert_eq!(first.turn.status, TurnStatus::Completed);

    // Eager thread/start connects before the listener is attached. The
    // connected notification below reports recovery, not initial state.
    disconnect.send(()).expect("original connection is running");
    timeout(DEFAULT_READ_TIMEOUT, dropped_rx).await??;
    let later_request_id = app
        .send_turn_start_request(turn("Keep the same selected workspace."))
        .await?;
    let later: TurnStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(later_request_id))
            .await
            .context("later turn was not admitted while the worker transport was down")??;
    assert_ne!(first.turn.id, later.turn.id);
    allow_resume.send(()).expect("relay is waiting to resume");
    // Observe recovery without sending another request to the environment.
    assert_eq!(
        timeout(
            DEFAULT_READ_TIMEOUT,
            app.read_notification::<EnvironmentConnectionNotification>(
                "thread/environment/connected"
            ),
        )
        .await??,
        expected_connection,
    );
    let completion = timeout(
        DEFAULT_READ_TIMEOUT,
        app.read_stream_until_matching_notification("later turn completion", |notification| {
            notification.method == "turn/completed"
                && notification.params.as_ref().is_some_and(|params| {
                    params["threadId"] == thread_id && params["turn"]["id"] == later.turn.id
                })
        }),
    )
    .await??;
    assert_eq!(
        completion.params.context("later turn params")?["turn"]["status"],
        "completed"
    );
    let request_id = app
        .send_raw_request(
            "environment/info",
            Some(json!({"environmentId": REMOTE_ID})),
        )
        .await?;
    let info: EnvironmentInfoResponse =
        timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(info.cwd, Some(remote_uri.clone()));

    let third = timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(turn("Read the marker on the rejoined worker.")),
    )
    .await??;
    assert_eq!(third.turn.status, TurnStatus::Completed);
    let requests = response_mock.requests();
    assert_eq!(requests.len(), 4);
    let expected_instructions = format!(
        "# AGENTS.md instructions for {}\n\n<INSTRUCTIONS>\n{INSTRUCTIONS}\n</INSTRUCTIONS>",
        remote_uri.inferred_native_path_string(),
    );
    for request in &requests {
        let input = request.message_input_texts("user");
        assert_eq!(
            input
                .iter()
                .rfind(|text| text.starts_with("# AGENTS.md instructions")),
            Some(&expected_instructions),
        );
    }
    let (output, success) = requests[3]
        .function_call_output_content_and_success(CALL_ID)
        .context("the model request did not receive the remote tool output")?;
    let output = output.context("the tool result did not contain text")?;
    assert_ne!(success, Some(false));
    assert!(
        output.contains("Process exited with code 0") && output.contains(MARKER),
        "{output}"
    );
    finish.send(()).expect("resumed connection is running");
    let (command, sandbox_type) = timeout(DEFAULT_READ_TIMEOUT, relay).await???;
    assert_eq!(command["metadata"]["threadId"], thread_id);
    assert_eq!(command["cwd"], json!(remote_uri));
    assert_eq!(command["sandbox"]["permissions"]["type"], "managed");
    assert!(matches!(
        sandbox_type.as_str(),
        Some("linuxSeccomp" | "macosSeatbelt")
    ));
    timeout(DEFAULT_READ_TIMEOUT, executor.kill()).await??;
    Ok(())
}

async fn relay_executor_session(
    listener: TcpListener,
    executor_url: String,
    disconnect: oneshot::Receiver<()>,
    dropped: oneshot::Sender<()>,
    allow_resume: oneshot::Receiver<()>,
    finish: oneshot::Receiver<()>,
    call_id: &str,
) -> Result<(serde_json::Value, serde_json::Value)> {
    use futures::StreamExt;
    use serde_json::Value;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::connect_async;

    let mut dropped = Some(dropped);
    let mut allow_resume = Some(allow_resume);
    let mut session_id = None;
    let mut command = None;
    let mut command_request_id = None;
    let mut sandbox_type = None;
    for (connection, mut stop) in [disconnect, finish].into_iter().enumerate() {
        if connection == 1 {
            allow_resume.take().expect("one resume").await?;
        }
        let mut downstream = accept_async(listener.accept().await?.0).await?;
        let (mut upstream, _) = connect_async(executor_url.as_str()).await?;

        let message = downstream.next().await.context("initialize request")??;
        let initialize: Value = serde_json::from_str(message.to_text()?)?;
        assert_eq!(initialize["method"], "initialize");
        assert_eq!(initialize["params"]["resumeSessionId"], json!(session_id));
        upstream.send(message).await?;
        let message = upstream.next().await.context("initialize response")??;
        let initialized: Value = serde_json::from_str(message.to_text()?)?;
        assert_eq!(initialized["id"], initialize["id"]);
        let returned_session = initialized["result"]["sessionId"]
            .as_str()
            .context("real executor session ID")?;
        if let Some(original_session) = &session_id {
            assert_eq!(returned_session, original_session);
        } else {
            session_id = Some(returned_session.to_string());
        }
        downstream.send(message).await?;

        loop {
            tokio::select! {
                result = &mut stop => {
                    result?;
                    break;
                }
                message = downstream.next() => {
                    let message = message.context("app-server closed before requested disconnect")??;
                    if let Message::Text(text) = &message {
                        let request: Value = serde_json::from_str(text)?;
                        if request["method"] == "process/start"
                            && request["params"]["metadata"]["toolCallId"] == call_id
                        {
                            assert_eq!(connection, 1);
                            assert!(command.is_none(), "tool command started more than once");
                            command_request_id = Some(request["id"].clone());
                            command = Some(request["params"].clone());
                        }
                    }
                    upstream.send(message).await?;
                }
                message = upstream.next() => {
                    let message = message.context("exec-server closed before requested disconnect")??;
                    if let Message::Text(text) = &message {
                        let response: Value = serde_json::from_str(text)?;
                        if command_request_id.as_ref() == response.get("id") && command_request_id.is_some() {
                            sandbox_type = Some(response["result"]["sandboxType"].clone());
                        }
                    }
                    downstream.send(message).await?;
                }
            }
        }
        drop(downstream);
        drop(upstream);
        if let Some(dropped) = dropped.take() {
            dropped.send(()).expect("test is waiting for disconnect");
        }
    }
    Ok((
        command.context("no process/start for the scoped tool")?,
        sandbox_type.context("no process/start response for the scoped tool")?,
    ))
}
