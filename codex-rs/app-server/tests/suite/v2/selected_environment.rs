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
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use super::exec_server_test_support::accept_exec_server_environment;
use super::exec_server_test_support::read_exec_server_json;

const AGENTS_INSTRUCTIONS: &str = "selected environment workspace instructions";
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
const SAVED_USER_MESSAGE: &str = "Saved environment selection";
// Three stalled lookups share one 10-second deadline, not three sequential deadlines.
const BOUNDED_RESUME_TIMEOUT: Duration = Duration::from_secs(20);

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
    const THREAD_START_TIMEOUT: Duration = Duration::from_secs(20);
    const LOCAL_INSTRUCTIONS: &str = "local workspace instructions";

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
    let workspace_dir = TempDir::new()?;
    let workspace = workspace_dir.path().to_path_buf().abs().canonicalize()?;
    std::fs::write(workspace.join("AGENTS.md"), LOCAL_INSTRUCTIONS)?;
    let workspace_uri = PathUri::from_abs_path(&workspace);
    let local_source = workspace_uri.join("AGENTS.md")?;
    let healthy_source = workspace_uri.join("AGENTS.override.md")?;

    let healthy = TcpListener::bind("127.0.0.1:0").await?;
    let stalled = TcpListener::bind("127.0.0.1:0").await?;
    std::fs::write(
        codex_home.path().join("environments.toml"),
        format!(
            "[[environments]]\nid = \"healthy\"\nurl = \"ws://{}\"\n\n\
             [[environments]]\nid = \"stalled\"\nurl = \"ws://{}\"\n",
            healthy.local_addr()?,
            stalled.local_addr()?,
        ),
    )?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("danger-full-access")
        .with_provider_config("supports_websockets = false")
        .with_extra_config(&format!(
            "[mcp_servers.stalled_process]\ncommand = \"must-not-start\"\n\
             cwd = {}\nenvironment_id = \"stalled\"\nrequired = true\n",
            json!(workspace.to_string_lossy()),
        ))
        .write(codex_home.path())?;

    let (stop_healthy, healthy_stop) = oneshot::channel();
    let (stop_stalled, stalled_stop) = oneshot::channel();
    let stalled_requests = Arc::new(InstructionDiscoveryRequests::default());
    let healthy_server = tokio::spawn(serve_instruction_discovery_executor(
        healthy,
        InstructionDiscoveryExecutor::Ready {
            source: healthy_source.clone(),
            read_once: false,
        },
        Arc::new(InstructionDiscoveryRequests::default()),
        healthy_stop,
    ));
    let stalled_server = tokio::spawn(serve_instruction_discovery_executor(
        stalled,
        InstructionDiscoveryExecutor::Stalled,
        Arc::clone(&stalled_requests),
        stalled_stop,
    ));
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let start = ThreadStartParams {
        cwd: Some(workspace.to_string_lossy().into_owned()),
        ephemeral: Some(true),
        ..Default::default()
    };

    let request_id = app_server
        .send_raw_request(
            "thread/start",
            Some(json!({"cwd": workspace, "ephemeral": true})),
        )
        .await?;
    let response: ThreadStartResponse =
        timeout(THREAD_START_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(
        response.instruction_sources,
        vec![local_source.into(), healthy_source.into()],
    );
    timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.start_turn_and_wait_for_completion(TurnStartParams {
            cwd: Some(workspace.to_path_buf()),
            ..text_turn_params(response.thread.id, "inspect workspace instructions")
        }),
    )
    .await??;
    let input = response_mock.single_request().message_input_texts("user");
    for expected in [LOCAL_INSTRUCTIONS, AGENTS_INSTRUCTIONS] {
        assert!(
            input.iter().any(|text| text.contains(expected)),
            "{input:?}"
        );
    }

    let request_id = app_server
        .send_thread_start_request(ThreadStartParams {
            environments: Some(vec![TurnEnvironmentParams {
                environment_id: "stalled".to_string(),
                cwd: LegacyAppPathString::from_path(workspace.as_path()),
                runtime_workspace_roots: None,
            }]),
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
        error.error.message.contains(
            "Environment `stalled` did not complete executor readiness and AGENTS.md discovery."
        ),
        "{error:?}",
    );

    let _ = stop_healthy.send(());
    let _ = stop_stalled.send(());
    timeout(DEFAULT_READ_TIMEOUT, healthy_server).await???;
    timeout(DEFAULT_READ_TIMEOUT, stalled_server).await???;
    assert!(stalled_requests.stalled_metadata.load(Ordering::Relaxed) > 0);
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
        .build_initialized()
        .await?;
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: implicit_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let response: ThreadResumeResponse =
        timeout(BOUNDED_RESUME_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(response.thread.id, implicit_id);
    assert!(response.thread.turns.is_empty());
    let expected_instruction_sources: Vec<LegacyAppPathString> =
        vec![local_source.clone().into(), healthy_source.into()];
    assert_eq!(response.instruction_sources, expected_instruction_sources);
    assert_eq!(
        healthy_requests
            .instruction_reads
            .swap(0, Ordering::Relaxed),
        1
    );
    for (id, requests, _, _) in &executors {
        if *id != "healthy" {
            assert!(
                requests.stalled_metadata.load(Ordering::Relaxed) > 0,
                "{id} never received an instruction metadata request"
            );
        }
    }

    assert_saved_resume_requires_environment(
        &mut app_server,
        &required_id,
        /*optional_ids*/ None,
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
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: required_id.clone(),
            ..Default::default()
        })
        .await?;
    let reopened: ThreadResumeResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert_eq!(reopened.thread.id, required_id);
    assert_eq!(reopened.instruction_sources, expected_instruction_sources);
    assert_eq!(saved_user_item(&reopened.thread)?, &original_user_item);
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
        vec![local_source.into(), recovered_source.into()],
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
    stalled_metadata: AtomicUsize,
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
                requests.stalled_metadata.fetch_add(1, Ordering::Relaxed);
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
