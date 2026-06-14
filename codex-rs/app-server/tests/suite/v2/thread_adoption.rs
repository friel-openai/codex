use std::path::Path;
use std::path::PathBuf;
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
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSource;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadStatus;
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

const TIMEOUT: Duration = Duration::from_secs(20);
const COLLABORATION_NAMESPACE: &str = "collaboration";
const FRODEX_NAMESPACE: &str = "frodex";
const THREAD_ADOPTION_FEATURE_CONFIG: &str =
    "[features.multi_agent_v2]\nenabled = true\nenable_thread_adoption = true";
const THREAD_ADOPTION_DISABLED_CONFIG: &str =
    "[features.multi_agent_v2]\nenabled = true\nenable_thread_adoption = false";
const ORIGINAL_PROMPT: &str = "original root history before native adoption";
const ORIGINAL_ANSWER: &str = "original root answer before native adoption";
const ADOPT_PROMPT: &str = "adopt the existing durable worker thread";
const ADOPT_MESSAGE: &str = "continue the original thread as my adopted worker";
const ADOPT_ANSWER: &str = "adopted worker completed the assigned task";
const ADOPT_CALL_ID: &str = "native-adoption-spawn-call";
const FOLLOWUP_PROMPT: &str = "follow up with the adopted durable worker";
const FOLLOWUP_MESSAGE: &str = "perform the adopted worker follow-up";
const FOLLOWUP_ANSWER: &str = "adopted worker completed the follow-up";
const FOLLOWUP_CALL_ID: &str = "native-adoption-followup-call";
const LIST_ADOPTED_PROMPT: &str = "list the adopted durable worker";
const LIST_ADOPTED_CALL_ID: &str = "native-adoption-list-agents-call";
const PROMOTE_PROMPT: &str = "promote the adopted worker to an independent thread";
const PROMOTE_CALL_ID: &str = "native-adoption-promote-call";
const LIST_PROMOTED_PROMPT: &str = "list agents after promoting the durable worker";
const LIST_PROMOTED_CALL_ID: &str = "native-promotion-list-agents-call";
const DIRECT_PROMPT: &str = "direct input after native thread promotion";
const DIRECT_ANSWER: &str = "promoted root accepted direct input";
const RESTART_PROMPT: &str = "direct input after promoted thread restart";
const RESTART_ANSWER: &str = "promoted root survived app-server restart";
const DESCENDANT_SPAWN_PROMPT: &str = "create an original root worker before native adoption";
const DESCENDANT_SPAWN_CALL_ID: &str = "native-original-descendant-spawn-call";
const DESCENDANT_MESSAGE: &str = "complete the original descendant assignment";
const DESCENDANT_ANSWER: &str = "original descendant completed its assignment";
const DESCENDANT_ACKNOWLEDGMENT_PROMPT: &str = "acknowledge the completed original worker";
const DESCENDANT_ACKNOWLEDGMENT_ANSWER: &str = "acknowledged the completed original worker";
const DESCENDANT_FOLLOWUP_PROMPT: &str = "follow up with the adopted root descendant";
const DESCENDANT_FOLLOWUP_CALL_ID: &str = "native-adopted-descendant-followup-call";
const DESCENDANT_FOLLOWUP_MESSAGE: &str = "continue the original descendant after root adoption";
const DESCENDANT_FOLLOWUP_ANSWER: &str = "original descendant completed its adopted follow-up";
const DESCENDANT_COMPLETION_PROMPT: &str =
    "have the adopted root acknowledge its worker completion";
const DESCENDANT_COMPLETION_CALL_ID: &str = "native-adopted-descendant-completion-call";
const DESCENDANT_COMPLETION_MESSAGE: &str = "acknowledge the original worker's adopted completion";
const DESCENDANT_COMPLETION_ANSWER: &str = "acknowledged the original worker's adopted completion";
const ANCESTOR_CYCLE_PROMPT: &str = "ask the worker to attempt adopting its ancestor";
const ANCESTOR_CYCLE_MESSAGE: &str = "attempt to adopt the original root ancestor";
const ANCESTOR_CYCLE_FOLLOWUP_CALL_ID: &str = "native-ancestor-cycle-followup-call";
const ANCESTOR_CYCLE_ADOPT_CALL_ID: &str = "native-ancestor-cycle-adoption-call";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdoptionSource {
    Loaded,
    Stored,
    Ephemeral,
    DescendantTree,
    AncestorCycle,
    VsCode,
    DisabledAfterAdoption,
}

struct OriginalDescendant {
    thread_id: String,
    rollout_path: PathBuf,
    rollout_bytes: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_thread_adoption_and_promotion_preserve_thread_history_and_identity() -> Result<()> {
    assert_native_thread_adoption_and_promotion(AdoptionSource::Loaded).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_stored_thread_adoption_and_promotion_preserve_thread_history_and_identity()
-> Result<()> {
    assert_native_thread_adoption_and_promotion(AdoptionSource::Stored).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_ephemeral_thread_adoption_and_promotion_preserve_thread_history_and_identity()
-> Result<()> {
    assert_native_thread_adoption_and_promotion(AdoptionSource::Ephemeral).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_descendant_tree_adoption_and_promotion_preserve_thread_history_and_identity()
-> Result<()> {
    assert_native_thread_adoption_and_promotion(AdoptionSource::DescendantTree).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_thread_adoption_and_promotion_preserve_non_cli_session_source() -> Result<()> {
    assert_native_thread_adoption_and_promotion(AdoptionSource::VsCode).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_adopted_thread_resume_preserves_ownership_after_feature_is_disabled() -> Result<()>
{
    assert_native_thread_adoption_and_promotion(AdoptionSource::DisabledAfterAdoption).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_thread_adoption_rejects_ancestor_cycle_without_changing_existing_tree() -> Result<()>
{
    assert_native_thread_adoption_and_promotion(AdoptionSource::AncestorCycle).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_ephemeral_child_promotion_materializes_original_thread_and_history() -> Result<()> {
    let server = responses::start_mock_server().await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::Collab)
        .with_extra_config(THREAD_ADOPTION_FEATURE_CONFIG)
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;

    let mut client = start_isolated_app_server(codex_home.path()).await?;
    let parent: ThreadStartResponse = request(
        &client,
        ClientRequest::ThreadStart {
            request_id: RequestId::Integer(100),
            params: ThreadStartParams {
                ephemeral: Some(true),
                ..ThreadStartParams::default()
            },
        },
    )
    .await?;
    assert!(parent.thread.ephemeral);
    assert_eq!(parent.thread.path, None);
    let parent_id = parent.thread.id;

    let spawn_arguments = serde_json::to_string(&json!({
        "task_name": "ephemeral_worker",
        "fork_turns": "none",
        "message": DESCENDANT_MESSAGE,
    }))?;
    let _spawn_call = mount_tool_response(
        &server,
        DESCENDANT_SPAWN_PROMPT,
        "resp-native-ephemeral-worker-spawn",
        DESCENDANT_SPAWN_CALL_ID,
        "spawn_agent",
        &spawn_arguments,
    )
    .await;
    let ephemeral_work = mount_new_agent_response(
        &server,
        &parent_id,
        DESCENDANT_MESSAGE,
        "resp-native-ephemeral-worker",
        DESCENDANT_ANSWER,
    )
    .await;
    let spawn_return = mount_final_response(
        &server,
        DESCENDANT_SPAWN_CALL_ID,
        "resp-native-ephemeral-worker-spawn-return",
        "the ephemeral native worker was created",
    )
    .await;

    start_turn(&client, 101, &parent_id, DESCENDANT_SPAWN_PROMPT).await?;
    wait_for_completed_turn(&mut client, &parent_id).await?;
    let spawn_output = spawn_return
        .function_call_output_text(DESCENDANT_SPAWN_CALL_ID)
        .context("spawn_agent did not return the ephemeral native worker")?;
    let spawn_output: Value = serde_json::from_str(&spawn_output)
        .with_context(|| format!("spawn_agent returned {spawn_output:?}"))?;
    assert_eq!(
        spawn_output.get("task_name").and_then(Value::as_str),
        Some("/root/ephemeral_worker")
    );
    let worker_request =
        wait_for_new_agent_request(&ephemeral_work, &parent_id, DESCENDANT_MESSAGE).await?;
    let worker_id = worker_request
        .header("thread-id")
        .or_else(|| worker_request.header("x-client-request-id"))
        .context("the ephemeral native worker request must contain its original thread ID")?;
    assert_eq!(
        worker_request.header("session-id").as_deref(),
        Some(parent_id.as_str())
    );

    let ephemeral: ThreadReadResponse = request(
        &client,
        ClientRequest::ThreadRead {
            request_id: RequestId::Integer(102),
            params: ThreadReadParams {
                thread_id: worker_id.clone(),
                include_turns: false,
            },
        },
    )
    .await?;
    assert_eq!(ephemeral.thread.id, worker_id);
    assert!(ephemeral.thread.ephemeral);
    let pre_promotion_rollout_path = ephemeral.thread.path.clone();
    assert_eq!(
        ephemeral.thread.parent_thread_id.as_deref(),
        Some(parent_id.as_str())
    );

    let promote_arguments = serde_json::to_string(&json!({"target": "ephemeral_worker"}))?;
    let _promote_call = mount_frodex_tool_response(
        &server,
        PROMOTE_PROMPT,
        "resp-native-ephemeral-worker-promote",
        PROMOTE_CALL_ID,
        "promote_agent",
        &promote_arguments,
    )
    .await;
    let promote_return = mount_final_response(
        &server,
        PROMOTE_CALL_ID,
        "resp-native-ephemeral-worker-promote-return",
        "the originally ephemeral worker is an independent durable thread",
    )
    .await;
    start_turn(&client, 103, &parent_id, PROMOTE_PROMPT).await?;
    wait_for_completed_turn(&mut client, &parent_id).await?;

    let output = promote_return
        .function_call_output_text(PROMOTE_CALL_ID)
        .context("promote_agent did not return the originally ephemeral worker")?;
    let output: Value = serde_json::from_str(&output)
        .with_context(|| format!("promote_agent returned {output:?}"))?;
    assert_eq!(
        output.get("thread_id").and_then(Value::as_str),
        Some(worker_id.as_str())
    );

    let promoted = read_thread(&client, 104, &worker_id).await?;
    let rollout_path = promoted
        .path
        .as_ref()
        .context("promotion must materialize the ephemeral worker's original rollout")?;
    if let Some(original_path) = pre_promotion_rollout_path {
        assert_eq!(rollout_path, &original_path);
    }
    assert_eq!(promoted.id, worker_id);
    assert!(!promoted.ephemeral);
    assert_eq!(promoted.parent_thread_id, None);
    assert_eq!(promoted.session_id, worker_id);
    assert_eq!(promoted.source, SessionSource::Cli);
    assert_eq!(promoted.can_accept_direct_input, Some(true));
    assert_agent_message(&promoted, DESCENDANT_ANSWER);
    let rollout_bytes = std::fs::read(rollout_path)
        .with_context(|| format!("cannot read promoted rollout {}", rollout_path.display()))?;
    assert!(
        list_threads(&client, 105, Some(parent_id.clone()))
            .await?
            .data
            .is_empty(),
        "the former parent must not retain the promoted ephemeral worker"
    );

    let direct_work = mount_final_response(
        &server,
        DIRECT_PROMPT,
        "resp-native-ephemeral-worker-direct",
        DIRECT_ANSWER,
    )
    .await;
    start_turn(&client, 106, &worker_id, DIRECT_PROMPT).await?;
    wait_for_completed_turn(&mut client, &worker_id).await?;
    wait_for_mock_request(&direct_work, "promoted ephemeral worker direct turn").await?;
    let direct = read_thread(&client, 107, &worker_id).await?;
    assert_agent_message(&direct, DESCENDANT_ANSWER);
    assert_agent_message(&direct, DIRECT_ANSWER);
    assert_original_rollout_prefix(rollout_path, &rollout_bytes)?;

    client.shutdown().await?;
    let restarted = start_isolated_app_server(codex_home.path()).await?;
    let resumed: ThreadResumeResponse = request(
        &restarted,
        ClientRequest::ThreadResume {
            request_id: RequestId::Integer(108),
            params: ThreadResumeParams {
                thread_id: worker_id.clone(),
                ..Default::default()
            },
        },
    )
    .await?;
    assert_eq!(resumed.thread.id, worker_id);
    assert_eq!(resumed.thread.path.as_ref(), Some(rollout_path));
    assert!(!resumed.thread.ephemeral);
    assert_eq!(resumed.thread.parent_thread_id, None);
    assert_eq!(resumed.thread.session_id, worker_id);
    assert_eq!(resumed.thread.source, SessionSource::Cli);
    assert_eq!(resumed.thread.can_accept_direct_input, Some(true));
    assert_agent_message(&resumed.thread, DESCENDANT_ANSWER);
    assert_agent_message(&resumed.thread, DIRECT_ANSWER);
    assert_original_rollout_prefix(rollout_path, &rollout_bytes)?;
    restarted.shutdown().await?;
    Ok(())
}

async fn assert_native_thread_adoption_and_promotion(
    adoption_source: AdoptionSource,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::Collab)
        .with_extra_config(THREAD_ADOPTION_FEATURE_CONFIG)
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;

    let _original_response = mount_final_response(
        &server,
        ORIGINAL_PROMPT,
        "resp-original-root",
        ORIGINAL_ANSWER,
    )
    .await;
    let mut client = start_isolated_app_server_with_source(
        codex_home.path(),
        match adoption_source {
            AdoptionSource::VsCode => CoreSessionSource::VSCode,
            AdoptionSource::Loaded
            | AdoptionSource::Stored
            | AdoptionSource::Ephemeral
            | AdoptionSource::DescendantTree
            | AdoptionSource::AncestorCycle
            | AdoptionSource::DisabledAfterAdoption => CoreSessionSource::Cli,
        },
    )
    .await?;
    let original: ThreadStartResponse = request(
        &client,
        ClientRequest::ThreadStart {
            request_id: RequestId::Integer(1),
            params: ThreadStartParams {
                ephemeral: (adoption_source == AdoptionSource::Ephemeral).then_some(true),
                model: (adoption_source == AdoptionSource::AncestorCycle)
                    .then(|| "gpt-5.6-sol".to_string()),
                ..ThreadStartParams::default()
            },
        },
    )
    .await?;
    let original_model = original.model.clone();
    let original_approval_policy = original.approval_policy;
    let original_sandbox = original.sandbox.clone();
    let original_permission_profile = original.active_permission_profile.clone();
    let original_reasoning_effort = original.reasoning_effort;
    let original_source = original.thread.source.clone();
    let original_thread_source = original.thread.thread_source.clone();
    let original_model_provider = original.thread.model_provider.clone();
    let original_cwd = original.thread.cwd.clone();
    let original_id = original.thread.id;
    let initial_rollout_path = original.thread.path;

    start_turn(&client, 2, &original_id, ORIGINAL_PROMPT).await?;
    wait_for_completed_turn(&mut client, &original_id).await?;
    if adoption_source == AdoptionSource::Ephemeral {
        assert!(
            initial_rollout_path.is_none(),
            "an ephemeral adoption source must not already have a materialized rollout"
        );
        wait_for_mock_request(&_original_response, "original ephemeral root turn").await?;
    } else {
        let original_history = read_thread(&client, 3, &original_id).await?;
        assert_eq!(original_history.path, initial_rollout_path);
        assert!(!original_history.ephemeral);
        assert_eq!(original_history.parent_thread_id, None);
        assert_eq!(original_history.source, original_source);
        assert_eq!(original_history.thread_source, original_thread_source);
        assert_user_message(&original_history, ORIGINAL_PROMPT);
        assert_agent_message(&original_history, ORIGINAL_ANSWER);
    }
    let initial_rollout_bytes = initial_rollout_path
        .as_ref()
        .map(|path| {
            std::fs::read(path)
                .with_context(|| format!("cannot read original rollout {}", path.display()))
        })
        .transpose()?;

    if adoption_source == AdoptionSource::Stored {
        let original_path = initial_rollout_path
            .as_ref()
            .context("a stored adoption source must have a persisted rollout")?;
        let original_rollout_bytes = initial_rollout_bytes
            .as_deref()
            .context("a stored adoption source must retain its original rollout bytes")?;
        client.shutdown().await?;
        client = start_isolated_app_server(codex_home.path()).await?;

        let loaded: ThreadLoadedListResponse = request(
            &client,
            ClientRequest::ThreadLoadedList {
                request_id: RequestId::Integer(24),
                params: ThreadLoadedListParams::default(),
            },
        )
        .await?;
        assert!(
            loaded.data.is_empty(),
            "the isolated restarted app server must not load the adoption source"
        );

        let stored = read_thread(&client, 25, &original_id).await?;
        assert_eq!(stored.id, original_id);
        assert_eq!(stored.path.as_ref(), Some(original_path));
        assert_eq!(stored.status, ThreadStatus::NotLoaded);
        assert_eq!(stored.parent_thread_id, None);
        assert_eq!(stored.can_accept_direct_input, None);
        assert_eq!(stored.model_provider, original_model_provider);
        assert_eq!(stored.cwd, original_cwd);
        assert_user_message(&stored, ORIGINAL_PROMPT);
        assert_agent_message(&stored, ORIGINAL_ANSWER);
        assert_original_rollout_prefix(original_path, original_rollout_bytes)?;

        let loaded_after_read: ThreadLoadedListResponse = request(
            &client,
            ClientRequest::ThreadLoadedList {
                request_id: RequestId::Integer(26),
                params: ThreadLoadedListParams::default(),
            },
        )
        .await?;
        assert!(
            loaded_after_read.data.is_empty(),
            "reading a stored thread must not turn cold-thread adoption into live-thread adoption"
        );
    }

    let original_descendant = if matches!(
        adoption_source,
        AdoptionSource::DescendantTree | AdoptionSource::AncestorCycle
    ) {
        let spawn_arguments = serde_json::to_string(&json!({
            "task_name": "worker",
            "message": DESCENDANT_MESSAGE,
        }))?;
        let _descendant_spawn_call = mount_tool_response(
            &server,
            DESCENDANT_SPAWN_PROMPT,
            "resp-native-original-descendant-spawn",
            DESCENDANT_SPAWN_CALL_ID,
            "spawn_agent",
            &spawn_arguments,
        )
        .await;
        let descendant_work = mount_new_agent_response(
            &server,
            &original_id,
            DESCENDANT_MESSAGE,
            "resp-native-original-descendant-work",
            DESCENDANT_ANSWER,
        )
        .await;
        let descendant_spawn_return = mount_final_response(
            &server,
            DESCENDANT_SPAWN_CALL_ID,
            "resp-native-original-descendant-return",
            "the original root spawned its worker",
        )
        .await;

        start_turn(&client, 27, &original_id, DESCENDANT_SPAWN_PROMPT).await?;
        wait_for_completed_turn(&mut client, &original_id).await?;
        let output = descendant_spawn_return
            .function_call_output_text(DESCENDANT_SPAWN_CALL_ID)
            .context("the original root did not return its native worker")?;
        let output: Value = serde_json::from_str(&output)
            .with_context(|| format!("the original root returned {output:?}"))?;
        assert_eq!(
            output.get("task_name").and_then(Value::as_str),
            Some("/root/worker")
        );
        let worker_request =
            wait_for_new_agent_request(&descendant_work, &original_id, DESCENDANT_MESSAGE).await?;
        let child_id = worker_request
            .header("thread-id")
            .or_else(|| worker_request.header("x-client-request-id"))
            .context("the original worker request must contain its thread ID")?;
        assert_eq!(
            worker_request.header("session-id").as_deref(),
            Some(original_id.as_str())
        );
        let child = wait_for_persisted_agent_message(
            &client,
            /*first_request_id*/ 3_000,
            &child_id,
            DESCENDANT_ANSWER,
        )
        .await?;
        let rollout_path = child
            .path
            .context("the original root worker must have a persisted rollout")?;
        let rollout_bytes = std::fs::read(&rollout_path).with_context(|| {
            format!(
                "cannot read original worker rollout {}",
                rollout_path.display()
            )
        })?;
        assert_eq!(
            child.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(child.session_id, original_id);

        let acknowledgment = mount_final_response(
            &server,
            DESCENDANT_ACKNOWLEDGMENT_PROMPT,
            "resp-native-original-descendant-acknowledgment",
            DESCENDANT_ACKNOWLEDGMENT_ANSWER,
        )
        .await;
        start_turn(&client, 38, &original_id, DESCENDANT_ACKNOWLEDGMENT_PROMPT).await?;
        wait_for_completed_turn(&mut client, &original_id).await?;
        wait_for_mock_request(&acknowledgment, "original worker completion acknowledgment").await?;

        Some(OriginalDescendant {
            thread_id: child_id,
            rollout_path,
            rollout_bytes,
        })
    } else {
        None
    };

    if adoption_source == AdoptionSource::AncestorCycle {
        let descendant = original_descendant
            .as_ref()
            .context("ancestor-cycle rejection requires an original root worker")?;
        let followup_arguments = serde_json::to_string(&json!({
            "target": "worker",
            "message": ANCESTOR_CYCLE_MESSAGE,
        }))?;
        let _followup_call = mount_tool_response(
            &server,
            ANCESTOR_CYCLE_PROMPT,
            "resp-native-ancestor-cycle-followup",
            ANCESTOR_CYCLE_FOLLOWUP_CALL_ID,
            "followup_task",
            &followup_arguments,
        )
        .await;
        let root_id = original_id.clone();
        let descendant_id = descendant.thread_id.clone();
        let cycle_arguments = serde_json::to_string(&json!({
            "existing_thread_id": original_id,
            "task_name": "cycle",
            "message": "incorrectly adopt the original ancestor",
        }))?;
        let _cycle_call = responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                request_has_thread_id(request, &descendant_id)
                    && request
                        .headers
                        .get("session-id")
                        .and_then(|value| value.to_str().ok())
                        == Some(root_id.as_str())
                    && request_contains(request, ANCESTOR_CYCLE_MESSAGE)
                    && request_has_agent_message(request)
            },
            responses::sse(vec![
                responses::ev_response_created("resp-native-ancestor-cycle-attempt"),
                responses::ev_function_call_with_namespace(
                    ANCESTOR_CYCLE_ADOPT_CALL_ID,
                    FRODEX_NAMESPACE,
                    "adopt_agent",
                    &cycle_arguments,
                ),
                responses::ev_completed("resp-native-ancestor-cycle-attempt"),
            ]),
        )
        .await;
        let cycle_result = mount_final_response(
            &server,
            ANCESTOR_CYCLE_ADOPT_CALL_ID,
            "resp-native-ancestor-cycle-rejected",
            "the attempt to adopt an ancestor was rejected",
        )
        .await;
        let root_return = mount_final_response(
            &server,
            ANCESTOR_CYCLE_FOLLOWUP_CALL_ID,
            "resp-native-ancestor-cycle-followup-return",
            "the existing worker attempted the ownership cycle",
        )
        .await;

        start_turn(&client, 35, &original_id, ANCESTOR_CYCLE_PROMPT).await?;
        wait_for_completed_turn(&mut client, &original_id).await?;
        let (content, success) =
            wait_for_function_call_output(&root_return, ANCESTOR_CYCLE_FOLLOWUP_CALL_ID).await?;
        assert_ne!(
            success,
            Some(false),
            "the native worker did not receive its ancestor-cycle task: {content:?}"
        );
        let cycle_request =
            wait_for_new_agent_request(&_cycle_call, &original_id, ANCESTOR_CYCLE_MESSAGE).await?;
        let cycle_thread_id = cycle_request
            .header("thread-id")
            .or_else(|| cycle_request.header("x-client-request-id"))
            .context("the ancestor-cycle attempt must identify the original worker")?;
        assert_eq!(cycle_thread_id, descendant.thread_id);
        let (content, success) =
            wait_for_function_call_output(&cycle_result, ANCESTOR_CYCLE_ADOPT_CALL_ID).await?;
        assert_ne!(
            success,
            Some(true),
            "adopting an ancestor must not succeed: {content:?}"
        );
        assert_eq!(
            content.as_deref(),
            Some("collab spawn failed: a thread cannot adopt its own ancestor"),
            "the ownership rejection must identify the ancestor cycle"
        );

        let original = read_thread(&client, 36, &original_id).await?;
        assert_eq!(original.id, original_id);
        assert_eq!(original.parent_thread_id, None);
        assert_eq!(original.session_id, original_id);
        assert_eq!(original.source, original_source);
        assert_user_message(&original, ORIGINAL_PROMPT);
        assert_agent_message(&original, ORIGINAL_ANSWER);
        if let (Some(path), Some(bytes)) = (
            initial_rollout_path.as_ref(),
            initial_rollout_bytes.as_deref(),
        ) {
            assert_eq!(original.path.as_ref(), Some(path));
            assert_original_rollout_prefix(path, bytes)?;
        }

        let original_child = read_thread(&client, 37, &descendant.thread_id).await?;
        assert_eq!(original_child.id, descendant.thread_id);
        assert_eq!(
            original_child.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(original_child.session_id, original_id);
        assert_eq!(original_child.path.as_ref(), Some(&descendant.rollout_path));
        assert_agent_message(&original_child, DESCENDANT_ANSWER);
        assert_original_rollout_prefix(&descendant.rollout_path, &descendant.rollout_bytes)?;
        client.shutdown().await?;
        return Ok(());
    }

    let parent: ThreadStartResponse = request(
        &client,
        ClientRequest::ThreadStart {
            request_id: RequestId::Integer(4),
            params: ThreadStartParams::default(),
        },
    )
    .await?;
    let parent_id = parent.thread.id;
    let parent_session_id = parent.thread.session_id;

    let adopt_args = serde_json::to_string(&json!({
        "existing_thread_id": original_id,
        "task_name": "adopted_worker",
        "message": ADOPT_MESSAGE,
    }))?;
    let _adopt_call = mount_frodex_tool_response(
        &server,
        ADOPT_PROMPT,
        "resp-native-adopt-call",
        ADOPT_CALL_ID,
        "adopt_agent",
        &adopt_args,
    )
    .await;
    let adopted_work = mount_agent_response(
        &server,
        &original_id,
        &parent_session_id,
        ADOPT_MESSAGE,
        "resp-native-adopted-worker",
        ADOPT_ANSWER,
    )
    .await;
    let adopt_return = mount_final_response(
        &server,
        ADOPT_CALL_ID,
        "resp-native-adopt-return",
        "the existing thread was adopted",
    )
    .await;

    start_turn(&client, 5, &parent_id, ADOPT_PROMPT).await?;
    wait_for_completed_turn(&mut client, &parent_id).await?;
    let adoption_output = adopt_return
        .function_call_output_text(ADOPT_CALL_ID)
        .context("spawn_agent did not return a result for the existing thread")?;
    let adoption_output: Value = serde_json::from_str(&adoption_output)
        .with_context(|| format!("spawn_agent returned {adoption_output:?}"))?;
    assert_eq!(
        adoption_output.get("task_name").and_then(Value::as_str),
        Some("/root/adopted_worker")
    );
    wait_for_mock_request(&adopted_work, "adopted worker assignment").await?;

    let adopted = wait_for_persisted_agent_message(
        &client,
        /*first_request_id*/ 1_000,
        &original_id,
        ADOPT_ANSWER,
    )
    .await?;
    let original_path = adopted
        .path
        .clone()
        .context("adoption must materialize the original thread's persisted rollout")?;
    if let Some(initial_path) = initial_rollout_path {
        assert_eq!(original_path, initial_path);
    }
    let original_rollout_bytes = match initial_rollout_bytes {
        Some(original_rollout_bytes) => original_rollout_bytes,
        None => std::fs::read(&original_path).with_context(|| {
            format!(
                "cannot read the newly materialized rollout {}",
                original_path.display()
            )
        })?,
    };
    assert_eq!(adopted.id, original_id);
    assert_eq!(adopted.path.as_ref(), Some(&original_path));
    assert!(!adopted.ephemeral);
    assert_eq!(adopted.model_provider, original_model_provider);
    assert_eq!(adopted.cwd, original_cwd);
    assert_eq!(
        adopted.parent_thread_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert_eq!(adopted.session_id, parent_session_id);
    assert!(matches!(adopted.source, SessionSource::SubAgent(_)));
    assert_eq!(adopted.thread_source, Some(ThreadSource::Subagent));
    assert_eq!(adopted.can_accept_direct_input, Some(false));
    assert_user_message(&adopted, ORIGINAL_PROMPT);
    assert_agent_message(&adopted, ORIGINAL_ANSWER);
    assert_original_rollout_prefix(&original_path, &original_rollout_bytes)?;

    if adoption_source == AdoptionSource::DisabledAfterAdoption {
        client.shutdown().await?;
        MockResponsesConfig::new(&server.uri())
            .enable_feature(Feature::Collab)
            .with_extra_config(THREAD_ADOPTION_DISABLED_CONFIG)
            .write(codex_home.path())?;
        let restarted = start_isolated_app_server(codex_home.path()).await?;

        let stored = read_thread(&restarted, 40, &original_id).await?;
        assert_eq!(stored.status, ThreadStatus::NotLoaded);
        assert_eq!(stored.parent_thread_id.as_deref(), Some(parent_id.as_str()));
        assert_eq!(stored.session_id, parent_session_id);
        assert!(matches!(stored.source, SessionSource::SubAgent(_)));

        let resumed: ThreadResumeResponse = request(
            &restarted,
            ClientRequest::ThreadResume {
                request_id: RequestId::Integer(41),
                params: ThreadResumeParams {
                    thread_id: "ignored-when-path-is-set".to_string(),
                    path: Some(original_path.clone()),
                    ..Default::default()
                },
            },
        )
        .await?;
        assert_eq!(resumed.thread.id, original_id);
        assert_eq!(
            resumed.thread.parent_thread_id.as_deref(),
            Some(parent_id.as_str())
        );
        assert_eq!(resumed.thread.session_id, parent_session_id);
        assert!(matches!(resumed.thread.source, SessionSource::SubAgent(_)));
        restarted.shutdown().await?;
        return Ok(());
    }

    if let Some(descendant) = &original_descendant {
        let adopted_descendant = read_thread(&client, 29, &descendant.thread_id).await?;
        assert_eq!(adopted_descendant.id, descendant.thread_id);
        assert_eq!(
            adopted_descendant.path.as_ref(),
            Some(&descendant.rollout_path)
        );
        assert_eq!(
            adopted_descendant.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(adopted_descendant.session_id, parent_session_id);
        assert_eq!(adopted_descendant.status, ThreadStatus::NotLoaded);
        assert_agent_message(&adopted_descendant, DESCENDANT_ANSWER);
        assert_original_rollout_prefix(&descendant.rollout_path, &descendant.rollout_bytes)?;

        let loaded: ThreadLoadedListResponse = request(
            &client,
            ClientRequest::ThreadLoadedList {
                request_id: RequestId::Integer(30),
                params: ThreadLoadedListParams::default(),
            },
        )
        .await?;
        assert!(
            !loaded.data.contains(&descendant.thread_id),
            "adopting a root must transfer its existing worker without eagerly loading it"
        );
    }

    let adopted_children = list_threads(&client, 7, Some(parent_id.clone())).await?;
    assert_eq!(
        adopted_children
            .data
            .iter()
            .map(|thread| thread.id.as_str())
            .collect::<Vec<_>>(),
        vec![original_id.as_str()]
    );

    let direct_while_adopted = client
        .request(ClientRequest::TurnStart {
            request_id: RequestId::Integer(8),
            params: turn_params(&original_id, "reject direct adopted worker input"),
        })
        .await?;
    assert!(
        direct_while_adopted.is_err(),
        "a native V2 adopted worker must reject direct app-server input"
    );

    let _list_adopted_call = mount_tool_response(
        &server,
        LIST_ADOPTED_PROMPT,
        "resp-native-list-adopted-call",
        LIST_ADOPTED_CALL_ID,
        "list_agents",
        "{}",
    )
    .await;
    let list_adopted_result = mount_final_response(
        &server,
        LIST_ADOPTED_CALL_ID,
        "resp-native-list-adopted-return",
        "the adopted worker appears in the native agent registry",
    )
    .await;
    start_turn(&client, 22, &parent_id, LIST_ADOPTED_PROMPT).await?;
    wait_for_completed_turn(&mut client, &parent_id).await?;
    assert_listed_agent(
        &list_adopted_result,
        LIST_ADOPTED_CALL_ID,
        "/root/adopted_worker",
        /*should_be_listed*/ true,
    )?;
    if original_descendant.is_some() {
        assert_listed_agent(
            &list_adopted_result,
            LIST_ADOPTED_CALL_ID,
            "/root/adopted_worker/worker",
            /*should_be_listed*/ true,
        )?;
    }

    let followup_args = serde_json::to_string(&json!({
        "target": "adopted_worker",
        "message": FOLLOWUP_MESSAGE,
    }))?;
    let _followup_call = mount_tool_response(
        &server,
        FOLLOWUP_PROMPT,
        "resp-native-followup-call",
        FOLLOWUP_CALL_ID,
        "followup_task",
        &followup_args,
    )
    .await;
    let followed_up_work = mount_agent_response(
        &server,
        &original_id,
        &parent_session_id,
        FOLLOWUP_MESSAGE,
        "resp-native-followed-up-worker",
        FOLLOWUP_ANSWER,
    )
    .await;
    let followup_return = mount_final_response(
        &server,
        FOLLOWUP_CALL_ID,
        "resp-native-followup-return",
        "the adopted worker received its follow-up",
    )
    .await;

    start_turn(&client, 9, &parent_id, FOLLOWUP_PROMPT).await?;
    wait_for_completed_turn(&mut client, &parent_id).await?;
    let (followup_content, followup_success) = followup_return
        .single_request()
        .function_call_output_content_and_success(FOLLOWUP_CALL_ID)
        .context("followup_task did not return a function-call result")?;
    assert_ne!(
        followup_success,
        Some(false),
        "followup_task failed: {followup_content:?}"
    );
    wait_for_mock_request(&followed_up_work, "adopted worker follow-up").await?;

    let followed_up = wait_for_persisted_agent_message(
        &client,
        /*first_request_id*/ 2_000,
        &original_id,
        FOLLOWUP_ANSWER,
    )
    .await?;
    assert_eq!(followed_up.id, original_id);
    assert_eq!(followed_up.path.as_ref(), Some(&original_path));
    assert_agent_message(&followed_up, FOLLOWUP_ANSWER);

    if let Some(descendant) = &original_descendant {
        let arguments = serde_json::to_string(&json!({
            "target": "adopted_worker/worker",
            "message": DESCENDANT_FOLLOWUP_MESSAGE,
        }))?;
        let _descendant_followup_call = mount_tool_response(
            &server,
            DESCENDANT_FOLLOWUP_PROMPT,
            "resp-native-adopted-descendant-followup-call",
            DESCENDANT_FOLLOWUP_CALL_ID,
            "followup_task",
            &arguments,
        )
        .await;
        let descendant_followup = mount_agent_response(
            &server,
            &descendant.thread_id,
            &parent_session_id,
            DESCENDANT_FOLLOWUP_MESSAGE,
            "resp-native-adopted-descendant-followup",
            DESCENDANT_FOLLOWUP_ANSWER,
        )
        .await;
        let descendant_followup_return = mount_final_response(
            &server,
            DESCENDANT_FOLLOWUP_CALL_ID,
            "resp-native-adopted-descendant-followup-return",
            "the original worker received its adopted follow-up",
        )
        .await;

        start_turn(&client, 31, &parent_id, DESCENDANT_FOLLOWUP_PROMPT).await?;
        wait_for_completed_turn(&mut client, &parent_id).await?;
        let (content, success) = descendant_followup_return
            .single_request()
            .function_call_output_content_and_success(DESCENDANT_FOLLOWUP_CALL_ID)
            .context("the adopted worker follow-up did not return a function-call result")?;
        assert_ne!(
            success,
            Some(false),
            "followup_task failed for the adopted worker: {content:?}"
        );
        wait_for_mock_request(&descendant_followup, "adopted root worker follow-up").await?;

        let followed_up_descendant = wait_for_persisted_agent_message(
            &client,
            /*first_request_id*/ 4_000,
            &descendant.thread_id,
            DESCENDANT_FOLLOWUP_ANSWER,
        )
        .await?;
        assert_eq!(followed_up_descendant.id, descendant.thread_id);
        assert_eq!(
            followed_up_descendant.path.as_ref(),
            Some(&descendant.rollout_path)
        );
        assert_eq!(followed_up_descendant.session_id, parent_session_id);
        assert_agent_message(&followed_up_descendant, DESCENDANT_ANSWER);
        assert_original_rollout_prefix(&descendant.rollout_path, &descendant.rollout_bytes)?;

        let acknowledgment_arguments = serde_json::to_string(&json!({
            "target": "adopted_worker",
            "message": DESCENDANT_COMPLETION_MESSAGE,
        }))?;
        let _acknowledgment_call = mount_tool_response(
            &server,
            DESCENDANT_COMPLETION_PROMPT,
            "resp-native-adopted-descendant-completion-call",
            DESCENDANT_COMPLETION_CALL_ID,
            "followup_task",
            &acknowledgment_arguments,
        )
        .await;
        let acknowledgment_work = mount_agent_response(
            &server,
            &original_id,
            &parent_session_id,
            DESCENDANT_COMPLETION_MESSAGE,
            "resp-native-adopted-descendant-completion",
            DESCENDANT_COMPLETION_ANSWER,
        )
        .await;
        let acknowledgment_return = mount_final_response(
            &server,
            DESCENDANT_COMPLETION_CALL_ID,
            "resp-native-adopted-descendant-completion-return",
            "the adopted root consumed its worker's completion",
        )
        .await;

        start_turn(&client, 39, &parent_id, DESCENDANT_COMPLETION_PROMPT).await?;
        wait_for_completed_turn(&mut client, &parent_id).await?;
        let (content, success) =
            wait_for_function_call_output(&acknowledgment_return, DESCENDANT_COMPLETION_CALL_ID)
                .await?;
        assert_ne!(
            success,
            Some(false),
            "the adopted root did not receive its worker-completion acknowledgment: {content:?}"
        );
        let acknowledged_root = wait_for_persisted_agent_message(
            &client,
            /*first_request_id*/ 6_000,
            &original_id,
            DESCENDANT_COMPLETION_ANSWER,
        )
        .await?;
        assert_eq!(acknowledged_root.id, original_id);
        assert_eq!(acknowledged_root.session_id, parent_session_id);
        assert_agent_message(&acknowledged_root, ORIGINAL_ANSWER);
        assert_agent_message(&acknowledged_root, FOLLOWUP_ANSWER);
        let acknowledgment_request = acknowledgment_work
            .requests()
            .into_iter()
            .find(|request| {
                request
                    .header("thread-id")
                    .or_else(|| request.header("x-client-request-id"))
                    .as_deref()
                    == Some(original_id.as_str())
                    && request.header("session-id").as_deref() == Some(parent_session_id.as_str())
                    && request.body_contains_text(DESCENDANT_COMPLETION_MESSAGE)
                    && request.body_contains_text(DESCENDANT_FOLLOWUP_ANSWER)
            })
            .context("the adopted root did not consume its worker's exact completion message")?;
        assert_eq!(
            acknowledgment_request.header("session-id").as_deref(),
            Some(parent_session_id.as_str())
        );
        assert_original_rollout_prefix(&original_path, &original_rollout_bytes)?;
    }

    let promote_args = serde_json::to_string(&json!({"target": "adopted_worker"}))?;
    let _promote_call = mount_frodex_tool_response(
        &server,
        PROMOTE_PROMPT,
        "resp-native-promote-call",
        PROMOTE_CALL_ID,
        "promote_agent",
        &promote_args,
    )
    .await;
    let promote_return = mount_final_response(
        &server,
        PROMOTE_CALL_ID,
        "resp-native-promote-return",
        "the adopted worker is an independent root thread",
    )
    .await;

    start_turn(&client, 11, &parent_id, PROMOTE_PROMPT).await?;
    wait_for_completed_turn(&mut client, &parent_id).await?;
    let (promote_content, promote_success) = promote_return
        .single_request()
        .function_call_output_content_and_success(PROMOTE_CALL_ID)
        .context("promote_agent did not return a function-call result")?;
    assert_ne!(
        promote_success,
        Some(false),
        "promote_agent failed: {promote_content:?}"
    );
    let promote_output = promote_return
        .function_call_output_text(PROMOTE_CALL_ID)
        .context("promote_agent did not return the promoted thread ID")?;
    let promote_output: Value = serde_json::from_str(&promote_output)
        .with_context(|| format!("promote_agent returned {promote_output:?}"))?;
    assert_eq!(
        promote_output.get("thread_id").and_then(Value::as_str),
        Some(original_id.as_str())
    );

    let promoted = read_thread(&client, 12, &original_id).await?;
    assert_eq!(promoted.id, original_id);
    assert_eq!(promoted.path.as_ref(), Some(&original_path));
    assert_eq!(promoted.model_provider, original_model_provider);
    assert_eq!(promoted.cwd, original_cwd);
    assert_eq!(promoted.parent_thread_id, None);
    assert_eq!(promoted.session_id, original_id);
    assert_eq!(promoted.source, original_source);
    assert_eq!(
        promoted.thread_source, original_thread_source,
        "promotion must restore the original root's thread classification"
    );
    assert_eq!(promoted.can_accept_direct_input, Some(true));
    assert_user_message(&promoted, ORIGINAL_PROMPT);
    assert_agent_message(&promoted, ORIGINAL_ANSWER);
    assert_agent_message(&promoted, FOLLOWUP_ANSWER);
    assert_original_rollout_prefix(&original_path, &original_rollout_bytes)?;

    if let Some(descendant) = &original_descendant {
        let promoted_descendant = read_thread(&client, 32, &descendant.thread_id).await?;
        assert_eq!(promoted_descendant.id, descendant.thread_id);
        assert_eq!(
            promoted_descendant.path.as_ref(),
            Some(&descendant.rollout_path)
        );
        assert_eq!(
            promoted_descendant.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(promoted_descendant.session_id, original_id);
        assert_agent_message(&promoted_descendant, DESCENDANT_ANSWER);
        assert_agent_message(&promoted_descendant, DESCENDANT_FOLLOWUP_ANSWER);
        assert_original_rollout_prefix(&descendant.rollout_path, &descendant.rollout_bytes)?;

        let promoted_children = list_threads(&client, 33, Some(original_id.clone())).await?;
        assert_eq!(
            promoted_children
                .data
                .iter()
                .map(|thread| thread.id.as_str())
                .collect::<Vec<_>>(),
            vec![descendant.thread_id.as_str()]
        );
    }

    let remaining_children = list_threads(&client, 13, Some(parent_id.clone())).await?;
    assert!(remaining_children.data.is_empty());
    let interactive = list_threads(&client, 14, None).await?;
    assert!(interactive.data.iter().any(|thread| {
        thread.id == original_id
            && thread.parent_thread_id.is_none()
            && thread.session_id == original_id
    }));

    let _list_promoted_call = mount_tool_response(
        &server,
        LIST_PROMOTED_PROMPT,
        "resp-native-list-promoted-call",
        LIST_PROMOTED_CALL_ID,
        "list_agents",
        "{}",
    )
    .await;
    let list_promoted_result = mount_final_response(
        &server,
        LIST_PROMOTED_CALL_ID,
        "resp-native-list-promoted-return",
        "the promoted thread no longer appears in the native agent registry",
    )
    .await;
    start_turn(&client, 23, &parent_id, LIST_PROMOTED_PROMPT).await?;
    wait_for_completed_turn(&mut client, &parent_id).await?;
    assert_listed_agent(
        &list_promoted_result,
        LIST_PROMOTED_CALL_ID,
        "/root/adopted_worker",
        /*should_be_listed*/ false,
    )?;
    if original_descendant.is_some() {
        assert_listed_agent(
            &list_promoted_result,
            LIST_PROMOTED_CALL_ID,
            "/root/adopted_worker/worker",
            /*should_be_listed*/ false,
        )?;
    }

    let parent_before_direct = read_thread(&client, 15, &parent_id).await?;
    let direct_work = mount_final_response(
        &server,
        DIRECT_PROMPT,
        "resp-native-promoted-direct",
        DIRECT_ANSWER,
    )
    .await;
    start_turn(&client, 16, &original_id, DIRECT_PROMPT).await?;
    wait_for_completed_turn(&mut client, &original_id).await?;
    wait_for_mock_request(&direct_work, "promoted root direct turn").await?;

    let parent_after_direct = read_thread(&client, 17, &parent_id).await?;
    assert_eq!(parent_after_direct.turns, parent_before_direct.turns);
    let direct_history = read_thread(&client, 18, &original_id).await?;
    assert_agent_message(&direct_history, DIRECT_ANSWER);

    client.shutdown().await?;
    let mut restarted = start_isolated_app_server(codex_home.path()).await?;
    let resumed: ThreadResumeResponse = request(
        &restarted,
        ClientRequest::ThreadResume {
            request_id: RequestId::Integer(19),
            params: ThreadResumeParams {
                thread_id: original_id.clone(),
                ..Default::default()
            },
        },
    )
    .await?;
    assert_eq!(resumed.model, original_model);
    assert_eq!(resumed.model_provider, original_model_provider);
    assert_eq!(resumed.cwd, original_cwd);
    assert_eq!(resumed.approval_policy, original_approval_policy);
    assert_eq!(resumed.sandbox, original_sandbox);
    assert_eq!(
        resumed.active_permission_profile,
        original_permission_profile
    );
    assert_eq!(resumed.reasoning_effort, original_reasoning_effort);
    assert_eq!(resumed.thread.id, original_id);
    assert_eq!(resumed.thread.path.as_ref(), Some(&original_path));
    assert_eq!(resumed.thread.model_provider, original_model_provider);
    assert_eq!(resumed.thread.cwd, original_cwd);
    assert_eq!(resumed.thread.parent_thread_id, None);
    assert_eq!(resumed.thread.session_id, original_id);
    assert_eq!(resumed.thread.source, original_source);
    assert_eq!(
        resumed.thread.thread_source, original_thread_source,
        "restart must preserve the original root's thread classification"
    );
    assert_user_message(&resumed.thread, ORIGINAL_PROMPT);
    assert_agent_message(&resumed.thread, ORIGINAL_ANSWER);
    assert_agent_message(&resumed.thread, FOLLOWUP_ANSWER);
    assert_agent_message(&resumed.thread, DIRECT_ANSWER);
    assert_original_rollout_prefix(&original_path, &original_rollout_bytes)?;

    if let Some(descendant) = &original_descendant {
        let restarted_descendant = read_thread(&restarted, 34, &descendant.thread_id).await?;
        assert_eq!(restarted_descendant.id, descendant.thread_id);
        assert_eq!(
            restarted_descendant.path.as_ref(),
            Some(&descendant.rollout_path)
        );
        assert_eq!(
            restarted_descendant.parent_thread_id.as_deref(),
            Some(original_id.as_str())
        );
        assert_eq!(restarted_descendant.session_id, original_id);
        assert_agent_message(&restarted_descendant, DESCENDANT_ANSWER);
        assert_agent_message(&restarted_descendant, DESCENDANT_FOLLOWUP_ANSWER);
        assert_original_rollout_prefix(&descendant.rollout_path, &descendant.rollout_bytes)?;
    }

    let restart_work = mount_final_response(
        &server,
        RESTART_PROMPT,
        "resp-native-promoted-restart",
        RESTART_ANSWER,
    )
    .await;
    start_turn(&restarted, 20, &original_id, RESTART_PROMPT).await?;
    wait_for_completed_turn(&mut restarted, &original_id).await?;
    wait_for_mock_request(&restart_work, "promoted root after app-server restart").await?;

    let restarted_history = read_thread(&restarted, 21, &original_id).await?;
    assert_eq!(restarted_history.path.as_ref(), Some(&original_path));
    assert_agent_message(&restarted_history, RESTART_ANSWER);
    restarted.shutdown().await?;
    Ok(())
}

async fn start_isolated_app_server(codex_home: &Path) -> Result<InProcessClientHandle> {
    start_isolated_app_server_with_source(codex_home, CoreSessionSource::Cli).await
}

async fn start_isolated_app_server_with_source(
    codex_home: &Path,
    session_source: CoreSessionSource,
) -> Result<InProcessClientHandle> {
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
        session_source,
        enable_codex_api_key_env: false,
        initialize: InitializeParams {
            client_info: ClientInfo {
                name: "native-thread-adoption-test".to_string(),
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

fn turn_params(thread_id: &str, prompt: &str) -> TurnStartParams {
    TurnStartParams {
        thread_id: thread_id.to_string(),
        input: vec![UserInput::Text {
            text: prompt.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    }
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
            params: turn_params(thread_id, prompt),
        },
    )
    .await?;
    Ok(())
}

async fn wait_for_completed_turn(
    client: &mut InProcessClientHandle,
    thread_id: &str,
) -> Result<()> {
    timeout(TIMEOUT, async {
        loop {
            let event = client
                .next_event()
                .await
                .context("isolated app-server stopped before completing the turn")?;
            if let InProcessServerEvent::ServerNotification(notification) = event
                && let ServerNotification::TurnCompleted(completed) = notification.as_ref()
                && completed.thread_id == thread_id
            {
                assert_eq!(completed.turn.status, TurnStatus::Completed);
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;
    Ok(())
}

async fn read_thread(
    client: &InProcessClientHandle,
    request_id: i64,
    thread_id: &str,
) -> Result<Thread> {
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
    Ok(response.thread)
}

async fn wait_for_persisted_agent_message(
    client: &InProcessClientHandle,
    first_request_id: i64,
    thread_id: &str,
    expected: &str,
) -> Result<Thread> {
    timeout(TIMEOUT, async {
        let mut request_id = first_request_id;
        loop {
            let thread = read_thread(client, request_id, thread_id).await?;
            if thread.turns.iter().flat_map(|turn| &turn.items).any(
                |item| matches!(item, ThreadItem::AgentMessage { text, .. } if text == expected),
            ) {
                return Ok::<Thread, anyhow::Error>(thread);
            }
            request_id += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| {
        format!("timed out waiting for thread {thread_id} to persist agent message {expected:?}")
    })?
}

async fn list_threads(
    client: &InProcessClientHandle,
    request_id: i64,
    parent_thread_id: Option<String>,
) -> Result<ThreadListResponse> {
    request(
        client,
        ClientRequest::ThreadList {
            request_id: RequestId::Integer(request_id),
            params: ThreadListParams {
                cursor: None,
                limit: Some(10),
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                section_id: None,
                cwd: None,
                use_state_db_only: false,
                search_term: None,
                parent_thread_id,
                ancestor_thread_id: None,
            },
        },
    )
    .await
}

fn assert_user_message(thread: &Thread, expected: &str) {
    assert!(
        thread.turns.iter().flat_map(|turn| &turn.items).any(|item| {
            matches!(item, ThreadItem::UserMessage { content, .. }
                if content.iter().any(|input| matches!(input, UserInput::Text { text, .. } if text == expected)))
        }),
        "thread {} should retain user message {expected:?}",
        thread.id
    );
}

fn assert_agent_message(thread: &Thread, expected: &str) {
    assert!(
        thread
            .turns
            .iter()
            .flat_map(|turn| &turn.items)
            .any(|item| matches!(item, ThreadItem::AgentMessage { text, .. } if text == expected)),
        "thread {} should retain agent message {expected:?}",
        thread.id
    );
}

fn assert_original_rollout_prefix(path: &Path, original: &[u8]) -> Result<()> {
    let current =
        std::fs::read(path).with_context(|| format!("cannot read rollout {}", path.display()))?;
    assert!(
        current.starts_with(original),
        "adoption or promotion rewrote the original rollout {}",
        path.display()
    );
    Ok(())
}

async fn mount_final_response(
    server: &wiremock::MockServer,
    matching_text: &'static str,
    response_id: &'static str,
    answer: &'static str,
) -> responses::ResponseMock {
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| request_contains(request, matching_text),
        responses::sse(vec![
            responses::ev_response_created(response_id),
            responses::ev_assistant_message(response_id, answer),
            responses::ev_completed(response_id),
        ]),
    )
    .await
}

async fn mount_tool_response(
    server: &wiremock::MockServer,
    matching_text: &'static str,
    response_id: &'static str,
    call_id: &'static str,
    tool_name: &'static str,
    arguments: &str,
) -> responses::ResponseMock {
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| request_contains(request, matching_text),
        responses::sse(vec![
            responses::ev_response_created(response_id),
            responses::ev_function_call_with_namespace(
                call_id,
                COLLABORATION_NAMESPACE,
                tool_name,
                arguments,
            ),
            responses::ev_completed(response_id),
        ]),
    )
    .await
}

async fn mount_frodex_tool_response(
    server: &wiremock::MockServer,
    matching_text: &'static str,
    response_id: &'static str,
    call_id: &'static str,
    tool_name: &'static str,
    arguments: &str,
) -> responses::ResponseMock {
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| request_contains(request, matching_text),
        responses::sse(vec![
            responses::ev_response_created(response_id),
            responses::ev_function_call_with_namespace(
                call_id,
                FRODEX_NAMESPACE,
                tool_name,
                arguments,
            ),
            responses::ev_completed(response_id),
        ]),
    )
    .await
}

async fn mount_agent_response(
    server: &wiremock::MockServer,
    expected_thread_id: &str,
    expected_session_id: &str,
    matching_text: &'static str,
    response_id: &'static str,
    answer: &'static str,
) -> responses::ResponseMock {
    let expected_thread_id = expected_thread_id.to_string();
    let expected_session_id = expected_session_id.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            request_has_thread_id(request, &expected_thread_id)
                && request
                    .headers
                    .get("session-id")
                    .and_then(|value| value.to_str().ok())
                    == Some(expected_session_id.as_str())
                && request_contains(request, matching_text)
                && request_has_agent_message(request)
        },
        responses::sse(vec![
            responses::ev_response_created(response_id),
            responses::ev_assistant_message(response_id, answer),
            responses::ev_completed(response_id),
        ]),
    )
    .await
}

async fn mount_new_agent_response(
    server: &wiremock::MockServer,
    parent_thread_id: &str,
    matching_text: &'static str,
    response_id: &'static str,
    answer: &'static str,
) -> responses::ResponseMock {
    let parent_thread_id = parent_thread_id.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            !request_has_thread_id(request, &parent_thread_id)
                && request
                    .headers
                    .get("session-id")
                    .and_then(|value| value.to_str().ok())
                    == Some(parent_thread_id.as_str())
                && request_contains(request, matching_text)
                && request_has_agent_message(request)
        },
        responses::sse(vec![
            responses::ev_response_created(response_id),
            responses::ev_assistant_message(response_id, answer),
            responses::ev_completed(response_id),
        ]),
    )
    .await
}

async fn wait_for_new_agent_request(
    response: &responses::ResponseMock,
    parent_thread_id: &str,
    matching_text: &str,
) -> Result<responses::ResponsesRequest> {
    timeout(TIMEOUT, async {
        loop {
            if let Ok(request) = matched_new_agent_request(response, parent_thread_id, matching_text)
            {
                return Ok::<responses::ResponsesRequest, anyhow::Error>(request);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "timed out waiting for parent {parent_thread_id}'s actual worker request containing {matching_text:?}"
        )
    })?
}

fn matched_new_agent_request(
    response: &responses::ResponseMock,
    parent_thread_id: &str,
    matching_text: &str,
) -> Result<responses::ResponsesRequest> {
    let mut requests = response.requests().into_iter().filter(|request| {
        let thread_id = request
            .header("thread-id")
            .or_else(|| request.header("x-client-request-id"));
        thread_id
            .as_deref()
            .is_some_and(|thread_id| thread_id != parent_thread_id)
            && request.header("session-id").as_deref() == Some(parent_thread_id)
            && request.body_contains_text(matching_text)
            && request.body_json()["input"]
                .as_array()
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item.get("type").and_then(Value::as_str) == Some("agent_message")
                    })
                })
    });
    let request = requests
        .next()
        .context("the mock did not capture the new worker's actual model request")?;
    if requests.next().is_some() {
        anyhow::bail!("the mock captured more than one matching new worker request");
    }
    Ok(request)
}

async fn wait_for_function_call_output(
    response: &responses::ResponseMock,
    call_id: &str,
) -> Result<(Option<String>, Option<bool>)> {
    timeout(TIMEOUT, async {
        loop {
            for request in response.requests() {
                let has_output = request
                    .inputs_of_type("function_call_output")
                    .iter()
                    .any(|item| item.get("call_id").and_then(Value::as_str) == Some(call_id));
                if has_output {
                    return request
                        .function_call_output_content_and_success(call_id)
                        .context("the captured function-call output had an invalid result");
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| {
        let observed = response
            .requests()
            .iter()
            .map(|request| {
                let call_ids = request
                    .inputs_of_type("function_call_output")
                    .iter()
                    .filter_map(|item| {
                        item.get("call_id")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .collect::<Vec<_>>();
                (
                    request.header("thread-id"),
                    request.header("x-client-request-id"),
                    request.header("session-id"),
                    call_ids,
                )
            })
            .collect::<Vec<_>>();
        format!(
            "timed out waiting for function-call output {call_id:?}; observed thread, request, session and call IDs: {observed:?}"
        )
    })?
}

fn request_contains(request: &wiremock::Request, expected: &str) -> bool {
    String::from_utf8_lossy(&request.body).contains(expected)
}

fn request_has_thread_id(request: &wiremock::Request, expected: &str) -> bool {
    ["thread-id", "x-client-request-id"]
        .into_iter()
        .any(|header| {
            request
                .headers
                .get(header)
                .and_then(|value| value.to_str().ok())
                == Some(expected)
        })
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

fn assert_listed_agent(
    response: &responses::ResponseMock,
    call_id: &str,
    expected_agent_name: &str,
    should_be_listed: bool,
) -> Result<()> {
    let output = response
        .function_call_output_text(call_id)
        .with_context(|| format!("missing list_agents result for {call_id}"))?;
    let output: Value = serde_json::from_str(&output)?;
    let agents = output
        .get("agents")
        .and_then(Value::as_array)
        .context("list_agents must return a structured agents array")?;
    let is_listed = agents
        .iter()
        .any(|agent| agent.get("agent_name").and_then(Value::as_str) == Some(expected_agent_name));
    assert_eq!(
        is_listed, should_be_listed,
        "list_agents returned {agents:?} for expected agent {expected_agent_name:?}"
    );
    Ok(())
}

async fn wait_for_mock_request(mock: &responses::ResponseMock, description: &str) -> Result<()> {
    timeout(TIMEOUT, async {
        while mock.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {description}"))?;
    Ok(())
}
