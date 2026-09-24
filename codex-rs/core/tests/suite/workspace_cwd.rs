use std::path::Path;
use std::process::Command;

use anyhow::Result;
use codex_core::TurnInputRequest;
use codex_core::config::Config;
use codex_exec_server::LOCAL_ENVIRONMENT_ID;
use codex_extension_api::ExtensionRegistry;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_features::Feature;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EnvironmentConfigState;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::user_input::UserInput;
use codex_skills_extension::SkillsExtensionConfig;
use codex_skills_extension::install;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::test_codex;
use core_test_support::test_codex::turn_permission_fields;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::oneshot;

fn skills_extensions() -> std::sync::Arc<ExtensionRegistry<Config>> {
    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    install(&mut extensions, |config: &Config| SkillsExtensionConfig {
        include_instructions: config.include_skill_instructions,
        max_context_tokens: config.skill_max_context_tokens,
        bundled_skills_enabled: config.bundled_skills_enabled(),
        orchestrator_skills_enabled: config.orchestrator_skills_enabled,
        shadow_selection_enabled: config.features.enabled(Feature::SkillSearch),
    });
    std::sync::Arc::new(extensions.build())
}

struct LinkedWorktreeFixture {
    _temp_dir: TempDir,
    primary: AbsolutePathBuf,
    linked: AbsolutePathBuf,
}

fn linked_worktree_fixture() -> LinkedWorktreeFixture {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let primary = temp_dir.path().join("primary");
    let linked = temp_dir.path().join("linked");
    std::fs::create_dir(&primary).expect("create primary checkout");
    run_git(&primary, &["init", "-q"]);
    run_git(&primary, &["config", "user.email", "codex@example.com"]);
    run_git(&primary, &["config", "user.name", "Codex Test"]);
    std::fs::write(primary.join("README.md"), "test\n").expect("write initial file");
    run_git(&primary, &["add", "README.md"]);
    run_git(&primary, &["commit", "-qm", "initial"]);
    run_git(
        &primary,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "linked-test",
            linked.to_str().expect("linked path is UTF-8"),
        ],
    );

    LinkedWorktreeFixture {
        primary: absolute_canonical(&primary),
        linked: absolute_canonical(&linked),
        _temp_dir: temp_dir,
    }
}

fn run_git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn absolute_canonical(path: &Path) -> AbsolutePathBuf {
    AbsolutePathBuf::try_from(std::fs::canonicalize(path).expect("canonical path"))
        .expect("absolute path")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_cwd_switches_context_before_the_next_model_step() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CWD_CALL_ID: &str = "set-cwd";
    const REPEAT_CWD_CALL_ID: &str = "repeat-set-cwd";
    const PWD_CALL_ID: &str = "pwd-after-set-cwd";

    let fixture = linked_worktree_fixture();
    std::fs::write(
        fixture.linked.join("AGENTS.md"),
        "Use linked immediate instructions.\n",
    )?;
    let skill_dir = fixture.linked.join(".agents/skills/linked-cwd");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: linked-cwd\ndescription: linked worktree skill\n---\n\n# Linked cwd\n",
    )?;
    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call_with_namespace(
                    CWD_CALL_ID,
                    "workspace",
                    "set_cwd",
                    &json!({ "path": fixture.linked }).to_string(),
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_function_call_with_namespace(
                    REPEAT_CWD_CALL_ID,
                    "workspace",
                    "set_cwd",
                    &json!({ "path": fixture.linked }).to_string(),
                ),
                ev_completed("resp-2"),
            ]),
            sse(vec![
                ev_response_created("resp-3"),
                ev_function_call(
                    PWD_CALL_ID,
                    "exec_command",
                    &json!({ "cmd": "pwd", "max_output_tokens": 1000 }).to_string(),
                ),
                ev_completed("resp-3"),
            ]),
            sse(vec![
                ev_response_created("resp-4"),
                ev_assistant_message("msg-4", "done"),
                ev_completed("resp-4"),
            ]),
            sse(vec![
                ev_response_created("resp-5"),
                ev_assistant_message("msg-5", "continued"),
                ev_completed("resp-5"),
            ]),
        ],
    )
    .await;
    let primary = fixture.primary.clone();
    let builder = test_codex().with_config(move |config| {
        config.cwd = primary.clone();
        config.workspace_roots = vec![primary];
        config
            .features
            .enable(Feature::WorkspaceCwdTool)
            .expect("enable workspace cwd tool");
    });
    // workspace.set_cwd requires linked worktrees on the local executor, not a remote test home.
    let test = builder
        .with_extensions(skills_extensions())
        .build(&server)
        .await?;

    test.submit_turn_with_permission_profile(
        "move into the linked worktree and continue",
        PermissionProfile::workspace_write(),
    )
    .await?;

    let requests = responses.requests();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].tool_by_name("workspace", "set_cwd").is_some());
    assert!(
        requests[1]
            .function_call_output_text(CWD_CALL_ID)
            .is_some_and(|output| output.contains("subsequent_model_steps"))
    );
    assert!(
        requests[1].body_contains_text("Use linked immediate instructions."),
        "refreshed request should contain the linked worktree AGENTS.md: {}",
        requests[1].body_json()
    );
    assert!(
        requests[1].body_contains_text("linked-cwd: linked worktree skill"),
        "refreshed request should contain the linked worktree skill: {}",
        requests[1].body_json()
    );
    let linked_cwd = fixture.linked.as_path().to_string_lossy();
    let developer_texts = requests[1].message_input_texts("developer");
    assert!(
        developer_texts
            .iter()
            .any(|text| text.contains("<permissions instructions>")
                && text.contains(linked_cwd.as_ref())),
        "refreshed request should supersede the old permissions context: {developer_texts:?}"
    );
    let repeat_output = requests[2]
        .function_call_output_text(REPEAT_CWD_CALL_ID)
        .expect("third request should contain the repeated cwd result");
    let repeat_output: serde_json::Value = serde_json::from_str(&repeat_output)?;
    assert_eq!(
        repeat_output["previous_cwd"],
        fixture.linked.as_path().to_string_lossy().as_ref()
    );
    assert_eq!(repeat_output["changed"], false);
    let pwd_output = requests[3]
        .function_call_output_text(PWD_CALL_ID)
        .expect("fourth request should contain execution in the linked worktree");
    assert!(
        pwd_output.contains(linked_cwd.as_ref()),
        "shell did not adopt the linked worktree cwd: {pwd_output}"
    );

    test.submit_turn_with_environments(
        "continue in the linked worktree",
        Some(vec![TurnEnvironmentSelection {
            environment_id: LOCAL_ENVIRONMENT_ID.to_string(),
            cwd: PathUri::from_abs_path(&fixture.linked),
            workspace_roots: vec![PathUri::from_abs_path(&fixture.linked)],
            config: EnvironmentConfigState::FromThread,
        }]),
    )
    .await?;
    let requests = responses.requests();
    assert_eq!(requests.len(), 5);
    assert!(requests[4].body_contains_text("Use linked immediate instructions."));
    assert!(requests[4].body_contains_text("continue in the linked worktree"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_cwd_rejects_a_response_with_sibling_tool_calls() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CWD_CALL_ID: &str = "set-cwd-with-sibling";
    const SHELL_CALL_ID: &str = "sibling-shell";

    let fixture = linked_worktree_fixture();
    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call_with_namespace(
                    CWD_CALL_ID,
                    "workspace",
                    "set_cwd",
                    &json!({ "path": fixture.linked }).to_string(),
                ),
                ev_function_call(
                    SHELL_CALL_ID,
                    "exec_command",
                    &json!({ "cmd": "pwd", "max_output_tokens": 1000 }).to_string(),
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-2", "retry later"),
                ev_completed("resp-2"),
            ]),
        ],
    )
    .await;
    let primary = fixture.primary.clone();
    let mut builder = test_codex().with_config(move |config| {
        config.cwd = primary.clone();
        config.workspace_roots = vec![primary];
        config
            .features
            .enable(Feature::WorkspaceCwdTool)
            .expect("enable workspace cwd tool");
    });
    let test = builder.build(&server).await?;

    test.submit_turn_with_permission_profile(
        "try an unsafe mixed context transition",
        PermissionProfile::Disabled,
    )
    .await?;

    let requests = responses.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .function_call_output_text(CWD_CALL_ID)
            .is_some_and(|output| output.contains("must be the only tool call"))
    );
    let shell_output = requests[1]
        .function_call_output_text(SHELL_CALL_ID)
        .expect("second request should contain sibling shell output");
    assert!(
        shell_output.contains(fixture.primary.as_path().to_string_lossy().as_ref()),
        "sibling shell did not retain primary cwd: {shell_output}\n{}",
        requests[1].body_json()
    );

    Ok(())
}

#[tokio::test]
async fn workspace_cwd_waits_for_a_delayed_sibling_before_changing_settings() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CWD_CALL_ID: &str = "set-cwd-before-delayed-sibling";
    const SHELL_CALL_ID: &str = "delayed-sibling-shell";
    const STREAM_MARKER: &str = "workspace call received; more tools follow";

    let fixture = linked_worktree_fixture();
    let mut marker = ev_assistant_message("stream-marker", STREAM_MARKER);
    marker["item"]["phase"] = json!("commentary");
    let (sibling_tx, sibling_rx) = oneshot::channel();
    let (completed_tx, completed_rx) = oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        vec![
            StreamingSseChunk {
                gate: None,
                body: sse(vec![
                    ev_response_created("resp-1"),
                    ev_function_call_with_namespace(
                        CWD_CALL_ID,
                        "workspace",
                        "set_cwd",
                        &json!({ "path": fixture.linked }).to_string(),
                    ),
                    marker,
                ]),
            },
            StreamingSseChunk {
                gate: Some(sibling_rx),
                body: sse(vec![ev_function_call(
                    SHELL_CALL_ID,
                    "exec_command",
                    &json!({ "cmd": "pwd", "login": false, "max_output_tokens": 1000 }).to_string(),
                )]),
            },
            StreamingSseChunk {
                gate: Some(completed_rx),
                body: sse(vec![ev_completed("resp-1")]),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-2", "retry the workspace change alone"),
                ev_completed("resp-2"),
            ]),
        }],
    ])
    .await;
    let primary = fixture.primary.clone();
    let mut builder = test_codex().with_config(move |config| {
        config.cwd = primary.clone();
        config.workspace_roots = vec![primary];
        config
            .features
            .enable(Feature::WorkspaceCwdTool)
            .expect("enable workspace cwd tool");
    });
    let test = builder.build_with_streaming_server(&server).await?;
    let original_environments = test.codex.environment_selections().await;
    let (sandbox_policy, permission_profile) =
        turn_permission_fields(PermissionProfile::Disabled, fixture.primary.as_path());
    test.codex
        .start_or_steer_turn(
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: "reject a workspace change even when its sibling arrives later".to_string(),
                text_elements: Vec::new(),
            }])
            .with_thread_settings(ThreadSettingsOverrides {
                approval_policy: Some(AskForApproval::Never),
                sandbox_policy: Some(sandbox_policy),
                permission_profile,
                ..Default::default()
            }),
        )
        .await?;

    // The marker follows the first tool item, so the second tool cannot arrive until
    // the stream consumer has already queued workspace.set_cwd.
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::AgentMessage(message) if message.message == STREAM_MARKER)
    })
    .await;
    sibling_tx.send(()).expect("release delayed sibling");

    // Keep response.completed withheld until ordinary tool execution finishes. This
    // catches eager workspace dispatch without relying on a short scheduling delay.
    let shell_end = wait_for_event(
        &test.codex,
        |event| matches!(event, EventMsg::ExecCommandEnd(end) if end.call_id == SHELL_CALL_ID),
    )
    .await;
    let EventMsg::ExecCommandEnd(shell_end) = shell_end else {
        unreachable!("waited for the sibling shell result");
    };
    assert_eq!(shell_end.exit_code, 0);
    assert_eq!(
        shell_end.stdout.trim(),
        fixture.primary.as_path().to_string_lossy()
    );
    assert_eq!(
        test.codex.environment_selections().await,
        original_environments
    );

    completed_tx.send(()).expect("complete mixed-tool response");
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    let requests = server.requests().await;
    assert_eq!(requests.len(), 2);
    let follow_up: serde_json::Value = serde_json::from_slice(&requests[1])?;
    let input = follow_up["input"].as_array().expect("model input items");
    let workspace_output = input
        .iter()
        .find(|item| item["type"] == "function_call_output" && item["call_id"] == CWD_CALL_ID)
        .expect("workspace rejection must reach the model");
    assert!(
        workspace_output["output"]
            .as_str()
            .is_some_and(|output| output.contains("must be the only tool call")),
        "workspace must reject the delayed sibling: {workspace_output}"
    );
    assert_eq!(
        test.codex.environment_selections().await,
        original_environments
    );
    server.shutdown().await;
    Ok(())
}
