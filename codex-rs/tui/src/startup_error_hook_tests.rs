//! Isolate the process-global eyre hook while exercising the production TUI entry point.

use std::process::Command;

use clap::Parser;
use codex_arg0::Arg0DispatchPaths;
use codex_config::LoaderOverrides;
use pretty_assertions::assert_eq;

use super::Cli;
use super::RemoteAppServerEndpoint;
use super::run_main;

const CHILD_ENV: &str = "CODEX_TUI_STARTUP_ERROR_HOOK_CHILD";

#[test]
fn handled_startup_error_does_not_prevent_startup_retry() {
    run_child("startup_error_hook_tests::handled_startup_error_child");
}

#[test]
fn conflicting_error_hook_is_reported_on_each_startup_attempt() {
    run_child("startup_error_hook_tests::conflicting_error_hook_child");
}

fn run_child(test_name: &str) {
    let output = Command::new(std::env::current_exe().expect("current test binary"))
        .args(["--exact", test_name, "--ignored", "--nocapture"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("run startup error-hook child");
    assert!(
        output.status.success(),
        "child stdout: {}\nchild stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn handled_startup_error_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }

    for _ in 0..2 {
        let error = rejected_startup().await;
        assert_eq!(
            error.to_string(),
            "--no-daemon cannot be used with --remote."
        );

        // Handling a report during startup must not claim eyre's default hook before color-eyre.
        let report = color_eyre::Report::new(error);
        assert!(
            report
                .handler()
                .downcast_ref::<color_eyre::Handler>()
                .is_some()
        );
        drop(report);
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn conflicting_error_hook_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }

    // A report created by another caller installs eyre's default hook in this fresh process.
    drop(color_eyre::eyre::eyre!("report created before TUI startup"));
    let expected_error = color_eyre::eyre::InstallError.to_string();
    for _ in 0..2 {
        assert_eq!(rejected_startup().await.to_string(), expected_error);
    }
}

async fn rejected_startup() -> std::io::Error {
    // Reject these arguments before loading configuration, opening a terminal, or connecting.
    run_main(
        Cli::parse_from(["codex", "--no-daemon"]),
        Arg0DispatchPaths::default(),
        LoaderOverrides::default(),
        Some(RemoteAppServerEndpoint::WebSocket {
            websocket_url: "ws://127.0.0.1:1".to_string(),
            auth_token: None,
        }),
    )
    .await
    .expect_err("--no-daemon and a remote endpoint must be rejected")
}
