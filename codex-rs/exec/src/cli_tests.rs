use super::*;
use pretty_assertions::assert_eq;

#[test]
fn resume_parses_prompt_after_global_flags() {
    const PROMPT: &str = "echo resume-with-global-flags-after-subcommand";
    let cli = Cli::parse_from([
        "codex-exec",
        "resume",
        "--last",
        "--json",
        "--model",
        "gpt-5.2-codex",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        PROMPT,
    ]);

    assert!(cli.ephemeral);
    assert!(cli.ignore_user_config);
    assert!(cli.ignore_rules);
    let Some(Command::Resume(args)) = cli.command else {
        panic!("expected resume command");
    };
    let effective_prompt = args.prompt.clone().or_else(|| {
        if args.last {
            args.session_id.clone()
        } else {
            None
        }
    });
    assert_eq!(effective_prompt.as_deref(), Some(PROMPT));
}

#[test]
fn resume_accepts_output_last_message_flag_after_subcommand() {
    const PROMPT: &str = "echo resume-with-output-file";
    let cli = Cli::parse_from([
        "codex-exec",
        "resume",
        "session-123",
        "-o",
        "/tmp/resume-output.md",
        PROMPT,
    ]);

    assert_eq!(
        cli.last_message_file,
        Some(PathBuf::from("/tmp/resume-output.md"))
    );
    let Some(Command::Resume(args)) = cli.command else {
        panic!("expected resume command");
    };
    assert_eq!(args.session_id.as_deref(), Some("session-123"));
    assert_eq!(args.prompt.as_deref(), Some(PROMPT));
}

#[test]
fn parses_config_isolation_flags() {
    let cli = Cli::parse_from([
        "codex-exec",
        "--ignore-user-config",
        "--ignore-rules",
        "summarize",
    ]);

    assert!(cli.ignore_user_config);
    assert!(cli.ignore_rules);
}

#[test]
fn removed_full_auto_flag_reports_migration_path() {
    let cli = Cli::parse_from(["codex-exec", "--full-auto", "summarize"]);

    assert_eq!(
        cli.removed_full_auto_warning(),
        Some("warning: `--full-auto` is deprecated; use `--sandbox workspace-write` instead.")
    );
}

#[test]
fn fork_option_parses_prompt() {
    const PROMPT: &str = "echo fork-non-interactive";
    let cli = Cli::parse_from(["codex-exec", "--fork", "session-123", "--json", PROMPT]);

    assert_eq!(cli.fork_session_id.as_deref(), Some("session-123"));
    assert_eq!(cli.prompt.as_deref(), Some(PROMPT));
    assert!(cli.command.is_none());
}

#[test]
fn fork_option_conflicts_with_subcommands() {
    let err = Cli::try_parse_from(["codex-exec", "--fork", "session-123", "resume"])
        .and_then(Cli::validate)
        .expect_err("fork should conflict with subcommands");

    assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
}
