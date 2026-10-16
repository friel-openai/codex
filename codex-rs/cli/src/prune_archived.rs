use std::time::Duration;

use clap::Parser;
use codex_agent_message_board_extension::LocalAgentMessageBoard;
use codex_core::config::ConfigBuilder;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_utils_cli::CliConfigOverrides;

/// A one-time, explicitly requested archive retention operation.
#[derive(Debug, Parser)]
pub(crate) struct PruneArchivedCommand {
    /// Required archive age: a positive integer followed by h (hours), d (days), or w (weeks).
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    older_than: Duration,

    /// Permanently delete eligible threads. Without this flag, only preview the deletion set.
    #[arg(long, requires = "offline")]
    apply: bool,

    /// Confirm ALL other processes using this Codex home are stopped and will remain stopped.
    /// Includes app servers, fork/import/restore operations, and older Codex versions.
    #[arg(long)]
    offline: bool,

    /// Emit the complete report as JSON.
    #[arg(long)]
    json: bool,
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let (digits, multiplier) = match value.as_bytes().last() {
        Some(b'h') => (&value[..value.len() - 1], 3_600_u64),
        Some(b'd') => (&value[..value.len() - 1], 86_400_u64),
        Some(b'w') => (&value[..value.len() - 1], 604_800_u64),
        _ => {
            return Err(
                "use a positive integer followed by h, d, or w (for example 30d)".to_string(),
            );
        }
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(
            "retention must be a positive whole number of hours, days, or weeks".to_string(),
        );
    }
    let seconds = digits
        .parse::<u64>()
        .ok()
        .and_then(|count| count.checked_mul(multiplier))
        .filter(|seconds| *seconds > 0 && *seconds <= i64::MAX as u64 / 1_000)
        .ok_or_else(|| "retention duration is zero or out of range".to_string())?;
    Ok(Duration::from_secs(seconds))
}

pub(crate) async fn run(
    command: PruneArchivedCommand,
    config_overrides: CliConfigOverrides,
) -> anyhow::Result<()> {
    let config = ConfigBuilder::default()
        .cli_overrides(
            config_overrides
                .parse_overrides()
                .map_err(anyhow::Error::msg)?,
        )
        .build()
        .await?;
    // Do not initialize or backfill SQLite for preview: absent archive dates never authorize expiry.
    let sqlite = config.sqlite.clone();
    let store = LocalThreadStore::new(LocalThreadStoreConfig::from_config(&config), None)
        .with_thread_data_cleanup(move |thread_ids| {
            let sqlite = sqlite.clone();
            Box::pin(async move {
                let boards = thread_ids.into_iter().map(Into::into).collect::<Vec<_>>();
                LocalAgentMessageBoard::delete_boards(&sqlite, &boards)
                    .await
                    .map_err(|error| codex_thread_store::ThreadStoreError::Internal {
                        message: format!("failed to delete agent message boards: {error}"),
                    })
            })
        });
    eprintln!("Inspecting complete local rollout history. Large archives may take a long time.");
    let report = store
        .prune_archived(command.older_than, command.apply, command.offline)
        .await?;
    if command.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Archive cutoff: {} (strictly older).", report.cutoff);
        if !report.archive_metadata_available {
            println!("No existing archive metadata database; no threads are eligible.");
        }
        println!(
            "{} eligible; {} retained because other history references them; {} deleted.",
            report.eligible_thread_ids.len(),
            report.referenced_thread_ids.len(),
            report.deleted_thread_ids.len()
        );
        for id in &report.eligible_thread_ids {
            println!("{id}");
        }
        if !command.apply {
            println!("Preview only. No threads were deleted. No retention policy was saved.");
            println!(
                "To delete, stop every other process using this Codex home, then repeat with --apply --offline."
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn prune_requires_period_and_offline_for_apply() {
        assert!(PruneArchivedCommand::try_parse_from(["prune-archived"]).is_err());
        assert!(
            PruneArchivedCommand::try_parse_from(["prune-archived", "--apply", "--offline"])
                .is_err()
        );
        assert!(
            PruneArchivedCommand::try_parse_from([
                "prune-archived",
                "--older-than",
                "30d",
                "--apply"
            ])
            .is_err()
        );
        let preview =
            PruneArchivedCommand::try_parse_from(["prune-archived", "--older-than", "30d"])
                .expect("preview");
        assert!(!preview.apply);
        assert!(!preview.offline);
        assert_eq!(preview.older_than, Duration::from_secs(30 * 86_400));
        let apply = PruneArchivedCommand::try_parse_from([
            "prune-archived",
            "--older-than",
            "30d",
            "--apply",
            "--offline",
        ])
        .expect("explicit offline apply");
        assert!(apply.apply && apply.offline);
    }

    #[test]
    fn prune_duration_rejects_implicit_zero_negative_and_overflow_values() {
        for value in [
            "",
            "30",
            "0d",
            "-1d",
            "1.5d",
            "+1d",
            "1m",
            "18446744073709551615w",
        ] {
            assert!(parse_duration(value).is_err(), "{value}");
        }
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7_200));
        assert_eq!(
            parse_duration("3w").unwrap(),
            Duration::from_secs(21 * 86_400)
        );
    }
}
