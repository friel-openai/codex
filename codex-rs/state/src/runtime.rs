use crate::LogEntry;
use crate::LogQuery;
use crate::LogRow;
use crate::SortKey;
use crate::SqliteConfig;
use crate::ThreadMetadata;
use crate::ThreadMetadataBuilder;
use crate::ThreadsPage;
use crate::apply_rollout_item;
use crate::migrations::runtime_goals_migrator;
use crate::migrations::runtime_logs_migrator;
use crate::migrations::runtime_memories_migrator;
use crate::migrations::runtime_queue_migrator;
use crate::migrations::runtime_state_migrator;
use crate::migrations::runtime_thread_history_migrator;
use crate::model::ThreadRow;
use crate::model::anchor_from_item;
use crate::model::datetime_to_epoch_millis;
use crate::model::datetime_to_epoch_seconds;
use crate::model::epoch_millis_to_datetime;
use crate::paths::file_modified_time_utc;
use crate::telemetry::DbKind;
use crate::telemetry::DbTelemetry;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::RolloutItem;
use serde_json::Value;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::Sqlite;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Instant;
use tracing::warn;

mod backfill;
mod external_agent_config_imports;
mod goals;
mod logs;
mod memories;
mod queued_items;
mod recovery;
mod remote_control;
mod rollout_migration;
#[cfg(test)]
pub(crate) mod test_support;
mod thread_section_order;
mod thread_sections;
mod thread_spawn_close;
mod thread_spawn_close_repair;
#[cfg(test)]
#[path = "runtime/thread_spawn_current_only_close_tests.rs"]
mod thread_spawn_current_only_close_tests;
mod thread_spawn_graph;
#[cfg(test)]
#[path = "runtime/thread_spawn_graph_tests.rs"]
mod thread_spawn_graph_tests;
mod threads;

pub use external_agent_config_imports::ExternalAgentConfigImportDetailsRecord;
pub use external_agent_config_imports::ExternalAgentConfigImportFailureRecord;
pub use external_agent_config_imports::ExternalAgentConfigImportHistoryRecord;
pub use external_agent_config_imports::ExternalAgentConfigImportSuccessRecord;
pub use goals::ActiveGoalSupervisorSchedule;
pub use goals::ActiveGoalSupervisorSchedulesPage;
pub use goals::GoalAccountingMode;
pub use goals::GoalAccountingOutcome;
pub use goals::GoalStore;
pub use goals::GoalUpdate;
pub use goals::ListActiveGoalSupervisorSchedulesParams;
pub use goals::ThreadGoalCleanupCounts;
pub use memories::MemoryStore;
pub use queued_items::SqliteQueueStore;
pub use recovery::RuntimeDbBackup;
pub(super) use recovery::RuntimeDbInitError;
pub use recovery::backup_runtime_db_for_fresh_start;
pub use recovery::is_sqlite_corruption_error;
pub use recovery::runtime_db_path_for_corruption_error;
pub use recovery::sqlite_error_detail_is_corruption;
pub use recovery::sqlite_error_detail_is_lock;
pub use remote_control::RemoteControlEnrollmentRecord;
pub use threads::ThreadFilterOptions;

// "Partition" is the retained-log-content bucket we cap at 10 MiB:
// - one bucket per non-null thread_id
// - one bucket per threadless (thread_id IS NULL) non-null process_uuid
// - one bucket for threadless rows with process_uuid IS NULL
// This budget tracks each row's persisted rendered log body plus non-body
// metadata, rather than the exact sum of all persisted SQLite column bytes.
const LOG_PARTITION_SIZE_LIMIT_BYTES: i64 = 10 * 1024 * 1024;
const LOG_PARTITION_ROW_LIMIT: i64 = 1_000;
const FRODEX_THREAD_GOAL_SUPERVISOR_STATE_MIGRATION_DESCRIPTION: &str =
    "thread goal supervisor state";
const THREAD_CLEANUP_BATCH_SIZE: usize = 256;

#[derive(Clone)]
pub struct StateRuntime {
    sqlite: SqliteConfig,
    default_provider: String,
    pool: Arc<sqlx::SqlitePool>,
    logs_pool: Arc<sqlx::SqlitePool>,
    thread_goals: GoalStore,
    memories: MemoryStore,
    thread_queue: SqliteQueueStore,
    thread_updated_at_millis: Arc<AtomicI64>,
    thread_recency_at_millis: Arc<AtomicI64>,
}

impl StateRuntime {
    /// Initialize the state runtime using the provided SQLite configuration and default provider.
    ///
    /// This opens (and migrates) the SQLite databases under the configured
    /// `sqlite_home`.
    /// Logs and paginated thread history live in dedicated files to reduce
    /// lock contention with the rest of the state store.
    pub async fn init(sqlite: SqliteConfig, default_provider: String) -> anyhow::Result<Arc<Self>> {
        Self::init_inner(sqlite, default_provider, /*telemetry_override*/ None).await
    }

    #[cfg(test)]
    pub(crate) async fn init_with_telemetry_for_tests(
        sqlite: SqliteConfig,
        default_provider: String,
        telemetry_override: &dyn DbTelemetry,
    ) -> anyhow::Result<Arc<Self>> {
        Self::init_inner(sqlite, default_provider, Some(telemetry_override)).await
    }

    async fn init_inner(
        sqlite: SqliteConfig,
        default_provider: String,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<Arc<Self>> {
        tokio::fs::create_dir_all(sqlite.home()).await?;
        let state_migrator = runtime_state_migrator();
        let logs_migrator = runtime_logs_migrator();
        let goals_migrator = runtime_goals_migrator();
        let memories_migrator = runtime_memories_migrator();
        let queue_migrator = runtime_queue_migrator();
        let state_path = sqlite.state_db_path();
        let logs_path = sqlite.logs_db_path();
        let goals_path = sqlite.goals_db_path();
        let memories_path = sqlite.memories_db_path();
        let queue_path = sqlite.queue_db_path();
        let pool = match sqlite
            .open_state_db(&state_migrator, telemetry_override)
            .await
        {
            Ok(db) => Arc::new(db),
            Err(err) => {
                warn!("failed to open state db at {}: {err}", state_path.display());
                return Err(err);
            }
        };
        let logs_pool = match sqlite
            .open_logs_db(&logs_migrator, telemetry_override)
            .await
        {
            Ok(db) => Arc::new(db),
            Err(err) => {
                warn!("failed to open logs db at {}: {err}", logs_path.display());
                close_sqlite_pools(&[pool.as_ref()]).await;
                return Err(err);
            }
        };
        let goals_pool = match sqlite
            .open_goals_db(&goals_migrator, telemetry_override)
            .await
        {
            Ok(db) => Arc::new(db),
            Err(err) => {
                warn!("failed to open goals db at {}: {err}", goals_path.display());
                close_sqlite_pools(&[pool.as_ref(), logs_pool.as_ref()]).await;
                return Err(err);
            }
        };
        let memories_pool = match sqlite
            .open_memories_db(&memories_migrator, telemetry_override)
            .await
        {
            Ok(db) => Arc::new(db),
            Err(err) => {
                warn!(
                    "failed to open memories db at {}: {err}",
                    memories_path.display()
                );
                close_sqlite_pools(&[pool.as_ref(), logs_pool.as_ref(), goals_pool.as_ref()]).await;
                return Err(err);
            }
        };
        let queue_pool = match sqlite
            .open_queue_db(&queue_migrator, telemetry_override)
            .await
        {
            Ok(db) => Arc::new(db),
            Err(err) => {
                warn!("failed to open queue db at {}: {err}", queue_path.display());
                close_sqlite_pools(&[
                    pool.as_ref(),
                    logs_pool.as_ref(),
                    goals_pool.as_ref(),
                    memories_pool.as_ref(),
                ])
                .await;
                return Err(err);
            }
        };
        let thread_goals = GoalStore::new(Arc::clone(&goals_pool));
        let thread_goals_init_result = async {
            thread_goals
                .ensure_thread_goal_supervisor_state_table()
                .await?;
            migrate_frodex_goal_supervisor_state_from_state_db(pool.as_ref(), &thread_goals).await
        }
        .await;
        if let Err(err) = thread_goals_init_result {
            close_sqlite_pools(&[
                pool.as_ref(),
                logs_pool.as_ref(),
                goals_pool.as_ref(),
                memories_pool.as_ref(),
                queue_pool.as_ref(),
            ])
            .await;
            return Err(err);
        }
        let started = Instant::now();
        let backfill_state_result = ensure_backfill_state_row_in_pool(pool.as_ref()).await;
        crate::telemetry::record_init_result(
            telemetry_override,
            DbKind::State,
            "ensure_backfill_state",
            started.elapsed(),
            &backfill_state_result,
        );
        if let Err(err) = backfill_state_result {
            close_sqlite_pools(&[
                pool.as_ref(),
                logs_pool.as_ref(),
                goals_pool.as_ref(),
                memories_pool.as_ref(),
                queue_pool.as_ref(),
            ])
            .await;
            return Err(err);
        }
        let started = Instant::now();
        let thread_timestamp_millis_result: anyhow::Result<(Option<i64>, Option<i64>)> =
            sqlx::query_as(
                "SELECT MAX(threads.updated_at_ms), MAX(threads.recency_at_ms) FROM threads",
            )
            .fetch_one(pool.as_ref())
            .await
            .map_err(anyhow::Error::from);
        crate::telemetry::record_init_result(
            telemetry_override,
            DbKind::State,
            "post_init_query",
            started.elapsed(),
            &thread_timestamp_millis_result,
        );
        let (thread_updated_at_millis, thread_recency_at_millis) =
            match thread_timestamp_millis_result {
                Ok(value) => value,
                Err(err) => {
                    close_sqlite_pools(&[
                        pool.as_ref(),
                        logs_pool.as_ref(),
                        goals_pool.as_ref(),
                        memories_pool.as_ref(),
                        queue_pool.as_ref(),
                    ])
                    .await;
                    return Err(err);
                }
            };
        let thread_updated_at_millis = thread_updated_at_millis.unwrap_or(0);
        let thread_recency_at_millis = thread_recency_at_millis.unwrap_or(0);
        let runtime = Arc::new(Self {
            thread_goals,
            memories: MemoryStore::new(Arc::clone(&memories_pool), Arc::clone(&pool)),
            thread_queue: SqliteQueueStore::new(queue_pool),
            pool,
            logs_pool,
            sqlite,
            default_provider,
            thread_updated_at_millis: Arc::new(AtomicI64::new(thread_updated_at_millis)),
            thread_recency_at_millis: Arc::new(AtomicI64::new(thread_recency_at_millis)),
        });
        if let Err(err) = runtime.run_logs_startup_maintenance().await {
            warn!(
                "failed to run startup maintenance for logs db at {}: {err}",
                logs_path.display(),
            );
        }
        Ok(runtime)
    }

    /// Return the SQLite configuration for this runtime.
    pub fn sqlite(&self) -> &SqliteConfig {
        &self.sqlite
    }

    pub fn thread_goals(&self) -> &GoalStore {
        &self.thread_goals
    }

    pub fn memories(&self) -> &MemoryStore {
        &self.memories
    }

    /// Return the durable, SQLite-backed user-message queue.
    pub fn thread_queue(&self) -> &SqliteQueueStore {
        &self.thread_queue
    }

    /// Close all SQLite pools and wait for outstanding pool workers to exit.
    pub async fn close(&self) {
        self.thread_queue.close().await;
        self.memories.close().await;
        self.thread_goals.close().await;
        self.logs_pool.close().await;
        self.pool.close().await;
    }

    pub async fn clear_memory_data_in_sqlite_home(sqlite: &SqliteConfig) -> anyhow::Result<bool> {
        let memories_path = sqlite.memories_db_path();
        if !tokio::fs::try_exists(&memories_path).await? {
            return Ok(false);
        }

        let memories_migrator = runtime_memories_migrator();
        let pool = sqlite
            .open_memories_db(&memories_migrator, /*telemetry_override*/ None)
            .await?;
        memories::clear_memory_data_in_pool(&pool).await?;
        pool.close().await;
        Ok(true)
    }
}

async fn close_sqlite_pools(pools: &[&SqlitePool]) {
    for pool in pools {
        pool.close().await;
    }
}

/// Open and migrate the rebuildable paginated thread-history database.
pub async fn open_thread_history_db(sqlite: &SqliteConfig) -> anyhow::Result<SqlitePool> {
    let migrator = runtime_thread_history_migrator();
    sqlite
        .open_thread_history_db(&migrator, /*telemetry_override*/ None)
        .await
}

pub(super) async fn repair_frodex_state_migration_rows(pool: &SqlitePool) -> anyhow::Result<()> {
    let migrations_table_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await?;
    if migrations_table_exists == 0 {
        return Ok(());
    }

    let frodex_versions = sqlx::query_scalar::<_, i64>(
        r#"
SELECT version
FROM _sqlx_migrations
WHERE description = ?
  AND success = 1
  AND version IN (33, 34)
ORDER BY version
        "#,
    )
    .bind(FRODEX_THREAD_GOAL_SUPERVISOR_STATE_MIGRATION_DESCRIPTION)
    .fetch_all(pool)
    .await?;
    if frodex_versions.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
DELETE FROM _sqlx_migrations
WHERE description = ?
  AND success = 1
  AND version IN (33, 34)
        "#,
    )
    .bind(FRODEX_THREAD_GOAL_SUPERVISOR_STATE_MIGRATION_DESCRIPTION)
    .execute(pool)
    .await
    .context("removing frodex state migration rows")?;
    warn!("removed frodex state migration rows at versions {frodex_versions:?}");
    Ok(())
}

async fn migrate_frodex_goal_supervisor_state_from_state_db(
    state_pool: &SqlitePool,
    thread_goals: &GoalStore,
) -> anyhow::Result<()> {
    let supervisor_table_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'thread_goal_supervisor_state'",
    )
    .fetch_one(state_pool)
    .await?;
    if supervisor_table_exists == 0 {
        return Ok(());
    }

    let supervisor_rows = sqlx::query_as::<_, (String, String, Option<i64>, i64)>(
        r#"
SELECT thread_id, goal_id, snoozed_until_ms, updated_at_ms
FROM thread_goal_supervisor_state
        "#,
    )
    .fetch_all(state_pool)
    .await?;
    thread_goals
        .upsert_frodex_goal_supervisor_state_rows(supervisor_rows)
        .await?;
    sqlx::query("DROP TABLE thread_goal_supervisor_state")
        .execute(state_pool)
        .await
        .context("dropping frodex supervisor state from state db")?;
    Ok(())
}

pub(super) async fn ensure_backfill_state_row_in_pool(
    pool: &sqlx::SqlitePool,
) -> anyhow::Result<()> {
    // Eagerly check if the operation would have no effect to avoid blocking waiting for a SQLite
    // writer for no reason in the hot startup path.
    if sqlx::query_scalar::<_, i64>("SELECT 1 FROM backfill_state WHERE id = 1")
        .fetch_optional(pool)
        .await?
        .is_some()
    {
        return Ok(());
    }

    sqlx::query(
        r#"
INSERT INTO backfill_state (id, status, last_watermark, last_success_at, updated_at)
VALUES (?, ?, NULL, NULL, ?)
ON CONFLICT(id) DO NOTHING
            "#,
    )
    .bind(1_i64)
    .bind(crate::BackfillStatus::Pending.as_str())
    .bind(Utc::now().timestamp())
    .execute(pool)
    .await?;
    Ok(())
}

/// Run SQLite's built-in integrity check against an existing database file.
pub async fn sqlite_integrity_check(
    sqlite: &SqliteConfig,
    path: &Path,
) -> anyhow::Result<Vec<String>> {
    let pool = sqlite.open_read_only_pool(path).await?;
    let rows = sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
        .fetch_all(&pool)
        .await?;
    pool.close().await;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::FRODEX_THREAD_GOAL_SUPERVISOR_STATE_MIGRATION_DESCRIPTION;
    use super::StateRuntime;
    use super::runtime_state_migrator;
    use super::sqlite_integrity_check;
    use super::test_support::unique_temp_dir;
    use crate::DB_INIT_METRIC;
    use crate::DbTelemetry;
    use crate::migrations::STATE_MIGRATOR;
    use codex_protocol::ThreadId;
    use codex_utils_absolute_path::test_support::PathExt;
    use pretty_assertions::assert_eq;
    use sqlx::SqlitePool;
    use sqlx::migrate::MigrateError;
    use sqlx::migrate::Migration;
    use sqlx::migrate::Migrator;
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestTelemetry {
        counters: Mutex<Vec<MetricEvent>>,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct MetricEvent {
        name: String,
        tags: BTreeMap<String, String>,
    }

    impl TestTelemetry {
        fn counters(&self) -> Vec<MetricEvent> {
            self.counters
                .lock()
                .expect("telemetry lock")
                .iter()
                .map(|event| MetricEvent {
                    name: event.name.clone(),
                    tags: event.tags.clone(),
                })
                .collect()
        }
    }

    impl DbTelemetry for TestTelemetry {
        fn counter(&self, name: &str, _inc: i64, tags: &[(&str, &str)]) {
            self.counters
                .lock()
                .expect("telemetry lock")
                .push(MetricEvent {
                    name: name.to_string(),
                    tags: tags_to_map(tags),
                });
        }

        fn record_duration(
            &self,
            _name: &str,
            _duration: std::time::Duration,
            _tags: &[(&str, &str)],
        ) {
        }
    }

    fn tags_to_map(tags: &[(&str, &str)]) -> BTreeMap<String, String> {
        tags.iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    async fn open_db_pool(path: &Path) -> SqlitePool {
        crate::SqliteConfig::new_for_testing(path.parent().unwrap_or(path).abs())
            .open_read_write_pool(path)
            .await
            .expect("open sqlite pool")
    }

    async fn seed_frodex_supervisor_state_migration(
        state_path: &Path,
        recorded_version: i64,
    ) -> (String, String) {
        let thread_id = "00000000-0000-0000-0000-000000000321".to_string();
        let goal_id = "goal-321".to_string();
        let pool = open_db_pool(state_path).await;
        let latest_upstream_version = if recorded_version == 33 { 32 } else { 33 };
        let upstream_migrations = STATE_MIGRATOR
            .iter()
            .filter(|migration| migration.version <= latest_upstream_version)
            .cloned()
            .collect::<Vec<Migration>>();
        Migrator {
            migrations: Cow::Owned(upstream_migrations),
            ignore_missing: false,
            locking: STATE_MIGRATOR.locking,
            no_tx: STATE_MIGRATOR.no_tx,
            table_name: STATE_MIGRATOR.table_name.clone(),
            create_schemas: STATE_MIGRATOR.create_schemas.clone(),
        }
        .run(&pool)
        .await
        .expect("apply migrations through 33");
        sqlx::query(
            r#"
INSERT INTO threads (
    id,
    rollout_path,
    created_at,
    updated_at,
    created_at_ms,
    updated_at_ms,
    source,
    model_provider,
    cwd,
    cli_version,
    title,
    preview,
    sandbox_policy,
    approval_mode,
    tokens_used,
    first_user_message,
    archived,
    memory_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(thread_id.as_str())
        .bind("/tmp/rollout.jsonl")
        .bind(1_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind("cli")
        .bind("test-provider")
        .bind("/tmp")
        .bind("0.0.0")
        .bind("")
        .bind("")
        .bind("{}")
        .bind("on-request")
        .bind(0_i64)
        .bind("")
        .bind(false)
        .bind("enabled")
        .execute(&pool)
        .await
        .expect("insert thread");
        sqlx::query(
            r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(thread_id.as_str())
        .bind(goal_id.as_str())
        .bind("persist supervisor snooze")
        .bind("active")
        .bind(Option::<i64>::None)
        .bind(0_i64)
        .bind(0_i64)
        .bind(1_i64)
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("insert goal");
        sqlx::query(
            r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
            "#,
        )
        .execute(&pool)
        .await
        .expect("create old frodex supervisor state table");
        sqlx::query(
            r#"
INSERT INTO thread_goal_supervisor_state (
    thread_id,
    goal_id,
    snoozed_until_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(thread_id.as_str())
        .bind(goal_id.as_str())
        .bind(123_456_i64)
        .bind(7_i64)
        .execute(&pool)
        .await
        .expect("insert old frodex supervisor state");
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(recorded_version)
        .bind(FRODEX_THREAD_GOAL_SUPERVISOR_STATE_MIGRATION_DESCRIPTION)
        .bind(true)
        .bind(vec![9_u8, 9, 9, 9])
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("record old frodex supervisor state migration");
        pool.close().await;
        (thread_id, goal_id)
    }

    #[tokio::test]
    async fn sqlite_integrity_check_reports_ok_for_valid_db() {
        let codex_home = unique_temp_dir();
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        let sqlite = crate::SqliteConfig::new_for_testing(codex_home.as_path().abs());
        let path = sqlite.state_db_path();
        let pool = sqlite
            .open_read_write_pool(&path)
            .await
            .expect("open sqlite db");
        sqlx::query("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
            .execute(&pool)
            .await
            .expect("create sample table");
        pool.close().await;

        let result = sqlite_integrity_check(&sqlite, &path)
            .await
            .expect("integrity check should run");

        assert_eq!(result, vec!["ok".to_string()]);
        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn open_state_sqlite_tolerates_newer_applied_migrations() {
        let codex_home = unique_temp_dir();
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        let sqlite = crate::SqliteConfig::new_for_testing(codex_home.as_path().abs());
        let state_path = sqlite.state_db_path();
        let pool = sqlite
            .open_read_write_pool(&state_path)
            .await
            .expect("open state db");
        STATE_MIGRATOR
            .run(&pool)
            .await
            .expect("apply current state schema");
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(9_999_i64)
        .bind("future migration")
        .bind(true)
        .bind(vec![1_u8, 2, 3, 4])
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("insert future migration record");
        pool.close().await;

        let strict_pool = open_db_pool(state_path.as_path()).await;
        let strict_err = STATE_MIGRATOR
            .run(&strict_pool)
            .await
            .expect_err("strict migrator should reject newer applied migrations");
        assert!(matches!(strict_err, MigrateError::VersionMissing(9_999)));
        strict_pool.close().await;

        let tolerant_migrator = runtime_state_migrator();
        let tolerant_pool = sqlite
            .open_state_db(&tolerant_migrator, /*telemetry_override*/ None)
            .await
            .expect("runtime migrator should tolerate newer applied migrations");
        tolerant_pool.close().await;

        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn open_state_sqlite_repairs_frodex_supervisor_state_migration_rows() {
        let codex_home = unique_temp_dir();
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        let sqlite = crate::SqliteConfig::new_for_testing(codex_home.as_path().abs());
        let state_path = sqlite.state_db_path();
        seed_frodex_supervisor_state_migration(state_path.as_path(), /*recorded_version*/ 34).await;

        let strict_pool = open_db_pool(state_path.as_path()).await;
        let strict_err = STATE_MIGRATOR
            .run(&strict_pool)
            .await
            .expect_err("strict migrator should reject the old frodex migration 34");
        assert!(matches!(strict_err, MigrateError::VersionMismatch(34)));
        strict_pool.close().await;

        let tolerant_migrator = runtime_state_migrator();
        let repaired_pool = sqlite
            .open_state_db(&tolerant_migrator, /*telemetry_override*/ None)
            .await
            .expect("runtime migrator should repair old frodex migration rows");
        let applied = sqlx::query_as::<_, (i64, String)>(
            "SELECT version, description FROM _sqlx_migrations WHERE version IN (33, 34) ORDER BY version",
        )
        .fetch_all(&repaired_pool)
        .await
        .expect("read repaired migrations");
        assert_eq!(
            applied,
            vec![
                (33, "thread goal stopped statuses".to_string()),
                (34, "drop thread goals".to_string()),
            ]
        );
        repaired_pool.close().await;

        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn state_runtime_moves_frodex_supervisor_state_out_of_state_db() {
        let codex_home = unique_temp_dir();
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        let sqlite = crate::SqliteConfig::new_for_testing(codex_home.as_path().abs());
        let state_path = sqlite.state_db_path();
        let (thread_id, goal_id) = seed_frodex_supervisor_state_migration(
            state_path.as_path(),
            /*recorded_version*/ 33,
        )
        .await;

        let runtime = StateRuntime::init(sqlite, "test-provider".to_string())
            .await
            .expect("state runtime should repair old frodex supervisor state");
        let thread_id = ThreadId::from_string(thread_id.as_str()).expect("valid thread id");
        assert_eq!(
            Some(123_456),
            runtime
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(thread_id, goal_id.as_str())
                .await
                .expect("migrated supervisor snooze should read")
        );
        let state_supervisor_table_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'thread_goal_supervisor_state'",
        )
        .fetch_one(runtime.pool.as_ref())
        .await
        .expect("read repaired state schema");
        assert_eq!(0, state_supervisor_table_exists);
        let applied = sqlx::query_as::<_, (i64, String)>(
            "SELECT version, description FROM _sqlx_migrations WHERE version IN (33, 34) ORDER BY version",
        )
        .fetch_all(runtime.pool.as_ref())
        .await
        .expect("read repaired state migrations");
        assert_eq!(
            applied,
            vec![
                (33, "thread goal stopped statuses".to_string()),
                (34, "drop thread goals".to_string()),
            ]
        );

        runtime.pool.close().await;
        runtime.logs_pool.close().await;
        drop(runtime);
        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }

    #[tokio::test]
    async fn init_records_successful_sqlite_init_phases_to_explicit_telemetry() {
        let codex_home = unique_temp_dir();
        let telemetry = TestTelemetry::default();

        let runtime = StateRuntime::init_with_telemetry_for_tests(
            crate::SqliteConfig::new_for_testing(codex_home.as_path().abs()),
            "test-provider".to_string(),
            &telemetry,
        )
        .await
        .expect("state runtime should initialize");

        let phases = telemetry
            .counters()
            .into_iter()
            .filter(|event| event.name == DB_INIT_METRIC)
            .filter(|event| event.tags.get("status").map(String::as_str) == Some("success"))
            .filter_map(|event| event.tags.get("phase").cloned())
            .collect::<BTreeSet<_>>();
        let expected = [
            "open_state",
            "migrate_state",
            "open_logs",
            "migrate_logs",
            "open_goals",
            "migrate_goals",
            "open_memories",
            "migrate_memories",
            "open_queue",
            "migrate_queue",
            "ensure_backfill_state",
            "post_init_query",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
        assert_eq!(phases, expected);

        runtime.close().await;
        let _ = tokio::fs::remove_dir_all(codex_home).await;
    }
}
