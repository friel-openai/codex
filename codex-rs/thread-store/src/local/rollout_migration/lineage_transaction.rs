//! Executes and recovers a same-thread segmented Legacy migration transaction.

use std::path::Path;
use std::path::PathBuf;

use super::LocalThreadStore;
use super::RolloutMigrationRateLimiter;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage::LegacyLineagePredecessor;
use super::lineage::plan_legacy_lineage;
use super::lineage::validate_segment_migration;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationPhase;
use super::lineage_journal::read_lineage_migration_journal;
use super::lineage_journal::write_lineage_migration_journal;
use super::lineage_publish::publish_lineage_targets;
use super::lineage_publish::verify_published_lineage_targets;
use super::lineage_stage::stage_legacy_lineage;
use super::migration_error;
use super::publish::decompress_rollout_to_path;
use super::publish::sync_parent_directory;
use super::thread_history;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

impl LocalThreadStore {
    pub(super) async fn validate_legacy_lineage_plan(
        &self,
        plan: &LegacyLineageMigrationPlan,
    ) -> ThreadStoreResult<()> {
        validate_segment_migration(plan)?;
        for position in plan
            .sources
            .iter()
            .filter_map(|source| match source.predecessor.as_ref() {
                Some(LegacyLineagePredecessor::HistoryBase(position)) => Some(*position),
                _ => None,
            })
        {
            let lineage = self.resolve_rollout_lineage_at(position).await?;
            if lineage.root_rollout_id != position.thread_id {
                return Err(migration_error(
                    "history_base validation resolved another physical rollout",
                ));
            }
        }
        for dependency in &plan.reference_dependencies {
            let reference = plan
                .sources
                .iter()
                .find(|source| source.rollout_id == dependency.successor_rollout_id)
                .and_then(|source| match source.predecessor.as_ref() {
                    Some(LegacyLineagePredecessor::RolloutReference(reference)) => Some(reference),
                    _ => None,
                })
                .ok_or_else(|| {
                    migration_error("Paginated reference dependency has no successor edge")
                })?;
            let resolved = codex_rollout::resolve_rollout_reference_path(
                self.config.codex_home.as_path(),
                reference,
            )
            .await
            .map_err(migration_error)?;
            let resolved = tokio::fs::canonicalize(resolved)
                .await
                .map_err(migration_error)?;
            let expected = tokio::fs::canonicalize(dependency.path.as_path())
                .await
                .map_err(migration_error)?;
            if resolved != expected {
                return Err(migration_error(
                    "Paginated reference dependency resolved another physical rollout",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn validate_legacy_lineage_desktop_compatibility(
        &self,
        plan: &mut LegacyLineageMigrationPlan,
    ) -> ThreadStoreResult<()> {
        // Paginated sources already persist stable turn and item identities. The bounded-view
        // comparison below protects Legacy synthetic IDs, which can depend on how many
        // predecessor segments a reader materializes. Applying that comparison to a Paginated
        // reference chain would reject a lossless migration whenever an ordinary bounded view
        // intentionally omits older item bodies.
        if plan.sources.iter().all(|source| {
            source.history_mode == codex_protocol::protocol::ThreadHistoryMode::Paginated
        }) {
            return Ok(());
        }
        super::lineage_compatibility::validate_bounded_desktop_history(
            self.config.codex_home.as_path(),
            plan,
        )
        .await
    }

    pub(super) async fn cleanup_unpublished_lineage_migration(
        &self,
        journal_path: &Path,
    ) -> ThreadStoreResult<()> {
        if !tokio::fs::try_exists(journal_path)
            .await
            .map_err(migration_error)?
        {
            return Ok(());
        }
        let journal = read_lineage_migration_journal(journal_path).await?;
        if !matches!(
            journal.phase,
            LineageMigrationPhase::Planned | LineageMigrationPhase::TargetsDurable
        ) {
            return Ok(());
        }
        for target in &journal.targets {
            thread_history::delete_thread(self, target.rollout_id).await?;
        }
        let stage_root = journal_path.with_extension("staging");
        if tokio::fs::try_exists(stage_root.as_path())
            .await
            .map_err(migration_error)?
        {
            tokio::fs::remove_dir_all(stage_root.as_path())
                .await
                .map_err(migration_error)?;
        }
        tokio::fs::remove_file(journal_path)
            .await
            .map_err(migration_error)?;
        sync_parent_directory(journal_path).await
    }

    pub(super) async fn migrate_legacy_lineage(
        &self,
        selected_source_path: &Path,
        journal_path: &Path,
        plan: LegacyLineageMigrationPlan,
        limiter: &mut RolloutMigrationRateLimiter,
    ) -> ThreadStoreResult<PathBuf> {
        self.migrate_legacy_lineage_inner(
            selected_source_path,
            journal_path,
            plan,
            limiter,
            /*stop_after*/ None,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn migrate_legacy_lineage_until_phase_for_test(
        &self,
        selected_source_path: &Path,
        journal_path: &Path,
        plan: LegacyLineageMigrationPlan,
        limiter: &mut RolloutMigrationRateLimiter,
        stop_after: LineageMigrationPhase,
    ) -> ThreadStoreResult<PathBuf> {
        self.migrate_legacy_lineage_inner(
            selected_source_path,
            journal_path,
            plan,
            limiter,
            Some(stop_after),
        )
        .await
    }

    pub(super) async fn recover_legacy_lineage(
        &self,
        journal_path: &Path,
        limiter: &mut RolloutMigrationRateLimiter,
    ) -> ThreadStoreResult<PathBuf> {
        let journal = read_lineage_migration_journal(journal_path).await?;
        let selected_source_path = journal
            .sources
            .last()
            .map(|source| source.path.clone())
            .ok_or_else(|| migration_error("lineage journal has no selected source"))?;
        let plan = plan_legacy_lineage(
            self.config.codex_home.as_path(),
            selected_source_path.as_path(),
        )
        .await?;
        self.migrate_legacy_lineage(selected_source_path.as_path(), journal_path, plan, limiter)
            .await
    }

    async fn migrate_legacy_lineage_inner(
        &self,
        selected_source_path: &Path,
        journal_path: &Path,
        mut plan: LegacyLineageMigrationPlan,
        limiter: &mut RolloutMigrationRateLimiter,
        stop_after: Option<LineageMigrationPhase>,
    ) -> ThreadStoreResult<PathBuf> {
        self.validate_legacy_lineage_plan(&plan).await?;
        self.validate_legacy_lineage_desktop_compatibility(&mut plan)
            .await?;
        let stage_root = journal_path.with_extension("staging");
        let mut journal = if tokio::fs::try_exists(journal_path)
            .await
            .map_err(migration_error)?
        {
            read_lineage_migration_journal(journal_path).await?
        } else {
            let journal = LineageMigrationJournal::from_plan(&plan);
            write_lineage_migration_journal(journal_path, &journal).await?;
            journal
        };
        if journal.requires_target_identity_upgrade(&plan)? {
            self.restart_preselection_lineage_after_target_upgrade(journal_path, &journal, limiter)
                .await?;
            journal = LineageMigrationJournal::from_plan(&plan);
            write_lineage_migration_journal(journal_path, &journal).await?;
        }
        journal.verify_plan(&plan)?;

        if journal.phase == LineageMigrationPhase::Planned {
            stop_after_phase(stop_after, journal.phase)?;
            journal.verify_sources().await?;
            if tokio::fs::try_exists(stage_root.as_path())
                .await
                .map_err(migration_error)?
            {
                tokio::fs::remove_dir_all(stage_root.as_path())
                    .await
                    .map_err(migration_error)?;
            }
            let staged = stage_legacy_lineage(&plan, stage_root.as_path()).await?;
            journal.record_staged_targets(staged.as_slice())?;
            write_lineage_migration_journal(journal_path, &journal).await?;
            stop_after_phase(stop_after, journal.phase)?;
        }

        if journal.phase == LineageMigrationPhase::TargetsDurable {
            journal.verify_sources().await?;
            journal.verify_staged_targets().await?;
            for target in &journal.targets {
                let staged_path = target
                    .staged_path
                    .as_ref()
                    .ok_or_else(|| migration_error("lineage target is missing its staged path"))?;
                thread_history::delete_thread(self, target.rollout_id).await?;
                prepare_lineage_target_projection(
                    self,
                    target.rollout_id,
                    staged_path,
                    target.start_ordinal.ok_or_else(|| {
                        migration_error("lineage target is missing its start ordinal")
                    })?,
                )
                .await?;
                self.project_rollout_in_batches(target.rollout_id, staged_path, limiter)
                    .await?;
                let projection = thread_history::projection_state(self, target.rollout_id)
                    .await?
                    .ok_or_else(|| {
                        migration_error("staged lineage target has no SQLite projection")
                    })?;
                if projection.next_byte_offset
                    != target.byte_count.ok_or_else(|| {
                        migration_error("lineage target is missing its byte count")
                    })?
                    || projection.next_ordinal
                        != target.end_ordinal_exclusive.ok_or_else(|| {
                            migration_error("lineage target is missing its ordinal boundary")
                        })?
                {
                    return Err(migration_error(
                        "SQLite projection does not cover a complete staged lineage target",
                    ));
                }
            }
            journal.advance(LineageMigrationPhase::ProjectionDurable)?;
            write_lineage_migration_journal(journal_path, &journal).await?;
            stop_after_phase(stop_after, journal.phase)?;
        }

        if journal.phase == LineageMigrationPhase::ProjectionDurable {
            journal.verify_sources().await?;
            publish_lineage_targets(journal_path, &mut journal).await?;
            let selected_target = journal
                .targets
                .iter()
                .find(|target| target.selected)
                .ok_or_else(|| migration_error("lineage journal has no selected target"))?;
            let state_db = self.state_db.as_ref().ok_or_else(|| {
                migration_error("lineage migration requires SQLite thread metadata")
            })?;
            let current = state_db
                .get_thread(journal.selected_thread_id)
                .await
                .map_err(migration_error)?
                .ok_or_else(|| migration_error("selected lineage thread is missing"))?;
            if current.rollout_path != selected_target.path
                && !state_db
                    .replace_rollout_path_if_current(
                        journal.selected_thread_id,
                        selected_source_path,
                        selected_target.path.as_path(),
                    )
                    .await
                    .map_err(migration_error)?
            {
                return Err(ThreadStoreError::Conflict {
                    message: "selected rollout changed during lineage migration".to_string(),
                });
            }
            if !state_db
                .mark_thread_paginated(journal.selected_thread_id)
                .await
                .map_err(migration_error)?
            {
                return Err(migration_error("selected lineage thread is missing"));
            }
            journal.advance(LineageMigrationPhase::Selected)?;
            write_lineage_migration_journal(journal_path, &journal).await?;
            stop_after_phase(stop_after, journal.phase)?;
        }

        if journal.phase == LineageMigrationPhase::Selected {
            verify_published_lineage_targets(&journal).await?;
            let selected_target = journal
                .targets
                .iter()
                .find(|target| target.selected)
                .ok_or_else(|| migration_error("lineage journal has no selected target"))?;
            let selected = self
                .state_db
                .as_ref()
                .ok_or_else(|| migration_error("lineage migration requires SQLite metadata"))?
                .get_thread(journal.selected_thread_id)
                .await
                .map_err(migration_error)?
                .ok_or_else(|| migration_error("selected lineage thread is missing"))?;
            if selected.rollout_path != selected_target.path {
                return Err(migration_error(
                    "selected lineage target does not match SQLite metadata",
                ));
            }
            for target in &journal.targets {
                let projection = thread_history::projection_state(self, target.rollout_id)
                    .await?
                    .ok_or_else(|| {
                        migration_error("published lineage target has no SQLite projection")
                    })?;
                if projection.next_byte_offset
                    != target.byte_count.ok_or_else(|| {
                        migration_error("published lineage target is missing its byte count")
                    })?
                    || projection.next_ordinal
                        != target.end_ordinal_exclusive.ok_or_else(|| {
                            migration_error(
                                "published lineage target is missing its ordinal boundary",
                            )
                        })?
                {
                    return Err(migration_error(
                        "published lineage projection failed verification",
                    ));
                }
            }
            journal.advance(LineageMigrationPhase::Verified)?;
            write_lineage_migration_journal(journal_path, &journal).await?;
            stop_after_phase(stop_after, journal.phase)?;
        }

        if journal.phase == LineageMigrationPhase::Verified {
            journal.advance(LineageMigrationPhase::Complete)?;
            write_lineage_migration_journal(journal_path, &journal).await?;
            stop_after_phase(stop_after, journal.phase)?;
        }
        if journal.phase != LineageMigrationPhase::Complete {
            return Err(migration_error(
                "lineage migration stopped before completion",
            ));
        }
        let selected_path = journal
            .targets
            .iter()
            .find(|target| target.selected)
            .map(|target| target.path.clone())
            .ok_or_else(|| migration_error("lineage journal has no selected target"))?;
        if tokio::fs::try_exists(stage_root.as_path())
            .await
            .map_err(migration_error)?
        {
            tokio::fs::remove_dir_all(stage_root.as_path())
                .await
                .map_err(migration_error)?;
        }
        tokio::fs::remove_file(journal_path)
            .await
            .map_err(migration_error)?;
        sync_parent_directory(journal_path).await?;
        Ok(selected_path)
    }

    async fn restart_preselection_lineage_after_target_upgrade(
        &self,
        journal_path: &Path,
        journal: &LineageMigrationJournal,
        limiter: &mut RolloutMigrationRateLimiter,
    ) -> ThreadStoreResult<()> {
        journal.verify_sources().await?;
        let stage_root = journal_path.with_extension("staging");
        for (index, target) in journal.targets.iter().enumerate() {
            thread_history::delete_thread(self, target.rollout_id).await?;
            if tokio::fs::try_exists(target.path.as_path())
                .await
                .map_err(migration_error)?
            {
                let start_ordinal = target.start_ordinal.ok_or_else(|| {
                    migration_error(
                        "outdated lineage target is missing its projection start ordinal",
                    )
                })?;
                let projection_path = if target
                    .path
                    .extension()
                    .is_some_and(|extension| extension == "zst")
                {
                    let path = stage_root.join(format!("restore-{index:08}.jsonl"));
                    decompress_rollout_to_path(target.path.as_path(), path.as_path()).await?;
                    path
                } else {
                    target.path.clone()
                };
                prepare_lineage_target_projection(
                    self,
                    target.rollout_id,
                    projection_path.as_path(),
                    start_ordinal,
                )
                .await?;
                self.project_rollout_in_batches(
                    target.rollout_id,
                    projection_path.as_path(),
                    limiter,
                )
                .await?;
            }
        }
        if tokio::fs::try_exists(stage_root.as_path())
            .await
            .map_err(migration_error)?
        {
            tokio::fs::remove_dir_all(stage_root.as_path())
                .await
                .map_err(migration_error)?;
        }
        tokio::fs::remove_file(journal_path)
            .await
            .map_err(migration_error)?;
        sync_parent_directory(journal_path).await
    }
}

async fn prepare_lineage_target_projection(
    store: &LocalThreadStore,
    rollout_id: codex_protocol::RolloutId,
    rollout_path: &Path,
    start_ordinal: u64,
) -> ThreadStoreResult<()> {
    let session_meta = codex_rollout::read_session_meta_line(rollout_path)
        .await
        .map_err(migration_error)?;
    if session_meta.meta.history_base.is_some() {
        thread_history::begin_incomplete_paginated_projection(store, rollout_id, start_ordinal)
            .await
    } else {
        thread_history::reset_projection_for_replacement(store, rollout_id, start_ordinal).await
    }
}

fn stop_after_phase(
    stop_after: Option<LineageMigrationPhase>,
    phase: LineageMigrationPhase,
) -> ThreadStoreResult<()> {
    if stop_after == Some(phase) {
        return Err(migration_error(format!(
            "injected lineage migration stop after {phase:?}"
        )));
    }
    Ok(())
}
