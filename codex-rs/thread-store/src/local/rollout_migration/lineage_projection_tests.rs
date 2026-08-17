use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;

use super::RolloutMigrationRateLimiter;
use super::lineage::plan_legacy_lineage;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_projection::BULK_PROJECTION_BUDGET;
use super::lineage_projection::try_project_staged_targets;
use super::lineage_stage::stage_legacy_lineage;
use super::tests::complete_projection_rows;
use super::tests::indexed_store;
use super::tests::user_message;
use super::tests::write_rollout;
use super::thread_history;

#[tokio::test]
async fn bulk_lineage_projection_checks_phase_budget_and_authenticated_coordinates() {
    let home = tempfile::tempdir().expect("Codex home");
    let source = write_rollout(
        home.path(),
        ThreadId::new(),
        SessionSource::Cli,
        vec![user_message("retained user message")],
    );
    let plan = plan_legacy_lineage(home.path(), &source)
        .await
        .expect("plan source");
    let stage = tempfile::tempdir().expect("private stage");
    let staged = stage_legacy_lineage(&plan, stage.path())
        .await
        .expect("canonical targets");
    let store = indexed_store(home.path()).await;
    let mut journal = LineageMigrationJournal::from_plan(&plan);
    let mut limiter =
        RolloutMigrationRateLimiter::new(/*max_mib_per_second*/ None).expect("unlimited migration");
    assert!(
        try_project_staged_targets(
            &store,
            &journal,
            /*complete_root*/ None,
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .is_err()
    );
    journal
        .record_staged_targets(&staged)
        .expect("durable targets");
    journal.verify_sources().await.expect("unchanged source");
    journal
        .verify_staged_targets()
        .await
        .expect("authenticated target");
    let target = journal.targets[0].rollout_id;
    assert!(
        !try_project_staged_targets(
            &store,
            &journal,
            Some(target),
            &mut limiter,
            /*memory_budget*/ 0
        )
        .await
        .expect("bounded fallback")
    );
    assert!(
        thread_history::projection_state(&store, target)
            .await
            .expect("projection state")
            .is_none()
    );

    let mut wrong_boundary = journal.clone();
    *wrong_boundary.targets[0]
        .byte_count
        .as_mut()
        .expect("byte boundary") += 1;
    assert!(
        try_project_staged_targets(
            &store,
            &wrong_boundary,
            Some(target),
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .is_err()
    );
    assert!(
        thread_history::projection_state(&store, target)
            .await
            .expect("projection state")
            .is_none()
    );

    assert!(
        try_project_staged_targets(
            &store,
            &journal,
            Some(target),
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .expect("bulk projection")
    );
    let reference = ThreadId::new();
    thread_history::reset_projection_for_replacement(
        &store, reference, /*next_rollout_ordinal*/ 0,
    )
    .await
    .expect("reference checkpoint");
    store
        .project_rollout_in_batches(
            reference,
            &staged[0].staged_path,
            /*complete_root*/ None,
            &mut limiter,
        )
        .await
        .expect("ordered SQL writer");
    assert_eq!(
        complete_projection_rows(&store, target).await,
        complete_projection_rows(&store, reference).await
    );
}
