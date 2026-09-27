use super::*;
use pretty_assertions::assert_eq;

fn write_review_input_source(home: &Path, source_id: ThreadId) -> PathBuf {
    let baseline = input_response_message("user", "Keep the original review instruction.");
    let RolloutItem::Compacted(mut checkpoint) = compacted(vec![baseline.clone()]) else {
        unreachable!()
    };
    checkpoint.guardian_history =
        Some(serde_json::from_value(json!([baseline])).expect("Guardian transcript baseline"));
    checkpoint.retained_context = Some(Default::default());
    checkpoint.resume_metadata = Some(codex_rollout::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        previous_turn_settings: None,
    });
    let mut temporary = codex_rollout::ResponseItemEnvelope::new(input_response_message(
        "user",
        "Temporary review instruction.",
    ));
    temporary.item.set_turn_id_if_missing("temporary-turn");
    write_rollout(
        home,
        source_id,
        SessionSource::Cli,
        vec![
            RolloutItem::Compacted(checkpoint),
            started("temporary-turn"),
            RolloutItem::ResponseItem(temporary),
            completed("temporary-turn"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
        ],
    )
}

#[tokio::test]
async fn migrated_review_input_survives_source_archive_and_deletion() {
    let home = TempDir::new().expect("create Codex home");
    let source_id = ThreadId::new();
    write_review_input_source(home.path(), source_id);
    let store = indexed_store(home.path()).await;
    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate review transcript");
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    let checkpoint = read_rollout(&report.outcomes[0].rollout_path)
        .into_iter()
        .rev()
        .find_map(|line| match line.item {
            RolloutItem::Compacted(checkpoint) => Some(checkpoint),
            _ => None,
        })
        .expect("synthetic rollback checkpoint");
    let position = checkpoint
        .retained_context_replay
        .as_ref()
        .and_then(|replay| replay.review_input)
        .expect("finite review input reference");
    let expected = codex_rollout::load_review_input_prefix(home.path(), position)
        .await
        .expect("published review prefix");
    let dependency = codex_rollout::review_input_segment_path(home.path(), position.thread_id);
    assert!(
        dependency.starts_with(
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        )
    );

    // A standalone task can retain the checkpoint without referencing the source conversation.
    let other_id = ThreadId::new();
    let other_path = write_rollout(
        home.path(),
        other_id,
        SessionSource::Cli,
        vec![RolloutItem::Compacted(checkpoint)],
    );
    store
        .archive_thread(crate::ArchiveThreadParams {
            thread_id: source_id,
        })
        .await
        .expect("archive source conversation");
    assert_eq!(
        codex_rollout::load_review_input_prefix(home.path(), position)
            .await
            .expect("review input after archive"),
        expected,
    );
    store
        .delete_thread(crate::DeleteThreadParams {
            thread_id: source_id,
        })
        .await
        .expect("delete source conversation without deleting detached review input");
    assert!(dependency.exists());
    let mut retained = read_rollout(&other_path)
        .into_iter()
        .find_map(|line| match line.item {
            RolloutItem::Compacted(checkpoint) => Some(checkpoint),
            _ => None,
        })
        .expect("other task checkpoint");
    codex_rollout::resolve_review_input(home.path(), &mut retained)
        .await
        .expect("other task resolves its finite prefix after source deletion");
    assert_eq!(
        retained
            .retained_context_replay
            .expect("retained replay")
            .resolved_review_input
            .as_deref(),
        Some(&expected),
    );
}

#[tokio::test]
async fn review_input_publication_recovers_from_every_durable_phase() {
    for phase in [
        LineageMigrationPhase::Planned,
        LineageMigrationPhase::TargetsDurable,
        LineageMigrationPhase::ProjectionDurable,
        LineageMigrationPhase::Selected,
        LineageMigrationPhase::Verified,
        LineageMigrationPhase::Complete,
    ] {
        let home = TempDir::new().expect("create Codex home");
        let thread_id = ThreadId::new();
        let source_path = write_review_input_source(home.path(), thread_id);
        let source_bytes = fs::read(&source_path).expect("source bytes");
        let store = indexed_store(home.path()).await;
        let plan = plan_legacy_lineage(home.path(), &source_path)
            .await
            .expect("plan review input transaction");
        let dependency = plan
            .review_input_target
            .as_ref()
            .expect("planned review input")
            .path
            .clone();
        let journal_path = migration_journal_path(home.path(), thread_id);
        let mut limiter = RolloutMigrationRateLimiter::new(None).expect("migration limiter");
        let error = store
            .migrate_legacy_lineage_until_phase_for_test(
                &source_path,
                &journal_path,
                plan,
                &mut limiter,
                phase,
            )
            .await
            .expect_err("stop at durable phase");
        assert!(
            error
                .to_string()
                .contains("injected lineage migration stop")
        );
        let journal: serde_json::Value =
            serde_json::from_slice(&fs::read(&journal_path).expect("durable transaction journal"))
                .expect("journal JSON");
        assert!(
            journal
                .get("review_input_target")
                .is_some_and(|target| !target.is_null())
        );
        if matches!(
            phase,
            LineageMigrationPhase::Selected
                | LineageMigrationPhase::Verified
                | LineageMigrationPhase::Complete
        ) {
            assert!(
                dependency.exists(),
                "selected rollout requires its durable dependency"
            );
        }
        drop(store);

        let restarted = indexed_store(home.path()).await;
        let report = restarted
            .migrate_rollouts(apply_options())
            .await
            .expect("recover review migration");
        assert!(report.outcomes.iter().all(|outcome| matches!(
            outcome.status,
            RolloutMigrationStatus::Migrated | RolloutMigrationStatus::AlreadyPaginated
        )));
        let selected = restarted
            .state_db
            .as_ref()
            .expect("state DB")
            .get_thread(thread_id)
            .await
            .expect("read selected metadata")
            .expect("selected thread");
        let position = read_rollout(&selected.rollout_path)
            .into_iter()
            .rev()
            .find_map(|line| match line.item {
                RolloutItem::Compacted(checkpoint) => checkpoint
                    .retained_context_replay
                    .and_then(|replay| replay.review_input),
                _ => None,
            })
            .expect("recovered finite checkpoint");
        codex_rollout::load_review_input_prefix(home.path(), position)
            .await
            .expect("recovered immutable prefix");
        assert_eq!(
            fs::read(&source_path).expect("retained source"),
            source_bytes
        );
        assert!(!journal_path.exists());
    }
}
