use super::*;
use codex_history::CodexHarnessMetadata;
use codex_rollout::GuardianHistoryCheckpoint;
use codex_rollout::ResponseItemEnvelope;
use codex_rollout::RetainedContextReplay;
use codex_rollout::ReviewInputRecord;
use codex_rollout::ReviewTranscriptApplicability;
use pretty_assertions::assert_eq;

fn write_review_input_source(home: &Path, source_id: ThreadId) -> PathBuf {
    write_review_input_source_with_replay(home, source_id, None)
}

fn write_review_input_source_with_replay(
    home: &Path,
    source_id: ThreadId,
    retained_context_replay: Option<RetainedContextReplay>,
) -> PathBuf {
    let baseline = input_response_message("user", "Keep the original review instruction.");
    let RolloutItem::Compacted(mut checkpoint) = compacted(vec![baseline.clone()]) else {
        unreachable!()
    };
    checkpoint.guardian_history =
        Some(serde_json::from_value(json!([baseline])).expect("Guardian transcript baseline"));
    checkpoint.retained_context = Some(Default::default());
    checkpoint.retained_context_replay = retained_context_replay;
    checkpoint.resume_metadata = Some(codex_rollout::CompactionResumeMetadata {
        multi_agent_version: None,
        last_started_turn_id: None,
        turn_attribution: None,
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
async fn remigrated_review_input_preserves_root_legacy_transcript_decision() {
    for root_retains_legacy_transcript in [None, Some(false), Some(true)] {
        let home = TempDir::new().expect("create Codex home");
        let prefix_id = ThreadId::new();
        let prefix_path = codex_rollout::review_input_segment_path(home.path(), prefix_id);
        fs::create_dir_all(prefix_path.parent().expect("prefix directory"))
            .expect("create prefix directory");
        let header = RolloutLine {
            timestamp: TIMESTAMP.to_string(),
            ordinal: Some(0),
            item: RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    id: prefix_id,
                    session_id: prefix_id.into(),
                    segment_id: Some(
                        SegmentId::from_string(&prefix_id.to_string()).expect("segment ID"),
                    ),
                    history_mode: ThreadHistoryMode::Paginated,
                    ..Default::default()
                },
                git: None,
            }),
        };
        let boundary = input_response_message("user", "Earlier temporary instruction.");
        let original_records = vec![
            ReviewInputRecord::Baseline {
                applicability: ReviewTranscriptApplicability::Both,
                history: GuardianHistoryCheckpoint(vec![ResponseItemEnvelope::new(
                    input_response_message("user", "Keep the original review instruction."),
                )]),
                root_retains_legacy_transcript,
            },
            ReviewInputRecord::ResponseItem {
                response: codex_rollout::ResponseItemEnvelope::new(boundary.clone()),
            },
            ReviewInputRecord::Rollback {
                boundary,
                boundary_metadata: None,
            },
        ];
        let mut prefix_bytes = serde_json::to_vec(&header).expect("serialize prefix header");
        prefix_bytes.push(b'\n');
        for (index, record) in original_records.iter().enumerate() {
            serde_json::to_writer(
                &mut prefix_bytes,
                &json!({ "ordinal": index + 1, "record": record }),
            )
            .expect("serialize prefix record");
            prefix_bytes.push(b'\n');
        }
        fs::write(&prefix_path, &prefix_bytes).expect("write immutable prefix");
        let original_position = HistoryPosition {
            thread_id: prefix_id,
            end_ordinal_exclusive: 4,
            end_byte_offset: prefix_bytes.len() as u64,
        };
        let source_id = ThreadId::new();
        write_review_input_source_with_replay(
            home.path(),
            source_id,
            Some(RetainedContextReplay {
                legacy: Default::default(),
                thread_owned_worker: Default::default(),
                thread_owned_root: Default::default(),
                review_input: Some(original_position),
                resolved_review_input: None,
            }),
        );
        let store = indexed_store(home.path()).await;
        let report = store
            .migrate_rollouts(apply_options())
            .await
            .expect("remigrate referenced review prefix");
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, RolloutMigrationStatus::Migrated);
        let published_position = read_rollout(&outcome.rollout_path)
            .into_iter()
            .rev()
            .find_map(|line| match line.item {
                RolloutItem::Compacted(checkpoint) => checkpoint
                    .retained_context_replay
                    .and_then(|replay| replay.review_input),
                _ => None,
            })
            .expect("remigrated finite review prefix");
        assert_ne!(published_position.thread_id, original_position.thread_id);
        let published = codex_rollout::load_review_input_prefix(home.path(), published_position)
            .await
            .expect("load remigrated review prefix");
        assert!(published.len() > original_records.len());
        assert_eq!(&published[..original_records.len()], &original_records);
        assert_eq!(
            fs::read(&prefix_path).expect("read original immutable prefix"),
            prefix_bytes,
        );
    }
}

#[tokio::test]
async fn migrated_review_input_preserves_independent_only_baseline_envelopes() {
    for has_retained_replay in [false, true] {
        let home = TempDir::new().expect("create Codex home");
        let baseline = ResponseItemEnvelope {
            item: input_response_message("user", "Keep the original review instruction."),
            metadata: Some(CodexHarnessMetadata {
                user_input_order: Some(7),
                history_truncation_token_limit: Some(128),
                ..Default::default()
            }),
        };
        let summary = ResponseItemEnvelope {
            item: input_response_message("assistant", "Compaction summary."),
            metadata: Some(CodexHarnessMetadata {
                compaction_output: true,
                ..Default::default()
            }),
        };
        let opaque = ResponseItemEnvelope::new(
            serde_json::from_value(json!({
                "type": "compaction", "id": "cmp_base", "encrypted_content": "opaque",
            }))
            .expect("opaque compaction item"),
        );
        let RolloutItem::Compacted(mut checkpoint) = compacted(Vec::new()) else {
            unreachable!()
        };
        checkpoint.replacement_history = Some(vec![baseline.clone(), summary, opaque]);
        // Incomplete retained evidence prevents older reviewers from using this partial window.
        checkpoint.retained_context = Some(
            serde_json::from_value(json!({ "verified_answers": [], "incomplete": true }))
                .expect("incomplete retained context"),
        );
        checkpoint.retained_context_replay = has_retained_replay.then(|| RetainedContextReplay {
            legacy: Default::default(),
            thread_owned_worker: Default::default(),
            thread_owned_root: Default::default(),
            review_input: None,
            resolved_review_input: None,
        });
        checkpoint.resume_metadata = Some(codex_rollout::CompactionResumeMetadata {
            multi_agent_version: None,
            last_started_turn_id: None,
            turn_attribution: None,
            previous_turn_settings: None,
        });
        let mut temporary = ResponseItemEnvelope {
            item: input_response_message("user", "Temporary review instruction."),
            metadata: Some(CodexHarnessMetadata {
                user_input_order: Some(8),
                ..Default::default()
            }),
        };
        temporary.item.set_turn_id_if_missing("temporary-turn");
        write_rollout(
            home.path(),
            ThreadId::new(),
            SessionSource::Cli,
            vec![
                RolloutItem::Compacted(checkpoint),
                started("temporary-turn"),
                RolloutItem::ResponseItem(temporary.clone()),
                completed("temporary-turn"),
                RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                    num_turns: 1,
                })),
            ],
        );
        let store = indexed_store(home.path()).await;
        let report = store
            .migrate_rollouts(apply_options())
            .await
            .expect("migrate independent review transcript");
        assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
        let position = read_rollout(&report.outcomes[0].rollout_path)
            .into_iter()
            .rev()
            .find_map(|line| match line.item {
                RolloutItem::Compacted(checkpoint) => checkpoint
                    .retained_context_replay
                    .and_then(|replay| replay.review_input),
                _ => None,
            })
            .expect("finite independent review input reference");
        assert_eq!(
            codex_rollout::load_review_input_prefix(home.path(), position)
                .await
                .expect("published independent review prefix"),
            vec![
                ReviewInputRecord::Baseline {
                    applicability: ReviewTranscriptApplicability::Legacy,
                    history: GuardianHistoryCheckpoint(vec![baseline]),
                    root_retains_legacy_transcript: Some(false),
                },
                ReviewInputRecord::ResponseItem {
                    response: temporary.clone(),
                },
                ReviewInputRecord::Rollback {
                    boundary: temporary.item,
                    boundary_metadata: temporary.metadata,
                },
            ],
        );
    }
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
