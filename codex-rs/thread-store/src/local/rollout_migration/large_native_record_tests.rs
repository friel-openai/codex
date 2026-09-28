use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn migration_replays_large_native_checkpoint_with_finite_history_base() {
    for with_rollback in [false, true] {
        for compressed in [false, true] {
            let home = TempDir::new().expect("create Codex home");
            let thread_id = ThreadId::new();
            let native_rollout_id = ThreadId::new();
            let native_path = home
                .path()
                .join(codex_rollout::SESSIONS_SUBDIR)
                .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
                .join("2025/01/03")
                .join(format!(
                    "rollout-2025-01-03T12-00-00-{thread_id}_{native_rollout_id}.jsonl"
                ));
            let mut surviving_user =
                input_response_message("user", "surviving native model context");
            surviving_user.set_turn_id_if_missing("surviving-native-turn");
            let large_text = "x".repeat(super::super::MAX_ROLLOUT_LINE_BYTES);
            let expected_model_digest = Sha256::digest(large_text.as_bytes());
            let large_response = ResponseItem::Message {
                id: None,
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText { text: large_text }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            };
            let RolloutItem::Compacted(mut checkpoint) =
                compacted(vec![surviving_user.clone(), large_response])
            else {
                unreachable!("compacted helper creates a checkpoint");
            };
            let guardian_history = vec![
                surviving_user.clone(),
                input_response_message("developer", "surviving native review evidence"),
            ];
            checkpoint.guardian_history = Some(
                serde_json::from_value(json!(guardian_history))
                    .expect("Guardian checkpoint fixture"),
            );
            checkpoint.resume_metadata = Some(codex_rollout::CompactionResumeMetadata {
                multi_agent_version: None,
                last_started_turn_id: Some("surviving-native-turn".to_string()),
                previous_turn_settings: None,
            });
            let answer = serde_json::from_value(json!({
                "type": "verified_answer",
                "turn_id": "surviving-native-turn",
                "call_id": "surviving-native-answer",
                "questions": [{"question": "Publish?", "answer": "Only privately."}],
            }))
            .expect("verified answer fixture");
            let mut retained = codex_rollout::RetainedContext::default();
            retained.record(&answer);
            let expected_answers =
                serde_json::to_value(retained.verified_answers().collect::<Vec<_>>())
                    .expect("serialize retained answers");
            checkpoint.retained_context = Some(retained);
            let checkpoint = RolloutItem::Compacted(checkpoint);
            assert!(
                serde_json::to_vec(&checkpoint)
                    .expect("serialize large checkpoint")
                    .len()
                    > super::super::MAX_ROLLOUT_LINE_BYTES
            );
            let native_end = write_paginated_segment(
                &native_path,
                home.path(),
                thread_id,
                SegmentId::new(),
                /*start_ordinal*/ 0,
                vec![
                    turn_started("surviving-native-turn"),
                    completed_user_message(
                        thread_id,
                        "surviving-native-turn",
                        "surviving-native-item",
                        "surviving native model context",
                    ),
                    turn_complete("surviving-native-turn"),
                    checkpoint,
                ],
            );
            let history_base = HistoryPosition {
                thread_id: native_rollout_id,
                end_ordinal_exclusive: native_end,
                end_byte_offset: fs::metadata(&native_path)
                    .expect("native prefix metadata")
                    .len(),
            };
            // Both coordinates stop before a valid suffix, rather than merely selecting EOF.
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&native_path)
                .expect("open native suffix");
            for (offset, item) in [
                turn_started("excluded-native-turn"),
                completed_user_message(
                    thread_id,
                    "excluded-native-turn",
                    "excluded-native-item",
                    "excluded native suffix",
                ),
                rollout_response_item(input_response_message("user", "excluded native suffix")),
                turn_complete("excluded-native-turn"),
            ]
            .into_iter()
            .enumerate()
            {
                let line = RolloutLine {
                    timestamp: TIMESTAMP.to_string(),
                    ordinal: Some(native_end + offset as u64),
                    item,
                };
                writeln!(
                    file,
                    "{}",
                    serde_json::to_string(&line).expect("serialize suffix")
                )
                .expect("append native suffix");
            }
            drop(file);
            assert!(fs::metadata(&native_path).unwrap().len() > history_base.end_byte_offset);
            let native_path = if compressed {
                compress_rollout(&native_path)
            } else {
                native_path
            };

            let active_path = home
                .path()
                .join("sessions/2025/01/03")
                .join(format!("rollout-2025-01-03T12-00-01-{thread_id}.jsonl"));
            let mut later_user = input_response_message("user", "later legacy model context");
            later_user.set_turn_id_if_missing("later-legacy-turn");
            let mut legacy_items = vec![
                turn_started("later-legacy-turn"),
                rollout_response_item(later_user.clone()),
                user_message("later legacy model context"),
                turn_complete("later-legacy-turn"),
            ];
            if with_rollback {
                legacy_items.push(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
                    ThreadRolledBackEvent { num_turns: 1 },
                )));
            }
            write_legacy_segment(
                &active_path,
                home.path(),
                thread_id,
                SegmentId::new(),
                legacy_items,
            );
            set_history_base(&active_path, history_base);
            let source_hashes = [
                hash_file(&native_path).await.expect("hash native source"),
                hash_file(&active_path).await.expect("hash Legacy source"),
            ];
            let plan = plan_legacy_lineage(home.path(), &active_path)
                .await
                .expect("plan large native checkpoint migration");
            assert_eq!(plan.replay_native_rollbacks, with_rollback);
            if with_rollback {
                let native_source = plan
                    .sources
                    .iter()
                    .find(|source| source.path == native_path)
                    .expect("rollback planning must replay the large native checkpoint");
                assert_eq!(native_source.replay_end, Some(history_base));
                assert_eq!(native_source.history_mode, ThreadHistoryMode::Paginated);
            } else {
                assert_eq!(plan.history_bases.len(), 1);
                assert_eq!(plan.history_bases[0].position, history_base);
            }
            let store = indexed_store(home.path()).await;
            let report = store
                .migrate_rollouts(RolloutMigrationOptions {
                    thread_ids: vec![thread_id],
                    ..apply_options()
                })
                .await
                .expect("migrate large native checkpoint");
            assert_eq!(
                report.outcomes[0].status,
                RolloutMigrationStatus::Migrated,
                "with_rollback={with_rollback}, compressed={compressed}: {:?}",
                report.outcomes[0]
            );
            let materialized = codex_rollout::materialize_rollout_lines(
                home.path(),
                &report.outcomes[0].rollout_path,
            )
            .await
            .expect("materialize migrated finite native prefix");
            let materialized_json =
                serde_json::to_string(&materialized).expect("serialize migrated history");
            assert!(!materialized_json.contains("excluded native suffix"));
            assert_eq!(
                materialized_json.contains("later legacy model context"),
                !with_rollback
            );

            let context = store
                .load_latest_model_context(LoadThreadHistoryParams {
                    thread_id,
                    include_archived: false,
                })
                .await
                .expect("load surviving model checkpoint");
            let checkpoint = context
                .items
                .iter()
                .rev()
                .find_map(|item| match item {
                    RolloutItem::Compacted(checkpoint) => Some(checkpoint),
                    _ => None,
                })
                .expect("large checkpoint survives migration and rollback");
            let replacement = checkpoint
                .replacement_history
                .as_ref()
                .expect("surviving model replacement history");
            assert_eq!(replacement.len(), 2);
            assert_eq!(replacement[0].item, surviving_user);
            let ResponseItem::Message { role, content, .. } = &replacement[1].item else {
                panic!("large assistant response must survive");
            };
            assert_eq!(role, "assistant");
            let [ContentItem::OutputText { text }] = content.as_slice() else {
                panic!("large assistant text must survive");
            };
            assert_eq!(Sha256::digest(text.as_bytes()), expected_model_digest);
            let retained = checkpoint
                .retained_context
                .as_ref()
                .expect("surviving review authorization");
            assert_eq!(
                serde_json::to_value(retained.verified_answers().collect::<Vec<_>>())
                    .expect("serialize surviving answers"),
                expected_answers
            );
            if with_rollback {
                let position = checkpoint
                    .retained_context_replay
                    .as_ref()
                    .and_then(|replay| replay.review_input)
                    .expect("rollback retains the native review baseline");
                let review_inputs = codex_rollout::load_review_input_prefix(home.path(), position)
                    .await
                    .expect("load surviving review evidence");
                assert!(matches!(
                    review_inputs.first(),
                    Some(codex_history::ReviewInputRecord::Baseline { history, .. })
                        if history.0 == guardian_history
                ));
                assert!(matches!(
                    review_inputs.last(),
                    Some(codex_history::ReviewInputRecord::Rollback { boundary })
                        if *boundary == later_user
                ));
                assert!(
                    !serde_json::to_string(&review_inputs)
                        .expect("serialize review evidence")
                        .contains("excluded native suffix")
                );
            } else {
                assert_eq!(
                    checkpoint
                        .guardian_history
                        .as_ref()
                        .map(|history| &history.0),
                    Some(&guardian_history)
                );
            }
            assert_eq!(
                [
                    hash_file(&native_path).await.expect("rehash native source"),
                    hash_file(&active_path).await.expect("rehash Legacy source"),
                ],
                source_hashes,
                "migration must retain both original sources unchanged"
            );
        }
    }
}
