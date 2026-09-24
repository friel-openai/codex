use super::*;
use codex_protocol::ThreadId;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn migration_preserves_latest_runtime_version_without_replacing_original_metadata() {
    let home = tempfile::tempdir().unwrap();
    let thread_id = ThreadId::new();
    let update = |id, version| {
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id,
                multi_agent_version: version,
                cwd: home.path().join("not-the-original-cwd"),
                ..SessionMeta::default()
            },
            git: None,
        })
    };
    let path = super::super::tests::write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            update(thread_id, Some(MultiAgentVersion::V1)),
            update(thread_id, Some(MultiAgentVersion::V2)),
            update(ThreadId::new(), Some(MultiAgentVersion::Disabled)),
            update(thread_id, None),
        ],
    );
    let original = codex_rollout::read_session_meta_line(&path).await.unwrap();
    let mut expected = original.clone();
    expected.meta.multi_agent_version = Some(MultiAgentVersion::V2);
    assert_eq!(
        serde_json::to_value(canonical_session_meta(&path).await.unwrap().item).unwrap(),
        serde_json::to_value(RolloutItem::SessionMeta(expected.clone())).unwrap(),
    );

    let store = super::super::tests::indexed_store(home.path()).await;
    let dry_run = store
        .migrate_rollouts(super::super::RolloutMigrationOptions::default())
        .await
        .unwrap();
    let manifest = dry_run.outcomes[0].manifest.as_ref().unwrap();
    let result = store
        .migrate_rollouts(super::super::tests::apply_options())
        .await
        .unwrap();
    assert_eq!(
        result.outcomes[0].status,
        super::super::RolloutMigrationStatus::Migrated
    );
    expected.meta.history_mode = codex_protocol::protocol::ThreadHistoryMode::Paginated;
    assert_eq!(
        serde_json::to_value(codex_rollout::read_session_meta_line(&path).await.unwrap()).unwrap(),
        serde_json::to_value(expected).unwrap(),
    );
    let (bytes, digest) = super::super::lineage::hash_file(&path).await.unwrap();
    assert_eq!(
        (bytes, digest),
        (
            manifest.targets[0].byte_count,
            manifest.targets[0].sha256.clone()
        )
    );
}

#[tokio::test]
async fn metadata_candidate_accepts_escaped_discriminant_and_rejects_payload_false_positive() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("rollout.jsonl");
    let mut metadata = SessionMetaLine {
        meta: SessionMeta::default(),
        git: None,
    };
    let payload = serde_json::to_string(&metadata).unwrap();
    metadata.meta.multi_agent_version = Some(MultiAgentVersion::V2);
    let updated_payload = serde_json::to_string(&metadata).unwrap();
    std::fs::write(&path, format!(
        "{{\"timestamp\":\"2025-01-03T12:00:00Z\",\"type\":\"session_meta\",\"payload\":{payload}}}\n{{\"timestamp\":\"2025-01-03T12:00:00Z\",\"t\\u0079pe\":\"sess\\u0069on_meta\",\"payload\":{updated_payload}}}\n{{\"timestamp\":\"2025-01-03T12:00:00Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"agent_message\",\"message\":\"session_meta\"}}}}\n"
    )).unwrap();
    assert_eq!(
        serde_json::to_value(canonical_session_meta(&path).await.unwrap().item).unwrap(),
        serde_json::to_value(RolloutItem::SessionMeta(metadata)).unwrap(),
    );
}
