use std::fs;

use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ARCHIVED_SESSIONS_SUBDIR;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use crate::ArchiveThreadParams;
use crate::ThreadStore;
use crate::local::LocalThreadStore;
use crate::local::test_support::test_config;
use crate::local::test_support::write_session_file_with_history_mode;

#[tokio::test]
async fn archive_preserves_selected_alias_and_moves_owned_physical_rollouts_only() {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new().expect("home");
        let config = test_config(home.path());
        let state = codex_state::StateRuntime::init(
            codex_state::SqliteConfig::new_for_testing(home.path().abs()),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state");
        state
            .mark_backfill_complete(/*last_watermark*/ None)
            .await
            .expect("backfill");
        let store = LocalThreadStore::new(config.clone(), Some(state.clone()));
        let id = Uuid::from_u128(301);
        let thread_id = ThreadId::from_string(&id.to_string()).expect("thread id");
        let selected = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            id,
            history_mode,
        )
        .expect("selected");
        let contents = fs::read(&selected).expect("contents");
        let retained =
            codex_rollout::history_rollout_path_with_rollout_id(&selected, ThreadId::new())
                .expect("retained path");
        fs::write(&retained, &contents).expect("retained");
        let compressed =
            codex_rollout::history_rollout_path_with_rollout_id(&selected, ThreadId::new())
                .expect("compressed path")
                .with_extension("jsonl.zst");
        let compressed_bytes = zstd::stream::encode_all(contents.as_slice(), 3).expect("compress");
        fs::write(&compressed, &compressed_bytes).expect("compressed");

        let parent = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T11-00-00",
            Uuid::from_u128(302),
            history_mode,
        )
        .expect("parent");
        let parent_contents = fs::read(&parent).expect("parent contents");
        // A matching filename does not authorize moving a file owned by another thread.
        let foreign =
            codex_rollout::history_rollout_path_with_rollout_id(&selected, ThreadId::new())
                .expect("foreign path");
        fs::write(&foreign, &parent_contents).expect("foreign");

        // SQLite can select an authenticated alias whose filename encodes a different ID.
        let alias = selected.with_file_name(format!(
            "rollout-2025-01-03T12-00-00-{}.jsonl",
            ThreadId::new(),
        ));
        fs::rename(&selected, &alias).expect("alias");
        let mut metadata = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            alias.clone(),
            Utc::now(),
            SessionSource::Cli,
        )
        .build(&config.default_model_provider_id);
        metadata.history_mode = history_mode;
        state
            .upsert_thread(&metadata)
            .await
            .expect("selected metadata");

        store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("archive");
        let archived = home.path().join(ARCHIVED_SESSIONS_SUBDIR);
        for (path, bytes) in [
            (&alias, &contents),
            (&retained, &contents),
            (&compressed, &compressed_bytes),
        ] {
            assert!(!path.exists());
            assert_eq!(
                fs::read(archived.join(path.file_name().expect("filename")))
                    .expect("archived bytes"),
                *bytes
            );
        }
        assert_eq!(
            fs::read(&parent).expect("unchanged parent"),
            parent_contents
        );
        assert_eq!(
            fs::read(&foreign).expect("unchanged foreign"),
            parent_contents
        );
        let updated = state
            .get_thread(thread_id)
            .await
            .expect("metadata")
            .expect("thread");
        assert_eq!(
            updated.rollout_path,
            archived.join(alias.file_name().expect("alias name"))
        );
        assert!(updated.archived_at.is_some());
    }
}

#[tokio::test]
async fn archive_leaves_matching_filename_with_malformed_session_meta_untouched() {
    let home = TempDir::new().expect("home");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let id = Uuid::from_u128(304);
    let thread_id = ThreadId::from_string(&id.to_string()).expect("thread id");
    let selected = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T12-00-00",
        id,
        ThreadHistoryMode::Legacy,
    )
    .expect("selected");
    let selected_bytes = fs::read(&selected).expect("selected bytes");
    let malformed_candidate =
        codex_rollout::history_rollout_path_with_rollout_id(&selected, ThreadId::new())
            .expect("matching candidate path");
    let malformed_bytes = br#"{"timestamp":"2025-01-03T11:59:59Z","type":"session_meta""#;
    fs::write(&malformed_candidate, malformed_bytes).expect("malformed candidate");

    store
        .archive_thread(ArchiveThreadParams { thread_id })
        .await
        .expect("archive selected rollout");

    let archived = home.path().join(ARCHIVED_SESSIONS_SUBDIR);
    assert_eq!(
        fs::read(archived.join(selected.file_name().expect("selected filename")))
            .expect("archived selected bytes"),
        selected_bytes
    );
    assert_eq!(
        fs::read(&malformed_candidate).expect("unchanged malformed candidate"),
        malformed_bytes
    );
    assert!(
        !archived
            .join(
                malformed_candidate
                    .file_name()
                    .expect("malformed candidate filename")
            )
            .exists()
    );
}

#[tokio::test]
async fn archive_moves_sqlite_selected_damaged_alias_byte_for_byte() {
    let home = TempDir::new().expect("home");
    let config = test_config(home.path());
    let state = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state");
    state
        .mark_backfill_complete(/*last_watermark*/ None)
        .await
        .expect("backfill");
    let store = LocalThreadStore::new(config.clone(), Some(state.clone()));
    let id = Uuid::from_u128(305);
    let thread_id = ThreadId::from_string(&id.to_string()).expect("thread id");
    let selected = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T12-00-00",
        id,
        ThreadHistoryMode::Legacy,
    )
    .expect("selected");
    let alias = selected.with_file_name(format!(
        "rollout-2025-01-03T12-00-00-{}.jsonl",
        ThreadId::new(),
    ));
    fs::rename(&selected, &alias).expect("alias selected rollout");
    let damaged_bytes = br#"{"timestamp":"2025-01-03T12:00:00Z","type":"session_meta","payload":"#;
    fs::write(&alias, damaged_bytes).expect("damage selected alias");
    let metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        alias.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(&config.default_model_provider_id);
    state
        .upsert_thread(&metadata)
        .await
        .expect("selected metadata");

    store
        .archive_thread(ArchiveThreadParams { thread_id })
        .await
        .expect("archive damaged selected alias");

    let archived_path = home
        .path()
        .join(ARCHIVED_SESSIONS_SUBDIR)
        .join(alias.file_name().expect("alias filename"));
    assert!(!alias.exists());
    assert_eq!(
        fs::read(&archived_path).expect("archived damaged bytes"),
        damaged_bytes
    );
    let updated = state
        .get_thread(thread_id)
        .await
        .expect("metadata")
        .expect("thread");
    assert_eq!(updated.rollout_path, archived_path);
    assert!(updated.archived_at.is_some());
}

#[tokio::test]
async fn archive_restores_already_moved_rollouts_after_rename_failure() {
    let home = TempDir::new().expect("home");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let id = Uuid::from_u128(303);
    let thread_id = ThreadId::from_string(&id.to_string()).expect("thread id");
    let selected = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T12-00-00",
        id,
        ThreadHistoryMode::Legacy,
    )
    .expect("selected");
    let retained = codex_rollout::history_rollout_path_with_rollout_id(&selected, ThreadId::new())
        .expect("retained path");
    let contents = fs::read(&selected).expect("contents");
    fs::write(&retained, &contents).expect("retained");
    let archive_folder = home.path().join(ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir_all(archive_folder.join(selected.file_name().expect("filename")))
        .expect("block selected destination with directory");

    // Explicit order guarantees that one rename succeeds before the second fails.
    super::archive_thread_with_paths(&store, thread_id, vec![retained.clone(), selected.clone()])
        .await
        .expect_err("rename must fail");
    assert_eq!(fs::read(&retained).expect("restored retained"), contents);
    assert_eq!(fs::read(&selected).expect("selected preserved"), contents);
    assert!(
        !archive_folder
            .join(retained.file_name().expect("filename"))
            .exists()
    );
}
