use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::InternalSessionSource;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSource;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::super::test_support::write_session_file_with;
use super::ThreadSearchItem;
use super::cursor_from_thread_search_item;
use super::is_user_chat;
use super::search_candidate;
use crate::SearchThreadsParams;
use crate::SortDirection;
use crate::ThreadSortKey;
use crate::ThreadStore;

#[test]
fn recency_cursor_includes_thread_id_tie_breaker() {
    let thread_id = ThreadId::from_string("00000000-0000-0000-0000-000000000123")
        .expect("thread ID should parse");
    let item = ThreadSearchItem {
        item: codex_state::ThreadMetadataBuilder::new(
            thread_id,
            "rollout.jsonl".into(),
            "2026-01-27T12:34:56Z".parse().unwrap(),
            SessionSource::Cli,
        )
        .build("test-provider"),
        snippet: String::new(),
    };

    let cursor = cursor_from_thread_search_item(&item, ThreadSortKey::RecencyAt)
        .expect("cursor should build");

    assert_eq!(
        serde_json::to_string(&cursor).expect("cursor should serialize"),
        format!("\"2026-01-27T12:34:56Z|{thread_id}\"")
    );
}

#[tokio::test]
async fn search_matches_selected_rollout_across_path_spellings_and_compression() {
    for compressed in [false, true] {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let state_db = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("initialize state database");
        state_db.mark_backfill_complete(None).await.unwrap();
        let uuid = Uuid::new_v4();
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread ID should parse");
        let selected_path = write_session_file_with(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-03T12-00-00",
            uuid,
            "needle in selected rollout",
            Some("test-provider"),
            ThreadHistoryMode::Legacy,
        )
        .expect("write selected rollout");
        // Reverting a thread leaves its previous rollout on disk with the same thread ID.
        write_session_file_with(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-03T11-00-00",
            uuid,
            "obsolete needle in previous rollout",
            Some("test-provider"),
            ThreadHistoryMode::Legacy,
        )
        .expect("write unselected rollout");

        let home_paths = [home.path().to_path_buf()];
        #[cfg(windows)]
        let home_paths = {
            let verbatim = std::fs::canonicalize(&home_paths[0]).expect("canonicalize home");
            let ordinary =
                codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path(&verbatim)
                    .expect("normalize home")
                    .into_path_buf();
            [ordinary, verbatim]
        };
        let relative_path = selected_path
            .strip_prefix(home.path())
            .expect("relative rollout path");
        let selected_paths = home_paths.each_ref().map(|home| home.join(relative_path));
        #[cfg(windows)]
        assert_ne!(selected_paths[0], selected_paths[1]);

        if compressed {
            let contents = std::fs::read(&selected_path).expect("read selected rollout");
            let contents = zstd::stream::encode_all(contents.as_slice(), /*level*/ 3)
                .expect("compress selected rollout");
            std::fs::write(selected_path.with_extension("jsonl.zst"), contents)
                .expect("write compressed rollout");
            std::fs::remove_file(&selected_path).expect("remove plain rollout");
        }
        let selected_paths = selected_paths
            .into_iter()
            .flat_map(|path| {
                let compressed_path = compressed.then(|| path.with_extension("jsonl.zst"));
                std::iter::once(path).chain(compressed_path)
            })
            .collect::<Vec<_>>();

        for codex_home in home_paths {
            let store = LocalThreadStore::new(
                super::super::LocalThreadStoreConfig {
                    codex_home,
                    ..config.clone()
                },
                Some(state_db.clone()),
            );
            for selected_path in &selected_paths {
                let mut metadata = codex_state::ThreadMetadataBuilder::new(
                    thread_id,
                    selected_path.clone(),
                    Utc::now(),
                    SessionSource::Cli,
                )
                .build("test-provider");
                metadata.first_user_message = Some("needle in selected rollout".to_string());
                state_db
                    .upsert_thread(&metadata)
                    .await
                    .expect("select rollout in database");

                for (search_term, expected) in [
                    ("needle", vec![(thread_id, "needle in selected rollout")]),
                    ("obsolete", Vec::new()),
                ] {
                    let page = store
                        .search_threads(SearchThreadsParams {
                            page_size: 10,
                            cursor: None,
                            sort_key: ThreadSortKey::CreatedAt,
                            sort_direction: SortDirection::Desc,
                            allowed_sources: vec![SessionSource::Cli],
                            archived: false,
                            search_term: search_term.to_string(),
                        })
                        .await
                        .expect("search selected rollout");
                    assert_eq!(
                        (
                            page.items
                                .iter()
                                .map(|item| (item.thread.thread_id, item.snippet.as_str()))
                                .collect::<Vec<_>>(),
                            page.next_cursor,
                        ),
                        (expected, None),
                        "compressed={compressed}, selected_path={selected_path:?}",
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn search_prefers_the_selected_existing_representation() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    state_db.mark_backfill_complete(None).await.unwrap();
    let uuid = Uuid::new_v4();
    let id = ThreadId::from_string(&uuid.to_string()).unwrap();
    let dir = home.path().join("sessions/2025/01/03");
    let plain = write_session_file_with(
        home.path(),
        dir.clone(),
        "2025-01-03T12-00-00",
        uuid,
        "plain-only",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let temporary = write_session_file_with(
        home.path(),
        dir,
        "2025-01-03T11-00-00",
        uuid,
        "compressed-only",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let contents = std::fs::read(&temporary).unwrap();
    let compressed = plain.with_extension("jsonl.zst");
    std::fs::write(
        &compressed,
        zstd::stream::encode_all(contents.as_slice(), 3).unwrap(),
    )
    .unwrap();
    std::fs::remove_file(temporary).unwrap();
    let store = LocalThreadStore::new(config, Some(state_db.clone()));
    for (selected, present, absent) in [
        (&compressed, "compressed-only", "plain-only"),
        (&plain, "plain-only", "compressed-only"),
    ] {
        let mut metadata = codex_state::ThreadMetadataBuilder::new(
            id,
            selected.clone(),
            Utc::now(),
            SessionSource::Cli,
        )
        .build("test-provider");
        metadata.first_user_message = Some("a user message".into());
        state_db.upsert_thread(&metadata).await.unwrap();
        for (term, expected_count) in [(present, 1), (absent, 0)] {
            let page = store
                .search_threads(SearchThreadsParams {
                    page_size: 10,
                    cursor: None,
                    sort_key: ThreadSortKey::CreatedAt,
                    sort_direction: SortDirection::Desc,
                    allowed_sources: vec![],
                    archived: false,
                    search_term: term.into(),
                })
                .await
                .expect("search selected representation");
            assert_eq!(
                page.items.len(),
                expected_count,
                "selected={selected:?}, term={term}"
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn search_does_not_open_unselected_or_internal_rollouts() {
    use std::io::Read;

    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .unwrap();
    state_db.mark_backfill_complete(None).await.unwrap();
    let uuid = Uuid::new_v4();
    let id = ThreadId::from_string(&uuid.to_string()).unwrap();
    let dir = home.path().join("sessions/2025/01/03");
    let selected = write_session_file_with(
        home.path(),
        dir.clone(),
        "2025-01-03T12-00-00",
        uuid,
        "needle",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let alternate = write_session_file_with(
        home.path(),
        dir.clone(),
        "2025-01-03T11-00-00",
        uuid,
        "needle",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let guardian_id = ThreadId::new();
    let guardian = write_session_file_with(
        home.path(),
        dir.clone(),
        "2025-01-03T12-00-00",
        Uuid::parse_str(&guardian_id.to_string()).unwrap(),
        "needle",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let orphan_id = ThreadId::new();
    let orphan = write_session_file_with(
        home.path(),
        dir,
        "2025-01-03T12-00-00",
        Uuid::parse_str(&orphan_id.to_string()).unwrap(),
        "needle",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    for (thread_id, path, source) in [
        (id, selected.clone(), SessionSource::Cli),
        (
            guardian_id,
            guardian.clone(),
            SessionSource::Internal(InternalSessionSource::Guardian),
        ),
    ] {
        let mut metadata =
            codex_state::ThreadMetadataBuilder::new(thread_id, path, Utc::now(), source)
                .build("test-provider");
        metadata.first_user_message = Some("needle".into());
        state_db.upsert_thread(&metadata).await.unwrap();
    }
    let mut orphan_metadata = codex_state::ThreadMetadataBuilder::new(
        orphan_id,
        orphan.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build("test-provider");
    orphan_metadata.thread_source = Some(ThreadSource::User);
    orphan_metadata.agent_role = Some("worker".into());
    orphan_metadata.first_user_message = Some("needle".into());
    state_db.upsert_thread(&orphan_metadata).await.unwrap();

    let mut selected_watch = watch_file_opens(&selected);
    let mut alternate_watch = watch_file_opens(&alternate);
    let mut guardian_watch = watch_file_opens(&guardian);
    let mut orphan_watch = watch_file_opens(&orphan);
    let store = LocalThreadStore::new(config, Some(state_db));
    let page = store
        .search_threads(SearchThreadsParams {
            page_size: 10,
            cursor: None,
            sort_key: ThreadSortKey::CreatedAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: vec![],
            archived: false,
            search_term: "needle".into(),
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].thread.thread_id, id);

    let mut events = [0; 256];
    assert!(
        selected_watch.read(&mut events).unwrap() > 0,
        "selected file must be watched"
    );
    for watch in [&mut alternate_watch, &mut guardian_watch, &mut orphan_watch] {
        let err = watch
            .read(&mut events)
            .expect_err("unselected file must not be opened");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    }
}

#[cfg(target_os = "linux")]
fn watch_file_opens(path: &std::path::Path) -> std::fs::File {
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    // SAFETY: These flags require no pointer arguments; nonblocking reads reveal no events.
    let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    assert!(
        fd >= 0,
        "inotify_init1: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: The fresh descriptor is owned by this File from this point on.
    let watch = unsafe { std::fs::File::from_raw_fd(fd) };
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: `watch` owns a live descriptor and `path` is NUL-terminated.
    let descriptor =
        unsafe { libc::inotify_add_watch(watch.as_raw_fd(), path.as_ptr(), libc::IN_OPEN) };
    assert!(
        descriptor >= 0,
        "inotify_add_watch: {}",
        std::io::Error::last_os_error()
    );
    watch
}

#[tokio::test]
async fn search_reads_only_top_level_user_rollouts() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    state_db.mark_backfill_complete(None).await.unwrap();
    let parent_uuid = Uuid::new_v4();
    let parent = ThreadId::from_string(&parent_uuid.to_string()).expect("parent ID");
    let spawned = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: parent,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });
    let legacy_guardian = SessionSource::SubAgent(SubAgentSource::Other("guardian".to_string()));
    let guardian = SessionSource::Internal(InternalSessionSource::Guardian);

    let other_parented = ThreadId::from_string(&Uuid::new_v4().to_string()).unwrap();
    let promoted_root = ThreadId::new();
    let closed_child = ThreadId::new();
    let mut candidate_ids = Vec::new();
    let mut user_ids = Vec::new();
    for (thread_id, source, thread_source, attachment, searchable) in [
        (parent, SessionSource::Cli, None, None, true),
        (
            promoted_root,
            SessionSource::Cli,
            Some(ThreadSource::User),
            None,
            true,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            Some(ThreadSource::Feature("automation".into())),
            None,
            true,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            Some(ThreadSource::Feature("voice_chat".into())),
            None,
            true,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            Some(ThreadSource::Feature("realtime_voice".into())),
            None,
            true,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            Some(ThreadSource::User),
            None,
            true,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            None,
            Some(parent),
            false,
        ),
        (
            other_parented,
            SessionSource::Cli,
            Some(ThreadSource::User),
            None,
            false,
        ),
        (
            closed_child,
            spawned.clone(),
            Some(ThreadSource::User),
            None,
            false,
        ),
        (ThreadId::new(), legacy_guardian.clone(), None, None, false),
        (
            ThreadId::new(),
            guardian.clone(),
            Some(ThreadSource::User),
            None,
            false,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            Some(ThreadSource::GuardianReview),
            None,
            false,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            Some(ThreadSource::Subagent),
            None,
            false,
        ),
        (
            ThreadId::new(),
            SessionSource::Cli,
            Some(ThreadSource::Feature("internal".to_string())),
            None,
            false,
        ),
        (
            ThreadId::new(),
            SessionSource::Unknown,
            Some(ThreadSource::User),
            None,
            false,
        ),
    ] {
        let uuid = Uuid::parse_str(&thread_id.to_string()).unwrap();
        let path = write_session_file_with(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-03T12-00-00",
            uuid,
            "searchable needle",
            Some("test-provider"),
            ThreadHistoryMode::Legacy,
        )
        .expect("write rollout");
        let mut metadata =
            codex_state::ThreadMetadataBuilder::new(thread_id, path.clone(), Utc::now(), source)
                .build("test-provider");
        metadata.first_user_message = Some("searchable needle".to_string());
        metadata.thread_source = thread_source;
        metadata.attached_thread_parent_id = attachment;
        state_db
            .upsert_thread(&metadata)
            .await
            .expect("index rollout");
        if attachment.is_none() {
            candidate_ids.push(thread_id);
        }
        if searchable {
            user_ids.push(thread_id);
        }
        // Opening an excluded rollout would fail even when tests run as root.
        if !searchable {
            std::fs::remove_file(&path).expect("remove excluded rollout");
            std::fs::create_dir(&path).expect("replace excluded rollout with directory");
        }
    }

    state_db
        .upsert_thread_spawn_edge(
            parent,
            other_parented,
            codex_state::DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("record spawn edge");
    for thread_id in [promoted_root, closed_child] {
        state_db
            .upsert_thread_spawn_edge(
                parent,
                thread_id,
                codex_state::DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("record closed spawn edge");
    }

    let candidates = state_db
        .list_threads_for_content_search(
            100,
            codex_state::ThreadFilterOptions {
                archived_only: false,
                allowed_sources: &[],
                model_providers: None,
                cwd_filters: None,
                section: None,
                project_id: None,
                anchor: None,
                sort_key: codex_state::SortKey::RecencyAt,
                sort_direction: codex_state::SortDirection::Desc,
                search_term: None,
            },
        )
        .await
        .expect("list candidate metadata");
    for id in candidate_ids {
        assert!(
            candidates.items.iter().any(|item| item.id == id),
            "missing candidate {id}"
        );
    }

    let store = LocalThreadStore::new(config, Some(state_db));
    let page = store
        .search_threads(SearchThreadsParams {
            page_size: 10,
            cursor: None,
            sort_key: ThreadSortKey::RecencyAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: vec![],
            archived: false,
            search_term: "needle".to_string(),
        })
        .await
        .expect("search top-level chat without reading hidden histories");
    let mut actual_ids = page
        .items
        .iter()
        .map(|item| item.thread.thread_id)
        .collect::<Vec<_>>();
    actual_ids.sort_by_key(ToString::to_string);
    user_ids.sort_by_key(ToString::to_string);
    assert_eq!(actual_ids, user_ids);
    assert_eq!(page.next_cursor, None);
}

#[test]
fn search_requires_classifiable_user_metadata() {
    let mut item = codex_state::ThreadMetadataBuilder::new(
        ThreadId::new(),
        "rollout.jsonl".into(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build("test-provider");
    assert!(is_user_chat(&item), "legacy CLI metadata is a user chat");
    item.source.clear();
    assert!(
        !is_user_chat(&item),
        "missing session source is not classifiable"
    );
    item.source = "unrecognized-source".into();
    assert!(
        !is_user_chat(&item),
        "unknown session source is not classifiable"
    );
    item.thread_source = Some(ThreadSource::User);
    assert!(
        !is_user_chat(&item),
        "a user thread label cannot override an unknown session source"
    );
    item.source = "cli".into();
    assert!(is_user_chat(&item));
    item.agent_role = Some("worker".into());
    assert!(
        !is_user_chat(&item),
        "an agent role conflicts with a user source"
    );
    item.agent_role = None;
    item.agent_nickname = Some("Worker".into());
    assert!(!is_user_chat(&item));
    item.agent_nickname = None;
    item.agent_path = Some("/root/worker".into());
    assert!(!is_user_chat(&item));
    item.agent_path = Some("/root".into());
    assert!(is_user_chat(&item));
    item.agent_path = None;
    for source in ["automation", "voice_chat", "realtime_voice"] {
        item.thread_source = Some(ThreadSource::Feature(source.into()));
        assert!(
            is_user_chat(&item),
            "{source} is a supported conversation source"
        );
    }
    item.thread_source = Some(ThreadSource::Feature("unrecognized".into()));
    assert!(!is_user_chat(&item));
    item.thread_source = Some(ThreadSource::MemoryConsolidation);
    assert!(!is_user_chat(&item));
}

#[tokio::test]
async fn search_requires_a_working_state_database() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let params = SearchThreadsParams {
        page_size: 10,
        cursor: None,
        sort_key: ThreadSortKey::RecencyAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: vec![],
        archived: false,
        search_term: "needle".into(),
    };
    let store = LocalThreadStore::new(config.clone(), None);
    let err = store
        .search_threads(params.clone())
        .await
        .expect_err("missing DB must not scan files");
    assert!(err.to_string().contains("requires the state database"));

    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    let uuid = Uuid::new_v4();
    let id = ThreadId::from_string(&uuid.to_string()).unwrap();
    let path = write_session_file_with(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-00-00",
        uuid,
        "needle",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let mut metadata =
        codex_state::ThreadMetadataBuilder::new(id, path, Utc::now(), SessionSource::Cli)
            .build("test-provider");
    metadata.first_user_message = Some("needle".into());
    state_db.upsert_thread(&metadata).await.unwrap();
    let store = LocalThreadStore::new(config.clone(), Some(state_db.clone()));
    let err = store
        .search_threads(params.clone())
        .await
        .expect_err("incomplete index must not be searched");
    assert!(err.to_string().contains("metadata backfill is incomplete"));
    state_db.mark_backfill_complete(None).await.unwrap();
    let stale_path = home.path().join("stale-rollout.jsonl");
    std::fs::create_dir(&stale_path).unwrap();
    let mut stale = codex_state::ThreadMetadataBuilder::new(
        ThreadId::new(),
        stale_path,
        Utc::now(),
        SessionSource::Cli,
    )
    .build("test-provider");
    stale.first_user_message = Some("needle".into());
    state_db.upsert_thread(&stale).await.unwrap();
    let page = store
        .search_threads(params.clone())
        .await
        .expect("complete index can be searched");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].thread.thread_id, id);
    state_db.close().await;
    let store = LocalThreadStore::new(config, Some(state_db));
    let err = store
        .search_threads(params)
        .await
        .expect_err("failed DB query must not scan files");
    assert!(
        err.to_string()
            .contains("failed to query thread/search metadata backfill state")
    );
}

#[tokio::test]
async fn search_rechecks_selected_metadata_after_lifecycle_reservation() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .unwrap();
    state_db.mark_backfill_complete(None).await.unwrap();
    let store = std::sync::Arc::new(LocalThreadStore::new(config, Some(state_db.clone())));
    for change in ["path", "source", "archive", "edge"] {
        let uuid = Uuid::new_v4();
        let id = ThreadId::from_string(&uuid.to_string()).unwrap();
        let dir = home.path().join("sessions/2025/01/03");
        let selected = write_session_file_with(
            home.path(),
            dir.clone(),
            "2025-01-03T12-00-00",
            uuid,
            "needle",
            Some("test-provider"),
            ThreadHistoryMode::Legacy,
        )
        .unwrap();
        let replacement = write_session_file_with(
            home.path(),
            dir,
            "2025-01-03T11-00-00",
            uuid,
            "replacement needle",
            Some("test-provider"),
            ThreadHistoryMode::Legacy,
        )
        .unwrap();
        let mut metadata = codex_state::ThreadMetadataBuilder::new(
            id,
            selected.clone(),
            Utc::now(),
            SessionSource::Cli,
        )
        .build("test-provider");
        metadata.first_user_message = Some("needle".into());
        state_db.upsert_thread(&metadata).await.unwrap();
        let snapshot = state_db.get_thread(id).await.unwrap().unwrap();
        #[cfg(target_os = "linux")]
        let mut selected_watch = watch_file_opens(&selected);

        let exclusive = store.live_writer_locks.lock_lifecycle(id).await;
        let task_store = store.clone();
        let task_db = state_db.clone();
        let search = tokio::spawn(async move {
            search_candidate(
                task_store.as_ref(),
                task_db.as_ref(),
                snapshot,
                false,
                "needle",
            )
            .await
        });
        match change {
            "path" => metadata.rollout_path = replacement,
            "source" => metadata.thread_source = Some(ThreadSource::GuardianReview),
            "archive" => metadata.archived_at = Some(Utc::now()),
            "edge" => state_db
                .upsert_thread_spawn_edge(
                    ThreadId::new(),
                    id,
                    codex_state::DirectionalThreadSpawnEdgeStatus::Open,
                )
                .await
                .unwrap(),
            _ => unreachable!(),
        }
        if change != "edge" {
            state_db.upsert_thread(&metadata).await.unwrap();
        }
        drop(exclusive);
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), search)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            result.is_none(),
            "stale {change} metadata must not be searched"
        );
        #[cfg(target_os = "linux")]
        {
            use std::io::Read;
            let err = selected_watch
                .read(&mut [0; 256])
                .expect_err("stale rollout must not be opened");
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
        }
    }
}

#[tokio::test]
async fn search_paginates_equal_timestamps_in_active_and_archived_rollouts() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    state_db.mark_backfill_complete(None).await.unwrap();
    let timestamp = "2025-01-03T12:00:00Z".parse().unwrap();
    for archived in [false, true] {
        for n in 1..=3 {
            let uuid = Uuid::from_u128(n + if archived { 100 } else { 0 });
            let thread_id = ThreadId::from_string(&uuid.to_string()).unwrap();
            let dir = if archived {
                home.path().join("archived_sessions")
            } else {
                home.path().join("sessions/2025/01/03")
            };
            let path = write_session_file_with(
                home.path(),
                dir,
                "2025-01-03T12-00-00",
                uuid,
                "needle",
                Some("test-provider"),
                ThreadHistoryMode::Legacy,
            )
            .expect("write rollout");
            if n == 2 {
                let contents = std::fs::read(&path).unwrap();
                let compressed = zstd::stream::encode_all(contents.as_slice(), 3).unwrap();
                std::fs::write(path.with_extension("jsonl.zst"), compressed).unwrap();
                std::fs::remove_file(&path).unwrap();
            }
            let mut metadata = codex_state::ThreadMetadataBuilder::new(
                thread_id,
                path,
                timestamp,
                SessionSource::Cli,
            )
            .build("test-provider");
            metadata.first_user_message = Some("needle".into());
            metadata.archived_at = archived.then_some(timestamp);
            state_db
                .upsert_thread(&metadata)
                .await
                .expect("index rollout");
        }
    }
    let store = LocalThreadStore::new(config, Some(state_db));
    for archived in [false, true] {
        for sort_key in [
            ThreadSortKey::CreatedAt,
            ThreadSortKey::UpdatedAt,
            ThreadSortKey::RecencyAt,
        ] {
            for sort_direction in [SortDirection::Asc, SortDirection::Desc] {
                let mut cursor = None;
                let mut ids = Vec::new();
                loop {
                    let page = store
                        .search_threads(SearchThreadsParams {
                            page_size: 1,
                            cursor,
                            sort_key,
                            sort_direction,
                            allowed_sources: vec![],
                            archived,
                            search_term: "needle".into(),
                        })
                        .await
                        .expect("search page");
                    ids.extend(page.items.into_iter().map(|item| item.thread.thread_id));
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                    assert!(ids.len() < 4, "pagination repeated a result");
                }
                let mut expected = (1..=3)
                    .map(|n| {
                        ThreadId::from_string(
                            &Uuid::from_u128(n + if archived { 100 } else { 0 }).to_string(),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                if sort_direction == SortDirection::Desc {
                    expected.reverse();
                }
                assert_eq!(
                    ids, expected,
                    "archived={archived}, sort={sort_key:?}, direction={sort_direction:?}"
                );
            }
        }
    }
}
