use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::InternalSessionSource;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSource;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use uuid::Uuid;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::super::test_support::write_session_file_with;
use super::super::test_support::write_session_file_with_fork;
use super::ThreadSearchItem;
use super::cursor_from_thread_search_item;
use super::is_user_chat;
use super::search_candidate;
use crate::ListThreadsParams;
use crate::SearchThreadsParams;
use crate::SortDirection;
use crate::ThreadRelationFilter;
use crate::ThreadSearchSourceKind;
use crate::ThreadSortKey;
use crate::ThreadStore;

async fn write_named_rollout(home: &Path, id: Uuid, attached_to: Option<Uuid>) -> PathBuf {
    let path = write_session_file_with_fork(
        home,
        home.join("sessions/2025/01/03"),
        "2025-01-03T12-00-00",
        id,
        "searchable message",
        Some("test-provider"),
        attached_to,
        attached_to.is_some(),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    codex_rollout::append_thread_name(home, ThreadId::from_u128(id.as_u128()), "matching title")
        .await
        .unwrap();
    path
}

fn inclusive_title_search() -> ListThreadsParams {
    ListThreadsParams {
        included_thread_ids: None,
        page_size: 10,
        cursor: None,
        sort_key: ThreadSortKey::CreatedAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: vec![SessionSource::Cli],
        model_providers: None,
        cwd_filters: None,
        section: None,
        project_id: None,
        archived: false,
        search_term: Some("matching title".into()),
        relation_filter: Some(ThreadRelationFilter::IncludeAttached),
        use_state_db_only: false,
    }
}

#[tokio::test]
async fn title_search_returns_attached_parent_without_state_db() {
    let home = TempDir::new().unwrap();
    let store = LocalThreadStore::new(test_config(home.path()), None);
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    write_named_rollout(home.path(), child, Some(parent)).await;
    let result = store.list_threads(inclusive_title_search()).await.unwrap();
    assert_eq!(
        result
            .items
            .into_iter()
            .map(|item| (item.thread_id, item.attached_to_thread_id))
            .collect::<Vec<_>>(),
        vec![(
            ThreadId::from_u128(child.as_u128()),
            Some(ThreadId::from_u128(parent.as_u128()))
        )]
    );
}

#[tokio::test]
async fn inclusive_title_search_finds_unindexed_matches_when_filesystem_scanning_is_allowed() {
    let home = TempDir::new().unwrap();
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .unwrap();
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let standalone = Uuid::new_v4();
    let parent_path = write_named_rollout(home.path(), parent, None).await;
    write_named_rollout(home.path(), standalone, None).await;
    write_named_rollout(home.path(), child, Some(parent)).await;
    let parent_id = ThreadId::from_u128(parent.as_u128());
    let child_id = ThreadId::from_u128(child.as_u128());
    let mut row = codex_state::ThreadMetadataBuilder::new(
        parent_id,
        parent_path,
        Utc::now(),
        SessionSource::Cli,
    )
    .build("test-provider");
    row.first_user_message = Some("searchable message".into());
    row.title = "matching title".into();
    state_db.upsert_thread(&row).await.unwrap();
    let store = LocalThreadStore::new(config, Some(state_db));
    let all = HashMap::from([
        (parent_id, None),
        (child_id, Some(parent_id)),
        (ThreadId::from_u128(standalone.as_u128()), None),
    ]);
    for (use_state_db_only, expected) in [
        (true, HashMap::from([(parent_id, None)])),
        (false, all.clone()),
        (true, all),
    ] {
        let page = store
            .list_threads(ListThreadsParams {
                use_state_db_only,
                ..inclusive_title_search()
            })
            .await
            .unwrap();
        assert_eq!(
            page.items
                .into_iter()
                .map(|item| (item.thread_id, item.attached_to_thread_id))
                .collect::<HashMap<_, _>>(),
            expected
        );
    }
}

#[tokio::test]
async fn content_search_excludes_attached_threads_even_when_requested() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .unwrap();
    state.mark_backfill_complete(None).await.unwrap();
    let parent = Uuid::new_v4();
    let uuid = Uuid::new_v4();
    let id = ThreadId::from_u128(uuid.as_u128());
    let path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T12-00-00",
        uuid,
        "private needle",
        Some("test-provider"),
        Some(parent),
        true,
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let content = std::fs::read_to_string(&path).unwrap();
    let (header, rest) = content.split_once('\n').unwrap();
    let mut header: serde_json::Value = serde_json::from_str(header).unwrap();
    header["payload"]["account_scoped"] = true.into();
    header["payload"]["creator_user_id"] = "owner".into();
    std::fs::write(&path, format!("{}\n{}", header, rest)).unwrap();
    let mut row = codex_state::ThreadMetadataBuilder::new(id, path, Utc::now(), SessionSource::Cli)
        .build("test-provider");
    row.attached_thread_parent_id = Some(ThreadId::from_u128(parent.as_u128()));
    row.first_user_message = Some("private needle".into());
    state.upsert_thread(&row).await.unwrap();
    let store = LocalThreadStore::new(config, Some(state));
    let mut params = SearchThreadsParams {
        include_attached_threads: false,
        account: Some(crate::LocalHistoryAccount {
            user_id: Some("owner".into()),
            account_id: None,
        }),
        page_size: 10,
        cursor: None,
        sort_key: ThreadSortKey::CreatedAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: vec![SessionSource::Cli],
        source_kinds: None,
        archived: false,
        search_term: "needle".into(),
    };
    assert!(
        store
            .search_threads(params.clone())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    params.include_attached_threads = true;
    assert!(
        store
            .search_threads(params.clone())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    params.account.as_mut().unwrap().user_id = Some("other".into());
    assert!(store.search_threads(params).await.unwrap().items.is_empty());
}

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

                let selected_is_present = selected_path.is_file();
                for (search_term, expected) in [
                    (
                        "needle",
                        if selected_is_present {
                            vec![(thread_id, "needle in selected rollout")]
                        } else {
                            Vec::new()
                        },
                    ),
                    ("obsolete", Vec::new()),
                ] {
                    let page = store
                        .search_threads(SearchThreadsParams {
                            include_attached_threads: false,
                            account: None,
                            page_size: 10,
                            cursor: None,
                            sort_key: ThreadSortKey::CreatedAt,
                            sort_direction: SortDirection::Desc,
                            allowed_sources: vec![SessionSource::Cli],
                            source_kinds: None,
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
                    include_attached_threads: false,
                    account: None,
                    page_size: 10,
                    cursor: None,
                    sort_key: ThreadSortKey::CreatedAt,
                    sort_direction: SortDirection::Desc,
                    allowed_sources: vec![],
                    source_kinds: None,
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
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(4);
    let page = store
        .search_threads_observed(
            SearchThreadsParams {
                include_attached_threads: false,
                account: None,
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                sort_direction: SortDirection::Desc,
                allowed_sources: vec![],
                source_kinds: None,
                archived: false,
                search_term: "needle".into(),
            },
            crate::ThreadSearchObserver {
                progress: Some(progress_tx),
                cancelled: Box::pin(std::future::pending()),
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].thread.thread_id, id);
    let progress = progress_rx
        .recv()
        .await
        .expect("selected match is reported");
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].thread.thread_id, id);
    assert!(progress_rx.recv().await.is_none());

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
                included_thread_ids: None,
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
            /*include_attached_threads*/ true,
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
            include_attached_threads: false,
            account: None,
            page_size: 10,
            cursor: None,
            sort_key: ThreadSortKey::RecencyAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: vec![],
            source_kinds: None,
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
        include_attached_threads: false,
        account: None,
        page_size: 10,
        cursor: None,
        sort_key: ThreadSortKey::RecencyAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: vec![],
        source_kinds: None,
        archived: false,
        search_term: "needle".into(),
    };
    let store = LocalThreadStore::new(config.clone(), None);
    let err = store
        .search_threads(params.clone())
        .await
        .expect_err("missing DB must not scan files");
    assert!(err.to_string().contains("requires the state database"));
    let mut attached_params = params.clone();
    attached_params.include_attached_threads = true;
    let err = store
        .search_threads(attached_params)
        .await
        .expect_err("attached content search also requires a state database");
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
                            include_attached_threads: false,
                            account: None,
                            page_size: 1,
                            cursor,
                            sort_key,
                            sort_direction,
                            allowed_sources: vec![],
                            source_kinds: None,
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

#[tokio::test]
async fn source_kind_filter_applies_before_search_pagination() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    state_db.mark_backfill_complete(None).await.unwrap();
    let mut cli_ids = Vec::new();
    for (age, source) in [
        (0, SessionSource::Cli),
        (1, SessionSource::VSCode),
        (2, SessionSource::Cli),
    ] {
        let uuid = Uuid::new_v4();
        let id = ThreadId::from_u128(uuid.as_u128());
        let path = write_session_file_with(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-03T12-00-00",
            uuid,
            "needle message",
            Some("test-provider"),
            ThreadHistoryMode::Legacy,
        )
        .expect("write rollout");
        let mut metadata = codex_state::ThreadMetadataBuilder::new(
            id,
            path,
            Utc::now() - chrono::TimeDelta::days(age),
            source.clone(),
        )
        .build("test-provider");
        metadata.first_user_message = Some("needle message".to_string());
        state_db
            .upsert_thread(&metadata)
            .await
            .expect("index rollout");
        if matches!(source, SessionSource::Cli) {
            cli_ids.push(id);
        }
    }

    let store = LocalThreadStore::new(config, Some(state_db));
    let mut params = SearchThreadsParams {
        include_attached_threads: false,
        account: None,
        page_size: 1,
        cursor: None,
        sort_key: ThreadSortKey::CreatedAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: Vec::new(),
        source_kinds: Some(vec![ThreadSearchSourceKind::Cli]),
        archived: false,
        search_term: "needle".to_string(),
    };
    let first = store
        .search_threads(params.clone())
        .await
        .expect("first page");
    params.cursor = first.next_cursor.clone();
    let second = store.search_threads(params).await.expect("second page");
    assert_eq!(
        (
            first
                .items
                .into_iter()
                .map(|item| item.thread.thread_id)
                .collect::<Vec<_>>(),
            first.next_cursor.is_some(),
            second
                .items
                .into_iter()
                .map(|item| item.thread.thread_id)
                .collect::<Vec<_>>(),
            second.next_cursor,
        ),
        (vec![cli_ids[0]], true, vec![cli_ids[1]], None),
    );
}

#[tokio::test]
async fn searches_are_independent_across_catalog_refills() -> Result<(), Box<dyn std::error::Error>>
{
    let home = TempDir::new()?;
    let config = test_config(home.path());
    let db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    db.mark_backfill_complete(None).await?;
    let store = LocalThreadStore::new(config, Some(db.clone()));
    // At this page size the catalog returns 256 rows. Put a hit on either side.
    let created_at = Utc::now();
    for id in 1..=258 {
        let message = match id {
            258 => "first needle",
            257 => "other query",
            1 => "last NEEDLE",
            _ => "ordinary conversation",
        };
        let path = write_session_file_with(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-03T12-00-00",
            Uuid::from_u128(id),
            message,
            Some("test-provider"),
            ThreadHistoryMode::Legacy,
        )?;
        let mut row = codex_state::ThreadMetadataBuilder::new(
            ThreadId::from_u128(id),
            path,
            created_at,
            SessionSource::Cli,
        )
        .build("test-provider");
        row.first_user_message = Some(message.to_owned());
        db.upsert_thread(&row).await?;
    }
    let search = async |query: &str| {
        let page = store
            .search_threads(SearchThreadsParams {
                include_attached_threads: false,
                account: None,
                page_size: 2,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                sort_direction: SortDirection::Desc,
                allowed_sources: vec![SessionSource::Cli],
                source_kinds: None,
                archived: false,
                search_term: query.to_owned(),
            })
            .await?;
        Ok::<_, crate::ThreadStoreError>((
            page.items
                .into_iter()
                .map(|item| (item.thread.thread_id, item.snippet))
                .collect::<Vec<_>>(),
            page.next_cursor,
        ))
    };
    let (many, one, none) = tokio::try_join!(search("needle"), search("other"), search("absent"))?;
    assert_eq!(
        (many, one, none),
        (
            (
                vec![
                    (
                        ThreadId::from_u128(/*value*/ 258),
                        "first needle".to_owned()
                    ),
                    (ThreadId::from_u128(/*value*/ 1), "last NEEDLE".to_owned()),
                ],
                None,
            ),
            (
                vec![(ThreadId::from_u128(/*value*/ 257), "other query".to_owned())],
                None
            ),
            (vec![], None),
        )
    );
    Ok(())
}

#[tokio::test]
async fn streaming_search_does_not_read_a_catalog_path_outside_its_home() {
    let home = TempDir::new().unwrap();
    let other = TempDir::new().unwrap();
    std::fs::create_dir_all(home.path().join("sessions")).unwrap();
    let config = test_config(home.path());
    let db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .unwrap();
    db.mark_backfill_complete(None).await.unwrap();
    let uuid = Uuid::new_v4();
    let path = write_session_file_with(
        other.path(),
        other.path().join("sessions/2025/01/03"),
        "2025-01-03T12-00-00",
        uuid,
        "private needle",
        Some("test-provider"),
        ThreadHistoryMode::Legacy,
    )
    .unwrap();
    let metadata = codex_state::ThreadMetadataBuilder::new(
        ThreadId::from_u128(uuid.as_u128()),
        path,
        Utc::now(),
        SessionSource::Cli,
    )
    .build("test-provider");
    db.upsert_thread(&metadata).await.unwrap();
    let store = LocalThreadStore::new(config, Some(db));
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let page = store
        .search_threads_observed(
            SearchThreadsParams {
                include_attached_threads: false,
                account: None,
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                sort_direction: SortDirection::Desc,
                allowed_sources: vec![SessionSource::Cli],
                source_kinds: None,
                archived: false,
                search_term: "needle".into(),
            },
            crate::ThreadSearchObserver {
                progress: Some(tx),
                cancelled: Box::pin(std::future::pending()),
            },
        )
        .await
        .unwrap();
    assert!(page.items.is_empty());
    assert!(rx.recv().await.is_none());
}
