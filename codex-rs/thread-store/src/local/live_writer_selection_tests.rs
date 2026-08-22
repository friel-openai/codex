use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use tokio::sync::Notify;

use super::super::LocalThreadStore;
use super::super::goal_supervisor_runtime_repair;
use super::super::test_support::test_config;
use crate::AppendThreadItemsParams;
use crate::ResumeThreadParams;
use crate::ThreadPersistenceMetadata;
use crate::ThreadStore;
use crate::ThreadStoreError;

/// Pauses only the test's thread after resolving its requested rollout and before locking it.
type ResumePause = (Arc<Notify>, Arc<Notify>);
static RESUME_PAUSES: LazyLock<Mutex<HashMap<ThreadId, ResumePause>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) async fn pause_before_history_access(thread_id: ThreadId) {
    let pause = RESUME_PAUSES
        .lock()
        .expect("resume pauses")
        .remove(&thread_id);
    if let Some((entered, release)) = pause {
        entered.notify_one();
        release.notified().await;
    }
}

fn output(text: &str) -> RolloutItem {
    serde_json::from_value(serde_json::json!({
        "type":"response_item",
        "payload":{"type":"function_call_output","call_id":text,"output":text}
    }))
    .expect("model-history item")
}

fn write_rollout(path: &Path, thread_id: ThreadId, text: &str) -> Vec<RolloutItem> {
    let metadata = SessionMeta {
        id: thread_id,
        session_id: thread_id.into(),
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        cwd: path.parent().expect("parent").to_path_buf(),
        history_mode: ThreadHistoryMode::Paginated,
        ..SessionMeta::default()
    };
    let items = vec![
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: metadata,
            git: None,
        }),
        output(text),
    ];
    let mut bytes = Vec::new();
    for (ordinal, item) in items.iter().enumerate() {
        serde_json::to_writer(
            &mut bytes,
            &RolloutLine {
                timestamp: "2025-01-03T12:00:00Z".to_string(),
                ordinal: Some(ordinal as u64),
                item: item.clone(),
            },
        )
        .expect("encode record");
        bytes.push(b'\n');
    }
    std::fs::write(path, bytes).expect("write rollout");
    items
}

#[tokio::test]
async fn resume_rejects_selection_changed_while_old_rollout_is_retained() {
    let home = tempfile::tempdir().expect("home");
    let directory = home.path().join("sessions/2025/01/03");
    std::fs::create_dir_all(&directory).expect("sessions");
    let thread_id = ThreadId::new();
    let first = directory.join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    let second = directory.join(format!(
        "rollout-2025-01-03T12-00-01-{}.jsonl",
        ThreadId::new()
    ));
    let first_history = write_rollout(&first, thread_id, "original history");
    write_rollout(&second, thread_id, "replacement history");
    let first_bytes = std::fs::read(&first).expect("first bytes");
    let second_bytes = std::fs::read(&second).expect("second bytes");
    let config = test_config(home.path());
    let db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state database");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        first.clone(),
        chrono::Utc::now(),
        SessionSource::Cli,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    db.upsert_thread(&metadata.build("test-provider"))
        .await
        .expect("select first");
    let store = LocalThreadStore::new(config, Some(db));
    #[cfg(unix)]
    {
        let alias = home.path().join("session-alias");
        std::os::unix::fs::symlink(&directory, &alias).expect("session directory alias");
        let alias_path = alias.join(first.file_name().expect("rollout filename"));
        let access = goal_supervisor_runtime_repair::repair_selected_history_before_access(
            &store,
            thread_id,
            &alias_path,
            goal_supervisor_runtime_repair::RepairAccess::Recent,
        )
        .await
        .expect("directory alias retains the selected rollout");
        drop(access);
    }
    let persistence = ThreadPersistenceMetadata {
        cwd: Some(home.path().to_path_buf()),
        model_provider: "test-provider".to_string(),
        memory_mode: ThreadMemoryMode::Enabled,
    };
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    RESUME_PAUSES
        .lock()
        .expect("resume pauses")
        .insert(thread_id, (Arc::clone(&entered), Arc::clone(&release)));
    let resume_store = store.clone();
    let params = ResumeThreadParams {
        thread_id,
        rollout_path: Some(first.clone()),
        history: Some(Arc::new(first_history)),
        include_archived: false,
        metadata: persistence.clone(),
    };
    let resume = tokio::spawn(async move { resume_store.resume_thread(params).await });
    entered.notified().await;
    let reservation = store
        .reserve_rollout_writers(&[thread_id])
        .await
        .expect("publication writer");
    let db = store.state_db().await.expect("state database");
    assert!(
        db.replace_rollout_path_if_current(thread_id, &first, &second)
            .await
            .expect("select second")
    );
    drop(reservation);
    release.notify_one();
    assert!(matches!(
        resume.await.expect("join resume"),
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(!store.live_recorders.lock().await.contains_key(&thread_id));
    assert_eq!(std::fs::read(&first).expect("retained first"), first_bytes);
    assert_eq!(
        std::fs::read(&second).expect("retained second"),
        second_bytes
    );
    assert_eq!(
        db.get_thread(thread_id)
            .await
            .expect("metadata")
            .expect("thread")
            .rollout_path,
        second
    );
    assert!(matches!(
        goal_supervisor_runtime_repair::repair_selected_history_before_access(
            &store,
            thread_id,
            &first,
            goal_supervisor_runtime_repair::RepairAccess::Recent,
        )
        .await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    let exact = store
        .read_thread_by_rollout_path(
            first.clone(),
            /*include_archived*/ false,
            /*include_history*/ true,
        )
        .await
        .expect("explicit retained history");
    assert!(
        serde_json::to_string(&exact.history.expect("history").items)
            .expect("history JSON")
            .contains("original history")
    );

    store
        .resume_thread(ResumeThreadParams {
            thread_id,
            rollout_path: None,
            history: None,
            include_archived: false,
            metadata: persistence,
        })
        .await
        .expect("fresh resume");
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![output("new append")],
        })
        .await
        .expect("append to second");
    store.flush_thread(thread_id).await.expect("flush second");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown second");
    assert_eq!(std::fs::read(&first).expect("unchanged first"), first_bytes);
    assert!(
        std::fs::read_to_string(&second)
            .expect("second history")
            .contains("new append")
    );
    assert_eq!(
        db.get_thread(thread_id)
            .await
            .expect("metadata")
            .expect("thread")
            .rollout_path,
        second
    );
}
