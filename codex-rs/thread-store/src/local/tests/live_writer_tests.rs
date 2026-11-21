//! Shutdown releases writer ownership even when metadata synchronization fails.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn shutdown_releases_writer_after_metadata_sync_failure() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state db should initialize");
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let thread_id = ThreadId::default();
    store
        .create_thread(create_thread_params(thread_id))
        .await
        .expect("create live thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist live thread");

    let locks = Arc::new(WriterLockCoordinator::new(home.path()));
    assert!(
        matches!(locks.acquire(thread_id), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    // Closing the metadata database leaves rollout IO intact but makes synchronization fail.
    runtime.close().await;

    let error = store
        .shutdown_thread(thread_id)
        .await
        .expect_err("shutdown must return the metadata synchronization error");
    assert!(
        matches!(error, ThreadStoreError::Internal { message } if message.starts_with(&format!("failed to read thread metadata for {thread_id}:")))
    );
    assert!(!store.live_recorders.lock().await.contains_key(&thread_id));
    let _writer_lock = locks
        .acquire(thread_id)
        .expect("shutdown must release cross-process writer ownership before returning");
}
