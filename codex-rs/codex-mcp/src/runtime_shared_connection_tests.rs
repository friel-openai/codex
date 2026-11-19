use super::*;
use crate::connection_manager::tests::create_ready_async_managed_client;
use crate::connection_pool::ConnectionSharing;
use crate::connection_pool::ConnectionSlot;
use crate::connection_pool::McpConnectionPool;
use crate::connection_pool::tests as fixture;
use crate::request_router::McpConnectionRequestRouter;
use futures::FutureExt;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

fn publication_pause(runtime: &McpRuntime, submit_id: &str) -> Arc<RuntimePublicationPause> {
    let pause = Arc::new(RuntimePublicationPause {
        arrived: Notify::new(),
        release: Notify::new(),
    });
    runtime
        .publication_pauses
        .lock()
        .unwrap()
        .insert(submit_id.to_owned(), Arc::clone(&pause));
    pause
}

fn runtime_input(
    home: &std::path::Path,
    pool: McpConnectionPool,
    submit_id: &str,
    context: &McpRuntimeContext,
) -> McpRuntimeInput {
    let executable = std::env::var("CARGO_BIN_EXE_test_stdio_server")
        .expect("the test_stdio_server binary must be built and bound by the test runner");
    let server_config: codex_config::McpServerConfig = serde_json::from_value(serde_json::json!({
        "command": executable,
        "env": {
            "MCP_TEST_INITIALIZE_BARRIER_FILE": home.join("allow-initialize"),
            "MCP_TEST_DYNAMIC_SERVER_METADATA": "1"
        },
        "startup_timeout_sec": 30,
    }))
    .unwrap();
    let mut catalog = crate::ResolvedMcpCatalog::builder();
    catalog.register(crate::McpServerRegistration::from_config(
        "test_server".to_owned(),
        server_config.clone(),
    ));
    let mut config = crate::mcp::tests::test_mcp_config(home.to_path_buf());
    config.mcp_server_catalog = catalog.build();
    McpRuntimeInput {
        network_policy_metadata: None,
        runtime_auth: None,
        startup_policy: McpStartupPolicy::Eager,
        config: Arc::new(config),
        plugins_available: false,
        ready_selected_capability_roots: Vec::new(),
        mcp_servers: HashMap::from([(
            "test_server".to_owned(),
            crate::server::EffectiveMcpServer::from_host_config(server_config),
        )]),
        submit_id: submit_id.to_owned(),
        tx_event: None,
        startup_cancellation_token: CancellationToken::new(),
        connection_pool: pool.clone(),
        connection_pool_mode: McpConnectionPoolMode::Reuse,
        runtime_context: context.clone().with_connection_pool(pool),
        codex_apps_tools_cache: ConnectorRuntimeManager::default(),
        tool_catalog_cache: McpToolCatalogCache::default(),
        codex_apps_tools_cache_key: ConnectorRuntimeContextKey::personal(None, None),
        client_mcp_extensions: ClientMcpExtensions::default(),
        auth: None,
        auth_manager: None,
        elicitation_reviewer: None,
        elicitation_lifecycle: None,
    }
}

async fn wait_arrived(pause: &RuntimePublicationPause) {
    tokio::time::timeout(Duration::from_secs(10), pause.arrived.notified())
        .await
        .expect("candidate reached publication");
}

async fn publish_bounded(runtime: &McpRuntime, input: McpRuntimeInput, reuse: bool) -> bool {
    tokio::time::timeout(Duration::from_secs(20), runtime.publish(input, reuse))
        .await
        .expect("runtime publication timed out")
}

async fn shutdown_bounded(runtime: &McpRuntime) {
    tokio::time::timeout(Duration::from_secs(20), runtime.shutdown())
        .await
        .expect("runtime shutdown timed out");
}

fn process_id(binding: &crate::McpBinding) -> u32 {
    let echo = binding
        .tools()
        .iter()
        .find(|tool| tool.server_name == "test_server" && tool.tool.name == "echo")
        .expect("echo tool from the test server");
    echo.tool
        .description
        .as_deref()
        .expect("test process description")
        .strip_prefix("Echo from rmcp-test-process-")
        .and_then(|value| value.strip_suffix('.'))
        .expect("exact test process description")
        .parse()
        .expect("numeric test process ID")
}

#[tokio::test]
async fn concurrent_first_publications_converge_within_root_and_isolate_other_roots() {
    let home = tempfile::tempdir().unwrap();
    let pool = McpConnectionPool::default();
    let context = fixture::context();
    let first = Arc::new(McpRuntime::empty(true));
    let second = Arc::new(McpRuntime::empty(true));
    let first_pause = publication_pause(&first, "first");
    let second_pause = publication_pause(&second, "second");
    let first_task = {
        let runtime = Arc::clone(&first);
        let input = runtime_input(home.path(), pool.clone(), "first", &context);
        tokio::spawn(async move { runtime.publish(input, true).await })
    };
    let second_task = {
        let runtime = Arc::clone(&second);
        let input = runtime_input(home.path(), pool.clone(), "second", &context);
        tokio::spawn(async move { runtime.publish(input, true).await })
    };
    wait_arrived(&first_pause).await;
    wait_arrived(&second_pause).await;
    first_pause.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), first_task)
            .await
            .unwrap()
            .unwrap()
    );
    second_pause.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), second_task)
            .await
            .unwrap()
            .unwrap()
    );
    let first_set = Arc::clone(&first.current.load_full().connections);
    let second_set = Arc::clone(&second.current.load_full().connections);
    assert!(first_set.shares_server_connection(&second_set, "test_server"));

    let other = McpRuntime::empty(true);
    assert!(
        publish_bounded(
            &other,
            runtime_input(home.path(), McpConnectionPool::default(), "other", &context),
            true
        )
        .await
    );
    let other_set = Arc::clone(&other.current.load_full().connections);
    assert!(!first_set.shares_server_connection(&other_set, "test_server"));
    std::fs::write(home.path().join("allow-initialize"), b"ready").unwrap();
    let first_binding = tokio::time::timeout(
        Duration::from_secs(20),
        first.current_binding_for_call("test_server"),
    )
    .await
    .unwrap()
    .expect("first binding");
    let second_binding = tokio::time::timeout(
        Duration::from_secs(20),
        second.current_binding_for_call("test_server"),
    )
    .await
    .unwrap()
    .expect("second binding");
    let other_binding = tokio::time::timeout(
        Duration::from_secs(20),
        other.current_binding_for_call("test_server"),
    )
    .await
    .unwrap()
    .expect("other-root binding");
    assert_eq!(process_id(&first_binding), process_id(&second_binding));
    assert_ne!(process_id(&first_binding), process_id(&other_binding));
    drop((first_binding, second_binding, other_binding));
    shutdown_bounded(&first).await;
    shutdown_bounded(&second).await;
    shutdown_bounded(&other).await;
}

#[tokio::test]
async fn superseded_and_cancelled_publications_never_replace_the_current_runtime() {
    let home = tempfile::tempdir().unwrap();
    let runtime = Arc::new(McpRuntime::empty(true));
    let pool = McpConnectionPool::default();
    let context = fixture::context();
    let pause = publication_pause(&runtime, "superseded");
    let task = {
        let runtime = Arc::clone(&runtime);
        let input = runtime_input(home.path(), pool.clone(), "superseded", &context);
        tokio::spawn(async move { runtime.publish(input, true).await })
    };
    wait_arrived(&pause).await;
    assert!(
        publish_bounded(
            &runtime,
            runtime_input(home.path(), pool.clone(), "winner", &context),
            true
        )
        .await
    );
    let winner = runtime.current.load_full();
    pause.release.notify_one();
    assert!(
        !tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
    );
    assert!(Arc::ptr_eq(&winner, &runtime.current.load_full()));

    let pause = publication_pause(&runtime, "cancelled");
    let input = runtime_input(home.path(), pool, "cancelled", &context);
    let cancellation = input.startup_cancellation_token.clone();
    let task = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move { runtime.publish(input, true).await })
    };
    wait_arrived(&pause).await;
    cancellation.cancel();
    pause.release.notify_one();
    assert!(
        !tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
    );
    assert!(Arc::ptr_eq(&winner, &runtime.current.load_full()));
    shutdown_bounded(&runtime).await;
}

#[tokio::test]
async fn replacement_keeps_an_existing_binding_usable_until_its_call_finishes() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(home.path().join("allow-initialize"), b"ready").unwrap();
    let pool = McpConnectionPool::default();
    let context = fixture::context();
    let runtime = McpRuntime::empty(true);
    assert!(
        publish_bounded(
            &runtime,
            runtime_input(home.path(), pool.clone(), "old", &context),
            true
        )
        .await
    );
    let binding = tokio::time::timeout(
        Duration::from_secs(20),
        runtime.current_binding_for_call("test_server"),
    )
    .await
    .unwrap()
    .expect("old binding is ready");
    let old_call = binding
        .prepare_call("test_server", "echo")
        .expect("old echo tool");
    let old_pid = process_id(&binding);
    let mut replacement = runtime_input(home.path(), pool, "new", &context);
    replacement.connection_pool_mode = McpConnectionPoolMode::Replace;
    assert!(publish_bounded(&runtime, replacement, false).await);
    let old_result = old_call
        .call(
            Some(serde_json::json!({"message": "old generation"})),
            None,
            Some(Duration::from_secs(10)),
        )
        .await
        .expect("captured old generation remains usable");
    assert_ne!(old_result.is_error, Some(true));
    assert_eq!(
        old_result
            .structured_content
            .as_ref()
            .and_then(|value| value["echo"].as_str()),
        Some(format!("rmcp-test-process-{old_pid}").as_str())
    );
    let new_binding = tokio::time::timeout(
        Duration::from_secs(20),
        runtime.current_binding_for_call("test_server"),
    )
    .await
    .unwrap()
    .expect("new binding is ready");
    assert_ne!(process_id(&binding), process_id(&new_binding));
    let new_pid = process_id(&new_binding);
    let new_result = new_binding
        .prepare_call("test_server", "echo")
        .unwrap()
        .call(
            Some(serde_json::json!({"message": "new generation"})),
            None,
            Some(Duration::from_secs(10)),
        )
        .await
        .expect("new generation is usable");
    assert_ne!(new_result.is_error, Some(true));
    assert_eq!(
        new_result
            .structured_content
            .as_ref()
            .and_then(|value| value["echo"].as_str()),
        Some(format!("rmcp-test-process-{new_pid}").as_str())
    );
    drop(old_call);
    drop(binding);
    shutdown_bounded(&runtime).await;
}

#[tokio::test]
async fn forced_refresh_replaces_pending_connection_without_interrupting_other_chats() {
    let pool = McpConnectionPool::default();
    let key = fixture::key(&fixture::context(), &fixture::config());
    let request_router = McpConnectionRequestRouter::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let launch = Arc::new(fixture::startup(
        ConnectionSharing::Negotiated,
        Arc::clone(&starts),
        Arc::clone(&started),
        Arc::clone(&release),
        fixture::TestServer::default(),
    ));
    let slot = Arc::new(ConnectionSlot::default());
    let first = {
        let pool = pool.clone();
        let key = key.clone();
        let request_router = request_router.clone();
        let slot = Arc::clone(&slot);
        let launch = Arc::clone(&launch);
        async move {
            pool.connect(
                slot,
                key,
                request_router,
                Arc::default(),
                /*tool_catalog_cache_context*/ None,
                CancellationToken::new(),
                &*launch,
            )
            .await
        }
        .boxed()
        .shared()
    };
    let mut poll_first = first.clone();
    assert!(futures::poll!(&mut poll_first).is_pending());
    let other = pool.connect(
        Arc::default(),
        key.clone(),
        request_router.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        CancellationToken::new(),
        &*launch,
    );
    tokio::pin!(other);
    assert!(futures::poll!(&mut other).is_pending());
    started.notified().await;
    let mut owner = create_ready_async_managed_client(Vec::new()).await;
    owner.client = first;
    owner.connection_slot = Some(slot);
    let mut connections = McpConnectionSet::empty(/*prefix_mcp_tool_names*/ true);
    connections.insert_test_client("test_server", owner);
    let connections = Arc::new(connections);
    let config = Arc::new(crate::mcp::tests::test_mcp_config(std::env::temp_dir()));
    let runtime = McpRuntime::empty(/*prefix_mcp_tool_names*/ true);
    runtime.current.store(Arc::new(PublishedMcpRuntime {
        connections: Arc::clone(&connections),
        config: Some(Arc::clone(&config)),
        auth: None,
        auth_manager: None,
        auth_token: None,
        auth_owner: None,
        auth_generation: Arc::new(()),
        plugins_available: false,
        ready_selected_capability_roots: Vec::new(),
        environment_selections: Arc::default(),
        ready_environments: HashMap::new(),
        shutting_down: AtomicBool::new(false),
        cached_binding: Mutex::new(None),
    }));
    let input = McpRuntimeInput {
        network_policy_metadata: None,
        runtime_auth: None,
        startup_policy: McpStartupPolicy::Eager,
        connection_pool: pool.clone(),
        connection_pool_mode: McpConnectionPoolMode::Reuse,
        config,
        plugins_available: false,
        ready_selected_capability_roots: Vec::new(),
        mcp_servers: HashMap::new(),
        submit_id: "refresh".to_string(),
        tx_event: None,
        startup_cancellation_token: CancellationToken::new(),
        runtime_context: fixture::context(),
        codex_apps_tools_cache: ConnectorRuntimeManager::default(),
        tool_catalog_cache: McpToolCatalogCache::default(),
        codex_apps_tools_cache_key: ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        client_mcp_extensions: ClientMcpExtensions::default(),
        auth: None,
        auth_manager: None,
        elicitation_reviewer: None,
        elicitation_lifecycle: None,
    };
    let binding = runtime.current_binding_for_call("test_server");
    tokio::pin!(binding);
    assert!(futures::poll!(&mut binding).is_pending());
    runtime.reconnect_on_next_refresh();
    runtime.replace(input).await;
    let _replacement = tokio::time::timeout(
        Duration::from_secs(1),
        pool.connect(
            Arc::default(),
            key.clone(),
            request_router.clone(),
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &*launch,
        ),
    )
    .await
    .expect("forced refresh must not wait for the old startup")
    .unwrap();
    release.notify_one();
    let other = other.await.unwrap();
    other
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
            /*lease*/ None,
        )
        .await
        .unwrap();

    // A second refresh of the old owner must not evict the newer replacement.
    connections.retire_shared_connections();
    let _joined = pool
        .connect(
            Arc::default(),
            key,
            request_router,
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &*launch,
        )
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    let binding = binding.await.expect("binding from the old publication");
    runtime.shutdown().await;
    assert!(
        binding
            .list_resources("test_server", /*params*/ None)
            .await
            .is_err()
    );
    other
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
            /*lease*/ None,
        )
        .await
        .unwrap();
    let transport = Arc::clone(&other.client);
    let mut other_owner = create_ready_async_managed_client(Vec::new()).await;
    other_owner.client = futures::future::ready(Ok(other)).boxed().shared();
    other_owner.shutdown().await;
    assert!(transport.is_closed().await);
    drop(binding);
}
