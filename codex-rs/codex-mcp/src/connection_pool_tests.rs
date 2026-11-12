//! Tests for root-family ownership, routing, and negotiated MCP transports.

use super::*;
use crate::binding::PreparedMcpCall;
use crate::binding_clients::McpBindingClients;
use crate::connection_manager::McpConnectionSet;
use crate::connection_manager::tests::create_ready_async_managed_client;
use crate::connection_manager::tests::create_test_tool;
use crate::connection_manager::tests::reusable_server_identity;
use crate::elicitation::ElicitationRequestManager;
use crate::elicitation::ElicitationRequestRouter;
use crate::rmcp_client::CodexAppsStartupReconnect;
use crate::runtime::McpRuntimeContext;
use crate::server::EffectiveMcpServer;
use crate::server::McpCredentialPolicy;
use crate::server::McpServerMetadata;
use codex_config::McpServerAuth;
use codex_config::McpServerConfig;
use codex_config::McpServerTransportConfig;
use codex_config::types::AuthKeyringBackendKind;
use codex_config::types::OAuthCredentialsStoreMode;
use codex_exec_server::EnvironmentManager;
use codex_exec_server_test_support::environment_manager_without_environments;
use codex_login::CodexAuth;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::mcp::McpServerInfo;
use codex_protocol::mcp::OPENAI_FORM_EXTENSION_ID;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_rmcp_client::Elicitation;
use codex_rmcp_client::InProcessTransportFactory;
use codex_rmcp_client::McpOAuthRefreshMode;
use codex_rmcp_client::RmcpClient;
use futures::future::BoxFuture;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use pretty_assertions::assert_eq;
use rmcp::ServerHandler;
use rmcp::ServiceExt;
use rmcp::model::ClientCapabilities;
use rmcp::model::ElicitationCapability;
use rmcp::model::FormElicitationCapability;
use rmcp::model::Implementation;
use rmcp::model::InitializeRequestParams;
use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::ListToolsResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ProtocolVersion;
use rmcp::model::RequestId;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerConfig;
use rmcp::model::Tool;
use rmcp::service::RequestContext;
use rmcp::service::RoleServer;
use serde_json::json;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::Barrier;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::DuplexStream;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing_subscriber::prelude::*;

#[derive(Clone)]
pub(crate) struct TestServer {
    tools: Arc<Mutex<Vec<Tool>>>,
    block_listing: Arc<AtomicBool>,
    block_resource_listing: Arc<AtomicBool>,
    started_listings: Arc<Semaphore>,
    cancelled_listings: Arc<Semaphore>,
}

impl Default for TestServer {
    fn default() -> Self {
        Self {
            tools: Arc::new(Mutex::new(vec![
                create_test_tool("test_server", "test").tool,
            ])),
            block_listing: Arc::default(),
            block_resource_listing: Arc::default(),
            started_listings: Arc::new(Semaphore::new(/*permits*/ 0)),
            cancelled_listings: Arc::new(Semaphore::new(/*permits*/ 0)),
        }
    }
}

impl ServerHandler for TestServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
    }

    async fn call_tool(
        &self,
        _request: rmcp::model::CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, rmcp::ErrorData> {
        Ok(rmcp::model::CallToolResult::success(Vec::new()).into())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        if self.block_listing.load(Ordering::Acquire) {
            context.ct.cancelled().await;
            self.cancelled_listings.add_permits(1);
            return Err(rmcp::ErrorData::internal_error(
                "listing cancelled",
                /*data*/ None,
            ));
        }
        Ok(ListToolsResult::with_all_items(
            self.tools.lock().unwrap().clone(),
        ))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::ErrorData> {
        if self.block_listing.load(Ordering::Acquire)
            || self.block_resource_listing.load(Ordering::Acquire)
        {
            self.started_listings.add_permits(1);
            context.ct.cancelled().await;
            self.cancelled_listings.add_permits(1);
        }
        Ok(ListResourcesResult::with_all_items(Vec::new()))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, rmcp::ErrorData> {
        if self.block_listing.load(Ordering::Acquire) {
            self.started_listings.add_permits(1);
            context.ct.cancelled().await;
            self.cancelled_listings.add_permits(1);
        }
        Ok(ListResourceTemplatesResult::with_all_items(Vec::new()))
    }
}

impl InProcessTransportFactory for TestServer {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let server = self.clone();
        async move {
            let (client, transport) = tokio::io::duplex(4096);
            tokio::spawn(async move {
                server
                    .serve(transport)
                    .await
                    .unwrap()
                    .waiting()
                    .await
                    .unwrap();
            });
            Ok(client)
        }
        .boxed()
    }
}

pub(crate) fn key(context: &McpRuntimeContext, config: &McpServerConfig) -> ConnectionKey {
    ConnectionKey {
        server_name: "test_server".to_string(),
        identity: reusable_server_identity("test_server", config, context),
        account: ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        auth_state: None,
        startup_timeout: Duration::from_secs(30),
        catalog_item_limit: 100,
    }
}

pub(crate) fn context() -> McpRuntimeContext {
    McpRuntimeContext::new(
        Arc::new(EnvironmentManager::default_for_tests()),
        std::env::temp_dir(),
    )
}

pub(crate) fn config() -> McpServerConfig {
    serde_json::from_value(json!({"command": "test-server", "args": ["server.mjs"]})).unwrap()
}

pub(crate) fn startup(
    sharing: ConnectionSharing,
    starts: Arc<AtomicUsize>,
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
    server: impl InProcessTransportFactory + Clone + 'static,
) -> impl Fn(CancellationToken, Option<CancellationToken>) -> ManagedClientFuture {
    move |cancellation, _sharing_declined| {
        let sharing = sharing.clone();
        let starts = Arc::clone(&starts);
        let first_started = Arc::clone(&first_started);
        let release_first = Arc::clone(&release_first);
        let server = server.clone();
        async move {
            async {
                if starts.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_started.notify_one();
                    release_first.notified().await;
                }
                let mut client = create_ready_async_managed_client(Vec::new())
                    .await
                    .client()
                    .await?;
                client.client = Arc::new(
                    RmcpClient::new_in_process_client(Arc::new(server))
                        .await
                        .expect("create test MCP client"),
                );
                client
                    .client
                    .initialize(
                        InitializeRequestParams::new(
                            ClientCapabilities::default(),
                            Implementation::new("test", "1"),
                        )
                        .with_protocol_version(ProtocolVersion::V_2025_06_18),
                        Some(Duration::from_secs(30)),
                        Box::new(|_, _| {
                            futures::future::ready(Ok(ElicitationResponse {
                                action: ElicitationAction::Decline,
                                content: None,
                                meta: None,
                            }))
                            .boxed()
                        }),
                    )
                    .await?;
                let (tools, meta) = list_tools_for_client_uncached(
                    "test_server",
                    /*is_codex_apps_mcp_server*/ false,
                    "test",
                    &client.client,
                    /*timeout*/ None,
                    /*catalog_item_limit*/ 100,
                    /*server_instructions*/ None,
                )
                .await?;
                client.tool_catalog =
                    Arc::new(ClientToolCatalog::new(tools, meta, /*updates*/ None));
                client.connection_sharing = sharing;
                Ok(client)
            }
            .or_cancel(&cancellation)
            .await
            .unwrap_or(Err(StartupOutcomeError::Cancelled))
        }
        .boxed()
        .shared()
    }
}

struct LeaseFrame {
    message: serde_json::Value,
    replies: mpsc::UnboundedSender<serde_json::Value>,
}

impl LeaseFrame {
    fn reply(&self, result: serde_json::Value) {
        self.replies
            .send(json!({"jsonrpc": "2.0", "id": self.message["id"], "result": result}))
            .unwrap();
    }

    fn reject(&self) {
        self.replies
            .send(json!({
                "jsonrpc": "2.0", "id": self.message["id"],
                "error": {"code": -32602, "message": "lease rejected"}
            }))
            .unwrap();
    }
}

#[derive(Clone)]
struct LeaseTransport(mpsc::UnboundedSender<LeaseFrame>);

impl InProcessTransportFactory for LeaseTransport {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let (client, server) = tokio::io::duplex(4096);
        let (reader, mut writer) = tokio::io::split(server);
        let mut lines = BufReader::new(reader).lines();
        let frames = self.0.clone();
        let (replies, mut responses) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(response) = responses.recv() => {
                        writer.write_all(format!("{response}\n").as_bytes()).await.unwrap();
                    }
                    line = lines.next_line() => {
                        let Some(line) = line.unwrap() else { break };
                        let frame = LeaseFrame {
                            message: serde_json::from_str(&line).unwrap(),
                            replies: replies.clone(),
                        };
                        match frame.message["method"].as_str().unwrap() {
                            "initialize" => frame.reply(json!({
                                "protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
                                "serverInfo": {"name": "test", "version": "1"}
                            })),
                            "notifications/initialized" => {}
                            "tools/list" => frame.reply(json!({"tools": []})),
                            _ => frames.send(frame).unwrap(),
                        }
                    }
                }
            }
        });
        futures::future::ready(Ok(client)).boxed()
    }
}

struct LeasePeer {
    pool: McpConnectionPool,
    key: ConnectionKey,
    transport: LeaseTransport,
    frames: mpsc::UnboundedReceiver<LeaseFrame>,
}

impl LeasePeer {
    fn new() -> Self {
        let (tx, frames) = mpsc::unbounded_channel();
        Self {
            pool: McpConnectionPool::default(),
            key: key(&context(), &config()),
            transport: LeaseTransport(tx),
            frames,
        }
    }

    fn lease(&self) -> (McpConnectionLease, Arc<McpSessionRoute>) {
        let pool = self.pool.clone();
        let key = self.key.clone();
        let transport = self.transport.clone();
        let route = route();
        let (lease, _) = self.pool.acquire_named_with_ema_startup_route(
            self.key.clone(),
            McpConnectionPoolMode::Reuse,
            &route,
            None,
            move |router| {
                let slot = Arc::<ConnectionSlot>::default();
                let capabilities = Arc::new(Mutex::new(Some(
                    json!({"extensions": {LEASE_CAPABILITY: {}}}),
                )));
                let cancellation = CancellationToken::new();
                let mut client = client_with_startup(router.clone(), {
                    let pool = pool.clone();
                    let key = key.clone();
                    let transport = transport.clone();
                    let slot = Arc::clone(&slot);
                    let capabilities = Arc::clone(&capabilities);
                    let cancellation = cancellation.clone();
                    async move {
                        let ready = Arc::<Notify>::default();
                        ready.notify_one();
                        let launch = startup(
                            ConnectionSharing::Negotiated,
                            Arc::default(),
                            Arc::default(),
                            ready,
                            transport,
                        );
                        pool.connect(
                            slot,
                            key,
                            router,
                            capabilities,
                            /*tool_catalog_cache_context*/ None,
                            cancellation,
                            launch,
                        )
                        .await
                    }
                    .boxed()
                    .shared()
                });
                client.connection_slot = Some(slot);
                client.server_capabilities = capabilities;
                client.cancel_token = cancellation;
                client
            },
        );
        (lease, route)
    }

    fn attach(
        &self,
    ) -> tokio::task::JoinHandle<Result<(McpPooledBindingClient, serde_json::Value)>> {
        let (lease, route) = self.lease();
        tokio::spawn(async move {
            let binding = bind_wire_test_client(&lease, route).await?;
            let (route, wire) = binding.acquire_wire_route().await?;
            let wire = wire.expect("test server negotiated a wire lease");
            drop(route);
            Ok((binding, json!(wire.protocol.id)))
        })
    }

    async fn next(&mut self, method: &str) -> LeaseFrame {
        let frame = self.next_frame().await;
        assert_eq!(frame.message["method"], method);
        frame
    }

    async fn next_frame(&mut self) -> LeaseFrame {
        tokio::time::timeout(Duration::from_secs(2), self.frames.recv())
            .await
            .expect("expected a frame from the client")
            .unwrap()
    }

    async fn chat(&mut self) -> (McpPooledBindingClient, serde_json::Value) {
        let attach = self.attach();
        let open = self.next("codex/connection/open").await;
        open.reply(json!({}));
        let (binding, id) = attach.await.unwrap().unwrap();
        assert_eq!(id, open.message["params"]["leaseId"]);
        (binding, id)
    }
}

async fn bind_wire_test_client(
    lease: &McpConnectionLease,
    route: Arc<McpSessionRoute>,
) -> Result<McpPooledBindingClient> {
    lease
        .bind_mcp_request(route, |client| async move {
            Ok(Arc::new(client.client().await?))
        })
        .await
}

#[tokio::test(start_paused = true)]
async fn releasing_lease_cancels_pending_call_before_release() {
    let mut peer = LeasePeer::new();
    let (first, first_id) = peer.chat().await;
    let (other, other_id) = peer.chat().await;
    let call = first.run_wire(|client, lease| async move {
        client
            .call_tool("wait".into(), None, |_| Ok(None), None, lease)
            .await
    });
    tokio::pin!(call);
    let request = tokio::select! {
        result = &mut call => panic!("call completed without a reply: {result:?}"),
        frame = peer.next("tools/call") => frame,
    };
    first.lease.unregister_route(&first.route);
    first.lease.release();
    // Leave the caller unpolled: the release must drive the cancellation write itself.
    let cancel = peer.next("notifications/cancelled").await;
    assert_eq!(cancel.message["params"]["requestId"], request.message["id"]);
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], first_id);
    release.reply(json!({}));
    assert!(call.await.is_err());
    first.lease.wait_for_wire_shutdown().await;

    // Frodex retires a physical generation after active attribution is lost.
    let recovered = bind_wire_test_client(&other.lease, Arc::clone(&other.route))
        .await
        .unwrap();
    assert_ne!(recovered.connection.id, other.connection.id);
    let (result, ()) = tokio::join!(
        recovered.run_wire(|client, lease| async move {
            client
                .call_tool("test".into(), None, |_| Ok(None), None, lease)
                .await
        }),
        async {
            let open = peer.next("codex/connection/open").await;
            let id = open.message["params"]["leaseId"].clone();
            assert_ne!(id, other_id);
            open.reply(json!({}));
            let request = peer.next("tools/call").await;
            assert_eq!(request.message["params"]["_meta"][LEASE_META_KEY], id);
            request.reply(json!({"content": []}));
        }
    );
    result.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dropping_lease_outside_runtime_releases_it() {
    let mut peer = LeasePeer::new();
    let (_other, _) = peer.chat().await;
    let (chat, id) = peer.chat().await;
    std::thread::spawn(move || drop(chat)).join().unwrap();
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], id);
    release.reply(json!({}));
}

#[tokio::test(start_paused = true)]
async fn timed_out_open_is_released_after_the_server_acknowledges_it() {
    let mut peer = LeasePeer::new();
    let (_other, _) = peer.chat().await;
    let attach = peer.attach();
    let open = peer.next("codex/connection/open").await;
    tokio::time::advance(LEASE_REQUEST_TIMEOUT).await;
    assert!(attach.await.unwrap().is_err());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), peer.frames.recv())
            .await
            .is_err()
    );
    open.reply(json!({}));
    let release = peer.next("codex/connection/release").await;
    assert_eq!(
        release.message["params"]["leaseId"],
        open.message["params"]["leaseId"]
    );
    release.reply(json!({}));
}

#[tokio::test(start_paused = true)]
async fn concurrent_attributable_requests_open_once_and_override_both_metadata_keys() -> Result<()>
{
    let mut peer = LeasePeer::new();
    let (lease, route) = peer.lease();
    let binding = bind_wire_test_client(&lease, route).await?;
    let tool = binding.run_wire(|client, lease| async move {
        client
            .call_tool(
                "test".into(),
                None,
                |_| {
                    Ok(Some(json!({
                        (LEASE_META_KEY): "forged", "retained": true,
                    })))
                },
                None,
                lease,
            )
            .await
    });
    let read = binding.run_wire(|client, lease| async move {
        let mut params = rmcp::model::ReadResourceRequestParams::new("memo://test".to_string());
        params
            .meta
            .get_or_insert_default()
            .insert(LEASE_META_KEY.to_owned(), json!("forged"));
        client.read_resource(params, None, lease).await
    });
    let (tool, read, ()) = tokio::join!(tool, read, async {
        let open = peer.next("codex/connection/open").await;
        let id = open.message["params"]["leaseId"].clone();
        assert_ne!(id, "forged");
        open.reply(json!({}));
        let mut methods = HashSet::new();
        for _ in 0..2 {
            let frame = peer.next_frame().await;
            assert_eq!(frame.message["params"]["_meta"][LEASE_META_KEY], id);
            let method = frame.message["method"].as_str().unwrap();
            assert!(methods.insert(method.to_string()));
            match method {
                "tools/call" => {
                    assert_eq!(frame.message["params"]["_meta"]["retained"], true);
                    frame.reply(json!({"content": []}));
                }
                "resources/read" => frame.reply(json!({"contents": []})),
                _ => panic!("unexpected request: {method}"),
            }
        }
    });
    tool?;
    read?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn rejected_release_evicts_only_its_physical_generation() -> Result<()> {
    let mut peer = LeasePeer::new();
    let (first, first_id) = peer.chat().await;
    let (sibling, _) = peer.chat().await;
    let (ordinary, wire) = first.acquire_wire_route().await?;
    let retired = wire.unwrap();
    drop(ordinary);
    first.lease.unregister_route(&first.route);
    first.lease.release();
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], first_id);
    release.reject();
    first.lease.wait_for_wire_shutdown().await;
    let replacement = bind_wire_test_client(&sibling.lease, Arc::clone(&sibling.route)).await?;
    assert_ne!(replacement.connection.id, retired.physical.connection.id);
    // Delayed failure handling for the captured old transport cannot evict this replacement.
    retired.physical.retire_after_failure();
    assert_eq!(sibling.lease.current()?.id, replacement.connection.id);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn idle_wire_release_does_not_evict_a_busy_sibling() -> Result<()> {
    let mut peer = LeasePeer::new();
    let (idle, idle_id) = peer.chat().await;
    let (busy, _) = peer.chat().await;
    let ordinary = busy.acquire_route().await?;
    idle.lease.unregister_route(&idle.route);
    idle.lease.release();
    tokio::time::advance(RETIREMENT_SHUTDOWN_TIMEOUT + Duration::from_secs(1)).await;
    assert!(peer.frames.try_recv().is_err());
    assert_eq!(busy.lease.current()?.id, busy.connection.id);
    drop(ordinary);
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], idle_id);
    release.reply(json!({}));
    idle.lease.wait_for_wire_shutdown().await;
    assert_eq!(busy.lease.current()?.id, busy.connection.id);
    Ok(())
}

#[tokio::test]
async fn raw_transport_is_not_reused_by_a_different_request_router() -> Result<()> {
    let pool = McpConnectionPool::default();
    let key = key(&context(), &config());
    let starts = Arc::new(AtomicUsize::new(0));
    let start_next = Arc::new(Notify::new());
    let launch = startup(
        ConnectionSharing::Negotiated,
        Arc::clone(&starts),
        Arc::default(),
        Arc::clone(&start_next),
        TestServer::default(),
    );
    start_next.notify_one();
    let first = pool
        .connect(
            Arc::default(),
            key.clone(),
            McpConnectionRequestRouter::default(),
            Arc::default(),
            None,
            CancellationToken::new(),
            &launch,
        )
        .await?;
    start_next.notify_one();
    let second = pool
        .connect(
            Arc::default(),
            key,
            McpConnectionRequestRouter::default(),
            Arc::default(),
            None,
            CancellationToken::new(),
            &launch,
        )
        .await?;
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert!(!Arc::ptr_eq(&first.client, &second.client));
    Ok(())
}

#[tokio::test]
async fn shared_startup_survives_first_chat_cancellation_and_rejects_released_bindings() {
    let pool = McpConnectionPool::default();
    let key = key(&context(), &config());
    let server = TestServer::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let launch = startup(
        ConnectionSharing::Negotiated,
        Arc::clone(&starts),
        Arc::clone(&started),
        Arc::clone(&release),
        server.clone(),
    );
    let router = McpConnectionRequestRouter::default();
    let first_cancel = CancellationToken::new();
    let first = pool.connect(
        Arc::default(),
        key.clone(),
        router.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        first_cancel.clone(),
        &launch,
    );
    let second = pool.connect(
        Arc::default(),
        key.clone(),
        router.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        CancellationToken::new(),
        &launch,
    );
    tokio::pin!(first, second);
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    started.notified().await;
    first_cancel.cancel();
    assert!(matches!(first.await, Err(StartupOutcomeError::Cancelled)));
    release.notify_one();
    let client = second.await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(!client.client.is_closed().await);

    let other = pool
        .connect(
            Arc::default(),
            key,
            router.clone(),
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &launch,
        )
        .await
        .unwrap();
    let mut config = crate::mcp::tests::test_mcp_config(std::env::temp_dir());
    config
        .server_permission_profiles
        .insert("test_server".to_string(), config.permission_profile.clone());
    let prepared = crate::binding::PreparedMcpCall::new(
        Arc::new(crate::connection_manager::McpConnectionSet::empty(
            /*prefix_mcp_tool_names*/ true,
        )),
        McpPooledBindingClient::for_test(Arc::new(client.clone())),
        Arc::new(config),
        client.tool_catalog.read(Arc::new).await,
        create_test_tool("test_server", "test"),
        crate::server::McpServerMetadata {
            environment_id: "local".to_string(),
            pollutes_memory: false,
            origin: None,
            supports_parallel_tool_calls: true,
            default_tools_approval_mode: None,
            tool_approval_modes: Default::default(),
        },
        /*plugin_id*/ None,
        /*selected_plugin_server*/ false,
    )
    .unwrap();
    let bindings = McpBindingClients::new(HashMap::from([(
        "test_server".to_string(),
        McpPooledBindingClient::for_test(Arc::new(client.clone())),
    )]));
    server.block_listing.store(true, Ordering::Release);
    let waiting = async {
        tokio::join!(
            bindings.list_resources("test_server", /*params*/ None),
            bindings.list_resource_templates("test_server", /*params*/ None),
        )
    };
    tokio::pin!(waiting);
    tokio::select! {
        _ = &mut waiting => panic!("resource listings must wait for cancellation"),
        permits = server.started_listings.acquire_many(2) => permits.unwrap().forget(),
    }
    let mut owned = create_ready_async_managed_client(Vec::new()).await;
    owned.client = futures::future::ready(Ok(client.clone())).boxed().shared();
    owned.shutdown().await;
    let (resources, templates) = waiting.await;
    assert!(resources.is_err());
    assert!(templates.is_err());
    tokio::time::timeout(
        Duration::from_secs(1),
        server.cancelled_listings.acquire_many(2),
    )
    .await
    .expect("released resource listings must be cancelled")
    .unwrap()
    .forget();
    let prepared_error = prepared
        .call_with_preparation(/*requested_timeout*/ None, || async {
            panic!("released bindings must not perform irreversible preparation")
        })
        .await
        .unwrap_err();
    assert!(
        prepared_error
            .to_string()
            .contains("lease has been released")
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
    owned.client = futures::future::ready(Ok(other)).boxed().shared();
    owned.shutdown().await;
}

#[tokio::test]
async fn callback_during_startup_releases_waiting_chats_without_sharing_authority() {
    let pool = McpConnectionPool::default();
    let key = key(&context(), &config());
    let starts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let second_callback = Arc::new(Notify::new());
    let release_second = Arc::new(Notify::new());
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let launch = |name| {
        let launch = startup(
            ConnectionSharing::Private,
            Arc::clone(&starts),
            Arc::clone(&started),
            Arc::clone(&release),
            TestServer::default(),
        );
        let callbacks = Arc::clone(&callbacks);
        let second_callback = Arc::clone(&second_callback);
        let release_second = Arc::clone(&release_second);
        move |cancellation, sharing_declined: Option<CancellationToken>| {
            let startup = launch(cancellation, sharing_declined.clone());
            let callbacks = Arc::clone(&callbacks);
            let second_callback = Arc::clone(&second_callback);
            let release_second = Arc::clone(&release_second);
            async move {
                let mut client = startup.await?;
                let (negotiation, callback) = SharingNegotiation::new(
                    Box::new(move |_, _| {
                        callbacks.lock().unwrap().push(name);
                        futures::future::ready(Ok(ElicitationResponse {
                            action: ElicitationAction::Decline,
                            content: None,
                            meta: None,
                        }))
                        .boxed()
                    }),
                    sharing_declined.unwrap_or_default(),
                );
                callback(
                    rmcp::model::RequestId::Number(0),
                    Elicitation::OpenAiForm {
                        meta: None,
                        message: "startup prompt".to_string(),
                        requested_schema: json!({}),
                    },
                )
                .await
                .unwrap();
                if name == "first" {
                    second_callback.notified().await;
                } else {
                    second_callback.notify_one();
                    release_second.notified().await;
                }
                client.connection_sharing = negotiation.finish(&ProtocolVersion::V_2026_07_28);
                Ok(client)
            }
            .boxed()
            .shared()
        }
    };
    let router = McpConnectionRequestRouter::default();
    let first = pool.connect(
        Arc::default(),
        key.clone(),
        router.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        CancellationToken::new(),
        launch("first"),
    );
    let second = pool.connect(
        Arc::default(),
        key,
        router.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        CancellationToken::new(),
        launch("second"),
    );
    tokio::pin!(first, second);
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    started.notified().await;
    release.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            first = &mut first => first,
            _ = &mut second => panic!("the joining chat should still be starting"),
        }
    })
    .await
    .expect("the joining chat must start before the first finishes")
    .unwrap();
    first
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
            /*lease*/ None,
        )
        .await
        .unwrap();
    let mut owner = create_ready_async_managed_client(Vec::new()).await;
    owner.client = futures::future::ready(Ok(first.clone())).boxed().shared();
    tokio::time::timeout(Duration::from_secs(10), owner.shutdown())
        .await
        .expect("private owner shutdown must not wait for the joining chat");
    assert!(first.client.is_closed().await);

    release_second.notify_one();
    let second = second.await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(*callbacks.lock().unwrap(), ["first", "second"]);
    second
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
            /*lease*/ None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn connection_key_changes_with_the_owner_but_survives_credential_refresh() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    let mut original = key(&context(), &config());
    original.auth_state = Some((manager, AuthChangeState::default()));
    let mut refreshed = original.clone();
    refreshed.auth_state.as_mut().unwrap().1.generation += 1;
    assert!(original == refreshed);

    refreshed.auth_state.as_mut().unwrap().1.owner_generation += 1;
    assert!(original != refreshed);
}

#[tokio::test(start_paused = true)]
async fn new_chat_relists_tools_without_invalidating_or_waiting_for_existing_calls() {
    let pool = McpConnectionPool::default();
    let key = key(&context(), &config());
    let server = TestServer::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    release.notify_one();
    let launch = startup(
        ConnectionSharing::Negotiated,
        Arc::clone(&starts),
        Arc::default(),
        release,
        server.clone(),
    );
    let router = McpConnectionRequestRouter::default();
    let first = pool
        .connect(
            Arc::default(),
            key.clone(),
            router.clone(),
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &launch,
        )
        .await
        .unwrap();
    let before = first.tool_catalog.read(|snapshot| snapshot).await;
    let release_call = Notify::new();
    let call = first
        .tool_catalog
        .run_with_snapshot(&before, || release_call.notified());
    tokio::pin!(call);
    assert!(futures::poll!(&mut call).is_pending());
    for tool_name in ["test", "new"] {
        *server.tools.lock().unwrap() = vec![create_test_tool("test_server", tool_name).tool];
        let second = tokio::time::timeout(
            Duration::from_secs(1),
            pool.connect(
                Arc::default(),
                key.clone(),
                router.clone(),
                Arc::default(),
                /*tool_catalog_cache_context*/ None,
                CancellationToken::new(),
                &launch,
            ),
        )
        .await
        .expect("new chats must not wait for existing calls")
        .unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(
            second
                .tool_catalog
                .read(|snapshot| snapshot.tools[0].tool.name.to_string())
                .await,
            tool_name
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            second.tool_catalog.refresh(
                || async { Ok((vec![create_test_tool("test_server", "refreshed")], None, ())) },
                |_, _, ()| {},
            ),
        )
        .await
        .expect("one chat's refresh must not wait for another chat's calls")
        .unwrap();
        assert_eq!(
            first
                .tool_catalog
                .read(|snapshot| snapshot.tools[0].tool.name.to_string())
                .await,
            "test"
        );
    }
    release_call.notify_one();
    assert_eq!(call.await, Some(()));

    server.block_listing.store(true, Ordering::Release);
    let failed = pool
        .connect(
            Arc::default(),
            key,
            router.clone(),
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &launch,
        )
        .await;
    assert!(failed.is_err());
    let _cancelled =
        tokio::time::timeout(Duration::from_secs(1), server.cancelled_listings.acquire())
            .await
            .expect("timed-out listings must be cancelled")
            .unwrap();
    first
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
            /*lease*/ None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn negotiation_requires_the_stateless_protocol_and_detaches_startup_callbacks() {
    for (protocol_version, expected_sharing, expected_action) in [
        (
            ProtocolVersion::V_2025_06_18,
            false,
            ElicitationAction::Decline,
        ),
        (
            ProtocolVersion::V_2026_07_28,
            true,
            ElicitationAction::Cancel,
        ),
    ] {
        let (negotiation, callback) = SharingNegotiation::new(
            Box::new(|_, _| {
                futures::future::ready(Ok(ElicitationResponse {
                    action: ElicitationAction::Decline,
                    content: None,
                    meta: None,
                }))
                .boxed()
            }),
            CancellationToken::new(),
        );
        let sharing = negotiation.finish(&protocol_version);
        assert_eq!(
            matches!(sharing, ConnectionSharing::Negotiated),
            expected_sharing
        );
        let response = callback(
            rmcp::model::RequestId::Number(0),
            Elicitation::OpenAiForm {
                meta: None,
                message: "unexpected".to_string(),
                requested_schema: json!({}),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.action, expected_action);
    }
}

fn root_pool_config(command: &str) -> McpServerConfig {
    McpServerConfig {
        transport: McpServerTransportConfig::Stdio {
            command: command.to_string(),
            args: Vec::new(),
            env: None,
            env_vars: Vec::new(),
            cwd: None,
        },
        auth: Default::default(),
        environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
        enabled: true,
        required: false,
        startup_readiness: Default::default(),
        supports_parallel_tool_calls: false,
        tool_input_schema_max_bytes: None,
        omit_tools_from: None,
        disabled_reason: None,
        startup_timeout_sec: None,
        tool_timeout_sec: None,
        default_tools_approval_mode: None,
        enabled_tools: None,
        disabled_tools: None,
        scopes: None,
        oauth: None,
        oauth_resource: None,
        tools: HashMap::new(),
    }
}

fn form_extensions() -> ClientMcpExtensions {
    ClientMcpExtensions::new([(OPENAI_FORM_EXTENSION_ID.to_string(), serde_json::json!({}))])
}

fn identity(command: &str, cwd: &str) -> McpConnectionIdentity {
    identity_with(
        root_pool_config(command),
        cwd,
        &Ok(None),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    )
}

fn identity_with(
    config: McpServerConfig,
    cwd: &str,
    resolved_environment: &Result<Option<Arc<codex_exec_server::Environment>>, String>,
    connector_context: ConnectorRuntimeContextKey,
    has_runtime_auth: bool,
    elicitation_capability: &ElicitationCapability,
    client_mcp_extensions: ClientMcpExtensions,
) -> McpConnectionIdentity {
    identity_with_catalog_limit(
        config,
        cwd,
        resolved_environment,
        connector_context,
        has_runtime_auth,
        elicitation_capability,
        client_mcp_extensions,
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    )
}

#[allow(clippy::too_many_arguments)]
fn identity_with_catalog_limit(
    config: McpServerConfig,
    cwd: &str,
    resolved_environment: &Result<Option<Arc<codex_exec_server::Environment>>, String>,
    connector_context: ConnectorRuntimeContextKey,
    has_runtime_auth: bool,
    elicitation_capability: &ElicitationCapability,
    client_mcp_extensions: ClientMcpExtensions,
    catalog_item_limit: usize,
) -> McpConnectionIdentity {
    let server = EffectiveMcpServer::from_host_config(config);
    let runtime_context = McpRuntimeContext::new(
        Arc::new(environment_manager_without_environments()),
        PathBuf::from(cwd),
    );
    let auth = has_runtime_auth.then(|| CodexAuth::from_api_key("test-runtime-auth"));
    let runtime_auth_provider = auth
        .as_ref()
        .map(codex_model_provider::auth_provider_from_auth);
    McpConnectionIdentity::new(
        "test",
        &server,
        /*host_plugin_root*/ None,
        OAuthCredentialsStoreMode::default(),
        AuthKeyringBackendKind::default(),
        McpOAuthRefreshMode::Legacy,
        resolved_environment,
        &runtime_context,
        runtime_auth_provider.as_ref(),
        auth.as_ref(),
        Some((PathBuf::from("/codex-home"), connector_context)),
        elicitation_capability.clone(),
        client_mcp_extensions,
        /*previous_identity*/ None,
        /*effective_protocol_mode*/ Some(crate::McpProtocolMode::Legacy),
        catalog_item_limit,
    )
}

fn identity_with_auth(auth: &CodexAuth) -> McpConnectionIdentity {
    let server = EffectiveMcpServer::from_host_config(root_pool_config("server"));
    let runtime_context = McpRuntimeContext::new(
        Arc::new(environment_manager_without_environments()),
        PathBuf::from("/one"),
    );
    let provider = codex_model_provider::auth_provider_from_auth(auth);
    McpConnectionIdentity::new(
        "test",
        &server,
        /*host_plugin_root*/ None,
        OAuthCredentialsStoreMode::default(),
        AuthKeyringBackendKind::default(),
        McpOAuthRefreshMode::Legacy,
        &Ok(None),
        &runtime_context,
        Some(&provider),
        Some(auth),
        Some((
            PathBuf::from("/codex-home"),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
        )),
        ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        /*previous_identity*/ None,
        /*effective_protocol_mode*/ Some(crate::McpProtocolMode::Legacy),
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    )
}

fn identity_with_agent_plugin(agent_plugin: bool) -> McpConnectionIdentity {
    let server = EffectiveMcpServer::from_host_config(root_pool_config("server"))
        .with_agent_plugin(agent_plugin);
    let runtime_context = McpRuntimeContext::new(
        Arc::new(environment_manager_without_environments()),
        PathBuf::from("/one"),
    );
    McpConnectionIdentity::new(
        "test",
        &server,
        /*host_plugin_root*/ None,
        OAuthCredentialsStoreMode::default(),
        AuthKeyringBackendKind::default(),
        McpOAuthRefreshMode::Legacy,
        &Ok(None),
        &runtime_context,
        /*runtime_auth_provider*/ None,
        /*auth*/ None,
        Some((
            PathBuf::from("/codex-home"),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
        )),
        ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        /*previous_identity*/ None,
        /*effective_protocol_mode*/ Some(crate::McpProtocolMode::Legacy),
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    )
}

fn ema_identity(owner: Arc<OnceLock<crate::ema_auth::EmaAuthContext>>) -> McpConnectionIdentity {
    let mut server = root_pool_config("enterprise");
    server.auth = McpServerAuth::EmaAuth;
    server.transport = McpServerTransportConfig::StreamableHttp {
        url: "https://enterprise.example/mcp".to_string(),
        bearer_token_env_var: None,
        http_headers: None,
        env_http_headers: None,
        http_headers_helper: None,
    };
    identity_with(
        server,
        "/one",
        &Ok(None),
        ConnectorRuntimeContextKey::personal(None, None),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    )
    .with_ema_owner(owner)
}

#[tokio::test]
async fn ema_connection_pool_isolates_runtime_authority_owners() {
    let pool = McpConnectionPool::default();
    let first_owner = Arc::new(OnceLock::new());
    let second_owner = Arc::new(OnceLock::new());
    let first_route = route();
    let second_route = route();
    let first = pool.acquire(
        ema_identity(Arc::clone(&first_owner)),
        McpConnectionPoolMode::Reuse,
        &first_route,
        client,
    );
    let second = pool.acquire(
        ema_identity(second_owner),
        McpConnectionPoolMode::Reuse,
        &second_route,
        client,
    );
    assert!(!first.ptr_eq(&second));
    assert_ne!(first.connection_id(), second.connection_id());
    let same_owner = pool.acquire(
        ema_identity(first_owner),
        McpConnectionPoolMode::Reuse,
        &first_route,
        |_| panic!("the same runtime must retain its original physical EMA connection"),
    );
    assert!(first.ptr_eq(&same_owner));
    assert_eq!(first.connection_id(), same_owner.connection_id());
}

#[test]
fn ema_connection_identity_preserves_failed_owner_context() {
    let failed_owner = Arc::new(OnceLock::new());
    failed_owner.get_or_init(|| {
        Err(StartupOutcomeError::Failed {
            error: "owner account is unavailable".to_string(),
            is_authentication_required: true,
            is_ema_authentication_required: true,
            shutdown: None,
        })
    });
    let original = ema_identity(Arc::clone(&failed_owner));
    assert!(original == ema_identity(Arc::clone(&failed_owner)));
    assert!(original != ema_identity(Arc::new(OnceLock::new())));
    assert!(matches!(
        failed_owner.get(),
        Some(Err(StartupOutcomeError::Failed {
            is_ema_authentication_required: true,
            ..
        }))
    ));
}

#[test]
fn uncached_ema_identity_does_not_change_with_cache_notification_tracking() {
    let owner = Arc::new(OnceLock::new());
    assert!(
        ema_identity(Arc::clone(&owner)).with_tool_list_change_tracking(false)
            == ema_identity(owner).with_tool_list_change_tracking(true)
    );
}

#[tokio::test]
async fn ema_same_owner_refresh_retains_valid_physical_authority() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let identity = ema_identity(Arc::new(OnceLock::new()));
    let first_route = route();
    let refresh_route = route();
    let managed = test_managed_client("EMA").await?;
    let authority = codex_rmcp_client::EmaRequestAuthority::default();
    let physical_authority = authority.clone();
    let first = pool.acquire(
        identity.clone(),
        McpConnectionPoolMode::Reuse,
        &first_route,
        move |router| {
            let mut client = ready_client(router, managed.clone());
            client.ema_authority = Some(physical_authority.clone());
            client
        },
    );
    first.await_current_startup(first_route).await?;
    assert!(first.is_reusable_connection(&identity).await);
    let refreshed = pool.acquire(
        identity,
        McpConnectionPoolMode::Replace,
        &refresh_route,
        |_| panic!("explicit reconnect must not mint another EMA connection"),
    );
    assert_eq!(first.connection_id(), refreshed.connection_id());
    let retained = first
        .retain_ema_for_route(&refresh_route)
        .expect("current authority");
    assert_eq!(first.connection_id(), retained.connection_id());
    assert_eq!(
        retained
            .run(refresh_route, |_| async { "same owner" })
            .await?,
        "same owner"
    );
    assert!(!authority.is_revoked());
    Ok(())
}

#[tokio::test]
async fn revoked_ema_authority_rejects_reuse_dispatch_and_replacement() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let identity = ema_identity(Arc::new(OnceLock::new()));
    let session_route = route();
    let managed = test_managed_client("revocable EMA").await?;
    let authority = codex_rmcp_client::EmaRequestAuthority::default();
    let physical_authority = authority.clone();
    let lease = pool.acquire(
        identity.clone(),
        McpConnectionPoolMode::Reuse,
        &session_route,
        move |router| {
            let mut client = ready_client(router, managed.clone());
            client.ema_authority = Some(physical_authority.clone());
            client
        },
    );
    let context = Arc::new(crate::mcp::ToolPluginContext::default());
    let (binding, _, _) = lease
        .capture_ready_client_and_tools(Arc::clone(&session_route), Arc::clone(&context), None)
        .await
        .expect("authorized binding");
    authority.revoke();
    assert!(!lease.is_reusable_connection(&identity).await);
    assert!(lease.retain_ema_for_route(&session_route).is_none());
    assert!(matches!(
        lease
            .await_current_startup(Arc::clone(&session_route))
            .await,
        Err(StartupOutcomeError::Failed {
            is_authentication_required: true,
            is_ema_authentication_required: true,
            ..
        })
    ));
    assert!(
        lease
            .capture_ready_client_and_tools(Arc::clone(&session_route), context, None,)
            .await
            .is_none()
    );
    let error = lease
        .run_mcp_request(Arc::clone(&session_route), |_| async {
            panic!("revoked authority must reject dispatch before running the operation");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .expect_err("revoked physical authority");
    assert!(matches!(
        error.downcast_ref::<codex_exec_server::ExecServerError>(),
        Some(codex_exec_server::ExecServerError::AuthenticationRequired(
            _
        ))
    ));
    let error = binding
        .run(|_| async {
            panic!("captured bindings must not bypass revocation");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .expect_err("revoked captured authority");
    assert!(matches!(
        error.downcast_ref::<codex_exec_server::ExecServerError>(),
        Some(codex_exec_server::ExecServerError::AuthenticationRequired(
            _
        ))
    ));
    assert!(
        lease
            .inner
            .slot
            .replace_if_current(&lease.current()?, Some(&session_route))
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn tool_list_tracking_changes_isolate_physical_cache_contexts() {
    let pool = McpConnectionPool::default();
    let first_route = route();
    let tracking_route = route();
    let untracked_identity = identity("server", "/one").with_tool_list_change_tracking(false);
    let tracked_identity = identity("server", "/one").with_tool_list_change_tracking(true);
    let untracked = pool.acquire(
        untracked_identity,
        McpConnectionPoolMode::Reuse,
        &first_route,
        client,
    );
    let tracked = pool.acquire(
        tracked_identity.clone(),
        McpConnectionPoolMode::Reuse,
        &tracking_route,
        client,
    );
    assert!(!untracked.ptr_eq(&tracked));
    assert_ne!(untracked.connection_id(), tracked.connection_id());
    let reused = pool.acquire(
        tracked_identity,
        McpConnectionPoolMode::Reuse,
        &tracking_route,
        |_| panic!("matching tracked contexts should reuse their connection"),
    );
    assert_eq!(tracked.connection_id(), reused.connection_id());
}

#[tokio::test]
async fn dropping_async_client_snapshot_does_not_cancel_physical_connection() {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        client,
    );
    let cancelled = lease.connection_cancel_token();
    drop(lease.test_client());
    assert!(!cancelled.is_cancelled());
    drop(lease);
    assert!(cancelled.is_cancelled());
}

#[tokio::test]
async fn connection_pool_reuse_and_replacement_isolate_credential_policies() {
    let pool = McpConnectionPool::default();
    let route = route();
    let runtime_context = McpRuntimeContext::new(
        Arc::new(environment_manager_without_environments()),
        PathBuf::from("/one"),
    );
    let identity_with_policy = |credential_policy| {
        let server = EffectiveMcpServer::from_config_with_policy(
            root_pool_config("server"),
            credential_policy,
        );
        McpConnectionIdentity::new(
            "test",
            &server,
            /*host_plugin_root*/ None,
            OAuthCredentialsStoreMode::default(),
            AuthKeyringBackendKind::default(),
            McpOAuthRefreshMode::Legacy,
            &Ok(None),
            &runtime_context,
            /*runtime_auth_provider*/ None,
            /*auth*/ None,
            /*codex_apps_cache_identity*/ None,
            ElicitationCapability::default(),
            ClientMcpExtensions::default(),
            /*previous_identity*/ None,
            /*effective_protocol_mode*/ Some(crate::McpProtocolMode::Legacy),
            crate::pagination::MAX_MCP_CATALOG_ITEMS,
        )
    };
    let host_identity = identity_with_policy(McpCredentialPolicy::HostFallbackAllowed);
    let executor_identity = identity_with_policy(McpCredentialPolicy::ExecutorOnly);
    let host = pool.acquire(
        host_identity.clone(),
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );
    let executor = pool.acquire(
        executor_identity.clone(),
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );

    assert!(!host.ptr_eq(&executor));
    assert_ne!(host.connection_id(), executor.connection_id());
    for (identity, lease, other) in [
        (host_identity, &host, &executor),
        (executor_identity, &executor, &host),
    ] {
        let original_connection_id = lease.connection_id();
        let other_connection_id = other.connection_id();
        let reused = pool.acquire(
            identity.clone(),
            McpConnectionPoolMode::Reuse,
            &route,
            |_| panic!("same-policy reuse should retain the existing connection"),
        );
        assert!(lease.ptr_eq(&reused));
        assert_eq!(reused.connection_id(), original_connection_id);

        let replacement = pool.acquire(identity, McpConnectionPoolMode::Replace, &route, client);
        assert!(lease.ptr_eq(&replacement));
        assert_eq!(lease.connection_id(), replacement.connection_id());
        assert_ne!(replacement.connection_id(), original_connection_id);
        assert_eq!(other.connection_id(), other_connection_id);
        assert!(!replacement.ptr_eq(other));
    }
}

#[tokio::test]
async fn agent_plugin_is_part_of_the_connection_pool_identity() {
    let pool = McpConnectionPool::default();
    let route = route();
    let standard_identity = identity_with_agent_plugin(/*agent_plugin*/ false);
    let agent_plugin_identity = identity_with_agent_plugin(/*agent_plugin*/ true);

    assert!(!standard_identity.has_same_connection_config(&agent_plugin_identity));
    let standard = pool.acquire(
        standard_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );
    let agent_plugin = pool.acquire(
        agent_plugin_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );

    assert!(!standard.ptr_eq(&agent_plugin));
    assert_ne!(standard.connection_id(), agent_plugin.connection_id());
}

#[tokio::test]
async fn catalog_item_limit_is_part_of_the_connection_pool_identity() {
    let pool = McpConnectionPool::default();
    let route = route();
    let connector_context = ConnectorRuntimeContextKey::personal(
        /*account_id*/ None, /*chatgpt_user_id*/ None,
    );
    let standard_identity = identity_with_catalog_limit(
        root_pool_config("server"),
        "/one",
        &Ok(None),
        connector_context.clone(),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    );
    let elevated_identity = identity_with_catalog_limit(
        root_pool_config("server"),
        "/one",
        &Ok(None),
        connector_context,
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        crate::pagination::MAX_CODEX_APPS_TOOL_CATALOG_ITEMS,
    );

    assert!(!standard_identity.has_same_connection_config(&elevated_identity));
    let standard = pool.acquire(
        standard_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );
    let elevated = pool.acquire(
        elevated_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );

    assert!(!standard.ptr_eq(&elevated));
    assert_ne!(standard.connection_id(), elevated.connection_id());
}

#[tokio::test]
async fn client_mcp_extensions_are_part_of_the_connection_pool_identity() {
    let pool = McpConnectionPool::default();
    let route = route();
    let connector_context = ConnectorRuntimeContextKey::personal(
        /*account_id*/ None, /*chatgpt_user_id*/ None,
    );
    let default_identity = identity_with(
        root_pool_config("server"),
        "/one",
        &Ok(None),
        connector_context.clone(),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    );
    let form_identity = identity_with(
        root_pool_config("server"),
        "/one",
        &Ok(None),
        connector_context,
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        form_extensions(),
    );

    assert!(!default_identity.has_same_connection_config(&form_identity));
    let default_connection = pool.acquire(
        default_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );
    let form_connection = pool.acquire(form_identity, McpConnectionPoolMode::Reuse, &route, client);

    assert!(!default_connection.ptr_eq(&form_connection));
    assert_ne!(
        default_connection.connection_id(),
        form_connection.connection_id()
    );
}

fn route() -> Arc<McpSessionRoute> {
    route_with(AskForApproval::Never, PermissionProfile::default())
}

fn route_with(
    approval_policy: AskForApproval,
    permission_profile: PermissionProfile,
) -> Arc<McpSessionRoute> {
    route_with_events(approval_policy, permission_profile).0
}

fn route_with_events(
    approval_policy: AskForApproval,
    permission_profile: PermissionProfile,
) -> (
    Arc<McpSessionRoute>,
    ElicitationRequestManager,
    async_channel::Receiver<Event>,
) {
    let manager = ElicitationRequestManager::new(
        crate::mcp::tests::test_elicitation_config("test", approval_policy, permission_profile),
        /*reviewer*/ None,
        /*lifecycle*/ None,
        ElicitationRequestRouter::default(),
    );
    let (tx, rx) = async_channel::unbounded();
    (
        Arc::new(McpSessionRoute::new(
            "test-submit".to_string(),
            manager.clone(),
            Some(tx),
        )),
        manager,
        rx,
    )
}

fn client(request_router: crate::request_router::McpConnectionRequestRouter) -> AsyncManagedClient {
    client_with_startup(
        request_router,
        async { Err(StartupOutcomeError::Cancelled) }
            .boxed()
            .shared(),
    )
}

fn client_with_startup(
    request_router: crate::request_router::McpConnectionRequestRouter,
    client: ManagedClientFuture,
) -> AsyncManagedClient {
    AsyncManagedClient {
        ema_authority: None,
        client,
        is_codex_apps_mcp_server: false,
        server_capabilities: Arc::new(std::sync::Mutex::new(None)),
        cached_server_info: None,
        codex_apps_tools_cache_context: None,
        tool_catalog_cache_context: None,
        startup_complete: Arc::new(AtomicBool::new(true)),
        initialized_apps: None,
        connection_slot: None,
        startup_reconnect: None,
        cancel_token: CancellationToken::new(),
        request_router,
    }
}

struct TestInProcessTransportFactory;

impl InProcessTransportFactory for TestInProcessTransportFactory {
    fn open(&self) -> futures::future::BoxFuture<'static, io::Result<DuplexStream>> {
        async {
            let (client_stream, _server_stream) = tokio::io::duplex(1);
            Ok(client_stream)
        }
        .boxed()
    }
}

async fn test_managed_client(label: &str) -> anyhow::Result<ManagedClient> {
    Ok(ManagedClient {
        connection_sharing: Default::default(),
        shared_elicitation: None,
        _auth_change_notifications: None,
        client: Arc::new(
            RmcpClient::new_in_process_client(Arc::new(TestInProcessTransportFactory)).await?,
        ),
        server_info: McpServerInfo {
            name: label.to_string(),
            title: None,
            version: "1".to_string(),
            description: None,
            icons: None,
            website_url: None,
        },
        tool_catalog: Arc::new(crate::client_tool_catalog::ClientToolCatalog::new(
            Vec::new(),
            /*meta*/ None,
            /*updates*/ None,
        )),
        tool_timeout: None,
        server_instructions: None,
        server_supports_sandbox_state_meta_capability: false,
        client_supports_network_policy_meta_capability: false,
        codex_apps_tools_cache_context: None,
    })
}

fn ready_client(
    request_router: crate::request_router::McpConnectionRequestRouter,
    managed: ManagedClient,
) -> AsyncManagedClient {
    client_with_startup(
        request_router,
        futures::future::ready(Ok(managed)).boxed().shared(),
    )
}

#[tokio::test]
async fn startup_wait_retries_a_superseded_connection_generation() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let root_route = route();
    let replacement_route = route();
    let old_started = Arc::new(Notify::new());
    let release_old = Arc::new(Notify::new());
    let old_started_for_client = Arc::clone(&old_started);
    let release_old_for_client = Arc::clone(&release_old);
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        move |request_router| {
            let old_started = Arc::clone(&old_started_for_client);
            let release_old = Arc::clone(&release_old_for_client);
            client_with_startup(
                request_router,
                async move {
                    old_started.notify_one();
                    release_old.notified().await;
                    Err(StartupOutcomeError::Failed {
                        error: "superseded startup".to_string(),
                        is_authentication_required: false,
                        is_ema_authentication_required: false,
                        shutdown: None,
                    })
                }
                .boxed()
                .shared(),
            )
        },
    );
    let startup = {
        let lease = lease.clone();
        tokio::spawn(async move { lease.await_current_startup(root_route).await })
    };
    old_started.notified().await;

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &replacement_route,
        |request_router| {
            client_with_startup(
                request_router,
                async {
                    Err(StartupOutcomeError::Failed {
                        error: "current startup".to_string(),
                        is_authentication_required: false,
                        is_ema_authentication_required: false,
                        shutdown: None,
                    })
                }
                .boxed()
                .shared(),
            )
        },
    );
    let error = match tokio::time::timeout(Duration::from_secs(1), startup).await?? {
        Ok(_) => anyhow::bail!("the current generation should report its startup failure"),
        Err(error) => error,
    };
    release_old.notify_one();
    assert!(error.to_string().contains("current startup"));
    assert!(lease.ptr_eq(&replacement));
    Ok(())
}

#[tokio::test]
async fn cancelling_exclusive_startup_does_not_start_a_replacement() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let route = route();
    let starts = Arc::new(AtomicUsize::new(0));
    let startup_started = Arc::new(Notify::new());
    let starts_for_factory = Arc::clone(&starts);
    let startup_started_for_factory = Arc::clone(&startup_started);
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route,
        move |request_router| {
            starts_for_factory.fetch_add(1, Ordering::AcqRel);
            let startup_started = Arc::clone(&startup_started_for_factory);
            client_with_startup(
                request_router,
                async move {
                    startup_started.notify_one();
                    std::future::pending().await
                }
                .boxed()
                .shared(),
            )
        },
    );
    let startup = {
        let lease = lease.clone();
        tokio::spawn(async move { lease.await_current_startup(route).await })
    };
    startup_started.notified().await;

    startup.abort();
    let error = match startup.await {
        Err(error) => error,
        Ok(_) => anyhow::bail!("startup waiter should be cancelled"),
    };
    assert!(error.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !lease.is_connection_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(starts.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn reconnect_elicitation_uses_the_triggering_session_route() -> anyhow::Result<()> {
    let request_router = crate::request_router::McpConnectionRequestRouter::default();
    let sender = Arc::new(request_router.make_sender("test".to_string(), false));
    let reconnect_finished = Arc::new(Notify::new());
    let reconnect_finished_for_factory = Arc::clone(&reconnect_finished);
    let reconnect_factory = Arc::new(move || {
        let reconnect_finished = Arc::clone(&reconnect_finished_for_factory);
        let sender = sender.clone();
        async move {
            let _ = sender(
                RequestId::String("reconnect-elicitation".into()),
                Elicitation::OpenAiForm {
                    meta: None,
                    message: "reconnect".to_string(),
                    requested_schema: serde_json::json!({ "type": "object" }),
                },
            )
            .await
            .map_err(StartupOutcomeError::from)?;
            reconnect_finished.notify_one();
            Err(StartupOutcomeError::Failed {
                error: "test reconnect completed".to_string(),
                is_authentication_required: false,
                is_ema_authentication_required: false,
                shutdown: None,
            })
        }
        .boxed()
        .shared()
    });
    let async_client = AsyncManagedClient {
        ema_authority: None,
        client: async {
            Err(StartupOutcomeError::Failed {
                error: "initial startup failed".to_string(),
                is_authentication_required: false,
                is_ema_authentication_required: false,
                shutdown: None,
            })
        }
        .boxed()
        .shared(),
        is_codex_apps_mcp_server: true,
        server_capabilities: Arc::new(std::sync::Mutex::new(None)),
        cached_server_info: None,
        codex_apps_tools_cache_context: None,
        tool_catalog_cache_context: None,
        startup_complete: Arc::new(AtomicBool::new(true)),
        initialized_apps: None,
        connection_slot: None,
        startup_reconnect: Some(Arc::new(CodexAppsStartupReconnect::new(
            reconnect_factory,
            "test".to_string(),
            request_router.clone(),
        ))),
        cancel_token: CancellationToken::new(),
        request_router,
    };
    let lease = crate::connection_pool::McpConnectionLease::from(async_client);
    let (route, manager, events) =
        route_with_events(AskForApproval::OnRequest, PermissionProfile::default());

    lease.reconnect_failed_startup(Arc::clone(&route)).await;
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
    let EventMsg::ElicitationRequest(request) = event.msg else {
        anyhow::bail!("expected reconnect elicitation, got {:?}", event.msg);
    };
    let codex_protocol::mcp::RequestId::String(id) = request.id else {
        anyhow::bail!("expected string elicitation id");
    };
    manager
        .resolve(
            request.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(1), reconnect_finished.notified()).await?;
    Ok(())
}

#[tokio::test]
async fn initial_startup_elicitation_uses_the_starting_session_route() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let (route, manager, events) =
        route_with_events(AskForApproval::OnRequest, PermissionProfile::default());
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route,
        |request_router| {
            let sender = request_router.make_sender("test".to_string(), false);
            client_with_startup(
                request_router,
                async move {
                    let _ = sender(
                        RequestId::String("startup-elicitation".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "startup".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                    .map_err(StartupOutcomeError::from)?;
                    Err(StartupOutcomeError::Failed {
                        error: "test startup completed".to_string(),
                        is_authentication_required: false,
                        is_ema_authentication_required: false,
                        shutdown: None,
                    })
                }
                .boxed()
                .shared(),
            )
        },
    );
    let startup = {
        let route = Arc::clone(&route);
        tokio::spawn(async move { lease.await_current_startup(route).await })
    };

    let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
    let EventMsg::ElicitationRequest(request) = event.msg else {
        anyhow::bail!("expected startup elicitation, got {:?}", event.msg);
    };
    let codex_protocol::mcp::RequestId::String(id) = request.id else {
        anyhow::bail!("expected string elicitation id");
    };
    manager
        .resolve(
            request.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    let error = match startup.await? {
        Ok(_) => anyhow::bail!("test startup should end with its sentinel failure"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("test startup completed"));
    Ok(())
}

#[test]
fn compatible_acquisitions_reuse_one_client() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let second = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(first.ptr_eq(&second));
}

#[test]
fn different_server_names_do_not_reuse_one_client() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire_named(
        "first".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let second = pool.acquire_named(
        "second".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(!first.ptr_eq(&second));
}

#[test]
fn incompatible_startup_inputs_do_not_reuse_clients() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let different_command = pool.acquire(
        identity("other-server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let different_cwd = pool.acquire(
        identity("server", "/two"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(!first.ptr_eq(&different_command));
    assert!(!first.ptr_eq(&different_cwd));
}

#[test]
fn environment_configuration_is_part_of_connection_identity() {
    let mut first_literal_environment = root_pool_config("server");
    let mut second_literal_environment = root_pool_config("server");
    let mut environment_binding = root_pool_config("server");
    let McpServerTransportConfig::Stdio { env, .. } = &mut first_literal_environment.transport
    else {
        panic!("test server should use stdio");
    };
    *env = Some(HashMap::from([("TOKEN".to_string(), "one".to_string())]));
    let McpServerTransportConfig::Stdio { env, .. } = &mut second_literal_environment.transport
    else {
        panic!("test server should use stdio");
    };
    *env = Some(HashMap::from([("TOKEN".to_string(), "two".to_string())]));
    let McpServerTransportConfig::Stdio { env_vars, .. } = &mut environment_binding.transport
    else {
        panic!("test server should use stdio");
    };
    env_vars.push(codex_config::McpServerEnvVar::Name("TOKEN".to_string()));

    assert!(
        identity_with(
            first_literal_environment,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) != identity_with(
            second_literal_environment,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
    assert!(
        identity("server", "/one")
            != identity_with(
                environment_binding,
                "/one",
                &Ok(None),
                ConnectorRuntimeContextKey::personal(
                    /*account_id*/ None, /*chatgpt_user_id*/ None
                ),
                /*has_runtime_auth*/ false,
                &ElicitationCapability::default(),
                ClientMcpExtensions::default(),
            )
    );
}

#[test]
fn configured_environment_id_is_part_of_connection_identity() {
    let first = root_pool_config("server");
    let mut second = root_pool_config("server");
    second.environment_id = "remote".to_string();

    assert!(
        identity_with(
            first,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) != identity_with(
            second,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
}

#[test]
fn equivalent_map_configurations_have_one_connection_identity() {
    let mut first = root_pool_config("server");
    let mut second = root_pool_config("server");
    let McpServerTransportConfig::Stdio { env: first_env, .. } = &mut first.transport else {
        panic!("test server should use stdio");
    };
    let McpServerTransportConfig::Stdio {
        env: second_env, ..
    } = &mut second.transport
    else {
        panic!("test server should use stdio");
    };
    *first_env = Some(HashMap::from([
        ("ALPHA".to_string(), "one".to_string()),
        ("BETA".to_string(), "two".to_string()),
    ]));
    *second_env = Some(HashMap::from([
        ("BETA".to_string(), "two".to_string()),
        ("ALPHA".to_string(), "one".to_string()),
    ]));

    assert!(
        identity_with(
            first,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) == identity_with(
            second,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
}

#[test]
fn authorization_and_client_capabilities_are_part_of_connection_identity() {
    let default_capability = ElicitationCapability::default();
    let form_capability = ElicitationCapability::new().with_form(FormElicitationCapability::new());
    let baseline = identity_with(
        root_pool_config("server"),
        "/one",
        &Ok(None),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &default_capability,
        ClientMcpExtensions::default(),
    );

    for incompatible in [
        identity_with(
            root_pool_config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ true,
            &default_capability,
            ClientMcpExtensions::default(),
        ),
        identity_with(
            root_pool_config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ false,
            &form_capability,
            ClientMcpExtensions::default(),
        ),
        identity_with(
            root_pool_config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ false,
            &default_capability,
            form_extensions(),
        ),
        identity_with(
            root_pool_config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::workspace(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ false,
            &default_capability,
            ClientMcpExtensions::default(),
        ),
    ] {
        assert!(baseline != incompatible);
    }
}

#[test]
fn effective_runtime_credentials_are_part_of_connection_identity() {
    let first = CodexAuth::from_api_key("first");
    let second = CodexAuth::from_api_key("second");

    assert!(identity_with_auth(&first) != identity_with_auth(&second));
}

#[tokio::test]
async fn resolved_execution_environment_is_part_of_connection_identity() {
    let first_environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let second_environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let first = Ok(Some(first_environment));
    let second = Ok(Some(second_environment));

    assert!(
        identity_with(
            root_pool_config("server"),
            "/one",
            &first,
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) != identity_with(
            root_pool_config("server"),
            "/one",
            &second,
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
}

#[tokio::test]
async fn connection_identity_retains_its_resolved_environment() {
    let environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let weak = Arc::downgrade(&environment);
    let identity = identity_with(
        root_pool_config("server"),
        "/one",
        &Ok(Some(Arc::clone(&environment))),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    );
    drop(environment);

    assert!(weak.upgrade().is_some());
    drop(identity);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn dropping_last_lease_releases_connection_identity() {
    let pool = McpConnectionPool::default();
    let environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let weak = Arc::downgrade(&environment);
    let identity = identity_with(
        root_pool_config("server"),
        "/one",
        &Ok(Some(Arc::clone(&environment))),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    );
    let lease = pool.acquire_named(
        "test".to_string(),
        identity,
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    drop(environment);

    assert!(weak.upgrade().is_some());
    drop(lease);
    assert!(weak.upgrade().is_none());
}

#[test]
fn approval_policy_and_permission_profile_do_not_prevent_reuse() {
    let pool = McpConnectionPool::default();
    let never = route_with(AskForApproval::Never, PermissionProfile::Disabled);
    let on_request = route_with(AskForApproval::OnRequest, PermissionProfile::default());
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &never,
        client,
    );
    let second = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &on_request,
        client,
    );

    assert!(first.ptr_eq(&second));
}

#[test]
fn replacement_becomes_preferred_without_revoking_old_lease() {
    let pool = McpConnectionPool::default();
    let old = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &route(),
        client,
    );
    let reused = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(old.ptr_eq(&replacement));
    assert!(replacement.ptr_eq(&reused));
    assert!(!old.is_connection_cancelled());
}

#[tokio::test]
async fn ready_binding_capture_does_not_wait_for_a_sibling_request_route() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let worker_route = route();
    let root_route = route();
    let managed = test_managed_client("shared").await?;
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &worker_route,
        move |request_router| ready_client(request_router, managed.clone()),
    );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let worker = tokio::spawn({
        let lease = lease.clone();
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        async move {
            lease
                .run(worker_route, move |client| async move {
                    client.client().await.expect("ready worker client");
                    entered.notify_one();
                    release.notified().await;
                })
                .await
        }
    });
    entered.notified().await;
    let (binding, _, _) = tokio::time::timeout(
        Duration::from_secs(1),
        lease.capture_ready_client_and_tools(
            root_route,
            Arc::new(crate::mcp::ToolPluginContext::default()),
            /*tool_timeout*/ None,
        ),
    )
    .await?
    .expect("cached binding must not wait for the worker");
    let request = binding.run(|_| async { Ok(()) });
    tokio::pin!(request);
    assert!(
        futures::poll!(&mut request).is_pending(),
        "actual requests still need the route"
    );
    release.notify_one();
    worker.await??;
    request.await?;
    Ok(())
}

#[tokio::test]
async fn pooled_capture_retains_raw_catalog_snapshot_when_connector_tools_are_filtered()
-> anyhow::Result<()> {
    let raw_tools = ["enabled", "disabled"]
        .into_iter()
        .map(|connector| crate::tools::ToolInfo {
            server_name: crate::mcp::CODEX_APPS_MCP_SERVER_NAME.to_string(),
            supports_parallel_tool_calls: false,
            server_origin: None,
            callable_name: connector.to_string(),
            callable_namespace: connector.to_string(),
            namespace_description: None,
            tool: rmcp::model::Tool::new(
                connector.to_string(),
                "raw connector tool".to_string(),
                Arc::new(rmcp::model::JsonObject::default()),
            ),
            openai_file_input_optional_fields: Default::default(),
            connector_id: Some(connector.to_string()),
            connector_name: None,
            plugin_display_names: Vec::new(),
        })
        .collect::<Vec<_>>();
    let catalog = Arc::new(crate::client_tool_catalog::ClientToolCatalog::new(
        raw_tools.clone(),
        /*meta*/ None,
        /*updates*/ None,
    ));
    let mut managed = test_managed_client("filtered Apps").await?;
    managed.tool_catalog = Arc::clone(&catalog);
    let codex_home = tempfile::tempdir()?;
    let cache_context = codex_connectors::ConnectorRuntimeManager::default().context(
        codex_home.path().to_path_buf(),
        ConnectorRuntimeContextKey::personal(None, None),
    );
    let _ = cache_context.publish_runtime_if_newest_accepted(
        cache_context.begin_fetch(codex_connectors::ConnectorRuntimeFetchSource::HardRefresh),
        &managed.server_info,
        raw_tools.clone(),
        /*tool_list_meta*/ None,
    );
    let mut config = crate::mcp::tests::test_mcp_config(codex_home.path().to_path_buf());
    config.connector_snapshot = codex_connectors::ConnectorSnapshot::from_plugin_sources(
        [],
        &[],
        HashSet::from(["disabled".to_string()]),
    );
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        move |request_router| {
            let mut client = ready_client(request_router, managed.clone());
            client.is_codex_apps_mcp_server = true;
            client.codex_apps_tools_cache_context = Some(cache_context.clone());
            client
        },
    );
    // Cached Apps tools do not establish a ready physical client for binding capture.
    lease
        .await_current_startup(Arc::clone(&session_route))
        .await?;
    let (_binding, snapshot, visible_tools) = lease
        .capture_ready_client_and_tools(
            session_route,
            Arc::new(crate::mcp::tool_plugin_context(&config)),
            /*tool_timeout*/ None,
        )
        .await
        .expect("ready Apps catalog should be captured");

    assert_eq!(snapshot.tools.as_ref(), raw_tools.as_slice());
    assert_eq!(visible_tools, vec![raw_tools[0].clone()]);
    let (cached_visible_tools, measured_bytes) = lease
        .prepared_cached_tools(
            None,
            &crate::mcp::tool_plugin_context(&config),
            |_| true,
            |tools| {
                assert_eq!(tools, raw_tools.as_slice());
                crate::connection_manager::catalog_telemetry::tool_definition_json_bytes(
                    tools.iter().map(|tool| &tool.tool),
                    true,
                )
            },
        )
        .expect("raw cached Apps catalog should be measured before filtering");
    let serialized_bytes = |tools: &[crate::tools::ToolInfo]| {
        tools
            .iter()
            .map(|tool| {
                serde_json::to_vec(&tool.tool)
                    .expect("serialized tool")
                    .len()
            })
            .sum::<usize>()
    };
    assert_eq!(cached_visible_tools, visible_tools);
    assert_eq!(measured_bytes, serialized_bytes(&raw_tools));
    assert!(measured_bytes > serialized_bytes(&cached_visible_tools));
    assert_eq!(
        catalog
            .run_with_snapshot(&snapshot, || async { "captured raw catalog" })
            .await,
        Some("captured raw catalog")
    );

    catalog
        .refresh(|| async { Ok((raw_tools, None, ())) }, |_, _, ()| {})
        .await?;
    assert_eq!(
        catalog
            .run_with_snapshot(&snapshot, || async { "stale catalog" })
            .await,
        None
    );
    Ok(())
}

// Both session routes share one physical client and catalog, so these tests exercise
// the same attribution and catalog locks as calls competing with a tools refresh.
struct SharedPreparedCall {
    prepared: PreparedMcpCall,
    catalog: Arc<ClientToolCatalog>,
    other_lease: McpConnectionLease,
    other_route: Arc<McpSessionRoute>,
    other_binding: McpPooledBindingClient,
}

async fn shared_prepared_call() -> anyhow::Result<SharedPreparedCall> {
    let tool = crate::tools::ToolInfo {
        server_name: "server".to_string(),
        supports_parallel_tool_calls: false,
        server_origin: None,
        callable_name: "search".to_string(),
        callable_namespace: "server".to_string(),
        namespace_description: None,
        tool: rmcp::model::Tool::new(
            "search".to_string(),
            "shared catalog tool".to_string(),
            Arc::new(rmcp::model::JsonObject::default()),
        ),
        openai_file_input_optional_fields: Default::default(),
        connector_id: None,
        connector_name: None,
        plugin_display_names: Vec::new(),
    };
    let catalog = Arc::new(ClientToolCatalog::new(
        vec![tool.clone()],
        /*meta*/ None,
        /*updates*/ None,
    ));
    let mut managed = test_managed_client("shared prepared call").await?;
    managed.tool_catalog = Arc::clone(&catalog);
    let pool = McpConnectionPool::default();
    let call_route = route();
    let other_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &call_route,
        move |request_router| ready_client(request_router, managed.clone()),
    );
    let other_lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &other_route,
        |_| panic!("the second session must reuse the physical connection"),
    );
    assert_eq!(lease.connection_id(), other_lease.connection_id());
    let plugin_context = Arc::new(crate::mcp::ToolPluginContext::default());
    let (binding, snapshot, _) = lease
        .capture_ready_client_and_tools(
            call_route,
            Arc::clone(&plugin_context),
            /*tool_timeout*/ None,
        )
        .await
        .expect("ready call binding");
    let (other_binding, _, _) = other_lease
        .capture_ready_client_and_tools(
            Arc::clone(&other_route),
            plugin_context,
            /*tool_timeout*/ None,
        )
        .await
        .expect("ready sibling binding");
    let mut config = crate::mcp::tests::test_mcp_config(std::env::temp_dir());
    config
        .server_permission_profiles
        .insert("server".to_string(), config.permission_profile.clone());
    let prepared = PreparedMcpCall::new(
        Arc::new(McpConnectionSet::empty(/*prefix_mcp_tool_names*/ true)),
        binding,
        Arc::new(config),
        snapshot,
        tool,
        McpServerMetadata {
            environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
            pollutes_memory: false,
            origin: None,
            supports_parallel_tool_calls: false,
            default_tools_approval_mode: None,
            tool_approval_modes: HashMap::new(),
        },
        /*plugin_id*/ None,
        /*selected_plugin_server*/ false,
    )
    .expect("test server has permission authority");
    Ok(SharedPreparedCall {
        prepared,
        catalog,
        other_lease,
        other_route,
        other_binding,
    })
}

#[tokio::test]
async fn prepared_call_waits_for_refresh_route_before_locking_catalog() -> anyhow::Result<()> {
    let SharedPreparedCall {
        prepared,
        catalog,
        other_lease,
        other_route,
        ..
    } = shared_prepared_call().await?;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let refresh = tokio::spawn(async move {
        other_lease
            .run_mcp_request(other_route, move |_| async move {
                catalog
                    .refresh(
                        || async move {
                            entered_tx.send(()).expect("refresh observer is alive");
                            release_rx.await?;
                            Ok((Vec::new(), None, ()))
                        },
                        |_, _, ()| {},
                    )
                    .await
            })
            .await
    });
    entered_rx.await?;

    let preparation_ran = AtomicBool::new(false);
    let mut call = Box::pin(prepared.call_with_preparation(
        /*requested_timeout*/ None,
        || async {
            preparation_ran.store(true, Ordering::SeqCst);
            Err(anyhow::anyhow!("stale calls must not run preparation"))
        },
    ));
    assert!(futures::poll!(&mut call).is_pending());
    assert!(!preparation_ran.load(Ordering::SeqCst));

    release_tx.send(()).expect("refresh is waiting for release");
    refresh.await??;
    let error = call
        .await
        .expect_err("refresh invalidates the prepared catalog");
    assert!(
        error
            .to_string()
            .contains("catalog changed after `server/search` was prepared")
    );
    assert!(!preparation_ran.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn cancelling_prepared_call_releases_route_and_catalog_locks() -> anyhow::Result<()> {
    let SharedPreparedCall {
        prepared,
        catalog,
        other_binding,
        ..
    } = shared_prepared_call().await?;
    let preparation_ran = AtomicBool::new(false);
    let mut call = Box::pin(prepared.call_with_preparation(
        /*requested_timeout*/ None,
        || async {
            preparation_ran.store(true, Ordering::SeqCst);
            std::future::pending().await
        },
    ));
    assert!(futures::poll!(&mut call).is_pending());
    assert!(preparation_ran.load(Ordering::SeqCst));

    let mut other_route = Box::pin(other_binding.acquire_route());
    assert!(futures::poll!(&mut other_route).is_pending());
    let mut refresh =
        Box::pin(catalog.refresh(|| async { Ok((Vec::new(), None, ())) }, |_, _, ()| {}));
    assert!(futures::poll!(&mut refresh).is_pending());

    drop(call);
    let std::task::Poll::Ready(active_route) = futures::poll!(&mut other_route) else {
        panic!("dropping preparation must release its session route");
    };
    let _active_route = active_route?;
    let std::task::Poll::Ready(refreshed) = futures::poll!(&mut refresh) else {
        panic!("dropping preparation must release catalog authority");
    };
    refreshed?;
    Ok(())
}

#[tokio::test]
async fn captured_binding_retains_its_exact_generation_across_replacement() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let old_meta = Arc::new(serde_json::json!({"generation": "old"}));
    let replacement_meta = Arc::new(serde_json::json!({"generation": "replacement"}));
    let old_rmcp =
        Arc::new(RmcpClient::new_in_process_client(Arc::new(TestInProcessTransportFactory)).await?);
    let old_managed = ManagedClient {
        connection_sharing: Default::default(),
        shared_elicitation: None,
        _auth_change_notifications: None,
        client: Arc::clone(&old_rmcp),
        server_info: McpServerInfo {
            name: "old".to_string(),
            title: None,
            version: "1".to_string(),
            description: None,
            icons: None,
            website_url: None,
        },
        tool_catalog: Arc::new(crate::client_tool_catalog::ClientToolCatalog::new(
            Vec::new(),
            Some(Arc::clone(&old_meta)),
            /*updates*/ None,
        )),
        tool_timeout: None,
        server_instructions: None,
        server_supports_sandbox_state_meta_capability: false,
        client_supports_network_policy_meta_capability: false,
        codex_apps_tools_cache_context: None,
    };
    let old = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        move |request_router| ready_client(request_router, old_managed.clone()),
    );
    let old_cancelled = old.current()?.client.cancel_token.clone();
    let (binding, old_snapshot, _) = old
        .capture_ready_client_and_tools(
            Arc::clone(&session_route),
            Arc::new(crate::mcp::ToolPluginContext::default()),
            /*tool_timeout*/ None,
        )
        .await
        .expect("ready generation should be captured");

    let replacement_rmcp =
        Arc::new(RmcpClient::new_in_process_client(Arc::new(TestInProcessTransportFactory)).await?);
    let replacement_managed = ManagedClient {
        connection_sharing: Default::default(),
        shared_elicitation: None,
        _auth_change_notifications: None,
        client: replacement_rmcp,
        server_info: McpServerInfo {
            name: "replacement".to_string(),
            title: None,
            version: "1".to_string(),
            description: None,
            icons: None,
            website_url: None,
        },
        tool_catalog: Arc::new(crate::client_tool_catalog::ClientToolCatalog::new(
            Vec::new(),
            Some(Arc::clone(&replacement_meta)),
            /*updates*/ None,
        )),
        tool_timeout: None,
        server_instructions: None,
        server_supports_sandbox_state_meta_capability: false,
        client_supports_network_policy_meta_capability: false,
        codex_apps_tools_cache_context: None,
    };
    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &session_route,
        move |request_router| ready_client(request_router, replacement_managed.clone()),
    );
    let (_, replacement_snapshot, _) = replacement
        .capture_ready_client_and_tools(
            Arc::clone(&session_route),
            Arc::new(crate::mcp::ToolPluginContext::default()),
            /*tool_timeout*/ None,
        )
        .await
        .expect("replacement generation should be captured");
    assert!(Arc::ptr_eq(
        old_snapshot.meta.as_ref().expect("original metadata"),
        &old_meta
    ));
    assert!(Arc::ptr_eq(
        replacement_snapshot
            .meta
            .as_ref()
            .expect("replacement metadata"),
        &replacement_meta,
    ));

    assert!(!old_cancelled.is_cancelled());
    binding
        .run(move |managed| async move {
            assert!(Arc::ptr_eq(&managed.client, &old_rmcp));
            let pinned_snapshot = managed.tool_catalog.read(|snapshot| snapshot).await;
            assert!(Arc::ptr_eq(
                pinned_snapshot.meta.as_ref().expect("pinned metadata"),
                &old_meta
            ));
            Ok(())
        })
        .await?;
    drop(binding);
    assert!(old_cancelled.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn abandoning_a_bound_operation_retires_the_captured_generation() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let managed = test_managed_client("abandoned").await?;
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        move |request_router| ready_client(request_router, managed.clone()),
    );
    let old_connection_id = lease.connection_id();
    let old_cancelled = lease.connection_cancel_token();
    let (binding, _, _) = lease
        .capture_ready_client_and_tools(
            Arc::clone(&session_route),
            Arc::new(crate::mcp::ToolPluginContext::default()),
            /*tool_timeout*/ None,
        )
        .await
        .expect("ready generation should be captured");
    let operation_started = Arc::new(Notify::new());
    let operation_started_for_task = Arc::clone(&operation_started);
    let operation = tokio::spawn(async move {
        binding
            .run(move |_| async move {
                operation_started_for_task.notify_one();
                std::future::pending::<anyhow::Result<()>>().await
            })
            .await
    });
    operation_started.notified().await;
    operation.abort();
    assert!(
        operation
            .await
            .expect_err("bound operation should be cancelled")
            .is_cancelled()
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        while !old_cancelled.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_ne!(lease.connection_id(), old_connection_id);
    Ok(())
}

#[test]
fn sequential_replacements_keep_every_lease_on_one_generation() {
    let pool = McpConnectionPool::default();
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let first_token = root.connection_cancel_token();

    let refreshed_root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &route(),
        client,
    );
    let second_token = refreshed_root.connection_cancel_token();
    assert!(first_token.is_cancelled());

    let refreshed_child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &route(),
        client,
    );
    assert!(second_token.is_cancelled());
    let current = root.connection_id();
    assert_eq!(child.connection_id(), current);
    assert_eq!(refreshed_root.connection_id(), current);
    assert_eq!(refreshed_child.connection_id(), current);
}

#[test]
fn simultaneous_replacements_converge_every_lease_on_one_generation() {
    let pool = McpConnectionPool::default();
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let barrier = Arc::new(Barrier::new(2));
    // Both refresh threads must reach the barrier before either is joined.
    #[allow(clippy::needless_collect)]
    let refreshes = (0..2)
        .map(|_| {
            let pool = pool.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                pool.acquire(
                    identity("server", "/one"),
                    McpConnectionPoolMode::Replace,
                    &route(),
                    client,
                )
            })
        })
        .collect::<Vec<_>>();
    let refreshes = refreshes
        .into_iter()
        .map(|refresh| refresh.join().expect("refresh thread should finish"))
        .collect::<Vec<_>>();

    let current = root.connection_id();
    assert_eq!(child.connection_id(), current);
    assert!(
        refreshes
            .iter()
            .all(|refresh| refresh.connection_id() == current)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_operations_preserve_each_callers_w3c_trace_context() -> anyhow::Result<()> {
    let provider = SdkTracerProvider::builder().build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("pooled-mcp-trace-test")));
    // This current-thread runtime keeps spawned operations on the subscriber's thread.
    let _subscriber = subscriber.set_default();
    let pool = McpConnectionPool::default();
    let root_route = route();
    let child_route = route();
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        client,
    );
    let child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &child_route,
        client,
    );
    let connection_id = root.connection_id();
    assert_eq!(child.connection_id(), connection_id);

    let root_trace = async {
        let expected = codex_otel::current_span_w3c_trace_context().expect("root caller trace");
        let (actual_connection, actual_trace) = root
            .run(root_route, |client| async move {
                tokio::task::yield_now().await;
                (
                    client.connection_id(),
                    codex_otel::current_span_w3c_trace_context(),
                )
            })
            .await?;
        assert_eq!(actual_connection, connection_id);
        assert_eq!(actual_trace, Some(expected.clone()));
        Ok::<_, anyhow::Error>(expected)
    }
    .instrument(tracing::info_span!("root_mcp_caller"))
    .await?;
    let child_trace = async {
        let expected = codex_otel::current_span_w3c_trace_context().expect("child caller trace");
        let (actual_connection, actual_trace) = child
            .run_mcp_request(child_route, |client| async move {
                tokio::task::yield_now().await;
                Ok((
                    client.connection_id(),
                    codex_otel::current_span_w3c_trace_context(),
                ))
            })
            .await?;
        assert_eq!(actual_connection, connection_id);
        assert_eq!(actual_trace, Some(expected.clone()));
        Ok::<_, anyhow::Error>(expected)
    }
    .instrument(tracing::info_span!("child_mcp_caller"))
    .await?;
    assert!(root_trace.traceparent.is_some());
    assert!(child_trace.traceparent.is_some());
    assert_ne!(root_trace.traceparent, child_trace.traceparent);
    assert!(codex_otel::current_span_w3c_trace_context().is_none());
    Ok(())
}

#[tokio::test]
async fn queued_operation_converges_on_the_replacement_generation() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let first_route = route();
    let queued_route = route();
    let replacement_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &first_route,
        client,
    );
    let queued_lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &queued_route,
        client,
    );
    let active = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let active_for_operation = Arc::clone(&active);
    let release_for_operation = Arc::clone(&release);
    let first = {
        let lease = lease.clone();
        tokio::spawn(async move {
            lease
                .run(first_route, move |_| async move {
                    active_for_operation.notify_one();
                    release_for_operation.notified().await;
                })
                .await
        })
    };
    active.notified().await;
    let queued = tokio::spawn(async move {
        queued_lease
            .run(queued_route, |client| async move { client.connection_id() })
            .await
    });
    tokio::task::yield_now().await;

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &replacement_route,
        client,
    );
    let replacement_id = replacement.connection_id();
    release.notify_one();

    assert_eq!(queued.await??, replacement_id);
    first.await??;
    Ok(())
}

#[tokio::test]
async fn closing_an_active_route_retires_without_wedging_a_sibling() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let first_route = route();
    let second_route = route();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &first_route,
        client,
    );
    let second = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &second_route,
        client,
    );
    let started = Arc::new(tokio::sync::Notify::new());
    let operation = {
        let started = Arc::clone(&started);
        let first_route = Arc::clone(&first_route);
        let operation_lease = first.clone();
        tokio::spawn(async move {
            operation_lease
                .run(first_route, move |_| async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        })
    };
    started.notified().await;
    first.unregister_route(&first_route);
    assert!(!first.release());
    assert!(operation.await?.is_err());

    let value = second.run(second_route, |_| async move { 7 }).await?;
    assert_eq!(value, 7);
    Ok(())
}

#[tokio::test]
async fn closing_an_active_route_returns_before_unresponsive_shutdown() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        |request_router| {
            client_with_startup(request_router, std::future::pending().boxed().shared())
        },
    );
    let started = Arc::new(Notify::new());
    let operation = {
        let started = Arc::clone(&started);
        let session_route = Arc::clone(&session_route);
        tokio::spawn(async move {
            lease
                .run(session_route, move |_| async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        })
    };
    started.notified().await;
    session_route.close();

    let result = tokio::time::timeout(Duration::from_secs(1), operation).await??;
    assert_eq!(
        result
            .expect_err("the closed route should stop the operation")
            .to_string(),
        "MCP session route closed"
    );
    Ok(())
}

#[tokio::test]
async fn failure_retirement_replaces_an_exclusive_session_connection() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        client,
    );
    let old_connection = lease.current()?;
    let old_token = old_connection.client.cancel_token.clone();
    let old_id = lease.connection_id();
    McpPooledClient {
        connection: old_connection,
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    }
    .retire_after_failure();

    assert!(old_token.is_cancelled());
    assert_ne!(lease.connection_id(), old_id);
    let result = lease.run(session_route, |_| async move { 7 }).await?;
    assert_eq!(result, 7);
    Ok(())
}

#[tokio::test]
async fn failure_retirement_does_not_replace_after_the_final_session_releases() {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let create_count = Arc::new(AtomicUsize::new(0));
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );
    let old_connection = lease.current().expect("connection should be live");
    let pooled_client = McpPooledClient {
        connection: old_connection,
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    };

    lease.unregister_route(&session_route);
    assert!(lease.release());
    pooled_client.retire_after_failure();

    assert_eq!(create_count.load(Ordering::SeqCst), 1);
}

#[test]
fn session_lease_tracks_sibling_ownership_but_not_operation_clones() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let operation_clone = first.clone();
    assert!(first.is_exclusive());

    let sibling = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    assert!(!first.is_exclusive());
    assert!(!sibling.is_exclusive());

    drop(sibling);
    assert!(first.is_exclusive());
    drop(operation_clone);
    assert!(first.is_exclusive());
}

#[test]
fn repeated_child_acquire_and_release_preserves_one_live_root_connection() {
    let pool = McpConnectionPool::default();
    let root_route = route();
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        client,
    );
    let connection_id = root.connection_id();

    for _ in 0..100 {
        let child_route = route();
        let child = pool.acquire(
            identity("server", "/one"),
            McpConnectionPoolMode::Reuse,
            &child_route,
            client,
        );
        assert_eq!(child.connection_id(), connection_id);
        child.unregister_route(&child_route);
        assert!(!child.release());
    }

    assert!(root.is_exclusive());
    assert_eq!(root.connection_id(), connection_id);
    assert!(!root.is_connection_cancelled());
}

#[test]
fn last_lease_drop_removes_the_client() {
    let pool = McpConnectionPool::default();
    let cancel_token = {
        let leased = pool.acquire(
            identity("server", "/one"),
            McpConnectionPoolMode::Reuse,
            &route(),
            client,
        );
        leased.connection_cancel_token()
    };
    assert!(cancel_token.is_cancelled());

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    assert!(!replacement.is_connection_cancelled());
}

#[test]
fn released_but_retained_lease_is_not_reused() {
    let pool = McpConnectionPool::default();
    let create_count = Arc::new(AtomicUsize::new(0));
    let retained = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );
    assert!(retained.release());

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );

    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    assert!(!retained.ptr_eq(&replacement));
}

#[test]
fn simultaneous_acquisition_starts_one_client() {
    const THREADS: usize = 8;
    let pool = McpConnectionPool::default();
    let barrier = Arc::new(Barrier::new(THREADS));
    let create_count = Arc::new(AtomicUsize::new(0));
    // Every acquisition thread must reach the barrier before the first join.
    #[allow(clippy::needless_collect)]
    let handles = (0..THREADS)
        .map(|_| {
            let pool = pool.clone();
            let barrier = Arc::clone(&barrier);
            let create_count = Arc::clone(&create_count);
            std::thread::spawn(move || {
                barrier.wait();
                pool.acquire(
                    identity("server", "/one"),
                    McpConnectionPoolMode::Reuse,
                    &route(),
                    move |request_router| {
                        create_count.fetch_add(1, Ordering::SeqCst);
                        client(request_router)
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    let clients = handles
        .into_iter()
        .map(|handle| handle.join().expect("acquisition thread should finish"))
        .collect::<Vec<_>>();

    assert_eq!(create_count.load(Ordering::SeqCst), 1);
    assert!(clients.iter().all(|client| client.ptr_eq(&clients[0])));
}

#[tokio::test]
async fn cancelling_a_dispatched_request_replaces_the_connection_for_siblings() -> anyhow::Result<()>
{
    let pool = McpConnectionPool::default();
    let root_route = route();
    let child_route = route();
    let create_count = Arc::new(AtomicUsize::new(0));
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );
    let child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &child_route,
        client,
    );
    let original_connection = root.connection_id();
    let original_cancel = root.connection_cancel_token();
    let request_started = Arc::new(Notify::new());

    let caller = {
        let root = root.clone();
        let request_started = Arc::clone(&request_started);
        tokio::spawn(async move {
            root.run(root_route, move |client| async move {
                request_started.notify_one();
                client.cancel_token.cancelled().await;
            })
            .await
        })
    };
    request_started.notified().await;
    caller.abort();

    tokio::time::timeout(Duration::from_secs(1), original_cancel.cancelled()).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while child.connection_id() == original_connection {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    child
        .run(child_route, |_| async move {})
        .await
        .expect("sibling request should use the replacement connection");
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn superseded_failure_replacement_does_not_start_after_its_final_check() -> anyhow::Result<()>
{
    let pool = McpConnectionPool::default();
    let root_route = route();
    let refresh_route = route();
    let create_count = Arc::new(AtomicUsize::new(0));
    let stale_startup_polled = Arc::new(AtomicBool::new(false));
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        {
            let create_count = Arc::clone(&create_count);
            let stale_startup_polled = Arc::clone(&stale_startup_polled);
            move |request_router| {
                let creation = create_count.fetch_add(1, Ordering::SeqCst);
                if creation == 1 {
                    let stale_startup_polled = Arc::clone(&stale_startup_polled);
                    client_with_startup(
                        request_router,
                        async move {
                            stale_startup_polled.store(true, Ordering::SeqCst);
                            std::future::pending().await
                        }
                        .boxed()
                        .shared(),
                    )
                } else {
                    client(request_router)
                }
            }
        },
    );
    let old_connection = root.current()?;
    let (failure_replacement, startup_route) = root
        .inner
        .slot
        .replace_if_current(&old_connection, Some(&root_route))
        .expect("the failed current generation should be replaced");
    let startup_reached = Arc::new(Notify::new());
    let release_startup = Arc::new(Notify::new());
    McpPooledClient {
        connection: failure_replacement,
        slot: Arc::downgrade(&root.inner.slot),
        route: startup_route,
    }
    .start_in_background_after_check({
        let startup_reached = Arc::clone(&startup_reached);
        let release_startup = Arc::clone(&release_startup);
        async move {
            startup_reached.notify_one();
            release_startup.notified().await;
        }
    });
    startup_reached.notified().await;

    let refreshed = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &refresh_route,
        client,
    );
    release_startup.notify_one();
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }

    assert!(root.ptr_eq(&refreshed));
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    assert!(!stale_startup_polled.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn route_close_retirement_bounds_unresponsive_shutdown() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        |request_router| {
            client_with_startup(request_router, std::future::pending().boxed().shared())
        },
    );
    let pooled_client = McpPooledClient {
        connection: lease.current()?,
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    };
    session_route.close();
    let retirement = tokio::spawn(async move {
        pooled_client.retire_after_route_close().await;
    });

    tokio::time::advance(Duration::from_secs(11)).await;
    retirement.await?;
    Ok(())
}

struct AdoptedRetirementPair {
    pool: McpConnectionPool,
    leases: [McpConnectionLease; 2],
    siblings: [McpConnectionLease; 2],
    routes: [Arc<McpSessionRoute>; 2],
    surviving_routes: [Arc<McpSessionRoute>; 2],
    starts: [Arc<AtomicUsize>; 2],
    server: TestServer,
    original: Arc<SharedConnection>,
}

async fn private_retirement_client(
    server: TestServer,
) -> Result<ManagedClient, StartupOutcomeError> {
    startup(
        ConnectionSharing::Private,
        Arc::new(AtomicUsize::new(1)),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        server,
    )(CancellationToken::new(), None)
    .await
}

async fn adopted_retirement_pair() -> anyhow::Result<AdoptedRetirementPair> {
    let pool = McpConnectionPool::default();
    let routes = [route(), route()];
    let surviving_routes = [route(), route()];
    let starts = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    let server = TestServer::default();
    let mut legacy_key = key(&context(), &config());
    legacy_key.identity = legacy_key
        .identity
        .with_effective_protocol_mode(Some(crate::McpProtocolMode::Legacy));
    let mut discover_key = legacy_key.clone();
    discover_key.identity = discover_key
        .identity
        .with_effective_protocol_mode(Some(crate::McpProtocolMode::DiscoverLocalStdio));
    let legacy = pool
        .acquire_named_with_ema_startup_route(
            legacy_key.clone(),
            McpConnectionPoolMode::Reuse,
            &routes[0],
            None,
            {
                let starts = Arc::clone(&starts[0]);
                let server = server.clone();
                move |router| {
                    starts.fetch_add(1, Ordering::SeqCst);
                    client_with_startup(
                        router,
                        private_retirement_client(server.clone()).boxed().shared(),
                    )
                }
            },
        )
        .0;
    legacy.await_current_startup(Arc::clone(&routes[0])).await?;
    let original = legacy.current()?;
    let fallback = legacy
        .prepare_legacy_fallback(&discover_key)
        .await
        .expect("real private Legacy fallback");
    let (release, waiting) = tokio::sync::oneshot::channel::<()>();
    let waiting = Arc::new(Mutex::new(Some(waiting)));
    let discover = pool
        .acquire_named_with_ema_startup_route(
            discover_key.clone(),
            McpConnectionPoolMode::Reuse,
            &routes[1],
            None,
            {
                let starts = Arc::clone(&starts[1]);
                let server = server.clone();
                move |router| {
                    if starts.fetch_add(1, Ordering::SeqCst) == 0 {
                        let waiting = waiting.lock().unwrap().take().unwrap();
                        let client = client_with_startup(
                            router,
                            async move {
                                waiting
                                    .await
                                    .expect("installed fallback before discovery fails");
                                Err(StartupOutcomeError::Failed {
                                    error: "discovery trial failed".to_owned(),
                                    is_authentication_required: false,
                                    is_ema_authentication_required: false,
                                    shutdown: None,
                                })
                            }
                            .boxed()
                            .shared(),
                        );
                        client.startup_complete.store(false, Ordering::Release);
                        client
                    } else {
                        client_with_startup(
                            router,
                            private_retirement_client(server.clone()).boxed().shared(),
                        )
                    }
                }
            },
        )
        .0;
    assert!(!legacy.ptr_eq(&discover));
    assert_ne!(legacy.connection_id(), discover.connection_id());
    discover.retain_on_failed_discovery(fallback);
    assert!(discover.may_use_legacy_fallback());
    release.send(()).unwrap();
    discover
        .await_current_startup(Arc::clone(&routes[1]))
        .await?;
    assert!(discover.retained_legacy());
    assert_eq!(legacy.connection_id(), discover.connection_id());
    let siblings = [legacy_key, discover_key]
        .into_iter()
        .enumerate()
        .map(|(i, key)| {
            pool.acquire_named_with_ema_startup_route(
                key,
                McpConnectionPoolMode::Reuse,
                &surviving_routes[i],
                None,
                |_| panic!("same-key lease must reuse its original factory"),
            )
            .0
        })
        .collect::<Vec<_>>();
    let [legacy_sibling, discover_sibling]: [McpConnectionLease; 2] =
        siblings.try_into().ok().unwrap();
    Ok(AdoptedRetirementPair {
        pool,
        leases: [legacy, discover],
        siblings: [legacy_sibling, discover_sibling],
        routes,
        surviving_routes,
        starts,
        server,
        original,
    })
}

impl AdoptedRetirementPair {
    async fn retire(&self, side: usize, cause: usize) -> anyhow::Result<()> {
        if cause == 1 {
            self.server
                .block_resource_listing
                .store(true, Ordering::Release);
            let error = self.leases[side]
                .run_mcp_request(Arc::clone(&self.routes[side]), |client| async move {
                    client
                        .client()
                        .await?
                        .client
                        .list_resources(None, Some(Duration::ZERO))
                        .await
                })
                .await
                .expect_err("real rmcp request timeout");
            assert!(codex_rmcp_client::is_connection_unusable(&error));
        } else {
            let started = Arc::new(Notify::new());
            let operation = {
                let lease = self.leases[side].clone();
                let route = Arc::clone(&self.routes[side]);
                let started = Arc::clone(&started);
                tokio::spawn(async move {
                    lease
                        .run(route, move |_| async move {
                            started.notify_one();
                            std::future::pending::<()>().await;
                        })
                        .await
                })
            };
            started.notified().await;
            if cause == 0 {
                operation.abort();
                assert!(operation.await.unwrap_err().is_cancelled());
            } else {
                self.routes[side].close();
                assert!(operation.await?.is_err());
            }
        }
        assert!(self.original.client.cancel_token.is_cancelled());
        Ok(())
    }

    async fn shutdown(&self) {
        for lease in self.leases.iter().chain(self.siblings.iter()) {
            lease.shutdown().await;
        }
        self.original.shutdown_tasks.close();
        self.original.shutdown_tasks.wait().await;
    }
}

#[tokio::test]
async fn adopted_legacy_request_retirement_recovers_each_live_logical_slot() -> anyhow::Result<()> {
    for side in 0..2 {
        for cause in 0..3 {
            let pair = adopted_retirement_pair().await?;
            pair.retire(side, cause).await?;
            for i in 0..2 {
                let id = pair.siblings[i]
                    .run_mcp_request(Arc::clone(&pair.surviving_routes[i]), |client| async move {
                        client.client().await?;
                        Ok(client.connection_id())
                    })
                    .await?;
                assert_ne!(id, pair.original.id, "side={side} cause={cause} slot={i}");
                assert_eq!(pair.starts[i].load(Ordering::SeqCst), 2);
            }
            assert_ne!(
                pair.leases[0].connection_id(),
                pair.leases[1].connection_id()
            );
            pair.shutdown().await;
        }
    }
    Ok(())
}

#[tokio::test]
async fn adopted_legacy_retirement_coordinates_same_key_recovery() -> anyhow::Result<()> {
    let pair = adopted_retirement_pair().await?;
    let decoy_route = route();
    let decoy = pair.pool.acquire(
        identity("incompatible", "/elsewhere"),
        McpConnectionPoolMode::Reuse,
        &decoy_route,
        client,
    );
    let decoy_id = decoy.connection_id();
    let started = Arc::new(Notify::new());
    let initiating = {
        let lease = pair.leases[0].clone();
        let route = Arc::clone(&pair.routes[0]);
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            lease
                .run(route, move |_| async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        })
    };
    started.notified().await;
    let calls = (0..16).map(|i| {
        let lease = if i % 2 == 0 {
            pair.leases[1].clone()
        } else {
            pair.siblings[1].clone()
        };
        let route = Arc::clone(&pair.surviving_routes[1]);
        async move {
            lease
                .run_mcp_request(route, |client| async move {
                    client.client().await?;
                    Ok(client.connection_id())
                })
                .await
        }
    });
    let mut calls = Box::pin(futures::future::join_all(calls));
    assert!(
        futures::poll!(&mut calls).is_pending(),
        "calls queue behind the other logical owner"
    );
    assert_eq!(pair.starts[1].load(Ordering::SeqCst), 1);
    initiating.abort();
    assert!(initiating.await.unwrap_err().is_cancelled());
    let ids = calls
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert!(
        ids.iter()
            .all(|id| *id == ids[0] && *id != pair.original.id)
    );
    assert_eq!(pair.starts[1].load(Ordering::SeqCst), 2);
    assert_eq!(decoy.connection_id(), decoy_id);
    assert!(!decoy.connection_cancel_token().is_cancelled());
    decoy.shutdown().await;
    pair.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn adopted_legacy_retirement_does_not_revive_invalid_owners() -> anyhow::Result<()> {
    for invalidity in [
        "released",
        "route",
        "all routes",
        "owner",
        "source",
        "login",
        "all leases",
        "credential only",
    ] {
        let pair = adopted_retirement_pair().await?;
        pair.retire(0, 0).await?;
        match invalidity {
            "released" => {
                pair.leases[1].release();
            }
            "route" => pair.routes[1].close(),
            "all routes" => {
                pair.routes[1].close();
                pair.surviving_routes[1].close();
            }
            "all leases" => {
                for lease in pair.leases.iter().chain(pair.siblings.iter()) {
                    lease.release();
                }
            }
            _ => {
                let manager = AuthManager::from_auth_for_testing(
                    CodexAuth::create_dummy_chatgpt_auth_for_testing(),
                );
                let mut expected = *manager.auth_change_state_receiver().borrow();
                match invalidity {
                    "owner" => expected.owner_generation += 1,
                    "source" => expected.source_generation += 1,
                    "login" => expected.login_generation += 1,
                    "credential only" => expected.generation += 1,
                    _ => unreachable!(),
                }
                let mut key = pair.leases[1].inner.slot.key.lock().unwrap();
                key.as_mut().unwrap().auth_state = Some((manager, expected));
                pair.leases[1].inner.slot.factory.lock().unwrap().0 = key.clone();
            }
        }
        let effects = Arc::new(AtomicUsize::new(0));
        let result = pair.leases[1]
            .run_mcp_request(Arc::clone(&pair.routes[1]), {
                let effects = Arc::clone(&effects);
                move |client| async move {
                    client.client().await?;
                    effects.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;
        let allowed = invalidity == "credential only";
        assert_eq!(result.is_ok(), allowed, "{invalidity}");
        assert_eq!(
            effects.load(Ordering::SeqCst),
            usize::from(allowed),
            "{invalidity}"
        );
        assert_eq!(
            pair.starts[1].load(Ordering::SeqCst),
            1 + usize::from(allowed),
            "{invalidity}"
        );
        pair.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn terminal_generations_do_not_enter_request_retirement_recovery() -> anyhow::Result<()> {
    let pair = adopted_retirement_pair().await?;
    pair.retire(0, 0).await?;
    pair.original.start_shutdown();
    assert!(
        !pair.original.retire_request(),
        "terminal must dominate request retirement"
    );
    for _ in 0..2 {
        assert!(
            pair.leases[1]
                .run(Arc::clone(&pair.routes[1]), |_| async {
                    panic!("terminal dispatch")
                })
                .await
                .is_err()
        );
    }
    assert_eq!(pair.starts[1].load(Ordering::SeqCst), 1);
    pair.shutdown().await;
    for authentication_required in [false, true] {
        let pool = McpConnectionPool::default();
        let session_route = route();
        let starts = Arc::new(AtomicUsize::new(0));
        let lease = pool.acquire(
            identity("terminal", "/terminal"),
            McpConnectionPoolMode::Reuse,
            &session_route,
            {
                let starts = Arc::clone(&starts);
                move |router| {
                    starts.fetch_add(1, Ordering::SeqCst);
                    client_with_startup(
                        router,
                        async move {
                            if authentication_required {
                                Err(StartupOutcomeError::Failed {
                                    error: "authentication required".to_owned(),
                                    is_authentication_required: true,
                                    is_ema_authentication_required: false,
                                    shutdown: None,
                                })
                            } else {
                                Err(StartupOutcomeError::Cancelled)
                            }
                        }
                        .boxed()
                        .shared(),
                    )
                }
            },
        );
        for _ in 0..2 {
            assert!(
                lease
                    .await_current_startup(Arc::clone(&session_route))
                    .await
                    .is_err()
            );
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        lease.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn adopted_legacy_retirement_preserves_captured_bindings_and_cleanup() -> anyhow::Result<()> {
    let pair = adopted_retirement_pair().await?;
    let (binding, _, _) = pair.leases[1]
        .capture_ready_client_and_tools(
            Arc::clone(&pair.routes[1]),
            Arc::new(crate::mcp::ToolPluginContext::default()),
            None,
        )
        .await
        .expect("captured ready fallback");
    assert_eq!(binding.connection.id, pair.original.id);
    pair.retire(0, 0).await?;
    assert!(binding.acquire_route().await.is_err());
    assert_eq!(binding.connection.id, pair.original.id);
    let new_id = pair.leases[1]
        .run(Arc::clone(&pair.routes[1]), |client| async move {
            client.connection_id()
        })
        .await?;
    assert_ne!(new_id, pair.original.id);
    McpPooledClient {
        connection: Arc::clone(&pair.original),
        slot: Arc::downgrade(&pair.leases[1].inner.slot),
        route: Arc::clone(&pair.routes[1]),
    }
    .retire_after_failure();
    assert_eq!(pair.leases[1].connection_id(), new_id);
    assert_eq!(pair.starts[1].load(Ordering::SeqCst), 2);
    assert!(binding.acquire_route().await.is_err());
    pair.shutdown().await;
    assert!(pair.original.shutdown_tasks.is_empty());
    Ok(())
}
