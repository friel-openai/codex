use super::ConnectionSessionState;
use super::MessageProcessor;
use super::MessageProcessorArgs;
use crate::analytics_utils::analytics_events_client_from_config;
use crate::config_manager::ConfigManager;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessageSender;
use crate::plugin_config_reload::PluginStartupConfig;
use crate::transport::AppServerTransport;
use anyhow::Result;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::write_mock_responses_config_toml;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadForkImportParams;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use codex_login::AuthManager;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::W3cTraceContext;
use codex_utils_path_uri::LegacyAppPathString;
use opentelemetry::global;
use opentelemetry::trace::SpanId;
use opentelemetry::trace::SpanKind;
use opentelemetry::trace::TraceId;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::SpanData;
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use wiremock::MockServer;

pub(super) const TEST_CONNECTION_ID: ConnectionId = ConnectionId(7);

#[path = "message_processor_saved_thread_persistence_tests.rs"]
mod saved_thread_persistence_tests;

struct TestTracing {
    exporter: InMemorySpanExporter,
    provider: SdkTracerProvider,
}

struct RemoteTrace {
    trace_id: TraceId,
    parent_span_id: SpanId,
    context: W3cTraceContext,
}

impl RemoteTrace {
    fn new(trace_id: &str, parent_span_id: &str) -> Self {
        let trace_id = TraceId::from_hex(trace_id).expect("trace id");
        let parent_span_id = SpanId::from_hex(parent_span_id).expect("parent span id");
        let context = W3cTraceContext {
            traceparent: Some(format!("00-{trace_id}-{parent_span_id}-01")),
            tracestate: Some("vendor=value".to_string()),
        };

        Self {
            trace_id,
            parent_span_id,
            context,
        }
    }
}

fn init_test_tracing() -> &'static TestTracing {
    static TEST_TRACING: OnceLock<TestTracing> = OnceLock::new();
    TEST_TRACING.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("codex-app-server-message-processor-tests");
        global::set_text_map_propagator(TraceContextPropagator::new());
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::set_global_default(subscriber)
            .expect("global tracing subscriber should only be installed once");
        TestTracing { exporter, provider }
    })
}

fn request_from_client_request(request: ClientRequest) -> JSONRPCRequest {
    serde_json::from_value(serde_json::to_value(request).expect("serialize client request"))
        .expect("client request should convert to JSON-RPC")
}

struct TracingHarness {
    _server: MockServer,
    _codex_home: TempDir,
    processor: Arc<MessageProcessor>,
    outgoing_rx: mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    session: Arc<ConnectionSessionState>,
    tracing: &'static TestTracing,
}

impl TracingHarness {
    async fn new() -> Result<Self> {
        let server = create_mock_responses_server_repeating_assistant("Done").await;
        let codex_home = TempDir::new()?;
        let config = Arc::new(build_test_config(codex_home.path(), &server.uri()).await?);
        let auth_manager = AuthManager::shared_from_config(
            config.as_ref(),
            /*enable_codex_api_key_env*/ false,
        )
        .await?;
        let (processor, outgoing_rx) = build_test_processor(config, auth_manager).await;
        Self::from_processor(server, codex_home, processor, outgoing_rx).await
    }

    async fn new_with_sqlite() -> Result<Self> {
        let server = create_mock_responses_server_repeating_assistant("Done").await;
        let codex_home = TempDir::new()?;
        let config = Arc::new(build_test_config(codex_home.path(), &server.uri()).await?);
        let auth_manager = AuthManager::shared_from_config(
            config.as_ref(),
            /*enable_codex_api_key_env*/ false,
        )
        .await?;
        // Saved-thread repair reads the SQLite history projection, as production does.
        // Existing tracing-only fixtures intentionally leave this database unavailable.
        let state_db = crate::init_sqlite_state_db_with_fresh_start_on_corruption(&config)
            .await?
            .state_db
            .expect("initialized isolated SQLite state");
        let (processor, outgoing_rx) =
            build_test_processor_with_state_db(config, auth_manager, Some(state_db)).await;
        Self::from_processor(server, codex_home, processor, outgoing_rx).await
    }

    async fn from_processor(
        server: MockServer,
        codex_home: TempDir,
        processor: Arc<MessageProcessor>,
        outgoing_rx: mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    ) -> Result<Self> {
        let tracing = init_test_tracing();
        tracing.exporter.reset();
        tracing::callsite::rebuild_interest_cache();
        let mut harness = Self {
            _server: server,
            _codex_home: codex_home,
            processor,
            outgoing_rx,
            session: Arc::new(ConnectionSessionState::new(
                crate::transport::ConnectionOrigin::Stdio,
            )),
            tracing,
        };

        let _: InitializeResponse = harness
            .request(
                ClientRequest::Initialize {
                    request_id: RequestId::Integer(1),
                    params: InitializeParams {
                        client_info: ClientInfo {
                            name: "codex-app-server-tests".to_string(),
                            title: None,
                            version: "0.1.0".to_string(),
                        },
                        capabilities: Some(InitializeCapabilities {
                            experimental_api: true,
                            ..Default::default()
                        }),
                    },
                },
                /*trace*/ None,
            )
            .await;
        assert!(harness.session.initialized());

        Ok(harness)
    }

    fn reset_tracing(&self) {
        self.tracing.exporter.reset();
    }

    async fn shutdown(self) {
        self.processor.shutdown_threads().await;
        self.processor.drain_background_tasks().await;
    }

    async fn request<T>(&mut self, request: ClientRequest, trace: Option<W3cTraceContext>) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let request_id = match request.id() {
            RequestId::Integer(request_id) => *request_id,
            request_id => panic!("expected integer request id in test harness, got {request_id:?}"),
        };
        let mut request = request_from_client_request(request);
        request.trace = trace;

        self.processor
            .process_request(
                TEST_CONNECTION_ID,
                request,
                &AppServerTransport::Stdio,
                Arc::clone(&self.session),
            )
            .await;
        read_response(&mut self.outgoing_rx, request_id).await
    }

    async fn start_thread(
        &mut self,
        request_id: i64,
        trace: Option<W3cTraceContext>,
    ) -> ThreadStartResponse {
        let response = self
            .request(
                ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(request_id),
                    params: ThreadStartParams {
                        ephemeral: Some(true),
                        ..ThreadStartParams::default()
                    },
                },
                trace,
            )
            .await;
        read_thread_started_notification(&mut self.outgoing_rx).await;
        response
    }
}

async fn build_test_config(codex_home: &Path, server_uri: &str) -> Result<Config> {
    write_mock_responses_config_toml(
        codex_home,
        server_uri,
        &BTreeMap::new(),
        /*auto_compact_limit*/ 8_192,
        Some(false),
        "mock_provider",
        "compact",
    )?;

    Ok(ConfigBuilder::default()
        .codex_home(codex_home.to_path_buf())
        .build()
        .await?)
}

pub(super) async fn build_test_processor(
    config: Arc<Config>,
    auth_manager: Arc<AuthManager>,
) -> (
    Arc<MessageProcessor>,
    mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) {
    build_test_processor_with_state_db(config, auth_manager, /*state_db*/ None).await
}

async fn build_test_processor_with_state_db(
    config: Arc<Config>,
    auth_manager: Arc<AuthManager>,
    state_db: Option<crate::rollout_state_db::StateDbHandle>,
) -> (
    Arc<MessageProcessor>,
    mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) {
    build_test_processor_with_loader(
        config,
        auth_manager,
        state_db,
        CloudConfigBundleLoader::default(),
    )
    .await
}

async fn build_test_processor_with_loader(
    config: Arc<Config>,
    auth_manager: Arc<AuthManager>,
    state_db: Option<crate::rollout_state_db::StateDbHandle>,
    cloud_config_loader: CloudConfigBundleLoader,
) -> (
    Arc<MessageProcessor>,
    mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) {
    let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
    let config_manager = ConfigManager::new(
        config.codex_home.to_path_buf(),
        Vec::new(),
        LoaderOverrides::with_managed_config_path_for_tests(
            config.codex_home.join("managed_config.toml").to_path_buf(),
        ),
        /*strict_config*/ false,
        cloud_config_loader,
        Arg0DispatchPaths::default(),
        Arc::new(codex_config::NoopThreadConfigLoader),
    );
    let analytics_events_client =
        analytics_events_client_from_config(Arc::clone(&auth_manager), config.as_ref());
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        analytics_events_client.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(MessageProcessorArgs {
        cloud_skill_provider: None,
        outgoing,
        analytics_events_client,
        arg0_paths: Arg0DispatchPaths::default(),
        startup_cwd: Some(config.cwd.to_path_buf()),
        config,
        config_manager,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db,
        config_warnings: Vec::new(),
        startup_warnings_tx: mpsc::unbounded_channel().0,
        session_source: SessionSource::VSCode,
        user_verification: Arc::new(crate::user_verification::Service::new(Arc::clone(
            &auth_manager,
        ))),
        auth_manager,
        installation_id: "11111111-1111-4111-8111-111111111111".to_string(),
        code_mode_session_provider: None,
        rpc_transport: AppServerRpcTransport::Stdio,
        remote_control_handle: None,
        plugin_startup_tasks: Some(PluginStartupConfig::Current),
    }));
    (processor, outgoing_rx)
}

fn run_current_thread_test_with_stack<F>(name: &str, future: F) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(Box::pin(future))
        })?;

    match handle.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("{name} thread panicked")),
    }
}

fn span_attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a str> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| match &kv.value {
            opentelemetry::Value::String(value) => Some(value.as_str()),
            _ => None,
        })
}

fn find_rpc_span_with_trace<'a>(
    spans: &'a [SpanData],
    kind: SpanKind,
    method: &str,
    trace_id: TraceId,
) -> &'a SpanData {
    spans
        .iter()
        .find(|span| {
            span.span_kind == kind
                && span_attr(span, "rpc.system") == Some("jsonrpc")
                && span_attr(span, "rpc.method") == Some(method)
                && span.span_context.trace_id() == trace_id
        })
        .unwrap_or_else(|| {
            panic!(
                "missing {kind:?} span for rpc.method={method} trace={trace_id}; exported spans:\n{}",
                format_spans(spans)
            )
        })
}

fn find_span_with_trace<'a, F>(
    spans: &'a [SpanData],
    trace_id: TraceId,
    description: &str,
    predicate: F,
) -> &'a SpanData
where
    F: Fn(&SpanData) -> bool,
{
    spans
        .iter()
        .find(|span| span.span_context.trace_id() == trace_id && predicate(span))
        .unwrap_or_else(|| {
            panic!(
                "missing span matching {description} for trace={trace_id}; exported spans:\n{}",
                format_spans(spans)
            )
        })
}

fn format_spans(spans: &[SpanData]) -> String {
    spans
        .iter()
        .map(|span| {
            let rpc_method = span_attr(span, "rpc.method").unwrap_or("-");
            format!(
                "name={} span_id={} kind={:?} parent={} trace={} rpc.method={}",
                span.name,
                span.span_context.span_id(),
                span.span_kind,
                span.parent_span_id,
                span.span_context.trace_id(),
                rpc_method
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn span_depth_from_ancestor(
    spans: &[SpanData],
    child: &SpanData,
    ancestor: &SpanData,
) -> Option<usize> {
    let ancestor_span_id = ancestor.span_context.span_id();
    let mut parent_span_id = child.parent_span_id;
    let mut depth = 1;
    while parent_span_id != SpanId::INVALID {
        if parent_span_id == ancestor_span_id {
            return Some(depth);
        }
        let Some(parent_span) = spans
            .iter()
            .find(|span| span.span_context.span_id() == parent_span_id)
        else {
            break;
        };
        parent_span_id = parent_span.parent_span_id;
        depth += 1;
    }

    None
}

fn assert_span_descends_from(spans: &[SpanData], child: &SpanData, ancestor: &SpanData) {
    if span_depth_from_ancestor(spans, child, ancestor).is_some() {
        return;
    }

    panic!(
        "span {} does not descend from {}; exported spans:\n{}",
        child.name,
        ancestor.name,
        format_spans(spans)
    );
}

fn assert_has_internal_descendant_at_min_depth(
    spans: &[SpanData],
    ancestor: &SpanData,
    min_depth: usize,
) {
    if spans.iter().any(|span| {
        span.span_kind == SpanKind::Internal
            && span.span_context.trace_id() == ancestor.span_context.trace_id()
            && span_depth_from_ancestor(spans, span, ancestor)
                .is_some_and(|depth| depth >= min_depth)
    }) {
        return;
    }

    panic!(
        "missing internal descendant at depth >= {min_depth} below {}; exported spans:\n{}",
        ancestor.name,
        format_spans(spans)
    );
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn stalled_auth_config_delays_auth_status_but_not_initialize() -> Result<()> {
    use codex_app_server_protocol::GetAuthStatusParams;
    use codex_app_server_protocol::GetAuthStatusResponse;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use tokio::sync::Semaphore;

    let block_next = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let loader = CloudConfigBundleLoader::from_getter({
        let (block_next, entered, release) = (block_next.clone(), entered.clone(), release.clone());
        move || {
            let (block_next, entered, release) =
                (block_next.clone(), entered.clone(), release.clone());
            async move {
                if block_next.swap(false, Ordering::SeqCst) {
                    entered.add_permits(1);
                    release
                        .acquire()
                        .await
                        .expect("release config load")
                        .forget();
                }
                Ok(None)
            }
        }
    });
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let home = TempDir::new()?;
    let config = Arc::new(build_test_config(home.path(), &server.uri()).await?);
    let auth = AuthManager::shared_from_config(config.as_ref(), false).await?;
    let (processor, outgoing_rx) =
        build_test_processor_with_loader(config, auth, None, loader).await;
    let mut harness = TracingHarness::from_processor(server, home, processor, outgoing_rx).await?;

    block_next.store(true, Ordering::SeqCst);
    let auth_status = |id| ClientRequest::GetAuthStatus {
        request_id: RequestId::Integer(id),
        params: GetAuthStatusParams {
            include_token: Some(false),
            refresh_token: Some(false),
            allow_retryable_error: false,
        },
    };
    harness
        .processor
        .process_request(
            TEST_CONNECTION_ID,
            request_from_client_request(auth_status(2)),
            &AppServerTransport::Stdio,
            Arc::clone(&harness.session),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await??
        .forget();

    let connection = ConnectionId(8);
    let session = Arc::new(ConnectionSessionState::new(
        crate::transport::ConnectionOrigin::Stdio,
    ));
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(ClientRequest::Initialize {
                request_id: RequestId::Integer(3),
                params: InitializeParams {
                    client_info: ClientInfo {
                        name: "second-client".to_string(),
                        title: None,
                        version: "0.1.0".to_string(),
                    },
                    capabilities: Some(InitializeCapabilities {
                        experimental_api: true,
                        ..Default::default()
                    }),
                },
            }),
            &AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let _: InitializeResponse = tokio::time::timeout(
        Duration::from_secs(5),
        read_response_from(&mut harness.outgoing_rx, connection, 3),
    )
    .await?;
    assert!(session.initialized());
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(auth_status(4)),
            &AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let opted_in = ClientRequest::GetAuthStatus {
        request_id: RequestId::Integer(5),
        params: GetAuthStatusParams {
            include_token: Some(false),
            refresh_token: Some(false),
            allow_retryable_error: true,
        },
    };
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(opted_in),
            &AppServerTransport::Stdio,
            session,
        )
        .await;
    loop {
        let envelope = tokio::time::timeout(Duration::from_secs(5), harness.outgoing_rx.recv())
            .await?
            .expect("outgoing channel");
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
            && connection_id == connection
        {
            match message {
                crate::outgoing_message::OutgoingMessage::Error(error) => {
                    assert_eq!(error.id, RequestId::Integer(5));
                    assert_eq!(error.error.code, crate::error_code::OVERLOADED_ERROR_CODE);
                    assert_eq!(
                        error.error.data,
                        Some(serde_json::json!({"reason": "authStatusTemporarilyUnavailable"}))
                    );
                    break;
                }
                crate::outgoing_message::OutgoingMessage::Response(response) => {
                    assert_ne!(response.id, RequestId::Integer(4));
                }
                _ => {}
            }
        }
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            read_response_from::<GetAuthStatusResponse>(&mut harness.outgoing_rx, connection, 4)
        )
        .await
        .is_err()
    );

    release.add_permits(1);
    let _: GetAuthStatusResponse = tokio::time::timeout(
        Duration::from_secs(5),
        read_response_from(&mut harness.outgoing_rx, TEST_CONNECTION_ID, 2),
    )
    .await?;
    let _: GetAuthStatusResponse = tokio::time::timeout(
        Duration::from_secs(5),
        read_response_from(&mut harness.outgoing_rx, connection, 4),
    )
    .await?;
    harness.shutdown().await;
    Ok(())
}

async fn retryable_auth_test_harness() -> Result<(
    TracingHarness,
    Arc<std::sync::atomic::AtomicBool>,
    Arc<tokio::sync::Semaphore>,
    Arc<tokio::sync::Semaphore>,
    ConnectionId,
    Arc<ConnectionSessionState>,
)> {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use tokio::sync::Semaphore;

    let block_next = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let loader = CloudConfigBundleLoader::from_getter({
        let (block_next, entered, release) = (block_next.clone(), entered.clone(), release.clone());
        move || {
            let (block_next, entered, release) =
                (block_next.clone(), entered.clone(), release.clone());
            async move {
                if block_next.swap(false, Ordering::SeqCst) {
                    entered.add_permits(1);
                    release
                        .acquire()
                        .await
                        .expect("release config load")
                        .forget();
                }
                Ok(None)
            }
        }
    });
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let home = TempDir::new()?;
    let config = Arc::new(build_test_config(home.path(), &server.uri()).await?);
    let auth = AuthManager::shared_from_config(config.as_ref(), false).await?;
    let (processor, outgoing_rx) =
        build_test_processor_with_loader(config, auth, None, loader).await;
    let mut harness = TracingHarness::from_processor(server, home, processor, outgoing_rx).await?;
    let connection = ConnectionId(8);
    let session = Arc::new(ConnectionSessionState::new(
        crate::transport::ConnectionOrigin::Stdio,
    ));
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(ClientRequest::Initialize {
                request_id: RequestId::Integer(3),
                params: InitializeParams {
                    client_info: ClientInfo {
                        name: "retrying-client".to_string(),
                        title: None,
                        version: "0.1.0".to_string(),
                    },
                    capabilities: Some(InitializeCapabilities {
                        experimental_api: true,
                        ..Default::default()
                    }),
                },
            }),
            &AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let _: InitializeResponse = read_response_from(&mut harness.outgoing_rx, connection, 3).await;
    Ok((harness, block_next, entered, release, connection, session))
}

fn read_only_auth_status(id: i64) -> ClientRequest {
    ClientRequest::GetAuthStatus {
        request_id: RequestId::Integer(id),
        params: codex_app_server_protocol::GetAuthStatusParams {
            include_token: Some(false),
            refresh_token: Some(false),
            allow_retryable_error: true,
        },
    }
}

async fn read_auth_status_error(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    connection: ConnectionId,
    request_id: i64,
) {
    loop {
        let envelope = tokio::time::timeout(Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("auth status deadline")
            .expect("outgoing channel");
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message: crate::outgoing_message::OutgoingMessage::Error(error),
            ..
        } = envelope
            && connection_id == connection
            && error.id == RequestId::Integer(request_id)
        {
            assert_eq!(error.error.code, crate::error_code::OVERLOADED_ERROR_CODE);
            assert_eq!(
                error.error.data,
                Some(serde_json::json!({"reason": "authStatusTemporarilyUnavailable"}))
            );
            return;
        }
    }
}

async fn read_response_without_expired_auth<T: serde::de::DeserializeOwned>(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    connection: ConnectionId,
    expected_id: i64,
) -> T {
    loop {
        let envelope = tokio::time::timeout(Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("response deadline")
            .expect("outgoing channel");
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
            && connection_id == connection
        {
            match message {
                crate::outgoing_message::OutgoingMessage::Response(response) => {
                    assert_ne!(
                        response.id,
                        RequestId::Integer(4),
                        "late response to expired status"
                    );
                    assert_ne!(
                        response.id,
                        RequestId::Integer(5),
                        "late response to expired status"
                    );
                    if response.id == RequestId::Integer(expected_id) {
                        return serde_json::from_value(
                            serde_json::to_value(response.result).expect("serialize response"),
                        )
                        .expect("deserialize response");
                    }
                }
                crate::outgoing_message::OutgoingMessage::Error(error) => {
                    assert_ne!(
                        error.id,
                        RequestId::Integer(4),
                        "late error to expired status"
                    );
                    assert_ne!(
                        error.id,
                        RequestId::Integer(5),
                        "late error to expired status"
                    );
                    assert_ne!(
                        error.id,
                        RequestId::Integer(expected_id),
                        "unexpected error: {error:?}"
                    );
                }
                _ => {}
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn retryable_auth_status_preserves_auth_order_and_current_provider() -> Result<()> {
    use codex_app_server_protocol::GetAuthStatusResponse;
    use codex_app_server_protocol::LoginAccountParams;
    use codex_app_server_protocol::LoginAccountResponse;
    use codex_app_server_protocol::LogoutAccountResponse;
    use std::sync::atomic::Ordering;

    let (mut harness, block_next, entered, release, connection, session) =
        retryable_auth_test_harness().await?;
    block_next.store(true, Ordering::SeqCst);
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(read_only_auth_status(4)),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await??
        .forget();
    read_auth_status_error(&mut harness.outgoing_rx, connection, 4).await;

    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(read_only_auth_status(5)),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    read_auth_status_error(&mut harness.outgoing_rx, connection, 5).await;
    assert_eq!(
        harness
            .processor
            .request_serialization_queues
            .queued_count(
                &crate::request_serialization::RequestSerializationQueueKey::Global("account-auth")
            )
            .await,
        0
    );

    write_mock_responses_config_toml(
        harness._codex_home.path(),
        &harness._server.uri(),
        &BTreeMap::new(),
        8192,
        Some(true),
        "mock_provider",
        "compact",
    )?;
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(ClientRequest::LoginAccount {
                request_id: RequestId::Integer(6),
                params: LoginAccountParams::ApiKey {
                    api_key: "synthetic-test-key".to_string(),
                },
            }),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(read_only_auth_status(7)),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(ClientRequest::LogoutAccount {
                request_id: RequestId::Integer(8),
                params: None,
            }),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(read_only_auth_status(9)),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    release.add_permits(1);
    let _: LoginAccountResponse =
        read_response_without_expired_auth(&mut harness.outgoing_rx, connection, 6).await;

    let logged_in: GetAuthStatusResponse =
        read_response_without_expired_auth(&mut harness.outgoing_rx, connection, 7).await;
    assert_eq!(logged_in.requires_openai_auth, Some(true));
    assert!(logged_in.auth_method.is_some());
    assert_eq!(logged_in.auth_token, None);
    let _: LogoutAccountResponse =
        read_response_without_expired_auth(&mut harness.outgoing_rx, connection, 8).await;
    let logged_out: GetAuthStatusResponse =
        read_response_without_expired_auth(&mut harness.outgoing_rx, connection, 9).await;
    assert_eq!(logged_out.requires_openai_auth, Some(true));
    assert_eq!(logged_out.auth_method, None);

    write_mock_responses_config_toml(
        harness._codex_home.path(),
        &harness._server.uri(),
        &BTreeMap::new(),
        8192,
        Some(false),
        "mock_provider",
        "compact",
    )?;
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(read_only_auth_status(10)),
            &AppServerTransport::Stdio,
            session,
        )
        .await;
    let custom_provider: GetAuthStatusResponse =
        read_response_without_expired_auth(&mut harness.outgoing_rx, connection, 10).await;
    assert_eq!(custom_provider.requires_openai_auth, Some(false));
    assert_eq!(custom_provider.auth_method, None);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while let Ok(Some(envelope)) =
        tokio::time::timeout_at(deadline, harness.outgoing_rx.recv()).await
    {
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
            && connection_id == connection
        {
            match message {
                crate::outgoing_message::OutgoingMessage::Response(response) => {
                    assert!(!matches!(response.id, RequestId::Integer(4 | 5)))
                }
                crate::outgoing_message::OutgoingMessage::Error(error) => {
                    assert!(!matches!(error.id, RequestId::Integer(4 | 5)))
                }
                _ => {}
            }
        }
    }
    harness.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn retryable_auth_status_disconnect_discards_waiting_request() -> Result<()> {
    use codex_app_server_protocol::GetAuthStatusResponse;
    use std::sync::atomic::Ordering;

    let (mut harness, block_next, entered, release, connection, session) =
        retryable_auth_test_harness().await?;
    block_next.store(true, Ordering::SeqCst);
    harness
        .processor
        .process_request(
            TEST_CONNECTION_ID,
            request_from_client_request(read_only_auth_status(11)),
            &AppServerTransport::Stdio,
            harness.session.clone(),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await??
        .forget();
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(read_only_auth_status(12)),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    let key = crate::request_serialization::RequestSerializationQueueKey::Global("account-auth");
    assert_eq!(
        harness
            .processor
            .request_serialization_queues
            .queued_count(&key)
            .await,
        1
    );
    harness
        .processor
        .connection_closed(connection, &session)
        .await;
    assert_eq!(
        harness
            .processor
            .request_serialization_queues
            .queued_count(&key)
            .await,
        0
    );
    release.add_permits(1);
    let _: GetAuthStatusResponse =
        read_response_from(&mut harness.outgoing_rx, TEST_CONNECTION_ID, 11).await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(3200);
    while let Ok(Some(envelope)) =
        tokio::time::timeout_at(deadline, harness.outgoing_rx.recv()).await
    {
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
            && connection_id == connection
        {
            match message {
                crate::outgoing_message::OutgoingMessage::Response(response) => {
                    assert_ne!(response.id, RequestId::Integer(12))
                }
                crate::outgoing_message::OutgoingMessage::Error(error) => {
                    assert_ne!(error.id, RequestId::Integer(12))
                }
                _ => {}
            }
        }
    }
    harness.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn retryable_auth_status_disconnect_drains_started_request() -> Result<()> {
    use std::sync::atomic::Ordering;

    let (mut harness, block_next, entered, release, connection, session) =
        retryable_auth_test_harness().await?;
    block_next.store(true, Ordering::SeqCst);
    harness
        .processor
        .process_request(
            connection,
            request_from_client_request(read_only_auth_status(14)),
            &AppServerTransport::Stdio,
            session.clone(),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await??
        .forget();
    let processor = Arc::clone(&harness.processor);
    let mut close = tokio::spawn(async move {
        processor.connection_closed(connection, &session).await;
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut close)
            .await
            .is_err()
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), close).await??;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while let Ok(Some(envelope)) =
        tokio::time::timeout_at(deadline, harness.outgoing_rx.recv()).await
    {
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
            && connection_id == connection
        {
            match message {
                crate::outgoing_message::OutgoingMessage::Response(response) => {
                    assert_ne!(response.id, RequestId::Integer(14))
                }
                crate::outgoing_message::OutgoingMessage::Error(error) => {
                    assert_ne!(error.id, RequestId::Integer(14))
                }
                _ => {}
            }
        }
    }
    harness.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn retryable_auth_status_does_not_time_out_token_or_refresh_requests() -> Result<()> {
    use codex_app_server_protocol::GetAuthStatusParams;
    use codex_app_server_protocol::GetAuthStatusResponse;
    use std::sync::atomic::Ordering;

    let (mut harness, block_next, entered, release, connection, session) =
        retryable_auth_test_harness().await?;
    block_next.store(true, Ordering::SeqCst);
    harness
        .processor
        .process_request(
            TEST_CONNECTION_ID,
            request_from_client_request(ClientRequest::GetAuthStatus {
                request_id: RequestId::Integer(11),
                params: GetAuthStatusParams {
                    include_token: Some(false),
                    refresh_token: Some(false),
                    allow_retryable_error: false,
                },
            }),
            &AppServerTransport::Stdio,
            harness.session.clone(),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await??
        .forget();
    for (id, include_token, refresh_token) in [(12, true, false), (13, false, true)] {
        harness
            .processor
            .process_request(
                connection,
                request_from_client_request(ClientRequest::GetAuthStatus {
                    request_id: RequestId::Integer(id),
                    params: GetAuthStatusParams {
                        include_token: Some(include_token),
                        refresh_token: Some(refresh_token),
                        allow_retryable_error: true,
                    },
                }),
                &AppServerTransport::Stdio,
                session.clone(),
            )
            .await;
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(3200),
            read_response_from::<GetAuthStatusResponse>(&mut harness.outgoing_rx, connection, 12)
        )
        .await
        .is_err()
    );
    release.add_permits(1);
    let _: GetAuthStatusResponse =
        read_response_from(&mut harness.outgoing_rx, TEST_CONNECTION_ID, 11).await;
    let _: GetAuthStatusResponse =
        read_response_from(&mut harness.outgoing_rx, connection, 12).await;
    let _: GetAuthStatusResponse =
        read_response_from(&mut harness.outgoing_rx, connection, 13).await;
    harness.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn fork_handoff_requests_are_rejected_after_drain_starts() -> Result<()> {
    let mut harness = TracingHarness::new().await?;
    harness.processor.turn_admission.begin_drain();
    for request in [
        ClientRequest::ThreadForkPrepare {
            request_id: RequestId::Integer(2),
            params: ThreadForkParams {
                thread_id: codex_protocol::ThreadId::new().to_string(),
                exclude_turns: true,
                ..Default::default()
            },
        },
        ClientRequest::ThreadForkImport {
            request_id: RequestId::Integer(3),
            params: ThreadForkImportParams {
                socket_path: LegacyAppPathString::from_path(
                    &harness._codex_home.path().join("fork-pending.sock"),
                ),
            },
        },
    ] {
        let request_id = request.id().clone();
        harness
            .processor
            .process_request(
                TEST_CONNECTION_ID,
                request_from_client_request(request),
                &AppServerTransport::Stdio,
                Arc::clone(&harness.session),
            )
            .await;
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let envelope = harness.outgoing_rx.recv().await.expect("outgoing channel");
                let crate::outgoing_message::OutgoingEnvelope::ToConnection {
                    connection_id,
                    message,
                    ..
                } = envelope
                else {
                    continue;
                };
                if connection_id != TEST_CONNECTION_ID {
                    continue;
                }
                match message {
                    crate::outgoing_message::OutgoingMessage::Error(error)
                        if error.id == request_id =>
                    {
                        break error;
                    }
                    crate::outgoing_message::OutgoingMessage::Response(response)
                        if response.id == request_id =>
                    {
                        panic!("handoff admitted after drain: {response:?}");
                    }
                    _ => {}
                }
            }
        })
        .await?;
        assert_eq!(error.error, crate::error_code::server_draining_error());
    }
    harness.shutdown().await;
    Ok(())
}

pub(super) async fn read_response<T: serde::de::DeserializeOwned>(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    request_id: i64,
) -> T {
    read_response_from(outgoing_rx, TEST_CONNECTION_ID, request_id).await
}

pub(super) async fn read_response_from<T: serde::de::DeserializeOwned>(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
    request_id: i64,
) -> T {
    loop {
        let envelope = tokio::time::timeout(Duration::from_secs(/*secs*/ 30), outgoing_rx.recv())
            .await
            .unwrap_or_else(|error| {
                panic!("timed out waiting for response to request {request_id}: {error}")
            })
            .expect("outgoing channel closed");
        let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
        else {
            continue;
        };
        if connection_id != expected_connection_id {
            continue;
        }
        let response = match message {
            crate::outgoing_message::OutgoingMessage::Response(response) => response,
            crate::outgoing_message::OutgoingMessage::Error(error)
                if error.id == RequestId::Integer(request_id) =>
            {
                panic!("request {request_id} failed: {error:?}");
            }
            _ => continue,
        };
        if response.id != RequestId::Integer(request_id) {
            continue;
        }
        return serde_json::from_value(
            serde_json::to_value(response.result).expect("response payload should serialize"),
        )
        .expect("response payload should deserialize");
    }
}

async fn read_thread_started_notification(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for thread/started notification")
            .expect("outgoing channel closed");
        match envelope {
            crate::outgoing_message::OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                ..
            } => {
                if connection_id != TEST_CONNECTION_ID {
                    continue;
                }
                let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) =
                    message
                else {
                    continue;
                };
                if matches!(
                    notification.notification,
                    codex_app_server_protocol::ServerNotification::ThreadStarted(_)
                ) {
                    return;
                }
            }
            crate::outgoing_message::OutgoingEnvelope::Broadcast { message } => {
                let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) =
                    message
                else {
                    continue;
                };
                if matches!(
                    notification.notification,
                    codex_app_server_protocol::ServerNotification::ThreadStarted(_)
                ) {
                    return;
                }
            }
        }
    }
}

async fn wait_for_exported_spans<F>(tracing: &TestTracing, predicate: F) -> Vec<SpanData>
where
    F: Fn(&[SpanData]) -> bool,
{
    let mut last_spans = Vec::new();
    for _ in 0..200 {
        tokio::task::yield_now().await;
        tracing
            .provider
            .force_flush()
            .expect("force flush should succeed");
        let spans = tracing.exporter.get_finished_spans().expect("span export");
        last_spans = spans.clone();
        if predicate(&spans) {
            return spans;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    panic!(
        "timed out waiting for expected exported spans:\n{}",
        format_spans(&last_spans)
    );
}

async fn wait_for_new_exported_spans<F>(
    tracing: &TestTracing,
    baseline_len: usize,
    predicate: F,
) -> Vec<SpanData>
where
    F: Fn(&[SpanData]) -> bool,
{
    let spans = wait_for_exported_spans(tracing, |spans| {
        spans.len() > baseline_len && predicate(&spans[baseline_len..])
    })
    .await;
    spans.into_iter().skip(baseline_len).collect()
}

#[test]
#[serial(app_server_tracing)]
fn thread_start_jsonrpc_span_exports_server_span_and_parents_children() -> Result<()> {
    run_current_thread_test_with_stack(
        "thread_start_jsonrpc_span_exports_server_span_and_parents_children",
        async {
            let mut harness = TracingHarness::new().await?;

            let RemoteTrace {
                trace_id: remote_trace_id,
                parent_span_id: remote_parent_span_id,
                context: remote_trace,
                ..
            } = RemoteTrace::new("00000000000000000000000000000011", "0000000000000022");

            let _: ThreadStartResponse = harness
                .start_thread(/*request_id*/ 20_002, /*trace*/ None)
                .await;
            let untraced_spans = wait_for_exported_spans(harness.tracing, |spans| {
                spans.iter().any(|span| {
                    span.span_kind == SpanKind::Server
                        && span_attr(span, "rpc.method") == Some("thread/start")
                })
            })
            .await;
            let untraced_server_span = find_rpc_span_with_trace(
                &untraced_spans,
                SpanKind::Server,
                "thread/start",
                untraced_spans
                    .iter()
                    .rev()
                    .find(|span| {
                        span.span_kind == SpanKind::Server
                            && span_attr(span, "rpc.system") == Some("jsonrpc")
                            && span_attr(span, "rpc.method") == Some("thread/start")
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "missing latest thread/start server span; exported spans:\n{}",
                            format_spans(&untraced_spans)
                        )
                    })
                    .span_context
                    .trace_id(),
            );
            assert_has_internal_descendant_at_min_depth(
                &untraced_spans,
                untraced_server_span,
                /*min_depth*/ 1,
            );

            let baseline_len = untraced_spans.len();
            let _: ThreadStartResponse = harness
                .start_thread(/*request_id*/ 20_003, Some(remote_trace))
                .await;
            let spans = wait_for_new_exported_spans(harness.tracing, baseline_len, |spans| {
                spans.iter().any(|span| {
                    span.span_kind == SpanKind::Server
                        && span_attr(span, "rpc.method") == Some("thread/start")
                        && span.span_context.trace_id() == remote_trace_id
                }) && spans.iter().any(|span| {
                    span.name.as_ref() == "app_server.thread_start.notify_started"
                        && span.span_context.trace_id() == remote_trace_id
                })
            })
            .await;

            let server_request_span =
                find_rpc_span_with_trace(&spans, SpanKind::Server, "thread/start", remote_trace_id);
            assert_eq!(server_request_span.name.as_ref(), "thread/start");
            assert_eq!(server_request_span.parent_span_id, remote_parent_span_id);
            assert!(server_request_span.parent_span_is_remote);
            assert_eq!(server_request_span.span_context.trace_id(), remote_trace_id);
            assert_ne!(server_request_span.span_context.span_id(), SpanId::INVALID);
            assert_has_internal_descendant_at_min_depth(
                &spans,
                server_request_span,
                /*min_depth*/ 1,
            );
            assert_has_internal_descendant_at_min_depth(
                &spans,
                server_request_span,
                /*min_depth*/ 2,
            );
            harness.shutdown().await;

            Ok(())
        },
    )
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn turn_start_jsonrpc_span_parents_core_turn_spans() -> Result<()> {
    let mut harness = TracingHarness::new().await?;
    let thread_start_response = harness.start_thread(/*request_id*/ 2, /*trace*/ None).await;
    let thread_id = thread_start_response.thread.id.clone();

    harness.reset_tracing();

    let RemoteTrace {
        trace_id: remote_trace_id,
        parent_span_id: remote_parent_span_id,
        context: remote_trace,
    } = RemoteTrace::new("00000000000000000000000000000077", "0000000000000088");
    let turn_start_response: TurnStartResponse = harness
        .request(
            ClientRequest::TurnStart {
                request_id: RequestId::Integer(3),
                params: TurnStartParams {
                    disabled_plugin_ids: None,
                    environments: None,
                    thread_id,
                    client_user_message_id: None,
                    input: vec![UserInput::Text {
                        text: "hello".to_string(),
                        text_elements: Vec::new(),
                    }],
                    turn_trigger: None,
                    parent_turn_id: None,
                    root_turn_id: None,
                    tool_output: None,
                    responsesapi_client_metadata: None,
                    responsesapi_client_metadata_patch: None,
                    additional_context: None,
                    cwd: None,
                    runtime_workspace_roots: None,
                    approval_policy: None,
                    sandbox_policy: None,
                    permissions: None,
                    approvals_reviewer: None,
                    model: None,
                    service_tier: None,
                    service_tier_for_turn: None,
                    effort: None,
                    summary: None,
                    personality: None,
                    output_schema: None,
                    collaboration_mode: None,
                    multi_agent_mode: None,
                    cyber_access_program: None,
                },
            },
            Some(remote_trace),
        )
        .await;
    let spans = wait_for_exported_spans(harness.tracing, |spans| {
        spans.iter().any(|span| {
            span.span_kind == SpanKind::Server
                && span_attr(span, "rpc.method") == Some("turn/start")
                && span.span_context.trace_id() == remote_trace_id
        }) && spans.iter().any(|span| {
            span_attr(span, "codex.op") == Some("turn_input")
                && span.span_context.trace_id() == remote_trace_id
        })
    })
    .await;

    let server_request_span =
        find_rpc_span_with_trace(&spans, SpanKind::Server, "turn/start", remote_trace_id);
    let core_turn_span =
        find_span_with_trace(&spans, remote_trace_id, "codex.op=turn_input", |span| {
            span_attr(span, "codex.op") == Some("turn_input")
        });

    assert_eq!(server_request_span.parent_span_id, remote_parent_span_id);
    assert!(server_request_span.parent_span_is_remote);
    assert_eq!(server_request_span.span_context.trace_id(), remote_trace_id);
    assert_eq!(
        span_attr(server_request_span, "turn.id"),
        Some(turn_start_response.turn.id.as_str())
    );
    assert_span_descends_from(&spans, core_turn_span, server_request_span);
    harness.shutdown().await;

    Ok(())
}
