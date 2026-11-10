//! Root-family ownership of MCP generations and negotiated transports.
//!
//! The protocol requires request-scoped context and explicit handles for application state.
//! Request continuations use the calling chat's elicitation authority. Legacy connections and
//! servers that request input during startup remain private to their root generation.

use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use codex_async_utils::OrCancelExt;
use codex_connectors::ConnectorRuntimeContextKey;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;
use codex_login::AuthChangeState;
use codex_login::AuthManager;
use codex_rmcp_client::ElicitationResponse;
use codex_rmcp_client::RequestScope;
use codex_rmcp_client::RmcpClient;
use codex_rmcp_client::SendElicitation;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use rmcp::model::ElicitationAction;
use rmcp::model::ProtocolVersion;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tracing::Instrument;
use uuid::Uuid;

use crate::client_tool_catalog::ClientToolCatalog;
use crate::client_tool_catalog::ClientToolCatalogRevision;
use crate::client_tool_catalog::ToolCatalogSnapshot;
use crate::request_router::ActiveRoute;
use crate::request_router::LeaseControl;
use crate::request_router::McpConnectionRequestRouter;
use crate::request_router::McpRouteAcquireError;
use crate::request_router::McpSessionRoute;
use crate::rmcp_client::AsyncManagedClient;
use crate::rmcp_client::ManagedClient;
use crate::rmcp_client::ManagedClientFuture;
use crate::rmcp_client::StartupOutcomeError;
use crate::rmcp_client::list_tools_for_client_uncached;
use crate::server::McpServerConnectionIdentity;
pub(crate) use crate::server::McpServerConnectionIdentity as McpConnectionIdentity;
use crate::tool_catalog_cache::McpToolCatalogCacheContext;
use crate::tools::ToolInfo;

/// Opt-in wire lifecycle for negotiated stdio. Catalog requests have no wire lease.
pub(crate) const LEASE_CAPABILITY: &str = "codex/connection-lease";
pub(crate) const LEASE_META_KEY: &str = "codex/connectionLease";
const LEASE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// One logical chat's request scope on an exact initialized physical generation.
#[derive(Clone)]
pub(crate) struct ServerLease {
    pub(crate) id: Uuid,
    pub(crate) requests: RequestScope,
}

/// One root family's physical generations and eligible negotiated stdio transports.
#[derive(Clone, Default)]
pub struct McpConnectionPool {
    entries: Arc<Mutex<Vec<PoolEntry>>>,
    transport_entries: Arc<Mutex<Vec<Weak<Entry>>>>,
}

#[derive(Clone)]
pub(crate) struct ConnectionKey {
    pub(crate) server_name: String,
    pub(crate) identity: McpServerConnectionIdentity,
    pub(crate) account: ConnectorRuntimeContextKey,
    pub(crate) auth_state: Option<(Arc<AuthManager>, AuthChangeState)>,
    pub(crate) startup_timeout: Duration,
    pub(crate) catalog_item_limit: usize,
}

impl PartialEq for ConnectionKey {
    fn eq(&self, other: &Self) -> bool {
        self.server_name == other.server_name
            && self.identity == other.identity
            && self.same_owner(other)
            && self.startup_timeout == other.startup_timeout
            && self.catalog_item_limit == other.catalog_item_limit
    }
}

impl ConnectionKey {
    fn same_owner(&self, other: &Self) -> bool {
        self.account == other.account
            && match (&self.auth_state, &other.auth_state) {
                (Some((left, left_state)), Some((right, right_state))) => {
                    Arc::ptr_eq(left, right)
                        && left_state.owner_generation == right_state.owner_generation
                        && left_state.source_generation == right_state.source_generation
                        && left_state.login_generation == right_state.login_generation
                }
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
            }
    }
}

struct Entry {
    key: ConnectionKey,
    request_router: McpConnectionRequestRouter,
    client: ManagedClientFuture,
    capabilities: Arc<Mutex<Option<serde_json::Value>>>,
    // Share immutable definitions; every chat owns its catalog revisions and refresh locks.
    catalog: Mutex<Option<ToolCatalogSnapshot>>,
    cancellation: CancellationToken,
    /// Wakes joiners without cancelling the creator's startup.
    sharing_declined: CancellationToken,
    owners: Weak<LeaseOwners>,
    pool: Weak<Mutex<Vec<Weak<Entry>>>>,
}

/// One strong reference per active chat lease, independent of retained client bindings.
/// Every strong reference must be consumed by `ConnectionLease::release`.
struct LeaseOwners;

#[derive(Clone, Copy, PartialEq, Eq)]
enum AcquisitionRole {
    Creator,
    Joiner,
}

impl Entry {
    fn remove_from_pool(self: &Arc<Self>) {
        if let Some(pool) = self.pool.upgrade() {
            pool.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|entry| !entry.ptr_eq(&Arc::downgrade(self)));
        }
    }

    async fn tool_catalog_for_chat(
        &self,
        client: &ManagedClient,
        role: AcquisitionRole,
        cache_context: Option<&McpToolCatalogCacheContext>,
    ) -> Result<Arc<ClientToolCatalog>, StartupOutcomeError> {
        // Listing must not update another chat's catalog or wait for its calls.
        let listed = if role == AcquisitionRole::Creator {
            client.tool_catalog.read(|snapshot| snapshot).await
        } else {
            let fetch_ticket = cache_context.map(|context| (context, context.begin_fetch()));
            let (tools, meta) = list_tools_for_client_uncached(
                &self.key.server_name,
                self.key.server_name == crate::CODEX_APPS_MCP_SERVER_NAME,
                "shared_connection",
                &client.client,
                Some(self.key.startup_timeout),
                self.key.catalog_item_limit,
                client.server_instructions.as_deref(),
            )
            .await?;
            if let Some((context, mut ticket)) = fetch_ticket {
                if context.track_tool_list_changes {
                    ticket.tool_list_revision = Some(client.client.tool_list_revision());
                }
                context.publish_if_newest(ticket, &tools, meta.clone());
            }
            ToolCatalogSnapshot {
                revision: 0,
                tools: tools.into(),
                meta,
            }
        };
        let mut catalog = self
            .catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let catalog = catalog.get_or_insert_with(|| listed.clone());
        if catalog.tools != listed.tools || catalog.meta != listed.meta {
            *catalog = listed;
        }
        Ok(Arc::new(ClientToolCatalog::new(
            Arc::clone(&catalog.tools),
            catalog.meta.clone(),
            /*updates*/ None,
        )))
    }
}

/// A chat releases its lease explicitly on shutdown, even while bindings retain its client.
pub(crate) struct ConnectionLease {
    entry: Arc<Entry>,
    owner: Mutex<Option<Arc<LeaseOwners>>>,
    cancellation: CancellationToken,
    slot: Arc<ConnectionSlot>,
}

/// Tracks an acquisition without retaining its lease, including before startup completes.
#[derive(Default)]
pub(crate) struct ConnectionSlot {
    state: Mutex<SlotState>,
    shutdown_tasks: OnceLock<TaskTracker>,
}

#[derive(Default)]
enum SlotState {
    #[default]
    Available,
    Acquired(Weak<Entry>),
    Retired,
}

impl ConnectionSlot {
    pub(crate) fn retire(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let SlotState::Acquired(entry) = std::mem::replace(&mut *state, SlotState::Retired)
            && let Some(entry) = entry.upgrade()
        {
            entry.remove_from_pool();
        }
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        if let Some(tasks) = self.shutdown_tasks.get() {
            tasks.close();
            tasks.wait().await;
        }
    }
}

/// Whether manager construction may reuse or replace the compatible tree connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum McpConnectionPoolMode {
    #[default]
    Reuse,
    /// Atomically replaces the connection followed by every compatible tree member.
    Replace,
}

type ConnectionFactory =
    Arc<dyn Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync>;

const RETIREMENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static LIVE_CONNECTIONS: Gauge = Gauge::new("mcp.connections.live");

pub(crate) struct McpEmaStartupRoute {
    route: Arc<McpSessionRoute>,
}

impl McpEmaStartupRoute {
    pub(crate) fn new(route: Arc<McpSessionRoute>) -> Self {
        Self { route }
    }

    pub(crate) fn route(&self) -> Arc<McpSessionRoute> {
        Arc::clone(&self.route)
    }
}

impl Drop for McpEmaStartupRoute {
    fn drop(&mut self) {
        self.route.close();
    }
}

async fn shutdown_retired_connection(client: Arc<AsyncManagedClient>) {
    if tokio::time::timeout(RETIREMENT_SHUTDOWN_TIMEOUT, client.shutdown())
        .await
        .is_err()
    {
        tracing::warn!("timed out shutting down retired MCP connection");
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RetirementCause {
    Active,
    Request,
    Terminal,
}

struct SharedConnection {
    /// Process-local identity used to bind cached catalogs to this exact generation.
    id: u64,
    client: Arc<AsyncManagedClient>,
    startup_permit: Arc<AtomicBool>,
    owners: AtomicUsize,
    shutdown_started: AtomicBool,
    // Serializes terminal retirement with replacement admission across logical slots.
    retirement: Mutex<RetirementCause>,
    shutdown_tasks: TaskTracker,
    shutdown_reservation: Mutex<Option<TaskTrackerToken>>,
    _diagnostics_guard: GaugeGuard,
}

impl SharedConnection {
    fn new(mut client: AsyncManagedClient) -> Self {
        let startup_permit = Arc::new(AtomicBool::new(true));
        if client.client.peek().is_none() {
            let permit = Arc::clone(&startup_permit);
            let startup = client.client.clone();
            let startup_complete = Arc::clone(&client.startup_complete);
            client.client = async move {
                // Cleanup must not start an initializer superseded before its first poll.
                if !permit.swap(false, Ordering::AcqRel) {
                    startup_complete.store(true, Ordering::Release);
                    return Err(StartupOutcomeError::Cancelled);
                }
                startup.await
            }
            .boxed()
            .shared();
        }
        let shutdown_tasks = TaskTracker::new();
        let shutdown_reservation = shutdown_tasks.token();
        Self {
            id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            client: Arc::new(client),
            startup_permit,
            owners: AtomicUsize::new(1),
            shutdown_started: AtomicBool::new(false),
            retirement: Mutex::new(RetirementCause::Active),
            shutdown_tasks,
            shutdown_reservation: Mutex::new(Some(shutdown_reservation)),
            _diagnostics_guard: LIVE_CONNECTIONS.track(),
        }
    }

    fn check_new_request(&self) -> Result<()> {
        if self.owners.load(Ordering::Acquire) == 0
            || *self
                .retirement
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                != RetirementCause::Active
            || self.shutdown_started.load(Ordering::Acquire)
            || self.client.cancel_token.is_cancelled()
        {
            return Err(anyhow!("shared MCP connection closed"));
        }
        if let Some(authority) = &self.client.ema_authority {
            authority.check_new_request()?;
        }
        Ok(())
    }

    fn try_retain_owner(&self) -> bool {
        self.owners
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |owners| {
                (owners != 0).then(|| owners.checked_add(1)).flatten()
            })
            .is_ok()
    }

    fn release_owner(&self) -> bool {
        if self.owners.fetch_sub(1, Ordering::AcqRel) != 1 {
            return false;
        }
        self.start_shutdown();
        true
    }

    fn retire_acquisition(&self) {
        if let Some(slot) = &self.client.connection_slot {
            slot.retire();
        }
    }

    fn supersede_startup(&self) {
        self.startup_permit.store(false, Ordering::Release);
    }

    fn start_shutdown(&self) {
        *self
            .retirement
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = RetirementCause::Terminal;
        self.start_retired_shutdown();
    }

    fn retire_request(&self) -> bool {
        let mut cause = self
            .retirement
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *cause == RetirementCause::Terminal {
            return false;
        }
        *cause = RetirementCause::Request;
        true
    }

    fn start_retired_shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        self.retire_acquisition();
        self.client.request_router.close();
        if let Some(authority) = &self.client.ema_authority {
            authority.revoke();
        }
        self.client.cancel_token.cancel();
        if let Some(client) = self.client.ready_client()
            && let ConnectionSharing::Owned(lease) | ConnectionSharing::Shared(lease) =
                &client.connection_sharing
        {
            let _ = lease.release();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let client = Arc::clone(&self.client);
            drop(self.shutdown_tasks.spawn_on(
                async move { shutdown_retired_connection(client).await },
                &runtime,
            ));
        }
        // Keep the tracker nonempty until cleanup has been handed to its owned task.
        self.shutdown_reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    async fn shutdown(&self) {
        self.start_shutdown();
        self.shutdown_tasks.close();
        self.shutdown_tasks.wait().await;
    }
}

impl Drop for SharedConnection {
    fn drop(&mut self) {
        self.start_shutdown();
    }
}

struct ConnectionSlotState {
    generation: u64,
    superseded: CancellationToken,
    connection: Arc<SharedConnection>,
    retired: Vec<RetainedGeneration>,
    fallback_shutdowns: Vec<TaskTracker>,
}

struct RetainedGeneration {
    connection: Weak<SharedConnection>,
    shutdown_tasks: TaskTracker,
}

impl RetainedGeneration {
    fn new(connection: &Arc<SharedConnection>) -> Self {
        Self {
            connection: Arc::downgrade(connection),
            shutdown_tasks: connection.shutdown_tasks.clone(),
        }
    }
}

pub(crate) struct McpLegacyFallback {
    connection: Option<Arc<SharedConnection>>,
}

impl McpLegacyFallback {
    fn release(mut self) -> Option<TaskTracker> {
        let connection = self.connection.take()?;
        connection
            .release_owner()
            .then(|| connection.shutdown_tasks.clone())
    }
}

impl Drop for McpLegacyFallback {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            connection.release_owner();
        }
    }
}

struct RootConnectionSlot {
    /// Present for pooled connections and absent for test-only standalone leases.
    key: Mutex<Option<ConnectionKey>>,
    state: Mutex<ConnectionSlotState>,
    factory: Mutex<(Option<ConnectionKey>, ConnectionFactory)>,
    routes: Mutex<Vec<Weak<McpSessionRoute>>>,
    // Only views own this route; a pending initializer must not keep itself authorized.
    ema_startup_route: Option<Weak<McpEmaStartupRoute>>,
    active_leases: AtomicUsize,
    legacy_fallback: Mutex<Option<McpLegacyFallback>>,
    retained_legacy: AtomicBool,
}

impl RootConnectionSlot {
    fn new(
        key: ConnectionKey,
        factory: ConnectionFactory,
        route: &Arc<McpSessionRoute>,
        ema_startup_route: Option<&Arc<McpEmaStartupRoute>>,
    ) -> Arc<Self> {
        let request_router = McpConnectionRequestRouter::default();
        request_router.register(route);
        let mut routes = vec![Arc::downgrade(route)];
        if let Some(ema_startup_route) = ema_startup_route {
            request_router.register(&ema_startup_route.route);
            routes.push(Arc::downgrade(&ema_startup_route.route));
        }
        let connection = Arc::new(SharedConnection::new(factory(request_router)));
        Arc::new(Self {
            key: Mutex::new(Some(key.clone())),
            state: Mutex::new(ConnectionSlotState {
                generation: 0,
                superseded: CancellationToken::new(),
                connection,
                retired: Vec::new(),
                fallback_shutdowns: Vec::new(),
            }),
            factory: Mutex::new((Some(key), factory)),
            routes: Mutex::new(routes),
            ema_startup_route: ema_startup_route.map(Arc::downgrade),
            active_leases: AtomicUsize::new(1),
            legacy_fallback: Mutex::new(None),
            retained_legacy: AtomicBool::new(false),
        })
    }

    fn matches_key(&self, desired: &ConnectionKey) -> bool {
        self.key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|key| {
                key.server_name == desired.server_name
                    && key.identity == desired.identity
                    && key.same_owner(desired)
                    && key.catalog_item_limit == desired.catalog_item_limit
                    && (self
                        .current()
                        .client
                        .startup_complete
                        .load(Ordering::Acquire)
                        || key.startup_timeout == desired.startup_timeout)
            })
    }

    fn has_config_for_key(&self, desired: &ConnectionKey) -> bool {
        self.key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|key| {
                key.server_name == desired.server_name
                    && key.identity.has_same_connection_config(&desired.identity)
                    && key.same_owner(desired)
                    && key.catalog_item_limit == desired.catalog_item_limit
            })
    }

    async fn reconcile_ready_key(&self, desired: &ConnectionKey) -> Option<bool> {
        let previous_key = self
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        let observed = self.current();
        if !observed.client.startup_complete.load(Ordering::Acquire) {
            return Some(previous_key == *desired && observed.check_new_request().is_ok());
        }
        let reusable = self.is_reusable_connection(&desired.identity).await;
        let mut key = self
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if key.as_ref() != Some(&previous_key) || !Arc::ptr_eq(&state.connection, &observed) {
            return None;
        }
        if reusable && previous_key.same_owner(desired) {
            *key = Some(desired.clone());
        }
        Some(reusable)
    }

    fn prune_retired(state: &mut ConnectionSlotState) {
        state.retired.retain(|retired| {
            retired.connection.strong_count() > 0 || !retired.shutdown_tasks.is_empty()
        });
        state.fallback_shutdowns.retain(|tasks| !tasks.is_empty());
    }

    fn advance_generation(state: &mut ConnectionSlotState) {
        state.generation = state.generation.wrapping_add(1);
        state.superseded.cancel();
        state.superseded = CancellationToken::new();
    }

    fn superseded_for(&self, connection: &Arc<SharedConnection>) -> Option<CancellationToken> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::ptr_eq(&state.connection, connection).then(|| state.superseded.clone())
    }

    fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
    }

    fn discard_fallback(&self, state: &mut ConnectionSlotState) {
        if let Some(fallback) = self
            .legacy_fallback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            && let Some(tasks) = fallback.release()
        {
            state.fallback_shutdowns.push(tasks);
        }
    }

    fn finish_startup(&self, observed: &Arc<SharedConnection>, generation: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation == generation && Arc::ptr_eq(&state.connection, observed) {
            self.discard_fallback(&mut state);
            true
        } else {
            false
        }
    }

    fn install_legacy_fallback(&self, fallback: McpLegacyFallback) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.active_leases.load(Ordering::Acquire) == 0
            || state
                .connection
                .client
                .startup_complete
                .load(Ordering::Acquire)
        {
            return;
        }
        let mut pending = self
            .legacy_fallback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pending.is_none() {
            *pending = Some(fallback);
        }
    }

    async fn adopt_legacy_after_failure(
        &self,
        observed: &Arc<SharedConnection>,
        generation: u64,
        error: &StartupOutcomeError,
    ) -> bool {
        let StartupOutcomeError::Failed { shutdown, .. } = error else {
            return false;
        };
        if error.is_authentication_required() || observed.client.cancel_token.is_cancelled() {
            return false;
        }
        if let Some(shutdown) = shutdown
            && shutdown.clone().await.is_err()
        {
            return false;
        }
        let candidate = self
            .legacy_fallback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|fallback| fallback.connection.clone());
        let Some(candidate) = candidate else {
            return false;
        };
        let Some(client) = candidate.client.ready_client() else {
            return false;
        };
        if client.client.is_closed().await {
            return false;
        }
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation != generation
            || !Arc::ptr_eq(&state.connection, observed)
            || self.active_leases.load(Ordering::Acquire) == 0
            || observed.client.cancel_token.is_cancelled()
        {
            return false;
        }
        let mut pending = self
            .legacy_fallback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(fallback) = pending.as_mut() else {
            return false;
        };
        let Some(connection) = fallback.connection.as_ref() else {
            return false;
        };
        if !Arc::ptr_eq(connection, &candidate) || connection.check_new_request().is_err() {
            return false;
        }
        routes.retain(|route| route.upgrade().is_some());
        for route in routes.iter().filter_map(Weak::upgrade) {
            connection.client.register_route(&route);
        }
        // Transfer the fallback's physical ownership, not just its ready client.
        let connection = fallback
            .connection
            .take()
            .expect("checked fallback generation");
        pending.take();
        Self::prune_retired(&mut state);
        state.retired.push(RetainedGeneration::new(observed));
        state.connection = connection;
        Self::advance_generation(&mut state);
        self.retained_legacy.store(true, Ordering::Release);
        observed.start_shutdown();
        true
    }

    fn try_acquire(&self, route: &Arc<McpSessionRoute>) -> bool {
        if self
            .ema_startup_route
            .as_ref()
            .is_some_and(|owner| owner.upgrade().is_none_or(|owner| owner.route.is_closed()))
        {
            return false;
        }
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut active = self.active_leases.load(Ordering::Acquire);
        loop {
            if active == 0 {
                return false;
            }
            let Some(incremented) = active.checked_add(1) else {
                return false;
            };
            match self.active_leases.compare_exchange_weak(
                active,
                incremented,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.register_route_locked(&mut routes, route);
                    return true;
                }
                Err(current) => active = current,
            }
        }
    }

    fn register_route(&self, route: &Arc<McpSessionRoute>) {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.register_route_locked(&mut routes, route);
    }

    fn register_route_locked(
        &self,
        routes: &mut Vec<Weak<McpSessionRoute>>,
        route: &Arc<McpSessionRoute>,
    ) {
        routes.retain(|existing| existing.upgrade().is_some());
        if !routes
            .iter()
            .filter_map(Weak::upgrade)
            .any(|existing| Arc::ptr_eq(&existing, route))
        {
            routes.push(Arc::downgrade(route));
        }
        self.current().client.register_route(route);
    }

    fn unregister_route(&self, route: &Arc<McpSessionRoute>) {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|existing| {
            existing
                .upgrade()
                .is_some_and(|existing| !Arc::ptr_eq(&existing, route))
        });
        drop(routes);
        self.current().client.unregister_route(route);
    }

    fn has_live_route(&self) -> bool {
        self.first_live_route().is_some()
    }

    fn cancel_startup_if_sole_route(
        &self,
        connection: &Arc<SharedConnection>,
        excluded: &Arc<McpSessionRoute>,
    ) -> bool {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.active_leases.load(Ordering::Acquire) == 0
            || !Arc::ptr_eq(&state.connection, connection)
            || connection.owners.load(Ordering::Acquire) != 1
            || routes
                .iter()
                .filter_map(Weak::upgrade)
                .any(|route| !route.is_closed() && !Arc::ptr_eq(&route, excluded))
        {
            return false;
        }
        connection.start_shutdown();
        true
    }

    fn first_live_route(&self) -> Option<Arc<McpSessionRoute>> {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        routes
            .iter()
            .filter_map(Weak::upgrade)
            .find(|route| !route.is_closed())
    }

    fn current(&self) -> Arc<SharedConnection> {
        Arc::clone(
            &self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .connection,
        )
    }

    async fn is_reusable_connection(&self, desired: &McpConnectionIdentity) -> bool {
        let Some(current_identity) = self
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|key| key.identity.clone())
        else {
            return false;
        };
        if !current_identity.has_same_connection_config(desired) {
            return false;
        }
        let connection = self.current();
        if connection.check_new_request().is_err()
            || (desired.is_ema() && connection.client.ema_authority.is_none())
            || !connection.client.startup_complete.load(Ordering::Acquire)
        {
            return false;
        }
        let Ok(client) = connection.client.client().await else {
            return false;
        };
        if client.client.is_closed().await {
            return false;
        }
        if current_identity == *desired {
            return !(matches!(desired.oauth_credentials(), Ok(None))
                && tokio::time::timeout(
                    Duration::ZERO,
                    client.client.managed_oauth_credentials(),
                )
                .await
                .is_ok_and(|credentials| matches!(credentials, Some(Some(_)))));
        }
        let Ok(desired_credentials) = desired.oauth_credentials() else {
            return true;
        };
        match client.client.managed_oauth_credentials().await {
            Some(live_credentials) => live_credentials.as_ref() == desired_credentials,
            None => current_identity
                .oauth_credentials()
                .is_ok_and(|credentials| credentials == desired_credentials),
        }
    }

    fn replace(&self, key: ConnectionKey, factory: ConnectionFactory) -> Arc<SharedConnection> {
        let mut current_key = self
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *self
            .factory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            (Some(key.clone()), Arc::clone(&factory));
        let previous = self.replace_with(factory);
        *current_key = Some(key);
        previous
    }

    fn replace_if_current(
        &self,
        current: &Arc<SharedConnection>,
        preferred_route: Option<&Arc<McpSessionRoute>>,
    ) -> Option<(Arc<SharedConnection>, Arc<McpSessionRoute>)> {
        self.replace_if_current_for_lease(current, preferred_route, None)
    }

    fn replace_if_current_for_lease(
        &self,
        current: &Arc<SharedConnection>,
        preferred_route: Option<&Arc<McpSessionRoute>>,
        recovering_lease: Option<&LeaseInner>,
    ) -> Option<(Arc<SharedConnection>, Arc<McpSessionRoute>)> {
        let key = self
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = key.as_ref()?;
        if key.identity.is_ema() {
            // Only the owning runtime may retain its original EMA connection.
            return None;
        }
        let factory = self
            .factory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if factory.0.as_ref() != Some(key) {
            return None;
        }
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        let live_routes = routes
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|route| !route.is_closed())
            .collect::<Vec<_>>();
        if live_routes.is_empty() || self.active_leases.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !Arc::ptr_eq(&state.connection, current) {
            return None;
        }
        let cause = current
            .retirement
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *cause == RetirementCause::Terminal
            || recovering_lease.is_some_and(|lease| {
                *cause != RetirementCause::Request
                    || lease.released.load(Ordering::Acquire)
                    || lease.cancellation.is_cancelled()
                    || preferred_route.is_none_or(|route| route.is_closed())
            })
        {
            return None;
        }
        if let Some((manager, expected)) = &key.auth_state {
            let live = *manager.auth_change_state_receiver().borrow();
            if live.owner_generation != expected.owner_generation
                || live.source_generation != expected.source_generation
                || live.login_generation != expected.login_generation
            {
                return None;
            }
        }
        current.supersede_startup();
        current.retire_acquisition();
        Self::prune_retired(&mut state);
        self.discard_fallback(&mut state);
        let replacement = Self::make_connection_for_routes(Arc::clone(&factory.1), &live_routes);
        let startup_route = preferred_route
            .filter(|preferred| {
                live_routes
                    .iter()
                    .any(|route| Arc::ptr_eq(route, preferred))
            })
            .cloned()
            .unwrap_or_else(|| Arc::clone(&live_routes[0]));
        Self::advance_generation(&mut state);
        state.retired.push(RetainedGeneration::new(current));
        state.connection = Arc::clone(&replacement);
        self.retained_legacy.store(false, Ordering::Release);
        current.client.request_router.close();
        current.client.cancel_token.cancel();
        Some((replacement, startup_route))
    }
}

#[derive(Clone, Default)]
pub(crate) enum ConnectionSharing {
    #[default]
    Private,
    Negotiated,
    /// The server declined sharing, but its creator still owns the startup entry.
    Owned(Arc<ConnectionLease>),
    Shared(Arc<ConnectionLease>),
}

impl McpConnectionPool {
    pub(crate) async fn connect(
        &self,
        slot: Arc<ConnectionSlot>,
        key: ConnectionKey,
        request_router: McpConnectionRequestRouter,
        capabilities: Arc<Mutex<Option<serde_json::Value>>>,
        tool_catalog_cache_context: Option<McpToolCatalogCacheContext>,
        cancellation: CancellationToken,
        start: impl Fn(CancellationToken, Option<CancellationToken>) -> ManagedClientFuture + Sync,
    ) -> Result<ManagedClient, StartupOutcomeError> {
        let (lease, role) = self
            .acquire_transport(
                key,
                &slot,
                &request_router,
                Arc::clone(&capabilities),
                &cancellation,
                &start,
            )
            .or_cancel(&cancellation)
            .await
            .unwrap_or(Err(StartupOutcomeError::Cancelled))?;
        let outcome = tokio::select! {
            outcome = lease.entry.client.clone() => Some(outcome),
            () = lease.entry.sharing_declined.cancelled(), if role == AcquisitionRole::Joiner => None,
            () = cancellation.cancelled() => return Err(StartupOutcomeError::Cancelled),
        };
        match outcome {
            Some(Ok(mut client))
                if role == AcquisitionRole::Creator
                    || matches!(client.connection_sharing, ConnectionSharing::Negotiated) =>
            {
                let shared_capabilities = lease
                    .entry
                    .capabilities
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                *capabilities
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = shared_capabilities;
                client.connection_sharing =
                    if matches!(client.connection_sharing, ConnectionSharing::Negotiated) {
                        client.client.enable_request_cancellation();
                        client.tool_catalog = lease
                            .entry
                            .tool_catalog_for_chat(
                                &client,
                                role,
                                tool_catalog_cache_context.as_ref(),
                            )
                            .or_cancel(&cancellation)
                            .await
                            .unwrap_or(Err(StartupOutcomeError::Cancelled))?;
                        ConnectionSharing::Shared(lease)
                    } else {
                        lease.entry.remove_from_pool();
                        ConnectionSharing::Owned(lease)
                    };
                Ok(client)
            }
            Some(Err(error)) if role == AcquisitionRole::Creator => Err(error),
            _ => {
                // Older or failed servers leave other chats to initialize privately
                // with their own elicitation authority.
                drop(lease);
                start(cancellation, /*sharing_declined*/ None).await
            }
        }
    }

    async fn acquire_transport(
        &self,
        key: ConnectionKey,
        slot: &Arc<ConnectionSlot>,
        request_router: &McpConnectionRequestRouter,
        capabilities: Arc<Mutex<Option<serde_json::Value>>>,
        cancellation: &CancellationToken,
        start: impl Fn(CancellationToken, Option<CancellationToken>) -> ManagedClientFuture,
    ) -> Result<(Arc<ConnectionLease>, AcquisitionRole), StartupOutcomeError> {
        loop {
            let (lease, role) = {
                let mut state = slot
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if matches!(&*state, SlotState::Retired) {
                    return Err(StartupOutcomeError::Cancelled);
                }
                let mut entries = self
                    .transport_entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                entries.retain(|entry| entry.strong_count() > 0);
                let lease_cancellation = cancellation.child_token();
                let existing = entries
                    .iter()
                    .filter_map(Weak::upgrade)
                    .filter(|entry| {
                        entry.key == key
                            && entry.request_router.same_routing_domain(request_router)
                            && !entry.sharing_declined.is_cancelled()
                            && entry.client.peek().is_none_or(|result| {
                                result.as_ref().is_ok_and(|client| {
                                    matches!(
                                        client.connection_sharing,
                                        ConnectionSharing::Negotiated
                                    )
                                })
                            })
                    })
                    .find_map(|entry| entry.owners.upgrade().map(|owner| (entry, owner)));
                let (entry, owner, role) = if let Some((entry, owner)) = existing {
                    (entry, owner, AcquisitionRole::Joiner)
                } else {
                    let startup_cancellation = CancellationToken::new();
                    let sharing_declined = CancellationToken::new();
                    let client =
                        start(startup_cancellation.clone(), Some(sharing_declined.clone()));
                    // Startup belongs to the connection, including if its first chat is cancelled.
                    tokio::spawn(client.clone());
                    let owner = Arc::new(LeaseOwners);
                    let entry = Arc::new(Entry {
                        key: key.clone(),
                        request_router: request_router.clone(),
                        client,
                        capabilities: Arc::clone(&capabilities),
                        catalog: Mutex::new(None),
                        cancellation: startup_cancellation,
                        sharing_declined,
                        owners: Arc::downgrade(&owner),
                        pool: Arc::downgrade(&self.transport_entries),
                    });
                    entries.push(Arc::downgrade(&entry));
                    (entry, owner, AcquisitionRole::Creator)
                };
                let lease = Arc::new(ConnectionLease {
                    entry,
                    owner: Mutex::new(Some(owner)),
                    cancellation: lease_cancellation,
                    slot: Arc::clone(slot),
                });
                *state = SlotState::Acquired(Arc::downgrade(&lease.entry));
                (lease, role)
            };
            if let Some(Ok(client)) = lease.entry.client.peek()
                && client.client.is_closed().await
            {
                lease.entry.remove_from_pool();
                lease.release();
                continue;
            }
            return Ok((lease, role));
        }
    }
}

impl ConnectionLease {
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the slot tracking shutdown when this chat releases the last lease.
    pub(crate) fn release(&self) -> Option<&ConnectionSlot> {
        self.cancellation.cancel();
        let mut owner = self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::into_inner(owner.take()?)?;
        self.entry.cancellation.cancel();
        self.entry.remove_from_pool();
        let client = self.entry.client.clone();
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        // The chat must be able to await its final release even when cancellation drops the lease.
        let tasks = self.slot.shutdown_tasks.get_or_init(TaskTracker::new);
        drop(tasks.spawn_on(
            async move {
                match client.await {
                    Ok(client) => client.client.shutdown().await,
                    Err(error) => error.wait_for_shutdown().await,
                }
            },
            &runtime,
        ));
        Some(&self.slot)
    }
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

/// Releases waiting chats when startup becomes private and detaches callbacks after sharing.
pub(crate) struct SharingNegotiation {
    callback: Arc<Mutex<Option<SendElicitation>>>,
    sharing_declined: CancellationToken,
}

impl SharingNegotiation {
    pub(crate) fn new(
        sender: SendElicitation,
        sharing_declined: CancellationToken,
    ) -> (Self, SendElicitation) {
        let callback = Arc::new(Mutex::new(Some(sender)));
        let negotiation = Self {
            callback: Arc::clone(&callback),
            sharing_declined: sharing_declined.clone(),
        };
        let sender: SendElicitation = Box::new(move |id, request| {
            let callback = callback
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(callback) = callback.as_ref() {
                // Client input binds this startup to its original chat.
                sharing_declined.cancel();
                callback(id, request)
            } else {
                futures::future::ready(Ok(ElicitationResponse {
                    action: ElicitationAction::Cancel,
                    content: None,
                    meta: None,
                }))
                .boxed()
            }
        });
        (negotiation, sender)
    }

    pub(crate) fn finish(self, protocol_version: &ProtocolVersion) -> ConnectionSharing {
        let mut callback = self
            .callback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *protocol_version == ProtocolVersion::V_2026_07_28
            && !self.sharing_declined.is_cancelled()
        {
            callback.take();
            ConnectionSharing::Negotiated
        } else {
            self.sharing_declined.cancel();
            ConnectionSharing::Private
        }
    }
}

impl RootConnectionSlot {
    fn replace_with(&self, factory: ConnectionFactory) -> Arc<SharedConnection> {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        let live_routes = routes
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|route| !route.is_closed())
            .collect::<Vec<_>>();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.connection.supersede_startup();
        state.connection.retire_acquisition();
        Self::prune_retired(&mut state);
        self.discard_fallback(&mut state);
        let replacement = Self::make_connection_for_routes(factory, &live_routes);
        Self::advance_generation(&mut state);
        let previous = std::mem::replace(&mut state.connection, replacement);
        state.retired.push(RetainedGeneration::new(&previous));
        self.retained_legacy.store(false, Ordering::Release);
        previous
    }

    fn make_connection_for_routes(
        factory: ConnectionFactory,
        routes: &[Arc<McpSessionRoute>],
    ) -> Arc<SharedConnection> {
        let request_router = McpConnectionRequestRouter::default();
        for route in routes {
            request_router.register(route);
        }
        Arc::new(SharedConnection::new(factory(request_router)))
    }

    fn release(&self) -> bool {
        let _routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let last = self.active_leases.fetch_sub(1, Ordering::AcqRel) == 1;
        if last && let Some(owner) = self.ema_startup_route.as_ref().and_then(Weak::upgrade) {
            owner.route.close();
        }
        if last {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut generations = vec![Arc::clone(&state.connection)];
            for retired in &state.retired {
                if let Some(connection) = retired.connection.upgrade()
                    && !generations
                        .iter()
                        .any(|other| Arc::ptr_eq(other, &connection))
                {
                    generations.push(connection);
                }
            }
            for connection in generations {
                connection.release_owner();
            }
            self.discard_fallback(&mut state);
        }
        last
    }

    async fn wait_for_shutdown(&self) {
        let tasks = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tasks = state.fallback_shutdowns.clone();
            for generation in std::iter::once(RetainedGeneration::new(&state.connection)).chain(
                state.retired.iter().map(|retired| RetainedGeneration {
                    connection: retired.connection.clone(),
                    shutdown_tasks: retired.shutdown_tasks.clone(),
                }),
            ) {
                if generation
                    .connection
                    .upgrade()
                    .is_none_or(|connection| connection.owners.load(Ordering::Acquire) == 0)
                {
                    tasks.push(generation.shutdown_tasks);
                }
            }
            tasks
        };
        for tasks in &tasks {
            tasks.close();
        }
        futures::future::join_all(tasks.iter().map(TaskTracker::wait)).await;
    }

    #[cfg(test)]
    fn from_client(client: AsyncManagedClient) -> Arc<Self> {
        Arc::new(Self {
            key: Mutex::new(None),
            state: Mutex::new(ConnectionSlotState {
                generation: 0,
                superseded: CancellationToken::new(),
                connection: Arc::new(SharedConnection::new(client)),
                retired: Vec::new(),
                fallback_shutdowns: Vec::new(),
            }),
            factory: Mutex::new((
                None,
                Arc::new(|_| {
                    panic!("test-only standalone MCP lease cannot replace its connection")
                }),
            )),
            routes: Mutex::new(Vec::new()),
            ema_startup_route: None,
            active_leases: AtomicUsize::new(1),
            legacy_fallback: Mutex::new(None),
            retained_legacy: AtomicBool::new(false),
        })
    }
}

struct PoolEntry {
    slot: Weak<RootConnectionSlot>,
}

struct LeaseInner {
    slot: Arc<RootConnectionSlot>,
    released: AtomicBool,
    cancellation: CancellationToken,
    runtime: Option<tokio::runtime::Handle>,
    wire_leases: Mutex<Vec<Arc<WireLease>>>,
    wire_tasks: TaskTracker,
}

/// Keeps the exact transport owner alive until wire control has finished or the transport is lost.
struct WireConnectionOwner(Arc<SharedConnection>);

impl Drop for WireConnectionOwner {
    fn drop(&mut self) {
        self.0.release_owner();
    }
}

/// Wire state for one logical chat on one physical transport. A single worker owns both frames.
pub(crate) struct WireLease {
    protocol: ServerLease,
    physical: McpPooledClient,
    client: Arc<RmcpClient>,
    closing: CancellationToken,
    retiring: CancellationToken,
    opened: watch::Receiver<Option<Result<(), StartupOutcomeError>>>,
    disposition: Mutex<WireLeaseDisposition>,
}

#[derive(Default)]
struct WireLeaseDisposition {
    finished: bool,
    release_control: Option<LeaseControl>,
}

impl WireLease {
    fn close_locked(&self, disposition: &mut WireLeaseDisposition, retire: bool) {
        if disposition.finished {
            return;
        }
        if retire {
            self.retiring.cancel();
        }
        if !self.closing.is_cancelled() {
            disposition.release_control = self
                .physical
                .connection
                .client
                .request_router
                .reserve_lease_control(Arc::clone(&self.physical.route))
                .ok();
            self.closing.cancel();
        }
    }

    fn close(&self, retire: bool) {
        let mut disposition = self
            .disposition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut disposition, retire);
    }

    fn is_finished(&self) -> bool {
        self.disposition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finished
    }

    async fn wait_open(&self) -> Result<()> {
        let mut opened = self.opened.clone();
        let observed =
            tokio::time::timeout(LEASE_REQUEST_TIMEOUT, opened.wait_for(Option::is_some)).await;
        if let Ok(observed) = observed {
            return observed
                .map_err(|_| anyhow!("MCP lease open task exited"))?
                .as_ref()
                .expect("open outcome is available")
                .clone()
                .map_err(Into::into);
        }
        let mut disposition = self
            .disposition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(outcome) = self.opened.borrow().as_ref() {
            return outcome.clone().map_err(Into::into);
        }
        self.close_locked(&mut disposition, /*retire*/ true);
        Err(codex_rmcp_client::request_timeout_error(
            "codex/connection/open",
            LEASE_REQUEST_TIMEOUT,
        ))
    }

    async fn run(
        &self,
        mut open_control: LeaseControl,
        opened: watch::Sender<Option<Result<(), StartupOutcomeError>>>,
    ) {
        // The wire budget leaves time to observe a late acknowledgement after the caller times
        // out, but it cannot hold a retiring physical generation indefinitely.
        let budget = RETIREMENT_SHUTDOWN_TIMEOUT;
        let mut entered = false;
        let outcome = tokio::time::timeout(budget, async {
            open_control.enter().await.map_err(anyhow::Error::from)?;
            if self.closing.is_cancelled()
                || self.physical.route.is_closed()
                || !self.physical.is_current()
            {
                return Err(StartupOutcomeError::Cancelled);
            }
            self.physical.connection.check_new_request()?;
            entered = true;
            self.client
                .send_custom_request_with_timeout(
                    "codex/connection/open",
                    Some(serde_json::json!({"leaseId": self.protocol.id})),
                    Some(budget),
                )
                .await
                .map(|_| ())
                .map_err(StartupOutcomeError::from)
        })
        .await
        .unwrap_or_else(|_| {
            Err(codex_rmcp_client::request_timeout_error("codex/connection/open", budget).into())
        });
        if outcome.is_err() {
            if entered {
                self.physical.retire_after_failure();
            }
            let mut disposition = self
                .disposition
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            disposition.release_control.take();
            disposition.finished = true;
            opened.send_replace(Some(outcome));
            return;
        }
        let open_control = {
            let mut disposition = self
                .disposition
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let keep_control = self.closing.is_cancelled();
            if keep_control {
                // A waiter already gave up. Reuse the held control through release without
                // reopening the router between the acknowledgement and cleanup.
                disposition.release_control.take();
            }
            opened.send_replace(Some(Ok(())));
            if keep_control {
                Some(open_control)
            } else {
                drop(open_control);
                None
            }
        };
        let lost = tokio::select! {
            biased;
            () = self.physical.connection.client.cancel_token.cancelled() => true,
            () = self.closing.cancelled() => false,
        };
        if lost {
            let mut disposition = self
                .disposition
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            disposition.release_control.take();
            disposition.finished = true;
            return;
        }
        let release_control = open_control.or_else(|| {
            self.disposition
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .release_control
                .take()
        });
        if let Some(mut control) = release_control {
            let attempt = async {
                control.enter().await?;
                tokio::time::timeout(budget, async {
                    self.protocol.requests.cancel_and_wait().await;
                    self.client
                        .send_custom_request_with_timeout(
                            "codex/connection/release",
                            Some(serde_json::json!({"leaseId": self.protocol.id})),
                            Some(budget),
                        )
                        .await
                })
                .await??;
                anyhow::Ok(())
            };
            tokio::pin!(attempt);
            // An idle chat's release must not evict a sibling simply for being busy.
            // Once active attribution is lost, bound the whole retirement instead.
            let result = tokio::select! {
                biased;
                result = &mut attempt => result,
                () = self.retiring.cancelled() => tokio::time::timeout(budget, &mut attempt).await
                    .unwrap_or_else(|error| Err(error.into())),
            };
            if self.retiring.is_cancelled() || result.is_err() {
                self.physical.retire_after_failure();
            }
            // Drop the control only after retiring a generation that lost attribution.
        } else {
            self.physical.retire_after_failure();
        }
        self.disposition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finished = true;
    }
}

impl LeaseInner {
    fn wire_lease(
        &self,
        physical: &McpPooledClient,
        managed: &ManagedClient,
    ) -> Result<Option<Arc<WireLease>>> {
        if !matches!(managed.connection_sharing, ConnectionSharing::Shared(_))
            || !physical
                .connection
                .client
                .server_capabilities
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .and_then(|caps| caps.get("extensions"))
                .and_then(|extensions| extensions.get(LEASE_CAPABILITY))
                .is_some_and(serde_json::Value::is_object)
        {
            return Ok(None);
        }
        let mut leases = self
            .wire_leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        anyhow::ensure!(
            !self.released.load(Ordering::Acquire) && !physical.route.is_closed(),
            "MCP connection lease released"
        );
        anyhow::ensure!(
            physical.is_current(),
            "shared MCP generation was superseded"
        );
        physical.connection.check_new_request()?;
        leases.retain(|lease| {
            lease.physical.connection.id == physical.connection.id || !lease.is_finished()
        });
        if let Some(existing) = leases
            .iter()
            .find(|lease| lease.physical.connection.id == physical.connection.id)
        {
            anyhow::ensure!(
                Arc::ptr_eq(&existing.client, &managed.client),
                "MCP lease transport changed"
            );
            anyhow::ensure!(
                !existing.closing.is_cancelled(),
                "MCP wire lease is closing"
            );
            return Ok(Some(Arc::clone(existing)));
        }
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| anyhow!("MCP lease has no owning runtime"))?;
        let control = physical
            .connection
            .client
            .request_router
            .reserve_lease_control(Arc::clone(&physical.route))?;
        anyhow::ensure!(
            physical.connection.try_retain_owner(),
            "shared MCP generation was released"
        );
        let owner = WireConnectionOwner(Arc::clone(&physical.connection));
        let (opened, outcome) = watch::channel(None);
        let closing = self.cancellation.child_token();
        let lease = Arc::new(WireLease {
            protocol: ServerLease {
                id: Uuid::new_v4(),
                requests: RequestScope::new(runtime.clone(), closing.clone()),
            },
            physical: physical.clone(),
            client: Arc::clone(&managed.client),
            closing,
            retiring: CancellationToken::new(),
            opened: outcome,
            disposition: Mutex::default(),
        });
        leases.push(Arc::clone(&lease));
        let worker = Arc::clone(&lease);
        drop(
            self.wire_tasks.spawn_on(
                async move {
                    let _owner = owner;
                    worker.run(control, opened).await;
                }
                .in_current_span(),
                runtime,
            ),
        );
        Ok(Some(lease))
    }

    fn release(&self) -> bool {
        if self.released.swap(true, Ordering::AcqRel) {
            return false;
        }
        let leases = std::mem::take(
            &mut *self
                .wire_leases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for lease in leases {
            lease.close(/*retire*/ false);
        }
        self.cancellation.cancel();
        self.wire_tasks.close();
        self.slot.release()
    }
}

/// One session's ownership of a shared MCP connection slot.
///
/// Cloning a lease for an in-flight operation does not create another session lease. Every tree
/// member follows the slot's healthy generation, so retiring one physical connection does not
/// strand siblings on a cancelled client.
#[derive(Clone)]
pub(crate) struct McpConnectionLease {
    inner: Arc<LeaseInner>,
}

/// Stable-binding readiness for the physical connection currently preferred by a lease.
///
/// Callers use the connection id to distinguish replacements that keep the same tool catalog.
pub(crate) enum StableMcpConnectionState {
    Ready {
        connection_id: u64,
        catalog_revision: ClientToolCatalogRevision,
    },
    Cached {
        connection_id: u64,
        catalog_revision: u64,
    },
    TerminalFailure,
    PendingOrClosed,
}

#[derive(Clone)]
pub(crate) struct McpPooledClient {
    connection: Arc<SharedConnection>,
    slot: Weak<RootConnectionSlot>,
    route: Arc<McpSessionRoute>,
}

/// The exact physical connection generation and ready client captured for one model step.
///
/// A pool replacement may publish a new generation for later steps. Keeping this guard alive
/// prevents the advertised generation from shutting down until every bound tool and resource
/// operation using it has finished.
#[derive(Clone)]
pub(crate) struct McpPooledBindingClient {
    connection: Arc<SharedConnection>,
    lease: McpConnectionLease,
    route: Arc<McpSessionRoute>,
    managed: Arc<ManagedClient>,
}

impl Deref for McpPooledBindingClient {
    type Target = ManagedClient;

    fn deref(&self) -> &Self::Target {
        &self.managed
    }
}

impl McpPooledBindingClient {
    #[cfg(test)]
    pub(crate) fn for_test(managed: Arc<ManagedClient>) -> Self {
        use futures::FutureExt;

        let route = Arc::new(McpSessionRoute::new(
            "binding-test".to_string(),
            crate::elicitation::ElicitationRequestManager::new(
                crate::mcp::tests::test_elicitation_config(
                    "binding-test",
                    codex_protocol::protocol::AskForApproval::Never,
                    codex_protocol::models::PermissionProfile::default(),
                ),
                /*reviewer*/ None,
                /*lifecycle*/ None,
                crate::elicitation::ElicitationRequestRouter::default(),
            ),
            /*tx_event*/ None,
        ));
        let request_router = McpConnectionRequestRouter::default();
        request_router.register(&route);
        let client = AsyncManagedClient {
            ema_authority: None,
            client: futures::future::ready(Ok((*managed).clone()))
                .boxed()
                .shared(),
            is_codex_apps_mcp_server: false,
            cached_server_info: None,
            server_capabilities: Arc::new(Mutex::new(None)),
            codex_apps_tools_cache_context: None,
            tool_catalog_cache_context: None,
            startup_complete: Arc::new(AtomicBool::new(true)),
            initialized_apps: None,
            startup_reconnect: None,
            connection_slot: None,
            cancel_token: tokio_util::sync::CancellationToken::new(),
            request_router,
        };
        let lease = McpConnectionLease::from(client);
        Self {
            connection: lease.current().expect("test lease should be active"),
            lease,
            route,
            managed,
        }
    }

    pub(crate) async fn run<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<ManagedClient>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let active_route = self
            .with_active_shared_lease(self.acquire_route())
            .await??;
        self.run_with_route(active_route, operation).await
    }

    pub(crate) async fn with_active_shared_lease<T>(
        &self,
        operation: impl Future<Output = T>,
    ) -> Result<T> {
        self.lease.current()?;
        self.connection.check_new_request()?;
        tokio::select! {
            biased;
            () = self.lease.inner.cancellation.cancelled() => Err(anyhow!("MCP connection lease released")),
            () = self.route.closed() => Err(anyhow!("MCP session route closed")),
            () = self.connection.client.cancel_token.cancelled() => Err(anyhow!("shared MCP connection closed")),
            result = self.managed.with_active_shared_lease(operation) => result,
        }
    }

    /// Attribution is acquired before catalog locks, matching catalog refresh requests.
    pub(crate) async fn acquire_route(&self) -> Result<ActiveRoute> {
        self.lease.current()?;
        self.connection.check_new_request()?;
        let active_route = self
            .connection
            .client
            .request_router
            .acquire(Arc::clone(&self.route))
            .await
            .map_err(|error| anyhow!("MCP binding route unavailable: {error:?}"))?;
        self.lease.current()?;
        self.connection.check_new_request()?;
        Ok(active_route)
    }

    pub(crate) async fn acquire_wire_route(&self) -> Result<(ActiveRoute, Option<Arc<WireLease>>)> {
        self.lease.current()?;
        self.connection.check_new_request()?;
        let physical = McpPooledClient {
            connection: Arc::clone(&self.connection),
            slot: Arc::downgrade(&self.lease.inner.slot),
            route: Arc::clone(&self.route),
        };
        let wire = self.lease.inner.wire_lease(&physical, &self.managed)?;
        if let Some(wire) = &wire {
            self.with_active_shared_lease(wire.wait_open()).await??;
            anyhow::ensure!(
                physical.is_current(),
                "shared MCP generation was superseded"
            );
        }
        let route = self.acquire_route().await?;
        if let Some(wire) = &wire {
            anyhow::ensure!(
                physical.is_current() && !wire.closing.is_cancelled(),
                "MCP wire lease closed or superseded"
            );
        }
        Ok((route, wire))
    }

    pub(crate) async fn run_wire<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<ManagedClient>, Option<ServerLease>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let (route, wire) = self
            .with_active_shared_lease(self.acquire_wire_route())
            .await??;
        self.run_wire_with_route(route, wire, operation).await
    }

    pub(crate) async fn run_with_route<T, F, Fut>(
        &self,
        active_route: ActiveRoute,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<ManagedClient>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.run_wire_with_route(
            active_route,
            /*wire*/ None,
            |managed, _| operation(managed),
        )
        .await
    }

    pub(crate) async fn run_wire_with_route<T, F, Fut>(
        &self,
        active_route: ActiveRoute,
        wire: Option<Arc<WireLease>>,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<ManagedClient>, Option<ServerLease>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.lease.current()?;
        self.connection.check_new_request()?;
        if let Some(wire) = &wire {
            anyhow::ensure!(
                Arc::ptr_eq(&wire.physical.connection, &self.connection)
                    && Arc::ptr_eq(&wire.physical.route, &self.route)
                    && wire.physical.is_current()
                    && !wire.closing.is_cancelled(),
                "MCP wire lease closed or superseded"
            );
        }
        let pooled_client = McpPooledClient {
            connection: Arc::clone(&self.connection),
            slot: Arc::downgrade(&self.lease.inner.slot),
            route: Arc::clone(&self.route),
        };
        let route = Arc::clone(&self.route);
        let managed = Arc::clone(&self.managed);
        let cancelled = self.connection.client.cancel_token.clone();
        let lease_cancelled = self.lease.inner.cancellation.clone();
        let task_client = pooled_client.clone();
        let task_wire = wire.clone();
        let operation_task = tokio::spawn(
            async move {
                let _active_route = active_route;
                let protocol = task_wire.as_ref().map(|wire| wire.protocol.clone());
                let wire_closed = async {
                    match &task_wire {
                        Some(wire) => wire.closing.cancelled().await,
                        None => std::future::pending().await,
                    }
                };
                let retire = || {
                    if let Some(wire) = &task_wire {
                        wire.close(/*retire*/ true);
                    } else {
                        task_client.retire_after_route_close_in_background();
                    }
                };
                tokio::select! {
                    biased;
                    () = lease_cancelled.cancelled() => {
                        retire();
                        Err(anyhow!("MCP connection lease released"))
                    },
                    () = route.closed() => {
                        retire();
                        Err(anyhow!("MCP session route closed"))
                    },
                    () = wire_closed => {
                        retire();
                        Err(anyhow!("MCP wire lease closed"))
                    },
                    () = cancelled.cancelled() => Err(anyhow!("shared MCP connection closed")),
                    result = operation(managed, protocol) => {
                        if result
                            .as_ref()
                            .is_err_and(codex_rmcp_client::is_connection_unusable)
                        {
                            task_client.retire_after_failure();
                        }
                        result
                    },
                }
            }
            .in_current_span(),
        );
        let mut retire_on_drop =
            RetireConnectionOnDrop::new(pooled_client, AbandonedOperationAction::RetireConnection);
        retire_on_drop.wire = wire;
        let result = operation_task
            .await
            .map_err(|error| anyhow!("shared MCP binding operation task failed: {error}"));
        retire_on_drop.disarm();
        result?
    }

    pub(crate) fn tool_timeout(&self) -> Option<Duration> {
        self.managed.tool_timeout
    }
}

impl Deref for McpPooledClient {
    type Target = AsyncManagedClient;

    fn deref(&self) -> &Self::Target {
        &self.connection.client
    }
}

impl McpPooledClient {
    #[cfg(test)]
    pub(crate) fn connection_id(&self) -> u64 {
        self.connection.id
    }

    fn is_current(&self) -> bool {
        self.slot
            .upgrade()
            .is_some_and(|slot| Arc::ptr_eq(&slot.current(), &self.connection))
    }

    fn retire_after_failure(&self) {
        if self.connection.retire_request()
            && let Some(slot) = self.slot.upgrade()
            && let Some((replacement, route)) =
                slot.replace_if_current(&self.connection, Some(&self.route))
        {
            Self {
                connection: replacement,
                slot: Arc::downgrade(&slot),
                route,
            }
            .start_in_background();
        }
        self.connection.start_retired_shutdown();
    }

    fn prepare_retirement_after_route_close(&self) {
        if self.connection.retire_request()
            && let Some(slot) = self.slot.upgrade()
            && slot.has_live_route()
            && let Some((replacement, route)) =
                slot.replace_if_current(&self.connection, /*preferred_route*/ None)
        {
            Self {
                connection: replacement,
                slot: Arc::downgrade(&slot),
                route,
            }
            .start_in_background();
        }
    }

    fn retire_after_route_close_in_background(&self) {
        self.prepare_retirement_after_route_close();
        self.connection.start_retired_shutdown();
    }

    async fn retire_after_route_close(&self) {
        self.prepare_retirement_after_route_close();
        self.connection.start_retired_shutdown();
        self.connection.shutdown_tasks.close();
        self.connection.shutdown_tasks.wait().await;
    }

    fn start_in_background(self) {
        self.start_in_background_inner(
            #[cfg(test)]
            /*after_check*/
            None,
        );
    }

    #[cfg(test)]
    fn start_in_background_after_check(self, hook: impl Future<Output = ()> + Send + 'static) {
        self.start_in_background_inner(Some(Box::pin(hook)));
    }

    fn start_in_background_inner(
        mut self,
        #[cfg(test)] after_check: Option<
            std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
        >,
    ) {
        tokio::spawn(async move {
            let Some(slot) = self.slot.upgrade() else {
                return;
            };
            let Some(superseded) = slot.superseded_for(&self.connection) else {
                return;
            };
            let active_route = loop {
                match self
                    .connection
                    .client
                    .request_router
                    .acquire(Arc::clone(&self.route))
                    .await
                {
                    Ok(active_route) => break active_route,
                    Err(McpRouteAcquireError::ConnectionClosed) => return,
                    Err(McpRouteAcquireError::RouteClosed) => {
                        let Some(slot) = self.slot.upgrade() else {
                            return;
                        };
                        if !Arc::ptr_eq(&slot.current(), &self.connection) {
                            return;
                        }
                        let Some(route) = slot.first_live_route() else {
                            return;
                        };
                        self.route = route;
                    }
                }
            };
            let _active_route = active_route;
            if superseded.is_cancelled() || !Arc::ptr_eq(&slot.current(), &self.connection) {
                return;
            }
            #[cfg(test)]
            if let Some(after_check) = after_check {
                after_check.await;
            }
            tokio::select! {
                biased;
                () = superseded.cancelled() => {}
                () = self.route.closed() => self.retire_after_route_close().await,
                _ = self.connection.client.client() => {}
            }
        });
    }
}

/// Retires a physical connection when its dispatched operation outlives the caller.
///
/// Dropping a `JoinHandle` detaches its task. An MCP request that is waiting for a server request
/// such as elicitation could otherwise keep the session route active forever and block sibling
/// sessions that share the connection.
struct RetireConnectionOnDrop {
    client: Option<McpPooledClient>,
    action: AbandonedOperationAction,
    wire: Option<Arc<WireLease>>,
}

#[derive(Clone, Copy)]
enum AbandonedOperationAction {
    RetireConnection,
    CancelIfSoleRoute,
    KeepRunning,
}

#[derive(Clone, Copy)]
enum SupersededOperationAction {
    Drain,
    Retry,
}

impl RetireConnectionOnDrop {
    fn new(client: McpPooledClient, action: AbandonedOperationAction) -> Self {
        Self {
            client: Some(client),
            action,
            wire: None,
        }
    }

    fn disarm(&mut self) {
        self.client = None;
    }
}

impl Drop for RetireConnectionOnDrop {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        let action = self.action;
        if let Some(wire) = &self.wire {
            wire.close(/*retire*/ true);
            return;
        }
        tokio::spawn(async move {
            match action {
                AbandonedOperationAction::RetireConnection => {
                    client.retire_after_failure();
                }
                AbandonedOperationAction::CancelIfSoleRoute => {
                    let Some(slot) = client.slot.upgrade() else {
                        return;
                    };
                    if slot.cancel_startup_if_sole_route(&client.connection, &client.route) {
                        client.connection.shutdown().await;
                    }
                }
                AbandonedOperationAction::KeepRunning => {}
            }
        });
    }
}

impl McpConnectionLease {
    fn new(slot: Arc<RootConnectionSlot>) -> Self {
        Self {
            inner: Arc::new(LeaseInner {
                slot,
                released: AtomicBool::new(false),
                cancellation: CancellationToken::new(),
                runtime: tokio::runtime::Handle::try_current().ok(),
                wire_leases: Mutex::default(),
                wire_tasks: TaskTracker::new(),
            }),
        }
    }

    fn current(&self) -> Result<Arc<SharedConnection>> {
        if self.inner.released.load(Ordering::Acquire) {
            return Err(anyhow!("MCP connection lease released"));
        }
        Ok(self.inner.slot.current())
    }

    pub(crate) async fn run<T, F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.run_with_abandoned_operation_action(
            route,
            AbandonedOperationAction::RetireConnection,
            SupersededOperationAction::Drain,
            operation,
        )
        .await
    }

    async fn run_with_abandoned_operation_action<T, F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        abandoned_operation_action: AbandonedOperationAction,
        superseded_operation_action: SupersededOperationAction,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        if route.is_closed() {
            return Err(anyhow!("MCP session route closed"));
        }
        let initializer_route = self.ema_startup_route().map(|owner| owner.route());
        if let Some(initializer_route) = initializer_route
            && !Arc::ptr_eq(&initializer_route, &route)
            && !self.startup_complete()
        {
            let startup = self.run_on_current_connection(
                initializer_route,
                AbandonedOperationAction::CancelIfSoleRoute,
                SupersededOperationAction::Retry,
                |client| async move { client.client().await },
            );
            tokio::select! {
                outcome = startup => { outcome??; }
                () = route.closed() => return Err(anyhow!("MCP session route closed")),
            }
        }
        self.run_on_current_connection(
            route,
            abandoned_operation_action,
            superseded_operation_action,
            operation,
        )
        .await
    }

    fn repair_request_retirement(
        &self,
        connection: &Arc<SharedConnection>,
        route: &Arc<McpSessionRoute>,
    ) -> Result<bool> {
        self.current()?;
        if route.is_closed() {
            return Err(anyhow!("MCP session route closed"));
        }
        if !Arc::ptr_eq(&self.inner.slot.current(), connection) {
            return Ok(true);
        }
        self.inner.slot.register_route(route);
        if let Some((replacement, route)) =
            self.inner
                .slot
                .replace_if_current_for_lease(connection, Some(route), Some(&self.inner))
        {
            McpPooledClient {
                connection: replacement,
                slot: Arc::downgrade(&self.inner.slot),
                route,
            }
            .start_in_background();
            return Ok(true);
        }
        Ok(!Arc::ptr_eq(&self.inner.slot.current(), connection))
    }

    async fn run_on_current_connection<T, F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        abandoned_operation_action: AbandonedOperationAction,
        superseded_operation_action: SupersededOperationAction,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let mut operation = Some(operation);
        loop {
            let connection = self.current()?;
            if let Err(error) = connection.check_new_request() {
                if self.repair_request_retirement(&connection, &route)? {
                    continue;
                }
                return Err(error);
            }
            self.inner.slot.register_route(&route);
            let active_route = match tokio::select! {
                biased;
                () = self.inner.cancellation.cancelled() => return Err(anyhow!("MCP connection lease released")),
                acquired = connection.client.request_router.acquire(Arc::clone(&route)) => acquired,
            } {
                Ok(active_route) => active_route,
                Err(McpRouteAcquireError::ConnectionClosed) => {
                    if self.repair_request_retirement(&connection, &route)? {
                        continue;
                    }
                    return Err(anyhow!("shared MCP connection closed"));
                }
                Err(McpRouteAcquireError::RouteClosed) => {
                    return Err(anyhow!("MCP session route closed"));
                }
            };
            let Some(connection_superseded) = self.inner.slot.superseded_for(&connection) else {
                drop(active_route);
                continue;
            };
            self.current()?;
            if let Err(error) = connection.check_new_request() {
                drop(active_route);
                if self.repair_request_retirement(&connection, &route)? {
                    continue;
                }
                return Err(error);
            }
            let pooled_client = McpPooledClient {
                connection: Arc::clone(&connection),
                slot: Arc::downgrade(&self.inner.slot),
                route: Arc::clone(&route),
            };
            let Some(operation) = operation.take() else {
                return Err(anyhow!("shared MCP operation already started"));
            };
            let route_for_task = Arc::clone(&route);
            let task_client = pooled_client.clone();
            let connection_cancelled = task_client.connection.client.cancel_token.clone();
            let lease_cancelled = self.inner.cancellation.clone();
            // RmcpClient reads the current span when constructing request metadata.
            // Each operation must carry its caller's span, not the pooled connection's owner.
            let operation_task = tokio::spawn(
                async move {
                    let _active_route = active_route;
                    let superseded = async move {
                        match superseded_operation_action {
                            SupersededOperationAction::Drain => std::future::pending().await,
                            SupersededOperationAction::Retry => {
                                connection_superseded.cancelled().await
                            }
                        }
                    };
                    tokio::select! {
                        biased;
                        () = lease_cancelled.cancelled() => {
                            task_client.retire_after_route_close_in_background();
                            Err(anyhow!("MCP connection lease released"))
                        }
                        () = superseded => Err(anyhow!("shared MCP connection superseded")),
                        () = connection_cancelled.cancelled() => {
                            Err(anyhow!("shared MCP connection closed"))
                        }
                        () = route_for_task.closed() => {
                            task_client.retire_after_route_close_in_background();
                            Err(anyhow!("MCP session route closed"))
                        }
                        value = operation(task_client.clone()) => Ok(value),
                    }
                }
                .in_current_span(),
            );
            let mut retire_on_drop =
                RetireConnectionOnDrop::new(pooled_client, abandoned_operation_action);
            let result = operation_task
                .await
                .map_err(|error| anyhow!("shared MCP operation task failed: {error}"));
            retire_on_drop.disarm();
            return result?;
        }
    }

    pub(crate) async fn run_mcp_request<T, F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.run(route, move |client| async move {
            let result = operation(client.clone()).await;
            if result
                .as_ref()
                .is_err_and(codex_rmcp_client::is_connection_unusable)
            {
                client.retire_after_failure();
            }
            result
        })
        .await?
    }

    pub(crate) async fn bind_mcp_request<F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        initialized_client: F,
    ) -> Result<McpPooledBindingClient>
    where
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<ManagedClient>>> + Send + 'static,
    {
        let (physical, managed) = self
            .run_mcp_request(route, move |physical| async move {
                let managed = initialized_client(physical.clone()).await?;
                Ok((physical, managed))
            })
            .await?;
        anyhow::ensure!(
            physical.is_current(),
            "shared MCP generation was superseded"
        );
        Ok(McpPooledBindingClient {
            connection: physical.connection,
            lease: self.clone(),
            route: physical.route,
            managed,
        })
    }

    pub(crate) fn pending_early_apps_generation(&self) -> Option<u64> {
        let connection = self.current().ok()?;
        (connection.client.initialized_apps.is_some()
            && !connection.client.startup_complete.load(Ordering::Acquire))
        .then_some(connection.id)
    }

    /// Run catalog discovery beside the view-owned initializer, not after its tools/list.
    pub(crate) async fn run_early_apps_catalog<T, F, Fut>(
        &self,
        caller: Arc<McpSessionRoute>,
        expected_generation: u64,
        startup_timeout: Duration,
        request: F,
    ) -> Result<T>
    where
        F: FnOnce(Arc<RmcpClient>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let connection = self.current()?;
        anyhow::ensure!(
            connection.id == expected_generation,
            "Apps generation changed"
        );
        connection.check_new_request()?;
        self.inner.slot.register_route(&caller);
        let initializer = self
            .ema_startup_route()
            .map_or_else(|| Arc::clone(&caller), |owner| owner.route());
        let operation = async {
            let (_route, client) = tokio::time::timeout(startup_timeout, async {
                let client = connection.client.initialized_apps_client().await?;
                let route = connection
                    .client
                    .request_router
                    .acquire(initializer)
                    .await?;
                anyhow::ensure!(
                    self.current()?.id == expected_generation,
                    "Apps generation changed"
                );
                connection.check_new_request()?;
                anyhow::Ok((route, client))
            })
            .await
            .map_err(|_| anyhow!("timed out waiting for the initialized Apps client"))??;
            anyhow::ensure!(
                self.current()?.id == expected_generation,
                "Apps generation changed"
            );
            tokio::select! {
                biased;
                Err(error) = connection.client.client.clone() => Err(error.into()),
                result = request(client) => result,
            }
        };
        tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => Err(anyhow!("Apps session lease released")),
            () = caller.closed() => Err(anyhow!("Apps catalog caller closed")),
            () = connection.client.cancel_token.cancelled() => Err(anyhow!("Apps startup cancelled")),
            result = operation => result,
        }
    }

    pub(crate) async fn capture_ready_client_and_tools(
        &self,
        route: Arc<McpSessionRoute>,
        tool_plugin_context: Arc<crate::mcp::ToolPluginContext>,
        tool_timeout: Option<Duration>,
    ) -> Option<(
        McpPooledBindingClient,
        Arc<ToolCatalogSnapshot>,
        Vec<ToolInfo>,
    )> {
        // Binding capture reads cached metadata, not the MCP protocol. Acquiring a request
        // route here can deadlock a root turn behind a child's pending Guardian elicitation.
        loop {
            let connection = self.current().ok()?;
            self.inner.slot.register_route(&route);
            if route.is_closed() || connection.check_new_request().is_err() {
                return None;
            }
            let Some((managed, snapshot, tools)) = connection
                .client
                .capture_ready_binding_snapshot(tool_plugin_context.as_ref(), tool_timeout)
                .await
            else {
                break;
            };
            if !Arc::ptr_eq(&self.inner.slot.current(), &connection) {
                continue;
            }
            return Some((
                McpPooledBindingClient {
                    connection,
                    lease: self.clone(),
                    route,
                    managed,
                },
                snapshot,
                tools,
            ));
        }
        // A recoverable Apps startup has no ready client, and an active reconnect owns its
        // original session route. Do not queue a new publication merely to rediscover the known
        // failed initial client.
        if self.has_recoverable_failed_startup() {
            return None;
        }
        let binding_lease = self.clone();
        self.run(route.clone(), move |client| async move {
            let (managed, snapshot, tools) = client
                .capture_ready_client_and_tools(
                    tool_plugin_context.as_ref(),
                    Arc::clone(&route),
                    tool_timeout,
                )
                .await?;
            Some((
                McpPooledBindingClient {
                    connection: Arc::clone(&client.connection),
                    lease: binding_lease,
                    route,
                    managed,
                },
                snapshot,
                tools,
            ))
        })
        .await
        .ok()
        .flatten()
    }

    /// Waits for startup of the slot's current physical connection.
    ///
    /// Refresh may replace a connection while its startup future is still running. Startup status
    /// must describe the replacement, not a superseded generation that happened to finish later.
    pub(crate) async fn await_current_startup(
        &self,
        route: Arc<McpSessionRoute>,
    ) -> std::result::Result<
        crate::rmcp_client::ManagedClient,
        crate::rmcp_client::StartupOutcomeError,
    > {
        self.await_current_startup_with_abandoned_operation_action(
            route,
            AbandonedOperationAction::CancelIfSoleRoute,
        )
        .await
    }

    /// Recover a failed Apps startup before the caller takes its tool execution lock.
    pub(crate) async fn client_for_call(
        &self,
        route: Arc<McpSessionRoute>,
    ) -> Result<ManagedClient, crate::rmcp_client::StartupOutcomeError> {
        let result = self
            .await_current_startup_preserving_connection(Arc::clone(&route))
            .await;
        if matches!(
            result,
            Err(crate::rmcp_client::StartupOutcomeError::Failed { .. })
        ) && let Ok(connection) = self.current()
            && !connection.client.cancel_token.is_cancelled()
            && let Some(reconnect) = &connection.client.startup_reconnect
        {
            if let Some(attempt) = reconnect.reconnect_in_background(Arc::clone(&route)) {
                return attempt.await;
            }
            return self
                .await_current_startup_preserving_connection(route)
                .await;
        }
        result
    }

    /// Waits for startup without cancelling a pending optional server when the caller stops
    /// waiting after its shared startup grace.
    pub(crate) async fn await_current_startup_preserving_connection(
        &self,
        route: Arc<McpSessionRoute>,
    ) -> std::result::Result<
        crate::rmcp_client::ManagedClient,
        crate::rmcp_client::StartupOutcomeError,
    > {
        self.await_current_startup_with_abandoned_operation_action(
            route,
            AbandonedOperationAction::KeepRunning,
        )
        .await
    }

    async fn await_current_startup_with_abandoned_operation_action(
        &self,
        route: Arc<McpSessionRoute>,
        abandoned_operation_action: AbandonedOperationAction,
    ) -> std::result::Result<
        crate::rmcp_client::ManagedClient,
        crate::rmcp_client::StartupOutcomeError,
    > {
        loop {
            let observed_generation = self.inner.slot.generation();
            let attempt = self
                .run_with_abandoned_operation_action(
                    Arc::clone(&route),
                    abandoned_operation_action,
                    SupersededOperationAction::Retry,
                    |client| async move {
                        let outcome = client.client().await;
                        (client, outcome)
                    },
                )
                .await;
            match attempt {
                Ok((client, outcome))
                    if client.is_current()
                        && self.inner.slot.generation() == observed_generation =>
                {
                    if let Err(error) = &outcome
                        && self
                            .inner
                            .slot
                            .adopt_legacy_after_failure(
                                &client.connection,
                                observed_generation,
                                error,
                            )
                            .await
                    {
                        continue;
                    }
                    if self
                        .inner
                        .slot
                        .finish_startup(&client.connection, observed_generation)
                    {
                        return outcome;
                    }
                }
                Ok(_) => continue,
                Err(_) if self.inner.slot.generation() != observed_generation => {
                    continue;
                }
                Err(error)
                    if self.has_ema_authority()
                        && codex_rmcp_client::is_authentication_required_error(&error) =>
                {
                    return Err(crate::rmcp_client::StartupOutcomeError::Failed {
                        error: format!("{error:#}"),
                        is_authentication_required: true,
                        is_ema_authentication_required: true,
                        shutdown: None,
                    });
                }
                Err(error) => {
                    return Err(error
                        .downcast_ref::<crate::rmcp_client::StartupOutcomeError>()
                        .cloned()
                        .unwrap_or(crate::rmcp_client::StartupOutcomeError::Cancelled));
                }
            }
        }
    }

    pub(crate) fn has_cached_tools(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.has_cached_tools())
    }

    pub(crate) fn cached_tools_or(
        &self,
        fallback: Option<(u64, Vec<ToolInfo>)>,
    ) -> Option<(u64, Vec<ToolInfo>)> {
        let connection = self.current().ok()?;
        // Fallback metadata cannot cross a physical connection replacement.
        let fallback = fallback.and_then(|(id, tools)| (id == connection.id).then_some(tools));
        let tools = connection.client.cached_tools_or(fallback)?;
        Some((connection.id, tools))
    }

    pub(crate) fn prepared_cached_tools(
        &self,
        fallback: Option<(u64, Vec<ToolInfo>)>,
        context: &crate::mcp::ToolPluginContext,
        accepts_cached_tools: impl FnOnce(&[ToolInfo]) -> bool,
        measure_raw_tools: impl FnOnce(&[ToolInfo]) -> usize,
    ) -> Option<(Vec<ToolInfo>, usize)> {
        let connection = self.current().ok()?;
        // A cache snapshot must not carry tools across a physical connection replacement.
        let fallback = fallback.and_then(|(id, tools)| (id == connection.id).then_some(tools));
        let tools = connection
            .client
            .cached_tools_or(fallback)
            .filter(|tools| accepts_cached_tools(tools))?;
        // Measure the same physical catalog before model preparation changes its definitions.
        let raw_definition_json_bytes = measure_raw_tools(&tools);
        Some((
            connection.client.prepare_tools(tools, context),
            raw_definition_json_bytes,
        ))
    }

    pub(crate) fn cached_catalog_revision_if(
        &self,
        accepts_cached_tools: impl FnOnce(&[ToolInfo]) -> bool,
    ) -> Option<u64> {
        self.current()
            .ok()?
            .client
            .tool_catalog_cache_context
            .as_ref()?
            .current_revision_if(accepts_cached_tools)
    }

    pub(crate) fn has_ready_client(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.ready_client().is_some())
    }

    pub(crate) fn ready_client(&self) -> Option<ManagedClient> {
        self.current().ok()?.client.ready_client()
    }

    pub(crate) fn uncached_ready_client(&self) -> Option<ManagedClient> {
        let connection = self.current().ok()?;
        if connection.client.codex_apps_tools_cache_context.is_some() {
            return None;
        }
        connection.client.ready_client()
    }

    pub(crate) fn async_client(&self) -> Option<Arc<AsyncManagedClient>> {
        self.current()
            .ok()
            .map(|connection| Arc::clone(&connection.client))
    }

    pub(crate) fn has_transport_slot(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.connection_slot.is_some())
    }

    pub(crate) fn retained_legacy(&self) -> bool {
        self.inner.slot.retained_legacy.load(Ordering::Acquire)
    }

    pub(crate) fn may_use_legacy_fallback(&self) -> bool {
        self.retained_legacy()
            || self
                .inner
                .slot
                .legacy_fallback
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some()
    }

    pub(crate) async fn prepare_legacy_fallback(
        &self,
        desired: &ConnectionKey,
    ) -> Option<McpLegacyFallback> {
        let connection = self.current().ok()?;
        let previous_key = self
            .inner
            .slot
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        let legacy_identity = desired
            .identity
            .clone()
            .with_effective_protocol_mode(Some(crate::McpProtocolMode::Legacy));
        if !previous_key.same_owner(desired)
            || previous_key.identity != legacy_identity
            || connection.check_new_request().is_err()
        {
            return None;
        }
        let client = connection.client.ready_client()?;
        if client.client.protocol_mode() != crate::McpProtocolMode::Legacy
            || !matches!(client.connection_sharing, ConnectionSharing::Private)
            || client.client.is_closed().await
        {
            return None;
        }
        let key = self
            .inner
            .slot
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = self
            .inner
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if key.as_ref() != Some(&previous_key)
            || !Arc::ptr_eq(&state.connection, &connection)
            || self.inner.released.load(Ordering::Acquire)
            || connection.check_new_request().is_err()
            || !connection.try_retain_owner()
        {
            return None;
        }
        Some(McpLegacyFallback {
            connection: Some(connection),
        })
    }

    pub(crate) fn retain_on_failed_discovery(&self, fallback: McpLegacyFallback) {
        self.inner.slot.install_legacy_fallback(fallback);
    }

    pub(crate) fn retire_acquisition(&self) {
        if let Ok(connection) = self.current() {
            connection.retire_acquisition();
        }
    }

    pub(crate) fn has_ema_authority(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.ema_authority.is_some())
    }

    pub(crate) fn ema_startup_route(&self) -> Option<Arc<McpEmaStartupRoute>> {
        self.inner.slot.ema_startup_route.as_ref()?.upgrade()
    }

    pub(crate) fn retain_ema_for_route(&self, route: &Arc<McpSessionRoute>) -> Option<Self> {
        let connection = self.current().ok()?;
        connection
            .client
            .ema_authority
            .as_ref()?
            .check_new_request()
            .ok()?;
        connection.check_new_request().ok()?;
        self.inner
            .slot
            .try_acquire(route)
            .then(|| Self::new(Arc::clone(&self.inner.slot)))
    }

    pub(crate) fn tool_list_notifications_current(&self) -> bool {
        self.current().is_ok_and(|connection| {
            connection.client.ready_client().is_none_or(|client| {
                client
                    .tool_catalog
                    .notifications_current(&client.client.tool_list_revision())
            })
        })
    }

    pub(crate) fn startup_complete(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.startup_complete.load(Ordering::Acquire))
    }

    /// Identifies the physical connection without starting a dormant server.
    pub(crate) fn current_connection_id(&self) -> Option<u64> {
        self.current().ok().map(|connection| connection.id)
    }

    /// Initialization capabilities remain available even when tool discovery fails.
    pub(crate) fn server_capabilities(&self) -> Option<serde_json::Value> {
        self.current()
            .ok()?
            .client
            .server_capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn is_connection_cancelled(&self) -> bool {
        self.current().map_or(true, |connection| {
            connection.client.cancel_token.is_cancelled()
        })
    }

    pub(crate) fn has_recoverable_failed_startup(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.has_recoverable_failed_startup())
    }

    pub(crate) async fn stable_connection_state(
        &self,
        startup_readiness: codex_config::McpStartupReadiness,
        refresh_tool_list: bool,
        accepts_cached_tools: impl FnOnce(&[ToolInfo]) -> bool,
    ) -> StableMcpConnectionState {
        let Ok(connection) = self.current() else {
            return StableMcpConnectionState::PendingOrClosed;
        };
        if connection.check_new_request().is_err() {
            return StableMcpConnectionState::PendingOrClosed;
        }
        if !connection.client.startup_complete.load(Ordering::Acquire)
            || (startup_readiness == codex_config::McpStartupReadiness::Catalog
                && connection.client.ready_client().is_none())
        {
            return match connection
                .client
                .tool_catalog_cache_context
                .as_ref()
                .and_then(|cache| cache.current_revision_if(accepts_cached_tools))
            {
                Some(catalog_revision) => StableMcpConnectionState::Cached {
                    connection_id: connection.id,
                    catalog_revision,
                },
                None => StableMcpConnectionState::PendingOrClosed,
            };
        }
        let Some(client) = connection.client.ready_client() else {
            return if matches!(connection.client.client.peek(), Some(Err(_))) {
                StableMcpConnectionState::TerminalFailure
            } else {
                StableMcpConnectionState::PendingOrClosed
            };
        };
        if client.client.is_closed().await {
            return StableMcpConnectionState::PendingOrClosed;
        }
        if refresh_tool_list
            && !connection.client.is_codex_apps_mcp_server
            && !client
                .tool_catalog
                .notifications_current(&client.client.tool_list_revision())
        {
            return StableMcpConnectionState::PendingOrClosed;
        }
        let revision = client.tool_catalog.read(|catalog| catalog.revision).await;
        StableMcpConnectionState::Ready {
            connection_id: connection.id,
            catalog_revision: ClientToolCatalogRevision {
                catalog: Arc::clone(&client.tool_catalog),
                revision,
            },
        }
    }

    /// Observe the current physical client without starting or reconnecting it.
    pub(crate) async fn connection_status(&self) -> codex_protocol::mcp::McpServerConnectionStatus {
        match self.current() {
            Ok(connection) => connection.client.connection_status().await,
            Err(_) => codex_protocol::mcp::McpServerConnectionStatus::Cancelled,
        }
    }

    pub(crate) fn optional_startup_deadline(
        &self,
        default: tokio::time::Instant,
        grace: Duration,
    ) -> tokio::time::Instant {
        self.current()
            .ok()
            .and_then(|connection| {
                connection
                    .client
                    .tool_catalog_cache_context
                    .as_ref()
                    .map(|cache| cache.optional_startup_deadline(default, grace))
            })
            .unwrap_or(default)
    }

    pub(crate) async fn is_reusable_connection(&self, desired: &McpConnectionIdentity) -> bool {
        self.inner.slot.is_reusable_connection(desired).await
    }

    pub(crate) async fn authentication_failed(&self) -> bool {
        let Ok(connection) = self.current() else {
            return false;
        };
        if !connection.client.startup_complete.load(Ordering::Acquire) {
            return false;
        }
        connection
            .client
            .client
            .clone()
            .await
            .is_err_and(|error| error.is_authentication_required())
    }

    pub(crate) fn connection_identity(&self) -> Option<McpConnectionIdentity> {
        self.inner
            .slot
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|key| key.identity.clone())
    }

    pub(crate) fn has_same_connection_identity(&self, desired: &McpConnectionIdentity) -> bool {
        self.inner
            .slot
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|key| key.identity == *desired)
    }

    pub(crate) fn has_same_owner(&self, desired: &ConnectionKey) -> bool {
        self.inner
            .slot
            .key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|key| key.same_owner(desired))
    }

    pub(crate) fn cached_server_info(&self) -> Option<codex_protocol::mcp::McpServerInfo> {
        self.current()
            .ok()
            .and_then(|connection| connection.client.cached_server_info.clone())
    }

    #[cfg(test)]
    pub(crate) fn has_tool_catalog_cache_context(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.tool_catalog_cache_context.is_some())
    }

    pub(crate) async fn reconnect_failed_startup(&self, route: Arc<McpSessionRoute>) {
        let lease = self.clone();
        tokio::spawn(async move {
            let reconnect_route = Arc::clone(&route);
            let _ = lease
                .run(route, move |client| async move {
                    client.reconnect_failed_startup(reconnect_route).await;
                })
                .await;
        });
    }

    pub(crate) fn unregister_route(&self, route: &Arc<McpSessionRoute>) {
        self.inner.slot.unregister_route(route);
    }

    /// Releases this session's logical ownership, returning whether it was the final lease.
    pub(crate) fn release(&self) -> bool {
        self.inner.release()
    }

    pub(crate) async fn wait_for_wire_shutdown(&self) {
        self.inner.wire_tasks.wait().await;
    }

    pub(crate) async fn shutdown(&self) {
        self.release();
        self.wait_for_wire_shutdown().await;
        self.inner.slot.wait_for_shutdown().await;
    }

    #[cfg(test)]
    pub(crate) fn is_exclusive(&self) -> bool {
        self.inner.slot.active_leases.load(Ordering::Acquire) == 1
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner.slot, &other.inner.slot)
    }

    #[cfg(test)]
    pub(crate) fn connection_cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.inner.slot.current().client.cancel_token.clone()
    }

    #[cfg(test)]
    pub(crate) fn test_client(&self) -> AsyncManagedClient {
        self.inner.slot.current().client.as_ref().clone()
    }

    #[cfg(test)]
    pub(crate) async fn client(
        &self,
    ) -> Result<ManagedClient, crate::rmcp_client::StartupOutcomeError> {
        self.inner.slot.current().client.client().await
    }

    #[cfg(test)]
    pub(crate) fn connection_id(&self) -> u64 {
        self.inner.slot.current().id
    }

    #[cfg(test)]
    pub(crate) fn replace_connection_for_test(&self, client: AsyncManagedClient) {
        let mut state = self
            .inner
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.connection.supersede_startup();
        state.connection.retire_acquisition();
        let previous = Arc::clone(&state.connection);
        state.retired.push(RetainedGeneration::new(&previous));
        RootConnectionSlot::advance_generation(&mut state);
        state.connection = Arc::new(SharedConnection::new(client));
    }
}

#[cfg(test)]
impl From<AsyncManagedClient> for McpConnectionLease {
    fn from(client: AsyncManagedClient) -> Self {
        Self::new(RootConnectionSlot::from_client(client))
    }
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        self.release();
    }
}

impl McpConnectionPool {
    pub(crate) async fn preferred_connection_is_reusable(
        &self,
        key: &ConnectionKey,
    ) -> Option<bool> {
        loop {
            let slot = {
                let entries = self
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                entries
                    .iter()
                    .rev()
                    .filter_map(|entry| entry.slot.upgrade())
                    .find(|slot| slot.has_config_for_key(key))
            }?;
            if let Some(reusable) = slot.reconcile_ready_key(key).await {
                return Some(reusable);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn acquire(
        &self,
        identity: McpConnectionIdentity,
        mode: McpConnectionPoolMode,
        route: &Arc<McpSessionRoute>,
        create: impl Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync + 'static,
    ) -> McpConnectionLease {
        self.acquire_named("test".to_string(), identity, mode, route, create)
    }

    #[cfg(test)]
    pub(crate) fn acquire_named(
        &self,
        server_name: String,
        identity: McpConnectionIdentity,
        mode: McpConnectionPoolMode,
        route: &Arc<McpSessionRoute>,
        create: impl Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync + 'static,
    ) -> McpConnectionLease {
        self.acquire_named_with_ema_startup_route(
            ConnectionKey {
                server_name,
                identity,
                account: ConnectorRuntimeContextKey::personal(None, None),
                auth_state: None,
                startup_timeout: crate::rmcp_client::DEFAULT_STARTUP_TIMEOUT,
                catalog_item_limit: crate::pagination::MAX_MCP_CATALOG_ITEMS,
            },
            mode,
            route,
            None,
            create,
        )
        .0
    }

    pub(crate) fn acquire_named_with_ema_startup_route(
        &self,
        key: ConnectionKey,
        mode: McpConnectionPoolMode,
        route: &Arc<McpSessionRoute>,
        ema_startup_route: Option<&Arc<McpEmaStartupRoute>>,
        create: impl Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync + 'static,
    ) -> (McpConnectionLease, Option<Arc<McpEmaStartupRoute>>) {
        let factory: ConnectionFactory = Arc::new(create);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|entry| {
            entry
                .slot
                .upgrade()
                .is_some_and(|slot| slot.active_leases.load(Ordering::Acquire) > 0)
        });
        if let Some((slot, retained_startup_route)) = entries
            .iter()
            .rev()
            .filter_map(|entry| entry.slot.upgrade())
            .find(|slot| slot.matches_key(&key))
            .and_then(|slot| {
                let retained_startup_route = match &slot.ema_startup_route {
                    Some(owner) => Some(owner.upgrade()?),
                    None => None,
                };
                slot.try_acquire(route)
                    .then_some((slot, retained_startup_route))
            })
        {
            if mode == McpConnectionPoolMode::Replace && !key.identity.is_ema() {
                drop(slot.replace(key.clone(), factory));
            } else {
                let mut current_factory = slot
                    .factory
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if current_factory.0.as_ref() != Some(&key) {
                    *current_factory = (Some(key), factory);
                }
            }
            return (McpConnectionLease::new(slot), retained_startup_route);
        }

        let slot = RootConnectionSlot::new(key, factory, route, ema_startup_route);
        entries.push(PoolEntry {
            slot: Arc::downgrade(&slot),
        });
        (McpConnectionLease::new(slot), ema_startup_route.cloned())
    }
}

#[cfg(test)]
#[path = "connection_pool_tests.rs"]
pub(crate) mod tests;
