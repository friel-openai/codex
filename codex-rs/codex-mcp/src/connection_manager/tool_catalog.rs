use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use codex_config::McpStartupReadiness;
use codex_connectors::ConnectorRuntimeFetchSource;
use futures::future::join_all;
use tracing::Instrument;
use tracing::instrument;
use tracing::trace;
use tracing::trace_span;

use super::McpConnectionSet;
use super::McpServerMetadata;
use super::catalog_telemetry::emit_binding_catalog;
use super::catalog_telemetry::record_binding_catalog_size;
use super::catalog_telemetry::tool_definition_json_bytes;
use crate::binding::McpBinding;
use crate::binding::PreparedMcpCall;
use crate::binding_clients::McpBindingClients;
use crate::client_tool_catalog::ClientToolCatalogRevision;
use crate::client_tool_catalog::CodexAppsToolSnapshot;
use crate::client_tool_catalog::ToolCatalogSnapshot;
use crate::connection_pool::McpPooledBindingClient;
use crate::connection_pool::StableMcpConnectionState;
use crate::mcp::CODEX_APPS_MCP_SERVER_NAME;
use crate::rmcp_client::CODEX_APPS_REFRESH_DURATION_METRIC;
use crate::rmcp_client::MCP_TOOLS_LIST_DURATION_METRIC;
use crate::rmcp_client::list_tools_for_client_uncached;
use crate::rmcp_client::prepare_codex_apps_tools_for_model;
use crate::runtime::emit_duration;
use crate::tools::ToolInfo;
use crate::tools::filter_tools;
use crate::tools::normalize_tools_for_model_with_prefix;

const MCP_UI_META_KEY: &str = "ui";
const MCP_UI_VISIBILITY_META_KEY: &str = "visibility";
const MCP_UI_MODEL_VISIBILITY: &str = "model";

/// Tool-catalog and physical-connection identity for a reusable model-step binding.
///
/// A pooled connection replacement can preserve the catalog revision, so both values are
/// required before an existing binding can be returned.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StableMcpBindingIdentity {
    catalog_revisions: HashMap<String, BindingCatalogRevision>,
    connection_ids: Vec<(String, u64)>,
}

#[cfg(test)]
impl StableMcpBindingIdentity {
    pub(crate) fn for_test(connection_ids: Vec<(String, u64)>) -> Self {
        Self {
            catalog_revisions: HashMap::new(),
            connection_ids,
        }
    }
}

/// Returns whether a tool may be included in model-facing tool declarations.
///
/// Tools without visibility metadata remain visible. Tools with visibility
/// metadata are hidden unless they explicitly include `model`.
///
/// <https://github.com/modelcontextprotocol/ext-apps/blob/main/specification/2026-01-26/apps.mdx#resource-discovery>
pub fn tool_is_model_visible(tool: &ToolInfo) -> bool {
    let Some(visibility) = tool
        .tool
        .meta
        .as_deref()
        .and_then(|meta| meta.get(MCP_UI_META_KEY))
        .and_then(serde_json::Value::as_object)
        .and_then(|ui| ui.get(MCP_UI_VISIBILITY_META_KEY))
        .and_then(serde_json::Value::as_array)
    else {
        return true;
    };
    visibility
        .iter()
        .any(|target| target.as_str() == Some(MCP_UI_MODEL_VISIBILITY))
}

/// Catalog identity within one published connection set, including cached declarations.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum BindingCatalogRevision {
    Ready(ClientToolCatalogRevision),
    Cached(u64),
}

impl McpConnectionSet {
    /// Observes opted-in servers at startup gates without waiting or cloning catalogs.
    pub(crate) fn record_startup_readiness(
        &self,
        phase: &'static str,
        required_servers: &[String],
        required_plugins: &HashSet<String>,
    ) {
        if !tracing::enabled!(tracing::Level::INFO) {
            return;
        }
        for (server_name, view) in &self.servers {
            if view.startup_readiness != McpStartupReadiness::Catalog {
                continue;
            }
            let startup_pending = !view.connection.startup_complete();
            let cache_eligible = startup_pending
                && view
                    .connection
                    .cached_catalog_revision_if(|tools| {
                        view.accepts_cached_tools(server_name, tools)
                    })
                    .is_some();
            let explicitly_required = required_servers.contains(server_name)
                || (self.is_selected_plugin_mcp_server(server_name)
                    && self
                        .plugin_id_for_mcp_server_name(server_name)
                        .is_some_and(|plugin_id| required_plugins.contains(plugin_id)));
            tracing::info!(
                event.name = "mcp.startup_readiness",
                server_name = %server_name,
                phase,
                startup_pending,
                startup_readiness = "catalog",
                cache_eligible,
                required = self.required_servers.binary_search(server_name).is_ok(),
                explicitly_required,
                "MCP startup readiness observed"
            );
        }
    }

    #[cfg(test)]
    pub(crate) async fn stable_catalog_revisions(
        &self,
        required_servers: &[String],
        required_plugins: &HashSet<String>,
    ) -> Option<HashMap<String, BindingCatalogRevision>> {
        self.stable_binding_identity(required_servers, required_plugins)
            .await
            .map(|identity| identity.catalog_revisions)
    }

    pub(crate) async fn stable_binding_identity(
        &self,
        required_servers: &[String],
        required_plugins: &HashSet<String>,
    ) -> Option<StableMcpBindingIdentity> {
        let mut catalog_revisions = HashMap::with_capacity(self.servers.len());
        let mut connection_ids = Vec::with_capacity(self.servers.len());
        for (server_name, view) in &self.servers {
            match view
                .connection
                .stable_connection_state(view.startup_readiness, |tools| {
                    view.accepts_cached_tools(server_name, tools)
                })
                .await
            {
                StableMcpConnectionState::Ready {
                    connection_id,
                    catalog_revision,
                } => {
                    connection_ids.push((server_name.clone(), connection_id));
                    catalog_revisions.insert(
                        server_name.clone(),
                        BindingCatalogRevision::Ready(catalog_revision),
                    );
                }
                StableMcpConnectionState::Cached {
                    connection_id,
                    catalog_revision,
                } => {
                    // Explicit requirements still wait for a live connection during capture.
                    if !view.allows_cached_startup()
                        || required_servers
                            .iter()
                            .any(|required| required == server_name)
                        || (self.is_selected_plugin_mcp_server(server_name)
                            && self
                                .plugin_id_for_mcp_server_name(server_name)
                                .is_some_and(|plugin_id| required_plugins.contains(plugin_id)))
                    {
                        return None;
                    }
                    connection_ids.push((server_name.clone(), connection_id));
                    catalog_revisions.insert(
                        server_name.clone(),
                        BindingCatalogRevision::Cached(catalog_revision),
                    );
                }
                StableMcpConnectionState::TerminalFailure
                    if server_name != CODEX_APPS_MCP_SERVER_NAME
                        && self.required_servers.binary_search(server_name).is_err() =>
                {
                    continue;
                }
                StableMcpConnectionState::TerminalFailure
                | StableMcpConnectionState::PendingOrClosed => return None,
            }
        }
        connection_ids.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Some(StableMcpBindingIdentity {
            catalog_revisions,
            connection_ids,
        })
    }

    /// Returns all tools with model-visible names normalized.
    pub async fn list_all_tools(&self) -> Vec<ToolInfo> {
        Box::pin(self.list_tools_with_errors(|_| true)).await.0
    }

    #[instrument(level = "trace", skip_all, fields(mcp_server_count = self.servers.len()))]
    pub(crate) async fn list_tools_with_errors(
        &self,
        include_server: impl Fn(&str) -> bool,
    ) -> (Vec<ToolInfo>, HashMap<String, String>) {
        let mut tools = Vec::new();
        let mut errors = HashMap::new();
        let mut available_server_count = 0;
        let mut unavailable_server_count = 0;
        let server_results = join_all(
            self.servers
                .iter()
                .filter(|(name, _)| include_server(name))
                .map(|(server_name, view)| async move {
                    let has_cached_tools = view.connection.has_cached_tools();
                    if !has_cached_tools {
                        view.trigger_startup().await;
                    }
                    view.connection
                        .reconnect_failed_startup(Arc::clone(&self.session_route))
                        .await;
                    let startup_complete = view.connection.startup_complete();
                    let context = Arc::clone(&self.tool_plugin_context);
                    let tool_filter = view.tool_filter.clone();
                    let server_tools = view
                        .connection
                        .run_mcp_request(
                            Arc::clone(&self.session_route),
                            move |client| async move {
                                let tools = client
                                    .listed_tools(context.as_ref())
                                    .await
                                    .map_err(anyhow::Error::from)?;
                                Ok(filter_tools(tools, &tool_filter))
                            },
                        )
                        .instrument(trace_span!(
                            "list_tools_for_server",
                            server_name = %server_name,
                            has_cached_tools,
                            startup_complete
                        ))
                        .await;
                    let result = match server_tools {
                        Ok(server_tools) => Ok(server_tools
                            .into_iter()
                            .map(|tool| Self::with_server_metadata(tool, &view.metadata))
                            .collect::<Vec<_>>()),
                        Err(error) => {
                            trace!(
                                server_name = %server_name,
                                has_cached_tools,
                                startup_complete,
                                "MCP server tools unavailable while building tool list"
                            );
                            Err(error)
                        }
                    };
                    (server_name, result)
                }),
        )
        .await;
        for (server_name, server_tools) in server_results {
            match server_tools {
                Ok(server_tools) => {
                    available_server_count += 1;
                    tools.extend(server_tools);
                }
                Err(error) => {
                    unavailable_server_count += 1;
                    errors.insert(server_name.clone(), error.to_string());
                }
            }
        }
        let tools = normalize_tools_for_model_with_prefix(
            tools,
            self.prefix_mcp_tool_names,
            &self.non_prefixed_mcp_tool_servers,
        );
        trace!(
            available_server_count,
            unavailable_server_count,
            tool_count = tools.len(),
            "built MCP tool list"
        );
        (tools, errors)
    }

    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn capture_binding_with_metadata(
        self: &Arc<Self>,
        config: Arc<crate::McpConfig>,
        plugins_available: bool,
        required_servers: &[String],
        required_plugins: &HashSet<String>,
    ) -> McpBinding {
        let mut listed_tools = Vec::new();
        let mut clients = HashMap::new();
        let catalog_log_enabled = tracing::enabled!(
            target: "codex_otel.trace_safe",
            tracing::Level::INFO
        );
        let catalog_metrics = codex_otel::global();
        let catalog_measurement_enabled = catalog_log_enabled || catalog_metrics.is_some();
        let product_sku = codex_otel::bounded_product_sku(Some(
            config
                .apps_mcp_product_sku
                .as_deref()
                .unwrap_or(crate::mcp::DEFAULT_CODEX_APPS_MCP_PRODUCT_SKU),
        ))
        .unwrap_or("unknown");
        let optional_mcp_startup_grace = config.optional_mcp_startup_grace;
        let server_snapshots = join_all(self.servers.iter().map(|(server_name, view)| async move {
            if !view.connection.startup_complete() {
                let required = self.required_servers.binary_search(server_name).is_ok();
                // Keep the catalog that lets us skip startup even if it expires during the wait.
                let cached_tools = view.cached_startup_tools(server_name, /*fallback*/ None);
                let has_cached_tools = cached_tools.is_some();
                let must_wait_for_startup = (required
                    && (!view.allows_cached_startup() || !has_cached_tools))
                    || required_servers
                        .iter()
                        .any(|required| required == server_name)
                    || (self.is_selected_plugin_mcp_server(server_name)
                        && self
                            .plugin_id_for_mcp_server_name(server_name)
                            .is_some_and(|plugin_id| required_plugins.contains(plugin_id)))
                    || (server_name == CODEX_APPS_MCP_SERVER_NAME && !has_cached_tools);
                if !must_wait_for_startup && has_cached_tools {
                    trace!(server_name = %server_name, "using cached MCP catalog without waiting for startup");
                    return (server_name, view, cached_tools);
                }
                if !must_wait_for_startup && optional_mcp_startup_grace.is_zero() {
                    view.connection.optional_startup_deadline(
                        tokio::time::Instant::now(),
                        optional_mcp_startup_grace,
                    );
                } else if !must_wait_for_startup {
                    let optional_startup_deadline = if view.startup_is_dormant() {
                        tokio::time::Instant::now() + optional_mcp_startup_grace
                    } else {
                        *self.optional_startup_deadline.get_or_init(|| {
                            tokio::time::Instant::now() + optional_mcp_startup_grace
                        })
                    };
                    let startup_deadline = view
                        .connection
                        .optional_startup_deadline(optional_startup_deadline, optional_mcp_startup_grace);
                    if tokio::time::timeout_at(startup_deadline, async {
                        view.trigger_startup().await;
                        view.connection
                            .await_current_startup_preserving_connection(Arc::clone(
                                &self.session_route,
                            ))
                            .await
                    })
                    .await
                    .is_err()
                    {
                        trace!(server_name = %server_name, "omitting pending optional MCP server");
                    }
                    return (server_name, view, cached_tools);
                }
                view.trigger_startup().await;
                let _ = view
                    .connection
                    .await_current_startup_preserving_connection(Arc::clone(&self.session_route))
                    .await;
                return (server_name, view, cached_tools);
            }
            (server_name, view, None)
        }))
        .await;
        let server_results = join_all(server_snapshots.into_iter().map(
            |(server_name, view, cached_tools)| async move {
                let measure_catalog = |tools: &[ToolInfo], catalog_source| {
                    let raw_definition_json_bytes = tool_definition_json_bytes(
                        tools.iter().map(|tool| &tool.tool),
                        catalog_measurement_enabled,
                    );
                    if catalog_log_enabled {
                        let plugin_id = self.plugin_id_for_mcp_server_name(server_name);
                        emit_binding_catalog(
                            product_sku,
                            if server_name == CODEX_APPS_MCP_SERVER_NAME {
                                "codex_apps"
                            } else if plugin_id.is_some() {
                                "plugin"
                            } else {
                                "configured"
                            },
                            plugin_id,
                            catalog_source,
                            tools.len(),
                            raw_definition_json_bytes,
                        );
                    }
                    raw_definition_json_bytes
                };
                let startup_pending = !view.connection.startup_complete();
                let accepts_cached_catalog = view.startup_readiness == McpStartupReadiness::Catalog;
                let cached_tools = if startup_pending
                    || (accepts_cached_catalog && !view.connection.has_ready_client())
                {
                    view.connection.prepared_cached_tools(
                        cached_tools,
                        self.tool_plugin_context.as_ref(),
                        |tools| view.accepts_cached_tools(server_name, tools),
                        |tools| measure_catalog(tools, "cached"),
                    )
                } else {
                    None
                };
                let (client, server_tools, raw_definition_json_bytes) =
                    if let Some((tools, bytes)) = cached_tools {
                        (None, tools, bytes)
                    } else {
                        // Required catalog readiness must still wait if caching was disabled
                        // after the first pass, rather than silently omitting the server.
                        if startup_pending {
                            if !(accepts_cached_catalog
                                && self.required_servers.binary_search(server_name).is_ok())
                            {
                                return None;
                            }
                            view.trigger_startup().await;
                            let _ = view
                                .connection
                                .await_current_startup_preserving_connection(Arc::clone(
                                    &self.session_route,
                                ))
                                .await;
                        }
                        let Some((client, snapshot, tools)) = view
                            .connection
                            .capture_ready_client_and_tools(
                                Arc::clone(&self.session_route),
                                Arc::clone(&self.tool_plugin_context),
                                view.tool_timeout,
                            )
                            .await
                        else {
                            trace!(
                                server_name = %server_name,
                                "omitting MCP server without an exact ready client"
                            );
                            return None;
                        };
                        let bytes = measure_catalog(snapshot.tools.as_ref(), "live");
                        (Some((client, snapshot)), tools, bytes)
                    };
                let server_tools = filter_tools(server_tools, &view.tool_filter);
                let server_tools = server_tools
                    .into_iter()
                    .map(|mut tool| {
                        if client.is_none()
                            && let Some(annotations) = tool.tool.annotations.as_mut()
                        {
                            annotations.read_only_hint = None;
                        }
                        Self::with_server_metadata(tool, &view.metadata)
                    })
                    .collect::<Vec<_>>();
                Some((
                    server_name.clone(),
                    client,
                    server_tools,
                    raw_definition_json_bytes,
                ))
            },
        ))
        .await;
        let mut raw_definition_json_bytes = 0usize;
        for (server_name, client, server_tools, server_definition_json_bytes) in
            server_results.into_iter().flatten()
        {
            raw_definition_json_bytes =
                raw_definition_json_bytes.saturating_add(server_definition_json_bytes);
            if let Some((client, snapshot)) = client {
                clients.insert(server_name, (client, snapshot));
            }
            listed_tools.extend(server_tools);
        }
        if let Some(metrics) = catalog_metrics.as_ref() {
            record_binding_catalog_size(metrics, product_sku, raw_definition_json_bytes);
        }
        let listed_tools = normalize_tools_for_model_with_prefix(
            listed_tools,
            self.prefix_mcp_tool_names,
            &self.non_prefixed_mcp_tool_servers,
        );
        let mut tools = Vec::with_capacity(listed_tools.len());
        let mut calls = std::collections::HashMap::with_capacity(listed_tools.len());
        for tool_info in listed_tools {
            let model_visible = crate::tool_is_model_visible(&tool_info);
            let Some((client, snapshot)) = clients.get(&tool_info.server_name) else {
                if model_visible {
                    tools.push(tool_info);
                }
                continue;
            };
            let Some(call) = self.prepare_call(
                &tool_info,
                client.clone(),
                Arc::clone(&config),
                Arc::clone(snapshot),
            ) else {
                trace!(
                    server_name = %tool_info.server_name,
                    tool_name = %tool_info.tool.name,
                    "omitting MCP tool without an exact ready client"
                );
                continue;
            };
            calls.insert(
                (
                    tool_info.server_name.clone(),
                    tool_info.tool.name.to_string(),
                ),
                call,
            );
            if model_visible {
                tools.push(tool_info);
            }
        }
        let clients = Arc::new(McpBindingClients::new(
            clients
                .into_iter()
                .map(|(server_name, (client, _))| (server_name, client))
                .collect(),
        ));
        McpBinding::new(
            Arc::clone(self),
            clients,
            config,
            plugins_available,
            tools,
            calls,
        )
    }

    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn prepare_call_for_tool(
        self: &Arc<Self>,
        config: Arc<crate::McpConfig>,
        advertised_tool: &ToolInfo,
    ) -> Option<PreparedMcpCall> {
        let server_name = &advertised_tool.server_name;
        let view = self.servers.get(server_name)?;
        if !view.tool_filter.allows(&advertised_tool.tool.name) {
            return None;
        }
        view.trigger_startup().await;
        let (client, snapshot, tools) = view
            .connection
            .capture_ready_client_and_tools(
                Arc::clone(&self.session_route),
                Arc::clone(&self.tool_plugin_context),
                view.tool_timeout,
            )
            .await?;
        let mut tool_info = tools.into_iter().find(|tool| {
            tool.server_name == *server_name
                && tool.tool.name == advertised_tool.tool.name
                && tool.connector_id == advertised_tool.connector_id
        })?;
        if !tool_is_model_visible(&tool_info) {
            return None;
        }
        tool_info = Self::with_server_metadata(tool_info, &view.metadata);
        // Preserve the globally normalized identity advertised to the model, while
        // taking schema, annotations, and approval metadata from the current catalog.
        tool_info
            .callable_namespace
            .clone_from(&advertised_tool.callable_namespace);
        tool_info
            .callable_name
            .clone_from(&advertised_tool.callable_name);
        self.prepare_call(&tool_info, client, config, snapshot)
    }

    fn prepare_call(
        self: &Arc<Self>,
        tool_info: &ToolInfo,
        client: McpPooledBindingClient,
        config: Arc<crate::McpConfig>,
        tool_catalog_snapshot: Arc<ToolCatalogSnapshot>,
    ) -> Option<PreparedMcpCall> {
        let server_name = &tool_info.server_name;
        let view = self.servers.get(server_name)?;
        PreparedMcpCall::new(
            Arc::clone(self),
            client,
            config,
            tool_catalog_snapshot,
            tool_info.clone(),
            view.metadata.clone(),
            self.plugin_id_for_mcp_server_name(server_name)
                .map(str::to_string),
            self.is_selected_plugin_mcp_server(server_name),
        )
    }

    /// Refreshes one exact Apps catalog, preserving the raw inventory for app policy.
    pub(crate) async fn refresh_codex_apps_client_catalog(
        &self,
        config: &crate::McpConfig,
    ) -> Result<CodexAppsToolSnapshot> {
        let refresh_start = Instant::now();
        let view = self
            .servers
            .get(CODEX_APPS_MCP_SERVER_NAME)
            .ok_or_else(|| anyhow!("unknown MCP server '{CODEX_APPS_MCP_SERVER_NAME}'"))?;
        let (tools, _) = self.refresh_codex_apps_tool_catalog().await?;
        let server_has_permission = config
            .permission_profile_for_server(CODEX_APPS_MCP_SERVER_NAME)
            .is_some();
        let model_visible_tool_names = tools
            .iter()
            .filter(|tool| {
                server_has_permission
                    && view.tool_filter.allows(&tool.tool.name)
                    && self
                        .tool_plugin_context
                        .allows_connector_id(tool.connector_id.as_deref())
                    && tool_is_model_visible(tool)
            })
            .map(|tool| tool.tool.name.to_string())
            .collect();
        emit_duration(
            CODEX_APPS_REFRESH_DURATION_METRIC,
            refresh_start.elapsed(),
            &[("path", "legacy"), ("trigger", "explicit")],
        );
        Ok(CodexAppsToolSnapshot {
            tools,
            model_visible_tool_names,
        })
    }

    /// Refreshes Apps tools and returns the prepared shared-cache winner for discovery.
    pub async fn refresh_codex_apps_tools_for_discovery(&self) -> Result<Vec<ToolInfo>> {
        let refresh_start = Instant::now();
        let view = self
            .servers
            .get(CODEX_APPS_MCP_SERVER_NAME)
            .ok_or_else(|| anyhow!("unknown MCP server '{CODEX_APPS_MCP_SERVER_NAME}'"))?;
        let (_, tools) = self.refresh_codex_apps_tool_catalog().await?;
        let tools = prepare_codex_apps_tools_for_model(
            filter_tools(tools, &view.tool_filter),
            &self.tool_plugin_context,
        )
        .into_iter()
        .map(|tool| Self::with_server_metadata(tool, &view.metadata));
        let tools = normalize_tools_for_model_with_prefix(
            tools,
            self.prefix_mcp_tool_names,
            &self.non_prefixed_mcp_tool_servers,
        );
        emit_duration(
            CODEX_APPS_REFRESH_DURATION_METRIC,
            refresh_start.elapsed(),
            &[("path", "legacy"), ("trigger", "explicit")],
        );
        Ok(tools)
    }

    /// Publishes the exact client catalog and returns both raw inventories.
    async fn refresh_codex_apps_tool_catalog(&self) -> Result<(Vec<ToolInfo>, Vec<ToolInfo>)> {
        let view = self
            .servers
            .get(CODEX_APPS_MCP_SERVER_NAME)
            .ok_or_else(|| anyhow!("unknown MCP server '{CODEX_APPS_MCP_SERVER_NAME}'"))?;
        view.trigger_startup().await;
        let tool_timeout = view.tool_timeout;
        let catalog_item_limit = view.catalog_item_limit;
        let (client_tools, tools, list_start) = view
            .connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| async move {
                let managed_client = client.client().await.context("failed to get client")?;
                let (tools, list_start) = managed_client
                    .tool_catalog
                    .refresh(
                        || async {
                            let list_start = Instant::now();
                            let fetch_ticket = managed_client
                                .codex_apps_tools_cache_context
                                .as_ref()
                                .map(|cache_context| {
                                    cache_context.begin_fetch(
                                        ConnectorRuntimeFetchSource::HardRefresh,
                                    )
                                });
                            let client_tools = list_tools_for_client_uncached(
                                CODEX_APPS_MCP_SERVER_NAME,
                                /*is_codex_apps_mcp_server*/ true,
                                /*codex_apps_refresh_trigger*/ "explicit",
                                &managed_client.client,
                                tool_timeout,
                                catalog_item_limit,
                                managed_client.server_instructions.as_deref(),
                            )
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to refresh tools for MCP server '{CODEX_APPS_MCP_SERVER_NAME}'"
                                )
                            })?;
                            Ok((client_tools, (fetch_ticket, list_start)))
                        },
                        |client_tools, (fetch_ticket, list_start)| {
                            // Discovery can accept another scope's winner; executable catalogs
                            // receive only the latest successful fetch from their own scope.
                            let tools = match (
                                managed_client.codex_apps_tools_cache_context.as_ref(),
                                fetch_ticket,
                            ) {
                                (Some(cache_context), Some(fetch_ticket)) => cache_context
                                    .publish_if_newest_accepted(
                                        fetch_ticket,
                                        &managed_client.server_info,
                                        client_tools.to_vec(),
                                    ),
                                (None, None) => client_tools.to_vec(),
                                _ => {
                                    unreachable!("Codex Apps fetch ticket requires cache context")
                                }
                            };
                            (tools, list_start)
                        },
                    )
                    .await?;
                let client_tools = managed_client
                    .tool_catalog
                    .read(|catalog| catalog.tools.to_vec())
                    .await;
                Ok((client_tools, tools, list_start))
            })
            .await?;
        emit_duration(
            MCP_TOOLS_LIST_DURATION_METRIC,
            list_start.elapsed(),
            &[("cache", "miss")],
        );
        Ok((client_tools, tools))
    }

    fn with_server_metadata(mut tool: ToolInfo, metadata: &McpServerMetadata) -> ToolInfo {
        tool.supports_parallel_tool_calls = metadata.supports_parallel_tool_calls;
        tool.server_origin = metadata
            .origin
            .as_ref()
            .map(|origin| origin.as_str().to_string());
        tool
    }
}
