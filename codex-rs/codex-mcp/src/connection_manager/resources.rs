use crate::CODEX_APPS_MCP_SERVER_NAME;
use crate::McpRuntime;
use codex_rmcp_client::RmcpClient;
use rmcp::model::ServerResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ReadResourceRequestParams;
use rmcp::model::ReadResourceResult;
use rmcp::model::Resource;
use rmcp::model::ResourceTemplate;
use tokio::task::JoinSet;
use tracing::warn;

use super::McpConnectionSet;
use crate::pagination::collect_paginated;
use crate::resource_client::McpResourceServerCacheKey;

impl McpConnectionSet {
    pub(crate) fn resource_cache_key(
        &self,
        server: &str,
        generation: u64,
    ) -> Option<McpResourceServerCacheKey> {
        self.servers.get(server).and_then(|view| {
            view.connection
                .current_connection_id()
                .map(|connection_id| McpResourceServerCacheKey {
                    connection_id,
                    generation,
                })
        })
    }

    /// Returns resources from servers selected by `include_server`.
    pub async fn list_all_resources(
        &self,
        include_server: impl Fn(&str) -> bool,
    ) -> HashMap<String, Vec<Resource>> {
        let mut join_set = JoinSet::new();
        for (server_name, view) in self
            .servers
            .iter()
            .filter(|(server_name, _)| include_server(server_name))
        {
            let server_name = server_name.clone();
            view.trigger_startup().await;
            let connection = view.connection.clone();
            let session_route = Arc::clone(&self.session_route);
            let timeout = view.tool_timeout;
            join_set.spawn(async move {
                let result = async {
                    connection
                        .await_current_startup(Arc::clone(&session_route))
                        .await
                        .context("failed to get client")?;
                    connection
                        .run_mcp_request(session_route, move |client| async move {
                            let managed = client.client().await.context("failed to get client")?;
                            let client = Arc::new(managed);
                            collect_paginated("resources/list", timeout, |params| {
                                let client = Arc::clone(&client);
                                async move {
                                    let response = client.list_resources(params, timeout).await?;
                                    Ok((response.resources, response.next_cursor))
                                }
                            })
                            .await
                        })
                        .await
                }
                .await;
                (server_name, result)
            });
        }

        let mut resources = HashMap::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((server_name, Ok(server_resources))) => {
                    resources.insert(server_name, server_resources);
                }
                Ok((server_name, Err(error))) => {
                    warn!("Failed to list resources for MCP server '{server_name}': {error:#}");
                }
                Err(error) => {
                    warn!("Task panic when listing resources for MCP server: {error:#}");
                }
            }
        }
        resources
    }

    /// Returns resource templates from servers selected by `include_server`.
    pub async fn list_all_resource_templates(
        &self,
        include_server: impl Fn(&str) -> bool,
    ) -> HashMap<String, Vec<ResourceTemplate>> {
        let mut join_set = JoinSet::new();
        for (server_name, view) in self.servers.iter().filter(|(server_name, _)| {
            include_server(server_name) && !self.skips_resource_template_listing(server_name)
        }) {
            let server_name = server_name.clone();
            view.trigger_startup().await;
            let connection = view.connection.clone();
            let session_route = Arc::clone(&self.session_route);
            let timeout = view.tool_timeout;
            join_set.spawn(async move {
                let result = async {
                    connection
                        .await_current_startup(Arc::clone(&session_route))
                        .await
                        .context("failed to get client")?;
                    connection
                        .run_mcp_request(session_route, move |client| async move {
                            let managed = client.client().await.context("failed to get client")?;
                            let client = Arc::new(managed);
                            collect_paginated("resources/templates/list", timeout, |params| {
                                let client = Arc::clone(&client);
                                async move {
                                    let response =
                                        client.list_resource_templates(params, timeout).await?;
                                    Ok((response.resource_templates, response.next_cursor))
                                }
                            })
                            .await
                        })
                        .await
                }
                .await;
                (server_name, result)
            });
        }

        let mut templates = HashMap::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((server_name, Ok(server_templates))) => {
                    templates.insert(server_name, server_templates);
                }
                Ok((server_name, Err(error))) => {
                    warn!(
                        "Failed to list resource templates for MCP server '{server_name}': {error:#}"
                    );
                }
                Err(error) => {
                    warn!("Task panic when listing resource templates for MCP server: {error:#}");
                }
            }
        }
        templates
    }

    pub async fn list_resources(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourcesResult> {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        view.trigger_startup().await;
        view.connection
            .await_current_startup(Arc::clone(&self.session_route))
            .await
            .context("failed to get client")?;
        let server = server.to_string();
        let timeout = view.tool_timeout;
        view.connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| async move {
                let managed = client.client().await.context("failed to get client")?;
                managed
                    .list_resources(params, timeout)
                    .await
                    .with_context(|| format!("resources/list failed for `{server}`"))
            })
            .await
    }

    pub async fn list_resource_templates(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourceTemplatesResult> {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        view.trigger_startup().await;
        view.connection
            .await_current_startup(Arc::clone(&self.session_route))
            .await
            .context("failed to get client")?;
        let server = server.to_string();
        let timeout = view.tool_timeout;
        view.connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| async move {
                let managed = client.client().await.context("failed to get client")?;
                managed
                    .list_resource_templates(params, timeout)
                    .await
                    .with_context(|| format!("resources/templates/list failed for `{server}`"))
            })
            .await
    }

    // Hosted Apps always returns an empty list. Resume discovery if it gains templates.
    pub(crate) fn skips_resource_template_listing(&self, server: &str) -> bool {
        self.contains_server(server)
            && self
                .config
                .as_ref()
                .and_then(|config| config.mcp_server_catalog.server(server))
                .is_some_and(|registration| {
                    registration
                        .source()
                        .is_host_owned_apps(server, registration.config())
                })
    }

    pub async fn read_resource(
        &self,
        server: &str,
        params: ReadResourceRequestParams,
    ) -> Result<ReadResourceResult> {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        view.trigger_startup().await;
        view.connection
            .await_current_startup(Arc::clone(&self.session_route))
            .await
            .context("failed to get client")?;
        let server = server.to_string();
        let timeout = view.tool_timeout;
        let binding = view
            .connection
            .bind_mcp_request(Arc::clone(&self.session_route), |client| async move {
                Ok(Arc::new(
                    client.client().await.context("failed to get client")?,
                ))
            })
            .await?;
        binding
            .run_wire(move |managed, lease| async move {
                let uri = params.uri.clone();
                managed
                    .read_resource(params, timeout, lease)
                    .await
                    .with_context(|| format!("resources/read failed for `{server}` ({uri})"))
            })
            .await
    }

    pub(crate) fn server_environment_id(&self, server: &str) -> Option<&str> {
        self.servers
            .get(server)
            .map(|view| view.metadata.environment_id.as_str())
    }

    pub(crate) fn early_apps_generation(&self) -> Option<u64> {
        if !self.config.as_ref()?.apps_early_resource_discovery {
            return None;
        }
        self.servers
            .get(CODEX_APPS_MCP_SERVER_NAME)?
            .connection
            .pending_early_apps_generation()
    }

    /// Discard the page if startup fails or the runtime changes its physical generation,
    /// auth scope or resource generation before this request completes.
    pub(crate) async fn with_early_apps_client<F, Fut>(
        &self,
        runtime: &McpRuntime,
        expected_generation: u64,
        request: F,
    ) -> Result<ServerResult>
    where
        F: FnOnce(Arc<RmcpClient>, Option<Duration>) -> Fut,
        Fut: std::future::Future<Output = Result<ServerResult>>,
    {
        let view = self
            .servers
            .get(CODEX_APPS_MCP_SERVER_NAME)
            .context("Apps registration was removed")?;
        let auth = runtime.auth_cache_key_for_server(CODEX_APPS_MCP_SERVER_NAME);
        let key = runtime
            .resource_cache_key(CODEX_APPS_MCP_SERVER_NAME)
            .context("Apps registration was removed")?;
        anyhow::ensure!(
            key.connection_id == expected_generation,
            "Apps connection was replaced"
        );
        if let Some(trigger) = &view.startup_trigger {
            // trigger_startup waits for the final status, which follows tools/list.
            trigger.send_replace(true);
        }
        let response = view
            .connection
            .run_early_apps_catalog(
                Arc::clone(&self.session_route),
                expected_generation,
                view.startup_timeout,
                |client| request(client, view.tool_timeout),
            )
            .await?;
        anyhow::ensure!(
            Some(key) == runtime.resource_cache_key(CODEX_APPS_MCP_SERVER_NAME)
                && auth == runtime.auth_cache_key_for_server(CODEX_APPS_MCP_SERVER_NAME),
            "Apps connection changed during catalog discovery"
        );
        Ok(response)
    }
}
