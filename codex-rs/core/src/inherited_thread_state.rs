use codex_protocol::ThreadId;

use crate::client::ResponseContinuation;
use crate::state::McpToolSnapshot;

#[derive(Clone, Default)]
pub(crate) struct InheritedThreadState {
    prompt_cache_key: Option<ThreadId>,
    response_continuation: Option<ResponseContinuation>,
    mcp_tool_snapshot: Option<McpToolSnapshot>,
    app_server_client_name: Option<String>,
    app_server_client_version: Option<String>,
}

impl InheritedThreadState {
    pub(crate) fn builder() -> InheritedThreadStateBuilder {
        InheritedThreadStateBuilder::default()
    }

    pub(crate) fn prompt_cache_key(&self) -> Option<ThreadId> {
        self.prompt_cache_key
    }

    pub(crate) fn response_continuation(&self) -> Option<ResponseContinuation> {
        self.response_continuation.clone()
    }

    pub(crate) fn mcp_tool_snapshot(&self) -> Option<McpToolSnapshot> {
        self.mcp_tool_snapshot.clone()
    }

    pub(crate) fn app_server_client_name(&self) -> Option<&str> {
        self.app_server_client_name.as_deref()
    }

    pub(crate) fn app_server_client_version(&self) -> Option<&str> {
        self.app_server_client_version.as_deref()
    }
}

#[derive(Default)]
pub(crate) struct InheritedThreadStateBuilder {
    prompt_cache_key: Option<ThreadId>,
    response_continuation: Option<ResponseContinuation>,
    mcp_tool_snapshot: Option<McpToolSnapshot>,
    app_server_client_name: Option<String>,
    app_server_client_version: Option<String>,
}

impl InheritedThreadStateBuilder {
    pub(crate) fn prompt_cache_key(mut self, prompt_cache_key: Option<ThreadId>) -> Self {
        self.prompt_cache_key = prompt_cache_key;
        self
    }

    pub(crate) fn response_continuation(
        mut self,
        response_continuation: Option<ResponseContinuation>,
    ) -> Self {
        self.response_continuation = response_continuation;
        self
    }

    pub(crate) fn mcp_tool_snapshot(mut self, mcp_tool_snapshot: Option<McpToolSnapshot>) -> Self {
        self.mcp_tool_snapshot = mcp_tool_snapshot;
        self
    }

    pub(crate) fn app_server_client_metadata(
        mut self,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
    ) -> Self {
        self.app_server_client_name = app_server_client_name;
        self.app_server_client_version = app_server_client_version;
        self
    }

    pub(crate) fn build(self) -> InheritedThreadState {
        InheritedThreadState {
            prompt_cache_key: self.prompt_cache_key,
            response_continuation: self.response_continuation,
            mcp_tool_snapshot: self.mcp_tool_snapshot,
            app_server_client_name: self.app_server_client_name,
            app_server_client_version: self.app_server_client_version,
        }
    }
}
