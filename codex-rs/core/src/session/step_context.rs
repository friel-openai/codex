use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::agents_md::LoadedAgentsMd;
use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::session::McpRuntimeSnapshot;
use crate::session::turn_context::TurnContext;
use codex_exec_server::ExecutorCapabilityDiscoverySnapshot;
use codex_exec_server::ResolvedSelectedCapabilityRoot;
use codex_mcp::ToolInfo;
use tokio::sync::OnceCell;

/// Request-scoped state that may change between model sampling requests.
#[derive(Debug)]
pub(crate) struct StepContext {
    pub(crate) turn: Arc<TurnContext>,
    pub(crate) environments: TurnEnvironmentSnapshot,
    /// Capability roots bound to ready environments in this exact step.
    pub(crate) selected_capability_roots: Vec<ResolvedSelectedCapabilityRoot>,
    /// Executor-materialized capability files shared by MCP and skills in this exact step.
    pub(crate) executor_capability_discovery: Option<Arc<ExecutorCapabilityDiscoverySnapshot>>,
    /// The exact MCP config and manager used to advertise and execute tools for this step.
    pub(crate) mcp: Arc<McpRuntimeSnapshot>,
    /// The fixed MCP tool list used for this exact sampling request.
    mcp_tool_snapshot: OnceCell<Vec<ToolInfo>>,
    /// The canonical AGENTS.md value observed with this environment snapshot.
    pub(crate) loaded_agents_md: Option<Arc<LoadedAgentsMd>>,
    /// Coordinates a tool-triggered context transition at a model sampling boundary.
    context_transition: ContextTransitionState,
}

/// Request-scoped state for a tool that replaces the active turn context.
///
/// A transition tool must be the only tool call in its model response. Once it succeeds, the
/// caller rebuilds the turn context before sending another model request.
#[derive(Debug, Default)]
struct ContextTransitionState {
    mixed_with_sibling_tool: AtomicBool,
    refresh_requested: AtomicBool,
}

impl StepContext {
    pub(crate) fn new(
        turn: Arc<TurnContext>,
        environments: TurnEnvironmentSnapshot,
        selected_capability_roots: Vec<ResolvedSelectedCapabilityRoot>,
        executor_capability_discovery: Option<Arc<ExecutorCapabilityDiscoverySnapshot>>,
        mcp: Arc<McpRuntimeSnapshot>,
        loaded_agents_md: Option<Arc<LoadedAgentsMd>>,
    ) -> Self {
        Self {
            turn,
            environments,
            selected_capability_roots,
            executor_capability_discovery,
            mcp,
            mcp_tool_snapshot: OnceCell::new(),
            loaded_agents_md,
            context_transition: ContextTransitionState::default(),
        }
    }

    pub(crate) async fn mcp_tools(&self) -> &[ToolInfo] {
        self.mcp_tool_snapshot
            .get_or_init(|| self.mcp.manager().list_all_tools())
            .await
    }

    pub(crate) fn reject_context_transition_mixed_with_sibling_tool(&self) {
        self.context_transition
            .mixed_with_sibling_tool
            .store(true, Ordering::Release);
    }

    pub(crate) fn context_transition_has_sibling_tool(&self) -> bool {
        self.context_transition
            .mixed_with_sibling_tool
            .load(Ordering::Acquire)
    }

    pub(crate) fn request_turn_context_refresh(&self) {
        self.context_transition
            .refresh_requested
            .store(true, Ordering::Release);
    }

    pub(crate) fn turn_context_refresh_requested(&self) -> bool {
        self.context_transition
            .refresh_requested
            .load(Ordering::Acquire)
    }
}
