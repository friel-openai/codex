use codex_protocol::ThreadId;
use strum::AsRefStr;
use strum::Display;
use strum::EnumString;

/// Status attached to a directional thread-spawn edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum DirectionalThreadSpawnEdgeStatus {
    Open,
    /// Ownership ended because the child moved to another root.
    Closed,
    /// Ownership ended because `close_agent` permanently closed this subtree.
    PermanentlyClosed,
}

/// One persisted incoming thread-spawn edge selected by child thread ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectionalThreadSpawnEdge {
    pub parent_thread_id: ThreadId,
    pub child_thread_id: ThreadId,
    pub status: DirectionalThreadSpawnEdgeStatus,
}

/// One canonical ownership edge in a registered current-only ancestry chain.
///
/// The incoming edge may be absent or may already be Open below an absent ancestor. Permanent
/// close validates the persisted form and materializes only absent edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentOnlyThreadSpawnEdge {
    /// Canonical registered parent of the current-only child.
    pub parent_thread_id: ThreadId,
    /// Registered current-only child that is not yet reachable from the persisted subtree root.
    pub child_thread_id: ThreadId,
}

/// One thread whose incoming spawn edge belongs to a closed subtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedThreadSpawnSubtreeMember {
    /// Thread whose incoming edge was selected for closure.
    pub thread_id: ThreadId,
    /// Distance from the selected subtree root, which has depth zero.
    pub depth: u32,
}

/// Transactional result of closing a persisted thread-spawn subtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedThreadSpawnSubtree {
    /// Selected subtree members ordered deepest-first, then by thread id.
    pub members: Vec<ClosedThreadSpawnSubtreeMember>,
    /// Edges that changed from Open to PermanentlyClosed in this transaction.
    pub newly_closed_edge_count: usize,
}
