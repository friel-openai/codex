use std::future::Future;
use std::pin::Pin;

use codex_protocol::ThreadId;

use crate::AgentGraphStoreResult;
use crate::ThreadSpawnEdge;
use crate::ThreadSpawnEdgeStatus;

/// Future returned by [`AgentGraphStore`] operations.
pub type AgentGraphStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentGraphStoreResult<T>> + Send + 'a>>;

/// Storage-neutral boundary for persisted thread-spawn parent/child topology.
///
/// Implementations are expected to return stable ordering for list methods so callers can merge
/// persisted graph state with live in-memory state without introducing nondeterministic output.
pub trait AgentGraphStore: Send + Sync {
    /// Insert or replace the directional parent/child edge for a spawned thread.
    ///
    /// `child_thread_id` has at most one persisted parent. Re-inserting the same child updates
    /// both the parent and status unless the existing edge is PermanentlyClosed, which must be
    /// rejected rather than reopened.
    fn upsert_thread_spawn_edge(
        &self,
        parent_thread_id: ThreadId,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()>;

    /// Update the persisted lifecycle status of a spawned thread's incoming edge.
    ///
    /// Implementations should treat missing children as a successful no-op and must not overwrite
    /// PermanentlyClosed.
    fn set_thread_spawn_edge_status(
        &self,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()>;

    /// Change an Open incoming edge to ordinary Closed without overwriting a stronger status.
    fn transition_open_thread_spawn_edge_to_closed(
        &self,
        _child_thread_id: ThreadId,
    ) -> AgentGraphStoreFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    /// List direct spawned children of a parent thread.
    ///
    /// When `status_filter` is `Some`, only child edges with that exact status are returned. When
    /// it is `None`, all direct child edges are returned regardless of status, including statuses
    /// that may be added by a future store implementation.
    fn list_thread_spawn_children(
        &self,
        parent_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>>;

    /// List spawned descendants breadth-first by depth, then by thread id.
    ///
    /// `status_filter` is applied to every traversed edge, not just to the returned descendants.
    /// For example, `Some(Open)` walks only open edges, so descendants under a closed edge are not
    /// included even if their own incoming edge is open. `None` walks and returns every persisted
    /// edge regardless of status.
    fn list_thread_spawn_descendants(
        &self,
        root_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>>;

    /// Return existing incoming edges for the supplied child thread IDs.
    ///
    /// Missing child IDs have no persisted ownership edge. Implementations should batch this
    /// lookup so large current registries do not issue one query per identity.
    fn list_thread_spawn_edges_by_child_ids(
        &self,
        _child_thread_ids: &[ThreadId],
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadSpawnEdge>> {
        Box::pin(async {
            Err(crate::AgentGraphStoreError::Internal {
                message: "incoming thread-spawn edge lookup is not implemented".to_string(),
            })
        })
    }

    /// Return open descendant identities together when this graph owns their indexed metadata.
    ///
    /// Stores that cannot combine graph authorization with identity lookup retain the existing
    /// descendant-list and individual metadata restoration behavior.
    fn list_open_thread_spawn_descendant_identities(
        &self,
        _root_thread_id: ThreadId,
    ) -> Option<AgentGraphStoreFuture<'_, Vec<codex_state::ThreadSpawnDescendantIdentity>>> {
        None
    }

    /// Find one open-owned descendant by thread id without restoring its siblings.
    ///
    /// Every edge from `root_thread_id` to the returned descendant must be open. A closed edge is
    /// therefore a durable authorization fence for this lookup.
    fn find_open_thread_spawn_descendant_by_id(
        &self,
        _root_thread_id: ThreadId,
        _descendant_thread_id: ThreadId,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ThreadSpawnDescendantIdentity>> {
        Box::pin(async { Ok(None) })
    }

    /// Find one open-owned descendant by canonical agent path without restoring its siblings.
    ///
    /// Every edge from `root_thread_id` to the returned descendant must be open. Implementations
    /// must reject an ambiguous canonical path instead of selecting an arbitrary descendant.
    fn find_open_thread_spawn_descendant_by_path(
        &self,
        _root_thread_id: ThreadId,
        _agent_path: &str,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ThreadSpawnDescendantIdentity>> {
        Box::pin(async { Ok(None) })
    }

    /// List descendants reachable through Open or PermanentlyClosed edges.
    ///
    /// Permanent close uses this set to fence cleanup retries before the transactional mutation.
    /// Ordinary Closed edges remain ownership boundaries.
    fn list_permanent_close_thread_spawn_descendants(
        &self,
        _target_thread_id: ThreadId,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async {
            Err(crate::AgentGraphStoreError::Internal {
                message: "permanent-close descendant lookup is not implemented".to_string(),
            })
        })
    }

    /// Close a target and descendants reachable only through open ownership edges.
    ///
    /// Ordinary Closed edges are ownership boundaries, including promotion and adoption
    /// boundaries. This operation upgrades Open edges and returns already-PermanentlyClosed
    /// descendants so an ancestor close completes interrupted cleanup.
    fn close_open_thread_spawn_subtree(
        &self,
        _owner_root_thread_id: ThreadId,
        _target_thread_id: ThreadId,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ClosedThreadSpawnSubtree>> {
        Box::pin(async {
            Err(crate::AgentGraphStoreError::Internal {
                message: "open-only subtree closure is not implemented".to_string(),
            })
        })
    }

    /// Close an open-owned subtree after materializing canonical current-only descendants.
    ///
    /// The caller must prove `current_only_descendant_edges` from the canonical current registry
    /// while holding the lifecycle-mutation fence. Implementations must validate those edges,
    /// insert missing edges, accept existing edges only when the parent and Open status match, and
    /// upgrade the selected subtree to PermanentlyClosed in one transaction. Ordinary Closed edges
    /// remain ownership boundaries.
    fn close_open_thread_spawn_subtree_with_current_only_descendants(
        &self,
        _owner_root_thread_id: ThreadId,
        _target_thread_id: ThreadId,
        _current_only_descendant_edges: Vec<codex_state::CurrentOnlyThreadSpawnEdge>,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ClosedThreadSpawnSubtree>> {
        Box::pin(async {
            Err(crate::AgentGraphStoreError::Internal {
                message: "open subtree closure with current-only descendants is not implemented"
                    .to_string(),
            })
        })
    }

    /// Materialize and close a registered current-only target plus its persisted descendants.
    ///
    /// The caller must first prove `current_only_ownership_edges` from the canonical current
    /// registry while holding the lifecycle-mutation fence. Implementations must materialize
    /// missing intermediate ownership as Open, accept existing edges only when the parent and Open
    /// status match, and upgrade the target and Open descendants in the same transaction. Ordinary
    /// Closed edges remain ownership boundaries.
    fn close_current_only_thread_spawn_subtree(
        &self,
        _owner_root_thread_id: ThreadId,
        _target_thread_id: ThreadId,
        _current_only_ownership_edges: Vec<codex_state::CurrentOnlyThreadSpawnEdge>,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ClosedThreadSpawnSubtree>> {
        Box::pin(async {
            Err(crate::AgentGraphStoreError::Internal {
                message: "current-only subtree closure is not implemented".to_string(),
            })
        })
    }

    /// Return one durable permanently closed subtree for idempotent cleanup retry.
    ///
    /// The target must remain owned through Open ancestor edges. Implementations must traverse
    /// only PermanentlyClosed descendants and stop at ordinary Closed ownership boundaries.
    fn get_permanently_closed_thread_spawn_subtree(
        &self,
        _owner_root_thread_id: ThreadId,
        _target_thread_id: ThreadId,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ClosedThreadSpawnSubtree>> {
        Box::pin(async { Ok(None) })
    }

    /// Extend one durable cleanup retry with canonical current-only descendants.
    ///
    /// The target must already be PermanentlyClosed and owned through Open ancestors. The caller
    /// must prove `current_only_descendant_edges` from the canonical current registry while holding
    /// the lifecycle-mutation fence. Implementations must materialize the supplied edges and close
    /// existing Open descendants in the same transaction.
    fn extend_permanently_closed_thread_spawn_subtree_with_current_only_descendants(
        &self,
        _owner_root_thread_id: ThreadId,
        _target_thread_id: ThreadId,
        _current_only_descendant_edges: Vec<codex_state::CurrentOnlyThreadSpawnEdge>,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ClosedThreadSpawnSubtree>> {
        Box::pin(async {
            Err(crate::AgentGraphStoreError::Internal {
                message: "permanently closed subtree extension is not implemented".to_string(),
            })
        })
    }

    /// Upgrade one legacy one-edge close and its still-Open descendants to permanent closure.
    ///
    /// `expected_parent_thread_id` must match the target's stored incoming edge. Ordinary Closed
    /// descendants remain ownership boundaries.
    fn repair_legacy_closed_thread_spawn_subtree(
        &self,
        _owner_root_thread_id: ThreadId,
        _target_thread_id: ThreadId,
        _expected_parent_thread_id: ThreadId,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ClosedThreadSpawnSubtree>> {
        Box::pin(async { Ok(None) })
    }

    /// Upgrade a legacy close after materializing canonical current-only descendants.
    ///
    /// This has the same authorization contract as [`Self::repair_legacy_closed_thread_spawn_subtree`]
    /// and additionally persists `current_only_descendant_edges` in the same transaction.
    fn repair_legacy_closed_thread_spawn_subtree_with_current_only_descendants(
        &self,
        _owner_root_thread_id: ThreadId,
        _target_thread_id: ThreadId,
        _expected_parent_thread_id: ThreadId,
        _current_only_descendant_edges: Vec<codex_state::CurrentOnlyThreadSpawnEdge>,
    ) -> AgentGraphStoreFuture<'_, Option<codex_state::ClosedThreadSpawnSubtree>> {
        Box::pin(async {
            Err(crate::AgentGraphStoreError::Internal {
                message: "legacy subtree repair with current-only descendants is not implemented"
                    .to_string(),
            })
        })
    }
}
