use super::StateRuntime;
use crate::DirectionalThreadSpawnEdge;
use crate::DirectionalThreadSpawnEdgeStatus;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;

fn graph_thread_id(suffix: u128) -> ThreadId {
    ThreadId::from_string(&format!("00000000-0000-0000-0000-{suffix:012x}"))
        .expect("valid graph thread id")
}

#[tokio::test]
async fn current_only_close_materializes_missing_ancestor_chain_for_restart_retry() {
    let runtime = runtime().await;
    let root_thread_id = ThreadId::new();
    let current_only_parent_thread_id = ThreadId::new();
    let current_only_target_thread_id = ThreadId::new();

    let closed = runtime
        .close_current_only_thread_spawn_subtree(
            root_thread_id,
            current_only_target_thread_id,
            vec![
                crate::CurrentOnlyThreadSpawnEdge {
                    parent_thread_id: root_thread_id,
                    child_thread_id: current_only_parent_thread_id,
                },
                crate::CurrentOnlyThreadSpawnEdge {
                    parent_thread_id: current_only_parent_thread_id,
                    child_thread_id: current_only_target_thread_id,
                },
            ],
        )
        .await
        .expect("nested current-only close")
        .expect("missing ownership chain should be materialized");
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: vec![crate::ClosedThreadSpawnSubtreeMember {
                thread_id: current_only_target_thread_id,
                depth: 0,
            }],
            newly_closed_edge_count: 1,
        }
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("materialized ancestor ownership"),
        vec![current_only_parent_thread_id]
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                current_only_parent_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("materialized target closure"),
        vec![current_only_target_thread_id]
    );
    assert_eq!(
        runtime
            .get_permanently_closed_thread_spawn_subtree(
                root_thread_id,
                current_only_target_thread_id,
            )
            .await
            .expect("restart-safe cleanup retry")
            .expect("open ancestor chain should authorize target cleanup"),
        crate::ClosedThreadSpawnSubtree {
            members: closed.members,
            newly_closed_edge_count: 0,
        }
    );
}

#[tokio::test]
async fn current_only_close_materializes_target_and_closes_open_descendants() {
    let runtime = runtime().await;
    let root_thread_id = ThreadId::new();
    let current_only_thread_id = ThreadId::new();
    let child_thread_id = ThreadId::new();
    let detached_thread_id = ThreadId::new();
    runtime
        .upsert_thread_spawn_edge(
            current_only_thread_id,
            child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child edge");
    runtime
        .upsert_thread_spawn_edge(
            child_thread_id,
            detached_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("ordinary closed boundary");

    let closed = runtime
        .close_current_only_thread_spawn_subtree(
            root_thread_id,
            current_only_thread_id,
            vec![crate::CurrentOnlyThreadSpawnEdge {
                parent_thread_id: root_thread_id,
                child_thread_id: current_only_thread_id,
            }],
        )
        .await
        .expect("current-only close")
        .expect("missing target edge should be materialized");
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: vec![
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: child_thread_id,
                    depth: 1,
                },
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: current_only_thread_id,
                    depth: 0,
                },
            ],
            newly_closed_edge_count: 2,
        }
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                root_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("materialized target edge"),
        vec![current_only_thread_id]
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                current_only_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("closed child edge"),
        vec![child_thread_id]
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                child_thread_id,
                DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("detached boundary"),
        vec![detached_thread_id]
    );
    assert_eq!(
        runtime
            .get_permanently_closed_thread_spawn_subtree(root_thread_id, current_only_thread_id,)
            .await
            .expect("restart-safe cleanup retry")
            .expect("materialized target should authorize cleanup retry"),
        crate::ClosedThreadSpawnSubtree {
            members: closed.members.clone(),
            newly_closed_edge_count: 0,
        }
    );
    assert!(
        runtime
            .upsert_thread_spawn_edge(
                root_thread_id,
                current_only_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect_err("ownership transfer must not reopen the materialized permanent edge")
            .to_string()
            .contains("permanently closed")
    );
}

async fn runtime() -> std::sync::Arc<StateRuntime> {
    let codex_home = super::test_support::unique_temp_dir();
    StateRuntime::init(
        crate::SqliteConfig::new_for_testing(codex_home.as_path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("state db should initialize")
}

#[tokio::test]
async fn open_thread_spawn_descendant_lookup_requires_open_ownership_path() {
    let codex_home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(
        crate::SqliteConfig::new_for_testing(codex_home.as_path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("state db should initialize");
    let root_thread_id = graph_thread_id(/*suffix*/ 1_000);
    let open_child_thread_id = graph_thread_id(/*suffix*/ 1_001);
    let open_grandchild_thread_id = graph_thread_id(/*suffix*/ 1_002);
    let closed_child_thread_id = graph_thread_id(/*suffix*/ 1_003);
    let hidden_grandchild_thread_id = graph_thread_id(/*suffix*/ 1_004);
    let sibling_thread_id = graph_thread_id(/*suffix*/ 1_005);

    for (thread_id, agent_path) in [
        (open_child_thread_id, "/root/open"),
        (open_grandchild_thread_id, "/root/open/nested"),
        (closed_child_thread_id, "/root/closed"),
        (hidden_grandchild_thread_id, "/root/closed/hidden"),
        (sibling_thread_id, "/root/sibling"),
    ] {
        let mut metadata =
            super::test_support::test_thread_metadata(&codex_home, thread_id, codex_home.clone());
        metadata.agent_path = Some(agent_path.to_string());
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("thread metadata should persist");
    }
    for (parent_thread_id, child_thread_id, status) in [
        (
            root_thread_id,
            open_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            open_child_thread_id,
            open_grandchild_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            root_thread_id,
            closed_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        ),
        (
            closed_child_thread_id,
            hidden_grandchild_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            root_thread_id,
            sibling_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
    ] {
        runtime
            .upsert_thread_spawn_edge(parent_thread_id, child_thread_id, status)
            .await
            .expect("thread spawn edge should persist");
    }

    let by_id = runtime
        .find_open_thread_spawn_descendant_by_id(root_thread_id, open_grandchild_thread_id)
        .await
        .expect("open descendant id lookup should succeed")
        .expect("open grandchild should be found");
    let by_path = runtime
        .find_open_thread_spawn_descendant_by_path(root_thread_id, "/root/open/nested")
        .await
        .expect("open descendant path lookup should succeed")
        .expect("open grandchild should be found");
    assert_eq!(by_id, by_path);
    assert_eq!(by_id.thread_id, open_grandchild_thread_id);
    assert_eq!(by_id.parent_thread_id, open_child_thread_id);
    assert_eq!(by_id.depth, 2);
    assert_eq!(by_id.agent_path.as_deref(), Some("/root/open/nested"));

    for inaccessible_thread_id in [closed_child_thread_id, hidden_grandchild_thread_id] {
        assert_eq!(
            runtime
                .find_open_thread_spawn_descendant_by_id(root_thread_id, inaccessible_thread_id,)
                .await
                .expect("inaccessible descendant lookup should succeed"),
            None
        );
    }
    assert_eq!(
        runtime
            .find_open_thread_spawn_descendant_by_path(root_thread_id, "/root/closed/hidden")
            .await
            .expect("hidden path lookup should succeed"),
        None
    );
}

#[tokio::test]
async fn close_open_thread_spawn_subtree_stops_at_closed_ownership_boundary() {
    let runtime = runtime().await;
    let parent_thread_id = graph_thread_id(/*suffix*/ 2_100);
    let target_thread_id = graph_thread_id(/*suffix*/ 2_101);
    let open_child_thread_id = graph_thread_id(/*suffix*/ 2_102);
    let promoted_thread_id = graph_thread_id(/*suffix*/ 2_103);
    let promoted_child_thread_id = graph_thread_id(/*suffix*/ 2_104);
    for (parent_thread_id, child_thread_id, status) in [
        (
            parent_thread_id,
            target_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            target_thread_id,
            open_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            target_thread_id,
            promoted_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        ),
        (
            promoted_thread_id,
            promoted_child_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
    ] {
        runtime
            .upsert_thread_spawn_edge(parent_thread_id, child_thread_id, status)
            .await
            .expect("thread spawn edge should persist");
    }

    let closed = runtime
        .close_open_thread_spawn_subtree(parent_thread_id, target_thread_id)
        .await
        .expect("open subtree closure should succeed")
        .expect("open target should be owned");
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: vec![
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: open_child_thread_id,
                    depth: 1,
                },
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: target_thread_id,
                    depth: 0,
                },
            ],
            newly_closed_edge_count: 2,
        }
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                promoted_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("promoted subtree should remain open"),
        vec![promoted_child_thread_id]
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                target_thread_id,
                DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("promoted boundary should remain closed"),
        vec![promoted_thread_id]
    );
}

#[tokio::test]
async fn ancestor_close_returns_existing_permanently_closed_descendants_without_downgrade() {
    let runtime = runtime().await;
    let root_thread_id = ThreadId::new();
    let target_thread_id = ThreadId::new();
    let child_thread_id = ThreadId::new();
    runtime
        .upsert_thread_spawn_edge(
            root_thread_id,
            target_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("target edge");
    runtime
        .upsert_thread_spawn_edge(
            target_thread_id,
            child_thread_id,
            DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await
        .expect("permanently closed child edge");

    let closed = runtime
        .close_open_thread_spawn_subtree(root_thread_id, target_thread_id)
        .await
        .expect("ancestor close")
        .expect("owned target");
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: vec![
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: child_thread_id,
                    depth: 1,
                },
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: target_thread_id,
                    depth: 0,
                },
            ],
            newly_closed_edge_count: 1,
        }
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                target_thread_id,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("permanent child should remain permanent"),
        vec![child_thread_id]
    );
}

#[tokio::test]
async fn incoming_edge_batch_lookup_chunks_large_current_registries() {
    let runtime = runtime().await;
    let parent = ThreadId::new();
    let mut expected = Vec::new();
    for index in 0..901 {
        let child = ThreadId::new();
        let status = match index % 3 {
            0 => DirectionalThreadSpawnEdgeStatus::Open,
            1 => DirectionalThreadSpawnEdgeStatus::Closed,
            2 => DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            _ => unreachable!(),
        };
        runtime
            .upsert_thread_spawn_edge(parent, child, status)
            .await
            .expect("insert edge");
        expected.push(DirectionalThreadSpawnEdge {
            parent_thread_id: parent,
            child_thread_id: child,
            status,
        });
    }
    expected.sort_by_key(|edge| edge.child_thread_id.to_string());
    let child_thread_ids = expected
        .iter()
        .map(|edge| edge.child_thread_id)
        .collect::<Vec<_>>();

    assert_eq!(
        runtime
            .list_thread_spawn_edges_by_child_ids(&child_thread_ids)
            .await
            .expect("batch lookup"),
        expected
    );
}

#[tokio::test]
async fn open_descendant_listing_and_close_terminate_on_corrupt_cycle() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let child = ThreadId::new();
    let grandchild = ThreadId::new();
    for (parent, descendant) in [(root, child), (child, grandchild), (grandchild, root)] {
        runtime
            .upsert_thread_spawn_edge(parent, descendant, DirectionalThreadSpawnEdgeStatus::Open)
            .await
            .expect("insert corrupt cycle edge");
    }

    assert_eq!(
        runtime
            .list_thread_spawn_descendants_with_status(
                root,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("cycle-safe descendants"),
        vec![child, grandchild]
    );
    let closed = runtime
        .close_open_thread_spawn_subtree(root, child)
        .await
        .expect("cycle-safe close")
        .expect("owned subtree");
    assert_eq!(
        closed
            .members
            .iter()
            .map(|member| member.thread_id)
            .collect::<Vec<_>>(),
        vec![grandchild, child]
    );

    let reopen_err = runtime
        .upsert_thread_spawn_edge(root, child, DirectionalThreadSpawnEdgeStatus::Open)
        .await
        .expect_err("restart or ownership transfer must not reopen a permanently closed edge");
    assert!(reopen_err.to_string().contains("permanently closed"));
}

#[tokio::test]
async fn ordinary_completion_cannot_downgrade_permanent_close() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let child = ThreadId::new();
    runtime
        .upsert_thread_spawn_edge(root, child, DirectionalThreadSpawnEdgeStatus::Open)
        .await
        .expect("insert edge");
    runtime
        .close_open_thread_spawn_subtree(root, child)
        .await
        .expect("close subtree")
        .expect("owned subtree");

    assert!(
        !runtime
            .transition_open_thread_spawn_edge_to_closed(child)
            .await
            .expect("conditional completion")
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                root,
                DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
            )
            .await
            .expect("permanent children"),
        vec![child]
    );
}

#[tokio::test]
async fn permanently_closed_ancestor_cannot_spawn_a_new_open_child() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let closed_ancestor = ThreadId::new();
    let interrupted_open_parent = ThreadId::new();
    let rejected_child = ThreadId::new();
    runtime
        .upsert_thread_spawn_edge(
            root,
            closed_ancestor,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("insert ancestor edge");
    runtime
        .upsert_thread_spawn_edge(
            closed_ancestor,
            interrupted_open_parent,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("insert interrupted descendant edge");
    runtime
        .set_thread_spawn_edge_status(
            closed_ancestor,
            DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await
        .expect("interrupt close after closing the ancestor");

    let error = runtime
        .upsert_thread_spawn_edge(
            interrupted_open_parent,
            rejected_child,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect_err("descendant of permanently closed ancestor must not own new work");

    assert!(error.to_string().contains("permanently closed"));
    assert_eq!(
        runtime
            .list_thread_spawn_edges_by_child_ids(&[rejected_child])
            .await
            .expect("rejected child lookup"),
        Vec::new()
    );
}
