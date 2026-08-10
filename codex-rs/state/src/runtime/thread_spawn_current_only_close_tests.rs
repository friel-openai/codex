use super::StateRuntime;
use crate::CurrentOnlyThreadSpawnEdge;
use crate::DirectionalThreadSpawnEdgeStatus;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn open_close_materializes_current_only_descendants_for_restart_retry() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let target = ThreadId::new();
    let missing_child = ThreadId::new();
    let missing_grandchild = ThreadId::new();
    let persisted_great_grandchild = ThreadId::new();
    let promoted = ThreadId::new();
    let promoted_child = ThreadId::new();
    let sibling = ThreadId::new();
    for (parent, child, status) in [
        (root, target, DirectionalThreadSpawnEdgeStatus::Open),
        (
            missing_grandchild,
            persisted_great_grandchild,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (target, promoted, DirectionalThreadSpawnEdgeStatus::Closed),
        (
            promoted,
            promoted_child,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (root, sibling, DirectionalThreadSpawnEdgeStatus::Open),
    ] {
        runtime
            .upsert_thread_spawn_edge(parent, child, status)
            .await
            .expect("seed ownership edge");
    }

    let closed = runtime
        .close_open_thread_spawn_subtree_with_current_only_descendants(
            root,
            target,
            vec![
                CurrentOnlyThreadSpawnEdge {
                    parent_thread_id: target,
                    child_thread_id: missing_child,
                },
                CurrentOnlyThreadSpawnEdge {
                    parent_thread_id: missing_child,
                    child_thread_id: missing_grandchild,
                },
                CurrentOnlyThreadSpawnEdge {
                    parent_thread_id: missing_grandchild,
                    child_thread_id: persisted_great_grandchild,
                },
            ],
        )
        .await
        .expect("atomic current-only descendant close")
        .expect("open target remains owned");
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: vec![
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: persisted_great_grandchild,
                    depth: 3,
                },
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: missing_grandchild,
                    depth: 2,
                },
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: missing_child,
                    depth: 1,
                },
                crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: target,
                    depth: 0,
                },
            ],
            newly_closed_edge_count: 4,
        }
    );
    assert_eq!(
        runtime
            .get_permanently_closed_thread_spawn_subtree(root, target)
            .await
            .expect("restart lookup")
            .expect("durable closed subtree"),
        crate::ClosedThreadSpawnSubtree {
            members: closed.members,
            newly_closed_edge_count: 0,
        }
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                target,
                DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("promotion boundary"),
        vec![promoted]
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                promoted,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("promoted ownership"),
        vec![promoted_child]
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(root, DirectionalThreadSpawnEdgeStatus::Open)
            .await
            .expect("unrelated sibling"),
        vec![sibling]
    );
    assert!(
        runtime
            .upsert_thread_spawn_edge(root, missing_child, DirectionalThreadSpawnEdgeStatus::Open,)
            .await
            .expect_err("ownership transfer must not reopen a durable close")
            .to_string()
            .contains("permanently closed")
    );
}

#[tokio::test]
async fn open_close_rejects_invalid_current_only_edges_without_mutation() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let target = ThreadId::new();
    let inserted_before_failure = ThreadId::new();
    let closed_boundary = ThreadId::new();
    runtime
        .upsert_thread_spawn_edge(root, target, DirectionalThreadSpawnEdgeStatus::Open)
        .await
        .expect("seed target ownership");
    runtime
        .upsert_thread_spawn_edge(
            target,
            closed_boundary,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("seed closed ownership boundary");

    assert_eq!(
        runtime
            .close_open_thread_spawn_subtree_with_current_only_descendants(
                root,
                target,
                vec![
                    CurrentOnlyThreadSpawnEdge {
                        parent_thread_id: target,
                        child_thread_id: inserted_before_failure,
                    },
                    CurrentOnlyThreadSpawnEdge {
                        parent_thread_id: target,
                        child_thread_id: root,
                    },
                ],
            )
            .await
            .expect("invalid forest should not fail the database"),
        None
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(root, DirectionalThreadSpawnEdgeStatus::Open,)
            .await
            .expect("target edge should remain Open"),
        vec![target]
    );
    assert!(
        runtime
            .list_thread_spawn_children_with_status(target, DirectionalThreadSpawnEdgeStatus::Open,)
            .await
            .expect("valid prefix and invalid owner edge must both roll back")
            .is_empty()
    );
    assert_eq!(
        runtime
            .close_open_thread_spawn_subtree_with_current_only_descendants(
                root,
                target,
                vec![CurrentOnlyThreadSpawnEdge {
                    parent_thread_id: target,
                    child_thread_id: closed_boundary,
                }],
            )
            .await
            .expect("closed boundary should not fail the database"),
        None
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(
                target,
                DirectionalThreadSpawnEdgeStatus::Closed,
            )
            .await
            .expect("closed boundary should remain unchanged"),
        vec![closed_boundary]
    );
    assert_eq!(
        runtime
            .list_thread_spawn_children_with_status(root, DirectionalThreadSpawnEdgeStatus::Open,)
            .await
            .expect("target edge should still remain Open"),
        vec![target]
    );
}

#[tokio::test]
async fn permanent_close_retry_materializes_current_only_descendants() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let target = ThreadId::new();
    let current_only_child = ThreadId::new();
    runtime
        .upsert_thread_spawn_edge(
            root,
            target,
            DirectionalThreadSpawnEdgeStatus::PermanentlyClosed,
        )
        .await
        .expect("seed permanently closed target");

    let closed = runtime
        .extend_permanently_closed_thread_spawn_subtree_with_current_only_descendants(
            root,
            target,
            vec![CurrentOnlyThreadSpawnEdge {
                parent_thread_id: target,
                child_thread_id: current_only_child,
            }],
        )
        .await
        .expect("extend durable close")
        .expect("target remains owned");
    let expected_members = vec![
        crate::ClosedThreadSpawnSubtreeMember {
            thread_id: current_only_child,
            depth: 1,
        },
        crate::ClosedThreadSpawnSubtreeMember {
            thread_id: target,
            depth: 0,
        },
    ];
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: expected_members.clone(),
            newly_closed_edge_count: 1,
        }
    );
    assert_eq!(
        runtime
            .get_permanently_closed_thread_spawn_subtree(root, target)
            .await
            .expect("restart lookup")
            .expect("extended durable subtree"),
        crate::ClosedThreadSpawnSubtree {
            members: expected_members,
            newly_closed_edge_count: 0,
        }
    );
}

#[tokio::test]
async fn permanent_close_retry_closes_existing_open_descendants_without_missing_edges() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let target = ThreadId::new();
    let open_child = ThreadId::new();
    for (parent, child) in [(root, target), (target, open_child)] {
        runtime
            .upsert_thread_spawn_edge(parent, child, DirectionalThreadSpawnEdgeStatus::Open)
            .await
            .expect("seed interrupted durable close");
    }
    runtime
        .set_thread_spawn_edge_status(target, DirectionalThreadSpawnEdgeStatus::PermanentlyClosed)
        .await
        .expect("interrupt durable close after closing the target");

    let closed = runtime
        .extend_permanently_closed_thread_spawn_subtree_with_current_only_descendants(
            root,
            target,
            Vec::new(),
        )
        .await
        .expect("extend durable close")
        .expect("target remains owned");
    let expected_members = vec![
        crate::ClosedThreadSpawnSubtreeMember {
            thread_id: open_child,
            depth: 1,
        },
        crate::ClosedThreadSpawnSubtreeMember {
            thread_id: target,
            depth: 0,
        },
    ];
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: expected_members.clone(),
            newly_closed_edge_count: 1,
        }
    );
    assert_eq!(
        runtime
            .get_permanently_closed_thread_spawn_subtree(root, target)
            .await
            .expect("restart lookup")
            .expect("extended durable subtree"),
        crate::ClosedThreadSpawnSubtree {
            members: expected_members,
            newly_closed_edge_count: 0,
        }
    );
}

#[tokio::test]
async fn legacy_close_repair_materializes_current_only_descendants() {
    let runtime = runtime().await;
    let root = ThreadId::new();
    let target = ThreadId::new();
    let current_only_child = ThreadId::new();
    runtime
        .upsert_thread_spawn_edge(root, target, DirectionalThreadSpawnEdgeStatus::Closed)
        .await
        .expect("seed legacy closed target");

    let closed = runtime
        .repair_legacy_closed_thread_spawn_subtree_with_current_only_descendants(
            root,
            target,
            root,
            vec![CurrentOnlyThreadSpawnEdge {
                parent_thread_id: target,
                child_thread_id: current_only_child,
            }],
        )
        .await
        .expect("repair legacy close")
        .expect("target remains owned");
    let expected_members = vec![
        crate::ClosedThreadSpawnSubtreeMember {
            thread_id: current_only_child,
            depth: 1,
        },
        crate::ClosedThreadSpawnSubtreeMember {
            thread_id: target,
            depth: 0,
        },
    ];
    assert_eq!(
        closed,
        crate::ClosedThreadSpawnSubtree {
            members: expected_members.clone(),
            newly_closed_edge_count: 2,
        }
    );
    assert_eq!(
        runtime
            .get_permanently_closed_thread_spawn_subtree(root, target)
            .await
            .expect("restart lookup")
            .expect("repaired durable subtree"),
        crate::ClosedThreadSpawnSubtree {
            members: expected_members,
            newly_closed_edge_count: 0,
        }
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
