use super::*;

enum ExistingCloseTarget {
    PermanentlyClosed,
    LegacyClosed { expected_parent_thread_id: ThreadId },
}

impl StateRuntime {
    /// Extend one durable close retry with current-only descendants.
    ///
    /// The target must already be PermanentlyClosed and owned through Open ancestors. Missing
    /// current-only edges and every existing Open descendant are upgraded atomically so a later
    /// retry observes the complete cleanup set.
    pub async fn extend_permanently_closed_thread_spawn_subtree_with_current_only_descendants(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
        current_only_descendant_edges: Vec<crate::CurrentOnlyThreadSpawnEdge>,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        self.close_existing_thread_spawn_subtree_with_current_only_descendants(
            owner_root_thread_id,
            target_thread_id,
            ExistingCloseTarget::PermanentlyClosed,
            current_only_descendant_edges,
        )
        .await
    }

    /// Upgrade a legacy ordinary-Closed target and materialize current-only descendants.
    ///
    /// `expected_parent_thread_id` is revalidated with the target edge inside the same transaction
    /// that inserts missing ownership and upgrades the selected subtree to PermanentlyClosed.
    pub async fn repair_legacy_closed_thread_spawn_subtree_with_current_only_descendants(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
        expected_parent_thread_id: ThreadId,
        current_only_descendant_edges: Vec<crate::CurrentOnlyThreadSpawnEdge>,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        self.close_existing_thread_spawn_subtree_with_current_only_descendants(
            owner_root_thread_id,
            target_thread_id,
            ExistingCloseTarget::LegacyClosed {
                expected_parent_thread_id,
            },
            current_only_descendant_edges,
        )
        .await
    }

    async fn close_existing_thread_spawn_subtree_with_current_only_descendants(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
        target: ExistingCloseTarget,
        current_only_descendant_edges: Vec<crate::CurrentOnlyThreadSpawnEdge>,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        let owner_root_thread_id = owner_root_thread_id.to_string();
        let target_thread_id = target_thread_id.to_string();
        let open = crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref();
        let closed = crate::DirectionalThreadSpawnEdgeStatus::Closed.as_ref();
        let permanently_closed =
            crate::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed.as_ref();
        let (target_status, expected_parent_thread_id) = match target {
            ExistingCloseTarget::PermanentlyClosed => (permanently_closed, String::new()),
            ExistingCloseTarget::LegacyClosed {
                expected_parent_thread_id,
            } => (closed, expected_parent_thread_id.to_string()),
        };
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let target_is_owned = sqlx::query_scalar::<_, i64>(
            r#"
WITH RECURSIVE open_owned(child_thread_id) AS (
    SELECT child_thread_id
    FROM thread_spawn_edges
    WHERE parent_thread_id = ? AND child_thread_id != ? AND status = ?
    UNION
    SELECT edge.child_thread_id
    FROM thread_spawn_edges AS edge
    JOIN open_owned ON edge.parent_thread_id = open_owned.child_thread_id
    WHERE edge.child_thread_id != ? AND edge.status = ?
)
SELECT EXISTS(
    SELECT 1
    FROM thread_spawn_edges AS edge
    WHERE edge.child_thread_id = ?
      AND edge.status = ?
      AND (? = '' OR edge.parent_thread_id = ?)
      AND (
          edge.parent_thread_id = ?
          OR edge.parent_thread_id IN (SELECT child_thread_id FROM open_owned)
      )
)
            "#,
        )
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&target_thread_id)
        .bind(target_status)
        .bind(&expected_parent_thread_id)
        .bind(&expected_parent_thread_id)
        .bind(&owner_root_thread_id)
        .fetch_one(transaction.as_mut())
        .await?;
        if target_is_owned == 0 {
            transaction.rollback().await?;
            return Ok(None);
        }

        let mut selected_children = std::collections::HashSet::new();
        for edge in &current_only_descendant_edges {
            let parent_thread_id = edge.parent_thread_id.to_string();
            let child_thread_id = edge.child_thread_id.to_string();
            if child_thread_id == owner_root_thread_id
                || child_thread_id == target_thread_id
                || edge.parent_thread_id == edge.child_thread_id
                || !selected_children.insert(edge.child_thread_id)
            {
                transaction.rollback().await?;
                return Ok(None);
            }
            let parent_is_selected = sqlx::query_scalar::<_, i64>(
                r#"
WITH RECURSIVE subtree(child_thread_id, visited) AS (
    SELECT ?, '|' || ? || '|'
    UNION ALL
    SELECT edge.child_thread_id, subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.status IN (?, ?)
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
SELECT EXISTS(SELECT 1 FROM subtree WHERE child_thread_id = ?)
                "#,
            )
            .bind(&target_thread_id)
            .bind(&target_thread_id)
            .bind(&owner_root_thread_id)
            .bind(open)
            .bind(permanently_closed)
            .bind(&parent_thread_id)
            .fetch_one(transaction.as_mut())
            .await?;
            if parent_is_selected == 0 {
                transaction.rollback().await?;
                return Ok(None);
            }
            let insert_result = sqlx::query(
                r#"
INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status)
SELECT ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM thread_spawn_edges WHERE child_thread_id = ?
)
                "#,
            )
            .bind(&parent_thread_id)
            .bind(&child_thread_id)
            .bind(open)
            .bind(&child_thread_id)
            .execute(transaction.as_mut())
            .await?;
            if insert_result.rows_affected() == 0 {
                let existing_edge = sqlx::query_as::<_, (String, String)>(
                    r#"
SELECT parent_thread_id, status
FROM thread_spawn_edges
WHERE child_thread_id = ?
                    "#,
                )
                .bind(&child_thread_id)
                .fetch_optional(transaction.as_mut())
                .await?;
                if existing_edge.is_none_or(|(existing_parent, existing_status)| {
                    existing_parent != parent_thread_id || existing_status != open
                }) {
                    transaction.rollback().await?;
                    return Ok(None);
                }
            }
        }

        let member_rows = sqlx::query(
            r#"
WITH RECURSIVE subtree(child_thread_id, depth, visited) AS (
    SELECT edge.child_thread_id, 0, '|' || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    WHERE edge.child_thread_id = ? AND edge.status = ?
    UNION ALL
    SELECT
        edge.child_thread_id,
        subtree.depth + 1,
        subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.status IN (?, ?)
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
SELECT child_thread_id, depth
FROM subtree
ORDER BY depth DESC, child_thread_id ASC
            "#,
        )
        .bind(&target_thread_id)
        .bind(target_status)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(permanently_closed)
        .fetch_all(transaction.as_mut())
        .await?;
        if member_rows.is_empty() {
            transaction.rollback().await?;
            return Ok(None);
        }
        let update_result = sqlx::query(
            r#"
WITH RECURSIVE subtree(child_thread_id, visited) AS (
    SELECT edge.child_thread_id, '|' || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    WHERE edge.child_thread_id = ? AND edge.status = ?
    UNION ALL
    SELECT edge.child_thread_id, subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.status IN (?, ?)
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
UPDATE thread_spawn_edges
SET status = ?
WHERE child_thread_id IN (SELECT child_thread_id FROM subtree)
  AND (status = ? OR (child_thread_id = ? AND status = ?))
            "#,
        )
        .bind(&target_thread_id)
        .bind(target_status)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(permanently_closed)
        .bind(permanently_closed)
        .bind(open)
        .bind(&target_thread_id)
        .bind(closed)
        .execute(transaction.as_mut())
        .await?;
        let members = member_rows
            .into_iter()
            .map(|row| {
                Ok(crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: ThreadId::try_from(row.try_get::<String, _>("child_thread_id")?)?,
                    depth: u32::try_from(row.try_get::<i64, _>("depth")?)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let newly_closed_edge_count = usize::try_from(update_result.rows_affected())?;
        transaction.commit().await?;
        Ok(Some(crate::ClosedThreadSpawnSubtree {
            members,
            newly_closed_edge_count,
        }))
    }
}
