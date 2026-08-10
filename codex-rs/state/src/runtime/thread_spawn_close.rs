use super::*;

impl StateRuntime {
    /// Close one persisted ownership subtree and return its members deepest-first.
    ///
    /// Open edges are upgraded to PermanentlyClosed. Existing PermanentlyClosed descendants are
    /// returned for cleanup retry without changing their status. Ordinary Closed edges remain
    /// ownership boundaries.
    pub async fn close_open_thread_spawn_subtree(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        self.close_open_thread_spawn_subtree_with_current_only_descendants(
            owner_root_thread_id,
            target_thread_id,
            Vec::new(),
        )
        .await
    }

    /// Close one persisted ownership subtree after materializing current-only descendants.
    ///
    /// `current_only_descendant_edges` is a parent-before-child forest proven from the current
    /// registry while the lifecycle-mutation fence is held. This transaction revalidates that every
    /// parent belongs to the selected Open/PermanentlyClosed subtree, inserts each missing edge as
    /// Open, accepts an already-persisted edge only when its parent and Open status match, and
    /// permanently closes the complete subtree atomically. Ordinary Closed edges remain ownership
    /// boundaries.
    pub async fn close_open_thread_spawn_subtree_with_current_only_descendants(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
        current_only_descendant_edges: Vec<crate::CurrentOnlyThreadSpawnEdge>,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        let owner_root_thread_id = owner_root_thread_id.to_string();
        let target_thread_id = target_thread_id.to_string();
        let open = crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref();
        let permanently_closed =
            crate::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed.as_ref();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let target_is_open_owned = sqlx::query_scalar::<_, i64>(
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
    FROM thread_spawn_edges
    WHERE child_thread_id = ?
      AND status = ?
      AND child_thread_id IN (SELECT child_thread_id FROM open_owned)
)
            "#,
        )
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&target_thread_id)
        .bind(open)
        .fetch_one(transaction.as_mut())
        .await?;
        if target_is_open_owned == 0 {
            transaction.rollback().await?;
            return Ok(None);
        }

        let mut inserted_children = std::collections::HashSet::new();
        for edge in &current_only_descendant_edges {
            let parent_thread_id = edge.parent_thread_id.to_string();
            let child_thread_id = edge.child_thread_id.to_string();
            if child_thread_id == owner_root_thread_id
                || child_thread_id == target_thread_id
                || edge.parent_thread_id == edge.child_thread_id
                || !inserted_children.insert(edge.child_thread_id)
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
WITH RECURSIVE owned_descendants(child_thread_id) AS (
    SELECT child_thread_id
    FROM thread_spawn_edges
    WHERE parent_thread_id = ? AND child_thread_id != ? AND status = ?
    UNION
    SELECT edge.child_thread_id
    FROM thread_spawn_edges AS edge
    JOIN owned_descendants ON edge.parent_thread_id = owned_descendants.child_thread_id
    WHERE edge.child_thread_id != ? AND edge.status = ?
),
subtree(child_thread_id, depth, visited) AS (
    SELECT child_thread_id, 0, '|' || child_thread_id || '|'
    FROM thread_spawn_edges
    WHERE child_thread_id = ?
      AND status = ?
      AND child_thread_id IN (SELECT child_thread_id FROM owned_descendants)
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
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&target_thread_id)
        .bind(open)
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
WITH RECURSIVE owned_descendants(child_thread_id) AS (
    SELECT child_thread_id
    FROM thread_spawn_edges
    WHERE parent_thread_id = ? AND child_thread_id != ? AND status = ?
    UNION
    SELECT edge.child_thread_id
    FROM thread_spawn_edges AS edge
    JOIN owned_descendants ON edge.parent_thread_id = owned_descendants.child_thread_id
    WHERE edge.child_thread_id != ? AND edge.status = ?
),
subtree(child_thread_id, visited) AS (
    SELECT child_thread_id, '|' || child_thread_id || '|'
    FROM thread_spawn_edges
    WHERE child_thread_id = ?
      AND status = ?
      AND child_thread_id IN (SELECT child_thread_id FROM owned_descendants)
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
WHERE status = ?
  AND child_thread_id IN (SELECT child_thread_id FROM subtree)
            "#,
        )
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&target_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(permanently_closed)
        .bind(permanently_closed)
        .bind(open)
        .execute(transaction.as_mut())
        .await?;
        let members = member_rows
            .into_iter()
            .map(|row| {
                let depth = row.try_get::<i64, _>("depth")?;
                Ok(crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: ThreadId::try_from(row.try_get::<String, _>("child_thread_id")?)?,
                    depth: u32::try_from(depth)?,
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

    /// Persist a registered current-only target's missing edge and close its owned subtree.
    ///
    /// Core supplies the canonical parents only after proving current registry ownership under the
    /// lifecycle-mutation fence. This transaction inserts missing ownership edges, accepts an
    /// already-persisted edge only when its parent and Open status match, and upgrades the target
    /// and its Open descendants without crossing ordinary Closed ownership boundaries.
    pub async fn close_current_only_thread_spawn_subtree(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
        current_only_ownership_edges: Vec<crate::CurrentOnlyThreadSpawnEdge>,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        let owner_root_thread_id = owner_root_thread_id.to_string();
        let target_thread_id = target_thread_id.to_string();
        let open = crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref();
        let permanently_closed =
            crate::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed.as_ref();
        let Some(first_edge) = current_only_ownership_edges.first() else {
            return Ok(None);
        };
        let Some(target_edge_index) = current_only_ownership_edges
            .iter()
            .position(|edge| edge.child_thread_id.to_string() == target_thread_id)
        else {
            return Ok(None);
        };
        if current_only_ownership_edges[target_edge_index + 1..]
            .iter()
            .any(|edge| edge.child_thread_id.to_string() == target_thread_id)
            || current_only_ownership_edges[..=target_edge_index]
                .windows(2)
                .any(|edges| edges[0].child_thread_id != edges[1].parent_thread_id)
        {
            return Ok(None);
        }
        let mut visited = std::collections::HashSet::new();
        if current_only_ownership_edges.iter().any(|edge| {
            edge.child_thread_id.to_string() == owner_root_thread_id
                || edge.parent_thread_id == edge.child_thread_id
                || !visited.insert(edge.child_thread_id)
        }) {
            return Ok(None);
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let first_parent_thread_id = first_edge.parent_thread_id.to_string();
        let anchored = sqlx::query_scalar::<_, i64>(
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
SELECT CASE
    WHEN ? = ? OR ? IN (SELECT child_thread_id FROM open_owned) THEN 1
    ELSE 0
END
            "#,
        )
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&first_parent_thread_id)
        .bind(&owner_root_thread_id)
        .bind(&first_parent_thread_id)
        .fetch_one(transaction.as_mut())
        .await?;
        if anchored == 0 {
            transaction.rollback().await?;
            return Ok(None);
        }
        for (index, edge) in current_only_ownership_edges.iter().enumerate() {
            let child_thread_id = edge.child_thread_id.to_string();
            if index > target_edge_index {
                let parent_thread_id = edge.parent_thread_id.to_string();
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
            .bind(edge.parent_thread_id.to_string())
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
                    existing_parent != edge.parent_thread_id.to_string() || existing_status != open
                }) {
                    transaction.rollback().await?;
                    return Ok(None);
                }
            }
        }

        let member_rows = sqlx::query(
            r#"
WITH RECURSIVE subtree(child_thread_id, depth, visited) AS (
    SELECT ?, 0, '|' || ? || '|'
    UNION ALL
    SELECT
        edge.child_thread_id,
        subtree.depth + 1,
        subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.child_thread_id != ?
      AND edge.status IN (?, ?)
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
SELECT child_thread_id, depth
FROM subtree
ORDER BY depth DESC, child_thread_id ASC
            "#,
        )
        .bind(&target_thread_id)
        .bind(&target_thread_id)
        .bind(&owner_root_thread_id)
        .bind(&first_parent_thread_id)
        .bind(open)
        .bind(permanently_closed)
        .fetch_all(transaction.as_mut())
        .await?;
        let update_result = sqlx::query(
            r#"
WITH RECURSIVE subtree(child_thread_id, visited) AS (
    SELECT ?, '|' || ? || '|'
    UNION ALL
    SELECT edge.child_thread_id, subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.child_thread_id != ?
      AND edge.status IN (?, ?)
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
UPDATE thread_spawn_edges
SET status = ?
WHERE status = ?
  AND child_thread_id IN (SELECT child_thread_id FROM subtree)
            "#,
        )
        .bind(&target_thread_id)
        .bind(&target_thread_id)
        .bind(&owner_root_thread_id)
        .bind(&first_parent_thread_id)
        .bind(open)
        .bind(permanently_closed)
        .bind(permanently_closed)
        .bind(open)
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

    /// Return one previously permanently closed subtree for idempotent cleanup retry.
    ///
    /// The target must still be reached from `owner_root_thread_id` through Open ancestors. Only
    /// PermanentlyClosed descendants are traversed; ordinary Closed promotion boundaries and Open
    /// descendants are excluded.
    pub async fn get_permanently_closed_thread_spawn_subtree(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        let owner_root_thread_id = owner_root_thread_id.to_string();
        let target_thread_id = target_thread_id.to_string();
        let open = crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref();
        let permanently_closed =
            crate::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed.as_ref();
        let rows = sqlx::query(
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
),
subtree(child_thread_id, depth, visited) AS (
    SELECT edge.child_thread_id, 0, '|' || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    WHERE edge.child_thread_id = ?
      AND edge.status = ?
      AND (edge.parent_thread_id = ? OR edge.parent_thread_id IN (SELECT child_thread_id FROM open_owned))
    UNION ALL
    SELECT
        edge.child_thread_id,
        subtree.depth + 1,
        subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.status = ?
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
SELECT child_thread_id, depth
FROM subtree
ORDER BY depth DESC, child_thread_id ASC
            "#,
        )
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&target_thread_id)
        .bind(permanently_closed)
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(permanently_closed)
        .fetch_all(self.pool.as_ref())
        .await?;
        let members = rows
            .into_iter()
            .map(|row| {
                Ok(crate::ClosedThreadSpawnSubtreeMember {
                    thread_id: ThreadId::try_from(row.try_get::<String, _>("child_thread_id")?)?,
                    depth: u32::try_from(row.try_get::<i64, _>("depth")?)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if members.is_empty() {
            return Ok(None);
        }
        Ok(Some(crate::ClosedThreadSpawnSubtree {
            members,
            newly_closed_edge_count: 0,
        }))
    }

    /// Change one Open edge to ordinary Closed without downgrading permanent closure.
    pub async fn transition_open_thread_spawn_edge_to_closed(
        &self,
        child_thread_id: ThreadId,
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            "UPDATE thread_spawn_edges SET status = ? WHERE child_thread_id = ? AND status = ?",
        )
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Closed.as_ref())
        .bind(child_thread_id.to_string())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Upgrade a legacy target-only close and its Open descendants to permanent closure.
    ///
    /// The expected parent is supplied only after core verifies the stored SessionSource still
    /// identifies the target as that parent's subagent. Other Closed edges remain boundaries.
    pub async fn repair_legacy_closed_thread_spawn_subtree(
        &self,
        owner_root_thread_id: ThreadId,
        target_thread_id: ThreadId,
        expected_parent_thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ClosedThreadSpawnSubtree>> {
        let owner_root_thread_id = owner_root_thread_id.to_string();
        let target_thread_id = target_thread_id.to_string();
        let expected_parent_thread_id = expected_parent_thread_id.to_string();
        let open = crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref();
        let closed = crate::DirectionalThreadSpawnEdgeStatus::Closed.as_ref();
        let permanently_closed =
            crate::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed.as_ref();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let member_rows = sqlx::query(
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
),
subtree(child_thread_id, depth, visited) AS (
    SELECT edge.child_thread_id, 0, '|' || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    WHERE edge.child_thread_id = ?
      AND edge.parent_thread_id = ?
      AND edge.status = ?
      AND (edge.parent_thread_id = ? OR edge.parent_thread_id IN (SELECT child_thread_id FROM open_owned))
    UNION ALL
    SELECT
        edge.child_thread_id,
        subtree.depth + 1,
        subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.status = ?
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
SELECT child_thread_id, depth
FROM subtree
ORDER BY depth DESC, child_thread_id ASC
            "#,
        )
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(&target_thread_id)
        .bind(&expected_parent_thread_id)
        .bind(closed)
        .bind(&owner_root_thread_id)
        .bind(&owner_root_thread_id)
        .bind(open)
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
    WHERE edge.child_thread_id = ? AND edge.parent_thread_id = ? AND edge.status = ?
    UNION ALL
    SELECT edge.child_thread_id, subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.status = ?
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
UPDATE thread_spawn_edges
SET status = ?
WHERE child_thread_id IN (SELECT child_thread_id FROM subtree)
  AND ((child_thread_id = ? AND status = ?) OR (child_thread_id != ? AND status = ?))
            "#,
        )
        .bind(&target_thread_id)
        .bind(&expected_parent_thread_id)
        .bind(closed)
        .bind(&owner_root_thread_id)
        .bind(open)
        .bind(permanently_closed)
        .bind(&target_thread_id)
        .bind(closed)
        .bind(&target_thread_id)
        .bind(open)
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
