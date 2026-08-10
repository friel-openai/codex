use super::*;

const THREAD_SPAWN_EDGE_QUERY_CHUNK_SIZE: usize = 900;

impl StateRuntime {
    /// Return existing incoming edges for a bounded batch of child thread IDs.
    pub async fn list_thread_spawn_edges_by_child_ids(
        &self,
        child_thread_ids: &[ThreadId],
    ) -> anyhow::Result<Vec<crate::DirectionalThreadSpawnEdge>> {
        let mut edges = Vec::new();
        for child_thread_ids in child_thread_ids.chunks(THREAD_SPAWN_EDGE_QUERY_CHUNK_SIZE) {
            let mut query = QueryBuilder::<Sqlite>::new(
                "SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges WHERE child_thread_id IN (",
            );
            let mut separated = query.separated(", ");
            for child_thread_id in child_thread_ids {
                separated.push_bind(child_thread_id.to_string());
            }
            separated.push_unseparated(") ORDER BY child_thread_id");
            let rows = query.build().fetch_all(self.pool.as_ref()).await?;
            for row in rows {
                edges.push(crate::DirectionalThreadSpawnEdge {
                    parent_thread_id: ThreadId::try_from(
                        row.try_get::<String, _>("parent_thread_id")?,
                    )?,
                    child_thread_id: ThreadId::try_from(
                        row.try_get::<String, _>("child_thread_id")?,
                    )?,
                    status: row
                        .try_get::<String, _>("status")?
                        .parse::<crate::DirectionalThreadSpawnEdgeStatus>()?,
                });
            }
        }
        edges.sort_by_key(|edge| edge.child_thread_id.to_string());
        Ok(edges)
    }

    /// List descendants reachable through Open or PermanentlyClosed edges.
    pub async fn list_permanent_close_thread_spawn_descendants(
        &self,
        target_thread_id: ThreadId,
    ) -> anyhow::Result<Vec<ThreadId>> {
        let rows = sqlx::query(
            r#"
WITH RECURSIVE subtree(child_thread_id, visited) AS (
    SELECT child_thread_id, '|' || child_thread_id || '|'
    FROM thread_spawn_edges
    WHERE parent_thread_id = ?
      AND child_thread_id != ?
      AND status IN (?, ?)
    UNION ALL
    SELECT edge.child_thread_id, subtree.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id
    WHERE edge.child_thread_id != ?
      AND edge.status IN (?, ?)
      AND instr(subtree.visited, '|' || edge.child_thread_id || '|') = 0
)
SELECT DISTINCT child_thread_id
FROM subtree
ORDER BY child_thread_id
            "#,
        )
        .bind(target_thread_id.to_string())
        .bind(target_thread_id.to_string())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed.as_ref())
        .bind(target_thread_id.to_string())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::PermanentlyClosed.as_ref())
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ThreadId::try_from(
                    row.try_get::<String, _>("child_thread_id")?,
                )?)
            })
            .collect()
    }

    /// Find one descendant by id when every ownership edge from `root_thread_id` is open.
    pub async fn find_open_thread_spawn_descendant_by_id(
        &self,
        root_thread_id: ThreadId,
        descendant_thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadSpawnDescendantIdentity>> {
        let row = sqlx::query(
            r#"
WITH RECURSIVE open_ancestry(child_thread_id, parent_thread_id, depth, visited) AS (
    SELECT child_thread_id, parent_thread_id, 1, '|' || child_thread_id || '|'
    FROM thread_spawn_edges
    WHERE child_thread_id = ? AND status = ? AND child_thread_id != ?
    UNION ALL
    SELECT
        edge.child_thread_id,
        edge.parent_thread_id,
        open_ancestry.depth + 1,
        open_ancestry.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN open_ancestry ON edge.child_thread_id = open_ancestry.parent_thread_id
    WHERE edge.status = ?
      AND open_ancestry.parent_thread_id != ?
      AND instr(open_ancestry.visited, '|' || edge.child_thread_id || '|') = 0
),
authorized(depth) AS (
    SELECT depth
    FROM open_ancestry
    WHERE parent_thread_id = ?
    LIMIT 1
)
SELECT
    target_edge.child_thread_id,
    target_edge.parent_thread_id,
    authorized.depth,
    threads.source,
    threads.agent_path,
    threads.agent_role,
    threads.agent_nickname
FROM authorized
JOIN thread_spawn_edges AS target_edge ON target_edge.child_thread_id = ?
LEFT JOIN threads ON threads.id = target_edge.child_thread_id
            "#,
        )
        .bind(descendant_thread_id.to_string())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref())
        .bind(root_thread_id.to_string())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref())
        .bind(root_thread_id.to_string())
        .bind(root_thread_id.to_string())
        .bind(descendant_thread_id.to_string())
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.as_ref()
            .map(thread_spawn_descendant_identity_from_row)
            .transpose()
    }

    /// Find one descendant by canonical path when every ownership edge from the root is open.
    pub async fn find_open_thread_spawn_descendant_by_path(
        &self,
        root_thread_id: ThreadId,
        agent_path: &str,
    ) -> anyhow::Result<Option<crate::ThreadSpawnDescendantIdentity>> {
        let rows = sqlx::query(
            r#"
WITH RECURSIVE matching_thread_ids(id) AS MATERIALIZED (
    SELECT id
    FROM threads INDEXED BY idx_threads_agent_path
    WHERE agent_path = ?
),
open_ancestry(target_thread_id, child_thread_id, parent_thread_id, depth, visited) AS (
    SELECT
        matching_thread_ids.id,
        edge.child_thread_id,
        edge.parent_thread_id,
        1,
        '|' || edge.child_thread_id || '|'
    FROM matching_thread_ids
    JOIN thread_spawn_edges AS edge
        ON edge.child_thread_id = matching_thread_ids.id
    WHERE edge.status = ? AND edge.child_thread_id != ?
    UNION ALL
    SELECT
        open_ancestry.target_thread_id,
        edge.child_thread_id,
        edge.parent_thread_id,
        open_ancestry.depth + 1,
        open_ancestry.visited || edge.child_thread_id || '|'
    FROM thread_spawn_edges AS edge
    JOIN open_ancestry ON edge.child_thread_id = open_ancestry.parent_thread_id
    WHERE edge.status = ?
      AND open_ancestry.parent_thread_id != ?
      AND instr(open_ancestry.visited, '|' || edge.child_thread_id || '|') = 0
),
authorized(target_thread_id, depth) AS (
    SELECT target_thread_id, depth
    FROM open_ancestry
    WHERE parent_thread_id = ?
)
SELECT
    target_edge.child_thread_id,
    target_edge.parent_thread_id,
    authorized.depth,
    threads.source,
    threads.agent_path,
    threads.agent_role,
    threads.agent_nickname
FROM authorized
JOIN thread_spawn_edges AS target_edge ON target_edge.child_thread_id = authorized.target_thread_id
JOIN threads ON threads.id = target_edge.child_thread_id
ORDER BY target_edge.child_thread_id
LIMIT 2
            "#,
        )
        .bind(agent_path)
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref())
        .bind(root_thread_id.to_string())
        .bind(crate::DirectionalThreadSpawnEdgeStatus::Open.as_ref())
        .bind(root_thread_id.to_string())
        .bind(root_thread_id.to_string())
        .fetch_all(self.pool.as_ref())
        .await?;

        match rows.as_slice() {
            [] => Ok(None),
            [row] => thread_spawn_descendant_identity_from_row(row).map(Some),
            [_, _, ..] => Err(anyhow::anyhow!(
                "multiple agents found for canonical path `{agent_path}`"
            )),
        }
    }
}

fn thread_spawn_descendant_identity_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> anyhow::Result<crate::ThreadSpawnDescendantIdentity> {
    Ok(crate::ThreadSpawnDescendantIdentity {
        thread_id: ThreadId::try_from(row.try_get::<String, _>("child_thread_id")?)?,
        parent_thread_id: ThreadId::try_from(row.try_get::<String, _>("parent_thread_id")?)?,
        depth: u32::try_from(row.try_get::<i64, _>("depth")?)?,
        source: row.try_get("source")?,
        agent_path: row.try_get("agent_path")?,
        agent_role: row.try_get("agent_role")?,
        agent_nickname: row.try_get("agent_nickname")?,
    })
}
