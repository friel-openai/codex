use super::*;
use crate::MAX_QUEUE_ITEMS;
use crate::QueuedUserSubmissionRecord;
use uuid::Uuid;

/// SQLite-backed persistence for durable, thread-scoped user messages.
#[derive(Clone)]
pub struct SqliteQueueStore {
    pool: Arc<SqlitePool>,
}

impl SqliteQueueStore {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }

    pub async fn enqueue(
        &self,
        thread_id: ThreadId,
        payload_json: &str,
    ) -> anyhow::Result<QueuedUserSubmissionRecord> {
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let row = sqlx::query(
            "INSERT INTO queued_items (
                id, thread_id, payload_json, queue_order,
                created_at_ms, updated_at_ms
             )
             SELECT ?, ?, ?,
                    COALESCE((SELECT MAX(queue_order) FROM queued_items WHERE thread_id = ?), -1) + 1,
                    ?, ?
             WHERE (SELECT COUNT(*) FROM queued_items WHERE thread_id = ?) < ?
             RETURNING id, thread_id, payload_json",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(thread_id.to_string())
        .bind(payload_json)
        .bind(thread_id.to_string())
        .bind(now_ms)
        .bind(now_ms)
        .bind(thread_id.to_string())
        .bind(i64::try_from(MAX_QUEUE_ITEMS)?)
        .fetch_one(self.pool.as_ref())
        .await?;
        QueuedUserSubmissionRecord::try_from_row(&row)
    }

    pub async fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<QueuedUserSubmissionRecord>> {
        let rows = sqlx::query(
            "SELECT id, thread_id, payload_json
             FROM queued_items
             WHERE thread_id = ?
             ORDER BY queue_order LIMIT ? OFFSET ?",
        )
        .bind(thread_id.to_string())
        .bind(i64::try_from(limit)?)
        .bind(i64::try_from(offset)?)
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.iter()
            .map(QueuedUserSubmissionRecord::try_from_row)
            .collect()
    }

    pub async fn update(
        &self,
        thread_id: ThreadId,
        item_id: &str,
        payload_json: &str,
    ) -> anyhow::Result<Option<QueuedUserSubmissionRecord>> {
        let row = sqlx::query(
            "UPDATE queued_items
             SET payload_json = ?, updated_at_ms = ?
             WHERE thread_id = ? AND id = ?
             RETURNING id, thread_id, payload_json",
        )
        .bind(payload_json)
        .bind(datetime_to_epoch_millis(Utc::now()))
        .bind(thread_id.to_string())
        .bind(item_id)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.as_ref()
            .map(QueuedUserSubmissionRecord::try_from_row)
            .transpose()
    }

    pub async fn delete(&self, thread_id: ThreadId, item_id: &str) -> anyhow::Result<bool> {
        Ok(
            sqlx::query("DELETE FROM queued_items WHERE thread_id = ? AND id = ?")
                .bind(thread_id.to_string())
                .bind(item_id)
                .execute(self.pool.as_ref())
                .await?
                .rows_affected()
                > 0,
        )
    }

    pub async fn reorder(&self, thread_id: ThreadId, ordered_ids: &[String]) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT id, queue_order FROM queued_items
             WHERE thread_id = ? ORDER BY queue_order",
        )
        .bind(thread_id.to_string())
        .fetch_all(transaction.as_mut())
        .await?;
        let mut expected_ids = rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
        let mut requested_ids = ordered_ids.to_vec();
        expected_ids.sort();
        requested_ids.sort();
        if expected_ids != requested_ids {
            transaction.rollback().await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "queue reorder must include every queued item exactly once",
            )
            .into());
        }
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let max_queue_order = rows.last().map_or(-1, |(_, queue_order)| *queue_order);
        for (index, item_id) in ordered_ids.iter().enumerate() {
            sqlx::query(
                "UPDATE queued_items SET queue_order = ?, updated_at_ms = ?
                 WHERE thread_id = ? AND id = ?",
            )
            .bind(max_queue_order + i64::try_from(index)? + 1)
            .bind(now_ms)
            .bind(thread_id.to_string())
            .bind(item_id)
            .execute(transaction.as_mut())
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Delete every durable queued submission for one thread.
    pub async fn delete_thread_queue(&self, thread_id: ThreadId) -> anyhow::Result<bool> {
        Ok(sqlx::query("DELETE FROM queued_items WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .execute(self.pool.as_ref())
            .await?
            .rows_affected()
            > 0)
    }

    /// Delete durable queued submissions for a set of closed threads.
    ///
    /// The input is deduplicated and split into bounded SQL statements inside one transaction.
    /// The return value counts distinct thread queues that contained at least one item.
    pub async fn delete_thread_queues(&self, thread_ids: &[ThreadId]) -> anyhow::Result<usize> {
        let thread_ids = thread_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if thread_ids.is_empty() {
            return Ok(0);
        }

        let mut transaction = self.pool.begin().await?;
        let mut deleted_thread_ids = BTreeSet::new();
        for chunk in thread_ids.chunks(THREAD_CLEANUP_BATCH_SIZE) {
            let mut delete =
                QueryBuilder::<Sqlite>::new("DELETE FROM queued_items WHERE thread_id IN (");
            let mut separated = delete.separated(", ");
            for thread_id in chunk {
                separated.push_bind(thread_id);
            }
            separated.push_unseparated(") RETURNING thread_id");
            deleted_thread_ids.extend(
                delete
                    .build_query_scalar::<String>()
                    .fetch_all(transaction.as_mut())
                    .await?,
            );
        }
        transaction.commit().await?;
        Ok(deleted_thread_ids.len())
    }
}

#[cfg(test)]
#[path = "queued_items_tests.rs"]
mod tests;
