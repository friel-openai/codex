//! Writes each original review transcript input once and assigns finite checkpoint endpoints.

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ReviewInputRecord;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::Serialize;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use super::lineage::LegacyLineageMigrationPlan;
use super::migration_error;
use super::rollback_plan::RollbackPlan;
use crate::ThreadStoreResult;

/// Review records are separate from `RolloutItem`, so ordinary history readers cannot expose them.
#[derive(Serialize)]
struct ReviewInputLine<'a> {
    ordinal: u64,
    record: &'a ReviewInputRecord,
}

pub(super) async fn write_review_input<W: AsyncWrite + Unpin>(
    plan: &LegacyLineageMigrationPlan,
    rollback_plan: &mut RollbackPlan,
    writer: &mut W,
) -> ThreadStoreResult<u64> {
    let target = plan
        .review_input_target
        .as_ref()
        .ok_or_else(|| migration_error("review input has no journaled target"))?;
    let modern = rollback_plan
        .modern_rollbacks_mut()
        .ok_or_else(|| migration_error("review input has no rollback snapshots"))?;
    let timestamp = plan.sources[0].timestamp.clone();
    let header = RolloutLine {
        timestamp: timestamp.clone(),
        ordinal: Some(0),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                id: target.thread_id,
                session_id: target.thread_id.into(),
                segment_id: target.segment_id,
                timestamp,
                history_mode: ThreadHistoryMode::Paginated,
                ..Default::default()
            },
            git: None,
        }),
    };
    let mut offset = write_record(writer, &header).await?;
    let mut ordinal = 1_u64;
    if let Some(position) = modern.review_input_base {
        // Flatten the old finite prefix once. A new snapshot never recursively copies another
        // checkpoint, and appends beyond the old endpoint cannot enter this migration.
        for record in codex_rollout::load_review_input_prefix(&plan.codex_home, position)
            .await
            .map_err(migration_error)?
        {
            offset = offset
                .checked_add(
                    write_record(
                        writer,
                        &ReviewInputLine {
                            ordinal,
                            record: &record,
                        },
                    )
                    .await?,
                )
                .ok_or_else(|| migration_error("review input byte offset overflow"))?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| migration_error("review input ordinal overflow"))?;
        }
    }
    let base_ordinal = ordinal;
    let mut endpoints = Vec::with_capacity(modern.review_inputs.len() + 1);
    endpoints.push(offset);
    for record in &modern.review_inputs {
        offset = offset
            .checked_add(write_record(writer, &ReviewInputLine { ordinal, record }).await?)
            .ok_or_else(|| migration_error("review input byte offset overflow"))?;
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| migration_error("review input ordinal overflow"))?;
        endpoints.push(offset);
    }
    for snapshot in modern.snapshots.values_mut() {
        let Some(end) = snapshot.review_input_end else {
            continue;
        };
        let end_byte_offset = *endpoints
            .get(end)
            .ok_or_else(|| migration_error("review snapshot exceeds captured input"))?;
        let end_ordinal_exclusive = base_ordinal
            .checked_add(end as u64)
            .ok_or_else(|| migration_error("review snapshot ordinal overflow"))?;
        snapshot.retained_context_replay.review_input = Some(HistoryPosition {
            thread_id: target.rollout_id,
            end_ordinal_exclusive,
            end_byte_offset,
        });
        snapshot.retained_context_replay.resolved_review_input = None;
    }
    Ok(ordinal)
}

async fn write_record<W: AsyncWrite + Unpin>(
    writer: &mut W,
    record: &impl Serialize,
) -> ThreadStoreResult<u64> {
    let mut bytes = serde_json::to_vec(record).map_err(migration_error)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await.map_err(migration_error)?;
    Ok(bytes.len() as u64)
}
