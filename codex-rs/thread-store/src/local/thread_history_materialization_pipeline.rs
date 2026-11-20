//! Pipes bounded rollout batches into short SQLite transactions, committing each checkpoint.

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;

use super::super::LocalThreadStore;
use super::super::rollout_lineage::AncestorReads;
use super::super::thread_history::RolloutProjectionStep;
use super::super::thread_history::apply_projection;
use super::super::thread_history::apply_projection_with_leaf_proof;
use super::ProjectionRollout;
use super::read_projection_batches;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

const QUEUED_BATCHES: usize = 2;

pub(super) enum ProjectionMessage {
    Batch {
        projections: Vec<RolloutProjectionStep>,
        next_offset: u64,
        source_sha256: Option<[u8; 32]>,
    },
    Complete,
    Failed(ThreadStoreError),
}

pub(super) async fn read_and_apply(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout: ProjectionRollout,
    start_offset: u64,
    expected_ordinal: u64,
    initial_ordinal: u64,
    subagent_history_start_ordinal: Option<u64>,
    end: Option<HistoryPosition>,
) -> ThreadStoreResult<()> {
    // Two queued batches, one being produced and one being consumed. Each is limited by
    // source bytes and step count; an oversized individual record remains indivisible.
    let reads = rollout.reads.clone();
    let participating_leaf = rollout
        .participating_leaf
        .clone()
        .map(|identity| (rollout.path.clone(), identity));
    let (sender, receiver) = tokio::sync::mpsc::channel(QUEUED_BATCHES);
    let (stop, keep_running) = tokio::sync::oneshot::channel::<()>();
    let span = tracing::Span::current();
    let worker = tokio::task::spawn_blocking(move || {
        let _entered = span.enter();
        let read = read_projection_batches(
            &rollout,
            start_offset,
            expected_ordinal,
            thread_id,
            subagent_history_start_ordinal,
            end,
            || stop.is_closed(),
            |projections, next_offset, source_sha256| {
                sender
                    .blocking_send(ProjectionMessage::Batch {
                        projections,
                        next_offset,
                        source_sha256,
                    })
                    .map_err(|_| ThreadStoreError::Internal {
                        message: "projection consumer stopped".to_string(),
                    })
            },
        );
        let completion = match read {
            Ok(()) => ProjectionMessage::Complete,
            Err(error) => ProjectionMessage::Failed(error),
        };
        let _ = sender.blocking_send(completion);
    });
    let result = apply_batches(
        store,
        thread_id,
        start_offset,
        initial_ordinal,
        receiver,
        &reads,
        participating_leaf
            .as_ref()
            .map(|(path, identity)| (path.as_path(), identity)),
    )
    .await;
    // apply_batches drops the receiver on error, unblocking any pending producer send.
    drop(keep_running);
    worker.await.map_err(|err| ThreadStoreError::Internal {
        message: format!("projection producer failed: {err}"),
    })?;
    result
}

pub(super) async fn apply_batches(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    mut start_offset: u64,
    initial_ordinal: u64,
    mut receiver: tokio::sync::mpsc::Receiver<ProjectionMessage>,
    reads: &AncestorReads,
    participating_leaf: Option<(
        &std::path::Path,
        &super::super::thread_history::ProjectionLeafIdentity,
    )>,
) -> ThreadStoreResult<()> {
    // Receive before opening the transaction so a slow reader cannot hold the SQLite writer.
    while let Some(message) = receiver.recv().await {
        match message {
            ProjectionMessage::Batch {
                projections,
                next_offset,
                source_sha256,
            } => {
                // A failed or cancelled commit can have an ambiguous outcome. Do not retry a
                // different copy after attempting a batch transaction.
                reads.prevent_retry();
                if let (Some((path, identity)), Some(source_sha256)) =
                    (participating_leaf, source_sha256)
                {
                    apply_projection_with_leaf_proof(
                        store,
                        thread_id,
                        start_offset,
                        next_offset,
                        initial_ordinal,
                        projections,
                        path,
                        identity,
                        source_sha256,
                    )
                    .await?;
                } else {
                    apply_projection(
                        store,
                        thread_id,
                        start_offset,
                        next_offset,
                        initial_ordinal,
                        projections,
                    )
                    .await?;
                }
                start_offset = next_offset;
            }
            ProjectionMessage::Complete => return Ok(()),
            ProjectionMessage::Failed(error) => return Err(error),
        }
    }
    Err(ThreadStoreError::Internal {
        message: "projection producer stopped before successful completion".to_string(),
    })
}
