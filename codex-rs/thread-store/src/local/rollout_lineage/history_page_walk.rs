//! Resolves native history-base segments only as a descending page needs them.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;

use super::super::thread_history::ProjectionLeafIdentity;
use super::super::thread_history::projection_leaf_identity;
use super::super::thread_history::read_deferred_compacted_head;
use super::GoalSupervisorLineageProvenance;
use super::LineageOffsetMode;
use super::LineageResolutionMode;
use super::LocalThreadStore;
use super::RolloutLineage;
use super::RolloutLineageSegment;
use super::malformed_lineage;
use super::read_rollout_head;
use super::resolve_indexed_rollout_path_by_id;
use super::rollout_path_is_compressed;
use super::thread_rollout_resolver;
use super::trim_to_history_position;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// A reference or attached history needs the full compatibility resolver.
pub(in crate::local) enum WalkStep {
    Segment,
    End,
    FullResolution,
}

/// The selected root and the native history-base segments already opened, newest first.
pub(in crate::local) struct HistoryPageWalk<'a> {
    store: &'a LocalThreadStore,
    requested_thread_id: ThreadId,
    root_rollout_id: RolloutId,
    root_path: PathBuf,
    next: Option<(RolloutId, Option<PathBuf>, Option<HistoryPosition>)>,
    paths: HashSet<PathBuf>,
    path_index: Option<HashMap<ThreadId, PathBuf>>,
    prefetched_paths: HashMap<ThreadId, PathBuf>,
    fork_depth: usize,
    segments: Vec<RolloutLineageSegment>,
    deferred_identities: Vec<ProjectionLeafIdentity>,
}

impl<'a> HistoryPageWalk<'a> {
    pub(in crate::local) async fn new(
        store: &'a LocalThreadStore,
        requested_thread_id: ThreadId,
    ) -> ThreadStoreResult<Self> {
        let resolved =
            thread_rollout_resolver::resolve_current_including_archived(store, requested_thread_id)
                .await?
                .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
        Ok(Self {
            store,
            requested_thread_id,
            root_rollout_id: resolved.rollout_id,
            root_path: resolved.path.clone(),
            next: Some((resolved.rollout_id, Some(resolved.path), None)),
            paths: HashSet::new(),
            path_index: None,
            prefetched_paths: HashMap::new(),
            fork_depth: 0,
            segments: Vec::new(),
            deferred_identities: Vec::new(),
        })
    }

    pub(in crate::local) fn root_rollout_id(&self) -> RolloutId {
        self.root_rollout_id
    }

    pub(in crate::local) fn root_path(&self) -> &std::path::Path {
        &self.root_path
    }

    pub(in crate::local) async fn selected_is_current(&self) -> ThreadStoreResult<bool> {
        Ok(thread_rollout_resolver::resolve_current_including_archived(
            self.store,
            self.requested_thread_id,
        )
        .await?
        .is_some_and(|selected| {
            selected.rollout_id == self.root_rollout_id && selected.path == self.root_path
        }))
    }

    pub(in crate::local) fn current(&self) -> &RolloutLineageSegment {
        self.segments.last().expect("a segment was just opened")
    }

    pub(in crate::local) fn complete(&self) -> bool {
        self.next.is_none()
    }

    /// The history-base cutoff recorded by the most recently opened segment.
    pub(in crate::local) fn next_history_position(&self) -> Option<HistoryPosition> {
        self.next.as_ref().and_then(|(_, _, end)| *end)
    }

    pub(in crate::local) fn snapshot(&self) -> RolloutLineage {
        let mut segments = self.segments.clone();
        segments.reverse();
        RolloutLineage::from_segments(
            self.requested_thread_id,
            self.root_rollout_id,
            self.root_path.clone(),
            segments,
        )
    }

    pub(in crate::local) fn into_lineage(mut self) -> RolloutLineage {
        self.segments.reverse();
        RolloutLineage::from_segments(
            self.requested_thread_id,
            self.root_rollout_id,
            self.root_path,
            self.segments,
        )
    }

    /// Discards the partial walk and resolves the same selected root with compatibility rules.
    pub(in crate::local) async fn resolve_full(self) -> ThreadStoreResult<RolloutLineage> {
        if !self.selected_is_current().await? {
            return Err(ThreadStoreError::Conflict {
                message: "selected rollout changed while resolving history".to_string(),
            });
        }
        self.store
            .resolve_rollout_lineage_from_path_with_mode(
                self.requested_thread_id,
                self.root_path,
                LineageResolutionMode::HistoryPage,
                None,
                &[],
                LineageOffsetMode::Resolve,
            )
            .await
    }

    pub(in crate::local) async fn next_older(&mut self) -> ThreadStoreResult<WalkStep> {
        self.next_older_with_mode(LineageOffsetMode::Resolve).await
    }

    /// The caller must authenticate every deferred boundary before using the walk.
    pub(in crate::local) async fn next_older_deferred(&mut self) -> ThreadStoreResult<WalkStep> {
        self.next_older_with_mode(LineageOffsetMode::Deferred).await
    }

    pub(in crate::local) fn deferred_identities(&self) -> &[ProjectionLeafIdentity] {
        &self.deferred_identities
    }

    async fn next_older_with_mode(
        &mut self,
        offset_mode: LineageOffsetMode,
    ) -> ThreadStoreResult<WalkStep> {
        let Some((rollout_id, explicit_path, end)) = self.next.take() else {
            return Ok(WalkStep::End);
        };
        let path = if let Some(path) = explicit_path {
            path
        } else {
            if let [child] = self.segments.as_slice() {
                self.prefetched_paths = codex_rollout::prefetch_sibling_rollout_paths(
                    &self.store.config.codex_home,
                    &child.rollout_path,
                    rollout_id,
                )
                .await
                .unwrap_or_default();
            }
            match resolve_indexed_rollout_path_by_id(
                self.store,
                rollout_id,
                self.prefetched_paths.remove(&rollout_id),
                &mut self.path_index,
            )
            .await?
            {
                Some(path) => path,
                None => return Ok(WalkStep::FullResolution),
            }
        };
        if codex_rollout::rollout_id_from_path(codex_rollout::plain_rollout_path(&path).as_path())
            != Some(rollout_id)
            || !self.paths.insert(path.clone())
        {
            return Ok(WalkStep::FullResolution);
        }
        let identity = if offset_mode == LineageOffsetMode::Deferred {
            let Some(identity) = projection_leaf_identity(&path).await else {
                return Ok(WalkStep::FullResolution);
            };
            Some(identity)
        } else {
            None
        };
        // Deferred callers validate the entire selected predecessor before using this head.
        let deferred_head = if offset_mode == LineageOffsetMode::Deferred && end.is_some() {
            read_deferred_compacted_head(&path).await?
        } else {
            None
        };
        let head = match deferred_head {
            Some(head) => head,
            None => match read_rollout_head(&path).await {
                Ok(head) => head,
                Err(_) => return Ok(WalkStep::FullResolution),
            },
        };
        let meta = &head.session_meta.meta;
        if meta.history_mode != ThreadHistoryMode::Paginated
            || meta.attached_thread
            || meta.subagent_history_start_ordinal.is_some()
            || head.leading_reference.is_some()
            || (self.segments.is_empty() && meta.id != self.requested_thread_id)
        {
            return Ok(WalkStep::FullResolution);
        }
        if let Some(child) = self.segments.last()
            && child.thread_id != meta.id
        {
            if self.fork_depth >= codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH {
                return Ok(WalkStep::FullResolution);
            }
            self.fork_depth += 1;
        }
        if let Some(base) = meta.history_base
            && base.end_ordinal_exclusive > head.first_local_ordinal
        {
            // The compatibility resolver handles truncation and overlapping ranges.
            return Ok(WalkStep::FullResolution);
        }
        let size = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(_) => return Ok(WalkStep::FullResolution),
        };
        let mut segment = RolloutLineageSegment {
            thread_id: meta.id,
            rollout_id,
            segment_id: meta.segment_id,
            rollout_path: path.clone(),
            start_ordinal: head.first_local_ordinal,
            physical_start_ordinal: head.first_local_ordinal,
            history_page_visible: true,
            history_page_start: None,
            end_ordinal_exclusive: None,
            jsonl_end_byte_offset: (!rollout_path_is_compressed(&path)).then_some(size),
            end_byte_offset: Some(size),
            byte_boundary_recorded: false,
            physical_size: Some(size),
            filter_texts: Vec::new(),
            goal_supervisor_provenance: GoalSupervisorLineageProvenance::Untrusted,
            uses_history_base: meta.history_base.is_some(),
            uses_fork_boundary: false,
        };
        if let Some(end) = end {
            if end.end_ordinal_exclusive < segment.start_ordinal {
                return Ok(WalkStep::FullResolution);
            }
            let mut one = vec![segment];
            if trim_to_history_position(&mut one, end, offset_mode)
                .await
                .is_err()
            {
                // The full resolver can retry an authenticated alternate physical copy.
                return Ok(WalkStep::FullResolution);
            }
            segment = one.pop().expect("the cutoff retains its source segment");
        }
        self.next = meta
            .history_base
            .map(|base| (base.thread_id, None, Some(base)));
        self.segments.push(segment);
        if let Some(identity) = identity {
            self.deferred_identities.push(identity);
        }
        Ok(WalkStep::Segment)
    }
}
