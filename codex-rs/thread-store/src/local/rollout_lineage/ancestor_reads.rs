//! Retries a failed ancestor read without changing its immutable segment identity.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;

use super::LocalThreadStore;
use super::RolloutLineage;
use super::RolloutLineageSegment;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[derive(Clone, Debug, Default)]
pub(in crate::local) struct AncestorReads(Option<Arc<TrackedAncestors>>);

#[derive(Debug)]
struct TrackedAncestors {
    entries: HashMap<PathBuf, TrackedAncestor>,
    signals: Arc<RetrySignals>,
}

#[derive(Debug, Default)]
struct RetrySignals {
    failed_path: OnceLock<PathBuf>,
    projection_may_have_advanced: OnceLock<()>,
}

#[derive(Debug)]
struct TrackedAncestor {
    reference: RolloutReferenceItem,
    cutoff: Option<HistoryPosition>,
}

#[derive(Clone, Debug)]
pub(in crate::local) struct ExcludedAncestor {
    pub(super) reference: RolloutReferenceItem,
    pub(in crate::local) path: PathBuf,
    pub(super) cutoff: Option<HistoryPosition>,
    error: ThreadStoreError,
}

fn same_reference(a: &RolloutReferenceItem, b: &RolloutReferenceItem) -> bool {
    a.thread_id == b.thread_id
        && a.rollout_id == b.rollout_id
        && a.segment_id == b.segment_id
        && (a.segment_id.is_some()
            || codex_rollout::plain_rollout_path(&a.rollout_path).file_name()
                == codex_rollout::plain_rollout_path(&b.rollout_path).file_name())
}

impl ExcludedAncestor {
    pub(super) fn new(reference: RolloutReferenceItem, path: PathBuf, cutoff: Option<HistoryPosition>, error: ThreadStoreError) -> Self {
        Self { reference, path, cutoff, error }
    }

    pub(super) fn original_error(&self) -> ThreadStoreError { self.error.clone() }

    pub(super) fn path_for<'a>(excluded: &'a [Self], reference: &RolloutReferenceItem) -> Option<&'a Path> {
        excluded.iter().find(|item| same_reference(&item.reference, reference)).map(|item| item.path.as_path())
    }

    pub(super) fn matches_history_position(&self, end: HistoryPosition) -> bool {
        self.cutoff.is_some_and(|cutoff| cutoff.thread_id == end.thread_id
            && cutoff.end_ordinal_exclusive == end.end_ordinal_exclusive
            && cutoff.end_byte_offset == end.end_byte_offset)
    }
}

impl AncestorReads {
    pub(super) fn references(&self) -> HashMap<PathBuf, RolloutReferenceItem> {
        self.0.as_ref().map(|ancestors| ancestors.entries.iter().map(|(path, entry)| (path.clone(), entry.reference.clone())).collect()).unwrap_or_default()
    }

    pub(super) fn rebound(&self, segments: &[RolloutLineageSegment]) -> Self {
        let references = self.0.as_ref().map(|ancestors| ancestors.entries.iter().map(|(path, entry)| (path.clone(), entry.reference.clone())).collect()).unwrap_or_default();
        Self::with_signals(segments, references, self.0.as_ref().map(|ancestors| Arc::clone(&ancestors.signals)).unwrap_or_default())
    }

    pub(super) fn new(segments: &[RolloutLineageSegment], references: HashMap<PathBuf, RolloutReferenceItem>) -> Self {
        Self::with_signals(segments, references, Arc::default())
    }

    fn with_signals(segments: &[RolloutLineageSegment], references: HashMap<PathBuf, RolloutReferenceItem>, signals: Arc<RetrySignals>) -> Self {
        let entries = segments.iter().filter_map(|segment| {
            let reference = references.get(&segment.rollout_path)?.clone();
            let cutoff = segment.byte_boundary_recorded.then_some(segment.end_ordinal_exclusive.zip(segment.jsonl_end_byte_offset)).flatten().map(|(end_ordinal_exclusive, end_byte_offset)| HistoryPosition {
                thread_id: segment.rollout_id, end_ordinal_exclusive, end_byte_offset,
            });
            Some((segment.rollout_path.clone(), TrackedAncestor { reference, cutoff }))
        }).collect::<HashMap<_, _>>();
        Self((!entries.is_empty()).then(|| Arc::new(TrackedAncestors { entries, signals })))
    }

    pub(in crate::local) fn cutoff(&self, path: &Path) -> Option<HistoryPosition> {
        self.0.as_ref()?.entries.get(path)?.cutoff
    }

    pub(super) fn reference_for(&self, path: &Path) -> Option<&RolloutReferenceItem> {
        Some(&self.0.as_ref()?.entries.get(path)?.reference)
    }

    pub(in crate::local) fn tracks(&self, path: &Path) -> bool {
        self.0.as_ref().is_some_and(|ancestors| ancestors.entries.contains_key(path))
    }

    pub(in crate::local) fn has_tracked(&self) -> bool {
        self.0.is_some()
    }

    pub(in crate::local) fn record_failure(&self, path: &Path) {
        if let Some(ancestors) = &self.0 && ancestors.entries.contains_key(path) {
            ancestors.signals.failed_path.get_or_init(|| path.to_owned());
        }
    }

    pub(in crate::local) fn prevent_retry(&self) {
        if let Some(ancestors) = &self.0 {
            ancestors.signals.projection_may_have_advanced.get_or_init(|| ());
        }
    }

    /// Validate only recorded byte cutoffs; compressed ordinal-only bounds are checked by readers.
    pub(in crate::local) fn open<F: Fn() -> bool>(&self, path: &Path, is_cancelled: F) -> io::Result<impl Read + Seek> {
        codex_rollout::open_rollout_seekable_reader_with_exact_path(path, is_cancelled, self.tracks(path)).and_then(|mut file| {
            if let Some(end) = self.cutoff(path) && file.seek(SeekFrom::End(0))? < end.end_byte_offset {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "cutoff byte offset is past the source rollout"));
            }
            file.rewind()?;
            Ok(file)
        }).map_err(|err| {
            self.record_failure(path);
            io::Error::new(err.kind(), format!("{}: {err}", path.display()))
        })
    }
}

impl RolloutLineage {
    pub(in crate::local) fn excluded_ancestors(&self) -> Vec<ExcludedAncestor> {
        self.excluded.clone()
    }

    /// Returns a retry exclusion only when a tracked read failed before projection progress.
    pub(in crate::local) fn retry_exclusion(&self, error: ThreadStoreError) -> Option<ExcludedAncestor> {
        let ancestors = self.reads.0.as_ref()?;
        if ancestors.signals.projection_may_have_advanced.get().is_some() {
            return None;
        }
        let (path, tracked) = ancestors.entries.get_key_value(ancestors.signals.failed_path.get()?)?;
        if self.excluded.iter().any(|item| same_reference(&item.reference, &tracked.reference)) {
            return None;
        }
        Some(ExcludedAncestor::new(tracked.reference.clone(), path.clone(), tracked.cutoff, error))
    }

    /// A retry may change the physical copy, but not the source or selected logical ranges.
    pub(super) fn matches_retry(&self, other: &Self) -> bool {
        self.root_rollout_id == other.root_rollout_id
            && self.leaf_path == other.leaf_path
            && self.retry_source_stamp == other.retry_source_stamp
            && self.segments.len() == other.segments.len()
            && self.segments.iter().zip(&other.segments).all(|(original, current)| {
                let original_path = std::fs::canonicalize(&original.rollout_path)
                    .unwrap_or_else(|_| original.rollout_path.clone());
                let allowed_path = original.rollout_path == current.rollout_path
                    || self.reads.reference_for(&original.rollout_path).is_some_and(|reference| {
                        other.excluded.iter().any(|excluded| excluded.path == original_path
                            && same_reference(&excluded.reference, reference))
                    });
                allowed_path && original.thread_id == current.thread_id && original.rollout_id == current.rollout_id
                    && original.segment_id == current.segment_id && original.start_ordinal == current.start_ordinal
                    && original.physical_start_ordinal == current.physical_start_ordinal
                    && original.history_page_visible == current.history_page_visible
                    && original.history_page_start == current.history_page_start
                    && original.end_ordinal_exclusive == current.end_ordinal_exclusive
                    && original.byte_boundary_recorded == current.byte_boundary_recorded
                    && (!original.byte_boundary_recorded || original.jsonl_end_byte_offset == current.jsonl_end_byte_offset)
                    && (!(original.byte_boundary_recorded || original.end_ordinal_exclusive.is_none()) || original.end_byte_offset == current.end_byte_offset)
                    && (original.end_ordinal_exclusive.is_some() || original.physical_size == current.physical_size)
                    && original.filter_texts == current.filter_texts
            })
    }

    pub(in crate::local) async fn read_with_fallback<T, F>(self, store: &LocalThreadStore, read: impl Fn(Self) -> F) -> ThreadStoreResult<T>
    where F: Future<Output = ThreadStoreResult<T>> + Send {
        let requested_thread_id = self.requested_thread_id;
        let mode = self.mode;
        let leaf = self.leaf_path.clone();
        let leaf_cutoff = self.leaf_cutoff;
        let original_segments = self.segments.clone();
        let mut excluded = self.excluded.clone();
        let mut lineage = self;
        loop {
            let reads = lineage.reads.clone();
            let error = match read(lineage).await { Ok(value) => return Ok(value), Err(error) => error };
            if reads.0.as_ref().is_some_and(|ancestors| ancestors.signals.projection_may_have_advanced.get().is_some()) {
                return Err(error);
            }
            let Some((path, tracked)) = reads.0.as_ref().and_then(|ancestors| ancestors.entries.get_key_value(ancestors.signals.failed_path.get()?)) else { return Err(error) };
            if let Some(original) = excluded.iter().find(|item| same_reference(&item.reference, &tracked.reference)) {
                return Err(original.error.clone());
            }
            excluded.push(ExcludedAncestor { reference: tracked.reference.clone(), path: tokio::fs::canonicalize(path).await.unwrap_or_else(|_| path.clone()), cutoff: tracked.cutoff, error: error.clone() });
            lineage = store.resolve_rollout_lineage_from_path_with_mode(requested_thread_id, leaf.clone(), mode, leaf_cutoff, &excluded).await.map_err(|_| error.clone())?;
            if original_segments.len() != lineage.segments.len() || original_segments.iter().zip(&lineage.segments).any(|(original, current)| {
                original.thread_id != current.thread_id || original.rollout_id != current.rollout_id
                    || original.segment_id != current.segment_id || original.start_ordinal != current.start_ordinal
                    || original.physical_start_ordinal != current.physical_start_ordinal
                    || original.history_page_visible != current.history_page_visible
                    || original.history_page_start != current.history_page_start
                    || original.end_ordinal_exclusive != current.end_ordinal_exclusive
                    || original.byte_boundary_recorded != current.byte_boundary_recorded
                    || (original.byte_boundary_recorded && original.jsonl_end_byte_offset != current.jsonl_end_byte_offset)
                    || ((original.byte_boundary_recorded || original.end_ordinal_exclusive.is_none()) && original.end_byte_offset != current.end_byte_offset)
                    || (original.end_ordinal_exclusive.is_none() && original.physical_size != current.physical_size)
                    || original.filter_texts != current.filter_texts
            }) {
                return Err(error);
            }
        }
    }
}
