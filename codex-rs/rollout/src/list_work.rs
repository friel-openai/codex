//! Test-only operation counts for filesystem thread listing.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadListWork {
    pub session_meta_records: usize,
    pub full_head_summaries: usize,
    pub referenced_files: usize,
    pub compatibility_searches: usize,
    pub compatibility_directory_entries: usize,
}

#[derive(Default)]
struct ThreadListWorkRecorder {
    session_meta_records: AtomicUsize,
    full_head_summaries: AtomicUsize,
    referenced_files: AtomicUsize,
    compatibility_searches: AtomicUsize,
    compatibility_directory_entries: AtomicUsize,
}

impl ThreadListWorkRecorder {
    fn snapshot(&self) -> ThreadListWork {
        ThreadListWork {
            session_meta_records: self.session_meta_records.load(Ordering::Relaxed),
            full_head_summaries: self.full_head_summaries.load(Ordering::Relaxed),
            referenced_files: self.referenced_files.load(Ordering::Relaxed),
            compatibility_searches: self.compatibility_searches.load(Ordering::Relaxed),
            compatibility_directory_entries: self
                .compatibility_directory_entries
                .load(Ordering::Relaxed),
        }
    }
}

tokio::task_local! {
    static THREAD_LIST_WORK: Arc<ThreadListWorkRecorder>;
}

pub(crate) async fn record_thread_list_work<F>(future: F) -> (F::Output, ThreadListWork)
where
    F: Future,
{
    let recorder = Arc::new(ThreadListWorkRecorder::default());
    let output = THREAD_LIST_WORK.scope(Arc::clone(&recorder), future).await;
    (output, recorder.snapshot())
}

pub(crate) fn record_session_meta() {
    let _ = THREAD_LIST_WORK.try_with(|work| {
        work.session_meta_records.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn record_full_head_summary() {
    let _ = THREAD_LIST_WORK.try_with(|work| {
        work.full_head_summaries.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn record_referenced_file() {
    let _ = THREAD_LIST_WORK.try_with(|work| {
        work.referenced_files.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn record_compatibility_search() {
    let _ = THREAD_LIST_WORK.try_with(|work| {
        work.compatibility_searches.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn record_compatibility_directory_entry() {
    let _ = THREAD_LIST_WORK.try_with(|work| {
        work.compatibility_directory_entries
            .fetch_add(1, Ordering::Relaxed);
    });
}
