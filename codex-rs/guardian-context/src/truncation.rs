//! Guardian's shared UTF-8-safe, prefix/suffix text truncation and observations.
//!
//! The existing XML omission marker is preserved, including returning the whole
//! marker when a token budget is too small to contain it.

pub use codex_history::truncate_text;

#[cfg(test)]
#[path = "truncation_tests.rs"]
mod tests;

/// Actual evidence reduction, reported by consumers using their existing metrics.
#[derive(Clone)]
pub struct TruncationObservation {
    pub component: &'static str,
    pub original_bytes: usize,
    pub retained_bytes: usize,
}
