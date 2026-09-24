//! Per-class invalidation events (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, EH-400).
//!
//! The [`super::DepClock`] already learns, for every committed write, exactly which classes
//! (labels) and edge types it touched, and it floors on every write it could NOT attribute.
//! This module keeps that same decision as a bounded, cursor-addressable record so an
//! out-of-process cache (AU's semantic / context-bundle caches, EH-401) can invalidate by class
//! instead of guessing a TTL.
//!
//! Soundness mirrors the clock's: every version bump the clock observes is either attributed
//! ([`InvalidationScope::Classes`], naming the classes and edge types it touched) or coarse
//! ([`InvalidationScope::All`], "drop everything you hold for this graph"). A reader that fell
//! behind the ring gets `gap = true` and must treat it as `All`; a whole-image replacement bumps
//! the `epoch`, which a reader must treat the same way.

use std::collections::{BTreeSet, VecDeque};

use parking_lot::Mutex;

/// How many invalidation records a graph retains before the oldest is dropped (a reader behind
/// the ring sees `gap = true`). Bounded so an unread feed cannot grow without limit.
pub const INVALIDATION_LOG_CAP: usize = 4096;

/// Upper bound on the records one [`InvalidationLog::read_after`] call returns.
pub const INVALIDATION_PAGE_MAX: usize = 1024;

/// What a committed write invalidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationScope {
    /// Only entries depending on the named classes / edge types.
    Classes,
    /// Every cached entry for this graph (an un-attributable write).
    All,
}

/// One committed write's invalidation decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidationRecord {
    /// The committed graph version the write produced.
    pub version: u64,
    pub scope: InvalidationScope,
    /// Classes (node labels) whose members were added, removed or changed. Sorted, unique.
    pub classes: Vec<String>,
    /// Edge relationship types added or removed. Sorted, unique.
    pub edge_types: Vec<String>,
}

impl InvalidationRecord {
    /// A class-scoped record over the given labels and edge types (deduplicated and sorted, so
    /// the feed is deterministic whatever order the write reported them in).
    pub fn classes(version: u64, labels: &[String], edge_types: &[String]) -> Self {
        Self {
            version,
            scope: InvalidationScope::Classes,
            classes: sorted_unique(labels),
            edge_types: sorted_unique(edge_types),
        }
    }

    /// A coarse record: every entry for the graph is invalid at `version`.
    pub fn all(version: u64) -> Self {
        Self {
            version,
            scope: InvalidationScope::All,
            classes: Vec::new(),
            edge_types: Vec::new(),
        }
    }
}

fn sorted_unique(items: &[String]) -> Vec<String> {
    items
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// One page of the feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidationPage {
    /// Records with `version > after_version`, oldest first.
    pub records: Vec<InvalidationRecord>,
    /// `true` when records the reader has not seen were dropped from the ring: the reader must
    /// invalidate everything and resume from `head_version`.
    pub gap: bool,
    /// The newest version recorded (the cursor a caught-up reader resumes from).
    pub head_version: u64,
    /// Incremented whenever the graph's whole image is replaced. A changed epoch means the
    /// reader's cursor is meaningless: invalidate everything and restart from `head_version`.
    pub epoch: u64,
}

#[derive(Debug, Default)]
struct LogInner {
    ring: VecDeque<InvalidationRecord>,
    /// Highest version dropped from the ring (a reader at or below it has missed records).
    dropped_through: u64,
    head_version: u64,
    epoch: u64,
}

/// The bounded per-graph invalidation feed. Written by the clock (one record per committed,
/// touching write), read by the `FreshnessFeed` wire method.
#[derive(Debug, Default)]
pub struct InvalidationLog {
    inner: Mutex<LogInner>,
}

impl InvalidationLog {
    /// Append a record, dropping the oldest beyond [`INVALIDATION_LOG_CAP`].
    pub fn record(&self, record: InvalidationRecord) {
        let mut inner = self.inner.lock();
        inner.head_version = inner.head_version.max(record.version);
        inner.ring.push_back(record);
        while inner.ring.len() > INVALIDATION_LOG_CAP {
            if let Some(dropped) = inner.ring.pop_front() {
                inner.dropped_through = inner.dropped_through.max(dropped.version);
            }
        }
    }

    /// Forget every record and start a new epoch — the graph's whole image was replaced, so no
    /// earlier cursor is meaningful.
    pub fn start_epoch(&self) {
        let mut inner = self.inner.lock();
        let epoch = inner.epoch + 1;
        *inner = LogInner {
            epoch,
            ..LogInner::default()
        };
    }

    /// Records with `version > after_version`, oldest first, at most `limit`
    /// (0 or anything above [`INVALIDATION_PAGE_MAX`] means [`INVALIDATION_PAGE_MAX`]). A page
    /// never splits one version's records, so a reader that resumes after the last version it
    /// received cannot skip a record that shares it.
    pub fn read_after(&self, after_version: u64, limit: usize) -> InvalidationPage {
        let cap = match limit {
            0 => INVALIDATION_PAGE_MAX,
            n => n.min(INVALIDATION_PAGE_MAX),
        };
        let inner = self.inner.lock();
        let mut pending = inner
            .ring
            .iter()
            .filter(|record| record.version > after_version)
            .peekable();
        let mut records: Vec<InvalidationRecord> = Vec::new();
        while let Some(record) = pending.next_if(|next| {
            records.len() < cap
                || records
                    .last()
                    .is_some_and(|last| last.version == next.version)
        }) {
            records.push(record.clone());
        }
        InvalidationPage {
            records,
            gap: after_version < inner.dropped_through,
            head_version: inner.head_version,
            epoch: inner.epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn records_are_deduplicated_sorted_and_paged_after_the_cursor() {
        let log = InvalidationLog::default();
        log.record(InvalidationRecord::classes(
            3,
            &labels(&["B", "A", "B"]),
            &labels(&["knows"]),
        ));
        log.record(InvalidationRecord::all(4));
        let page = log.read_after(3, 0);
        assert_eq!(page.records, vec![InvalidationRecord::all(4)]);
        assert!(!page.gap);
        assert_eq!(page.head_version, 4);
        let first = &log.read_after(0, 1).records[0];
        assert_eq!(first.classes, labels(&["A", "B"]));
        assert_eq!(first.edge_types, labels(&["knows"]));
    }

    #[test]
    fn a_reader_behind_the_ring_sees_a_gap() {
        let log = InvalidationLog::default();
        for version in 1..=(INVALIDATION_LOG_CAP as u64 + 2) {
            log.record(InvalidationRecord::all(version));
        }
        assert!(
            log.read_after(0, 0).gap,
            "dropped records must surface as a gap"
        );
        assert!(
            !log.read_after(2, 0).gap,
            "a reader past the dropped prefix has no gap"
        );
    }

    #[test]
    fn a_page_never_splits_one_version() {
        let log = InvalidationLog::default();
        log.record(InvalidationRecord::classes(5, &labels(&["A"]), &[]));
        log.record(InvalidationRecord::all(5));
        log.record(InvalidationRecord::all(6));
        let page = log.read_after(0, 1);
        assert_eq!(
            page.records.len(),
            2,
            "both version-5 records travel together"
        );
    }

    #[test]
    fn a_replaced_image_starts_a_new_epoch() {
        let log = InvalidationLog::default();
        log.record(InvalidationRecord::all(9));
        log.start_epoch();
        let page = log.read_after(0, 0);
        assert_eq!(page.epoch, 1);
        assert!(page.records.is_empty());
        assert_eq!(page.head_version, 0);
    }
}
