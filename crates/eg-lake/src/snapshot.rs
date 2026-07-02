//! LSN-style as-of snapshots (CONCEPT:EG-317).
//!
//! An external lakehouse reader needs a CONSISTENT point-in-time view: the exact set
//! of Parquet files that were valid as of some engine version. The engine already has
//! this — versioned snapshots + the bi-temporal `Op::AsOf` (CONCEPT:KG-2.249/2.250).
//! This module is the interop seam that projects that internal version onto a single
//! monotonic **LSN** (log sequence number) and pins the file set valid as of it, so a
//! Delta/Iceberg snapshot the reader opens is reproducible and never sees a half-
//! materialized commit.
//!
//! The LSN maps 1:1 to the engine's WAL sequence / snapshot version; here it is an
//! opaque `u64` that only ever increases. The engine-side seam stamps each
//! materialization with the WAL LSN it drained up to (a documented follow-up wiring);
//! this crate owns the durable projection and the as-of query over it.

use serde::{Deserialize, Serialize};

/// A monotonic log sequence number identifying an engine version (CONCEPT:EG-317).
/// Reuses the engine's versioned-snapshot / WAL sequence concept (KG-2.249/2.250);
/// opaque and strictly increasing.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn value(self) -> u64 {
        self.0
    }
}

/// One Parquet data file and the LSN range over which it is live (CONCEPT:EG-317).
/// `added_at` is the LSN whose commit introduced the file; `removed_at` is the LSN
/// whose commit tombstoned it (a rewrite/compaction/delete), or `None` while live. An
/// as-of query at LSN `q` includes the file iff `added_at <= q < removed_at`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileEntry {
    /// Object-store-relative path of the Parquet file.
    pub path: String,
    pub size_bytes: u64,
    pub num_rows: u64,
    pub added_at: Lsn,
    pub removed_at: Option<Lsn>,
}

impl FileEntry {
    /// Whether this file is visible in a consistent read as of `lsn` (CONCEPT:EG-317).
    pub fn visible_at(&self, lsn: Lsn) -> bool {
        self.added_at <= lsn && self.removed_at.map(|r| lsn < r).unwrap_or(true)
    }
}

/// The durable, append-only projection of every materialized file and the LSN it was
/// committed at — the interop seam over the engine's versioned snapshots
/// (CONCEPT:EG-317). An as-of read reconstructs the exact live file set for any LSN
/// `<= current_lsn`, giving an external reader a reproducible point-in-time view.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLog {
    files: Vec<FileEntry>,
    current: Lsn,
}

impl SnapshotLog {
    pub fn new() -> Self {
        SnapshotLog::default()
    }

    /// The current (latest committed) LSN — what an external reader gets when it asks
    /// for "now" (CONCEPT:EG-317).
    pub fn current_lsn(&self) -> Lsn {
        self.current
    }

    /// Record a newly-materialized Parquet file committed at `lsn`, advancing the
    /// current LSN (CONCEPT:EG-317). Panics-free: a non-monotonic `lsn` is clamped so
    /// `current` never regresses.
    pub fn add_file(&mut self, path: impl Into<String>, size_bytes: u64, num_rows: u64, lsn: Lsn) {
        self.files.push(FileEntry {
            path: path.into(),
            size_bytes,
            num_rows,
            added_at: lsn,
            removed_at: None,
        });
        if lsn > self.current {
            self.current = lsn;
        }
    }

    /// Tombstone a live file as of `lsn` (a rewrite/compaction), advancing the LSN
    /// (CONCEPT:EG-317).
    pub fn remove_file(&mut self, path: &str, lsn: Lsn) {
        for f in self.files.iter_mut() {
            if f.path == path && f.removed_at.is_none() {
                f.removed_at = Some(lsn);
            }
        }
        if lsn > self.current {
            self.current = lsn;
        }
    }

    /// The set of Parquet files valid as of `lsn` — the consistent as-of view
    /// (CONCEPT:EG-317). Returns `path`/`size`/`rows` triples an external engine reads.
    pub fn files_as_of(&self, lsn: Lsn) -> Vec<&FileEntry> {
        self.files.iter().filter(|f| f.visible_at(lsn)).collect()
    }

    /// The live file set at the current LSN (CONCEPT:EG-317).
    pub fn live_files(&self) -> Vec<&FileEntry> {
        self.files_as_of(self.current)
    }

    /// Every recorded file entry (live + tombstoned) — the full log, e.g. for building
    /// a Delta commit history (CONCEPT:EG-317).
    pub fn all_files(&self) -> &[FileEntry] {
        &self.files
    }
}
