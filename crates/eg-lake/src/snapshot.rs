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

use crate::schema::CellValue;

/// Per-column statistics gathered over ONE materialized data file (CONCEPT:EG-350).
///
/// These are the numbers an external Iceberg reader (Spark / Trino / DuckDB) uses to
/// **skip whole data files** during predicate pushdown: `lower`/`upper` bound a
/// column's value range so a reader whose predicate falls outside the range never opens
/// the file. Computed once, as the file is materialized (the LTAP tier already walks the
/// rows to write Parquet, EG-317), and carried on the [`FileEntry`] so the Iceberg Avro
/// manifest writer (CONCEPT:EG-333) can emit the spec's `column_sizes` / `value_counts`
/// / `null_value_counts` / `nan_value_counts` / `lower_bounds` / `upper_bounds` maps.
///
/// The bounds are kept as neutral, typed [`CellValue`]s (NOT Iceberg's binary encoding)
/// so this stays a pure, dependency-free projection — the `lake`-gated manifest writer
/// owns the Iceberg single-value binary serialization. `field_id` is the Iceberg field
/// id of the column (1-based, matching the metadata schema's `id`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnStat {
    /// Iceberg field id of the column these stats describe (1-based).
    pub field_id: i32,
    /// Total number of values in the column for this file (includes nulls) — the
    /// Iceberg `value_counts` entry (= the file's row count).
    pub value_count: i64,
    /// Number of null values in the column for this file — `null_value_counts`.
    pub null_count: i64,
    /// Number of NaN values (Double columns only; 0 otherwise) — `nan_value_counts`.
    pub nan_count: i64,
    /// Total on-disk (compressed) size in bytes of the column chunk(s), read back from
    /// the Parquet file metadata — the Iceberg `column_sizes` entry. `None` when the
    /// writer could not attribute a size to the column.
    pub column_size: Option<i64>,
    /// Minimum non-null (non-NaN for Double) value — the `lower_bounds` entry. `None`
    /// when every value in the column is null.
    pub lower: Option<CellValue>,
    /// Maximum non-null (non-NaN for Double) value — the `upper_bounds` entry. `None`
    /// when every value in the column is null.
    pub upper: Option<CellValue>,
}

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
    /// Per-column statistics for predicate pushdown / file skipping (CONCEPT:EG-350).
    /// `None` for files recorded without stats (e.g. via the bare [`SnapshotLog::add_file`]
    /// seam a caller that wrote Parquet itself uses); `Some` when the `lake` materialize
    /// tier gathered them. Defaulted so older serialized logs deserialize unchanged.
    #[serde(default)]
    pub column_stats: Option<Vec<ColumnStat>>,
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
        self.add_file_with_stats(path, size_bytes, num_rows, lsn, None);
    }

    /// Record a newly-materialized Parquet file plus its per-column statistics
    /// (CONCEPT:EG-350). Identical to [`Self::add_file`] but pins the `column_stats` the
    /// `lake` materialize tier gathered so the Iceberg manifest writer can emit
    /// predicate-pushdown bounds. `None` stats are equivalent to `add_file`.
    pub fn add_file_with_stats(
        &mut self,
        path: impl Into<String>,
        size_bytes: u64,
        num_rows: u64,
        lsn: Lsn,
        column_stats: Option<Vec<ColumnStat>>,
    ) {
        self.files.push(FileEntry {
            path: path.into(),
            size_bytes,
            num_rows,
            added_at: lsn,
            removed_at: None,
            column_stats,
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
