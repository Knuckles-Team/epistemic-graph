//! # eg-lake — LTAP lakehouse interop for epistemic-graph (CONCEPT:EG-317)
//!
//! Makes the engine an **LTAP** (Lakehouse for Transactional + Analytical Processing)
//! superset: one open copy of a table — Parquet on object storage, described by a
//! Delta Lake / Apache Iceberg transaction log — that BOTH the engine's OLTP path and
//! external OLAP engines (Spark / Trino / DuckDB) read, with **no ETL**.
//!
//! ## The four interop seams (per CONCEPT:EG-317)
//! 1. **Parquet materialization** ([`parquet_io`], behind the `lake` feature) —
//!    transcode a [`schema::LakeBatch`] (a table / columnar segment, neutralized) into
//!    a Parquet file's bytes an external engine reads directly.
//! 2. **Table log** — a fully-written, read-consistent **Delta** `_delta_log`
//!    ([`delta`], pure JSON) AND an Iceberg `metadata.json` ([`iceberg`], real
//!    metadata + a documented Avro-manifest stub) over those Parquet files.
//! 3. **LSN as-of snapshot** ([`snapshot`]) — projects the engine's versioned
//!    snapshots / `Op::AsOf` (CONCEPT:KG-2.249/2.250) onto a monotonic LSN and pins
//!    the file set valid as of it, so an external read is a consistent point-in-time.
//! 4. **Iceberg-REST catalog** ([`catalog`]) — a discovery stub: `(namespace, table)`
//!    → `metadata.json` location, rendering the REST catalog response bodies.
//!
//! ## What is real vs. stub (per CONCEPT:EG-317)
//! * **Delta** is implemented FULLY and read-consistently (JSON log, replayable to the
//!   live file set).
//! * **Iceberg** `metadata.json` is real (format-version 2); its manifest layer is a
//!   documented JSON **stub** (the spec mandates Avro — a follow-up dep).
//! * The **catalog** is contents + response shapes, not an HTTP server.
//!
//! ## The engine-side seam (documented, intentionally NOT in this leaf)
//! `eg-lake` is a pure LEAF crate — it depends on nothing else in the workspace, and
//! the heavy arrow/parquet stack is behind `lake` and OUT of the Pi build. The
//! *wiring* — an async materialization tier that (a) drains the WAL (CONCEPT:EG-017)
//! up to an LSN, (b) converts an `eg-tsdb::ColumnarSegment` (CONCEPT:EG-089) or an
//! `eg-query` user-table row batch into a [`schema::LakeBatch`], (c) writes the
//! Parquet + logs to the blob/S3 backend, and (d) registers the table in the catalog —
//! is a documented seam the engine's server tier owns. This crate provides every piece
//! that seam needs ([`LakeTable`] is the orchestration entry point) without pulling
//! the engine into the leaf.

pub mod catalog;
pub mod delta;
pub mod iceberg;
pub mod schema;
pub mod snapshot;

#[cfg(feature = "lake")]
pub mod parquet_io;

pub use schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
pub use snapshot::{FileEntry, Lsn, SnapshotLog};

use catalog::IcebergRestCatalog;
use delta::DeltaLogFile;
use iceberg::IcebergTable;

/// A stable, deterministic UUID-shaped id derived from a table name (CONCEPT:EG-317).
/// Avoids a `uuid` dep; good enough for a stable table identity across re-exports.
fn stable_table_id(name: &str) -> String {
    // FNV-1a 64 over the name, laid out as a UUID-ish string.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let lo: u64 = h.rotate_left(17) ^ 0x9e3779b97f4a7c15;
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (h >> 32) as u32,
        (h >> 16) as u16,
        h as u16,
        (lo >> 48) as u16,
        lo & 0xffff_ffff_ffff
    )
}

/// High-level orchestration of one materialized table's lakehouse export
/// (CONCEPT:EG-317). Holds the schema, the durable [`SnapshotLog`] (the LSN → file-set
/// projection) and the table identity, and renders the Delta log, Iceberg metadata and
/// catalog entry over it. This is the entry point the engine-side seam drives.
#[derive(Clone, Debug)]
pub struct LakeTable {
    pub namespace: String,
    pub name: String,
    pub schema: LakeSchema,
    pub location: String,
    pub snapshot: SnapshotLog,
    table_id: String,
}

impl LakeTable {
    /// Create a table export rooted at `location` (its object-store prefix)
    /// (CONCEPT:EG-317).
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        schema: LakeSchema,
        location: impl Into<String>,
    ) -> Self {
        let name = name.into();
        let table_id = stable_table_id(&name);
        LakeTable {
            namespace: namespace.into(),
            name,
            schema,
            location: location.into(),
            snapshot: SnapshotLog::new(),
            table_id,
        }
    }

    /// The engine LSN of the latest committed materialization (CONCEPT:EG-317) — what
    /// an external reader is handed for a consistent "now" read.
    pub fn current_lsn(&self) -> Lsn {
        self.snapshot.current_lsn()
    }

    /// Record a Parquet data file (already written to the object store at `path`,
    /// relative to [`Self::location`]) committed at engine `lsn` (CONCEPT:EG-317). The
    /// `lake`-feature [`LakeTable::materialize`] calls this after transcoding; a caller
    /// that writes Parquet itself can call it directly.
    pub fn record_file(
        &mut self,
        path: impl Into<String>,
        size_bytes: u64,
        num_rows: u64,
        lsn: Lsn,
    ) {
        self.snapshot.add_file(path, size_bytes, num_rows, lsn);
    }

    /// Render the full Delta `_delta_log` for the current snapshot (CONCEPT:EG-317).
    pub fn delta_log(&self, created_time_ms: i64) -> Vec<DeltaLogFile> {
        delta::build_delta_log(
            &self.schema,
            &self.snapshot,
            &self.table_id,
            created_time_ms,
        )
    }

    /// Render the Iceberg `metadata.json` (+ manifest stub) for the current snapshot
    /// (CONCEPT:EG-317).
    pub fn iceberg(&self, timestamp_ms: i64) -> IcebergTable {
        iceberg::build_iceberg(
            &self.schema,
            &self.snapshot,
            &self.table_id,
            &self.location,
            timestamp_ms,
        )
    }

    /// Register this table's current Iceberg metadata into a REST catalog
    /// (CONCEPT:EG-317), so an external engine can discover + load it.
    pub fn register_in(&self, catalog: &mut IcebergRestCatalog, timestamp_ms: i64) {
        let ib = self.iceberg(timestamp_ms);
        catalog.register(
            self.namespace.clone(),
            self.name.clone(),
            ib.metadata_location,
            Some(ib.metadata_json),
        );
    }

    /// Transcode a [`LakeBatch`] to Parquet, returning the bytes + the object-store
    /// path they should be written to, and record the file in the snapshot at `lsn`
    /// (CONCEPT:EG-317). The caller persists the returned bytes to the blob/S3 backend
    /// under `location/<path>`. Only available with the `lake` feature (needs parquet).
    #[cfg(feature = "lake")]
    pub fn materialize(
        &mut self,
        batch: &LakeBatch,
        lsn: Lsn,
    ) -> Result<(String, Vec<u8>), String> {
        let (bytes, stats) = parquet_io::materialize_with_stats(batch)?;
        // Deterministic part name per commit LSN.
        let path = format!("data/part-{:020}.parquet", lsn.value());
        self.record_file(path.clone(), stats.size_bytes, stats.num_rows, lsn);
        Ok((path, bytes))
    }
}
