//! # eg-lake — LTAP lakehouse interop for epistemic-graph (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns)
//!
//! Makes the engine an **LTAP** (Lakehouse for Transactional + Analytical Processing)
//! superset: one open copy of a table — Parquet on object storage, described by a
//! Delta Lake / Apache Iceberg transaction log — that BOTH the engine's OLTP path and
//! external OLAP engines (Spark / Trino / DuckDB) read, with **no ETL**.
//!
//! ## The four interop seams (per CONCEPT:EG-KG.storage.lsn-as-snapshot-returns)
//! 1. **Parquet materialization** ([`parquet_io`], behind the `lake` feature) —
//!    transcode a [`schema::LakeBatch`] (a table / columnar segment, neutralized) into
//!    a Parquet file's bytes an external engine reads directly.
//! 2. **Table log** — a fully-written, read-consistent **Delta** `_delta_log`
//!    ([`delta`], pure JSON) AND an Iceberg `metadata.json` ([`iceberg`], real
//!    metadata) whose snapshot references **real Avro manifests** written by
//!    [`iceberg_avro`] (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-334, behind the `lake` feature) over those
//!    Parquet files.
//! 3. **LSN as-of snapshot** ([`snapshot`]) — projects the engine's versioned
//!    snapshots / `Op::AsOf` (CONCEPT:EG-KG.compute.preserved/2.250) onto a monotonic LSN and pins
//!    the file set valid as of it, so an external read is a consistent point-in-time.
//! 4. **Iceberg-REST catalog** ([`catalog`]) — a discovery stub: `(namespace, table)`
//!    → `metadata.json` location, rendering the REST catalog response bodies.
//!
//! ## What is real vs. stub (per CONCEPT:EG-KG.storage.lsn-as-snapshot-returns)
//! * **Delta** is implemented FULLY and read-consistently (JSON log, replayable to the
//!   live file set).
//! * **Iceberg** `metadata.json` is real (format-version 2); its manifest layer is now
//!   real spec-compliant **Avro** ([`iceberg_avro`], CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-334) behind the
//!   `lake` feature, INCLUDING per-column stats (`column_sizes` / `value_counts` /
//!   `null_value_counts` / `nan_value_counts` / `lower_bounds` / `upper_bounds`) gathered
//!   at materialize time for external predicate pushdown (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries). Partition
//!   `field_summary` is null by design — the spec is unpartitioned (zero partition
//!   fields).
//! * The **catalog** is contents + response shapes, not an HTTP server.
//!
//! ## The engine-side seam (documented, intentionally NOT in this leaf)
//! `eg-lake` is a pure LEAF crate — it depends on nothing else in the workspace, and
//! the heavy arrow/parquet stack is behind `lake` and OUT of the Pi build. The
//! *wiring* — an async materialization tier that (a) drains the WAL (CONCEPT:EG-KG.query.named-graph-support)
//! up to an LSN, (b) converts an `eg-tsdb::ColumnarSegment` (CONCEPT:EG-KG.temporal.columnar-schema-inference) or an
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

// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for LakeBatch`,
// behind the crate's own opt-in `contract` feature (default OFF). See `src/contract.rs`.
#[cfg(feature = "contract")]
mod contract;

#[cfg(feature = "lake")]
pub mod parquet_io;

// Real Iceberg v2 Avro manifest / manifest-list writer (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-334) — needs
// the `apache-avro` codec, so it lives behind the same `lake` gate as the Parquet
// transcode and stays OUT of the Pi build.
#[cfg(feature = "lake")]
pub mod iceberg_avro;

pub use schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
pub use snapshot::{ColumnStat, FileEntry, Lsn, SnapshotLog};

use catalog::IcebergRestCatalog;
use delta::DeltaLogFile;
use iceberg::IcebergTable;

/// A stable, deterministic UUID-shaped id derived from a table name (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
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
/// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Holds the schema, the durable [`SnapshotLog`] (the LSN → file-set
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
    /// The Iceberg schema-id CURRENTLY in effect for future writes (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id,
    /// INT-P2-4). Starts at `0`; [`Self::evolve_add_column`] bumps it by one per
    /// additive schema change. Threaded onto every [`FileEntry`] recorded from this
    /// point on (`record_file`/`materialize`), so a rewrite that later lands new
    /// files under a NEWER schema never relabels the older, already-live files'
    /// recorded schema-id.
    schema_id: i32,
    /// Every schema version this table has ever used, in id order (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id) —
    /// seeded with `(0, schema)` at construction, appended to on each
    /// `evolve_add_column`. Rendered into `metadata.json`'s `schemas` array so an
    /// external Iceberg reader sees the FULL schema-evolution history, not just
    /// whatever shape the table happens to be today.
    schema_versions: Vec<(i32, LakeSchema)>,
}

impl LakeTable {
    /// Create a table export rooted at `location` (its object-store prefix)
    /// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        schema: LakeSchema,
        location: impl Into<String>,
    ) -> Self {
        let name = name.into();
        let table_id = stable_table_id(&name);
        let schema_versions = vec![(0, schema.clone())];
        LakeTable {
            namespace: namespace.into(),
            name,
            schema,
            location: location.into(),
            snapshot: SnapshotLog::new(),
            table_id,
            schema_id: 0,
            schema_versions,
        }
    }

    /// The Iceberg schema-id currently in effect for future writes (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id).
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    /// Every schema version this table has used, oldest first (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id) —
    /// `(schema_id, schema)` pairs, seeded with `(0, <original schema>)`.
    pub fn schema_versions(&self) -> &[(i32, LakeSchema)] {
        &self.schema_versions
    }

    /// Additive-only schema evolution (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id, INT-P2-4): append a new
    /// column to the table's CURRENT schema and bump [`Self::schema_id`] so every
    /// file recorded from this point on (`record_file`/`materialize`) carries the
    /// NEW id, while files already recorded keep their OLDER id — the per-file
    /// schema-id tracking across a rewrite `iceberg.rs`'s module docs used to flag
    /// as an honest gap. Returns `false` (no-op) if a column of that name already
    /// exists; `true` once the field is added and the schema-id bumped.
    pub fn evolve_add_column(&mut self, field: LakeField) -> bool {
        if self.schema.index_of(&field.name).is_some() {
            return false;
        }
        self.schema.fields.push(field);
        self.schema_id += 1;
        self.schema_versions
            .push((self.schema_id, self.schema.clone()));
        true
    }

    /// The engine LSN of the latest committed materialization (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns) — what
    /// an external reader is handed for a consistent "now" read.
    pub fn current_lsn(&self) -> Lsn {
        self.snapshot.current_lsn()
    }

    /// Record a Parquet data file (already written to the object store at `path`,
    /// relative to [`Self::location`]) committed at engine `lsn` (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). The
    /// `lake`-feature [`LakeTable::materialize`] calls this after transcoding; a caller
    /// that writes Parquet itself can call it directly.
    pub fn record_file(
        &mut self,
        path: impl Into<String>,
        size_bytes: u64,
        num_rows: u64,
        lsn: Lsn,
    ) {
        self.snapshot
            .add_file(path, size_bytes, num_rows, lsn, self.schema_id);
    }

    /// Render the full Delta `_delta_log` for the current snapshot (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn delta_log(&self, created_time_ms: i64) -> Vec<DeltaLogFile> {
        delta::build_delta_log(
            &self.schema,
            &self.snapshot,
            &self.table_id,
            created_time_ms,
        )
    }

    /// Render the Iceberg `metadata.json` (+ manifest stub) for the current snapshot
    /// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn iceberg(&self, timestamp_ms: i64) -> IcebergTable {
        iceberg::build_iceberg(
            &self.schema_versions,
            self.schema_id,
            &self.snapshot,
            &self.table_id,
            &self.location,
            timestamp_ms,
        )
    }

    /// Materialize the real Iceberg **Avro** manifest + manifest-list for the current
    /// snapshot (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-334). The returned byte blobs land at the exact
    /// object-store paths [`Self::iceberg`]'s `metadata.json` references, so a committed
    /// snapshot resolves to a real Avro manifest chain a stock Iceberg reader follows.
    /// The caller persists both blobs to the blob/S3 backend. Only with the `lake`
    /// feature (needs the Avro codec).
    #[cfg(feature = "lake")]
    pub fn iceberg_manifests(&self) -> Result<iceberg_avro::IcebergManifests, String> {
        iceberg_avro::build_iceberg_manifests(
            &self.schema,
            self.schema_id,
            &self.snapshot,
            &self.location,
        )
    }

    /// Register this table's current Iceberg metadata into a REST catalog
    /// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns), so an external engine can discover + load it.
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
    /// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). The caller persists the returned bytes to the blob/S3 backend
    /// under `location/<path>`. Only available with the `lake` feature (needs parquet).
    #[cfg(feature = "lake")]
    pub fn materialize(
        &mut self,
        batch: &LakeBatch,
        lsn: Lsn,
    ) -> Result<(String, Vec<u8>), String> {
        let (bytes, stats, column_stats) = parquet_io::materialize_with_column_stats(batch)?;
        // Deterministic part name per commit LSN.
        let path = format!("data/part-{:020}.parquet", lsn.value());
        // Carry the per-column stats onto the snapshot so the Iceberg Avro manifest
        // (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries) can emit predicate-pushdown bounds a Spark/Trino reader uses.
        self.snapshot.add_file_with_stats(
            path.clone(),
            stats.size_bytes,
            stats.num_rows,
            lsn,
            self.schema_id,
            Some(column_stats),
        );
        Ok((path, bytes))
    }
}
