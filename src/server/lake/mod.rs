//! WAL/series → lakehouse materialization tier (CONCEPT:EG-317 engine-side seam,
//! INT-P2-3).
//!
//! `eg-lake` is deliberately a pure LEAF crate (no workspace edges) that owns the
//! Parquet transcode + the Delta/Iceberg logs + the LSN as-of snapshot + the
//! Iceberg-REST catalog *contents* — see its own crate docs for the "what is real vs.
//! stub" ledger. This module is the documented seam it leaves for the server tier: it
//! (a) drains the engine's OWN durable data (an `eg_tsdb::store::SeriesStore` series —
//! the WAL-backed table/series source), (b) converts it into an `eg_lake::LakeBatch`,
//! (c) materializes Parquet + writes the Delta/Iceberg logs and the real Iceberg Avro
//! manifests into the SAME content-addressed blob CAS `crate::server::obs::segment`
//! already rolls Parquet log segments through (CONCEPT:EG-KG.retrieval.observability-search), and (d) registers the
//! table into an in-process Iceberg-REST catalog + emits an OpenLineage run event.
//!
//! ## Table lifecycle
//! * [`LakeManager::drain_series`] — incremental **append**: only points newer than the
//!   per-series drain cursor are materialized (a WAL-drain, not a full rescan).
//! * [`LakeManager::compact`] — reads every LIVE Parquet file back (via
//!   `eg_lake::parquet_io::read_parquet`), tombstones them, and rewrites ALL their rows
//!   into ONE new file at a fresh LSN (an Iceberg/Delta "rewrite" commit — the SAME
//!   pattern real lakehouse compaction uses).
//! * [`LakeManager::delete_where`] — the SAME read-back-and-rewrite path as `compact`,
//!   but drops rows a predicate matches: real row-level DELETE via rewrite (Iceberg's
//!   own copy-on-write delete strategy), not a stub.
//! * [`LakeManager::evolve_add_column`] — additive-only schema evolution: a new
//!   nullable column is added to the table's `LakeSchema` for FUTURE writes, via
//!   `eg_lake::LakeTable::evolve_add_column`. As of INT-P2-4 (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id) each
//!   committed snapshot's `metadata.json` records the Iceberg schema-id that was
//!   ACTUALLY in effect when it (and every still-live data file) was written —
//!   `schemas[]` carries the FULL schema-version history, `current-schema-id` tracks
//!   the latest, and each live file's manifest-preview entry carries its OWN
//!   schema-id (a live file that predates a later evolution keeps its older id even
//!   after a rewrite lands newer files under the newer schema). Historical Parquet
//!   files still lack the added column's bytes (read back as absent, matching the
//!   engine's existing schema-on-read tolerance for a mismatched cell) — that part is
//!   unchanged. Remaining documented follow-ups: the real Avro manifest still
//!   declares ONE schema-id per manifest FILE (spec-correct, but a live manifest
//!   spanning >1 schema generation doesn't yet split into per-generation manifests),
//!   and partition-spec evolution (eg-lake does not model partitioning at all — every
//!   spec is `[]`/unpartitioned).
//!
//! ## OpenLineage
//! Every materialize/compact/delete run builds an OpenLineage `RunEvent` (job + run +
//! input dataset (the tsdb series) + output dataset (the lake table), with the
//! standard `schema` / `dataSource` / `outputStatistics` facets, a `lifecycleStateChange`
//! facet on CREATE/OVERWRITE, and a small custom `epistemicGraphLake` facet carrying the
//! engine-specific LSN/Iceberg-snapshot correlation) — see [`lineage`]. Kept in a
//! bounded in-memory ring (inspectable via [`LakeManager::recent_lineage`]) and, when
//! `EPISTEMIC_GRAPH_OPENLINEAGE_URL` is set, best-effort POSTed to it over the SAME
//! pure-Rust `ureq` client `sparql-service`/`federation-search` already link.

pub mod lineage;
#[cfg(feature = "lake-rest")]
pub mod rest;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde_json::Value;

use eg_lake::catalog::IcebergRestCatalog;
use eg_lake::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
use eg_lake::snapshot::Lsn;
use eg_lake::LakeTable;
use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;

use crate::server::blob::store::{hex_digest, BlobManifest, ChunkStore, DEFAULT_CHUNK_SIZE};

/// Env var naming the periodic WAL/series→lake materialization sweep interval in
/// seconds (`0`/unset ⇒ disabled — the standing sweep never runs; a caller can still
/// drive materialization directly, e.g. from a test or a future explicit trigger).
pub const LAKE_MATERIALIZE_INTERVAL_ENV: &str = "EPISTEMIC_GRAPH_LAKE_MATERIALIZE_INTERVAL_SECS";
/// Bound on the in-memory OpenLineage event ring (oldest events drop first).
pub const LINEAGE_RING_CAP: usize = 200;
/// The lake namespace new series-backed tables register under by default.
pub const DEFAULT_NAMESPACE: &str = "engine";

/// The kind of write a materialization performs (CONCEPT:EG-317). Threads into the
/// OpenLineage `lifecycleStateChange` facet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LakeOp {
    /// A brand-new table's first file.
    Create,
    /// New rows added to an existing table (no rewrite of prior files).
    Append,
    /// All live files rewritten into one (compaction, or a delete that still leaves
    /// rows).
    Overwrite,
    /// A delete-via-rewrite that removed every remaining row.
    Truncate,
}

impl LakeOp {
    pub fn as_str(self) -> &'static str {
        match self {
            LakeOp::Create => "CREATE",
            LakeOp::Append => "APPEND",
            LakeOp::Overwrite => "OVERWRITE",
            LakeOp::Truncate => "TRUNCATE",
        }
    }
}

/// One materialization run's outcome — round-tripped to the caller/test and mirrored
/// into the OpenLineage event.
#[derive(Clone, Debug)]
pub struct MaterializeReport {
    pub namespace: String,
    pub table: String,
    pub op: LakeOp,
    /// Table-relative Parquet path of the file this run wrote (e.g. `data/part-…parquet`).
    pub path: String,
    pub bytes_len: u64,
    pub num_rows: u64,
    pub lsn: u64,
    /// Object-store-relative location of the table's current `metadata.json`.
    pub metadata_location: String,
    pub lineage_event: Value,
}

/// Per-table durable state the manager owns: the `eg_lake::LakeTable` orchestration
/// handle plus the source series id it was drained from (empty for a table only ever
/// written via `compact`/`delete_where`/the REST commit bridge).
struct TableEntry {
    table: LakeTable,
    source_series: Option<String>,
}

/// Owns every materialized lake table, the aggregate Iceberg-REST catalog, the
/// blob-CAS path index for the bytes this tier writes, the per-series drain cursor, and
/// the bounded OpenLineage event ring (CONCEPT:EG-317, INT-P2-3). Process-global (like
/// `udf_registry`/`foreign_sources` on `ServerState`) — a lake table is not per-graph.
pub struct LakeManager {
    tables: Mutex<HashMap<(String, String), TableEntry>>,
    catalog: Mutex<IcebergRestCatalog>,
    /// Object-store-relative path (`"{location}/{rel}"`) → blob-CAS digest. Content
    /// bytes for every Parquet data file, `_delta_log` commit, `metadata.json` and
    /// Iceberg Avro manifest this tier writes are retrievable by their virtual path
    /// through this index (a future file-serving surface reuses it verbatim — see the
    /// module docs' "not yet done" note on physically serving file bytes over the
    /// REST listener).
    paths: Mutex<HashMap<String, String>>,
    /// Per-series drain cursor: the last point timestamp already materialized, so
    /// `drain_series` only picks up NEW rows each sweep (a WAL-drain, not a rescan).
    drain_cursor: Mutex<HashMap<String, i64>>,
    lineage: Mutex<VecDeque<Value>>,
    next_lsn: AtomicU64,
}

impl Default for LakeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LakeManager {
    pub fn new() -> Self {
        LakeManager {
            tables: Mutex::new(HashMap::new()),
            catalog: Mutex::new(IcebergRestCatalog::new()),
            paths: Mutex::new(HashMap::new()),
            drain_cursor: Mutex::new(HashMap::new()),
            lineage: Mutex::new(VecDeque::new()),
            // Starts at 1 — `Lsn::ZERO`/0 is reserved by eg-lake for "nothing committed
            // yet" (an as-of-0 read is always empty).
            next_lsn: AtomicU64::new(1),
        }
    }

    fn alloc_lsn(&self) -> Lsn {
        Lsn(self.next_lsn.fetch_add(1, Ordering::Relaxed))
    }

    /// The virtual object-store location a `(namespace, table)` pair materializes
    /// under. Descriptive only (mirrors eg-lake's own tests' `"s3://lake/quotes"`
    /// convention) — no real object-store bucket needs to exist at this path; the
    /// bytes live in the blob CAS, addressed through [`Self::paths`].
    fn location_for(namespace: &str, table: &str) -> String {
        format!("lake://{namespace}/{table}")
    }

    /// Store bytes in the blob CAS (chunked through [`DEFAULT_CHUNK_SIZE`], exactly the
    /// pattern `crate::server::obs::segment::store_segment_bytes` uses for Parquet log
    /// segments) and index them by their virtual path.
    fn put_path_bytes(
        &self,
        store: &dyn ChunkStore,
        path: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        let mut chunks = Vec::new();
        let mut chunk_lens = Vec::new();
        for part in bytes.chunks(DEFAULT_CHUNK_SIZE) {
            let (digest, _was_new) = store.put_chunk(part)?;
            chunks.push(digest);
            chunk_lens.push(part.len() as u32);
        }
        let manifest = BlobManifest {
            chunks,
            chunk_lens,
            len: bytes.len() as u64,
            chunk_size: 0,
        };
        let mbytes = rmp_serde::to_vec_named(&manifest).map_err(|e| e.to_string())?;
        let digest = hex_digest(&mbytes);
        store.put_manifest(&digest, &manifest)?;
        store.incref(&digest)?;
        self.paths.lock().insert(path.to_string(), digest);
        Ok(())
    }

    /// Read bytes previously stored at `path` back out of the blob CAS.
    fn read_path_bytes(&self, store: &dyn ChunkStore, path: &str) -> Result<Vec<u8>, String> {
        let digest = self
            .paths
            .lock()
            .get(path)
            .cloned()
            .ok_or_else(|| format!("no bytes indexed at lake path {path}"))?;
        let manifest = store
            .get_manifest(&digest)?
            .ok_or_else(|| format!("blob manifest {digest} missing (path {path})"))?;
        let mut out = Vec::with_capacity(manifest.len as usize);
        for c in &manifest.chunks {
            let chunk = store
                .get_chunk(c)?
                .ok_or_else(|| format!("blob chunk {c} missing (path {path})"))?;
            out.extend(chunk);
        }
        Ok(out)
    }

    /// The `LakeSchema` a series with `n_values` fields per point materializes under:
    /// `ts` (required Timestamp) + one `value`/`value_N` Double column per field.
    fn series_schema(n_values: usize) -> LakeSchema {
        let mut fields = vec![LakeField::required("ts", LakeType::Timestamp)];
        if n_values <= 1 {
            fields.push(LakeField::new("value", LakeType::Double));
        } else {
            for i in 0..n_values {
                fields.push(LakeField::new(format!("value_{i}"), LakeType::Double));
            }
        }
        LakeSchema::new(fields)
    }

    /// Convert tsdb points into a `LakeBatch` over `schema` (schema-on-read tolerant:
    /// a point with fewer/more values than the schema pads/truncates with nulls rather
    /// than erroring, matching the engine's existing columnar tolerance).
    fn points_to_batch(schema: &LakeSchema, points: &[Point]) -> Result<LakeBatch, String> {
        let n_value_cols = schema.len() - 1;
        let rows: Vec<Vec<CellValue>> = points
            .iter()
            .map(|p| {
                let mut row = Vec::with_capacity(schema.len());
                row.push(CellValue::Timestamp(p.ts));
                for i in 0..n_value_cols {
                    row.push(
                        p.values
                            .get(i)
                            .map(|v| CellValue::Double(*v))
                            .unwrap_or(CellValue::Null),
                    );
                }
                row
            })
            .collect();
        LakeBatch::new(schema.clone(), rows)
    }

    /// Get-or-create the `(namespace, table)` entry, seeding its schema on first
    /// creation. Returns whether the table is brand-new (CREATE) this call.
    fn get_or_create<'a>(
        tables: &'a mut HashMap<(String, String), TableEntry>,
        namespace: &str,
        table: &str,
        schema: &LakeSchema,
        source_series: Option<&str>,
    ) -> (&'a mut TableEntry, bool) {
        let key = (namespace.to_string(), table.to_string());
        let is_new = !tables.contains_key(&key);
        let entry = tables.entry(key).or_insert_with(|| TableEntry {
            table: LakeTable::new(
                namespace.to_string(),
                table.to_string(),
                schema.clone(),
                Self::location_for(namespace, table),
            ),
            source_series: source_series.map(str::to_string),
        });
        (entry, is_new)
    }

    /// Materialize one write (a fresh batch of rows) into `(namespace, table)`,
    /// persisting the Parquet file + the Delta log + the Iceberg metadata/Avro
    /// manifests to the blob CAS, registering the table into the catalog, and emitting
    /// an OpenLineage event. `op` should be `Append` for an incremental add — CREATE is
    /// detected automatically for a table's first write.
    #[allow(clippy::too_many_arguments)]
    fn materialize_batch(
        &self,
        store: &dyn ChunkStore,
        namespace: &str,
        table: &str,
        schema: &LakeSchema,
        batch: &LakeBatch,
        source_series: Option<&str>,
        op_hint: LakeOp,
        input_dataset: Option<(&str, &str)>,
    ) -> Result<MaterializeReport, String> {
        let lsn = self.alloc_lsn();
        let ts_ms = lineage::now_ms();
        let mut tables = self.tables.lock();
        let (entry, is_new) =
            Self::get_or_create(&mut tables, namespace, table, schema, source_series);
        let location = entry.table.location.clone();

        let (rel_path, bytes) = entry.table.materialize(batch, lsn)?;
        let bytes_len = bytes.len() as u64;
        let num_rows = batch.num_rows() as u64;
        self.put_path_bytes(store, &format!("{location}/{rel_path}"), &bytes)?;

        // Delta `_delta_log` (pure JSON — table-relative paths need the location prefix
        // added here; the Iceberg/Avro artifacts below already carry it internally).
        for f in entry.table.delta_log(ts_ms as i64) {
            self.put_path_bytes(
                store,
                &format!("{location}/{}", f.path),
                f.content.as_bytes(),
            )?;
        }

        // Iceberg metadata.json + the real Avro manifest / manifest-list.
        let ib = entry.table.iceberg(ts_ms as i64);
        self.put_path_bytes(store, &ib.metadata_location, ib.metadata_json.as_bytes())?;
        let manifests = entry.table.iceberg_manifests()?;
        self.put_path_bytes(store, &manifests.manifest_path, &manifests.manifest_avro)?;
        self.put_path_bytes(
            store,
            &manifests.manifest_list_path,
            &manifests.manifest_list_avro,
        )?;

        let mut cat = self.catalog.lock();
        entry.table.register_in(&mut cat, ts_ms as i64);
        let metadata_location = ib.metadata_location.clone();
        drop(cat);
        drop(tables);

        let op = if is_new { LakeOp::Create } else { op_hint };
        let event = lineage::build_run_event(
            namespace,
            table,
            op,
            schema,
            num_rows,
            bytes_len,
            &location,
            lsn.value(),
            manifests.snapshot_id,
            input_dataset,
        );
        self.push_lineage(event.clone());

        Ok(MaterializeReport {
            namespace: namespace.to_string(),
            table: table.to_string(),
            op,
            path: rel_path,
            bytes_len,
            num_rows,
            lsn: lsn.value(),
            metadata_location,
            lineage_event: event,
        })
    }

    /// Incremental append: materialize only tsdb points newer than the per-series
    /// drain cursor (the WAL-drain semantics — CONCEPT:EG-317's engine-side seam). The
    /// table name is the series id itself (sanitized); the namespace is
    /// [`DEFAULT_NAMESPACE`]. Returns `Ok(None)` when there is nothing new to drain.
    pub fn drain_series(
        &self,
        store: &dyn ChunkStore,
        tsdb: &SeriesStore,
        series_id: &str,
    ) -> Result<Option<MaterializeReport>, String> {
        let cursor = self.drain_cursor.lock().get(series_id).copied();
        let from = cursor.map(|c| c.saturating_add(1)).unwrap_or(i64::MIN);
        let points = tsdb
            .range(series_id, from, i64::MAX)
            .map_err(|e| e.to_string())?;
        if points.is_empty() {
            return Ok(None);
        }
        let n_values = points.iter().map(|p| p.values.len()).max().unwrap_or(1);
        let schema = Self::series_schema(n_values);
        let batch = Self::points_to_batch(&schema, &points)?;
        let table = sanitize_table_name(series_id);
        let max_ts = points.iter().map(|p| p.ts).max().unwrap_or(from);
        let report = self.materialize_batch(
            store,
            DEFAULT_NAMESPACE,
            &table,
            &schema,
            &batch,
            Some(series_id),
            LakeOp::Append,
            Some(("epistemic-graph.tsdb", series_id)),
        )?;
        self.drain_cursor
            .lock()
            .insert(series_id.to_string(), max_ts);
        Ok(Some(report))
    }

    /// Rewrite every LIVE Parquet file of `(namespace, table)` into ONE new file at a
    /// fresh LSN, keeping only rows for which `keep(&row)` is `true` — real row-level
    /// DELETE (`keep` excludes matching rows) and compaction (`keep` = "always true")
    /// share this one path, matching how a real lakehouse implements both as a
    /// copy-on-write rewrite. Returns `Ok(None)` if the table has no live files.
    pub fn delete_where(
        &self,
        store: &dyn ChunkStore,
        namespace: &str,
        table: &str,
        keep: impl Fn(&[CellValue]) -> bool,
    ) -> Result<Option<MaterializeReport>, String> {
        let (schema, location, live_paths, source_series) = {
            let tables = self.tables.lock();
            let Some(entry) = tables.get(&(namespace.to_string(), table.to_string())) else {
                return Ok(None);
            };
            let live: Vec<String> = entry
                .table
                .snapshot
                .live_files()
                .iter()
                .map(|f| f.path.clone())
                .collect();
            if live.is_empty() {
                return Ok(None);
            }
            (
                entry.table.schema.clone(),
                entry.table.location.clone(),
                live,
                entry.source_series.clone(),
            )
        };

        // Read every live file back and fold its rows through the keep predicate.
        let mut kept_rows: Vec<Vec<CellValue>> = Vec::new();
        for rel in &live_paths {
            let bytes = self.read_path_bytes(store, &format!("{location}/{rel}"))?;
            let batch = eg_lake::parquet_io::read_parquet(&bytes)?;
            for row in batch.rows {
                if keep(&row) {
                    kept_rows.push(row);
                }
            }
        }
        let had_rows = !kept_rows.is_empty();
        let new_batch = LakeBatch::new(schema.clone(), kept_rows)?;

        // Tombstone the old files at the SAME new LSN the rewrite commits under, so the
        // Delta log emits one contiguous "remove old + add new" version (a real
        // OVERWRITE commit), then materialize the rewritten batch.
        let lsn = self.alloc_lsn();
        {
            let mut tables = self.tables.lock();
            let entry = tables
                .get_mut(&(namespace.to_string(), table.to_string()))
                .ok_or_else(|| format!("table {namespace}.{table} vanished mid-rewrite"))?;
            for rel in &live_paths {
                entry.table.snapshot.remove_file(rel, lsn);
            }
        }
        // materialize_batch allocates its OWN lsn for the new file; that is fine (it is
        // strictly greater, so the file is recorded live from that point on) — the
        // tombstone above already advanced `current` to at least this rewrite's lsn.
        let op = if had_rows {
            LakeOp::Overwrite
        } else {
            LakeOp::Truncate
        };
        self.materialize_batch(
            store,
            namespace,
            table,
            &schema,
            &new_batch,
            source_series.as_deref(),
            op,
            None,
        )
        .map(Some)
    }

    /// Compaction: rewrite every live file into one, keeping every row. A thin wrapper
    /// over [`Self::delete_where`] with an always-true predicate — the SAME rewrite
    /// path, matching how a real lakehouse implements compaction as "delete nothing".
    pub fn compact(
        &self,
        store: &dyn ChunkStore,
        namespace: &str,
        table: &str,
    ) -> Result<Option<MaterializeReport>, String> {
        self.delete_where(store, namespace, table, |_row| true)
    }

    /// Additive-only schema evolution (CONCEPT:EG-317): append a new nullable column
    /// to the table's current `LakeSchema` for FUTURE writes, via
    /// [`LakeTable::evolve_add_column`] — which also bumps the table's Iceberg
    /// schema-id and records the new schema version, so a subsequent
    /// [`LakeTable::iceberg`] render carries the FULL schema-evolution history
    /// (CONCEPT:EG-KG.storage.iceberg-per-file-schema-id, INT-P2-4) instead of always schema-id 0. Returns
    /// `Ok(true)` if the column was added, `Ok(false)` if a column of that name
    /// already exists, `Err` if the table is unknown.
    pub fn evolve_add_column(
        &self,
        namespace: &str,
        table: &str,
        field: LakeField,
    ) -> Result<bool, String> {
        let mut tables = self.tables.lock();
        let entry = tables
            .get_mut(&(namespace.to_string(), table.to_string()))
            .ok_or_else(|| format!("no such table: {namespace}.{table}"))?;
        Ok(entry.table.evolve_add_column(field))
    }

    // ── Iceberg-REST catalog reads (delegated straight to `eg_lake::catalog`) ──────

    pub fn list_namespaces(&self) -> Value {
        self.catalog.lock().list_namespaces()
    }

    pub fn list_tables(&self, namespace: &str) -> Value {
        self.catalog.lock().list_tables(namespace)
    }

    pub fn load_table(&self, namespace: &str, table: &str) -> Option<Value> {
        self.catalog.lock().load_table(namespace, table)
    }

    pub fn namespace_exists(&self, namespace: &str) -> bool {
        let cat = self.catalog.lock();
        cat.list_namespaces()["namespaces"]
            .as_array()
            .map(|levels| levels.iter().any(|n| n[0] == namespace))
            .unwrap_or(false)
    }

    /// CommitTable bridge for the REST surface (INT-P2-3, honest scope note per the
    /// `lake-rest` feature docs): the REST `POST .../tables/{table}` endpoint is
    /// accepted per the Iceberg-REST spec's request/response envelope, but this tier
    /// does not ingest externally-authored manifests/data files (the engine is the
    /// sole writer of its own tables) — a commit simply triggers the engine's own
    /// compaction pass and returns the resulting `LoadTableResponse` shape.
    pub fn commit_table(
        &self,
        store: &dyn ChunkStore,
        namespace: &str,
        table: &str,
    ) -> Result<Value, String> {
        self.compact(store, namespace, table)?;
        self.load_table(namespace, table)
            .ok_or_else(|| format!("no such table: {namespace}.{table}"))
    }

    // ── OpenLineage ─────────────────────────────────────────────────────────────

    fn push_lineage(&self, event: Value) {
        tracing::info!(target: "eg_lake::lineage", event = %event, "OpenLineage run event");
        {
            let mut ring = self.lineage.lock();
            ring.push_back(event.clone());
            while ring.len() > LINEAGE_RING_CAP {
                ring.pop_front();
            }
        }
        lineage::maybe_push_http(&event);
    }

    /// The `n` most recent OpenLineage events (newest last) — inspection/tests.
    pub fn recent_lineage(&self, n: usize) -> Vec<Value> {
        let ring = self.lineage.lock();
        ring.iter().rev().take(n).rev().cloned().collect()
    }
}

/// Turn an arbitrary series id into a safe Iceberg/Delta table name (ASCII
/// alnum/`_`/`-`/`.`, anything else → `_`).
fn sanitize_table_name(series_id: &str) -> String {
    series_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::blob::store::RedbChunkStore;

    fn store() -> RedbChunkStore {
        RedbChunkStore::open_temp().unwrap()
    }

    const TEST_BUCKET_NS: u64 = 3_600_000_000_000;

    fn points(from: i64, n: i64) -> Vec<Point> {
        (0..n)
            .map(|i| Point::single(from + i, (from + i) as f64 * 1.5))
            .collect()
    }

    fn append(tsdb: &SeriesStore, series_id: &str, pts: &[Point]) {
        tsdb.append_batch(series_id, 1, TEST_BUCKET_NS, &["v".to_string()], pts)
            .unwrap();
    }

    #[test]
    fn drain_series_materializes_only_new_points_each_call() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-{}", std::process::id())),
        )
        .unwrap();
        append(&tsdb, "temp.sensor1", &points(0, 3));
        let mgr = LakeManager::new();

        let r1 = mgr
            .drain_series(&s, &tsdb, "temp.sensor1")
            .unwrap()
            .expect("first drain materializes rows");
        assert_eq!(r1.op, LakeOp::Create);
        assert_eq!(r1.num_rows, 3);
        assert_eq!(r1.namespace, DEFAULT_NAMESPACE);

        // No new points yet: nothing to drain.
        assert!(mgr
            .drain_series(&s, &tsdb, "temp.sensor1")
            .unwrap()
            .is_none());

        // New points land; only the delta is materialized (a SECOND file, not a
        // rescan of all 6).
        append(&tsdb, "temp.sensor1", &points(3, 2));
        let r2 = mgr
            .drain_series(&s, &tsdb, "temp.sensor1")
            .unwrap()
            .expect("second drain picks up only the new rows");
        assert_eq!(r2.op, LakeOp::Append);
        assert_eq!(r2.num_rows, 2);
        assert_ne!(r2.path, r1.path, "a distinct Parquet file per drain");

        // The catalog now lists + loads the table with BOTH files live.
        let loaded = mgr
            .load_table(DEFAULT_NAMESPACE, "temp_sensor1")
            .expect("table registered");
        assert_eq!(
            loaded["metadata"]["snapshots"][0]["summary"]["total-data-files"],
            "2"
        );
    }

    #[test]
    fn compact_merges_live_files_into_one_and_preserves_rows() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-compact-{}", std::process::id())),
        )
        .unwrap();
        append(&tsdb, "s1", &points(0, 3));
        let mgr = LakeManager::new();
        mgr.drain_series(&s, &tsdb, "s1").unwrap();
        append(&tsdb, "s1", &points(3, 3));
        mgr.drain_series(&s, &tsdb, "s1").unwrap();

        let table = sanitize_table_name("s1");
        let loaded_before = mgr.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_eq!(
            loaded_before["metadata"]["snapshots"][0]["summary"]["total-data-files"],
            "2"
        );

        let report = mgr
            .compact(&s, DEFAULT_NAMESPACE, &table)
            .unwrap()
            .expect("compaction produced a rewrite");
        assert_eq!(report.op, LakeOp::Overwrite);
        assert_eq!(report.num_rows, 6, "all rows survive a pure compaction");

        let loaded_after = mgr.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_eq!(
            loaded_after["metadata"]["snapshots"][0]["summary"]["total-data-files"], "1",
            "compaction merges to ONE live file"
        );
    }

    #[test]
    fn delete_where_removes_matching_rows_and_truncate_when_all_removed() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-delete-{}", std::process::id())),
        )
        .unwrap();
        append(&tsdb, "s2", &points(0, 4));
        let mgr = LakeManager::new();
        mgr.drain_series(&s, &tsdb, "s2").unwrap();
        let table = sanitize_table_name("s2");

        // Delete rows whose value column (index 1) is < 3.0 (ts=0,1 → values 0.0,1.5).
        let report = mgr
            .delete_where(
                &s,
                DEFAULT_NAMESPACE,
                &table,
                |row| !matches!(row[1], CellValue::Double(v) if v < 3.0),
            )
            .unwrap()
            .expect("delete produced a rewrite");
        assert_eq!(report.op, LakeOp::Overwrite);
        assert_eq!(report.num_rows, 2, "two of the four rows survive");

        // Delete everything that remains → TRUNCATE (0 rows).
        let report2 = mgr
            .delete_where(&s, DEFAULT_NAMESPACE, &table, |_row| false)
            .unwrap()
            .expect("delete-all still produces a (empty) rewrite");
        assert_eq!(report2.op, LakeOp::Truncate);
        assert_eq!(report2.num_rows, 0);
    }

    #[test]
    fn evolve_add_column_widens_schema_for_future_writes_only() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-evolve-{}", std::process::id())),
        )
        .unwrap();
        append(&tsdb, "s3", &points(0, 2));
        let mgr = LakeManager::new();
        mgr.drain_series(&s, &tsdb, "s3").unwrap();
        let table = sanitize_table_name("s3");

        assert!(mgr
            .evolve_add_column(
                DEFAULT_NAMESPACE,
                &table,
                LakeField::new("note", LakeType::String)
            )
            .unwrap());
        // Re-adding the same name is a no-op, not an error.
        assert!(!mgr
            .evolve_add_column(
                DEFAULT_NAMESPACE,
                &table,
                LakeField::new("note", LakeType::String)
            )
            .unwrap());
        // Unknown table errs.
        assert!(mgr
            .evolve_add_column(
                DEFAULT_NAMESPACE,
                "nope",
                LakeField::new("x", LakeType::Long)
            )
            .is_err());

        let loaded = mgr.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        // Not yet re-materialized, so the catalog's schema is still pre-evolution
        // (evolution affects the IN-MEMORY LakeTable schema for the NEXT write) —
        // confirm a subsequent compaction reflects the wider schema.
        let fields_before = loaded["metadata"]["schemas"][0]["fields"]
            .as_array()
            .unwrap()
            .len();
        mgr.compact(&s, DEFAULT_NAMESPACE, &table).unwrap();
        let loaded_after = mgr.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        let fields_after = loaded_after["metadata"]["schemas"][0]["fields"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(
            fields_after,
            fields_before + 1,
            "compaction re-renders metadata.json under the widened schema"
        );
    }

    #[test]
    fn recent_lineage_carries_openlineage_shaped_events() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-lineage-{}", std::process::id())),
        )
        .unwrap();
        append(&tsdb, "s4", &points(0, 2));
        let mgr = LakeManager::new();
        mgr.drain_series(&s, &tsdb, "s4").unwrap();

        let events = mgr.recent_lineage(10);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev["eventType"], "COMPLETE");
        assert!(ev["run"]["runId"].is_string());
        assert_eq!(ev["job"]["namespace"], "epistemic-graph.lake");
        assert!(ev["job"]["name"].as_str().unwrap().contains("materialize"));
        assert_eq!(ev["inputs"][0]["namespace"], "epistemic-graph.tsdb");
        assert_eq!(ev["inputs"][0]["name"], "s4");
        let out = &ev["outputs"][0];
        assert_eq!(out["name"], "engine.s4");
        assert!(out["facets"]["schema"]["fields"].as_array().unwrap().len() >= 2);
        assert_eq!(out["facets"]["dataSource"]["uri"], "lake://engine/s4");
        assert_eq!(out["facets"]["outputStatistics"]["rowCount"], 2);
        assert_eq!(
            out["facets"]["lifecycleStateChange"]["lifecycleStateChange"],
            "CREATE"
        );
        assert_eq!(out["facets"]["epistemicGraphLake"]["op"], "CREATE");
    }
}
