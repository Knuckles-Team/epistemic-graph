//! WAL/series → lakehouse materialization tier (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns engine-side seam,
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
//! pure-Rust `ureq` client `sparql-service`/`federation-search` already link. **With
//! feature `lineage-transport` on** (CA-15), `push_lineage` routes through
//! [`lineage_transport::configured_transports`] instead of calling
//! [`lineage::maybe_push_http`] directly — a superset that still includes that same
//! HTTP push (wrapped, unchanged) plus an optional Kafka leg (`openlineage.events` per
//! `DEC-CA-03`, feature `lineage-transport-kafka`) when configured. See
//! [`lineage_transport`]'s module doc for the full design and the `DEC-CA-05`
//! reconciliation note on inbound facets.

pub mod lineage;
// OpenLineage transport (CA-15, feature `lineage-transport`). A best-effort HTTP push to
// `EPISTEMIC_GRAPH_OPENLINEAGE_URL` already existed at `lineage::maybe_push_http`
// (CA-17's stub note, preserved here) -- this module wires it into a composable
// transport set and adds a Kafka leg, per `DEC-CA-03`/`DEC-CA-05`; see its own doc.
#[cfg(feature = "lineage-transport")]
pub mod lineage_transport;
#[cfg(feature = "lake-rest")]
pub mod rest;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde_json::{json, Value};

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

/// The kind of write a materialization performs (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Threads into the
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
/// written via `compact`/`delete_where`/the REST commit bridge), plus the REST-facing
/// ownership tag (W04, GOC-75-W04) used for catalog row-level visibility.
struct TableEntry {
    table: LakeTable,
    source_series: Option<String>,
    /// `None` = engine-internal/system table (e.g. drained straight from a tsdb
    /// series by the materialization sweep) — visible to every authenticated
    /// caller, matching this tier's behavior before W04. `Some(owner_scope)` = a
    /// table created through the authenticated Iceberg-REST `CreateTable` path,
    /// tagged with its creating carrier's `CarrierAuthority::owner_scope()` (the
    /// SAME per-agent ownership key `GraphReadAuthority`'s row-level security
    /// already uses elsewhere in this engine — see [`LakeVisibility`]).
    owner_tenant: Option<String>,
}

/// Row-level catalog visibility for one Iceberg-REST request (W04, GOC-75-W04).
///
/// This deliberately keys on the engine's existing per-AGENT RLS ownership
/// primitive (`CarrierAuthority::owner_scope()`, combining tenant+actor), not on
/// `EPISTEMIC_GRAPH_TENANT` alone: that single deployment-wide tenant value is
/// already enforced at carrier-MINTING time by BUG-222's W01/W02 fix
/// (`server::auth::authenticated_iceberg_bearer` rejects a bearer for any other
/// tenant before a `CarrierAuthority` is ever produced), so within one running
/// deployment every successfully-authenticated caller necessarily shares the
/// same `tenant_scope` — a raw tenant-scope filter here would be a no-op. Two
/// callers ("two tenants" in the lane's acceptance language) are made to see
/// disjoint catalogs by their distinct `owner_scope` instead, mirroring how
/// `server::access::GraphReadAuthority` already row-filters graph reads by
/// ownership rather than by the shared deployment tenant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LakeVisibility {
    /// The non-security `serve()` path (no live carrier at all) — unfiltered,
    /// byte-for-byte this tier's behavior before W04.
    Unfiltered,
    /// A verified, non-admin caller: sees engine-internal tables (`owner_tenant
    /// == None`) plus only the tables owned by this exact scope.
    Owner(String),
}

impl LakeVisibility {
    fn allows(&self, owner_tenant: Option<&str>) -> bool {
        match self {
            LakeVisibility::Unfiltered => true,
            LakeVisibility::Owner(scope) => match owner_tenant {
                None => true,
                Some(t) => t == scope,
            },
        }
    }
}

/// Failure returned when an as-of request names an LSN that was never
/// committed by this manager.  `Ok(None)` remains the result for an unknown or
/// unauthorized table, so callers cannot use an invalid LSN to probe catalog
/// visibility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadTableAsOfError {
    /// The requested LSN is not a representable, committed point in the
    /// manager's history.  LSN 0 is reserved for the valid empty history.
    LsnUnavailable { requested: u64, current_lsn: u64 },
}

/// Committed LSNs as sorted, coalesced ranges rather than one heap entry per
/// write.  Normal append traffic stays one range; only failed or concurrently
/// reordered reservations leave additional ranges, so the as-of validity index
/// remains bounded by the number of holes instead of table history length.
#[derive(Default)]
struct CommittedLsnLedger {
    ranges: Vec<(u64, u64)>,
}

impl CommittedLsnLedger {
    fn insert(&mut self, lsn: u64) {
        let mut start = lsn;
        let mut end = lsn;
        let mut index = 0;
        while index < self.ranges.len() {
            let (range_start, range_end) = self.ranges[index];
            if range_end.saturating_add(1) < start {
                index += 1;
                continue;
            }
            if end.saturating_add(1) < range_start {
                break;
            }
            start = start.min(range_start);
            end = end.max(range_end);
            self.ranges.remove(index);
        }
        self.ranges.insert(index, (start, end));
    }

    fn contains(&self, lsn: u64) -> bool {
        self.ranges
            .binary_search_by(|(start, end)| {
                if lsn < *start {
                    std::cmp::Ordering::Greater
                } else if lsn > *end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }
}

/// Split a catalog namespace back into its Iceberg-REST levels (W03, GOC-75-W03):
/// multi-level identifiers are decoded off the wire into ONE internal string
/// joined by the spec's own `\x1f` unit-separator (see `rest.rs`'s path decoding),
/// so rendering a response's `"namespace": [...]` array is just splitting on that
/// same byte back apart — no change to `eg-lake`'s underlying flat-string model.
pub(crate) fn namespace_levels(ns: &str) -> Vec<String> {
    ns.split('\u{1f}').map(str::to_string).collect()
}

/// Slice `items` (already sorted) by an opaque `page-token` (a plain decimal
/// offset) and an optional `page-size`, per the Iceberg-REST pagination
/// convention (W03, GOC-75-W03). `page_size: None` returns every remaining item
/// (server support for paging is opt-in per the spec; a caller that never asks
/// for a page gets today's "everything" behavior). Returns the page plus the
/// `next-page-token` to hand back (`None` on the final page).
fn paginate<T: Clone>(
    items: &[T],
    page_token: Option<&str>,
    page_size: Option<usize>,
) -> (Vec<T>, Option<String>) {
    let start = page_token
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0)
        .min(items.len());
    let size = page_size.unwrap_or(items.len().saturating_sub(start).max(1));
    let end = start.saturating_add(size).min(items.len());
    let page = items[start..end].to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// `CreateTable` failure modes (W03, GOC-75-W03) — kept distinct from a generic
/// string error so the REST layer can pick the spec-conformant status/type
/// (`409 AlreadyExistsException` vs `400 BadRequestException`).
#[derive(Debug)]
pub enum CreateTableError {
    AlreadyExists,
    Other(String),
}

/// `RenameTable` failure modes (W03, GOC-75-W03). A visibility failure on the
/// source is folded into `SourceNotFound` — same as `load_table_visible`, a
/// caller who cannot see a table gets the SAME 404 an actually-missing table
/// gets, never a distinguishing 403 (W04's "no leak via error messages" bar).
#[derive(Debug)]
pub enum RenameTableError {
    SourceNotFound,
    DestinationExists,
}

/// Owns every materialized lake table, the aggregate Iceberg-REST catalog, the
/// blob-CAS path index for the bytes this tier writes, the per-series drain cursor, and
/// the bounded OpenLineage event ring (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns, INT-P2-3). Process-global (like
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
    /// Bounded audit trail of catalog denials + mutations (W05, GOC-75-W05) —
    /// the SAME bounded-ring shape as `lineage` above. Every entry is ALSO
    /// emitted as a structured `tracing` line (target
    /// `epistemic_graph::lake::audit`) for real log aggregation; this ring is
    /// the in-process inspection surface (`recent_audit`) tests and callers use
    /// to prove an event landed, mirroring `recent_lineage`.
    audit: Mutex<VecDeque<Value>>,
    /// Coalesced ranges of every successfully persisted write LSN.  This is
    /// deliberately separate from each table's snapshot log: a committed
    /// global LSN before a table was created is a valid empty history for that
    /// table, while a reserved LSN from a failed write must never be accepted
    /// as an as-of boundary.
    committed_lsns: Mutex<CommittedLsnLedger>,
    next_lsn: AtomicU64,
}

/// Bound on the in-memory audit-event ring (oldest events drop first).
pub const AUDIT_RING_CAP: usize = 500;

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
            audit: Mutex::new(VecDeque::new()),
            committed_lsns: Mutex::new(CommittedLsnLedger::default()),
            // Starts at 1 — `Lsn::ZERO`/0 is reserved by eg-lake for "nothing committed
            // yet" (an as-of-0 read is always empty).
            next_lsn: AtomicU64::new(1),
        }
    }

    fn alloc_lsn(&self) -> Lsn {
        Lsn(self.next_lsn.fetch_add(1, Ordering::Relaxed))
    }

    fn record_committed_lsn(&self, lsn: Lsn) {
        self.committed_lsns.lock().insert(lsn.value());
    }

    fn is_committed_lsn(&self, lsn: u64) -> bool {
        lsn == 0 || self.committed_lsns.lock().contains(lsn)
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
            schema_version: crate::server::blob::BLOB_MANIFEST_VERSION,
            owner_scope: crate::server::blob::ENGINE_BLOB_OWNER_SCOPE.to_string(),
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
    #[allow(clippy::too_many_arguments)]
    fn get_or_create<'a>(
        tables: &'a mut HashMap<(String, String), TableEntry>,
        namespace: &str,
        table: &str,
        schema: &LakeSchema,
        source_series: Option<&str>,
        owner_tenant: Option<&str>,
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
            owner_tenant: owner_tenant.map(str::to_string),
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
        owner_tenant: Option<&str>,
    ) -> Result<MaterializeReport, String> {
        let lsn = self.alloc_lsn();
        let ts_ms = lineage::now_ms();
        let mut tables = self.tables.lock();
        let (entry, is_new) = Self::get_or_create(
            &mut tables,
            namespace,
            table,
            schema,
            source_series,
            owner_tenant,
        );
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
        self.record_committed_lsn(lsn);
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
    /// drain cursor (the WAL-drain semantics — CONCEPT:EG-KG.storage.lsn-as-snapshot-returns's engine-side seam). The
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
            None,
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
        //
        // A live file may predate a LATER `evolve_add_column` (module docs: "Historical
        // Parquet files still lack the added column's bytes (read back as absent,
        // matching the engine's existing schema-on-read tolerance for a mismatched
        // cell)") — its rows carry the file's OWN (narrower) column count, but
        // `new_batch` below is built against the table's CURRENT (possibly wider)
        // `schema`. `keep` runs against the row as the file actually stored it (the
        // predicate's column indices are relative to what the row's own writer
        // produced), then each surviving row is padded with `CellValue::Null` for any
        // columns added since — the SAME schema-on-read tolerance `points_to_batch`
        // already gives brand-new writes, applied here on the read-back-and-rewrite
        // path so `compact`/`delete_where` don't hand `LakeBatch::new` a column-count
        // mismatch on a table with evolution history.
        let mut kept_rows: Vec<Vec<CellValue>> = Vec::new();
        for rel in &live_paths {
            let bytes = self.read_path_bytes(store, &format!("{location}/{rel}"))?;
            let batch = eg_lake::parquet_io::read_parquet(&bytes)?;
            for mut row in batch.rows {
                if keep(&row) {
                    if row.len() < schema.len() {
                        row.resize(schema.len(), CellValue::Null);
                    }
                    kept_rows.push(row);
                }
            }
        }
        let had_rows = !kept_rows.is_empty();
        let new_batch = LakeBatch::new(schema.clone(), kept_rows)?;

        // Tombstone the old files at a reserved rewrite LSN, then materialize
        // the replacement batch at the next LSN.  The ledger records both
        // successful boundaries, preserving the valid intermediate empty
        // projection as well as the final overwrite.
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
        let report = self.materialize_batch(
            store,
            namespace,
            table,
            &schema,
            &new_batch,
            source_series.as_deref(),
            op,
            None,
            None,
        )?;
        // The rewrite's tombstone and replacement file have distinct LSNs in
        // the current implementation.  Both are committed boundaries: callers
        // may legitimately ask for the point between them and observe the
        // transiently empty file set.
        self.record_committed_lsn(lsn);
        Ok(Some(report))
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

    /// Additive-only schema evolution (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns): append a new nullable column
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

    /// Time-travel `LoadTable`: resolve a query-time as-of request — already reduced
    /// to a concrete engine `lsn` by the caller — and render the table's Iceberg
    /// metadata for the file set that was actually live AT THAT POINT (BUG-224, the
    /// `Op::AsOf` → `Lsn` seam `crates/eg-lake`'s docs flag as the server tier's to
    /// own: `eg_lake::snapshot::SnapshotLog::files_as_of` existed, but nothing on this
    /// server tier ever called it with anything but "now" — [`Self::load_table`]
    /// only ever serves the catalog's cached CURRENT snapshot). Unlike `load_table`,
    /// this reads the live [`LakeTable`] directly (the catalog only caches the latest
    /// metadata.json, not history), so a `lsn` from before a later
    /// compact/delete_where/drain_series still resolves to the file set live at that
    /// lsn, not today's. Returns the same `{"metadata-location", "metadata",
    /// "config"}` shape [`Self::load_table`] returns. `Ok(None)` means the table
    /// is unknown or hidden by `visibility`, matching [`Self::load_table_visible`]
    /// and preventing existence/error side channels. `Err` means the table is
    /// visible but `lsn` is not a committed point in this manager's global
    /// history. LSN `0` is always valid and represents the empty history; a
    /// committed LSN from before this table was created is also valid and
    /// returns an empty projection. Values above `i64::MAX` are rejected because
    /// Iceberg snapshot ids are signed 64-bit values.
    pub fn load_table_as_of(
        &self,
        namespace: &str,
        table: &str,
        lsn: u64,
        visibility: &LakeVisibility,
    ) -> Result<Option<Value>, LoadTableAsOfError> {
        let tables = self.tables.lock();
        let Some(entry) = tables.get(&(namespace.to_string(), table.to_string())) else {
            return Ok(None);
        };
        if !visibility.allows(entry.owner_tenant.as_deref()) {
            return Ok(None);
        }
        let current_lsn = entry.table.current_lsn().value();
        if lsn > i64::MAX as u64 || !self.is_committed_lsn(lsn) {
            return Err(LoadTableAsOfError::LsnUnavailable {
                requested: lsn,
                current_lsn,
            });
        }
        let ts_ms = lineage::now_ms();
        let ib = entry.table.iceberg_as_of(Lsn(lsn), ts_ms as i64);
        let metadata: Value = serde_json::from_str(&ib.metadata_json).unwrap_or(Value::Null);
        Ok(Some(json!({
            "metadata-location": ib.metadata_location,
            "metadata": metadata,
            "config": {},
        })))
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

    // ── Iceberg-REST catalog reads, visibility-projected (W04, GOC-75-W04) ─────────
    //
    // These are ADDITIVE siblings of the plain `list_namespaces`/`list_tables`/
    // `load_table`/`namespace_exists` above (kept byte-for-byte unchanged for their
    // existing internal callers — the drain sweep, this module's own tests). The
    // REST surface (`rest.rs`) calls these instead once a request's
    // `LakeVisibility` is known, so an owner-scoped table never appears in another
    // owner's listing, existence check, or load — the SAME 404 an actually-missing
    // table gets, never a distinguishing 403 (closes the existence/count/error-
    // message side channels the lane's security section calls out).

    fn namespace_visible(&self, namespace: &str, visibility: &LakeVisibility) -> bool {
        let tables = self.tables.lock();
        tables.iter().any(|((ns, _), entry)| {
            ns == namespace && visibility.allows(entry.owner_tenant.as_deref())
        })
    }

    pub(crate) fn list_namespaces_visible(
        &self,
        visibility: &LakeVisibility,
        page_token: Option<&str>,
        page_size: Option<usize>,
    ) -> Value {
        let mut namespaces: Vec<String> = {
            let tables = self.tables.lock();
            tables
                .iter()
                .filter(|(_, entry)| visibility.allows(entry.owner_tenant.as_deref()))
                .map(|((ns, _), _)| ns.clone())
                .collect()
        };
        namespaces.sort();
        namespaces.dedup();
        let (page, next) = paginate(&namespaces, page_token, page_size);
        let namespaces_json: Vec<Value> =
            page.iter().map(|ns| json!(namespace_levels(ns))).collect();
        let mut out = json!({ "namespaces": namespaces_json });
        if let Some(t) = next {
            out["next-page-token"] = json!(t);
        }
        out
    }

    pub(crate) fn namespace_exists_visible(
        &self,
        namespace: &str,
        visibility: &LakeVisibility,
    ) -> bool {
        self.namespace_visible(namespace, visibility)
    }

    pub(crate) fn list_tables_visible(
        &self,
        namespace: &str,
        visibility: &LakeVisibility,
        page_token: Option<&str>,
        page_size: Option<usize>,
    ) -> Value {
        let mut names: Vec<String> = {
            let tables = self.tables.lock();
            tables
                .iter()
                .filter(|((ns, _), entry)| {
                    ns == namespace && visibility.allows(entry.owner_tenant.as_deref())
                })
                .map(|((_, name), _)| name.clone())
                .collect()
        };
        names.sort();
        let (page, next) = paginate(&names, page_token, page_size);
        let levels = namespace_levels(namespace);
        let identifiers: Vec<Value> = page
            .iter()
            .map(|name| json!({ "namespace": levels, "name": name }))
            .collect();
        let mut out = json!({ "identifiers": identifiers });
        if let Some(t) = next {
            out["next-page-token"] = json!(t);
        }
        out
    }

    pub(crate) fn load_table_visible(
        &self,
        namespace: &str,
        table: &str,
        visibility: &LakeVisibility,
    ) -> Option<Value> {
        {
            let tables = self.tables.lock();
            let entry = tables.get(&(namespace.to_string(), table.to_string()))?;
            if !visibility.allows(entry.owner_tenant.as_deref()) {
                return None;
            }
        }
        self.load_table(namespace, table)
    }

    // ── Iceberg-REST catalog writes: CreateTable / DropTable / RenameTable ─────────
    // (W03, GOC-75-W03 — the REST surface's remaining verbs named in the lane's
    // "Still open" list; each still routes through THIS manager's one table store,
    // never a second catalog.)

    /// `CreateTable`: register a brand-new, empty `(namespace, table)` under
    /// `schema`, tagged with `owner_tenant` (W04's ownership tag — `None` from the
    /// non-security `serve()` path, `Some(carrier.owner_scope())` from an
    /// authenticated REST request). Materializes one (zero-row) Parquet/Delta/
    /// Iceberg-Avro commit via the SAME [`Self::materialize_batch`] pipeline every
    /// other write uses, so a freshly created table is immediately a real,
    /// loadable Iceberg table (`LoadTable` right after `CreateTable` needs no
    /// special-casing).
    pub fn create_table(
        &self,
        store: &dyn ChunkStore,
        namespace: &str,
        table: &str,
        schema: LakeSchema,
        owner_tenant: Option<&str>,
    ) -> Result<Value, CreateTableError> {
        {
            let tables = self.tables.lock();
            if tables.contains_key(&(namespace.to_string(), table.to_string())) {
                return Err(CreateTableError::AlreadyExists);
            }
        }
        let batch = LakeBatch::new(schema.clone(), Vec::new()).map_err(CreateTableError::Other)?;
        self.materialize_batch(
            store,
            namespace,
            table,
            &schema,
            &batch,
            None,
            LakeOp::Create,
            None,
            owner_tenant,
        )
        .map_err(CreateTableError::Other)?;
        self.load_table(namespace, table).ok_or_else(|| {
            CreateTableError::Other(format!(
                "table {namespace}.{table} vanished immediately after create"
            ))
        })
    }

    /// `DropTable`: remove `(namespace, table)` from both this manager's table
    /// store and the Iceberg-REST catalog index. `false` if the table does not
    /// exist OR is not visible to `visibility` — a caller cannot distinguish
    /// "doesn't exist" from "exists but isn't yours" (W04's error-message bar).
    /// The blob-CAS bytes under the table's location are released from the path
    /// index (a real Iceberg catalog similarly only ever unregisters the
    /// pointer; VACUUM/GC of orphaned files is a separate, out-of-band concern
    /// this tier does not model).
    pub fn drop_table(&self, namespace: &str, table: &str, visibility: &LakeVisibility) -> bool {
        let key = (namespace.to_string(), table.to_string());
        let removed = {
            let mut tables = self.tables.lock();
            match tables.get(&key) {
                Some(entry) if visibility.allows(entry.owner_tenant.as_deref()) => {
                    tables.remove(&key);
                    true
                }
                _ => false,
            }
        };
        if removed {
            self.catalog.lock().remove(namespace, table);
            let prefix = format!("{}/", Self::location_for(namespace, table));
            self.paths
                .lock()
                .retain(|path, _| !path.starts_with(&prefix));
        }
        removed
    }

    /// `RenameTable` (`POST /v1/tables/rename`): re-key `(from_ns, from_table)` to
    /// `(to_ns, to_table)` in both this manager's table store and the catalog. No
    /// data files move — Iceberg's `location` is independent of the catalog
    /// identifier, so only the catalog pointer changes, matching real Iceberg
    /// rename semantics. `visibility` gates the SOURCE the same way `load_table_
    /// visible`/`drop_table` do (folded into `SourceNotFound`, never a 403).
    pub fn rename_table(
        &self,
        from_ns: &str,
        from_table: &str,
        to_ns: &str,
        to_table: &str,
        visibility: &LakeVisibility,
    ) -> Result<(), RenameTableError> {
        let from_key = (from_ns.to_string(), from_table.to_string());
        let to_key = (to_ns.to_string(), to_table.to_string());
        let mut tables = self.tables.lock();
        match tables.get(&from_key) {
            Some(entry) if visibility.allows(entry.owner_tenant.as_deref()) => {}
            _ => return Err(RenameTableError::SourceNotFound),
        }
        if from_key != to_key && tables.contains_key(&to_key) {
            return Err(RenameTableError::DestinationExists);
        }
        let mut entry = tables.remove(&from_key).expect("checked above");
        entry.table.namespace = to_ns.to_string();
        entry.table.name = to_table.to_string();
        let ts_ms = lineage::now_ms();
        {
            let mut cat = self.catalog.lock();
            cat.remove(from_ns, from_table);
            entry.table.register_in(&mut cat, ts_ms as i64);
        }
        tables.insert(to_key, entry);
        Ok(())
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
        // CA-15: with `lineage-transport` on, push through every configured
        // transport (HTTP -- byte-identical to the line below, plus Kafka
        // when `EPISTEMIC_GRAPH_LINEAGE_KAFKA_BROKERS` is set). Without it,
        // this stays the exact call a plain `lake` build has always made --
        // a strict superset, never a behavior change for an unconfigured or
        // `lineage-transport`-less deployment (see `lineage_transport`'s
        // module doc, "Migration and rollback").
        #[cfg(feature = "lineage-transport")]
        lineage_transport::configured_transports().push_all(&event);
        #[cfg(not(feature = "lineage-transport"))]
        lineage::maybe_push_http(&event);
    }

    /// The `n` most recent OpenLineage events (newest last) — inspection/tests.
    pub fn recent_lineage(&self, n: usize) -> Vec<Value> {
        let ring = self.lineage.lock();
        ring.iter().rev().take(n).rev().cloned().collect()
    }

    // ── Audit trail (W05, GOC-75-W05) ───────────────────────────────────────────

    /// Record one Iceberg-REST catalog audit event (a denial or a mutation),
    /// mirroring [`Self::push_lineage`]'s pattern: a structured `tracing` line for
    /// real log aggregation, plus a bounded in-process ring so a test — or a
    /// future admin surface — can prove an event actually landed rather than
    /// trusting that a log line was emitted somewhere.
    pub(crate) fn record_audit(&self, event: Value) {
        tracing::info!(target: "epistemic_graph::lake::audit", event = %event, "iceberg-rest catalog audit event");
        let mut ring = self.audit.lock();
        ring.push_back(event);
        while ring.len() > AUDIT_RING_CAP {
            ring.pop_front();
        }
    }

    /// The `n` most recent audit events (newest last) — inspection/tests.
    pub fn recent_audit(&self, n: usize) -> Vec<Value> {
        let ring = self.audit.lock();
        ring.iter().rev().take(n).rev().cloned().collect()
    }
}

/// Turn an arbitrary series id into a safe Iceberg/Delta table name (ASCII
/// alnum/`_`/`-`, anything else → `_`).
///
/// `.` is deliberately NOT in the allowed set (unlike a plain filesystem-safe
/// filter): this tier's namespaces are single-level (module docs, `rest.rs`),
/// and a literal dot inside a bare table name collides with the conventional
/// `namespace.table` qualified-identifier separator every Iceberg-REST client
/// (PyIceberg/Spark/Trino) expects — a series id like `"rest.series1"` must
/// become the table name `rest_series1`, not `rest.series1`.
fn sanitize_table_name(series_id: &str) -> String {
    series_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
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
    fn committed_lsn_ledger_coalesces_successes_without_accepting_holes() {
        let mut ledger = CommittedLsnLedger::default();
        ledger.insert(1);
        ledger.insert(3);
        ledger.insert(2);
        ledger.insert(5);

        assert!(ledger.contains(1));
        assert!(ledger.contains(2));
        assert!(ledger.contains(3));
        assert!(
            !ledger.contains(4),
            "an uncommitted reservation stays a hole"
        );
        assert!(ledger.contains(5));
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

    /// NE-033/NE-049: after a second drain (2 live files) and a SUBSEQUENT
    /// compaction (rewrites to 1 live file at a NEWER LSN), a scoped as-of read
    /// pinned to the LSN recorded right after the second drain must still see
    /// the OLD 2-file historical state, even though `load_table` sees the
    /// compacted 1-file state.  An authenticated REST handler exercises this
    /// manager seam; this test keeps the lower-level historical projection proof
    /// close to the storage owner.
    #[test]
    fn load_table_as_of_returns_historical_state_after_a_later_compaction() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-asof-{}", std::process::id())),
        )
        .unwrap();
        append(&tsdb, "s5", &points(0, 3));
        let mgr = LakeManager::new();
        mgr.drain_series(&s, &tsdb, "s5").unwrap();
        append(&tsdb, "s5", &points(3, 3));
        let r2 = mgr.drain_series(&s, &tsdb, "s5").unwrap().unwrap();
        let historical_lsn = r2.lsn;

        let table = sanitize_table_name("s5");

        // Compact AFTER recording the as-of point: current state moves to 1 file at a
        // strictly newer lsn than `historical_lsn`.
        let report = mgr
            .compact(&s, DEFAULT_NAMESPACE, &table)
            .unwrap()
            .expect("compaction produced a rewrite");
        assert!(report.lsn > historical_lsn);

        let now = mgr.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_eq!(
            now["metadata"]["snapshots"][0]["summary"]["total-data-files"], "1",
            "the CURRENT view is post-compaction: one live file"
        );

        let historical = mgr
            .load_table_as_of(
                DEFAULT_NAMESPACE,
                &table,
                historical_lsn,
                &LakeVisibility::Unfiltered,
            )
            .expect("historical lsn was committed")
            .expect("table known and visible, as-of resolves to a snapshot");
        assert_eq!(
            historical["metadata"]["snapshots"][0]["summary"]["total-data-files"], "2",
            "as-of the pre-compaction lsn, BOTH original files are still visible \
             — a real historical read, not the current projection"
        );
        assert_eq!(
            historical["metadata"]["snapshots"][0]["summary"]["epistemic-graph-lsn"],
            historical_lsn.to_string(),
        );
        assert_eq!(
            historical["metadata"]["current-snapshot-id"],
            historical_lsn as i64
        );

        // Unknown table ⇒ None, not an error.
        assert!(mgr
            .load_table_as_of(
                DEFAULT_NAMESPACE,
                "nope",
                historical_lsn,
                &LakeVisibility::Unfiltered,
            )
            .expect("unknown tables are not an as-of error")
            .is_none());
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

        // `metadata.schemas` accumulates the table's FULL schema-evolution history in
        // id order (INT-P2-4) — index 0 is always the ORIGINAL schema, unchanged by
        // any later evolution. The active one is whichever entry's own `schema-id`
        // matches `current-schema-id` (the same lookup a real Iceberg reader does),
        // not a fixed array index.
        fn current_schema_field_count(loaded: &Value) -> usize {
            let current_id = &loaded["metadata"]["current-schema-id"];
            loaded["metadata"]["schemas"]
                .as_array()
                .unwrap()
                .iter()
                .find(|schema| &schema["schema-id"] == current_id)
                .expect("current-schema-id names a schema present in schemas[]")["fields"]
                .as_array()
                .unwrap()
                .len()
        }

        let loaded = mgr.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        // Not yet re-materialized, so the catalog's schema is still pre-evolution
        // (evolution affects the IN-MEMORY LakeTable schema for the NEXT write) —
        // confirm a subsequent compaction reflects the wider schema.
        let fields_before = current_schema_field_count(&loaded);
        mgr.compact(&s, DEFAULT_NAMESPACE, &table).unwrap();
        let loaded_after = mgr.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        let fields_after = current_schema_field_count(&loaded_after);
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

    /// CA-15 P9 negative slice: with `lineage-transport` on, no
    /// `EPISTEMIC_GRAPH_OPENLINEAGE_URL` and no
    /// `EPISTEMIC_GRAPH_LINEAGE_KAFKA_BROKERS` configured (both transports
    /// effectively unreachable/unconfigured), `drain_series` still succeeds
    /// and the event still lands in the local ring — the drop is silent at
    /// the transport layer, never fabricated OR lost at the materialization
    /// layer (`lineage.rs:212-214`'s "must never fail or block" invariant,
    /// extended to the composed transport set).
    #[cfg(feature = "lineage-transport")]
    #[test]
    fn materialize_succeeds_with_lineage_transport_enabled_and_every_transport_unconfigured() {
        // See `lineage_transport::tests`' doc on the same lock: this test
        // also mutates the process-global lineage env vars, so it joins the
        // same mutual-exclusion group (`crate::crypto::acquire_test_env_lock_blocking`)
        // rather than racing them.
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        std::env::remove_var(lineage::OPENLINEAGE_URL_ENV);
        #[cfg(feature = "lineage-transport-kafka")]
        std::env::remove_var(lineage_transport::KafkaTransport::ENV_BROKERS);

        let s = store();
        let tsdb = SeriesStore::open_in_dir(&std::env::temp_dir().join(format!(
            "eg-lake-test-lineage-unconfigured-{}",
            std::process::id()
        )))
        .unwrap();
        append(&tsdb, "s5", &points(0, 2));
        let mgr = LakeManager::new();

        let report = mgr
            .drain_series(&s, &tsdb, "s5")
            .unwrap()
            .expect("materialization succeeds even though every lineage transport is unconfigured");
        assert_eq!(report.op, LakeOp::Create);

        // The event was still built and ring-buffered -- "silently dropped
        // at the transport" never means "never recorded at all".
        let events = mgr.recent_lineage(10);
        assert_eq!(events.len(), 1);
        assert!(events[0]["run"]["runId"].is_string());
    }
}
