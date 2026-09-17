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

mod catalog_ops;
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
use serde_json::Value;

use eg_lake::catalog::IcebergRestCatalog;
use eg_lake::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
use eg_lake::snapshot::Lsn;
use eg_lake::LakeTable;
use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;

use crate::server::blob::store::ChunkStore;

use catalog_ops::{
    prepare_materialization, publish_artifacts, rollback_artifacts, stage_artifacts,
};

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
#[derive(Clone)]
struct TableEntry {
    table: LakeTable,
    source_series: Option<String>,
    /// Optimistic publication token. Every mutation of `table` advances this even
    /// when the mutation does not advance the materialization LSN (schema evolution).
    mutation_revision: u64,
    /// `None` = engine-internal/system table (e.g. drained straight from a tsdb
    /// series by the materialization sweep) — visible to every authenticated
    /// caller, matching this tier's behavior before W04. `Some(owner_scope)` = a
    /// table created through the authenticated Iceberg-REST `CreateTable` path,
    /// tagged with its creating carrier's `CarrierAuthority::owner_scope()` (the
    /// SAME per-agent ownership key `GraphReadAuthority`'s row-level security
    /// already uses elsewhere in this engine — see [`LakeVisibility`]).
    owner_tenant: Option<String>,
}

/// One materialization request. Grouping the table identity, row batch, lineage
/// hint, and optional ownership metadata keeps the write boundary explicit as the
/// pipeline grows.
struct MaterializeInput<'a> {
    namespace: &'a str,
    table: &'a str,
    schema: &'a LakeSchema,
    batch: &'a LakeBatch,
    source_series: Option<&'a str>,
    op_hint: LakeOp,
    input_dataset: Option<(&'a str, &'a str)>,
    owner_tenant: Option<&'a str>,
    /// Existing live files retired by this same atomic materialization LSN.
    retire_paths: &'a [String],
    /// Source materialization LSN a read-modify-write must still observe at publication.
    expected_lsn: Option<Lsn>,
    /// Complete table mutation revision the candidate must still observe at publication.
    expected_revision: Option<u64>,
}

fn materialization_base(
    tables: &HashMap<(String, String), TableEntry>,
    key: &(String, String),
    input: &MaterializeInput<'_>,
) -> Result<(Option<TableEntry>, Option<u64>), String> {
    let existing = tables.get(key);
    let observed_lsn = existing.map(|entry| entry.table.current_lsn());
    let observed_revision = existing.map(|entry| entry.mutation_revision);
    if (input.expected_lsn.is_some() && input.expected_lsn != observed_lsn)
        || (input.expected_revision.is_some() && input.expected_revision != observed_revision)
    {
        return Err(format!(
            "lake write conflict for {}.{}: expected source LSN/revision {:?}/{:?}, observed {:?}/{:?}",
            input.namespace,
            input.table,
            input.expected_lsn,
            input.expected_revision,
            observed_lsn,
            observed_revision
        ));
    }
    Ok((
        existing.cloned(),
        input.expected_revision.or(observed_revision),
    ))
}

fn publication_conflict(
    input: &MaterializeInput<'_>,
    expected_revision: Option<u64>,
    observed_revision: Option<u64>,
) -> Option<String> {
    if observed_revision == expected_revision {
        return None;
    }
    Some(format!(
        "lake write conflict for {}.{}: expected source revision {:?}, observed {:?}",
        input.namespace, input.table, expected_revision, observed_revision
    ))
}

fn rollback_publication_conflict(
    store: &dyn ChunkStore,
    staged_artifacts: &[catalog_ops::StagedArtifact],
    conflict: String,
) -> Result<MaterializeReport, String> {
    match rollback_artifacts(store, staged_artifacts) {
        Ok(()) => Err(conflict),
        Err(cleanup) => Err(format!("{conflict}; rollback failed: {cleanup}")),
    }
}

fn materialization_op(is_new: bool, op_hint: LakeOp) -> LakeOp {
    if is_new {
        LakeOp::Create
    } else {
        op_hint
    }
}

fn evolve_entry_schema(entry: &mut TableEntry, field: LakeField) -> bool {
    let evolved = entry.table.evolve_add_column(field);
    entry.mutation_revision = entry.mutation_revision.wrapping_add(u64::from(evolved));
    evolved
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
    /// manager's history. LSN 0 is a valid global boundary, represented by
    /// `Ok(None)` because no table generation exists there.
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
    /// global LSN can resolve to that table's latest earlier emitted snapshot,
    /// while a reservation from a failed write must never be accepted.
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

    /// Materialize one write (a fresh batch of rows) into `(namespace, table)`,
    /// persisting the Parquet file + the Delta log + the Iceberg metadata/Avro
    /// manifests to the blob CAS, registering the table into the catalog, and emitting
    /// an OpenLineage event. `op` should be `Append` for an incremental add — CREATE is
    /// detected automatically for a table's first write.
    fn materialize_batch(
        &self,
        store: &dyn ChunkStore,
        input: MaterializeInput<'_>,
    ) -> Result<MaterializeReport, String> {
        let lsn = self.alloc_lsn();
        let ts_ms = lineage::now_ms();
        let key = (input.namespace.to_string(), input.table.to_string());
        let (existing, expected_revision) = {
            let tables = self.tables.lock();
            materialization_base(&tables, &key, &input)?
        };
        let is_new = existing.is_none();
        let prepared = prepare_materialization(&input, existing.as_ref(), lsn, ts_ms as i64)?;
        let staged_artifacts = stage_artifacts(store, &prepared.artifacts)?;

        // Publishing is intentionally after every fallible write. From this point
        // onward only in-memory inserts remain, so readers see the previous table or
        // the complete candidate and never a half-written snapshot.
        let mut tables = self.tables.lock();
        let observed_revision = tables.get(&key).map(|entry| entry.mutation_revision);
        if let Some(conflict) = publication_conflict(&input, expected_revision, observed_revision) {
            drop(tables);
            return rollback_publication_conflict(store, &staged_artifacts, conflict);
        }
        publish_artifacts(&self.paths, staged_artifacts);
        let mut cat = self.catalog.lock();
        prepared.candidate.table.register_in(&mut cat, ts_ms as i64);
        tables.insert(key, prepared.candidate);
        self.record_committed_lsn(lsn);
        drop(cat);
        drop(tables);

        let op = materialization_op(is_new, input.op_hint);
        let event = lineage::build_run_event(
            input.namespace,
            input.table,
            op,
            input.schema,
            prepared.num_rows,
            prepared.bytes_len,
            &prepared.location,
            lsn.value(),
            prepared.snapshot_id,
            input.input_dataset,
        );
        self.push_lineage(event.clone());

        Ok(MaterializeReport {
            namespace: input.namespace.to_string(),
            table: input.table.to_string(),
            op,
            path: prepared.rel_path,
            bytes_len: prepared.bytes_len,
            num_rows: prepared.num_rows,
            lsn: lsn.value(),
            metadata_location: prepared.metadata_location,
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
            MaterializeInput {
                namespace: DEFAULT_NAMESPACE,
                table: &table,
                schema: &schema,
                batch: &batch,
                source_series: Some(series_id),
                op_hint: LakeOp::Append,
                input_dataset: Some(("epistemic-graph.tsdb", series_id)),
                owner_tenant: None,
                retire_paths: &[],
                expected_lsn: None,
                expected_revision: None,
            },
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
        let (schema, location, live_paths, source_series, source_lsn, source_revision) = {
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
                entry.table.current_lsn(),
                entry.mutation_revision,
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

        let op = if had_rows {
            LakeOp::Overwrite
        } else {
            LakeOp::Truncate
        };
        let report = self.materialize_batch(
            store,
            MaterializeInput {
                namespace,
                table,
                schema: &schema,
                batch: &new_batch,
                source_series: source_series.as_deref(),
                op_hint: op,
                input_dataset: None,
                owner_tenant: None,
                retire_paths: &live_paths,
                expected_lsn: Some(source_lsn),
                expected_revision: Some(source_revision),
            },
        )?;
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
        Ok(evolve_entry_schema(entry, field))
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
    use crate::server::blob::store::{BlobManifest, RedbChunkStore, SweepStats};

    struct FailureState {
        fail_on_manifest: Option<usize>,
        fail_on_incref: Option<usize>,
        fail_on_decref: Option<usize>,
        pause_manifest: Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
        manifest_calls: usize,
        incref_calls: usize,
        decref_calls: usize,
        refcount_baselines: HashMap<String, u64>,
    }

    struct FailingStore {
        inner: RedbChunkStore,
        state: std::sync::Mutex<FailureState>,
    }

    impl FailingStore {
        fn new() -> Self {
            Self {
                inner: store(),
                state: std::sync::Mutex::new(FailureState {
                    fail_on_manifest: None,
                    fail_on_incref: None,
                    fail_on_decref: None,
                    pause_manifest: None,
                    manifest_calls: 0,
                    incref_calls: 0,
                    decref_calls: 0,
                    refcount_baselines: HashMap::new(),
                }),
            }
        }

        fn fail_artifact(&self, artifact_ordinal: usize) {
            let mut state = self.state.lock().unwrap();
            state.fail_on_manifest = Some(artifact_ordinal);
            state.fail_on_incref = None;
            state.fail_on_decref = None;
            state.pause_manifest = None;
            state.manifest_calls = 0;
            state.incref_calls = 0;
            state.decref_calls = 0;
            state.refcount_baselines.clear();
        }

        fn fail_incref(&self, artifact_ordinal: usize) {
            let mut state = self.state.lock().unwrap();
            state.fail_on_manifest = None;
            state.fail_on_incref = Some(artifact_ordinal);
            state.fail_on_decref = None;
            state.pause_manifest = None;
            state.manifest_calls = 0;
            state.incref_calls = 0;
            state.decref_calls = 0;
            state.refcount_baselines.clear();
        }

        fn fail_manifest_and_decref(&self, artifact_ordinal: usize) {
            self.fail_artifact(artifact_ordinal);
            self.state.lock().unwrap().fail_on_decref = Some(1);
        }

        fn pause_next_manifest(
            &self,
        ) -> (
            std::sync::mpsc::Receiver<()>,
            std::sync::mpsc::SyncSender<()>,
        ) {
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            self.state.lock().unwrap().pause_manifest = Some((entered_tx, release_rx));
            (entered_rx, release_tx)
        }

        fn disarm(&self) {
            let mut state = self.state.lock().unwrap();
            state.fail_on_manifest = None;
            state.fail_on_incref = None;
            state.fail_on_decref = None;
            state.pause_manifest = None;
            state.refcount_baselines.clear();
        }

        fn assert_staged_refs_rolled_back(&self) {
            let baselines = self.state.lock().unwrap().refcount_baselines.clone();
            for (digest, baseline) in baselines {
                assert_eq!(
                    self.inner.refcount(&digest).unwrap(),
                    baseline,
                    "failed lake commit leaked a CAS reference for {digest}"
                );
            }
        }

        fn repair_staged_refs(&self) {
            let baselines = self.state.lock().unwrap().refcount_baselines.clone();
            for (digest, baseline) in baselines {
                while self.inner.refcount(&digest).unwrap() > baseline {
                    self.inner.decref(&digest).unwrap();
                }
            }
        }

        fn staged_ref_mismatches(&self) -> usize {
            let baselines = self.state.lock().unwrap().refcount_baselines.clone();
            baselines
                .iter()
                .filter(|(digest, baseline)| self.inner.refcount(digest).unwrap() != **baseline)
                .count()
        }
    }

    impl ChunkStore for FailingStore {
        fn put_chunk(&self, bytes: &[u8]) -> Result<(String, bool), String> {
            self.inner.put_chunk(bytes)
        }

        fn get_chunk(&self, digest: &str) -> Result<Option<Vec<u8>>, String> {
            self.inner.get_chunk(digest)
        }

        fn put_manifest(&self, digest: &str, manifest: &BlobManifest) -> Result<(), String> {
            let pause = self.state.lock().unwrap().pause_manifest.take();
            if let Some((entered, release)) = pause {
                entered
                    .send(())
                    .map_err(|error| format!("signal paused lake write: {error}"))?;
                release
                    .recv_timeout(crate::test_rendezvous::RENDEZVOUS_TIMEOUT)
                    .map_err(|error| format!("resume paused lake write: {error}"))?;
            }
            let should_fail = {
                let mut state = self.state.lock().unwrap();
                state.manifest_calls += 1;
                state.fail_on_manifest == Some(state.manifest_calls)
            };
            if should_fail {
                Err("injected lake artifact failure".to_string())
            } else {
                self.inner.put_manifest(digest, manifest)
            }
        }

        fn get_manifest(&self, digest: &str) -> Result<Option<BlobManifest>, String> {
            self.inner.get_manifest(digest)
        }

        fn incref(&self, digest: &str) -> Result<u64, String> {
            let (should_fail, track_baseline) = {
                let mut state = self.state.lock().unwrap();
                state.incref_calls += 1;
                (
                    state.fail_on_incref == Some(state.incref_calls),
                    state.fail_on_manifest.is_some()
                        || state.fail_on_incref.is_some()
                        || state.fail_on_decref.is_some(),
                )
            };
            if should_fail {
                return Err("injected lake incref failure".to_string());
            }
            let baseline = self.inner.refcount(digest)?;
            if track_baseline {
                let mut state = self.state.lock().unwrap();
                state
                    .refcount_baselines
                    .entry(digest.to_string())
                    .or_insert(baseline);
            }
            self.inner.incref(digest)
        }

        fn decref(&self, digest: &str) -> Result<u64, String> {
            let should_fail = {
                let mut state = self.state.lock().unwrap();
                state.decref_calls += 1;
                state.fail_on_decref == Some(state.decref_calls)
            };
            if should_fail {
                Err("injected lake decref failure".to_string())
            } else {
                self.inner.decref(digest)
            }
        }

        fn refcount(&self, digest: &str) -> Result<u64, String> {
            self.inner.refcount(digest)
        }

        fn sweep(&self) -> Result<SweepStats, String> {
            self.inner.sweep()
        }

        fn chunk_count(&self) -> Result<u64, String> {
            self.inner.chunk_count()
        }

        fn blob_count(&self) -> Result<u64, String> {
            self.inner.blob_count()
        }
    }

    fn store() -> RedbChunkStore {
        RedbChunkStore::open_temp().unwrap()
    }

    const TEST_BUCKET_NS: u64 = 3_600_000_000_000;
    const FAILURE_PHASES: &[(usize, &str)] = &[
        (1, "data"),
        (2, "delta"),
        (3, "manifest"),
        (4, "manifest-list"),
        (5, "metadata"),
    ];

    fn points(from: i64, n: i64) -> Vec<Point> {
        (0..n)
            .map(|i| Point::single(from + i, (from + i) as f64 * 1.5))
            .collect()
    }

    fn append(tsdb: &SeriesStore, series_id: &str, pts: &[Point]) {
        tsdb.append_batch(series_id, 1, TEST_BUCKET_NS, &["v".to_string()], pts)
            .unwrap();
    }

    fn current_snapshot(response: &Value) -> &Value {
        let current_id = &response["metadata"]["current-snapshot-id"];
        response["metadata"]["snapshots"]
            .as_array()
            .expect("Iceberg snapshots array")
            .iter()
            .find(|snapshot| &snapshot["snapshot-id"] == current_id)
            .expect("current-snapshot-id references an emitted snapshot")
    }

    fn assert_manifest_lists_are_published(manager: &LakeManager, response: &Value) {
        let paths = manager.paths.lock();
        for snapshot in response["metadata"]["snapshots"]
            .as_array()
            .expect("Iceberg snapshots array")
        {
            let manifest_list = snapshot["manifest-list"]
                .as_str()
                .expect("snapshot manifest-list path");
            assert!(
                paths.contains_key(manifest_list),
                "snapshot references unpublished manifest list {manifest_list}"
            );
        }
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
    fn failed_create_never_publishes_a_table_or_artifact_path() {
        let store = FailingStore::new();
        let schema = LakeSchema::new(vec![LakeField::new("v", LakeType::Double)]);
        for &(artifact_ordinal, phase) in FAILURE_PHASES {
            let manager = LakeManager::new();
            store.fail_artifact(artifact_ordinal);
            let result = manager.create_table(
                &store,
                DEFAULT_NAMESPACE,
                "failure-injected-create",
                schema.clone(),
                None,
            );
            assert!(result.is_err(), "{phase} must fail create");
            assert!(manager
                .load_table(DEFAULT_NAMESPACE, "failure-injected-create")
                .is_none());
            assert!(manager.paths.lock().is_empty());
            assert!(manager.recent_lineage(1).is_empty());
            store.assert_staged_refs_rolled_back();
        }
        store.disarm();
    }

    #[test]
    fn incref_and_rollback_failures_are_visible_without_publishing_state() {
        let store = FailingStore::new();
        let schema = LakeSchema::new(vec![LakeField::new("v", LakeType::Double)]);

        let incref_manager = LakeManager::new();
        store.fail_incref(3);
        let incref_error = match incref_manager.create_table(
            &store,
            DEFAULT_NAMESPACE,
            "incref-failure",
            schema.clone(),
            None,
        ) {
            Err(CreateTableError::Other(error)) => error,
            other => panic!("expected injected incref failure, got {other:?}"),
        };
        assert!(incref_error.contains("injected lake incref failure"));
        assert!(incref_manager.paths.lock().is_empty());
        assert!(incref_manager
            .load_table(DEFAULT_NAMESPACE, "incref-failure")
            .is_none());
        store.assert_staged_refs_rolled_back();

        let rollback_manager = LakeManager::new();
        store.fail_manifest_and_decref(4);
        let rollback_error = match rollback_manager.create_table(
            &store,
            DEFAULT_NAMESPACE,
            "rollback-failure",
            schema,
            None,
        ) {
            Err(CreateTableError::Other(error)) => error,
            other => panic!("expected injected rollback failure, got {other:?}"),
        };
        assert!(rollback_error.contains("rollback failed"));
        assert!(rollback_error.contains("injected lake decref failure"));
        assert!(rollback_manager.paths.lock().is_empty());
        assert!(rollback_manager
            .load_table(DEFAULT_NAMESPACE, "rollback-failure")
            .is_none());
        assert_eq!(store.staged_ref_mismatches(), 1);
        store.repair_staged_refs();
        store.assert_staged_refs_rolled_back();
        store.disarm();
    }

    #[test]
    fn failed_append_and_rewrite_preserve_the_previous_complete_snapshot() {
        let store = FailingStore::new();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!(
                "eg-lake-test-atomic-failure-{}",
                std::process::id()
            )),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
        )
        .unwrap();
        let manager = LakeManager::new();
        let series = "atomic-failure";
        append(&tsdb, series, &points(0, 3));
        let first = manager
            .drain_series(&store, &tsdb, series)
            .unwrap()
            .unwrap();
        let table = sanitize_table_name(series);
        let first_view = manager.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        let first_path_count = manager.paths.lock().len();
        append(&tsdb, series, &points(3, 2));

        for &(artifact_ordinal, phase) in FAILURE_PHASES {
            store.fail_artifact(artifact_ordinal);
            assert!(manager.drain_series(&store, &tsdb, series).is_err());
            assert_eq!(
                manager.load_table(DEFAULT_NAMESPACE, &table).unwrap(),
                first_view,
                "failed {phase} append changed the live table"
            );
            assert_eq!(manager.paths.lock().len(), first_path_count);
            assert_eq!(manager.recent_lineage(10).len(), 1);
            assert_manifest_lists_are_published(&manager, &first_view);
            store.assert_staged_refs_rolled_back();
        }

        store.disarm();
        let second = manager
            .drain_series(&store, &tsdb, series)
            .unwrap()
            .unwrap();
        assert!(
            second.lsn > first.lsn + 5,
            "failed reservations remain holes"
        );
        let second_view = manager.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_manifest_lists_are_published(&manager, &second_view);
        let before_rewrite = second_view.clone();
        let before_rewrite_path_count = manager.paths.lock().len();

        for &(artifact_ordinal, phase) in FAILURE_PHASES {
            store.fail_artifact(artifact_ordinal);
            assert!(manager.compact(&store, DEFAULT_NAMESPACE, &table).is_err());
            assert_eq!(
                manager.load_table(DEFAULT_NAMESPACE, &table).unwrap(),
                before_rewrite,
                "failed {phase} rewrite changed the live table"
            );
            assert_eq!(manager.paths.lock().len(), before_rewrite_path_count);
            assert_eq!(manager.recent_lineage(10).len(), 2);
            assert_manifest_lists_are_published(&manager, &before_rewrite);
            store.assert_staged_refs_rolled_back();
        }

        store.disarm();
        let rewrite = manager
            .compact(&store, DEFAULT_NAMESPACE, &table)
            .unwrap()
            .unwrap();
        let rewritten = manager.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_eq!(
            current_snapshot(&rewritten)["snapshot-id"],
            rewrite.lsn as i64
        );
        assert_eq!(
            current_snapshot(&rewritten)["summary"]["total-data-files"],
            "1"
        );
        assert_manifest_lists_are_published(&manager, &rewritten);
        let emitted_ids: Vec<u64> = rewritten["metadata"]["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|snapshot| snapshot["snapshot-id"].as_u64().unwrap())
            .collect();
        assert_eq!(emitted_ids, vec![first.lsn, second.lsn, rewrite.lsn]);
    }

    #[test]
    fn concurrent_append_fences_a_stale_delete_until_it_retries() {
        let store = std::sync::Arc::new(FailingStore::new());
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!(
                "eg-lake-test-delete-interleave-{}",
                std::process::id()
            )),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
        )
        .unwrap();
        let manager = std::sync::Arc::new(LakeManager::new());
        let series = "delete-interleave";
        let table = sanitize_table_name(series);
        append(&tsdb, series, &points(0, 3));
        let first = manager
            .drain_series(store.as_ref(), &tsdb, series)
            .unwrap()
            .unwrap();

        // The append exists in the source while the lake delete snapshots only the
        // first file. Pause that stale delete during object I/O, then let the append
        // publish before the delete reaches its expected-LSN publication fence.
        append(&tsdb, series, &points(3, 2));
        let (delete_entered, release_delete) = store.pause_next_manifest();
        let delete_manager = std::sync::Arc::clone(&manager);
        let delete_store = std::sync::Arc::clone(&store);
        let delete_table = table.clone();
        let stale_delete = std::thread::spawn(move || {
            delete_manager.delete_where(
                delete_store.as_ref(),
                DEFAULT_NAMESPACE,
                &delete_table,
                |_row| false,
            )
        });
        delete_entered
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("stale delete reached object publication");
        let concurrent_append = manager
            .drain_series(store.as_ref(), &tsdb, series)
            .unwrap()
            .unwrap();
        let path_count_after_append = manager.paths.lock().len();
        release_delete.send(()).expect("release stale delete");
        let conflict =
            crate::test_rendezvous::join_bounded(stale_delete, "the stale delete thread")
                .expect_err("stale delete must fail its expected-LSN fence");
        assert!(conflict.contains("lake write conflict"));

        let after_conflict = manager.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_eq!(manager.paths.lock().len(), path_count_after_append);
        assert_eq!(
            current_snapshot(&after_conflict)["summary"]["total-data-files"],
            "2",
            "the stale delete must not overwrite the concurrent append"
        );
        assert_eq!(
            current_snapshot(&after_conflict)["summary"]["total-records"],
            "5"
        );
        let after_conflict_ids: Vec<u64> = after_conflict["metadata"]["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|snapshot| snapshot["snapshot-id"].as_u64().unwrap())
            .collect();
        assert_eq!(after_conflict_ids, vec![first.lsn, concurrent_append.lsn]);

        // A fresh delete retries from the new two-file snapshot and commits at an
        // LSN after the append, so the append cannot escape a successful delete.
        let retry = manager
            .delete_where(store.as_ref(), DEFAULT_NAMESPACE, &table, |_row| false)
            .unwrap()
            .unwrap();
        assert!(retry.lsn > concurrent_append.lsn);
        let deleted = manager.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_eq!(current_snapshot(&deleted)["summary"]["total-records"], "0");
        let emitted_ids: Vec<u64> = deleted["metadata"]["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|snapshot| snapshot["snapshot-id"].as_u64().unwrap())
            .collect();
        assert_eq!(
            emitted_ids,
            vec![first.lsn, concurrent_append.lsn, retry.lsn]
        );
    }

    #[test]
    fn schema_evolution_fences_a_paused_materialization_candidate() {
        let store = std::sync::Arc::new(FailingStore::new());
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!(
                "eg-lake-test-evolve-interleave-{}",
                std::process::id()
            )),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
        )
        .unwrap();
        let manager = std::sync::Arc::new(LakeManager::new());
        let series = "evolve-interleave";
        let table = sanitize_table_name(series);
        append(&tsdb, series, &points(0, 3));
        manager
            .drain_series(store.as_ref(), &tsdb, series)
            .unwrap()
            .unwrap();
        let initial_catalog = manager.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        let initial_path_count = manager.paths.lock().len();

        let (write_entered, release_write) = store.pause_next_manifest();
        let write_manager = std::sync::Arc::clone(&manager);
        let write_store = std::sync::Arc::clone(&store);
        let write_table = table.clone();
        let stale_compaction = std::thread::spawn(move || {
            write_manager.compact(write_store.as_ref(), DEFAULT_NAMESPACE, &write_table)
        });
        write_entered
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("compaction reached object publication");
        assert!(manager
            .evolve_add_column(
                DEFAULT_NAMESPACE,
                &table,
                LakeField::new("note", LakeType::String),
            )
            .unwrap());
        release_write.send(()).expect("release stale compaction");
        let conflict =
            crate::test_rendezvous::join_bounded(stale_compaction, "the stale compaction thread")
                .expect_err("pre-evolution candidate must fail its revision fence");
        assert!(conflict.contains("lake write conflict"));
        assert_eq!(manager.paths.lock().len(), initial_path_count);
        assert_eq!(
            manager.load_table(DEFAULT_NAMESPACE, &table).unwrap(),
            initial_catalog,
            "failed stale publication must preserve the prior emitted catalog"
        );
        assert!(!manager
            .evolve_add_column(
                DEFAULT_NAMESPACE,
                &table,
                LakeField::new("note", LakeType::String),
            )
            .unwrap());

        manager
            .compact(store.as_ref(), DEFAULT_NAMESPACE, &table)
            .unwrap()
            .expect("retry compacts from evolved state");
        let evolved = manager.load_table(DEFAULT_NAMESPACE, &table).unwrap();
        assert_eq!(evolved["metadata"]["current-schema-id"], 1);
        assert_eq!(evolved["metadata"]["schemas"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn drain_series_materializes_only_new_points_each_call() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-{}", std::process::id())),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
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
            current_snapshot(&loaded)["summary"]["total-data-files"],
            "2"
        );
    }

    #[test]
    fn compact_merges_live_files_into_one_and_preserves_rows() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-compact-{}", std::process::id())),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
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
            current_snapshot(&loaded_before)["summary"]["total-data-files"],
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
            current_snapshot(&loaded_after)["summary"]["total-data-files"],
            "1",
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
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
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
            current_snapshot(&now)["summary"]["total-data-files"],
            "1",
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
            current_snapshot(&historical)["summary"]["total-data-files"],
            "2",
            "as-of the pre-compaction lsn, BOTH original files are still visible \
             — a real historical read, not the current projection"
        );
        assert_eq!(
            current_snapshot(&historical)["summary"]["epistemic-graph-lsn"],
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
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
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
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
        )
        .unwrap();
        append(&tsdb, "s3", &points(0, 2));
        let mgr = LakeManager::new();
        let first = mgr
            .drain_series(&s, &tsdb, "s3")
            .unwrap()
            .expect("first materialization");
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

        let historical = mgr
            .load_table_as_of(
                DEFAULT_NAMESPACE,
                &table,
                first.lsn,
                &LakeVisibility::Unfiltered,
            )
            .expect("first LSN is committed")
            .expect("table existed at its first emitted generation");
        assert_eq!(historical["metadata"]["current-schema-id"], 0);
        let historical_schemas = historical["metadata"]["schemas"].as_array().unwrap();
        assert_eq!(historical_schemas.len(), 1);
        assert_eq!(
            historical_schemas[0]["fields"].as_array().unwrap().len(),
            fields_before
        );
        let metadata_path = historical["metadata-location"].as_str().unwrap();
        let emitted_metadata: Value = serde_json::from_slice(
            &mgr.read_path_bytes(&s, metadata_path)
                .expect("historical metadata path was published"),
        )
        .expect("historical metadata is JSON");
        assert_eq!(historical["metadata"], emitted_metadata);
    }

    #[test]
    fn recent_lineage_carries_openlineage_shaped_events() {
        let s = store();
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-test-lineage-{}", std::process::id())),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
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
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!(
                "eg-lake-test-lineage-unconfigured-{}",
                std::process::id()
            )),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
        )
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
