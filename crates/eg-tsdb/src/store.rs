//! Time-partitioned series store over redb (CONCEPT:AU-KG.retrieval.god-nodes-communities) — the lean / Pi path.
//!
//! ## Layout (the storage thesis)
//!
//! The engine already keys its redb tables by composite tuples that redb
//! range-scans natively:
//! ```text
//! NODES: TableDefinition<(&str, &str), &[u8]>            // (graph, node_id)
//! EDGES: TableDefinition<(&str, &str, &str, u32), &[u8]> // (graph, src, dst, ord)
//! LEDGER: TableDefinition<(&str, u64), &str>             // (graph, seq)
//! ```
//! A time series is the SAME idea with `(scoped_series_key, bucket_start)` as the key, so
//! it lives in the SAME redb `Database` family next to nodes/edges with NO new
//! storage engine. The two `TableDefinition`s below are the canonical schema; the
//! facade's `src/server/persistence/redb_backend.rs` references them so they "live
//! with the other tables" while the store/query logic stays here in `eg-tsdb`.
//!
//! ```text
//! SERIES_CHUNKS: TableDefinition<(&str, u64), &[u8]>  // (tenant+graph+series, bucket_start_ns) -> packed chunk
//! SERIES_META:   TableDefinition<&str, &[u8]>          // tenant+graph+series -> SeriesMeta (msgpack)
//! ```
//!
//! ### Why chunks (the append-amortization decision)
//! One redb entry per point would mean one B-tree insert per 8-byte value —
//! per-key overhead dominates and append throughput/footprint both suffer. Instead
//! each key is a **time bucket** (e.g. 1h of wall-clock); its value is a packed
//! columnar chunk of every point whose ts falls in that bucket:
//! `[u32 n | u8 n_fields | i64 ts[n] | f64 vals[n*n_fields]]`. Append
//! read-modify-writes the touched bucket(s) in ONE write txn per batch; a
//! time-range scan is a redb RANGE over the covering bucket keys then a linear
//! walk of each decoded chunk. This is the LSM-of-chunks shape a TSDB wants,
//! expressed in redb's existing range contract. Multi-field (OHLCV) is just
//! `n_fields > 1` f64 columns per point.
//!
//! ### Durability
//! The store opens `{persist_dir}/series.redb` — a SEPARATE redb file from the
//! engine's authoritative graph shard, because redb holds an EXCLUSIVE per-process file lock
//! (two handles on one file error "Database already open"). A separate file in the
//! same persist dir gets the same group-commit/fsync durability while staying
//! independent of the graph writer's lock. In-memory deployments (no persist dir)
//! open a temp file.

#![cfg(feature = "redb-store")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use eg_storage::{
    OwnedStoreHandle, PhysicalStoreIdentity, ScopeGrantVerifier, ScopedRead, StorageKernelV1,
    TimeSeriesOwner,
};
use eg_transaction::{AdmittedOwnerWrite, Begin, MutationKernelV1};
use eg_types::mutation_batch::COMPILED_BATCH_INCARNATION;
use eg_types::MutationBatch;
use redb::{
    ReadOnlyTable, ReadTransaction, ReadableTable, Table, TableDefinition, TableError,
    WriteTransaction,
};
use serde::de::DeserializeOwned;

pub use crate::point::{Point, Ts, TsError};

/// `(series_id, bucket_start_ns)` -> packed chunk blob. Same composite-key shape as
/// the engine's `NODES`/`EDGES` tables. CANONICAL schema for the series store; the
/// facade's `redb_backend.rs` references this so the durable tier owns the name.
pub const SERIES_CHUNKS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("series_chunks");
/// `series_id` -> msgpack [`SeriesMeta`]. CANONICAL schema (see `SERIES_CHUNKS`).
pub const SERIES_META: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("series_meta");
/// Scoped series key -> durable projection cursor/health. This lets startup
/// reconciliation skip already-converged projections without a full point scan and
/// makes a crash-window catch-up state observable after restart.
pub const PROJECTION_STATE: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("series_projection_state");

/// Versioned prefix for the canonical tenant-safe series key. Length-prefixing every
/// component makes collisions impossible even when names contain separators.
pub const SCOPED_KEY_PREFIX: &str = "egts2:";

type Result<T> = std::result::Result<T, TsError>;

const MAX_TS_STORED_META_BYTES: usize = 4 * 1024 * 1024;
const MAX_TS_STORED_META_ITEMS: usize = 100_000;
const MAX_TS_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_TS_POINTS_PER_CHUNK: usize = 1_000_000;
const MAX_TS_FIELDS: usize = u8::MAX as usize;
const MAX_TS_FIELD_NAME_BYTES: usize = 4 * 1024;
const MAX_TS_SERIES_KEY_BYTES: usize = 4 * 1024;
const MAX_TS_SERIES_PER_SCAN: usize = 100_000;
const MAX_TS_SERIES_SCAN_BYTES: usize = 64 * 1024 * 1024;
const MAX_TS_QUERY_POINTS: usize = 1_000_000;
const MAX_TS_QUERY_BYTES: usize = 256 * 1024 * 1024;
const MAX_TS_BUCKETS_PER_OPERATION: usize = 1_000_000;
const MAX_TS_OPERATION_BYTES: usize = 256 * 1024 * 1024;
const MAX_TS_PROJECTION_ERROR_BYTES: usize = 64 * 1024;

/// A source of read-only handles on the three series tables.
///
/// Two things legitimately hold those tables: `series.redb`, whose sole physical
/// owner is `eg-storage` under `OwnerLayout::TimeSeries` (reached through a
/// kernel-issued [`ScopedRead`]), and a graph-shard transaction a cross-modal
/// atomic commit already owns (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
/// This trait is how one body of code serves both without eg-tsdb ever opening a
/// database: it only turns an already-authorized transaction into a table handle.
pub trait SeriesTableReader {
    fn open_series_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ReadOnlyTable<K, V>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static;

    /// `None` when the table has never been created.
    ///
    /// Only a caller-owned graph-shard transaction can be in that state (a shard
    /// that has never durably committed a measurement), which reads as "no series
    /// here", not as an error. A kernel-issued [`ScopedRead`] never can: the
    /// storage kernel materializes the whole declared census when the owner file
    /// is created and re-validates it on every open, so a missing table there is a
    /// genuine failure and is reported as one.
    fn open_series_table_if_present<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Option<ReadOnlyTable<K, V>>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static;
}

/// The write-side twin of [`SeriesTableReader`]. See its doc for why both a
/// kernel-issued [`AdmittedOwnerWrite`] and a caller-owned graph-shard
/// `WriteTransaction` implement it.
pub trait SeriesTableWriter {
    fn open_series_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Table<'_, K, V>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static;
}

impl SeriesTableReader for ReadTransaction {
    fn open_series_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ReadOnlyTable<K, V>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.open_table(definition).map_err(redb_err)
    }

    fn open_series_table_if_present<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Option<ReadOnlyTable<K, V>>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        match self.open_table(definition) {
            Ok(table) => Ok(Some(table)),
            Err(TableError::TableDoesNotExist(_)) => Ok(None),
            Err(error) => Err(redb_err(error)),
        }
    }
}

impl SeriesTableReader for ScopedRead<'_, TimeSeriesOwner> {
    fn open_series_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ReadOnlyTable<K, V>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        // The three series tables are OWNER rows whose keys carry no scope
        // component, so the bound is the layout, not the serving scope.
        self.open_owner_table(definition).map_err(redb_err)
    }

    fn open_series_table_if_present<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Option<ReadOnlyTable<K, V>>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        // Always present: see the trait's doc.
        self.open_series_table(definition).map(Some)
    }
}

impl SeriesTableWriter for WriteTransaction {
    fn open_series_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Table<'_, K, V>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.open_table(definition).map_err(redb_err)
    }
}

impl SeriesTableWriter for AdmittedOwnerWrite<'_, TimeSeriesOwner> {
    fn open_series_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Table<'_, K, V>>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.open_table(definition).map_err(redb_err)
    }
}

fn redb_err<E: std::fmt::Display>(e: E) -> TsError {
    TsError::Redb(e.to_string())
}
fn codec_err<E: std::fmt::Display>(e: E) -> TsError {
    TsError::Codec(e.to_string())
}


/// Fixed store-private scope used ONLY to bootstrap the physical `series.redb` root
/// and materialize the `SERIES_CHUNKS`/`SERIES_META`/`PROJECTION_STATE` tables on
/// first open (MutationBatch v1 store-ownership migration — see
/// `crates/eg-core/src/rbac_persist.rs` for the worked reference this mirrors).
/// Unlike `RbacStore`, `SeriesStore` serves arbitrarily many DYNAMIC scoped series
/// out of one physical file rather than a single fixed scope, so this bootstrap
/// identity is never used for a real append — every real series gets its own
/// identity, built by `series_scope_identity` below and bound lazily on first use
/// via [`SeriesStore::mutation_version`]. The resource name is fixed human-readable
/// text and can never collide with a real series' resource, which is always the
/// opaque `"ts-scope:<sha256-hex>"` form `opaque_coordinator_key` mints for the
/// `graph` a caller passes to `compile_opaque_method` (see
/// `src/server/handlers/timeseries.rs`).
const SERIES_BOOTSTRAP_TENANT: &str = "series-store";
const SERIES_BOOTSTRAP_RESOURCE: &str = "series-store-bootstrap";

/// Operator-facing identity of the ONE physical `series.redb` owner file. Names
/// the physical authority boundary the storage kernel stamps into the owner
/// manifest, independent of any logical serving scope.
const SERIES_PHYSICAL_STORE: &str = "eg-tsdb:series-store";

fn series_bootstrap_identity() -> Result<eg_types::MutationScopeIdentity> {
    series_scope_identity(SERIES_BOOTSTRAP_TENANT, SERIES_BOOTSTRAP_RESOURCE)
}

/// Build the native time-series mutation-scope identity for one (tenant, resource)
/// pair. `resource` must be EXACTLY the string a caller passes as
/// `CompileBatch::graph` for the same append: `crate::server::mutation_batch::
/// finish_batch` (top-level crate) builds the batch's authoritative `identity` from
/// that identical (tenant, graph) pair, the SAME `MutationDomain::TimeSeries`
/// domain (the one operation `compile_opaque_method` compiles for a `TsAppend`),
/// and the SAME `COMPILED_BATCH_INCARNATION` constant. See that constant's doc for
/// why a mismatch on any of the three fails every append on that scope closed.
fn series_scope_identity(tenant: &str, resource: &str) -> Result<eg_types::MutationScopeIdentity> {
    let tenant = eg_types::TenantId::new(tenant).map_err(codec_err)?;
    let resource = eg_types::LogicalName::new(resource).map_err(codec_err)?;
    let incarnation_id =
        eg_types::IncarnationId::new(COMPILED_BATCH_INCARNATION).map_err(codec_err)?;
    eg_types::MutationScopeIdentity::native(
        tenant,
        eg_types::mutation_batch::MutationDomain::TimeSeries,
        resource,
        incarnation_id,
    )
    .map_err(codec_err)
}

/// Authenticate and bind ONE logical serving scope on this owner file.
///
/// The proof bytes are interpreted only by the composition root's verifier; this
/// crate supplies the scope identity and the layout and never inspects them.
/// Binding is idempotent for an exact re-entry, so reopening a store re-binds
/// the identical scope rather than failing.
fn bind_serving_scope(
    kernel: &StorageKernelV1,
    verifier: &dyn ScopeGrantVerifier,
    principal: &str,
    proof: &[u8],
    identity: &eg_types::MutationScopeIdentity,
) -> Result<OwnedStoreHandle<TimeSeriesOwner>> {
    let grant = kernel
        .authenticate_scope::<TimeSeriesOwner>(
            verifier,
            identity.clone(),
            principal.to_string(),
            proof,
        )
        .map_err(redb_err)?;
    kernel.bind_serving_scope(grant, 0).map_err(redb_err)
}

/// The batch for one store-level maintenance mutation.
///
/// `batch_id` is `(kind, scope version)`: exactly one batch commits per version,
/// so this is unique per attempt and stable across a crash-retry of the same
/// attempt -- which is what makes a retry a replay rather than an
/// `IDEMPOTENCY_CONFLICT`. The storage key it acted on travels in the operation
/// rather than the identity, because it can be up to 4 KiB.
fn maintenance_batch(
    kind: &str,
    storage_key: &str,
    identity: &eg_types::MutationScopeIdentity,
    principal: &str,
    expected_version: u64,
) -> Result<MutationBatch> {
    let batch_id = format!("tsdb-{kind}:v{expected_version}");
    let operation = eg_types::MutationOperation {
        ordinal: 0,
        surface: eg_types::MutationSurface::Other,
        domain: eg_types::mutation_batch::MutationDomain::TimeSeries,
        method: eg_types::protocol::Method::ApplyMutation {
            event_type: format!("timeseries_{kind}"),
            query: storage_key.to_string(),
        },
    };
    let batch = MutationBatch {
        schema_version: eg_types::MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: eg_types::MutationRequestContext {
            request_id: 0,
            principal: principal.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // A maintenance mutation claims no capability: it is a plain
            // `Native`-versioned write, not the reserved-system `Unversioned`
            // path. Empty is the true fact here, not a placeholder.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: identity.clone(),
        placement_epoch: 0,
        idempotency_key: batch_id,
        version_expectation: eg_types::VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate().map_err(codec_err)?;
    Ok(batch)
}

fn decode_stored<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_TS_STORED_META_BYTES,
            MAX_TS_STORED_META_ITEMS,
            32,
        ),
    )
    .map_err(|_| codec_err("stored time-series metadata is invalid"))
}

fn validate_storage_key(key: &str) -> Result<()> {
    // A `SCOPED_KEY_PREFIX` key must round-trip through `SeriesKey::decode` — which
    // independently rejects a NUL byte inside any of its length-prefixed components —
    // so scoped-key structural integrity is already fully covered there. An UNSCOPED
    // key (the "lean" local path this store's own module docs describe: e.g.
    // `server::secondary_indexes::GraphTemporalIndex`'s `"{graph}\0{node_id}"` series
    // ids, and this crate's own `delete_series_tests`) may legitimately embed a NUL as
    // an unambiguous separator between components that themselves can never contain
    // one. A blanket `key.contains('\0')` reject here would deny that entire supported,
    // tested unscoped usage, not just malformed scoped keys.
    if key.is_empty()
        || key.len() > MAX_TS_SERIES_KEY_BYTES
        || (key.starts_with(SCOPED_KEY_PREFIX) && SeriesKey::decode(key).is_none())
    {
        return Err(codec_err("time-series key is invalid"));
    }
    Ok(())
}

fn validate_meta(meta: &SeriesMeta) -> Result<()> {
    if meta.n_fields == 0
        || meta.n_fields > MAX_TS_FIELDS
        || meta.bucket_ns == 0
        || meta.field_names.len() != meta.n_fields
        || meta
            .field_names
            .iter()
            .any(|name| name.len() > MAX_TS_FIELD_NAME_BYTES || name.contains('\0'))
        || (meta.count == 0 && (meta.min_ts != Ts::MAX || meta.max_ts != Ts::MIN))
        || (meta.count != 0 && meta.min_ts > meta.max_ts)
    {
        return Err(codec_err("stored time-series metadata is invalid"));
    }
    Ok(())
}

fn decode_meta(bytes: &[u8]) -> Result<SeriesMeta> {
    let meta = decode_stored(bytes)?;
    validate_meta(&meta)?;
    Ok(meta)
}

fn validate_projection(health: &ProjectionHealth) -> Result<()> {
    if health
        .last_error
        .as_ref()
        .is_some_and(|error| error.len() > MAX_TS_PROJECTION_ERROR_BYTES)
        || health
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.count != 0 && cursor.min_ts > cursor.max_ts)
    {
        return Err(codec_err("time-series projection metadata is invalid"));
    }
    Ok(())
}

fn decode_projection(bytes: &[u8]) -> Result<ProjectionHealth> {
    let health = decode_stored(bytes)?;
    validate_projection(&health)?;
    Ok(health)
}

/// Per-series metadata: field schema, bucket width, point count, ts span.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeriesMeta {
    pub n_fields: usize,
    /// Bucket width in nanoseconds (the time-partition size).
    pub bucket_ns: u64,
    pub field_names: Vec<String>,
    pub count: u64,
    pub min_ts: Ts,
    pub max_ts: Ts,
    /// Retention/legal-hold exemption (CONCEPT:EG-KG.storage.series-legal-hold, GOC-09).
    /// When `true`, this series is a correctness boundary: [`SeriesStore::evict_before`]
    /// and [`SeriesStore::delete_series`] refuse to remove ANY of its data, no matter how
    /// old, until the hold is explicitly cleared via [`SeriesStore::set_legal_hold`].
    /// `#[serde(default)]` so a series meta blob written before this field existed
    /// decodes as `false` (not-held) rather than failing to decode.
    #[serde(default)]
    pub legal_hold: bool,
}

/// Canonical identity for a served time series. A raw `series_id` is never sufficient
/// at a multi-tenant boundary: two graphs may legitimately use the same local name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SeriesKey {
    pub tenant: String,
    pub graph: String,
    pub series: String,
}

impl SeriesKey {
    pub fn new(
        tenant: impl Into<String>,
        graph: impl Into<String>,
        series: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            graph: graph.into(),
            series: series.into(),
        }
    }

    pub fn encode(&self) -> String {
        format!(
            "{SCOPED_KEY_PREFIX}{}:{}:{}:{}{}{}",
            self.tenant.len(),
            self.graph.len(),
            self.series.len(),
            self.tenant,
            self.graph,
            self.series
        )
    }

    /// Decode a canonical key. Malformed or unversioned keys return `None`.
    pub fn decode(encoded: &str) -> Option<Self> {
        fn take_len(input: &str) -> Option<(usize, &str)> {
            let (len, rest) = input.split_once(':')?;
            Some((len.parse().ok()?, rest))
        }
        fn take_bytes(input: &str, len: usize) -> Option<(&str, &str)> {
            Some((input.get(..len)?, input.get(len..)?))
        }

        let rest = encoded.strip_prefix(SCOPED_KEY_PREFIX)?;
        let (tenant_len, rest) = take_len(rest)?;
        let (graph_len, rest) = take_len(rest)?;
        let (series_len, payload) = take_len(rest)?;
        let (tenant, payload) = take_bytes(payload, tenant_len)?;
        let (graph, payload) = take_bytes(payload, graph_len)?;
        let (series, trailing) = take_bytes(payload, series_len)?;
        if !trailing.is_empty()
            || tenant.is_empty()
            || graph.is_empty()
            || series.is_empty()
            || tenant.contains('\0')
            || graph.contains('\0')
            || series.contains('\0')
        {
            return None;
        }
        Some(Self::new(tenant, graph, series))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionStatus {
    Missing,
    CatchingUp,
    Ready,
    Degraded,
}

/// Durable high-water mark for the authoritative-to-served projection.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProjectionCursor {
    pub count: u64,
    pub min_ts: Ts,
    pub max_ts: Ts,
}

impl From<&SeriesMeta> for ProjectionCursor {
    fn from(meta: &SeriesMeta) -> Self {
        Self {
            count: meta.count,
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProjectionHealth {
    pub status: ProjectionStatus,
    pub cursor: Option<ProjectionCursor>,
    pub updated_unix_ms: u64,
    pub last_error: Option<String>,
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A decoded chunk: a bucket's worth of points, kept ts-sorted.
/// On disk: `[u32 n | u8 n_fields | i64 ts[n] | f64 vals[n*n_fields]]` (LE).
#[derive(Default)]
struct Chunk {
    n_fields: usize,
    ts: Vec<Ts>,
    vals: Vec<f64>, // row-major: point i fields at [i*n_fields .. (i+1)*n_fields]
}

fn chunk_encoded_len(points: usize, fields: usize) -> Option<usize> {
    let values = points.checked_mul(fields)?;
    5usize
        .checked_add(points.checked_mul(8)?)?
        .checked_add(values.checked_mul(8)?)
}

impl Chunk {
    fn encode(&self) -> Result<Vec<u8>> {
        let n = self.ts.len();
        let values = n
            .checked_mul(self.n_fields)
            .ok_or_else(|| codec_err("time-series chunk dimensions overflow"))?;
        let encoded_len = chunk_encoded_len(n, self.n_fields)
            .filter(|len| *len <= MAX_TS_CHUNK_BYTES)
            .ok_or_else(|| codec_err("time-series chunk exceeds the storage limit"))?;
        if n > MAX_TS_POINTS_PER_CHUNK
            || n > u32::MAX as usize
            || self.n_fields == 0
            || self.n_fields > MAX_TS_FIELDS
            || self.vals.len() != values
        {
            return Err(codec_err("time-series chunk dimensions are invalid"));
        }
        let mut out = Vec::with_capacity(encoded_len);
        out.extend_from_slice(&(n as u32).to_le_bytes());
        out.push(self.n_fields as u8);
        for &t in &self.ts {
            out.extend_from_slice(&t.to_le_bytes());
        }
        for &v in &self.vals {
            out.extend_from_slice(&v.to_le_bytes());
        }
        Ok(out)
    }

    /// Decode a stored chunk. Returns `Codec` rather than panicking on a truncated
    /// blob (durable bytes are trusted but a typed error beats an index panic).
    fn decode(buf: &[u8]) -> Result<Chunk> {
        if buf.len() < 5 || buf.len() > MAX_TS_CHUNK_BYTES {
            return Err(codec_err("stored time-series chunk is invalid"));
        }
        let n = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let n_fields = buf[4] as usize;
        if n > MAX_TS_POINTS_PER_CHUNK || n_fields == 0 || n_fields > MAX_TS_FIELDS {
            return Err(codec_err("stored time-series chunk is invalid"));
        }
        let values = n
            .checked_mul(n_fields)
            .ok_or_else(|| codec_err("stored time-series chunk is invalid"))?;
        let need = chunk_encoded_len(n, n_fields)
            .filter(|need| *need == buf.len())
            .ok_or_else(|| codec_err("stored time-series chunk is invalid"))?;
        let mut off = 5;
        let mut ts = Vec::with_capacity(n);
        for _ in 0..n {
            ts.push(i64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
            off += 8;
        }
        let mut vals = Vec::with_capacity(values);
        for _ in 0..values {
            vals.push(f64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
            off += 8;
        }
        debug_assert_eq!(off, need);
        Ok(Chunk { n_fields, ts, vals })
    }

    /// Merge one incoming bucket batch while keeping timestamps sorted. Existing
    /// samples precede newly-arrived samples on equal timestamps, and the stable
    /// incoming sort preserves arrival order among equal-timestamp siblings.
    ///
    /// Repeated `Vec::insert` shifted the existing suffix once per point (and once
    /// per field), making a late batch O(B*N*F). Sorting only when needed and then
    /// merging two ordered runs is O(B log B + (N+B)*F) with one bounded output
    /// allocation.
    fn merge_points(&mut self, points: &mut [&Point]) {
        if points.is_empty() {
            return;
        }
        if !points.windows(2).all(|pair| pair[0].ts <= pair[1].ts) {
            // `sort_by_key` is stable: equal timestamps retain caller arrival order.
            points.sort_by_key(|point| point.ts);
        }

        let old_len = self.ts.len();
        let new_len = points.len();
        let mut merged_ts = Vec::with_capacity(old_len + new_len);
        let mut merged_vals = Vec::with_capacity((old_len + new_len) * self.n_fields);
        let mut old = 0usize;
        let mut new = 0usize;
        while old < old_len || new < new_len {
            // Existing rows win ties, matching the old partition_point(`<=`) insert
            // semantics: every later append at the same timestamp follows them.
            if new == new_len || (old < old_len && self.ts[old] <= points[new].ts) {
                merged_ts.push(self.ts[old]);
                let base = old * self.n_fields;
                merged_vals.extend_from_slice(&self.vals[base..base + self.n_fields]);
                old += 1;
            } else {
                merged_ts.push(points[new].ts);
                merged_vals.extend_from_slice(&points[new].values);
                new += 1;
            }
        }
        self.ts = merged_ts;
        self.vals = merged_vals;
    }

    /// CONCEPT:EG-KG.temporal.bucket-cutoff-trim — drop the leading points older than `cutoff`, keeping only
    /// `ts >= cutoff` (the per-point retention trim of a STRADDLING bucket). Because
    /// the chunk stays ts-sorted ascending the old points are a prefix, so the cut is
    /// a single `partition_point`; the kept suffix preserves order. Returns the number
    /// of points dropped (0 ⇒ nothing older than `cutoff`, leave the chunk untouched).
    fn trim_before(&mut self, cutoff: Ts) -> usize {
        let cut = self.ts.partition_point(|&t| t < cutoff);
        if cut == 0 {
            return 0;
        }
        self.ts.drain(0..cut);
        self.vals.drain(0..cut * self.n_fields);
        cut
    }
}

/// A time-partitioned series store over ONE kernel-owned physical file.
///
/// RF-RULING-004: `eg-storage` is the sole physical owner of `series.redb`
/// (declared `OwnerLayout::TimeSeries`, owner tables `series_chunks`,
/// `series_meta`, `series_projection_state`) and `eg-transaction` the sole
/// writer. Unlike `RbacStore`'s single fixed scope, this store serves
/// arbitrarily many DYNAMIC scoped series out of that one file, so it holds the
/// composition root's scope-grant authority and binds each series' serving scope
/// on first use, caching the resulting handle. Cross-series work (`meta`,
/// `list_series`, retention, compaction) runs on the bootstrap scope: the
/// capability gate is table-level, and those operations are store-level
/// maintenance rather than one series' mutations.
pub struct SeriesStore {
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
    /// The composition root's proof authority. Held (not borrowed) because a new
    /// series' scope must be authenticated lazily, long after `open` returned.
    grants: Arc<dyn ScopeGrantVerifier>,
    principal: String,
    proof: Vec<u8>,
    bootstrap: Arc<OwnedStoreHandle<TimeSeriesOwner>>,
    /// Bound serving scopes, keyed by the scope's ledger key. `OwnedStoreHandle`
    /// is deliberately not `Clone` -- a handle IS a capability -- so the cache
    /// hands out `Arc` clones of the one bound handle rather than copies of it.
    scopes: RwLock<BTreeMap<String, Arc<OwnedStoreHandle<TimeSeriesOwner>>>>,
}

/// The scoped-append request for [`SeriesStore::append_scoped_batch`], bundled into
/// one type so the entry point stays under clippy's `too_many_arguments` threshold
/// without dropping any of the fields the append + terminal MutationBatch commit need.
pub struct ScopedAppendBatch<'a> {
    pub n_fields: usize,
    pub bucket_ns: u64,
    pub field_names: &'a [String],
    pub points: &'a [Point],
    pub batch: &'a MutationBatch,
    pub committed_at_ms: u64,
}

impl SeriesStore {
    /// Open (or create) the series store at `path` through the storage kernel.
    ///
    /// The kernel materializes the whole declared `OwnerLayout::TimeSeries`
    /// census when the file is created and re-validates it on every open, so the
    /// hand-written bootstrap closure the retired raw constructor needed is gone.
    /// `verifier` is the composition root's proof authority: only it may decide
    /// that `principal` may serve a given series scope.
    pub fn open(
        path: &Path,
        verifier: Arc<dyn ScopeGrantVerifier>,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self> {
        let identity = series_bootstrap_identity()?;
        let physical = PhysicalStoreIdentity::new(SERIES_PHYSICAL_STORE).map_err(redb_err)?;
        let kernel = if path.exists() {
            StorageKernelV1::open_owner::<TimeSeriesOwner>(path, physical, None)
        } else {
            StorageKernelV1::create_owner::<TimeSeriesOwner>(path, physical, None)
        }
        .map_err(redb_err)?;
        let (kernel, authority) = kernel.into_read_and_mutation_authority().map_err(redb_err)?;
        let bootstrap = Arc::new(bind_serving_scope(
            &kernel,
            verifier.as_ref(),
            principal,
            proof,
            &identity,
        )?);
        Ok(Self {
            kernel,
            mutations: MutationKernelV1::new(authority),
            grants: verifier,
            principal: principal.to_string(),
            proof: proof.to_vec(),
            bootstrap,
            scopes: RwLock::new(BTreeMap::new()),
        })
    }

    /// The bound handle for one series' serving scope, binding it on first use.
    fn scope_handle(
        &self,
        identity: &eg_types::MutationScopeIdentity,
    ) -> Result<Arc<OwnedStoreHandle<TimeSeriesOwner>>> {
        let key = eg_storage::ledger_scope_key(identity);
        if let Some(handle) = self
            .scopes
            .read()
            .map_err(|_| redb_err("time-series scope cache is poisoned"))?
            .get(&key)
        {
            return Ok(Arc::clone(handle));
        }
        let handle = Arc::new(bind_serving_scope(
            &self.kernel,
            self.grants.as_ref(),
            &self.principal,
            &self.proof,
            identity,
        )?);
        self.scopes
            .write()
            .map_err(|_| redb_err("time-series scope cache is poisoned"))?
            .insert(key, Arc::clone(&handle));
        Ok(handle)
    }

    /// One kernel-issued scoped read over the bootstrap scope -- the cross-series
    /// view every non-scoped read uses.
    fn scoped_read(&self) -> Result<ScopedRead<'_, TimeSeriesOwner>> {
        self.kernel.read_scope(&self.bootstrap).map_err(redb_err)
    }

    /// Run one store-level MAINTENANCE mutation (RF-RULING-005) on the bootstrap
    /// scope: ledgered, fenced and version-bumping like any other write, but
    /// carrying no caller identity, which is exactly what retention, compaction,
    /// projection bookkeeping and a non-scoped append are. There is no
    /// un-ledgered owner-write path any more, so this is how they land.
    fn maintain<T, F>(&self, kind: &str, storage_key: &str, apply: F) -> Result<T>
    where
        F: FnOnce(&AdmittedOwnerWrite<'_, TimeSeriesOwner>) -> Result<T>,
    {
        let expected_version = self.scope_version(&self.bootstrap)?;
        let batch = maintenance_batch(
            kind,
            storage_key,
            self.bootstrap.identity(),
            &self.principal,
            expected_version,
        )?;
        let (write, begun) = self
            .mutations
            .admit_maintenance(&self.bootstrap, &batch)
            .map_err(redb_err)?;
        let source_version = match begun {
            Begin::Replay(_) => {
                write.abort().map_err(redb_err)?;
                return Err(codec_err(
                    "time-series maintenance batch was already committed",
                ));
            }
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = write
            .owner_rows(&self.bootstrap, &batch)
            .map_err(redb_err)?;
        let outcome = apply(&owner_write);
        owner_write.finish_owner().map_err(redb_err)?;
        let outcome = match outcome {
            Ok(value) => value,
            Err(error) => {
                write.abort().map_err(redb_err)?;
                return Err(error);
            }
        };
        self.mutations
            .finish(&write, &batch, None, unix_ms(), source_version)
            .map_err(redb_err)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(outcome)
    }

    fn scope_version(&self, owner: &OwnedStoreHandle<TimeSeriesOwner>) -> Result<u64> {
        let read = self.kernel.read_scope(owner).map_err(redb_err)?;
        eg_transaction::version(&read).map_err(redb_err)
    }

    /// Open `{persist_dir}/series.redb` — the durable location beside the graph shards.
    pub fn open_in_dir(
        persist_dir: &Path,
        verifier: Arc<dyn ScopeGrantVerifier>,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self> {
        std::fs::create_dir_all(persist_dir)
            .map_err(|e| TsError::Redb(format!("create persist dir: {e}")))?;
        Self::open(&persist_dir.join("series.redb"), verifier, principal, proof)
    }

    #[inline]
    fn bucket_of(ts: Ts, bucket_ns: u64) -> u64 {
        (ts as u64 / bucket_ns) * bucket_ns
    }

    /// Append a batch of points to a series in ONE redb write transaction — the
    /// throughput primitive. Groups points by bucket, read-modify-writes each
    /// touched chunk once, updates meta. Creates the series (and its meta) on first
    /// append. Out-of-order / late points are handled by the sorted chunk insert.
    ///
    /// `bucket_ns`/`field_names` are used only when the series is NEW; for an
    /// existing series the stored schema wins, and a width mismatch is a hard error.
    pub fn append_batch(
        &self,
        series_id: &str,
        n_fields: usize,
        bucket_ns: u64,
        field_names: &[String],
        points: &[Point],
    ) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        self.maintain("append", series_id, |wtx| {
            append_batch_in_wtx(wtx, series_id, n_fields, bucket_ns, field_names, points)
        })
    }

    /// Tenant-safe append. The canonical scoped key and projection cursor are written
    /// in the same redb transaction, so served data and health cannot diverge.
    pub fn append_scoped(
        &self,
        key: &SeriesKey,
        n_fields: usize,
        bucket_ns: u64,
        field_names: &[String],
        points: &[Point],
    ) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        let storage_key = key.encode();
        self.maintain("append-scoped", &storage_key, |wtx| {
            append_batch_in_wtx(wtx, &storage_key, n_fields, bucket_ns, field_names, points)?;
            let meta = meta_in_wtx(wtx, &storage_key)?
                .ok_or_else(|| codec_err("scoped append produced no series metadata"))?;
            put_projection_in_wtx(
                wtx,
                &storage_key,
                &ProjectionHealth {
                    status: ProjectionStatus::Ready,
                    cursor: Some(ProjectionCursor::from(&meta)),
                    updated_unix_ms: unix_ms(),
                    last_error: None,
                },
            )
        })
    }

    /// Current universal MutationBatch version for a native time-series scope.
    /// Binds the scope's serving identity on first use — `SeriesStore` serves
    /// arbitrarily many dynamic scoped series out of one physical file (unlike
    /// `RbacStore`'s single fixed scope), so each series' scope must be
    /// authenticated against the composition root's verifier and bound before the
    /// mutation kernel will admit a batch for it. The bound handle is cached, and
    /// the kernel's binding is idempotent for an exact re-entry.
    pub fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64> {
        let identity = series_scope_identity(tenant, graph)?;
        let owner = self.scope_handle(&identity)?;
        self.scope_version(&owner)
    }

    /// Append points and commit terminal MutationBatch status/fence/idempotency/
    /// outbox rows in the same `series.redb` transaction. The exact point count is
    /// retained as the replay result, so an ack-lost retry never appends twice.
    pub fn append_scoped_batch(
        &self,
        key: &SeriesKey,
        request: ScopedAppendBatch<'_>,
    ) -> Result<u64> {
        let ScopedAppendBatch {
            n_fields,
            bucket_ns,
            field_names,
            points,
            batch,
            committed_at_ms,
        } = request;
        let storage_key = key.encode();
        // The batch's own scope is bound on first use here, so a caller no longer
        // has to have called `mutation_version` first for the binding side effect
        // (it still must, to compute its OCC expectation).
        let owner = self.scope_handle(&batch.identity)?;
        let (write, begun) = self.mutations.admit(&owner, batch).map_err(redb_err)?;
        match begun {
            Begin::Replay(record) => {
                let bytes = record
                    .result_msgpack
                    .as_deref()
                    .ok_or_else(|| codec_err("committed time-series batch has no result"))?;
                let count = decode_stored(bytes)?;
                write.abort().map_err(redb_err)?;
                Ok(count)
            }
            Begin::Apply { source_version } => {
                let owner_write = write.owner_rows(&owner, batch).map_err(redb_err)?;
                let staged = (|| -> Result<()> {
                    if points.is_empty() {
                        return Ok(());
                    }
                    append_batch_in_wtx(
                        &owner_write,
                        &storage_key,
                        n_fields,
                        bucket_ns,
                        field_names,
                        points,
                    )?;
                    let meta = meta_in_wtx(&owner_write, &storage_key)?
                        .ok_or_else(|| codec_err("scoped append produced no series metadata"))?;
                    put_projection_in_wtx(
                        &owner_write,
                        &storage_key,
                        &ProjectionHealth {
                            status: ProjectionStatus::Ready,
                            cursor: Some(ProjectionCursor::from(&meta)),
                            updated_unix_ms: committed_at_ms,
                            last_error: None,
                        },
                    )
                })();
                // Always close the owner capability: dropping it unfinished poisons
                // the write and would mask the staging error below.
                owner_write.finish_owner().map_err(redb_err)?;
                if let Err(error) = staged {
                    write.abort().map_err(redb_err)?;
                    return Err(error);
                }
                let count = points.len() as u64;
                let result = rmp_serde::to_vec_named(&count).map_err(codec_err)?;
                self.mutations
                    .finish(&write, batch, Some(result), committed_at_ms, source_version)
                    .map_err(redb_err)?;
                self.mutations.commit(write, batch).map_err(redb_err)?;
                Ok(count)
            }
        }
    }

    /// Fetch a series' metadata (`None` if the series doesn't exist).
    pub fn meta(&self, series_id: &str) -> Result<Option<SeriesMeta>> {
        let read = self.scoped_read()?;
        meta_in_rtx(&read, series_id)
    }

    pub fn meta_scoped(&self, key: &SeriesKey) -> Result<Option<SeriesMeta>> {
        self.meta(&key.encode())
    }

    /// Scan `[from, to)` of a series in ts order — the time-range read primitive.
    /// Implemented as a redb RANGE over the covering bucket keys, decoding each
    /// chunk and trimming to the exact window. Empty for an unknown series.
    pub fn range(&self, series_id: &str, from: Ts, to: Ts) -> Result<Vec<Point>> {
        let read = self.scoped_read()?;
        range_in_rtx(&read, series_id, from, to)
    }

    pub fn range_scoped(&self, key: &SeriesKey, from: Ts, to: Ts) -> Result<Vec<Point>> {
        self.range(&key.encode(), from, to)
    }

    /// Full series scan (every point, ts order).
    pub fn scan_all(&self, series_id: &str) -> Result<Vec<Point>> {
        self.range(series_id, Ts::MIN, Ts::MAX)
    }

    pub fn scan_all_scoped(&self, key: &SeriesKey) -> Result<Vec<Point>> {
        self.scan_all(&key.encode())
    }

    /// Durable projection health for one scoped series. Unknown series return an
    /// explicit `missing` state rather than a healthy-looking empty cursor.
    pub fn projection_health(&self, key: &SeriesKey) -> Result<ProjectionHealth> {
        self.projection_health_by_storage_key(&key.encode())
    }

    pub fn projection_health_by_storage_key(&self, storage_key: &str) -> Result<ProjectionHealth> {
        validate_storage_key(storage_key)?;
        let read = self.scoped_read()?;
        let tab = read.open_series_table(PROJECTION_STATE)?;
        match tab.get(storage_key).map_err(redb_err)? {
            Some(g) => decode_projection(g.value()),
            None => Ok(ProjectionHealth {
                status: if meta_in_rtx(&read, storage_key)?.is_some() {
                    ProjectionStatus::CatchingUp
                } else {
                    ProjectionStatus::Missing
                },
                cursor: None,
                updated_unix_ms: 0,
                last_error: None,
            }),
        }
    }

    pub fn mark_projection_ready(&self, storage_key: &str, source_meta: &SeriesMeta) -> Result<()> {
        self.put_projection(
            storage_key,
            ProjectionHealth {
                status: ProjectionStatus::Ready,
                cursor: Some(ProjectionCursor::from(source_meta)),
                updated_unix_ms: unix_ms(),
                last_error: None,
            },
        )
    }

    pub fn mark_projection_degraded(&self, storage_key: &str, error: &str) -> Result<()> {
        let previous = self.projection_health_by_storage_key(storage_key)?;
        self.put_projection(
            storage_key,
            ProjectionHealth {
                status: ProjectionStatus::Degraded,
                cursor: previous.cursor,
                updated_unix_ms: unix_ms(),
                last_error: Some(error.to_string()),
            },
        )
    }

    fn put_projection(&self, storage_key: &str, health: ProjectionHealth) -> Result<()> {
        self.maintain("projection", storage_key, |wtx| {
            put_projection_in_wtx(wtx, storage_key, &health)
        })
    }

    /// List every series id present (scans the meta table's keys). Additive read used
    /// by the PromQL facade to resolve label matchers / enumerate labels (CONCEPT:EG-KG.query.prometheus-http-query-api)
    /// — the store keys series by opaque id, so the PromQL layer encodes a metric's
    /// labels INTO the id and enumerates them here.
    pub fn list_series(&self) -> Result<Vec<String>> {
        let read = self.scoped_read()?;
        list_series_in_rtx(&read)
    }

    /// Retention: drop every point of `series_id` older than `cutoff`. Returns the
    /// number of WHOLE buckets removed. Two cases (CONCEPT:EG-KG.temporal.bucket-cutoff-trim):
    ///  * a bucket whose entire span ends at-or-before `cutoff` is range-deleted whole
    ///    (the cheap fast path — one B-tree remove, no decode);
    ///  * a bucket STRADDLING `cutoff` (starts before, ends after) is rewritten in
    ///    place keeping only its points `>= cutoff` (per-point trim), so retention is
    ///    exact to the point rather than rounded up to the bucket. A straddler whose
    ///    points are all older than `cutoff` collapses to a whole-bucket drop.
    ///
    /// Meta's `count`/`min_ts` are recomputed from the surviving (possibly trimmed)
    /// buckets; `max_ts` is unchanged unless the series is fully emptied.
    pub fn evict_before(&self, series_id: &str, cutoff: Ts) -> Result<usize> {
        let meta = match self.meta(series_id)? {
            Some(m) => m,
            None => return Ok(0),
        };
        // Legal hold is a correctness boundary, not a policy nicety (CONCEPT:EG-KG.storage.series-legal-hold,
        // GOC-09): a held series must be un-deletable by the retention sweep, provably — so
        // this check runs BEFORE any bucket is classified for removal, never as a filter
        // applied after victims are already chosen.
        if meta.legal_hold {
            return Ok(0);
        }
        self.maintain("evict", series_id, |wtx| {
            let mut dropped = 0usize;
            {
                let mut chunks = wtx.open_series_table(SERIES_CHUNKS)?;
                let mut meta_tab = wtx.open_series_table(SERIES_META)?;

                // Pass 1 (read-only over the range — can't mutate while the iterator
                // borrows `chunks`): classify each bucket into a whole-bucket victim or a
                // straddler rewrite. A straddler trimmed to empty becomes a victim.
                let mut victims: Vec<u64> = Vec::new();
                let mut rewrites: Vec<(u64, Vec<u8>)> = Vec::new();
                let mut rewrite_bytes = 0usize;
                let mut scanned_buckets = 0usize;
                let lo = (series_id, 0u64);
                let hi = (series_id, u64::MAX);
                for item in chunks.range(lo..=hi).map_err(redb_err)? {
                    let (k, v) = item.map_err(redb_err)?;
                    scanned_buckets = scanned_buckets
                        .checked_add(1)
                        .filter(|count| *count <= MAX_TS_BUCKETS_PER_OPERATION)
                        .ok_or_else(|| {
                            codec_err("time-series retention exceeds the operation limit")
                        })?;
                    if victims.len().saturating_add(rewrites.len()) > MAX_TS_BUCKETS_PER_OPERATION {
                        return Err(codec_err(
                            "time-series retention exceeds the operation limit",
                        ));
                    }
                    let bucket = k.value().1;
                    let bucket_end = bucket.saturating_add(meta.bucket_ns);
                    if (bucket_end as i64) <= cutoff {
                        victims.push(bucket);
                    } else if (bucket as i64) < cutoff {
                        // Straddles `cutoff`: trim the older prefix in place (CONCEPT:EG-KG.temporal.bucket-cutoff-trim).
                        let mut chunk = Chunk::decode(v.value())?;
                        if chunk.trim_before(cutoff) > 0 {
                            if chunk.ts.is_empty() {
                                victims.push(bucket);
                            } else {
                                let encoded = chunk.encode()?;
                                rewrite_bytes = rewrite_bytes
                                    .checked_add(encoded.len())
                                    .filter(|bytes| *bytes <= MAX_TS_OPERATION_BYTES)
                                    .ok_or_else(|| {
                                        codec_err("time-series retention exceeds the operation limit")
                                    })?;
                                rewrites.push((bucket, encoded));
                            }
                        }
                    }
                }

                let mut new_count = 0u64;
                let mut new_min = Ts::MAX;
                for bucket in &victims {
                    if let Some(g) = chunks.remove((series_id, *bucket)).map_err(redb_err)? {
                        drop(g);
                        dropped += 1;
                    }
                }
                for (bucket, blob) in &rewrites {
                    chunks
                        .insert((series_id, *bucket), blob.as_slice())
                        .map_err(redb_err)?;
                }
                // Recompute count/min over survivors (max is unchanged — we only drop old).
                let mut survivor_buckets = 0usize;
                for item in chunks.range(lo..=hi).map_err(redb_err)? {
                    let (_k, v) = item.map_err(redb_err)?;
                    survivor_buckets = survivor_buckets
                        .checked_add(1)
                        .filter(|count| *count <= MAX_TS_BUCKETS_PER_OPERATION)
                        .ok_or_else(|| {
                            codec_err("time-series retention exceeds the operation limit")
                        })?;
                    let chunk = Chunk::decode(v.value())?;
                    new_count = new_count
                        .checked_add(chunk.ts.len() as u64)
                        .ok_or_else(|| codec_err("stored time-series point count overflow"))?;
                    if let Some(&first) = chunk.ts.first() {
                        new_min = new_min.min(first);
                    }
                }
                let mut m = meta.clone();
                m.count = new_count;
                m.min_ts = if new_count == 0 { Ts::MAX } else { new_min };
                if new_count == 0 {
                    m.max_ts = Ts::MIN;
                }
                let mblob = rmp_serde::to_vec(&m).map_err(codec_err)?;
                meta_tab
                    .insert(series_id, mblob.as_slice())
                    .map_err(redb_err)?;
            }
            Ok(dropped)
        })
    }

    pub fn evict_before_scoped(&self, key: &SeriesKey, cutoff: Ts) -> Result<usize> {
        let storage_key = key.encode();
        let dropped = self.evict_before(&storage_key, cutoff)?;
        if let Some(meta) = self.meta(&storage_key)? {
            self.mark_projection_ready(&storage_key, &meta)?;
        }
        Ok(dropped)
    }

    /// Drop a series ENTIRELY — every chunk plus its meta — in ONE write txn
    /// (CONCEPT:EG-KG.storage.incremental-temporal). The removal primitive the graph-node
    /// temporal index (`GraphTemporalIndex`) uses when its owning node is removed, and
    /// the idempotent-replace primitive it uses on a node UPDATE (delete-then-append the
    /// node's current series). Returns the number of chunks removed (0 for an unknown
    /// series). Unlike [`evict_before`], this removes the meta row too, so a
    /// subsequently re-appended series starts fresh (no stale count/span).
    pub fn delete_series(&self, series_id: &str) -> Result<usize> {
        // Same legal-hold correctness boundary as `evict_before`: a held series survives
        // a whole-series delete too, not just the point-level retention trim.
        if let Some(meta) = self.meta(series_id)? {
            if meta.legal_hold {
                return Ok(0);
            }
        }
        self.maintain("delete", series_id, |wtx| {
            let mut dropped = 0usize;
            {
                let mut chunks = wtx.open_series_table(SERIES_CHUNKS)?;
                let mut meta_tab = wtx.open_series_table(SERIES_META)?;
                // Collect the covering bucket keys first (can't remove while the range
                // iterator borrows `chunks`), then delete each.
                let lo = (series_id, 0u64);
                let hi = (series_id, u64::MAX);
                let mut buckets: Vec<u64> = Vec::new();
                for item in chunks.range(lo..=hi).map_err(redb_err)? {
                    let (k, _v) = item.map_err(redb_err)?;
                    if buckets.len() >= MAX_TS_BUCKETS_PER_OPERATION {
                        return Err(codec_err(
                            "time-series deletion exceeds the operation limit",
                        ));
                    }
                    buckets.push(k.value().1);
                }
                for b in buckets {
                    if chunks.remove((series_id, b)).map_err(redb_err)?.is_some() {
                        dropped += 1;
                    }
                }
                meta_tab.remove(series_id).map_err(redb_err)?;
                let mut projection = wtx.open_series_table(PROJECTION_STATE)?;
                projection.remove(series_id).map_err(redb_err)?;
            }
            Ok(dropped)
        })
    }
    pub fn delete_scoped(&self, key: &SeriesKey) -> Result<usize> {
        self.delete_series(&key.encode())
    }

    /// Set (or clear) the durable legal-hold flag on an EXISTING series
    /// (CONCEPT:EG-KG.storage.series-legal-hold, GOC-09). Once `hold=true`, neither
    /// [`SeriesStore::evict_before`] nor [`SeriesStore::delete_series`] will remove any of
    /// this series' data until a caller explicitly clears the hold (`hold=false`). Errors
    /// (rather than silently no-opping) when the series does not exist yet — marking a hold
    /// is a deliberate admin action, not a best-effort default like `evict_before`'s "unknown
    /// series → 0 dropped".
    pub fn set_legal_hold(&self, series_id: &str, hold: bool) -> Result<()> {
        self.maintain("legal-hold", series_id, |wtx| {
            {
                let mut meta_tab = wtx.open_series_table(SERIES_META)?;
                let mut meta = match meta_tab.get(series_id).map_err(redb_err)? {
                    Some(g) => decode_meta(g.value())?,
                    None => {
                        return Err(codec_err(format!(
                            "cannot set legal_hold on unknown series {series_id:?}"
                        )));
                    }
                };
                meta.legal_hold = hold;
                let blob = rmp_serde::to_vec(&meta).map_err(codec_err)?;
                meta_tab
                    .insert(series_id, blob.as_slice())
                    .map_err(redb_err)?;
            }
            Ok(())
        })
    }

    /// Tenant/graph/series-scoped twin of [`SeriesStore::set_legal_hold`].
    pub fn set_legal_hold_scoped(&self, key: &SeriesKey, hold: bool) -> Result<()> {
        self.set_legal_hold(&key.encode(), hold)
    }
}

fn meta_in_wtx<W: SeriesTableWriter>(wtx: &W, series_id: &str) -> Result<Option<SeriesMeta>> {
    validate_storage_key(series_id)?;
    let tab = wtx.open_series_table(SERIES_META)?;
    let value = match tab.get(series_id).map_err(redb_err)? {
        Some(g) => Ok(Some(decode_meta(g.value())?)),
        None => Ok(None),
    };
    value
}

fn put_projection_in_wtx<W: SeriesTableWriter>(
    wtx: &W,
    storage_key: &str,
    health: &ProjectionHealth,
) -> Result<()> {
    validate_storage_key(storage_key)?;
    validate_projection(health)?;
    let blob = rmp_serde::to_vec(health).map_err(codec_err)?;
    if blob.len() > MAX_TS_STORED_META_BYTES {
        return Err(codec_err(
            "time-series projection metadata exceeds the storage limit",
        ));
    }
    let mut table = wtx.open_series_table(PROJECTION_STATE)?;
    table
        .insert(storage_key, blob.as_slice())
        .map_err(redb_err)?;
    Ok(())
}

/// Append `points` to `series_id` INTO an already-open redb [`WriteTransaction`] the
/// CALLER owns (CONCEPT:EG-KG.backend.cross-modal-atomic-commit — cross-modal atomic commit). Byte-for-byte the SAME
/// chunk encoding + read-modify-write + meta bookkeeping as [`SeriesStore::append_batch`]
/// (that method now delegates here), but it opens `SERIES_CHUNKS`/`SERIES_META` on the
/// passed transaction instead of the store's own `series.redb`.
///
/// This is what lets a time-series measurement batch land in the SAME graph-shard
/// `WriteTransaction` as a cross-modal txn's graph/vector/blob writes: redb holds an
/// EXCLUSIVE per-process file lock, so the only way measurements can be atomic WITH the
/// graph modalities is to write them through the graph transaction the redb writer thread
/// already owns — not a second `series.redb` handle. The caller is responsible for
/// `begin_write`, `set_durability`, and `commit`; on any error it drops the `wtx` and NONE
/// of the modalities (measurements included) land — a true all-or-nothing rollback.
///
/// `bucket_ns`/`field_names` are used only when the series is NEW; for an existing series
/// the stored schema is authoritative and a width mismatch is a hard error (identical to
/// [`SeriesStore::append_batch`]).
pub fn append_batch_in_wtx<W: SeriesTableWriter>(
    wtx: &W,
    series_id: &str,
    n_fields: usize,
    bucket_ns: u64,
    field_names: &[String],
    points: &[Point],
) -> Result<()> {
    if points.is_empty() {
        return Ok(());
    }
    validate_storage_key(series_id)?;
    if n_fields == 0
        || n_fields > MAX_TS_FIELDS
        || bucket_ns == 0
        || field_names.len() != n_fields
        || field_names
            .iter()
            .any(|name| name.len() > MAX_TS_FIELD_NAME_BYTES || name.contains('\0'))
        || points.len() > MAX_TS_POINTS_PER_CHUNK
    {
        return Err(codec_err("time-series append dimensions are invalid"));
    }
    // Reject a mixed-width batch up front (each point must match n_fields).
    for p in points {
        if p.values.len() != n_fields {
            return Err(TsError::FieldMismatch {
                expected: n_fields,
                got: p.values.len(),
            });
        }
    }
    let mut chunks = wtx.open_series_table(SERIES_CHUNKS)?;
    let mut meta_tab = wtx.open_series_table(SERIES_META)?;

    // Load-or-init meta. An existing series' stored schema is authoritative.
    let mut meta: SeriesMeta = match meta_tab.get(series_id).map_err(redb_err)? {
        Some(g) => decode_meta(g.value())?,
        None => SeriesMeta {
            n_fields,
            bucket_ns,
            field_names: field_names.to_vec(),
            count: 0,
            min_ts: Ts::MAX,
            max_ts: Ts::MIN,
            legal_hold: false,
        },
    };
    if meta.n_fields != n_fields {
        return Err(TsError::FieldMismatch {
            expected: meta.n_fields,
            got: n_fields,
        });
    }
    if meta.bucket_ns != bucket_ns {
        return Err(codec_err(
            "time-series bucket width does not match stored schema",
        ));
    }

    // Group incoming points by bucket so each chunk is touched once.
    let mut by_bucket: BTreeMap<u64, Vec<&Point>> = BTreeMap::new();
    for p in points {
        by_bucket
            .entry(SeriesStore::bucket_of(p.ts, meta.bucket_ns))
            .or_default()
            .push(p);
    }

    for (bucket, mut pts) in by_bucket {
        let mut chunk = match chunks.get((series_id, bucket)).map_err(redb_err)? {
            Some(g) => Chunk::decode(g.value())?,
            None => Chunk {
                n_fields: meta.n_fields,
                ts: vec![],
                vals: vec![],
            },
        };
        if chunk.n_fields != meta.n_fields {
            return Err(codec_err("stored time-series chunk schema is invalid"));
        }
        let merged_points = chunk
            .ts
            .len()
            .checked_add(pts.len())
            .filter(|count| {
                *count <= MAX_TS_POINTS_PER_CHUNK
                    && chunk_encoded_len(*count, chunk.n_fields)
                        .is_some_and(|bytes| bytes <= MAX_TS_CHUNK_BYTES)
            })
            .ok_or_else(|| codec_err("time-series chunk exceeds the storage limit"))?;
        chunk.merge_points(&mut pts);
        debug_assert_eq!(chunk.ts.len(), merged_points);
        meta.count = meta
            .count
            .checked_add(pts.len() as u64)
            .ok_or_else(|| codec_err("time-series point count overflow"))?;
        // The merge sorted `pts`; use its endpoints rather than re-walking it.
        meta.min_ts = meta.min_ts.min(pts[0].ts);
        meta.max_ts = meta.max_ts.max(pts[pts.len() - 1].ts);
        let blob = chunk.encode()?;
        chunks
            .insert((series_id, bucket), blob.as_slice())
            .map_err(redb_err)?;
    }

    let mblob = rmp_serde::to_vec(&meta).map_err(codec_err)?;
    meta_tab
        .insert(series_id, mblob.as_slice())
        .map_err(redb_err)?;
    Ok(())
}

/// List every series id present, reading from an ALREADY-OPEN [`ReadTransaction`] the
/// caller owns (CONCEPT:EG-KG.backend.ts-startup-reconcile). Byte-for-byte the same walk as
/// [`SeriesStore::list_series`] (which now delegates here), extracted so a caller holding
/// a foreign `Database` handle — e.g. one facade graph shard, whose SERIES tables
/// share this EXACT schema (see the module doc) — can scan it directly without opening a
/// SECOND `Database` on that file (redb's exclusive per-process file lock would reject
/// it). A table that was never created (a graph shard that has never durably
/// committed a measurement) is reported as EMPTY rather than an error — the natural "no
/// series here" reading for a store whose schema simply hasn't been materialized yet.
pub fn list_series_in_rtx<R: SeriesTableReader>(rtx: &R) -> Result<Vec<String>> {
    let tab = match rtx.open_series_table_if_present(SERIES_META)? {
        Some(t) => t,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    let mut bytes = 0usize;
    for item in tab.iter().map_err(redb_err)? {
        let (k, _v) = item.map_err(redb_err)?;
        let key = k.value();
        validate_storage_key(key)?;
        bytes = bytes
            .checked_add(key.len())
            .filter(|total| *total <= MAX_TS_SERIES_SCAN_BYTES)
            .ok_or_else(|| codec_err("time-series list exceeds the response limit"))?;
        if out.len() >= MAX_TS_SERIES_PER_SCAN {
            return Err(codec_err("time-series list exceeds the response limit"));
        }
        out.push(key.to_string());
    }
    Ok(out)
}

/// Fetch a series' metadata from an ALREADY-OPEN [`ReadTransaction`] (CONCEPT:EG-KG.backend.ts-startup-reconcile).
/// Same "missing table ⇒ `None`, missing series ⇒ `None`" contract as
/// [`SeriesStore::meta`] (which now delegates here) — see [`list_series_in_rtx`] for why a
/// caller wants this over the store's own `begin_read()`.
pub fn meta_in_rtx<R: SeriesTableReader>(rtx: &R, series_id: &str) -> Result<Option<SeriesMeta>> {
    validate_storage_key(series_id)?;
    let tab = match rtx.open_series_table_if_present(SERIES_META)? {
        Some(t) => t,
        None => return Ok(None),
    };
    match tab.get(series_id).map_err(redb_err)? {
        Some(g) => Ok(Some(decode_meta(g.value())?)),
        None => Ok(None),
    }
}

/// Scan `[from, to)` of a series in ts order from an ALREADY-OPEN [`ReadTransaction`]
/// (CONCEPT:EG-KG.backend.ts-startup-reconcile). Byte-for-byte the same bucket-range walk as
/// [`SeriesStore::range`] (which now delegates here for its own `db`); see
/// [`list_series_in_rtx`] for why a caller wants the shared-transaction form. Empty for an
/// unknown series OR a `SERIES_CHUNKS` table that was never created.
pub fn range_in_rtx<R: SeriesTableReader>(
    rtx: &R,
    series_id: &str,
    from: Ts,
    to: Ts,
) -> Result<Vec<Point>> {
    validate_storage_key(series_id)?;
    if from >= to {
        return Ok(Vec::new());
    }
    let meta = match meta_in_rtx(rtx, series_id)? {
        Some(m) => m,
        None => return Ok(vec![]),
    };
    if meta.count == 0 {
        return Ok(vec![]);
    }
    // Clamp to the series' real bucket span — see `SeriesStore::range`'s doc comment
    // for why the caller's open bound must not be cast through u64 directly.
    let scan_from = from.max(meta.min_ts);
    if scan_from > meta.max_ts || to <= meta.min_ts {
        return Ok(vec![]);
    }
    // `[from, to)` includes at most `to - 1` at nanosecond resolution. Clamping
    // the upper bucket to that timestamp avoids the old scan through every future
    // bucket up to the series maximum for a narrow historical query.
    let scan_through = to.saturating_sub(1).min(meta.max_ts);
    let from_bucket = SeriesStore::bucket_of(scan_from, meta.bucket_ns);
    let to_bucket = SeriesStore::bucket_of(scan_through, meta.bucket_ns);
    let chunks = match rtx.open_series_table_if_present(SERIES_CHUNKS)? {
        Some(t) => t,
        None => return Ok(vec![]),
    };
    let mut out = Vec::new();
    let mut response_bytes = 0usize;
    let lo = (series_id, from_bucket);
    let hi = (series_id, to_bucket); // covering buckets; exact ts filtered below
    for item in chunks.range(lo..=hi).map_err(redb_err)? {
        let (_k, v) = item.map_err(redb_err)?;
        let chunk = Chunk::decode(v.value())?;
        let nf = chunk.n_fields;
        if nf != meta.n_fields {
            return Err(codec_err("stored time-series chunk schema is invalid"));
        }
        let point_bytes = std::mem::size_of::<Point>()
            .checked_add(
                nf.checked_mul(std::mem::size_of::<f64>())
                    .ok_or_else(|| codec_err("time-series response exceeds the limit"))?,
            )
            .ok_or_else(|| codec_err("time-series response exceeds the limit"))?;
        // Chunks are sorted, so narrow windows seek to their exact slice instead
        // of testing every point in the two boundary buckets.
        let start = chunk.ts.partition_point(|&t| t < from);
        let end = chunk.ts.partition_point(|&t| t < to);
        for i in start..end {
            response_bytes = response_bytes
                .checked_add(point_bytes)
                .filter(|bytes| *bytes <= MAX_TS_QUERY_BYTES)
                .ok_or_else(|| codec_err("time-series response exceeds the limit"))?;
            if out.len() >= MAX_TS_QUERY_POINTS {
                return Err(codec_err("time-series response exceeds the limit"));
            }
            out.push(Point {
                ts: chunk.ts[i],
                values: chunk.vals[i * nf..(i + 1) * nf].to_vec(),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod ordered_chunk_tests {
    use super::*;
    use crate::dev_scope_grant::open_dev_store as open_test_store;

    #[test]
    fn stored_chunk_dimensions_are_checked_before_allocation() {
        let mut bomb = Vec::from(u32::MAX.to_le_bytes());
        bomb.push(u8::MAX);
        assert!(Chunk::decode(&bomb).is_err());

        let chunk = Chunk {
            n_fields: 1,
            ts: vec![1],
            vals: vec![2.0],
        };
        let mut encoded = chunk.encode().unwrap();
        encoded.push(0);
        assert!(Chunk::decode(&encoded).is_err());
    }

    #[test]
    fn batch_merge_preserves_existing_and_arrival_order_on_equal_timestamps() {
        let mut chunk = Chunk {
            n_fields: 2,
            ts: vec![10, 20],
            vals: vec![10.0, 10.5, 20.0, 20.5],
        };
        let incoming = [
            Point {
                ts: 20,
                values: vec![21.0, 21.5],
            },
            Point {
                ts: 5,
                values: vec![5.0, 5.5],
            },
            Point {
                ts: 20,
                values: vec![22.0, 22.5],
            },
            Point {
                ts: 15,
                values: vec![15.0, 15.5],
            },
        ];
        let mut refs: Vec<&Point> = incoming.iter().collect();

        chunk.merge_points(&mut refs);

        assert_eq!(chunk.ts, vec![5, 10, 15, 20, 20, 20]);
        assert_eq!(
            chunk.vals,
            vec![5.0, 5.5, 10.0, 10.5, 15.0, 15.5, 20.0, 20.5, 21.0, 21.5, 22.0, 22.5]
        );
    }

    #[test]
    fn range_uses_exact_sorted_window_with_duplicate_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(&dir.path().join("series.redb")).unwrap();
        let points = vec![
            Point::single(30, 3.0),
            Point::single(10, 1.0),
            Point::single(20, 2.0),
            Point::single(20, 2.5),
            Point::single(40, 4.0),
        ];
        store
            .append_batch("series", 1, 1_000, &["value".into()], &points)
            .unwrap();

        let got = store.range("series", 20, 40).unwrap();
        assert_eq!(
            got,
            vec![
                Point::single(20, 2.0),
                Point::single(20, 2.5),
                Point::single(30, 3.0),
            ]
        );
        assert!(store.range("series", 40, 20).unwrap().is_empty());
    }

    #[test]
    fn narrow_range_does_not_decode_future_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(&dir.path().join("series.redb")).unwrap();
        store
            .append_batch(
                "series",
                1,
                10,
                &["value".into()],
                &[Point::single(1, 1.0), Point::single(101, 101.0)],
            )
            .unwrap();

        // Make the future bucket undecodable, through the ONLY write path this
        // crate now has: an admitted maintenance mutation. A correctly
        // upper-bounded redb range for [0, 10) never reads it; the old series-max
        // bound did and errored.
        store
            .maintain("test-corrupt", "series", |wtx| {
                wtx.open_series_table(SERIES_CHUNKS)?
                    .insert(("series", 100), &[0u8][..])
                    .map_err(redb_err)?;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            store.range("series", 0, 10).unwrap(),
            vec![Point::single(1, 1.0)]
        );
    }
}

#[cfg(test)]
mod delete_series_tests {
    use super::*;
    use crate::dev_scope_grant::open_dev_store as open_test_store;

    fn tmp_store() -> SeriesStore {
        let path = std::env::temp_dir().join(format!(
            "eg-tsdb-delseries-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        open_test_store(&path).expect("open temp series store")
    }

    const BUCKET_NS: u64 = 3_600_000_000_000; // 1h buckets
    fn fields() -> Vec<String> {
        vec!["value".to_string()]
    }

    /// `delete_series` removes every chunk + the meta row; a subsequent `meta` is None
    /// and a range scan is empty. The removal primitive the temporal index uses on a
    /// node removal (CONCEPT:EG-KG.storage.incremental-temporal).
    #[test]
    fn delete_series_removes_all_and_meta() {
        let s = tmp_store();
        let pts: Vec<Point> = (0..10).map(|i| Point::single(i * 1000, i as f64)).collect();
        s.append_batch("g\u{0}n1", 1, BUCKET_NS, &fields(), &pts)
            .unwrap();
        assert!(s.meta("g\u{0}n1").unwrap().is_some());
        let dropped = s.delete_series("g\u{0}n1").unwrap();
        assert!(dropped >= 1, "at least one chunk removed");
        assert!(s.meta("g\u{0}n1").unwrap().is_none(), "meta row gone");
        assert!(
            s.scan_all("g\u{0}n1").unwrap().is_empty(),
            "no points remain"
        );
        // Deleting an unknown series is a harmless no-op.
        assert_eq!(s.delete_series("g\u{0}nope").unwrap(), 0);
    }

    /// EQUIVALENCE (CONCEPT:EG-KG.storage.incremental-temporal): an INCREMENTAL sequence
    /// — append n1, append n2, then remove n1 (delete_series) — leaves the store in the
    /// IDENTICAL state (per-series `scan_all`) as a full REBUILD that only appended the
    /// survivor n2. This is the primitive the `GraphTemporalIndex.apply_delta` (add +
    /// remove) reduces to.
    #[test]
    fn incremental_delete_equals_rebuild_from_survivors() {
        let n1: Vec<Point> = (0..5).map(|i| Point::single(i * 1000, i as f64)).collect();
        let n2: Vec<Point> = (0..7)
            .map(|i| Point::single(i * 500, (i * 3) as f64))
            .collect();

        // Incremental: add both, then remove n1.
        let inc = tmp_store();
        inc.append_batch("g\u{0}n1", 1, BUCKET_NS, &fields(), &n1)
            .unwrap();
        inc.append_batch("g\u{0}n2", 1, BUCKET_NS, &fields(), &n2)
            .unwrap();
        inc.delete_series("g\u{0}n1").unwrap();

        // Rebuild: only the survivor.
        let base = tmp_store();
        base.append_batch("g\u{0}n2", 1, BUCKET_NS, &fields(), &n2)
            .unwrap();

        assert!(inc.scan_all("g\u{0}n1").unwrap().is_empty());
        assert_eq!(
            inc.scan_all("g\u{0}n2").unwrap(),
            base.scan_all("g\u{0}n2").unwrap(),
            "incremental survivor series must equal the rebuild baseline"
        );
        assert_eq!(
            inc.meta("g\u{0}n1").unwrap(),
            base.meta("g\u{0}n1").unwrap()
        );
    }

    /// Idempotent REPLACE (the node-UPDATE path): delete_series then re-append yields
    /// EXACTLY the new series (no stale points from the prior value), == appending the
    /// new series into a fresh store.
    #[test]
    fn delete_then_reappend_is_exact_replace() {
        let old: Vec<Point> = (0..9).map(|i| Point::single(i * 1000, 1.0)).collect();
        let new: Vec<Point> = (0..3).map(|i| Point::single(i * 2000, 9.0)).collect();

        let s = tmp_store();
        s.append_batch("g\u{0}n", 1, BUCKET_NS, &fields(), &old)
            .unwrap();
        s.delete_series("g\u{0}n").unwrap();
        s.append_batch("g\u{0}n", 1, BUCKET_NS, &fields(), &new)
            .unwrap();

        let base = tmp_store();
        base.append_batch("g\u{0}n", 1, BUCKET_NS, &fields(), &new)
            .unwrap();

        assert_eq!(
            s.scan_all("g\u{0}n").unwrap(),
            base.scan_all("g\u{0}n").unwrap()
        );
    }
}

#[cfg(test)]
mod scoped_series_tests {
    use super::*;
    use crate::dev_scope_grant::open_dev_store as open_test_store;

    const BUCKET_NS: u64 = 1_000;

    #[test]
    fn canonical_key_roundtrips_without_separator_collisions() {
        let key = SeriesKey::new("tenant:a", "tenant:a/graph:one", "cpu:0/usage");
        assert_eq!(SeriesKey::decode(&key.encode()), Some(key));
        assert!(SeriesKey::decode("unscoped-series").is_none());
    }

    #[test]
    fn equal_local_ids_are_isolated_by_tenant_and_graph() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(&dir.path().join("series.redb")).unwrap();
        let a = SeriesKey::new("acme", "acme:billing", "cpu");
        let b = SeriesKey::new("other", "other:billing", "cpu");
        store
            .append_scoped(&a, 1, BUCKET_NS, &["v".into()], &[Point::single(1, 10.0)])
            .unwrap();
        store
            .append_scoped(&b, 1, BUCKET_NS, &["v".into()], &[Point::single(1, 20.0)])
            .unwrap();
        assert_eq!(store.scan_all_scoped(&a).unwrap()[0].values, vec![10.0]);
        assert_eq!(store.scan_all_scoped(&b).unwrap()[0].values, vec![20.0]);
    }
}
