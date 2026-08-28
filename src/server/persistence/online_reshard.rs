//! Online single-node per-tenant resharding execution (CONCEPT:EG-KG.backend.catalog-shard-resolve, M3 keystone).
//!
//! ## What it solves
//!
//! [`super::shard_migrate`] (CONCEPT:EG-KG.sharding.atomic-shard-swap) moves shards OFFLINE — the engine must be
//! stopped because it rewrites the whole store to a new uniform K. This module moves ONE
//! graph between shards while the engine RUNS, with no data loss and no stop: the graph's
//! rows are copied verbatim to the destination shard, the [`super::tenant_catalog`]
//! (CONCEPT:EG-KG.sharding.empty-catalog-routing) route is flipped so reads/writes follow the graph to its new shard,
//! and the source rows are GC'd — all without touching any OTHER graph's writers.
//!
//! ## Correctness — the same verbatim row copy as EG-030
//!
//! Like the offline tool, the per-graph copy is **verbatim** (stored bytes are NOT
//! decoded / unsealed / re-derived):
//!
//! * Per-graph data (`NODES`/`EDGES`/`LEDGER`/`SEMANTIC`/`GRAPH_META`) plus
//!   MutationBatch replay/outbox/fence rows and governed ChangeEnvelope content,
//!   cursor, policy, evidence, feature, blob, and lineage rows are moved row for
//!   row, value blob unchanged — so encryption-at-rest blobs survive WITHOUT the key.
//! * Every RESOURCE_*, development_lane_*, and capacity_lease_* table, plus
//!   provenance-anchor-member rows and the WorkItem command sequence, move with the
//!   graph too (WD5-BUG-04 — the same class of gap BUG-CX-016/BUG-CX-054 were in
//!   `shard_migrate.rs`'s offline tool). NOT moved: `PLAN_MATVIEWS`/
//!   `MATVIEW_OPERATOR_STATE` (GLOBAL, shard-0-homed, no graph key — moving them
//!   per-graph would be its own corruption), and the `work_item_capability` trio
//!   (`CAPABILITIES`/`INVOCATIONS`/`NATIVE_WORK_ITEMS`), which is deliberately
//!   purged rather than copied — see below.
//! * Native WorkItem capability bytes, invocation idempotency, and claim
//!   provenance are intentionally NOT part of the raw image.  Destination raw
//!   and delta imports purge those private tables in the same transaction, so a
//!   shard reuse/retry invalidates every old capability before copied WorkItem
//!   rows become visible; the next lease must be claimed natively again.
//! * The tamper-evident hash-chained `AUDIT` log (CONCEPT:EG-KG.sharding.row-level-security) is copied verbatim
//!   `(graph, seq) -> blob`, so the chain stays verifiable (re-deriving would break it).
//!
//! ## Correctness — the quiesce / flip window
//!
//! The move is driven by [`RedbBackend::reshard_graph`](super::redb_backend::RedbBackend),
//! which holds the backend's `routing_epoch` WRITE guard for the duration of the move.
//! Every catalog-attached durable write (`record_durable` / `commit_crossmodal`) resolves
//! its shard and enqueues its op while holding a SHARED `routing_epoch` READ guard, so the
//! exclusive flip cannot interleave: when the flip holds the write guard, no write is
//! mid-resolve, and once it releases, every subsequent write resolves the catalog AFTER
//! the route flip — so a write is never lost or routed to the stale shard. The ordering
//! inside the move — `import(dst) durably committed -> catalog flip durable -> purge(src)`
//! — is crash-consistent: a crash before the flip leaves the data in BOTH shards with the
//! route still on `src` (reads find it on `src`); a crash after the flip leaves the route
//! on `dst` where `import` already landed the data; the `src` GC is the last, idempotent
//! step. See `RedbBackend::reshard_graph` for the quiesce wiring and the round-trip tests.

use std::collections::BTreeSet;
use std::sync::mpsc::SyncSender;

use redb::{Database, Durability, ReadableDatabase, ReadableTable};

use super::redb_backend::Cmd;
use super::tenant_catalog::TenantCatalog;
use crate::protocol::GraphType;
#[cfg(feature = "security")]
use crate::redb_store::AUDIT;
use crate::redb_store::{
    clear_change_material_rows, clear_graph_rows, decode_graph_meta_identity, sanitize,
    CHANGE_BLOBS, CHANGE_CURSORS, CHANGE_ENVELOPES, CHANGE_EVIDENCE, CHANGE_FEATURES,
    CHANGE_LINEAGE, CHANGE_POLICIES, CONTENT_VERSIONS, EDGES, GRAPH_META, LEDGER, MUTATION_BATCHES,
    MUTATION_FENCE, MUTATION_GRAPH_VERSION, MUTATION_IDEMPOTENCY, MUTATION_LIFECYCLE_HEAD,
    MUTATION_OUTBOX, MUTATION_OUTBOX_DELIVERY, MUTATION_PROJECTION_CURSOR, NODES, SEMANTIC,
};
// BUG-CX-016/BUG-CX-054 class, same 25-of-30-table gap WD3-BUG-01 fixed offline in
// `shard_migrate.rs` and this lane fixed in `backup.rs`: an online reshard moves ONE
// graph, so it needs every PER-GRAPH table below, routed with the graph — but NOT
// `PLAN_MATVIEWS`/`MATVIEW_OPERATOR_STATE` (global, shard-0-homed, no graph key at
// all) and NOT the `work_item_capability` trio (`CAPABILITIES`/`INVOCATIONS`/
// `NATIVE_WORK_ITEMS`), which this module already deliberately excludes from the raw
// image by design (see the module doc + `import_graph_raw`/`import_graph_delta`'s
// `work_item_capability::clear_graph_rows_in_wtx` purge) — see this lane's report for
// the full per-table per-graph-vs-global classification.
use crate::redb_store::{capacity_lease, development_lane};
#[cfg(feature = "security")]
use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
use crate::redb_store::{
    RESOURCE_ANTI_AFFINITY, RESOURCE_CONCURRENCY, RESOURCE_DISK_POLICIES, RESOURCE_EXCLUSIVITY,
    RESOURCE_FAIRNESS, RESOURCE_HOSTS, RESOURCE_RESERVATIONS, RESOURCE_RESERVATION_ATTEMPTS,
    RESOURCE_RESERVATION_TENANT_INDEX, WORK_ITEM_COMMAND_SEQUENCE,
};

/// Deserialize an explicitly present nullable field in the current raw-row
/// snapshot contract. Serde's intrinsic `Option<T>` handling otherwise accepts
/// an omitted field as `None`, masking an older snapshot schema.
fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

/// Raw graph-scoped MutationBatch authority. Batch/outbox tables use batch ids
/// as their first key, so export discovers those ids through the graph-scoped
/// idempotency index and then copies the referenced rows verbatim.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawMutationRows {
    pub idempotency: Vec<(String, String, String)>,
    pub batches: Vec<(String, Vec<u8>)>,
    pub outbox: Vec<(String, u32, Vec<u8>)>,
    pub deliveries: Vec<(String, u32, String, Vec<u8>)>,
    pub projection_cursors: Vec<(String, String, Vec<u8>)>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub graph_version: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fence: Option<Vec<u8>>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lifecycle_head: Option<String>,
}

/// Raw governed ChangeEnvelope material; every key is graph-first on disk.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawChangeRows {
    pub envelopes: Vec<(String, Vec<u8>)>,
    pub content_versions: Vec<(String, String, Vec<u8>)>,
    pub cursors: Vec<(String, String, String, Vec<u8>)>,
    pub blobs: Vec<(String, String, Vec<u8>)>,
    pub features: Vec<(String, String, Vec<u8>)>,
    pub evidence: Vec<(String, String, Vec<u8>)>,
    pub policies: Vec<(String, String, Vec<u8>)>,
    pub lineage: Vec<(String, String, Vec<u8>)>,
}

/// RESOURCE_* rows for ONE graph, captured verbatim (BUG-CX-054 class — the same
/// gap `shard_migrate.rs`'s `MigrationReport::capability_and_resource` fixed offline;
/// see this module's own doc comment for why an online move needs it too).
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawResourceRows {
    pub reservations: Vec<(String, Vec<u8>)>,
    pub reservation_tenant_index: Vec<(String, String, String)>,
    pub reservation_attempts: Vec<(String, u64, String)>,
    pub hosts: Vec<(String, Vec<u8>)>,
    pub exclusivity: Vec<(String, String)>,
    pub fairness: Vec<(String, Vec<u8>)>,
    pub concurrency: Vec<(String, u64)>,
    pub anti_affinity: Vec<(String, String, u64)>,
    pub disk_policies: Vec<(String, Vec<u8>)>,
}

/// development_lane_* rows for ONE graph, captured verbatim (BUG-CX-054/096 class).
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawDevelopmentLaneRows {
    pub holds: Vec<(String, Vec<u8>)>,
    pub tenant_index: Vec<(String, String, String)>,
    pub lane_index: Vec<(String, String, String)>,
    pub repository_branch_index: Vec<(String, String, String)>,
    pub worktree_index: Vec<(String, String)>,
    pub work_item_index: Vec<(String, u64, String)>,
    pub counters: Vec<(String, Vec<u8>)>,
    pub pressure_index: Vec<(String, String, String, u64, String, u8)>,
    pub policies: Vec<(String, Vec<u8>)>,
    pub invocations: Vec<(String, String, Vec<u8>)>,
}

/// capacity_lease_* rows for ONE graph, captured verbatim (undocumented gap in the
/// same class as BUG-CX-054, found by this lane's mechanical table inventory).
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCapacityLeaseRows {
    pub cells: Vec<(String, Vec<u8>)>,
    pub leases: Vec<(String, Vec<u8>)>,
    pub usage: Vec<(String, Vec<u8>)>,
    pub idempotency: Vec<(String, String, Vec<u8>)>,
}

/// One graph's durable rows captured VERBATIM for an online shard move (CONCEPT:EG-KG.backend.catalog-shard-resolve).
/// Value blobs are the raw on-disk bytes (encrypted if encryption-at-rest is on, the
/// audit chain untouched) so re-inserting them on the destination shard preserves both
/// encryption and audit-chain verifiability — exactly as EG-030's offline copy does.
pub(crate) const RAW_GRAPH_ROWS_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawGraphRows {
    /// Exact current raw-row contract. Missing or unsupported versions are
    /// rejected before an import transaction starts.
    pub schema_version: u16,
    /// `graph_meta` identity blob (`{name, graph_type}`), or `None` if the graph has no
    /// durable identity (nothing to move — the reshard becomes a pure route flip).
    #[serde(deserialize_with = "deserialize_required_option")]
    pub meta: Option<Vec<u8>>,
    /// `(node_id, raw_value_blob)`.
    pub nodes: Vec<(String, Vec<u8>)>,
    /// `(src_id, tgt_id, ordinal, raw_value_blob)`.
    pub edges: Vec<(String, String, u32, Vec<u8>)>,
    /// `(seq, ledger_line)`.
    pub ledger: Vec<(u64, String)>,
    /// The semantic-store blob, if any.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub semantic: Option<Vec<u8>>,
    /// `(seq, chained_audit_blob)` — copied verbatim to keep the hash chain valid.
    #[cfg(feature = "security")]
    pub audit: Vec<(u64, Vec<u8>)>,
    /// Mutation status/idempotency/outbox rows needed for replay after the move.
    pub mutation: RawMutationRows,
    /// Governed external-change material and typed version/cursor rows.
    pub change: RawChangeRows,
    /// Every RESOURCE_* row for this graph (BUG-CX-054 class — see this module's
    /// doc comment's classification note).
    pub resource: RawResourceRows,
    /// Every development_lane_* row for this graph (BUG-CX-054/096 class).
    pub development_lane: RawDevelopmentLaneRows,
    /// Every capacity_lease_* row for this graph (undocumented gap, same class).
    pub capacity_lease: RawCapacityLeaseRows,
    /// `(seq, anchor_member_blob)` — Merkle inclusion-proof anchor members
    /// (BUG-CX-016 class), copied verbatim like `audit`.
    #[cfg(feature = "security")]
    pub provenance_anchor_members: Vec<(u64, Vec<u8>)>,
    /// The graph's monotonic native WorkItem command sequence, if any (BUG-CX-054 class).
    #[serde(deserialize_with = "deserialize_required_option")]
    pub work_item_command_sequence: Option<u64>,
}

impl Default for RawGraphRows {
    fn default() -> Self {
        Self {
            schema_version: RAW_GRAPH_ROWS_SCHEMA_VERSION,
            meta: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            ledger: Vec::new(),
            semantic: None,
            #[cfg(feature = "security")]
            audit: Vec::new(),
            mutation: RawMutationRows::default(),
            change: RawChangeRows::default(),
            resource: RawResourceRows::default(),
            development_lane: RawDevelopmentLaneRows::default(),
            capacity_lease: RawCapacityLeaseRows::default(),
            #[cfg(feature = "security")]
            provenance_anchor_members: Vec::new(),
            work_item_command_sequence: None,
        }
    }
}

impl RawGraphRows {
    pub(crate) fn validate_schema(&self) -> Result<(), String> {
        if self.schema_version != RAW_GRAPH_ROWS_SCHEMA_VERSION {
            return Err(format!(
                "unsupported raw graph rows schema {} (expected {})",
                self.schema_version, RAW_GRAPH_ROWS_SCHEMA_VERSION
            ));
        }
        Ok(())
    }

    /// Validate the raw row set and derive its identity from `graph_meta`.
    /// A raw image may be completely empty during a route-only reshard, but it
    /// may never carry graph-scoped authority without the identity that owns it.
    pub(crate) fn durable_identity(
        &self,
        graph: &str,
    ) -> Result<Option<(String, GraphType, String)>, String> {
        self.validate_schema()?;
        if let Some(meta) = self.meta.as_deref() {
            let identity = decode_graph_meta_identity(graph, meta)?;
            if sanitize(&identity.0) != graph {
                return Err("raw graph rows durable identity does not match its key".to_string());
            }
            return Ok(Some(identity));
        }

        #[cfg(feature = "security")]
        let has_audit_or_provenance =
            !self.audit.is_empty() || !self.provenance_anchor_members.is_empty();
        #[cfg(not(feature = "security"))]
        let has_audit_or_provenance = false;
        let has_authority = !self.nodes.is_empty()
            || !self.edges.is_empty()
            || !self.ledger.is_empty()
            || self.semantic.is_some()
            || has_audit_or_provenance
            || self.mutation != RawMutationRows::default()
            || self.change != RawChangeRows::default()
            || self.resource != RawResourceRows::default()
            || self.development_lane != RawDevelopmentLaneRows::default()
            || self.capacity_lease != RawCapacityLeaseRows::default()
            || self.work_item_command_sequence.is_some();
        if has_authority {
            return Err("raw graph rows contain authority without durable identity".to_string());
        }
        Ok(None)
    }

    /// Total rows across every WD5-BUG-04 table (RESOURCE_*, development_lane_*,
    /// capacity_lease_*, provenance-anchor-members, WorkItem command sequence) —
    /// the `ReshardReport::capability_and_resource` source.
    fn capability_and_resource_row_count(&self) -> u64 {
        let mut count = self.resource.reservations.len()
            + self.resource.reservation_tenant_index.len()
            + self.resource.reservation_attempts.len()
            + self.resource.hosts.len()
            + self.resource.exclusivity.len()
            + self.resource.fairness.len()
            + self.resource.concurrency.len()
            + self.resource.anti_affinity.len()
            + self.resource.disk_policies.len()
            + self.development_lane.holds.len()
            + self.development_lane.tenant_index.len()
            + self.development_lane.lane_index.len()
            + self.development_lane.repository_branch_index.len()
            + self.development_lane.worktree_index.len()
            + self.development_lane.work_item_index.len()
            + self.development_lane.counters.len()
            + self.development_lane.pressure_index.len()
            + self.development_lane.policies.len()
            + self.development_lane.invocations.len()
            + self.capacity_lease.cells.len()
            + self.capacity_lease.leases.len()
            + self.capacity_lease.usage.len()
            + self.capacity_lease.idempotency.len();
        #[cfg(feature = "security")]
        {
            count += self.provenance_anchor_members.len();
        }
        count += self.work_item_command_sequence.is_some() as usize;
        count as u64
    }
}

/// Per-table counts of a completed online reshard (CONCEPT:EG-KG.backend.catalog-shard-resolve).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReshardReport {
    pub graph: String,
    pub from_shard: usize,
    pub to_shard: usize,
    pub nodes: u64,
    pub edges: u64,
    pub ledger: u64,
    pub semantic: u64,
    pub audit: u64,
    /// Rows copied under the EXCLUSIVE routing quiesce (CONCEPT:EG-KG.backend.flush-pending-first, R1 delta-copy) —
    /// the work that actually pauses the moved graph's writes. For an idle graph this is
    /// 0 (the whole graph rode the unquiesced bulk pass), so `delta_nodes + delta_edges
    /// << nodes + edges` is the proof the snapshot+delta path shrank the pause.
    pub delta_nodes: u64,
    pub delta_edges: u64,
    /// Rows copied from the tables this lane found missing from the online move
    /// (WD5-BUG-04, same class as `shard_migrate.rs`'s `MigrationReport::
    /// capability_and_resource`): every RESOURCE_*, development_lane_*, and
    /// capacity_lease_* row, plus provenance-anchor-member rows and the WorkItem
    /// command sequence — counted separately so a reshard report makes this
    /// coverage independently auditable.
    pub capability_and_resource: u64,
    /// `true` when the graph already routed to the target shard (nothing moved).
    pub no_op: bool,
}

impl ReshardReport {
    /// The graph already lives on the requested shard — nothing to move.
    pub fn no_op(graph: &str, shard: usize) -> Self {
        ReshardReport {
            graph: graph.to_string(),
            from_shard: shard,
            to_shard: shard,
            no_op: true,
            ..Default::default()
        }
    }

    fn counts(graph: &str, from: usize, to: usize, rows: &RawGraphRows) -> Self {
        ReshardReport {
            graph: graph.to_string(),
            from_shard: from,
            to_shard: to,
            nodes: rows.nodes.len() as u64,
            edges: rows.edges.len() as u64,
            ledger: rows.ledger.len() as u64,
            semantic: rows.semantic.is_some() as u64,
            #[cfg(feature = "security")]
            audit: rows.audit.len() as u64,
            #[cfg(not(feature = "security"))]
            audit: 0,
            delta_nodes: 0,
            delta_edges: 0,
            capability_and_resource: rows.capability_and_resource_row_count(),
            no_op: false,
        }
    }
}

/// The DELTA between a bulk snapshot (already on the destination shard) and the latest
/// source state, captured under the exclusive quiesce (CONCEPT:EG-KG.backend.flush-pending-first, R1). Only the rows
/// that changed/appeared since the bulk pass are upserted, and rows that DISAPPEARED (a
/// node/edge deleted on `src` between the bulk snapshot and the flip) are removed from
/// `dst` — so a deleted row can never be resurrected on the destination. Ledger/audit are
/// append-only (KG-2.231), so they only ever gain rows. Bounded by the writes that landed
/// during the unquiesced bulk copy — typically tiny, the whole point of the optimization.
#[derive(Default)]
pub(crate) struct RawGraphDelta {
    /// `Some(blob)` to re-write `graph_meta` when it changed (rare); `None` = unchanged.
    pub meta: Option<Vec<u8>>,
    pub upsert_nodes: Vec<(String, Vec<u8>)>,
    pub remove_nodes: Vec<String>,
    pub upsert_edges: Vec<(String, String, u32, Vec<u8>)>,
    pub remove_edges: Vec<(String, String, u32)>,
    pub upsert_ledger: Vec<(u64, String)>,
    /// `Some(Some(blob))` = re-write the semantic blob; `Some(None)` = it was cleared;
    /// `None` = unchanged.
    #[allow(clippy::option_option)]
    pub semantic: Option<Option<Vec<u8>>>,
    #[cfg(feature = "security")]
    pub upsert_audit: Vec<(u64, Vec<u8>)>,
    /// Auxiliary authority changes are rare and small relative to graph rows;
    /// when any changed during bulk copy, replace the graph's set atomically.
    pub replace_mutation: Option<Box<RawMutationRows>>,
    pub replace_change: Option<Box<RawChangeRows>>,
    /// WD5-BUG-04: the same "replace the whole set atomically when anything in it
    /// changed" strategy as `replace_mutation`/`replace_change`, extended to the
    /// tables this lane found missing from the online move.
    pub replace_resource: Option<Box<RawResourceRows>>,
    pub replace_development_lane: Option<Box<RawDevelopmentLaneRows>>,
    pub replace_capacity_lease: Option<Box<RawCapacityLeaseRows>>,
    #[cfg(feature = "security")]
    pub replace_provenance_anchor_members: Option<Vec<(u64, Vec<u8>)>>,
    /// `Some(Some(seq))` = re-write; `Some(None)` = it was cleared; `None` = unchanged.
    #[allow(clippy::option_option)]
    pub work_item_command_sequence: Option<Option<u64>>,
}

impl RawGraphDelta {
    /// Total node + edge rows in this delta — the work done under the exclusive quiesce.
    fn nodes(&self) -> u64 {
        (self.upsert_nodes.len() + self.remove_nodes.len()) as u64
    }
    fn edges(&self) -> u64 {
        (self.upsert_edges.len() + self.remove_edges.len()) as u64
    }
}

/// Diff the LATEST source rows against the BULK snapshot already on `dst` (CONCEPT:EG-KG.backend.flush-pending-first).
/// Emits only what changed: new/changed-blob rows to upsert + vanished rows to remove.
pub(crate) fn compute_delta(bulk: &RawGraphRows, latest: &RawGraphRows) -> RawGraphDelta {
    use std::collections::HashMap;
    let mut delta = RawGraphDelta::default();

    if bulk.meta != latest.meta {
        delta.meta = latest.meta.clone();
    }
    if bulk.semantic != latest.semantic {
        delta.semantic = Some(latest.semantic.clone());
    }

    // Nodes: keyed by id. Upsert new/changed; remove ids gone from `latest`.
    let base_nodes: HashMap<&str, &Vec<u8>> =
        bulk.nodes.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let latest_node_ids: std::collections::HashSet<&str> =
        latest.nodes.iter().map(|(k, _)| k.as_str()).collect();
    for (id, blob) in &latest.nodes {
        match base_nodes.get(id.as_str()) {
            Some(prev) if *prev == blob => {}
            _ => delta.upsert_nodes.push((id.clone(), blob.clone())),
        }
    }
    for (id, _) in &bulk.nodes {
        if !latest_node_ids.contains(id.as_str()) {
            delta.remove_nodes.push(id.clone());
        }
    }

    // Edges: keyed by (src, tgt, ordinal).
    type EdgeKey = (String, String, u32);
    let base_edges: HashMap<EdgeKey, &Vec<u8>> = bulk
        .edges
        .iter()
        .map(|(s, t, o, v)| ((s.clone(), t.clone(), *o), v))
        .collect();
    let latest_edge_keys: std::collections::HashSet<EdgeKey> = latest
        .edges
        .iter()
        .map(|(s, t, o, _)| (s.clone(), t.clone(), *o))
        .collect();
    for (s, t, o, blob) in &latest.edges {
        let key = (s.clone(), t.clone(), *o);
        match base_edges.get(&key) {
            Some(prev) if *prev == blob => {}
            _ => delta
                .upsert_edges
                .push((s.clone(), t.clone(), *o, blob.clone())),
        }
    }
    for (s, t, o, _) in &bulk.edges {
        let key = (s.clone(), t.clone(), *o);
        if !latest_edge_keys.contains(&key) {
            delta.remove_edges.push(key);
        }
    }

    // Ledger is append-only: copy entries with seq beyond the bulk's tail.
    let base_ledger_max = bulk.ledger.iter().map(|(seq, _)| *seq).max();
    for (seq, line) in &latest.ledger {
        if base_ledger_max.is_none_or(|m| *seq > m) {
            delta.upsert_ledger.push((*seq, line.clone()));
        }
    }

    #[cfg(feature = "security")]
    {
        let base_audit_max = bulk.audit.iter().map(|(seq, _)| *seq).max();
        for (seq, blob) in &latest.audit {
            if base_audit_max.is_none_or(|m| *seq > m) {
                delta.upsert_audit.push((*seq, blob.clone()));
            }
        }
    }

    if bulk.mutation != latest.mutation {
        delta.replace_mutation = Some(Box::new(latest.mutation.clone()));
    }
    if bulk.change != latest.change {
        delta.replace_change = Some(Box::new(latest.change.clone()));
    }
    compute_capability_and_resource_delta(bulk, latest, &mut delta);

    delta
}

/// WD5-BUG-04: the same "replace the whole set atomically when anything in it
/// changed" diff as `compute_delta`'s mutation/change checks above, extended to the
/// tables this lane found missing from the online move (RESOURCE_*,
/// development_lane_*, capacity_lease_*, provenance-anchor-members, WorkItem
/// command sequence). Split out so `compute_delta` itself stays under the
/// complexity cap.
fn compute_capability_and_resource_delta(
    bulk: &RawGraphRows,
    latest: &RawGraphRows,
    delta: &mut RawGraphDelta,
) {
    if bulk.resource != latest.resource {
        delta.replace_resource = Some(Box::new(latest.resource.clone()));
    }
    if bulk.development_lane != latest.development_lane {
        delta.replace_development_lane = Some(Box::new(latest.development_lane.clone()));
    }
    if bulk.capacity_lease != latest.capacity_lease {
        delta.replace_capacity_lease = Some(Box::new(latest.capacity_lease.clone()));
    }
    #[cfg(feature = "security")]
    if bulk.provenance_anchor_members != latest.provenance_anchor_members {
        delta.replace_provenance_anchor_members = Some(latest.provenance_anchor_members.clone());
    }
    if bulk.work_item_command_sequence != latest.work_item_command_sequence {
        delta.work_item_command_sequence = Some(latest.work_item_command_sequence);
    }
}

// D-P0-U04: the scan-and-remove logic for MUTATION_IDEMPOTENCY/MUTATION_BATCHES/
// MUTATION_OUTBOX/MUTATION_OUTBOX_DELIVERY/MUTATION_PROJECTION_CURSOR/
// MUTATION_GRAPH_VERSION/MUTATION_FENCE/MUTATION_LIFECYCLE_HEAD now lives ONCE in
// the server-independent `redb_store` layer (`clear_mutation_authority_rows`) --
// `Method::DeleteGraph`'s own commit path uses the SAME function so an ordinary
// graph delete purges prior-incarnation mutation authority exactly like a source
// shard does after an online move, instead of carrying two copies of this table
// set that could silently drift apart.
fn clear_mutation_rows(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    crate::redb_store::clear_mutation_authority_rows(wtx, graph)
}

#[cfg(feature = "security")]
fn clear_audit_rows(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
    let sequences: Vec<u64> = audit
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| key.value().1)
        .collect();
    for sequence in sequences {
        audit.remove((graph, sequence)).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn import_mutation_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawMutationRows,
) -> Result<(), String> {
    clear_mutation_rows(wtx, graph)?;
    {
        let mut table = wtx
            .open_table(MUTATION_IDEMPOTENCY)
            .map_err(|e| e.to_string())?;
        for (tenant, idempotency, batch_id) in &rows.idempotency {
            table
                .insert(
                    (tenant.as_str(), graph, idempotency.as_str()),
                    batch_id.as_str(),
                )
                .map_err(|e| e.to_string())?;
        }
    }
    {
        let mut table = wtx
            .open_table(MUTATION_BATCHES)
            .map_err(|e| e.to_string())?;
        for (batch_id, blob) in &rows.batches {
            table
                .insert(batch_id.as_str(), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
    }
    {
        let mut table = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
        for (batch_id, ordinal, blob) in &rows.outbox {
            table
                .insert((batch_id.as_str(), *ordinal), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
    }
    {
        let mut table = wtx
            .open_table(MUTATION_OUTBOX_DELIVERY)
            .map_err(|e| e.to_string())?;
        for (batch_id, ordinal, consumer, blob) in &rows.deliveries {
            table
                .insert(
                    (batch_id.as_str(), *ordinal, consumer.as_str()),
                    blob.as_slice(),
                )
                .map_err(|e| e.to_string())?;
        }
    }
    {
        let mut table = wtx
            .open_table(MUTATION_PROJECTION_CURSOR)
            .map_err(|e| e.to_string())?;
        for (tenant, projection, blob) in &rows.projection_cursors {
            table
                .insert(
                    (tenant.as_str(), graph, projection.as_str()),
                    blob.as_slice(),
                )
                .map_err(|e| e.to_string())?;
        }
    }
    if let Some(version) = rows.graph_version {
        let mut table = wtx
            .open_table(MUTATION_GRAPH_VERSION)
            .map_err(|e| e.to_string())?;
        table.insert(graph, version).map_err(|e| e.to_string())?;
    }
    if let Some(fence) = &rows.fence {
        let mut table = wtx.open_table(MUTATION_FENCE).map_err(|e| e.to_string())?;
        table
            .insert(graph, fence.as_slice())
            .map_err(|e| e.to_string())?;
    }
    if let Some(head) = &rows.lifecycle_head {
        let mut table = wtx
            .open_table(MUTATION_LIFECYCLE_HEAD)
            .map_err(|e| e.to_string())?;
        table
            .insert(graph, head.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn import_change_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawChangeRows,
) -> Result<(), String> {
    clear_change_material_rows(wtx, graph)?;
    {
        let mut table = wtx
            .open_table(CHANGE_ENVELOPES)
            .map_err(|e| e.to_string())?;
        for (id, blob) in &rows.envelopes {
            table
                .insert((graph, id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
    }
    macro_rules! import_three_part {
        ($definition:expr, $values:expr) => {{
            let mut table = wtx.open_table($definition).map_err(|e| e.to_string())?;
            for (tenant, id, blob) in $values {
                table
                    .insert((graph, tenant.as_str(), id.as_str()), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }};
    }
    import_three_part!(CONTENT_VERSIONS, &rows.content_versions);
    import_three_part!(CHANGE_BLOBS, &rows.blobs);
    import_three_part!(CHANGE_FEATURES, &rows.features);
    import_three_part!(CHANGE_EVIDENCE, &rows.evidence);
    import_three_part!(CHANGE_POLICIES, &rows.policies);
    import_three_part!(CHANGE_LINEAGE, &rows.lineage);
    {
        let mut table = wtx.open_table(CHANGE_CURSORS).map_err(|e| e.to_string())?;
        for (tenant, source, partition, blob) in &rows.cursors {
            table
                .insert(
                    (graph, tenant.as_str(), source.as_str(), partition.as_str()),
                    blob.as_slice(),
                )
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Remove the source-side replay/outbox and audit authority after an online route flip.
/// Graph/material rows are purged by the ordinary helper; mutation rows need
/// this explicit second pass because tenant deletion intentionally retains its
/// terminal history while shard movement must transfer, then remove, it. The
/// audit chain follows the same rule under the `security` feature: tenant deletion
/// retains it, but a successful shard move must leave exactly one authoritative copy.
pub(crate) fn purge_moved_mutation_rows(db: &Database, graph: &str) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    clear_mutation_rows(&wtx, graph)?;
    #[cfg(feature = "security")]
    clear_audit_rows(&wtx, graph)?;
    wtx.commit().map_err(|e| e.to_string())
}

/// Apply a [`RawGraphDelta`] to the destination `Database` in ONE durable transaction
/// (CONCEPT:EG-KG.backend.flush-pending-first). Runs on the destination shard's writer thread (via
/// [`Cmd::ImportGraphDelta`](super::redb_backend::Cmd)); the single `Durability::Immediate`
/// commit is the delta's commit-before-flip point. O(delta) rows — the short under-quiesce
/// write, vs the full O(graph) copy the bulk pass already did unquiesced.
pub(crate) fn import_graph_delta(
    db: &Database,
    graph: &str,
    delta: &RawGraphDelta,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        // Capability/provenance rows are deliberately absent from the raw
        // reshard image.  Invalidate the destination's private authority on
        // every delta import before applying copied WorkItem rows; otherwise a
        // retry into a reused shard could make an old capability valid against
        // a replacement image.  The purge is in this same transaction as the
        // delta, so no partial destination authority is observable.
        crate::redb_store::work_item_capability::clear_graph_rows_in_wtx(&wtx, graph)?;
        if let Some(blob) = &delta.meta {
            let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
            meta.insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        for (id, blob) in &delta.upsert_nodes {
            nodes
                .insert((graph, id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        for id in &delta.remove_nodes {
            nodes
                .remove((graph, id.as_str()))
                .map_err(|e| e.to_string())?;
        }
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        for (s, t, o, blob) in &delta.upsert_edges {
            edges
                .insert((graph, s.as_str(), t.as_str(), *o), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        for (s, t, o) in &delta.remove_edges {
            edges
                .remove((graph, s.as_str(), t.as_str(), *o))
                .map_err(|e| e.to_string())?;
        }
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (seq, line) in &delta.upsert_ledger {
            ledger
                .insert((graph, *seq), line.as_str())
                .map_err(|e| e.to_string())?;
        }
        if let Some(sem) = &delta.semantic {
            let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            match sem {
                Some(blob) => {
                    semantic
                        .insert(graph, blob.as_slice())
                        .map_err(|e| e.to_string())?;
                }
                None => {
                    semantic.remove(graph).map_err(|e| e.to_string())?;
                }
            }
        }
        #[cfg(feature = "security")]
        {
            let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
            for (seq, blob) in &delta.upsert_audit {
                audit
                    .insert((graph, *seq), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        if let Some(rows) = &delta.replace_mutation {
            import_mutation_rows(&wtx, graph, rows)?;
        }
        if let Some(rows) = &delta.replace_change {
            import_change_rows(&wtx, graph, rows)?;
        }
        apply_capability_and_resource_delta(&wtx, graph, delta)?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Apply the WD5-BUG-04 delta pieces (RESOURCE_*, development_lane_*,
/// capacity_lease_*, provenance-anchor-members, WorkItem command sequence) inside
/// the caller's already-open write transaction — the same "only touch what
/// `compute_delta` marked changed" contract as `import_mutation_rows`/
/// `import_change_rows` above. Split out of `import_graph_delta` so that function
/// stays under the complexity cap.
fn apply_capability_and_resource_delta(
    wtx: &redb::WriteTransaction,
    graph: &str,
    delta: &RawGraphDelta,
) -> Result<(), String> {
    if let Some(rows) = &delta.replace_resource {
        import_resource_rows(wtx, graph, rows)?;
    }
    if let Some(rows) = &delta.replace_development_lane {
        import_development_lane_rows(wtx, graph, rows)?;
    }
    if let Some(rows) = &delta.replace_capacity_lease {
        import_capacity_lease_rows(wtx, graph, rows)?;
    }
    #[cfg(feature = "security")]
    if let Some(rows) = &delta.replace_provenance_anchor_members {
        clear_provenance_anchor_members_rows_raw(wtx, graph)?;
        insert_provenance_anchor_members_rows(wtx, graph, rows)?;
    }
    if let Some(seq) = delta.work_item_command_sequence {
        import_work_item_command_sequence(wtx, graph, seq)?;
    }
    Ok(())
}

// ── WD5-BUG-04: per-table raw export/clear/insert + group wrappers for the
// 25 per-graph tables (RESOURCE_*, development_lane_*, capacity_lease_*,
// provenance_anchor_members, work_item_command_sequence) this lane found missing
// from the online reshard raw image — same defect class as BUG-CX-016/BUG-CX-054
// in `shard_migrate.rs`, following its proven one-table-per-function template.
// Deliberately excludes PLAN_MATVIEWS/MATVIEW_OPERATOR_STATE (global, no graph
// key) and the work_item_capability trio (already deliberately purged-not-copied
// by this module's design — see the import functions' own doc comments).

// ── RESOURCE_* per-table raw export/clear/insert (BUG-CX-054 class, online reshard) ──

fn export_resource_reservations_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_RESERVATIONS) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, reservation_id) = k.value();
        if g != graph {
            break;
        }
        out.push((reservation_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_resource_reservations_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_RESERVATIONS).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, reservation_id) = k.value();
        if g != graph {
            break;
        }
        keys.push(reservation_id.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_reservations_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_RESERVATIONS).map_err(|e| e.to_string())?;
    for (reservation_id, value) in rows {
        table
            .insert((graph, reservation_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_reservation_tenant_index_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, index_key) = k.value();
        if g != graph {
            break;
        }
        out.push((tenant.to_string(), index_key.to_string(), v.value().to_string()));
    }
    Ok(out)
}

fn clear_resource_reservation_tenant_index_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, index_key) = k.value();
        if g != graph {
            break;
        }
        keys.push((tenant.to_string(), index_key.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_reservation_tenant_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX).map_err(|e| e.to_string())?;
    for (tenant, index_key, value) in rows {
        table
            .insert((graph, tenant.as_str(), index_key.as_str()), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_reservation_attempts_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, u64, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_RESERVATION_ATTEMPTS) else {
        return Ok(out);
    };
    for row in table.range((graph, "", 0u64)..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, reservation_id, attempt_seq) = k.value();
        if g != graph {
            break;
        }
        out.push((reservation_id.to_string(), attempt_seq, v.value().to_string()));
    }
    Ok(out)
}

fn clear_resource_reservation_attempts_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, u64)> = Vec::new();
    for row in table.range((graph, "", 0u64)..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, reservation_id, attempt_seq) = k.value();
        if g != graph {
            break;
        }
        keys.push((reservation_id.to_string(), attempt_seq));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1)).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_reservation_attempts_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, u64, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS).map_err(|e| e.to_string())?;
    for (reservation_id, attempt_seq, value) in rows {
        table
            .insert((graph, reservation_id.as_str(), *attempt_seq), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_hosts_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_HOSTS) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, host_id) = k.value();
        if g != graph {
            break;
        }
        out.push((host_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_resource_hosts_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_HOSTS).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, host_id) = k.value();
        if g != graph {
            break;
        }
        keys.push(host_id.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_hosts_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_HOSTS).map_err(|e| e.to_string())?;
    for (host_id, value) in rows {
        table
            .insert((graph, host_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_exclusivity_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_EXCLUSIVITY) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        out.push((key.to_string(), v.value().to_string()));
    }
    Ok(out)
}

fn clear_resource_exclusivity_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_EXCLUSIVITY).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        keys.push(key.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_exclusivity_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_EXCLUSIVITY).map_err(|e| e.to_string())?;
    for (key, value) in rows {
        table
            .insert((graph, key.as_str()), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_fairness_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_FAIRNESS) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        out.push((key.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_resource_fairness_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_FAIRNESS).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        keys.push(key.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_fairness_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_FAIRNESS).map_err(|e| e.to_string())?;
    for (key, value) in rows {
        table
            .insert((graph, key.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_concurrency_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, u64)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_CONCURRENCY) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        out.push((key.to_string(), v.value()));
    }
    Ok(out)
}

fn clear_resource_concurrency_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_CONCURRENCY).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        keys.push(key.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_concurrency_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, u64)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_CONCURRENCY).map_err(|e| e.to_string())?;
    for (key, value) in rows {
        table
            .insert((graph, key.as_str()), *value)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_anti_affinity_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, u64)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_ANTI_AFFINITY) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, key, reservation_id) = k.value();
        if g != graph {
            break;
        }
        out.push((key.to_string(), reservation_id.to_string(), v.value()));
    }
    Ok(out)
}

fn clear_resource_anti_affinity_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_ANTI_AFFINITY).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, key, reservation_id) = k.value();
        if g != graph {
            break;
        }
        keys.push((key.to_string(), reservation_id.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_anti_affinity_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, u64)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_ANTI_AFFINITY).map_err(|e| e.to_string())?;
    for (key, reservation_id, value) in rows {
        table
            .insert((graph, key.as_str(), reservation_id.as_str()), *value)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_resource_disk_policies_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(RESOURCE_DISK_POLICIES) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        out.push((key.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_resource_disk_policies_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_DISK_POLICIES).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, key) = k.value();
        if g != graph {
            break;
        }
        keys.push(key.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_resource_disk_policies_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(RESOURCE_DISK_POLICIES).map_err(|e| e.to_string())?;
    for (key, value) in rows {
        table
            .insert((graph, key.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// ── development_lane_* per-table raw export/clear/insert (BUG-CX-054/096 class, online reshard) ──

fn export_lane_holds_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::HOLDS) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, hold_id) = k.value();
        if g != graph {
            break;
        }
        out.push((hold_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_lane_holds_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::HOLDS).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, hold_id) = k.value();
        if g != graph {
            break;
        }
        keys.push(hold_id.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_holds_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::HOLDS).map_err(|e| e.to_string())?;
    for (hold_id, value) in rows {
        table
            .insert((graph, hold_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_tenant_index_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::TENANT_INDEX) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, index_key) = k.value();
        if g != graph {
            break;
        }
        out.push((tenant.to_string(), index_key.to_string(), v.value().to_string()));
    }
    Ok(out)
}

fn clear_lane_tenant_index_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::TENANT_INDEX).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, index_key) = k.value();
        if g != graph {
            break;
        }
        keys.push((tenant.to_string(), index_key.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_tenant_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::TENANT_INDEX).map_err(|e| e.to_string())?;
    for (tenant, index_key, value) in rows {
        table
            .insert((graph, tenant.as_str(), index_key.as_str()), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_index_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::LANE_INDEX) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, lane, index_key) = k.value();
        if g != graph {
            break;
        }
        out.push((lane.to_string(), index_key.to_string(), v.value().to_string()));
    }
    Ok(out)
}

fn clear_lane_index_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::LANE_INDEX).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, lane, index_key) = k.value();
        if g != graph {
            break;
        }
        keys.push((lane.to_string(), index_key.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::LANE_INDEX).map_err(|e| e.to_string())?;
    for (lane, index_key, value) in rows {
        table
            .insert((graph, lane.as_str(), index_key.as_str()), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_repository_branch_index_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::REPOSITORY_BRANCH_INDEX) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, repository, branch) = k.value();
        if g != graph {
            break;
        }
        out.push((repository.to_string(), branch.to_string(), v.value().to_string()));
    }
    Ok(out)
}

fn clear_lane_repository_branch_index_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::REPOSITORY_BRANCH_INDEX).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, repository, branch) = k.value();
        if g != graph {
            break;
        }
        keys.push((repository.to_string(), branch.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_repository_branch_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::REPOSITORY_BRANCH_INDEX).map_err(|e| e.to_string())?;
    for (repository, branch, value) in rows {
        table
            .insert((graph, repository.as_str(), branch.as_str()), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_worktree_index_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::WORKTREE_INDEX) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, worktree) = k.value();
        if g != graph {
            break;
        }
        out.push((worktree.to_string(), v.value().to_string()));
    }
    Ok(out)
}

fn clear_lane_worktree_index_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::WORKTREE_INDEX).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, worktree) = k.value();
        if g != graph {
            break;
        }
        keys.push(worktree.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_worktree_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::WORKTREE_INDEX).map_err(|e| e.to_string())?;
    for (worktree, value) in rows {
        table
            .insert((graph, worktree.as_str()), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_work_item_index_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, u64, String)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::WORK_ITEM_INDEX) else {
        return Ok(out);
    };
    for row in table.range((graph, "", 0u64)..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, work_item_id, seq) = k.value();
        if g != graph {
            break;
        }
        out.push((work_item_id.to_string(), seq, v.value().to_string()));
    }
    Ok(out)
}

fn clear_lane_work_item_index_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::WORK_ITEM_INDEX).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, u64)> = Vec::new();
    for row in table.range((graph, "", 0u64)..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, work_item_id, seq) = k.value();
        if g != graph {
            break;
        }
        keys.push((work_item_id.to_string(), seq));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1)).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_work_item_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, u64, String)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::WORK_ITEM_INDEX).map_err(|e| e.to_string())?;
    for (work_item_id, seq, value) in rows {
        table
            .insert((graph, work_item_id.as_str(), *seq), value.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_counters_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::COUNTERS) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, counter) = k.value();
        if g != graph {
            break;
        }
        out.push((counter.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_lane_counters_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::COUNTERS).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, counter) = k.value();
        if g != graph {
            break;
        }
        keys.push(counter.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_counters_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::COUNTERS).map_err(|e| e.to_string())?;
    for (counter, value) in rows {
        table
            .insert((graph, counter.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_pressure_index_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, String, u64, String, u8)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::PRESSURE_INDEX) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "", "", 0u64, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, lane, repository, ts, kind) = k.value();
        if g != graph {
            break;
        }
        out.push((tenant.to_string(), lane.to_string(), repository.to_string(), ts, kind.to_string(), v.value()));
    }
    Ok(out)
}

fn clear_lane_pressure_index_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::PRESSURE_INDEX).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String, String, u64, String)> = Vec::new();
    for row in table.range((graph, "", "", "", 0u64, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, lane, repository, ts, kind) = k.value();
        if g != graph {
            break;
        }
        keys.push((tenant.to_string(), lane.to_string(), repository.to_string(), ts, kind.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str(), key_val.2.as_str(), key_val.3, key_val.4.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_pressure_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, String, u64, String, u8)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::PRESSURE_INDEX).map_err(|e| e.to_string())?;
    for (tenant, lane, repository, ts, kind, value) in rows {
        table
            .insert((graph, tenant.as_str(), lane.as_str(), repository.as_str(), *ts, kind.as_str()), *value)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_policies_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::POLICIES) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, policy_id) = k.value();
        if g != graph {
            break;
        }
        out.push((policy_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_lane_policies_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::POLICIES).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, policy_id) = k.value();
        if g != graph {
            break;
        }
        keys.push(policy_id.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_policies_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::POLICIES).map_err(|e| e.to_string())?;
    for (policy_id, value) in rows {
        table
            .insert((graph, policy_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_lane_invocations_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(development_lane::INVOCATIONS) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, hold_id, invocation_id) = k.value();
        if g != graph {
            break;
        }
        out.push((hold_id.to_string(), invocation_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_lane_invocations_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::INVOCATIONS).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, hold_id, invocation_id) = k.value();
        if g != graph {
            break;
        }
        keys.push((hold_id.to_string(), invocation_id.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_lane_invocations_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(development_lane::INVOCATIONS).map_err(|e| e.to_string())?;
    for (hold_id, invocation_id, value) in rows {
        table
            .insert((graph, hold_id.as_str(), invocation_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// ── capacity_lease_* per-table raw export/clear/insert (undocumented BUG-CX-054-class gap, online reshard) ──

fn export_capacity_cells_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(capacity_lease::CELLS) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, cell_id) = k.value();
        if g != graph {
            break;
        }
        out.push((cell_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_capacity_cells_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::CELLS).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, cell_id) = k.value();
        if g != graph {
            break;
        }
        keys.push(cell_id.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_capacity_cells_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::CELLS).map_err(|e| e.to_string())?;
    for (cell_id, value) in rows {
        table
            .insert((graph, cell_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_capacity_leases_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(capacity_lease::LEASES) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, lease_id) = k.value();
        if g != graph {
            break;
        }
        out.push((lease_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_capacity_leases_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::LEASES).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, lease_id) = k.value();
        if g != graph {
            break;
        }
        keys.push(lease_id.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_capacity_leases_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::LEASES).map_err(|e| e.to_string())?;
    for (lease_id, value) in rows {
        table
            .insert((graph, lease_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_capacity_usage_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(capacity_lease::USAGE) else {
        return Ok(out);
    };
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, cell_id) = k.value();
        if g != graph {
            break;
        }
        out.push((cell_id.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_capacity_usage_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::USAGE).map_err(|e| e.to_string())?;
    let mut keys: Vec<String> = Vec::new();
    for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, cell_id) = k.value();
        if g != graph {
            break;
        }
        keys.push(cell_id.to_string());
    }
    for key_val in keys {
        table.remove((graph, key_val.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_capacity_usage_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::USAGE).map_err(|e| e.to_string())?;
    for (cell_id, value) in rows {
        table
            .insert((graph, cell_id.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn export_capacity_idempotency_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(String, String, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(capacity_lease::IDEMPOTENCY) else {
        return Ok(out);
    };
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, key) = k.value();
        if g != graph {
            break;
        }
        out.push((tenant.to_string(), key.to_string(), v.value().to_vec()));
    }
    Ok(out)
}

fn clear_capacity_idempotency_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::IDEMPOTENCY).map_err(|e| e.to_string())?;
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, tenant, key) = k.value();
        if g != graph {
            break;
        }
        keys.push((tenant.to_string(), key.to_string()));
    }
    for key_val in keys {
        table.remove((graph, key_val.0.as_str(), key_val.1.as_str())).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_capacity_idempotency_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(String, String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(capacity_lease::IDEMPOTENCY).map_err(|e| e.to_string())?;
    for (tenant, key, value) in rows {
        table
            .insert((graph, tenant.as_str(), key.as_str()), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// ── provenance_anchor_members per-table raw export/clear/insert (BUG-CX-016 class, online reshard) ──

#[cfg(feature = "security")]
fn export_provenance_anchor_members_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Vec<(u64, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let Ok(table) = rtx.open_table(PROVENANCE_ANCHOR_MEMBERS) else {
        return Ok(out);
    };
    for row in table.range((graph, 0u64)..).map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, seq) = k.value();
        if g != graph {
            break;
        }
        out.push((seq, v.value().to_vec()));
    }
    Ok(out)
}

#[cfg(feature = "security")]
fn clear_provenance_anchor_members_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut table = wtx.open_table(PROVENANCE_ANCHOR_MEMBERS).map_err(|e| e.to_string())?;
    let mut keys: Vec<u64> = Vec::new();
    for row in table.range((graph, 0u64)..).map_err(|e| e.to_string())? {
        let (k, _v) = row.map_err(|e| e.to_string())?;
        let (g, seq) = k.value();
        if g != graph {
            break;
        }
        keys.push(seq);
    }
    for key_val in keys {
        table.remove((graph, key_val)).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg(feature = "security")]
fn insert_provenance_anchor_members_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &[(u64, Vec<u8>)],
) -> Result<(), String> {
    let mut table = wtx.open_table(PROVENANCE_ANCHOR_MEMBERS).map_err(|e| e.to_string())?;
    for (seq, value) in rows {
        table
            .insert((graph, *seq), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}
// ── RESOURCE_* group wrappers (BUG-CX-054 class; same gap as shard_migrate.rs's
// `populate_resource_tables_for_source`/`populate_resource_core_tables_for_source`/
// `populate_resource_policy_tables_for_source` split, mirrored here so an ONLINE move
// carries the same reservation/host/policy authority the OFFLINE tool now does) ──

fn export_resource_core_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
    out: &mut RawResourceRows,
) -> Result<(), String> {
    out.reservations = export_resource_reservations_for_graph(rtx, graph)?;
    out.reservation_tenant_index = export_resource_reservation_tenant_index_for_graph(rtx, graph)?;
    out.reservation_attempts = export_resource_reservation_attempts_for_graph(rtx, graph)?;
    out.hosts = export_resource_hosts_for_graph(rtx, graph)?;
    Ok(())
}

fn export_resource_policy_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
    out: &mut RawResourceRows,
) -> Result<(), String> {
    out.exclusivity = export_resource_exclusivity_for_graph(rtx, graph)?;
    out.fairness = export_resource_fairness_for_graph(rtx, graph)?;
    out.concurrency = export_resource_concurrency_for_graph(rtx, graph)?;
    out.anti_affinity = export_resource_anti_affinity_for_graph(rtx, graph)?;
    out.disk_policies = export_resource_disk_policies_for_graph(rtx, graph)?;
    Ok(())
}

/// Every RESOURCE_* table for ONE graph (BUG-CX-054 class): reservations, tenant
/// index, attempts, hosts, exclusivity, fairness, concurrency, anti-affinity, disk
/// policies. Same tables `shard_migrate.rs::populate_resource_tables_for_source`
/// routes offline; an online reshard must carry them too or a moved graph loses its
/// resource-reservation authority.
fn export_resource_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<RawResourceRows, String> {
    let mut out = RawResourceRows::default();
    export_resource_core_rows_for_graph(rtx, graph, &mut out)?;
    export_resource_policy_rows_for_graph(rtx, graph, &mut out)?;
    Ok(out)
}

fn clear_resource_core_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_resource_reservations_rows_raw(wtx, graph)?;
    clear_resource_reservation_tenant_index_rows_raw(wtx, graph)?;
    clear_resource_reservation_attempts_rows_raw(wtx, graph)?;
    clear_resource_hosts_rows_raw(wtx, graph)?;
    Ok(())
}

fn clear_resource_policy_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_resource_exclusivity_rows_raw(wtx, graph)?;
    clear_resource_fairness_rows_raw(wtx, graph)?;
    clear_resource_concurrency_rows_raw(wtx, graph)?;
    clear_resource_anti_affinity_rows_raw(wtx, graph)?;
    clear_resource_disk_policies_rows_raw(wtx, graph)?;
    Ok(())
}

/// Raw range-scan-and-remove every RESOURCE_* row for ONE graph (no crypto — these
/// are opaque bytes to this module, same "verbatim, never decode" contract as the
/// rest of `online_reshard.rs`). NOT `redb_store::clear_resource_rows`: that shared
/// helper decrypts rows to enforce the "no active reservation" invariant for a
/// graph DELETE, which is the wrong contract for a MOVE (a graph with a live
/// reservation must still reshard — the reservation moves with it).
fn clear_resource_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_resource_core_rows_raw(wtx, graph)?;
    clear_resource_policy_rows_raw(wtx, graph)?;
    Ok(())
}

fn import_resource_core_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawResourceRows,
) -> Result<(), String> {
    insert_resource_reservations_rows(wtx, graph, &rows.reservations)?;
    insert_resource_reservation_tenant_index_rows(wtx, graph, &rows.reservation_tenant_index)?;
    insert_resource_reservation_attempts_rows(wtx, graph, &rows.reservation_attempts)?;
    insert_resource_hosts_rows(wtx, graph, &rows.hosts)?;
    Ok(())
}

fn import_resource_policy_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawResourceRows,
) -> Result<(), String> {
    insert_resource_exclusivity_rows(wtx, graph, &rows.exclusivity)?;
    insert_resource_fairness_rows(wtx, graph, &rows.fairness)?;
    insert_resource_concurrency_rows(wtx, graph, &rows.concurrency)?;
    insert_resource_anti_affinity_rows(wtx, graph, &rows.anti_affinity)?;
    insert_resource_disk_policies_rows(wtx, graph, &rows.disk_policies)?;
    Ok(())
}

/// Replace, don't merely upsert: clear every RESOURCE_* row for `graph` first, then
/// insert the incoming raw set — matches `import_mutation_rows`/`import_change_rows`'s
/// existing "retried move never duplicates" contract.
fn import_resource_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawResourceRows,
) -> Result<(), String> {
    clear_resource_rows_raw(wtx, graph)?;
    import_resource_core_rows(wtx, graph, rows)?;
    import_resource_policy_rows(wtx, graph, rows)?;
    Ok(())
}

// ── development_lane_* group wrappers (BUG-CX-054/096 class; mirrors
// `shard_migrate.rs`'s `populate_lane_core_tables_for_source`/
// `populate_lane_index_tables_for_source`/`populate_lane_misc_tables_for_source` split) ──

fn export_lane_core_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
    out: &mut RawDevelopmentLaneRows,
) -> Result<(), String> {
    out.holds = export_lane_holds_for_graph(rtx, graph)?;
    out.counters = export_lane_counters_for_graph(rtx, graph)?;
    out.pressure_index = export_lane_pressure_index_for_graph(rtx, graph)?;
    out.policies = export_lane_policies_for_graph(rtx, graph)?;
    Ok(())
}

fn export_lane_index_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
    out: &mut RawDevelopmentLaneRows,
) -> Result<(), String> {
    out.tenant_index = export_lane_tenant_index_for_graph(rtx, graph)?;
    out.lane_index = export_lane_index_for_graph(rtx, graph)?;
    out.repository_branch_index = export_lane_repository_branch_index_for_graph(rtx, graph)?;
    out.worktree_index = export_lane_worktree_index_for_graph(rtx, graph)?;
    Ok(())
}

fn export_lane_misc_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
    out: &mut RawDevelopmentLaneRows,
) -> Result<(), String> {
    out.work_item_index = export_lane_work_item_index_for_graph(rtx, graph)?;
    out.invocations = export_lane_invocations_for_graph(rtx, graph)?;
    Ok(())
}

/// Every development_lane_* table for ONE graph (BUG-CX-054/BUG-CX-096 class): all
/// 10 lane tables, the same set `shard_migrate.rs::populate_development_lane_tables_for_source`
/// routes offline and `redb_store::purge_graph_rows` already clears unconditionally
/// after a graph is deleted OR (via this module's own `Cmd::PurgeGraph` step) after
/// it MOVES — so before this fix, an online reshard silently dropped every
/// in-flight development-lane hold/counter/policy for the moved graph.
fn export_development_lane_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<RawDevelopmentLaneRows, String> {
    let mut out = RawDevelopmentLaneRows::default();
    export_lane_core_rows_for_graph(rtx, graph, &mut out)?;
    export_lane_index_rows_for_graph(rtx, graph, &mut out)?;
    export_lane_misc_rows_for_graph(rtx, graph, &mut out)?;
    Ok(out)
}

fn clear_lane_core_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_lane_holds_rows_raw(wtx, graph)?;
    clear_lane_counters_rows_raw(wtx, graph)?;
    clear_lane_pressure_index_rows_raw(wtx, graph)?;
    clear_lane_policies_rows_raw(wtx, graph)?;
    Ok(())
}

fn clear_lane_index_group_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_lane_tenant_index_rows_raw(wtx, graph)?;
    clear_lane_index_rows_raw(wtx, graph)?;
    clear_lane_repository_branch_index_rows_raw(wtx, graph)?;
    clear_lane_worktree_index_rows_raw(wtx, graph)?;
    Ok(())
}

fn clear_lane_misc_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_lane_work_item_index_rows_raw(wtx, graph)?;
    clear_lane_invocations_rows_raw(wtx, graph)?;
    Ok(())
}

/// Raw range-scan-and-remove every development_lane_* row for ONE graph (no
/// crypto — same "verbatim, never decode" contract as the rest of this module; NOT
/// `development_lane::clear_native_graph_rows_in_wtx`, which decrypts rows to
/// enforce delete-time invariants that do not apply to a MOVE).
fn clear_development_lane_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_lane_core_rows_raw(wtx, graph)?;
    clear_lane_index_group_rows_raw(wtx, graph)?;
    clear_lane_misc_rows_raw(wtx, graph)?;
    Ok(())
}

fn import_lane_core_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawDevelopmentLaneRows,
) -> Result<(), String> {
    insert_lane_holds_rows(wtx, graph, &rows.holds)?;
    insert_lane_counters_rows(wtx, graph, &rows.counters)?;
    insert_lane_pressure_index_rows(wtx, graph, &rows.pressure_index)?;
    insert_lane_policies_rows(wtx, graph, &rows.policies)?;
    Ok(())
}

fn import_lane_index_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawDevelopmentLaneRows,
) -> Result<(), String> {
    insert_lane_tenant_index_rows(wtx, graph, &rows.tenant_index)?;
    insert_lane_index_rows(wtx, graph, &rows.lane_index)?;
    insert_lane_repository_branch_index_rows(wtx, graph, &rows.repository_branch_index)?;
    insert_lane_worktree_index_rows(wtx, graph, &rows.worktree_index)?;
    Ok(())
}

fn import_lane_misc_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawDevelopmentLaneRows,
) -> Result<(), String> {
    insert_lane_work_item_index_rows(wtx, graph, &rows.work_item_index)?;
    insert_lane_invocations_rows(wtx, graph, &rows.invocations)?;
    Ok(())
}

/// Replace, don't merely upsert (see `import_resource_rows`'s doc for why).
fn import_development_lane_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawDevelopmentLaneRows,
) -> Result<(), String> {
    clear_development_lane_rows_raw(wtx, graph)?;
    import_lane_core_rows(wtx, graph, rows)?;
    import_lane_index_rows(wtx, graph, rows)?;
    import_lane_misc_rows(wtx, graph, rows)?;
    Ok(())
}

// ── capacity_lease_* group wrappers (undocumented BUG-CX-054-class gap; mirrors
// `shard_migrate.rs::populate_capacity_lease_tables_for_source`) ──

/// capacity_cells + leases + usage + idempotency for ONE graph.
fn export_capacity_lease_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<RawCapacityLeaseRows, String> {
    Ok(RawCapacityLeaseRows {
        cells: export_capacity_cells_for_graph(rtx, graph)?,
        leases: export_capacity_leases_for_graph(rtx, graph)?,
        usage: export_capacity_usage_for_graph(rtx, graph)?,
        idempotency: export_capacity_idempotency_for_graph(rtx, graph)?,
    })
}

/// Raw range-scan-and-remove every capacity_lease_* row for ONE graph (no crypto —
/// NOT `capacity_lease::clear_graph_rows`, which is the delete-time helper; a move
/// carries this authority forward instead of draining it).
fn clear_capacity_lease_rows_raw(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    clear_capacity_cells_rows_raw(wtx, graph)?;
    clear_capacity_leases_rows_raw(wtx, graph)?;
    clear_capacity_usage_rows_raw(wtx, graph)?;
    clear_capacity_idempotency_rows_raw(wtx, graph)?;
    Ok(())
}

/// Replace, don't merely upsert (see `import_resource_rows`'s doc for why).
fn import_capacity_lease_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawCapacityLeaseRows,
) -> Result<(), String> {
    clear_capacity_lease_rows_raw(wtx, graph)?;
    insert_capacity_cells_rows(wtx, graph, &rows.cells)?;
    insert_capacity_leases_rows(wtx, graph, &rows.leases)?;
    insert_capacity_usage_rows(wtx, graph, &rows.usage)?;
    insert_capacity_idempotency_rows(wtx, graph, &rows.idempotency)?;
    Ok(())
}

/// work_item_command_sequence — graph -> monotonic native WorkItem command sequence
/// (BUG-CX-054 class). A single value per graph, unlike every other table in this
/// lane's scope.
fn export_work_item_command_sequence_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Option<u64>, String> {
    let Ok(table) = rtx.open_table(WORK_ITEM_COMMAND_SEQUENCE) else {
        return Ok(None);
    };
    Ok(table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value()))
}

/// Replace the graph's `WORK_ITEM_COMMAND_SEQUENCE` row: clear then write `seq` if
/// `Some`; clear only if `None`.
fn import_work_item_command_sequence(
    wtx: &redb::WriteTransaction,
    graph: &str,
    seq: Option<u64>,
) -> Result<(), String> {
    let mut table = wtx
        .open_table(WORK_ITEM_COMMAND_SEQUENCE)
        .map_err(|e| e.to_string())?;
    table.remove(graph).map_err(|e| e.to_string())?;
    if let Some(seq) = seq {
        table.insert(graph, seq).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Scan ONE graph's durable rows VERBATIM off a `Database` (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on the
/// owning shard's writer thread (via [`Cmd::ExportGraphRaw`]) AFTER it has flushed pending
/// writes, so the snapshot reflects every committed mutation. Value blobs are copied raw
/// (no `crypto.unseal`) so encryption-at-rest and the audit chain survive the move.
pub(crate) fn export_graph_raw(db: &Database, graph: &str) -> Result<RawGraphRows, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let mut out = RawGraphRows::default();

    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    out.meta = meta_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec());

    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    for row in nodes_table
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        if g != graph {
            break;
        }
        out.nodes.push((id.to_string(), v.value().to_vec()));
    }

    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    for row in edges_table
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, o) = k.value();
        if g != graph {
            break;
        }
        out.edges
            .push((s.to_string(), t.to_string(), o, v.value().to_vec()));
    }

    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    for row in ledger_table
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, seq) = k.value();
        if g != graph {
            break;
        }
        out.ledger.push((seq, v.value().to_string()));
    }

    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    out.semantic = semantic_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec());

    #[cfg(feature = "security")]
    {
        let audit_table = rtx.open_table(AUDIT).map_err(|e| e.to_string())?;
        for row in audit_table
            .range((graph, 0u64)..)
            .map_err(|e| e.to_string())?
        {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, seq) = k.value();
            if g != graph {
                break;
            }
            out.audit.push((seq, v.value().to_vec()));
        }
    }

    let mut batch_ids = BTreeSet::new();
    if let Ok(table) = rtx.open_table(MUTATION_IDEMPOTENCY) {
        for row in table.iter().map_err(|e| e.to_string())? {
            let (key, value) = row.map_err(|e| e.to_string())?;
            let (tenant, row_graph, idempotency) = key.value();
            if row_graph == graph {
                let batch_id = value.value().to_string();
                batch_ids.insert(batch_id.clone());
                out.mutation.idempotency.push((
                    tenant.to_string(),
                    idempotency.to_string(),
                    batch_id,
                ));
            }
        }
    }
    if let Ok(table) = rtx.open_table(MUTATION_BATCHES) {
        for batch_id in &batch_ids {
            if let Some(value) = table.get(batch_id.as_str()).map_err(|e| e.to_string())? {
                out.mutation
                    .batches
                    .push((batch_id.clone(), value.value().to_vec()));
            }
        }
    }
    if let Ok(table) = rtx.open_table(MUTATION_OUTBOX) {
        for batch_id in &batch_ids {
            for row in table
                .range((batch_id.as_str(), 0u32)..)
                .map_err(|e| e.to_string())?
            {
                let (key, value) = row.map_err(|e| e.to_string())?;
                let (row_batch, ordinal) = key.value();
                if row_batch != batch_id.as_str() {
                    break;
                }
                out.mutation
                    .outbox
                    .push((batch_id.clone(), ordinal, value.value().to_vec()));
            }
        }
    }
    if let Ok(table) = rtx.open_table(MUTATION_OUTBOX_DELIVERY) {
        for batch_id in &batch_ids {
            for row in table
                .range((batch_id.as_str(), 0u32, "")..)
                .map_err(|e| e.to_string())?
            {
                let (key, value) = row.map_err(|e| e.to_string())?;
                let (row_batch, ordinal, consumer) = key.value();
                if row_batch != batch_id.as_str() {
                    break;
                }
                out.mutation.deliveries.push((
                    batch_id.clone(),
                    ordinal,
                    consumer.to_string(),
                    value.value().to_vec(),
                ));
            }
        }
    }
    if let Ok(table) = rtx.open_table(MUTATION_PROJECTION_CURSOR) {
        for row in table.iter().map_err(|e| e.to_string())? {
            let (key, value) = row.map_err(|e| e.to_string())?;
            let (tenant, row_graph, projection) = key.value();
            if row_graph == graph {
                out.mutation.projection_cursors.push((
                    tenant.to_string(),
                    projection.to_string(),
                    value.value().to_vec(),
                ));
            }
        }
    }
    if let Ok(table) = rtx.open_table(MUTATION_GRAPH_VERSION) {
        out.mutation.graph_version = table
            .get(graph)
            .map_err(|e| e.to_string())?
            .map(|row| row.value());
    }
    if let Ok(table) = rtx.open_table(MUTATION_FENCE) {
        out.mutation.fence = table
            .get(graph)
            .map_err(|e| e.to_string())?
            .map(|row| row.value().to_vec());
    }
    if let Ok(table) = rtx.open_table(MUTATION_LIFECYCLE_HEAD) {
        out.mutation.lifecycle_head = table
            .get(graph)
            .map_err(|e| e.to_string())?
            .map(|row| row.value().to_string());
    }

    if let Ok(table) = rtx.open_table(CHANGE_ENVELOPES) {
        for row in table.range((graph, "")..).map_err(|e| e.to_string())? {
            let (key, value) = row.map_err(|e| e.to_string())?;
            let (row_graph, id) = key.value();
            if row_graph != graph {
                break;
            }
            out.change
                .envelopes
                .push((id.to_string(), value.value().to_vec()));
        }
    }
    macro_rules! export_three_part {
        ($definition:expr, $destination:expr) => {
            if let Ok(table) = rtx.open_table($definition) {
                for row in table.range((graph, "", "")..).map_err(|e| e.to_string())? {
                    let (key, value) = row.map_err(|e| e.to_string())?;
                    let (row_graph, tenant, id) = key.value();
                    if row_graph != graph {
                        break;
                    }
                    $destination.push((tenant.to_string(), id.to_string(), value.value().to_vec()));
                }
            }
        };
    }
    export_three_part!(CONTENT_VERSIONS, out.change.content_versions);
    export_three_part!(CHANGE_BLOBS, out.change.blobs);
    export_three_part!(CHANGE_FEATURES, out.change.features);
    export_three_part!(CHANGE_EVIDENCE, out.change.evidence);
    export_three_part!(CHANGE_POLICIES, out.change.policies);
    export_three_part!(CHANGE_LINEAGE, out.change.lineage);
    if let Ok(table) = rtx.open_table(CHANGE_CURSORS) {
        for row in table
            .range((graph, "", "", "")..)
            .map_err(|e| e.to_string())?
        {
            let (key, value) = row.map_err(|e| e.to_string())?;
            let (row_graph, tenant, source, partition) = key.value();
            if row_graph != graph {
                break;
            }
            out.change.cursors.push((
                tenant.to_string(),
                source.to_string(),
                partition.to_string(),
                value.value().to_vec(),
            ));
        }
    }

    populate_wd5_bug04_rows_for_graph(&rtx, graph, &mut out)?;

    Ok(out)
}

/// Populate the WD5-BUG-04 fields (RESOURCE_*, development_lane_*, capacity_lease_*,
/// provenance_anchor_members, work_item_command_sequence) of `out` for ONE graph.
/// Split out of `export_graph_raw` so that function's own complexity does not grow.
fn populate_wd5_bug04_rows_for_graph(
    rtx: &redb::ReadTransaction,
    graph: &str,
    out: &mut RawGraphRows,
) -> Result<(), String> {
    out.resource = export_resource_rows_for_graph(rtx, graph)?;
    out.development_lane = export_development_lane_rows_for_graph(rtx, graph)?;
    out.capacity_lease = export_capacity_lease_rows_for_graph(rtx, graph)?;
    #[cfg(feature = "security")]
    {
        out.provenance_anchor_members = export_provenance_anchor_members_for_graph(rtx, graph)?;
    }
    out.work_item_command_sequence = export_work_item_command_sequence_for_graph(rtx, graph)?;
    Ok(())
}

/// Replace, don't merely upsert, every WD5-BUG-04 table for ONE graph inside the
/// caller's already-open write transaction — same contract as `import_mutation_rows`/
/// `import_change_rows`. Split out of `import_graph_raw` for the same reason.
fn import_wd5_bug04_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    rows: &RawGraphRows,
) -> Result<(), String> {
    import_resource_rows(wtx, graph, &rows.resource)?;
    import_development_lane_rows(wtx, graph, &rows.development_lane)?;
    import_capacity_lease_rows(wtx, graph, &rows.capacity_lease)?;
    #[cfg(feature = "security")]
    {
        clear_provenance_anchor_members_rows_raw(wtx, graph)?;
        insert_provenance_anchor_members_rows(wtx, graph, &rows.provenance_anchor_members)?;
    }
    import_work_item_command_sequence(wtx, graph, rows.work_item_command_sequence)?;
    Ok(())
}

/// Insert ONE graph's verbatim rows into a destination `Database` in ONE durable
/// transaction (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on the destination shard's writer thread (via
/// [`Cmd::ImportGraphRaw`]); the single `Durability::Immediate` commit is the
/// commit-before-ack point of the move. Idempotent (keyed upserts), so a re-run after an
/// interrupted move overwrites rather than duplicates.
pub(crate) fn import_graph_raw(
    db: &Database,
    graph: &str,
    rows: &RawGraphRows,
) -> Result<(), String> {
    rows.durable_identity(graph)?;
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        // Raw online-reshard images intentionally do not carry opaque
        // capability bytes, invocation idempotency, or native WorkItem
        // provenance.  Purge any destination private state atomically before
        // replacing the copied graph, so a shard reuse/retry cannot resurrect
        // authority for a restored WorkItem tuple.
        crate::redb_store::work_item_capability::clear_graph_rows_in_wtx(&wtx, graph)?;
        // Replace, do not merely upsert, the graph projection. This makes a
        // retried online copy and a Raft snapshot install remove rows that are no
        // longer present in the source image.
        {
            let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
            let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
            let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
            clear_graph_rows(graph, &mut nodes, &mut edges, &mut ledger)?;
        }
        {
            let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            semantic.remove(graph).map_err(|e| e.to_string())?;
        }
        {
            let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
            meta.remove(graph).map_err(|e| e.to_string())?;
        }
        #[cfg(feature = "security")]
        {
            // Snapshot install/retried reshard is an exact replacement. Remove any
            // destination audit tail that is absent from the incoming image before
            // copying the source chain verbatim.
            clear_audit_rows(&wtx, graph)?;
        }

        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        if let Some(blob) = &rows.meta {
            meta.insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        for (id, blob) in &rows.nodes {
            nodes
                .insert((graph, id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        for (s, t, o, blob) in &rows.edges {
            edges
                .insert((graph, s.as_str(), t.as_str(), *o), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (seq, line) in &rows.ledger {
            ledger
                .insert((graph, *seq), line.as_str())
                .map_err(|e| e.to_string())?;
        }
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        if let Some(blob) = &rows.semantic {
            semantic
                .insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        #[cfg(feature = "security")]
        {
            let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
            for (seq, blob) in &rows.audit {
                audit
                    .insert((graph, *seq), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        import_mutation_rows(&wtx, graph, &rows.mutation)?;
        import_change_rows(&wtx, graph, &rows.change)?;
        import_wd5_bug04_rows(&wtx, graph, rows)?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Export the graph's rows VERBATIM off the source shard's flush-then-scan snapshot.
fn export_from(src_tx: &SyncSender<Cmd>, graph: &str) -> Result<RawGraphRows, String> {
    let (rtx, rrx) = std::sync::mpsc::channel();
    src_tx
        .send(Cmd::ExportGraphRaw {
            graph: graph.to_string(),
            reply: rtx,
        })
        .map_err(|_| "redb writer thread is gone".to_string())?;
    rrx.recv()
        .map_err(|_| "redb writer dropped export reply".to_string())?
}

/// PHASE 1 of the online move (CONCEPT:EG-KG.backend.flush-pending-first, R1 delta-copy) — the BULK pass, run WITHOUT
/// the exclusive routing quiesce so writes keep flowing to `src` and the graph is NOT
/// paused. Export the graph's rows verbatim off a src snapshot and import them into `dst`
/// in one durable commit. Returns the bulk snapshot so [`delta_flip_purge`] can diff the
/// final state against it under the quiesce. Idempotent (keyed upserts) — a re-run after an
/// interrupted move overwrites rather than duplicates.
pub(crate) fn bulk_copy(
    src_tx: &SyncSender<Cmd>,
    dst_tx: &SyncSender<Cmd>,
    graph: &str,
) -> Result<RawGraphRows, String> {
    let gone = || "redb writer thread is gone".to_string();
    let rows = export_from(src_tx, graph)?;
    let (itx, irx) = std::sync::mpsc::channel();
    dst_tx
        .send(Cmd::ImportGraphRaw {
            graph: graph.to_string(),
            rows: Box::new(rows.clone()),
            reply: itx,
        })
        .map_err(|_| gone())?;
    irx.recv()
        .map_err(|_| "redb writer dropped import reply".to_string())??;
    Ok(rows)
}

/// PHASE 2 of the online move (CONCEPT:EG-KG.backend.flush-pending-first, R1) — run on a blocking thread WHILE the
/// exclusive `routing_epoch` WRITE guard is held, so writes to this graph are quiesced for
/// only this short window. Re-export the LATEST source state, copy just the DELTA accrued
/// since the bulk pass into `dst`, flip the catalog route, then GC the source. The pause is
/// O(delta) (the under-quiesce import) rather than O(graph). Crash-consistent ordering:
/// `import(delta) committed -> catalog flip durable -> purge(src)`.
pub(crate) fn delta_flip_purge(
    src_tx: &SyncSender<Cmd>,
    dst_tx: &SyncSender<Cmd>,
    catalog: &TenantCatalog,
    graph: &str,
    src_idx: usize,
    dst_idx: usize,
    bulk: RawGraphRows,
) -> Result<ReshardReport, String> {
    let gone = || "redb writer thread is gone".to_string();

    // 1. Re-export the latest source state (it now reflects every write that landed during
    //    the unquiesced bulk pass) and diff it against the bulk snapshot.
    let latest = export_from(src_tx, graph)?;
    let delta = compute_delta(&bulk, &latest);
    let mut report = ReshardReport::counts(graph, src_idx, dst_idx, &latest);
    report.delta_nodes = delta.nodes();
    report.delta_edges = delta.edges();

    // 2. Import ONLY the delta into `dst`, awaiting the durable commit.
    let (itx, irx) = std::sync::mpsc::channel();
    dst_tx
        .send(Cmd::ImportGraphDelta {
            graph: graph.to_string(),
            delta: Box::new(delta),
            reply: itx,
        })
        .map_err(|_| gone())?;
    irx.recv()
        .map_err(|_| "redb writer dropped delta import reply".to_string())??;

    // 3. Flip the durable route — reads/writes now follow the graph to `dst` (preserving
    //    any cluster-node placement). The atomic commit point of the move.
    let node = catalog.lookup(graph).and_then(|a| a.node);
    catalog.assign(graph, dst_idx as u32, node)?;

    // 4. GC the now-orphaned source rows (idempotent purge). After the flip no writer can
    //    route to `src` for this graph, so the purge can never race a live write.
    let (done, done_rx) = tokio::sync::oneshot::channel();
    src_tx
        .send(Cmd::PurgeGraph {
            graph: graph.to_string(),
            done,
        })
        .map_err(|_| gone())?;
    done_rx
        .blocking_recv()
        .map_err(|_| "redb writer dropped purge reply".to_string())??;

    let (history_tx, history_rx) = std::sync::mpsc::channel();
    src_tx
        .send(Cmd::PurgeMovedMutationRows {
            graph: graph.to_string(),
            reply: history_tx,
        })
        .map_err(|_| gone())?;
    history_rx
        .recv()
        .map_err(|_| "redb writer dropped moved-history purge reply".to_string())??;

    Ok(report)
}
