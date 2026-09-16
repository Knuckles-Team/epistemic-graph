//! Online single-node per-tenant resharding execution (CONCEPT:EG-KG.backend.catalog-shard-resolve, M3 keystone).
//!
//! ## What it solves
//!
//! [`super::shard_migrate`] (CONCEPT:EG-KG.sharding.atomic-shard-swap) moves shards OFFLINE — the engine must be
//! stopped because it rewrites the whole store to a new uniform K. This module moves ONE
//! graph between shards while the engine RUNS, with no data loss and no stop: the graph's
//! OWNER rows are copied verbatim to the destination shard, its kernel LEDGER is grafted
//! across, the [`super::tenant_catalog`] (CONCEPT:EG-KG.sharding.empty-catalog-routing)
//! route is flipped so reads/writes follow the graph to its new shard, and the source
//! binding is retired — all without touching any OTHER graph's writers.
//!
//! ## The move has two halves, and only one of them is this module's to copy
//!
//! **Owner rows** (nodes, edges, the graph ledger, semantic store, audit chain,
//! provenance anchor members, the WorkItem command sequence, every `resource_*`,
//! `change_*`/`content_versions`, `capacity_*` and `development_lane_*` row) are the
//! shard layout's own, so this module reads them through the source graph's
//! [`ScopedRead`] and stages them through the destination's exact graft
//! reservation. The final Phase-B transaction copies the kernel ledger and
//! retires the source. The copy is **verbatim** — stored bytes are NOT decoded,
//! unsealed or re-derived — so encryption-at-rest blobs survive WITHOUT the key and the
//! tamper-evident hash-chained audit log stays verifiable.
//!
//! **Ledger rows** are the mutation kernel's, and a domain crate cannot write one at
//! all. Re-admitting the moved batches would not be equivalent to moving them: it would
//! re-execute effects, re-emit outbox rows, stamp new receipts, and RESET the version an
//! in-flight OCC expectation depends on. So the ledger moves by
//! [`Shard::graft_graph_from`], which copies all fifteen ledger tables verbatim, commits
//! the destination, and retires the source binding together with its owner payload.
//! [`eg_transaction::GraftedScope::version`] is the SOURCE's marker-inclusive version,
//! preserved by the copy — the Phase-A marker is the one intentional maintenance
//! increment. RF-RULING-004 application note 4 still applies to the ledger state: the
//! destination receives the source's complete committed history rather than a fresh
//! re-admission of its batches.
//!
//! NOT moved: `plan_matviews`/`matview_operator_state` (file-wide, shard-0-homed, no
//! graph key — moving them per-graph would be its own corruption), and the
//! `work_item_capability` trio, which is deliberately purged rather than copied: raw
//! reshard images carry no capability bytes, invocation idempotency or native WorkItem
//! provenance, so every destination import purges that private authority in the SAME
//! transaction as the copy and the next lease must be claimed natively again.
//!
//! `graph_meta` is the odd one out. Its key IS the graph name, but it is declared
//! file-wide — it is the shard file's catalog of which graphs it hosts, read by the boot
//! scan before any graph scope can be bound — so it is written through the control
//! member of the same admitted group that writes the graph's rows, and dropped from the
//! source through a control-only write.
//!
//! ## Correctness — the quiesce / flip window, and why the graft is last
//!
//! The move is driven by [`RedbBackend::reshard_graph`](super::redb_backend::RedbBackend),
//! which holds the backend's `routing_epoch` WRITE guard for the duration of the move.
//! Every catalog-attached durable write (`record_durable` / `commit_crossmodal`) resolves
//! its shard and enqueues its op while holding a SHARED `routing_epoch` READ guard, so the
//! exclusive flip cannot interleave: when the flip holds the write guard, no write is
//! mid-resolve, and once it releases, every subsequent write resolves the catalog AFTER
//! the route flip — so a write is never lost or routed to the stale shard.
//!
//! The ordering inside the move is `bulk copy(dst) -> delta catch-up(dst) -> route flip
//! -> graft(src -> dst)`, and the last step is the cutover, NOT the bulk copy. The graft
//! retires the source's binding and sweeps its owner payload, so **nothing may read the
//! source graph after it** — which is exactly why the delta catch-up, whose whole job is
//! to re-read the source, has to complete first. A crash before the graft leaves the
//! rows in BOTH shards with the ledger still authoritative on `src`; re-running the move
//! completes it, because both the copy and the graft are idempotent.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;

use eg_storage::{
    GraphShardOwner, OwnerRowScope, OwnerRowScopeStart, ScopedOwnerTableMut, ScopedRead,
};
use eg_transaction::{GraftedScope, OwnerPayloadWrite};
use redb::TableDefinition;

use super::redb_backend::Cmd;
use super::tenant_catalog::TenantCatalog;
use crate::protocol::GraphType;
use crate::redb_store::shard::{Shard, ShardWrite};
#[cfg(feature = "security")]
use crate::redb_store::AUDIT;
#[cfg(feature = "security")]
use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
use crate::redb_store::{capacity_lease, development_lane};
use crate::redb_store::{
    clear_change_material_rows_in_wtx, clear_graph_rows, decode_graph_meta_identity, sanitize,
    CHANGE_BLOBS, CHANGE_CURSORS, CHANGE_ENVELOPES, CHANGE_EVIDENCE, CHANGE_FEATURES,
    CHANGE_LINEAGE, CHANGE_POLICIES, CONTENT_VERSIONS, EDGES, GRAPH_META, LEDGER, NODES, SEMANTIC,
};
use crate::redb_store::{
    RESOURCE_ANTI_AFFINITY, RESOURCE_CONCURRENCY, RESOURCE_DISK_POLICIES, RESOURCE_EXCLUSIVITY,
    RESOURCE_FAIRNESS, RESOURCE_HOSTS, RESOURCE_RESERVATIONS, RESOURCE_RESERVATION_ATTEMPTS,
    RESOURCE_RESERVATION_TENANT_INDEX, WORK_ITEM_COMMAND_SEQUENCE,
};
use crate::server::persistence::writer_reply::await_writer_reply;

/// One graph's scope-bounded read of the shard's scope-prefixed owner tables.
type GraphRead<'a> = ScopedRead<'a, GraphShardOwner>;

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

/// What happened to one single-valued row between the bulk snapshot and the
/// latest source state.
///
/// A row that did not change is absent from the delta entirely, so the outer
/// `Option` says "changed or not" and this says "written or cleared". Named
/// rather than expressed as `Option<Option<T>>`, which reads as neither and
/// which clippy rejects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowChange<T> {
    Set(T),
    Cleared,
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

/// `resource_*` rows for ONE graph, captured verbatim (BUG-CX-054 class — the same
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

/// `development_lane_*` rows for ONE graph, captured verbatim (BUG-CX-054/096 class).
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
    pub pressure_index: Vec<LanePressureIndexRow>,
    pub policies: Vec<(String, Vec<u8>)>,
    pub invocations: Vec<(String, String, Vec<u8>)>,
}

/// `capacity_*` rows for ONE graph, captured verbatim (undocumented gap in the
/// same class as BUG-CX-054, found by this lane's mechanical table inventory).
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCapacityLeaseRows {
    pub cells: Vec<(String, Vec<u8>)>,
    pub leases: Vec<(String, Vec<u8>)>,
    pub usage: Vec<(String, Vec<u8>)>,
    pub idempotency: Vec<(String, String, Vec<u8>)>,
}

/// One graph's durable OWNER rows captured VERBATIM for an online shard move
/// (CONCEPT:EG-KG.backend.catalog-shard-resolve).
///
/// Value blobs are the raw on-disk bytes (encrypted if encryption-at-rest is on, the
/// audit chain untouched) so re-inserting them on the destination shard preserves both
/// encryption and audit-chain verifiability.
///
/// **Schema 2 carries no ledger rows.** Schema 1 serialized the eight retired private
/// mutation tables into this image and replayed them into the destination; under the
/// kernel a domain crate cannot write a ledger row, and a serialized round trip through
/// this struct would reset the moved scope's version. The ledger now crosses by graft —
/// see this module's doc comment — so the raw image is owner rows only, and an image
/// still carrying a `mutation` field is refused by `deny_unknown_fields` rather than
/// silently half-applied.
pub(crate) const RAW_GRAPH_ROWS_SCHEMA_VERSION: u16 = 2;

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
    /// `(seq, ledger_line)` — the graph's own append-only event ledger, an owner
    /// table, not the mutation kernel's ledger.
    pub ledger: Vec<(u64, String)>,
    /// The semantic-store blob, if any.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub semantic: Option<Vec<u8>>,
    /// `(seq, chained_audit_blob)` — copied verbatim to keep the hash chain valid.
    #[cfg(feature = "security")]
    pub audit: Vec<(u64, Vec<u8>)>,
    /// Governed external-change material and typed version/cursor rows.
    pub change: RawChangeRows,
    /// Every `resource_*` row for this graph (BUG-CX-054 class).
    pub resource: RawResourceRows,
    /// Every `development_lane_*` row for this graph (BUG-CX-054/096 class).
    pub development_lane: RawDevelopmentLaneRows,
    /// Every `capacity_*` row for this graph (undocumented gap, same class).
    pub capacity_lease: RawCapacityLeaseRows,
    /// `(seq, anchor_member_blob)` — Merkle inclusion-proof anchor members
    /// (BUG-CX-016 class), copied verbatim like `audit`.
    #[cfg(feature = "security")]
    pub provenance_anchor_members: Vec<(u64, Vec<u8>)>,
    /// The graph's monotonic native WorkItem command sequence, if any (BUG-CX-054 class).
    #[serde(deserialize_with = "deserialize_required_option")]
    pub work_item_command_sequence: Option<u64>,
    /// This image came from the reserved owner-payload phase of an online
    /// graft.  It must not fall through to an ordinary import if the durable
    /// reservation was canceled while the command was queued.
    #[serde(skip)]
    pub(crate) requires_graft_reservation: bool,
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
            change: RawChangeRows::default(),
            resource: RawResourceRows::default(),
            development_lane: RawDevelopmentLaneRows::default(),
            capacity_lease: RawCapacityLeaseRows::default(),
            #[cfg(feature = "security")]
            provenance_anchor_members: Vec::new(),
            work_item_command_sequence: None,
            requires_graft_reservation: false,
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

    /// Whether these raw rows carry ANY authority-bearing content.
    ///
    /// Split into two halves because each `||` is a cyclomatic decision point and
    /// inline the chain took `durable_identity` over the cap. The predicate itself
    /// is unchanged: same operands, same order, same short-circuit behaviour.
    fn has_core_authority(&self) -> bool {
        #[cfg(feature = "security")]
        let has_audit_or_provenance =
            !self.audit.is_empty() || !self.provenance_anchor_members.is_empty();
        #[cfg(not(feature = "security"))]
        let has_audit_or_provenance = false;
        !self.nodes.is_empty()
            || !self.edges.is_empty()
            || !self.ledger.is_empty()
            || self.semantic.is_some()
            || has_audit_or_provenance
    }

    /// The subsystem row-sets, all compared against their empty default.
    fn has_subsystem_authority(&self) -> bool {
        self.change != RawChangeRows::default()
            || self.resource != RawResourceRows::default()
            || self.development_lane != RawDevelopmentLaneRows::default()
            || self.capacity_lease != RawCapacityLeaseRows::default()
            || self.work_item_command_sequence.is_some()
    }

    fn has_any_authority(&self) -> bool {
        self.has_core_authority() || self.has_subsystem_authority()
    }

    /// Whether a graph binding carries only its kernel baseline and no owner
    /// payload.  A graft reservation may be created only for this state; a
    /// pre-existing catalog or owner row would otherwise be silently replaced.
    pub(crate) fn empty_owner_payload(&self) -> bool {
        self.meta.is_none() && !self.has_any_authority()
    }

    /// Validate the raw row set and derive its identity from `graph_meta`.
    ///
    /// A raw image may be completely empty during a route-only reshard, but it may
    /// never carry graph-scoped authority without the identity that owns it.
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

        if self.has_any_authority() {
            return Err("raw graph rows contain authority without durable identity".to_string());
        }
        Ok(None)
    }

    /// Total rows across every WD5-BUG-04 table (`resource_*`, `development_lane_*`,
    /// `capacity_*`, provenance-anchor-members, WorkItem command sequence) —
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
    /// (WD5-BUG-04): every `resource_*`, `development_lane_*` and `capacity_*` row,
    /// plus provenance-anchor-member rows and the WorkItem command sequence — counted
    /// separately so a reshard report makes this coverage independently auditable.
    pub capability_and_resource: u64,
    /// Kernel ledger rows the graft moved across all fifteen ledger tables.
    pub grafted_ledger_rows: u64,
    /// The authoritative marker-inclusive version the destination now serves. The
    /// graft preserves the source ledger and contributes only its one Phase-A
    /// maintenance increment (RF-RULING-004 application note 4).
    pub grafted_version: u64,
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
            grafted_ledger_rows: 0,
            grafted_version: 0,
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
    pub semantic: Option<RowChange<Vec<u8>>>,
    #[cfg(feature = "security")]
    pub upsert_audit: Vec<(u64, Vec<u8>)>,
    /// Auxiliary authority changes are rare and small relative to graph rows;
    /// when any changed during bulk copy, replace the graph's set atomically.
    pub replace_change: Option<Box<RawChangeRows>>,
    pub replace_resource: Option<Box<RawResourceRows>>,
    pub replace_development_lane: Option<Box<RawDevelopmentLaneRows>>,
    pub replace_capacity_lease: Option<Box<RawCapacityLeaseRows>>,
    #[cfg(feature = "security")]
    pub replace_provenance_anchor_members: Option<Vec<(u64, Vec<u8>)>>,
    pub work_item_command_sequence: Option<RowChange<u64>>,
    /// This delta belongs to the reserved owner-payload phase of an online
    /// graft and must fail closed if that reservation has since been canceled.
    pub(crate) requires_graft_reservation: bool,
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

/// A single-valued row's delta entry: absent when unchanged.
fn row_change<T: Clone + PartialEq>(bulk: &Option<T>, latest: &Option<T>) -> Option<RowChange<T>> {
    if bulk == latest {
        return None;
    }
    Some(match latest {
        Some(value) => RowChange::Set(value.clone()),
        None => RowChange::Cleared,
    })
}

/// Diff the LATEST source rows against the BULK snapshot already on `dst` (CONCEPT:EG-KG.backend.flush-pending-first).
/// Emits only what changed: new/changed-blob rows to upsert + vanished rows to remove.
pub(crate) fn compute_delta(bulk: &RawGraphRows, latest: &RawGraphRows) -> RawGraphDelta {
    use std::collections::HashMap;
    let mut delta = RawGraphDelta {
        semantic: row_change(&bulk.semantic, &latest.semantic),
        requires_graft_reservation: bulk.requires_graft_reservation,
        ..RawGraphDelta::default()
    };

    if bulk.meta != latest.meta {
        delta.meta = latest.meta.clone();
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

    if bulk.change != latest.change {
        delta.replace_change = Some(Box::new(latest.change.clone()));
    }
    compute_capability_and_resource_delta(bulk, latest, &mut delta);

    delta
}

/// The same "replace the whole set atomically when anything in it changed" diff as
/// `compute_delta`'s change check, extended to the tables WD5-BUG-04 found missing
/// from the online move. Split out so `compute_delta` stays under the complexity cap.
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
    delta.work_item_command_sequence = row_change(
        &bulk.work_item_command_sequence,
        &latest.work_item_command_sequence,
    );
}

// ── scope-bounded row primitives ──────────────────────────────────────────
//
// Every scope-prefixed shard table is read through `scope_rows()` and written
// through a `ScopedOwnerTableMut`, both of which take their graph bound from
// the capability rather than from an argument. The five clear and twelve insert
// helpers below are keyed by table SHAPE, not by table: the 30-odd per-table
// export/clear/insert functions this module used to carry were the same six
// shapes written out thirty times, and a shape is exactly what a scope-bounded
// scan removes the rest of.

/// Every row of this graph's scope in one scope-prefixed table, decoded.
///
/// `scope_rows()` starts at the least key the scope can own and stops on the
/// first key that leaves it, so there is no caller-supplied lower bound and no
/// `take_while` to forget — which is what the `range((graph, ""))..` +
/// `if row_graph != graph { break }` pair at each of the old call sites was.
fn export_rows<K, V, O, Decode>(
    read: &GraphRead<'_>,
    definition: TableDefinition<'static, K, V>,
    mut decode: Decode,
) -> Result<Vec<O>, String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope + OwnerRowScopeStart<'k>,
    V: redb::Value + 'static,
    Decode: for<'k> FnMut(K::SelfType<'k>, V::SelfType<'k>) -> O,
{
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (key, value) = row?;
        out.push(decode(key.value(), value.value()));
    }
    Ok(out)
}

/// The owned keys of this graph's rows in one scope-prefixed table.
///
/// Collect-then-remove is the shape a scoped table imposes: it has no
/// `retain`/`drain`, and the scan borrows the table immutably while a removal
/// needs it mutably, so the keys are materialized first and the iterator
/// dropped before the first `remove`.
fn scope_keys<K, V, O, Own>(
    table: &ScopedOwnerTableMut<'_, K, V>,
    own: Own,
) -> Result<Vec<O>, String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope + OwnerRowScopeStart<'k>,
    V: redb::Value + 'static,
    Own: for<'k> Fn(K::SelfType<'k>) -> O,
{
    let mut keys = Vec::new();
    for row in table.scope_rows()? {
        let (key, _) = row?;
        keys.push(own(key.value()));
    }
    Ok(keys)
}

fn clear_two_part<V: redb::Value + 'static>(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str), V>,
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    let keys = scope_keys(&table, |(_, key)| key.to_string())?;
    for key in keys {
        table.remove((graph, key.as_str()))?;
    }
    Ok(())
}

fn clear_three_part<V: redb::Value + 'static>(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str, &str), V>,
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    let keys = scope_keys(&table, |(_, first, second)| {
        (first.to_string(), second.to_string())
    })?;
    for (first, second) in keys {
        table.remove((graph, first.as_str(), second.as_str()))?;
    }
    Ok(())
}

fn clear_text_sequence<V: redb::Value + 'static>(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str, u64), V>,
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    let keys = scope_keys(&table, |(_, text, sequence)| (text.to_string(), sequence))?;
    for (text, sequence) in keys {
        table.remove((graph, text.as_str(), sequence))?;
    }
    Ok(())
}

fn clear_sequence<V: redb::Value + 'static>(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, u64), V>,
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    let keys = scope_keys(&table, |(_, sequence)| sequence)?;
    for sequence in keys {
        table.remove((graph, sequence))?;
    }
    Ok(())
}

fn clear_pressure_index(write: &impl OwnerPayloadWrite, graph: &str) -> Result<(), String> {
    let mut table = write.open_scoped_table(development_lane::PRESSURE_INDEX)?;
    let keys = scope_keys(&table, |(_, tenant, lane, repository, observed, kind)| {
        (
            tenant.to_string(),
            lane.to_string(),
            repository.to_string(),
            observed,
            kind.to_string(),
        )
    })?;
    for (tenant, lane, repository, observed, kind) in keys {
        table.remove((
            graph,
            tenant.as_str(),
            lane.as_str(),
            repository.as_str(),
            observed,
            kind.as_str(),
        ))?;
    }
    Ok(())
}

fn insert_two_part_bytes(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str), &[u8]>,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (key, value) in rows {
        table.insert((graph, key.as_str()), value.as_slice())?;
    }
    Ok(())
}

fn insert_two_part_text(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str), &str>,
    rows: &[(String, String)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (key, value) in rows {
        table.insert((graph, key.as_str()), value.as_str())?;
    }
    Ok(())
}

fn insert_two_part_count(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str), u64>,
    rows: &[(String, u64)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (key, value) in rows {
        table.insert((graph, key.as_str()), *value)?;
    }
    Ok(())
}

fn insert_three_part_bytes(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str, &str), &[u8]>,
    rows: &[(String, String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (first, second, value) in rows {
        table.insert((graph, first.as_str(), second.as_str()), value.as_slice())?;
    }
    Ok(())
}

fn insert_three_part_text(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str, &str), &str>,
    rows: &[(String, String, String)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (first, second, value) in rows {
        table.insert((graph, first.as_str(), second.as_str()), value.as_str())?;
    }
    Ok(())
}

fn insert_three_part_count(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str, &str), u64>,
    rows: &[(String, String, u64)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (first, second, value) in rows {
        table.insert((graph, first.as_str(), second.as_str()), *value)?;
    }
    Ok(())
}

fn insert_text_sequence_text(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str, u64), &str>,
    rows: &[(String, u64, String)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (text, sequence, value) in rows {
        table.insert((graph, text.as_str(), *sequence), value.as_str())?;
    }
    Ok(())
}

fn insert_four_part_bytes(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, &str, &str, &str), &[u8]>,
    rows: &[(String, String, String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (first, second, third, value) in rows {
        table.insert(
            (graph, first.as_str(), second.as_str(), third.as_str()),
            value.as_slice(),
        )?;
    }
    Ok(())
}

fn insert_sequence_bytes(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    definition: TableDefinition<'static, (&str, u64), &[u8]>,
    rows: &[(u64, Vec<u8>)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(definition)?;
    for (sequence, value) in rows {
        table.insert((graph, *sequence), value.as_slice())?;
    }
    Ok(())
}

fn insert_ledger_rows(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &[(u64, String)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(LEDGER)?;
    for (sequence, line) in rows {
        table.insert((graph, *sequence), line.as_str())?;
    }
    Ok(())
}

fn insert_edge_rows(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &[(String, String, u32, Vec<u8>)],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(EDGES)?;
    for (source, target, ordinal, value) in rows {
        table.insert(
            (graph, source.as_str(), target.as_str(), *ordinal),
            value.as_slice(),
        )?;
    }
    Ok(())
}

fn insert_pressure_index_rows(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &[LanePressureIndexRow],
) -> Result<(), String> {
    let mut table = write.open_scoped_table(development_lane::PRESSURE_INDEX)?;
    for (tenant, lane, repository, observed, kind, level) in rows {
        table.insert(
            (
                graph,
                tenant.as_str(),
                lane.as_str(),
                repository.as_str(),
                *observed,
                kind.as_str(),
            ),
            *level,
        )?;
    }
    Ok(())
}

/// One `development_lane::PRESSURE_INDEX` row, owned.
///
/// Named rather than repeated inline: the raw 6-tuple appears in three
/// signatures and clippy's `type_complexity` (a hard error under `-D warnings`)
/// rejects it. Field order matches the table's own key/value layout:
/// `(tenant, lane, repository, observed_at_ms, source_ref, level)`.
type LanePressureIndexRow = (String, String, String, u64, String, u8);

// ── per-subsystem export / clear / insert ─────────────────────────────────

/// Every `(graph, tenant, id) -> blob` owner table exports the same triple, so the
/// six change tables and the two other three-part byte tables share one decoder.
fn export_tenant_keyed_blobs(
    read: &GraphRead<'_>,
    definition: TableDefinition<'static, (&str, &str, &str), &[u8]>,
) -> Result<Vec<(String, String, Vec<u8>)>, String> {
    export_rows(read, definition, |(_, tenant, id), value| {
        (tenant.to_string(), id.to_string(), value.to_vec())
    })
}

/// Every `(graph, first, second) -> text` index table exports the same triple.
fn export_two_keyed_text(
    read: &GraphRead<'_>,
    definition: TableDefinition<'static, (&str, &str, &str), &str>,
) -> Result<Vec<(String, String, String)>, String> {
    export_rows(read, definition, |(_, first, second), value| {
        (first.to_string(), second.to_string(), value.to_string())
    })
}

/// Every `(graph, id) -> blob` owner table exports the same pair.
fn export_keyed_blobs(
    read: &GraphRead<'_>,
    definition: TableDefinition<'static, (&str, &str), &[u8]>,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    export_rows(read, definition, |(_, id), value| {
        (id.to_string(), value.to_vec())
    })
}

fn export_change_rows(read: &GraphRead<'_>) -> Result<RawChangeRows, String> {
    Ok(RawChangeRows {
        envelopes: export_keyed_blobs(read, CHANGE_ENVELOPES)?,
        content_versions: export_tenant_keyed_blobs(read, CONTENT_VERSIONS)?,
        cursors: export_rows(
            read,
            CHANGE_CURSORS,
            |(_, tenant, source, partition), value| {
                (
                    tenant.to_string(),
                    source.to_string(),
                    partition.to_string(),
                    value.to_vec(),
                )
            },
        )?,
        blobs: export_tenant_keyed_blobs(read, CHANGE_BLOBS)?,
        features: export_tenant_keyed_blobs(read, CHANGE_FEATURES)?,
        evidence: export_tenant_keyed_blobs(read, CHANGE_EVIDENCE)?,
        policies: export_tenant_keyed_blobs(read, CHANGE_POLICIES)?,
        lineage: export_tenant_keyed_blobs(read, CHANGE_LINEAGE)?,
    })
}

/// Replace, don't merely upsert: `clear_change_material_rows` is the shared
/// owner of this table set (its `Method::DeleteGraph` path uses the same
/// function), so a retried move never duplicates and never drifts from delete.
fn import_change_rows(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &RawChangeRows,
) -> Result<(), String> {
    clear_change_material_rows_in_wtx(write, graph)?;
    insert_two_part_bytes(write, graph, CHANGE_ENVELOPES, &rows.envelopes)?;
    insert_three_part_bytes(write, graph, CONTENT_VERSIONS, &rows.content_versions)?;
    insert_three_part_bytes(write, graph, CHANGE_BLOBS, &rows.blobs)?;
    insert_three_part_bytes(write, graph, CHANGE_FEATURES, &rows.features)?;
    insert_three_part_bytes(write, graph, CHANGE_EVIDENCE, &rows.evidence)?;
    insert_three_part_bytes(write, graph, CHANGE_POLICIES, &rows.policies)?;
    insert_three_part_bytes(write, graph, CHANGE_LINEAGE, &rows.lineage)?;
    insert_four_part_bytes(write, graph, CHANGE_CURSORS, &rows.cursors)
}

fn export_resource_core_rows(
    read: &GraphRead<'_>,
    out: &mut RawResourceRows,
) -> Result<(), String> {
    out.reservations = export_keyed_blobs(read, RESOURCE_RESERVATIONS)?;
    out.reservation_tenant_index = export_two_keyed_text(read, RESOURCE_RESERVATION_TENANT_INDEX)?;
    out.reservation_attempts = export_rows(
        read,
        RESOURCE_RESERVATION_ATTEMPTS,
        |(_, id, attempt), value| (id.to_string(), attempt, value.to_string()),
    )?;
    out.hosts = export_keyed_blobs(read, RESOURCE_HOSTS)?;
    Ok(())
}

fn export_resource_policy_rows(
    read: &GraphRead<'_>,
    out: &mut RawResourceRows,
) -> Result<(), String> {
    out.exclusivity = export_rows(read, RESOURCE_EXCLUSIVITY, |(_, key), value| {
        (key.to_string(), value.to_string())
    })?;
    out.fairness = export_keyed_blobs(read, RESOURCE_FAIRNESS)?;
    out.concurrency = export_rows(read, RESOURCE_CONCURRENCY, |(_, key), value| {
        (key.to_string(), value)
    })?;
    out.anti_affinity = export_rows(read, RESOURCE_ANTI_AFFINITY, |(_, key, id), value| {
        (key.to_string(), id.to_string(), value)
    })?;
    out.disk_policies = export_keyed_blobs(read, RESOURCE_DISK_POLICIES)?;
    Ok(())
}

/// Every `resource_*` table for ONE graph (BUG-CX-054 class). Same tables
/// `shard_migrate.rs::populate_resource_tables_for_source` routes offline; an online
/// reshard must carry them too or a moved graph loses its reservation authority.
fn export_resource_rows(read: &GraphRead<'_>) -> Result<RawResourceRows, String> {
    let mut out = RawResourceRows::default();
    export_resource_core_rows(read, &mut out)?;
    export_resource_policy_rows(read, &mut out)?;
    Ok(out)
}

/// Scope-bounded remove of every `resource_*` row for ONE graph (no crypto — these
/// are opaque bytes to this module, the same "verbatim, never decode" contract as the
/// rest of the module). NOT `redb_store::clear_resource_rows_with_tables`: that shared helper
/// decrypts rows to enforce the "no active reservation" invariant for a graph DELETE,
/// which is the wrong contract for a MOVE (a graph with a live reservation must still
/// reshard — the reservation moves with it).
fn clear_resource_rows(write: &impl OwnerPayloadWrite, graph: &str) -> Result<(), String> {
    clear_two_part(write, graph, RESOURCE_RESERVATIONS)?;
    clear_three_part(write, graph, RESOURCE_RESERVATION_TENANT_INDEX)?;
    clear_text_sequence(write, graph, RESOURCE_RESERVATION_ATTEMPTS)?;
    clear_two_part(write, graph, RESOURCE_HOSTS)?;
    clear_two_part(write, graph, RESOURCE_EXCLUSIVITY)?;
    clear_two_part(write, graph, RESOURCE_FAIRNESS)?;
    clear_two_part(write, graph, RESOURCE_CONCURRENCY)?;
    clear_three_part(write, graph, RESOURCE_ANTI_AFFINITY)?;
    clear_two_part(write, graph, RESOURCE_DISK_POLICIES)
}

/// Replace, don't merely upsert (see [`import_change_rows`] for why).
fn import_resource_rows(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &RawResourceRows,
) -> Result<(), String> {
    clear_resource_rows(write, graph)?;
    insert_two_part_bytes(write, graph, RESOURCE_RESERVATIONS, &rows.reservations)?;
    insert_three_part_text(
        write,
        graph,
        RESOURCE_RESERVATION_TENANT_INDEX,
        &rows.reservation_tenant_index,
    )?;
    insert_text_sequence_text(
        write,
        graph,
        RESOURCE_RESERVATION_ATTEMPTS,
        &rows.reservation_attempts,
    )?;
    insert_two_part_bytes(write, graph, RESOURCE_HOSTS, &rows.hosts)?;
    insert_two_part_text(write, graph, RESOURCE_EXCLUSIVITY, &rows.exclusivity)?;
    insert_two_part_bytes(write, graph, RESOURCE_FAIRNESS, &rows.fairness)?;
    insert_two_part_count(write, graph, RESOURCE_CONCURRENCY, &rows.concurrency)?;
    insert_three_part_count(write, graph, RESOURCE_ANTI_AFFINITY, &rows.anti_affinity)?;
    insert_two_part_bytes(write, graph, RESOURCE_DISK_POLICIES, &rows.disk_policies)
}

fn export_lane_core_rows(
    read: &GraphRead<'_>,
    out: &mut RawDevelopmentLaneRows,
) -> Result<(), String> {
    out.holds = export_keyed_blobs(read, development_lane::HOLDS)?;
    out.counters = export_keyed_blobs(read, development_lane::COUNTERS)?;
    out.pressure_index = export_rows(
        read,
        development_lane::PRESSURE_INDEX,
        |(_, tenant, lane, repository, observed, kind), level| {
            (
                tenant.to_string(),
                lane.to_string(),
                repository.to_string(),
                observed,
                kind.to_string(),
                level,
            )
        },
    )?;
    out.policies = export_keyed_blobs(read, development_lane::POLICIES)?;
    Ok(())
}

fn export_lane_index_rows(
    read: &GraphRead<'_>,
    out: &mut RawDevelopmentLaneRows,
) -> Result<(), String> {
    out.tenant_index = export_two_keyed_text(read, development_lane::TENANT_INDEX)?;
    out.lane_index = export_two_keyed_text(read, development_lane::LANE_INDEX)?;
    out.repository_branch_index =
        export_two_keyed_text(read, development_lane::REPOSITORY_BRANCH_INDEX)?;
    out.worktree_index = export_rows(
        read,
        development_lane::WORKTREE_INDEX,
        |(_, worktree), value| (worktree.to_string(), value.to_string()),
    )?;
    Ok(())
}

fn export_lane_misc_rows(
    read: &GraphRead<'_>,
    out: &mut RawDevelopmentLaneRows,
) -> Result<(), String> {
    out.work_item_index = export_rows(
        read,
        development_lane::WORK_ITEM_INDEX,
        |(_, work_item, sequence), value| (work_item.to_string(), sequence, value.to_string()),
    )?;
    out.invocations = export_tenant_keyed_blobs(read, development_lane::INVOCATIONS)?;
    Ok(())
}

/// Every `development_lane_*` table for ONE graph (BUG-CX-054/096 class): all 10 lane
/// tables, the same set `shard_migrate.rs` routes offline and `redb_store` clears after
/// a graph is deleted — so before WD5-BUG-04 an online reshard silently dropped every
/// in-flight development-lane hold/counter/policy for the moved graph.
fn export_development_lane_rows(read: &GraphRead<'_>) -> Result<RawDevelopmentLaneRows, String> {
    let mut out = RawDevelopmentLaneRows::default();
    export_lane_core_rows(read, &mut out)?;
    export_lane_index_rows(read, &mut out)?;
    export_lane_misc_rows(read, &mut out)?;
    Ok(out)
}

/// Scope-bounded remove of every `development_lane_*` row for ONE graph (no crypto —
/// NOT `development_lane::clear_native_graph_rows_in_wtx`, which decrypts rows to
/// enforce delete-time invariants that do not apply to a MOVE).
fn clear_development_lane_rows(write: &impl OwnerPayloadWrite, graph: &str) -> Result<(), String> {
    clear_two_part(write, graph, development_lane::HOLDS)?;
    clear_two_part(write, graph, development_lane::COUNTERS)?;
    clear_pressure_index(write, graph)?;
    clear_two_part(write, graph, development_lane::POLICIES)?;
    clear_three_part(write, graph, development_lane::TENANT_INDEX)?;
    clear_three_part(write, graph, development_lane::LANE_INDEX)?;
    clear_three_part(write, graph, development_lane::REPOSITORY_BRANCH_INDEX)?;
    clear_two_part(write, graph, development_lane::WORKTREE_INDEX)?;
    clear_text_sequence(write, graph, development_lane::WORK_ITEM_INDEX)?;
    clear_three_part(write, graph, development_lane::INVOCATIONS)
}

/// Replace, don't merely upsert (see [`import_change_rows`] for why).
fn import_development_lane_rows(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &RawDevelopmentLaneRows,
) -> Result<(), String> {
    clear_development_lane_rows(write, graph)?;
    insert_two_part_bytes(write, graph, development_lane::HOLDS, &rows.holds)?;
    insert_two_part_bytes(write, graph, development_lane::COUNTERS, &rows.counters)?;
    insert_pressure_index_rows(write, graph, &rows.pressure_index)?;
    insert_two_part_bytes(write, graph, development_lane::POLICIES, &rows.policies)?;
    insert_three_part_text(
        write,
        graph,
        development_lane::TENANT_INDEX,
        &rows.tenant_index,
    )?;
    insert_three_part_text(write, graph, development_lane::LANE_INDEX, &rows.lane_index)?;
    insert_three_part_text(
        write,
        graph,
        development_lane::REPOSITORY_BRANCH_INDEX,
        &rows.repository_branch_index,
    )?;
    insert_two_part_text(
        write,
        graph,
        development_lane::WORKTREE_INDEX,
        &rows.worktree_index,
    )?;
    insert_text_sequence_text(
        write,
        graph,
        development_lane::WORK_ITEM_INDEX,
        &rows.work_item_index,
    )?;
    insert_three_part_bytes(
        write,
        graph,
        development_lane::INVOCATIONS,
        &rows.invocations,
    )
}

/// `capacity_cells` + `capacity_leases` + `capacity_usage` + `capacity_idempotency`
/// for ONE graph.
fn export_capacity_lease_rows(read: &GraphRead<'_>) -> Result<RawCapacityLeaseRows, String> {
    Ok(RawCapacityLeaseRows {
        cells: export_keyed_blobs(read, capacity_lease::CELLS)?,
        leases: export_keyed_blobs(read, capacity_lease::LEASES)?,
        usage: export_keyed_blobs(read, capacity_lease::USAGE)?,
        idempotency: export_tenant_keyed_blobs(read, capacity_lease::IDEMPOTENCY)?,
    })
}

/// Scope-bounded remove of every `capacity_*` row for ONE graph (no crypto — NOT
/// `capacity_lease::clear_graph_rows`, which is the delete-time helper; a move carries
/// this authority forward instead of draining it).
fn clear_capacity_lease_rows(write: &impl OwnerPayloadWrite, graph: &str) -> Result<(), String> {
    clear_two_part(write, graph, capacity_lease::CELLS)?;
    clear_two_part(write, graph, capacity_lease::LEASES)?;
    clear_two_part(write, graph, capacity_lease::USAGE)?;
    clear_three_part(write, graph, capacity_lease::IDEMPOTENCY)
}

/// Replace, don't merely upsert (see [`import_change_rows`] for why).
fn import_capacity_lease_rows(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &RawCapacityLeaseRows,
) -> Result<(), String> {
    clear_capacity_lease_rows(write, graph)?;
    insert_two_part_bytes(write, graph, capacity_lease::CELLS, &rows.cells)?;
    insert_two_part_bytes(write, graph, capacity_lease::LEASES, &rows.leases)?;
    insert_two_part_bytes(write, graph, capacity_lease::USAGE, &rows.usage)?;
    insert_three_part_bytes(write, graph, capacity_lease::IDEMPOTENCY, &rows.idempotency)
}

/// Replace the graph's `work_item_command_sequence` row: a single value per graph,
/// unlike every other table in this lane's scope, and scope-prefixed like all of
/// them — the whole-table scan it used to need is now a bounded `get`/`remove`.
fn import_work_item_command_sequence(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    sequence: Option<u64>,
) -> Result<(), String> {
    let mut table = write.open_scoped_table(WORK_ITEM_COMMAND_SEQUENCE)?;
    table.remove(graph)?;
    match sequence {
        Some(sequence) => table.insert(graph, sequence),
        None => Ok(()),
    }
}

// ── the file-wide catalog row ─────────────────────────────────────────────

/// One graph's `graph_meta` catalog row, read through the CONTROL scope.
///
/// `graph_meta` is file-wide, not one graph's rows: it is the shard file's catalog
/// of which graphs it hosts, and the boot scan has to enumerate it before any graph
/// scope can be bound. A graph scope may not open it at all.
fn read_graph_meta(shard: &Shard, graph: &str) -> Result<Option<Vec<u8>>, String> {
    Ok(shard
        .control_read()?
        .open_owner_table(GRAPH_META)?
        .get(graph)
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_vec()))
}

/// Write or drop one graph's `graph_meta` catalog row through the control member of
/// the same admitted group that writes the graph's own rows.
fn write_graph_meta(
    control: &impl OwnerPayloadWrite,
    graph: &str,
    meta: Option<&[u8]>,
) -> Result<(), String> {
    let mut table = control.open_table(GRAPH_META)?;
    match meta {
        Some(blob) => table.insert(graph, blob).map(|_| ()),
        None => table.remove(graph).map(|_| ()),
    }
    .map_err(|error| error.to_string())
}

// ── the admitted write every destination-side import performs ─────────────

/// A maintenance id unique per ATTEMPT.
///
/// Deliberately not derived from the graph and shard alone: `admit_maintenance`
/// treats a repeated id at a moved version as a replay of a batch this attempt did
/// not apply, which fails the ledger's whole-batch identity comparison instead of
/// applying the rows. See `shard::drain_batch`'s doc on `drain_id`.
fn attempt_id(kind: &str, graph: &str) -> String {
    static ATTEMPT: AtomicU64 = AtomicU64::new(0);
    format!(
        "online_reshard/{kind}/{graph}:{}:{}",
        crate::server::txn::now_ms(),
        ATTEMPT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The five-step admitted write of contract §1, around one caller's row work.
///
/// Binding a cold graph opens its own write transaction, so `graph_members` runs
/// FIRST and outside the group's transaction. `rows` gets the whole [`ShardWrite`]
/// because a destination import writes both the graph's scope-prefixed rows and the
/// file-wide `graph_meta` catalog row, and both must land in the one commit. An empty
/// `graphs` is the control-only write, which is what dropping a catalog row is.
///
/// This is the shard's own bookkeeping, so the class is `Maintenance`: none of these
/// writes carries a caller operation identity, and the class is the honest label
/// rather than a knob.
fn admitted_write<Rows>(shard: &Shard, graphs: &[&str], op: &str, rows: Rows) -> Result<(), String>
where
    Rows: FnOnce(&ShardWrite<'_>) -> Result<(), String>,
{
    let members = shard.graph_members(graphs)?;
    let (group, batches) = shard.admit_maintenance(&members, op)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    // Every member's owner-row admission has to be closed even when the row work
    // failed: dropping one unfinished poisons the shared transaction.
    let outcome = rows(&write).and(write.finish());
    match outcome {
        Ok(()) => shard.commit_drain(group, &batches, crate::server::txn::now_ms()),
        Err(error) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

// ── export ────────────────────────────────────────────────────────────────

/// Scan ONE graph's durable OWNER rows VERBATIM off a shard
/// (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on the owning shard's writer
/// thread (via [`Cmd::ExportGraphRaw`]) AFTER it has flushed pending writes, so the
/// snapshot reflects every committed mutation. Value blobs are copied raw (no
/// `crypto.unseal`) so encryption-at-rest and the audit chain survive the move.
///
/// The graph's kernel LEDGER is deliberately absent: it moves by graft, not through
/// this image — see the module doc.
pub(crate) fn export_graph_raw(shard: &Shard, graph: &str) -> Result<RawGraphRows, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    Ok(RawGraphRows {
        meta: read_graph_meta(shard, graph)?,
        nodes: export_keyed_blobs(&read, NODES)?,
        edges: export_rows(&read, EDGES, |(_, source, target, ordinal), value| {
            (
                source.to_string(),
                target.to_string(),
                ordinal,
                value.to_vec(),
            )
        })?,
        ledger: export_rows(&read, LEDGER, |(_, sequence), line| {
            (sequence, line.to_string())
        })?,
        semantic: read
            .scoped_owner_table(SEMANTIC)?
            .get(graph)?
            .map(|value| value.value().to_vec()),
        #[cfg(feature = "security")]
        audit: export_rows(&read, AUDIT, |(_, sequence), value| {
            (sequence, value.to_vec())
        })?,
        #[cfg(feature = "security")]
        provenance_anchor_members: export_rows(
            &read,
            PROVENANCE_ANCHOR_MEMBERS,
            |(_, sequence), value| (sequence, value.to_vec()),
        )?,
        change: export_change_rows(&read)?,
        resource: export_resource_rows(&read)?,
        development_lane: export_development_lane_rows(&read)?,
        capacity_lease: export_capacity_lease_rows(&read)?,
        work_item_command_sequence: read
            .scoped_owner_table(WORK_ITEM_COMMAND_SEQUENCE)?
            .get(graph)?
            .map(|value| value.value()),
        requires_graft_reservation: false,
        schema_version: RAW_GRAPH_ROWS_SCHEMA_VERSION,
    })
}

// ── import ────────────────────────────────────────────────────────────────

/// Empty the destination graph's scope before a replacement image lands.
///
/// Replace, do not merely upsert: a retried online copy and a Raft snapshot install
/// must REMOVE rows that are no longer present in the source image, which an upsert
/// cannot express. Raw reshard images also intentionally carry no capability bytes,
/// invocation idempotency or native WorkItem provenance, so the destination's private
/// work-item authority is purged in this same transaction — otherwise a shard
/// reuse/retry could make an old capability valid against a replacement image.
fn clear_graph_scope(write: &impl OwnerPayloadWrite, graph: &str) -> Result<(), String> {
    crate::redb_store::work_item_capability::clear_graph_rows_in_payload(write)?;
    {
        // `redb` refuses a second open of a table whose first handle is alive, so
        // the three projection tables are opened once, cleared together by the
        // shared helper, and dropped before the insert pass reopens them.
        let mut nodes = write.open_scoped_table(NODES)?;
        let mut edges = write.open_scoped_table(EDGES)?;
        let mut ledger = write.open_scoped_table(LEDGER)?;
        clear_graph_rows(graph, &mut nodes, &mut edges, &mut ledger)?;
    }
    write.open_scoped_table(SEMANTIC)?.remove(graph)?;
    #[cfg(feature = "security")]
    {
        // A snapshot install or retried reshard is an exact replacement: any
        // destination audit tail absent from the incoming image goes before the
        // source chain is copied verbatim over it.
        clear_sequence(write, graph, AUDIT)?;
        clear_sequence(write, graph, PROVENANCE_ANCHOR_MEMBERS)?;
    }
    Ok(())
}

/// Land one graph's whole owner-row image inside an already-open member write.
fn insert_graph_scope(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    rows: &RawGraphRows,
) -> Result<(), String> {
    insert_two_part_bytes(write, graph, NODES, &rows.nodes)?;
    insert_edge_rows(write, graph, &rows.edges)?;
    insert_ledger_rows(write, graph, &rows.ledger)?;
    if let Some(blob) = &rows.semantic {
        write
            .open_scoped_table(SEMANTIC)?
            .insert(graph, blob.as_slice())?;
    }
    #[cfg(feature = "security")]
    {
        insert_sequence_bytes(write, graph, AUDIT, &rows.audit)?;
        insert_sequence_bytes(
            write,
            graph,
            PROVENANCE_ANCHOR_MEMBERS,
            &rows.provenance_anchor_members,
        )?;
    }
    import_change_rows(write, graph, &rows.change)?;
    import_resource_rows(write, graph, &rows.resource)?;
    import_development_lane_rows(write, graph, &rows.development_lane)?;
    import_capacity_lease_rows(write, graph, &rows.capacity_lease)?;
    import_work_item_command_sequence(write, graph, rows.work_item_command_sequence)
}

/// Insert ONE graph's verbatim owner rows into a destination shard in ONE admitted,
/// durable commit (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on the
/// destination shard's writer thread (via [`Cmd::ImportGraphRaw`]); the commit is the
/// commit-before-ack point of the bulk pass. Idempotent — a re-run after an
/// interrupted move replaces rather than duplicates.
pub(crate) fn import_graph_raw(
    shard: &Shard,
    graph: &str,
    rows: &RawGraphRows,
) -> Result<(), String> {
    let protocol_guard = shard.graft_protocol_guard(graph)?;
    let _protocol_guard = protocol_guard
        .lock()
        .map_err(|_| "graph graft protocol guard is poisoned".to_string())?;
    rows.durable_identity(graph)?;
    let reserved = shard.graft_destination_reserved(graph)?;
    if rows.requires_graft_reservation && !reserved {
        return Err(format!(
            "graft owner import for '{graph}' lost its destination reservation"
        ));
    }
    if reserved {
        shard.stage_reserved_graft_owner_payload(graph, |write| {
            clear_graph_scope(write, graph)?;
            insert_graph_scope(write, graph, rows)
        })?;
        // `graph_meta` is file-wide and therefore deliberately outside the
        // reservation-authenticated graph owner capability.  It lands in its
        // own control-only transaction after the owner stage; a crash between
        // these two commits is safe because the durable reservation makes the
        // owner stage resumable and the catalog write is idempotent.
        return admitted_write(shard, &[], &attempt_id("import_raw_meta", graph), |write| {
            write_graph_meta(write.control(), graph, rows.meta.as_deref())
        });
    }
    admitted_write(shard, &[graph], &attempt_id("import_raw", graph), |write| {
        let member = write.graph(graph)?;
        clear_graph_scope(member, graph)?;
        insert_graph_scope(member, graph, rows)?;
        write_graph_meta(write.control(), graph, rows.meta.as_deref())
    })
}

/// Apply the WD5-BUG-04 delta pieces (`resource_*`, `development_lane_*`,
/// `capacity_*`, provenance-anchor-members, WorkItem command sequence) — the same
/// "only touch what `compute_delta` marked changed" contract as the rest of the
/// delta. Split out of [`apply_graph_delta`] so that function stays under the cap.
fn apply_capability_and_resource_delta(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    delta: &RawGraphDelta,
) -> Result<(), String> {
    if let Some(rows) = &delta.replace_resource {
        import_resource_rows(write, graph, rows)?;
    }
    if let Some(rows) = &delta.replace_development_lane {
        import_development_lane_rows(write, graph, rows)?;
    }
    if let Some(rows) = &delta.replace_capacity_lease {
        import_capacity_lease_rows(write, graph, rows)?;
    }
    #[cfg(feature = "security")]
    if let Some(rows) = &delta.replace_provenance_anchor_members {
        clear_sequence(write, graph, PROVENANCE_ANCHOR_MEMBERS)?;
        insert_sequence_bytes(write, graph, PROVENANCE_ANCHOR_MEMBERS, rows)?;
    }
    match &delta.work_item_command_sequence {
        Some(RowChange::Set(sequence)) => {
            import_work_item_command_sequence(write, graph, Some(*sequence))
        }
        Some(RowChange::Cleared) => import_work_item_command_sequence(write, graph, None),
        None => Ok(()),
    }
}

fn apply_graph_delta(
    write: &impl OwnerPayloadWrite,
    graph: &str,
    delta: &RawGraphDelta,
) -> Result<(), String> {
    // The destination's private work-item authority is invalidated on EVERY delta
    // import for the same reason as on a raw import: a retry into a reused shard
    // must not leave an old capability valid against copied WorkItem rows.
    crate::redb_store::work_item_capability::clear_graph_rows_in_payload(write)?;
    {
        let mut nodes = write.open_scoped_table(NODES)?;
        for (id, blob) in &delta.upsert_nodes {
            nodes.insert((graph, id.as_str()), blob.as_slice())?;
        }
        for id in &delta.remove_nodes {
            nodes.remove((graph, id.as_str()))?;
        }
    }
    {
        let mut edges = write.open_scoped_table(EDGES)?;
        for (source, target, ordinal, blob) in &delta.upsert_edges {
            edges.insert(
                (graph, source.as_str(), target.as_str(), *ordinal),
                blob.as_slice(),
            )?;
        }
        for (source, target, ordinal) in &delta.remove_edges {
            edges.remove((graph, source.as_str(), target.as_str(), *ordinal))?;
        }
    }
    insert_ledger_rows(write, graph, &delta.upsert_ledger)?;
    if let Some(change) = &delta.semantic {
        let mut semantic = write.open_scoped_table(SEMANTIC)?;
        match change {
            RowChange::Set(blob) => semantic.insert(graph, blob.as_slice())?,
            RowChange::Cleared => semantic.remove(graph)?,
        }
    }
    #[cfg(feature = "security")]
    insert_sequence_bytes(write, graph, AUDIT, &delta.upsert_audit)?;
    if let Some(rows) = &delta.replace_change {
        import_change_rows(write, graph, rows)?;
    }
    apply_capability_and_resource_delta(write, graph, delta)
}

/// Apply a [`RawGraphDelta`] to the destination shard in ONE admitted, durable commit
/// (CONCEPT:EG-KG.backend.flush-pending-first). Runs on the destination shard's writer
/// thread (via [`Cmd::ImportGraphDelta`]); the commit is the delta's
/// commit-before-flip point. O(delta) rows — the short under-quiesce write, vs the
/// full O(graph) copy the bulk pass already did unquiesced.
pub(crate) fn import_graph_delta(
    shard: &Shard,
    graph: &str,
    delta: &RawGraphDelta,
) -> Result<(), String> {
    let protocol_guard = shard.graft_protocol_guard(graph)?;
    let _protocol_guard = protocol_guard
        .lock()
        .map_err(|_| "graph graft protocol guard is poisoned".to_string())?;
    let reserved = shard.graft_destination_reserved(graph)?;
    if delta.requires_graft_reservation && !reserved {
        return Err(format!(
            "graft owner delta for '{graph}' lost its destination reservation"
        ));
    }
    if reserved {
        shard.stage_reserved_graft_owner_payload(graph, |write| {
            apply_graph_delta(write, graph, delta)
        })?;
        if let Some(meta) = &delta.meta {
            return admitted_write(
                shard,
                &[],
                &attempt_id("import_delta_meta", graph),
                |write| write_graph_meta(write.control(), graph, Some(meta.as_slice())),
            );
        }
        return Ok(());
    }
    admitted_write(
        shard,
        &[graph],
        &attempt_id("import_delta", graph),
        |write| {
            apply_graph_delta(write.graph(graph)?, graph, delta)?;
            match &delta.meta {
                Some(blob) => write_graph_meta(write.control(), graph, Some(blob.as_slice())),
                None => Ok(()),
            }
        },
    )
}

// ── the cutover ───────────────────────────────────────────────────────────

/// Drop one graph from a shard file's own catalog.
///
/// A control-only admitted write: `members` is empty, because `graph_meta` belongs to
/// the file rather than to any one graph. It runs only after the graft has committed;
/// a refusal or crash before the graft therefore cannot erase the source catalog.
fn drop_from_catalog(shard: &Shard, graph: &str) -> Result<(), String> {
    admitted_write(shard, &[], &attempt_id("drop_catalog", graph), |write| {
        write_graph_meta(write.control(), graph, None)
    })
}

/// THE CUTOVER: move `graph`'s kernel ledger from `source` into `destination` and
/// retire the source.
///
/// This is the LAST step of an online move: the graft retires the source binding
/// together with its owner payload, so after it returns **nothing may read the source
/// graph**. A lost acknowledgement after that retirement resumes from the durable
/// destination marker without rebinding the source. Everything that re-reads the
/// source — the bulk pass and, critically, the delta catch-up whose whole job is to
/// re-read it — has to have completed first.
///
/// The ledger crosses verbatim rather than by re-admission, and
/// [`GraftedScope::version`] is the SOURCE's marker-inclusive version, preserved by the
/// copy: the Phase-A marker is the one intentional maintenance increment, and the
/// destination receives the complete source history rather than re-admitting batches
/// (RF-RULING-004 application note 4).
pub(crate) fn graft_and_retire(
    source: &Shard,
    destination: &Shard,
    graph: &str,
) -> Result<GraftedScope, String> {
    let grafted = destination.graft_graph_from(source, graph)?;
    drop_from_catalog(source, graph)?;
    Ok(grafted)
}

// ── the two phases ────────────────────────────────────────────────────────

/// The two shard files one online move spans.
///
/// Each endpoint is a pair, and both halves are load-bearing: the [`Shard`] itself,
/// which [`graft_and_retire`] needs on BOTH sides at once and so cannot reach through
/// a per-shard writer thread, and that writer thread's command channel, through which
/// the bulk and delta passes go so the shard's pending coalesced ops are flushed
/// before a snapshot is taken or rows land.
pub(crate) struct ReshardEndpoints<'a> {
    pub source: &'a Shard,
    pub source_tx: &'a SyncSender<Cmd>,
    pub source_index: usize,
    pub destination: &'a Shard,
    pub destination_tx: &'a SyncSender<Cmd>,
    pub destination_index: usize,
}

fn writer_gone() -> String {
    "redb writer thread is gone".to_string()
}

/// Export the graph's owner rows VERBATIM off the source shard's flush-then-scan
/// snapshot.
fn export_from(source_tx: &SyncSender<Cmd>, graph: &str) -> Result<RawGraphRows, String> {
    let (reply, receive) = std::sync::mpsc::sync_channel(1);
    source_tx
        .send(Cmd::ExportGraphRaw {
            graph: graph.to_string(),
            reply,
        })
        .map_err(|_| writer_gone())?;
    await_writer_reply(&receive, "export")?
}

/// PHASE 1 of the online move (CONCEPT:EG-KG.backend.flush-pending-first, R1 delta-copy) — the BULK pass, run WITHOUT
/// the exclusive routing quiesce so writes keep flowing to the source and the graph is
/// NOT paused. Export the graph's owner rows verbatim off a source snapshot and import
/// them into the destination in one admitted, durable commit. Returns the bulk snapshot
/// so [`delta_flip_purge`] can diff the final state against it under the quiesce.
/// Idempotent — a re-run after an interrupted move replaces rather than duplicates.
///
/// This is NOT the cutover: the source still serves the graph when it returns.
pub(crate) fn bulk_copy(
    endpoints: &ReshardEndpoints<'_>,
    graph: &str,
) -> Result<RawGraphRows, String> {
    let mut rows = export_from(endpoints.source_tx, graph)?;
    endpoints
        .destination
        .reserve_graft_destination(endpoints.source, graph)?;
    rows.requires_graft_reservation = true;
    let (reply, receive) = std::sync::mpsc::sync_channel(1);
    endpoints
        .destination_tx
        .send(Cmd::ImportGraphRaw {
            graph: graph.to_string(),
            rows: Box::new(rows.clone()),
            reply,
        })
        .map_err(|_| writer_gone())?;
    await_writer_reply(&receive, "import")??;
    Ok(rows)
}

/// PHASE 2 of the online move (CONCEPT:EG-KG.backend.flush-pending-first, R1) — run on a blocking thread WHILE the
/// exclusive `routing_epoch` WRITE guard is held, so writes to this graph are quiesced
/// for only this short window. Re-export the LATEST source state, copy just the DELTA
/// accrued since the bulk pass into the destination, flip the catalog route, then graft
/// the ledger and retire the source. The pause is O(delta) (the under-quiesce import)
/// rather than O(graph).
///
/// The ordering is `delta -> flip -> graft`, and each step depends on the one before:
///
/// 1. the delta is the LAST read of the source, because step 3 retires it;
/// 2. the flip is before the graft so a crash between them leaves the destination
///    holding every owner row with the ledger still authoritative on the source, which
///    re-running completes — the reverse order would leave a routed-to source whose
///    scope no longer exists;
/// 3. the graft is the cutover. Nothing may read the source graph after it.
///
/// No write can observe an intermediate state, because the whole function runs under
/// the exclusive routing quiesce its caller holds.
pub(crate) fn delta_flip_purge(
    endpoints: &ReshardEndpoints<'_>,
    catalog: &TenantCatalog,
    graph: &str,
    bulk: RawGraphRows,
) -> Result<ReshardReport, String> {
    // 1. Re-export the latest source state (it now reflects every write that landed
    //    during the unquiesced bulk pass) and diff it against the bulk snapshot.
    let latest = export_from(endpoints.source_tx, graph)?;
    let mut delta = compute_delta(&bulk, &latest);
    delta.requires_graft_reservation = true;
    let mut report = ReshardReport::counts(
        graph,
        endpoints.source_index,
        endpoints.destination_index,
        &latest,
    );
    report.delta_nodes = delta.nodes();
    report.delta_edges = delta.edges();

    // 2. Import ONLY the delta into the destination, awaiting the durable commit.
    let (reply, receive) = std::sync::mpsc::sync_channel(1);
    endpoints
        .destination_tx
        .send(Cmd::ImportGraphDelta {
            graph: graph.to_string(),
            delta: Box::new(delta),
            reply,
        })
        .map_err(|_| writer_gone())?;
    await_writer_reply(&receive, "delta import")??;

    // 3. Flip the durable route — reads/writes now follow the graph to the destination
    //    (preserving any cluster-node placement).
    let node = catalog.lookup(graph).and_then(|assignment| assignment.node);
    catalog.assign(graph, endpoints.destination_index as u32, node)?;

    // 4. Graft the ledger and retire the source. The cutover, and the source GC: the
    //    graft sweeps the source's owner payload, so there is no separate purge pass.
    let grafted = graft_and_retire(endpoints.source, endpoints.destination, graph)?;
    report.grafted_ledger_rows = grafted.rows;
    report.grafted_version = grafted.version;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redb_store::encode_meta_with_incarnation;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        crate::redb_store::temp_path("eg-online-reshard", tag)
    }

    fn image(graph: &str, nodes: &[(&str, &[u8])]) -> RawGraphRows {
        RawGraphRows {
            meta: Some(
                encode_meta_with_incarnation(
                    graph,
                    GraphType::Global,
                    "incarnation:test:online-reshard",
                )
                .unwrap(),
            ),
            nodes: nodes
                .iter()
                .map(|(id, blob)| ((*id).to_string(), blob.to_vec()))
                .collect(),
            ..RawGraphRows::default()
        }
    }

    /// The raw image is OWNER rows only. An image still carrying the retired
    /// `mutation` field — schema 1's serialized private-ledger tables — is refused
    /// outright rather than half-applied, because the ledger now moves by graft.
    #[test]
    fn a_raw_image_carrying_ledger_rows_is_refused() {
        #[derive(serde::Serialize)]
        struct WithMutationRows {
            schema_version: u16,
            meta: Option<Vec<u8>>,
            nodes: Vec<(String, Vec<u8>)>,
            mutation: Vec<(String, String)>,
        }
        let encoded = rmp_serde::to_vec_named(&WithMutationRows {
            schema_version: RAW_GRAPH_ROWS_SCHEMA_VERSION,
            meta: None,
            nodes: Vec::new(),
            mutation: Vec::new(),
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<RawGraphRows>(&encoded).is_err());

        let stale = RawGraphRows {
            schema_version: 1,
            ..RawGraphRows::default()
        };
        assert!(stale.validate_schema().is_err());
    }

    /// An import REPLACES the destination graph's rows: a row present only in the
    /// destination's previous image is gone afterwards, which an upsert cannot express
    /// and which a retried move and a Raft snapshot install both depend on.
    #[test]
    fn an_import_replaces_the_destination_graphs_rows() {
        let path = temp_path("replace");
        let shard = Shard::open(&path).unwrap();
        import_graph_raw(&shard, "graph-a", &image("graph-a", &[("n1", b"one")])).unwrap();
        import_graph_raw(&shard, "graph-a", &image("graph-a", &[("n2", b"two")])).unwrap();

        let exported = export_graph_raw(&shard, "graph-a").unwrap();
        assert_eq!(exported.nodes, vec![("n2".to_string(), b"two".to_vec())]);
        let _ = std::fs::remove_file(&path);
    }

    /// An export reads exactly one graph's rows, and no other graph's, because the
    /// scan takes its bound from the scope the read was issued for.
    #[test]
    fn an_export_reads_only_its_own_graphs_rows() {
        let path = temp_path("bounded");
        let shard = Shard::open(&path).unwrap();
        import_graph_raw(&shard, "graph-a", &image("graph-a", &[("n1", b"a")])).unwrap();
        import_graph_raw(&shard, "graph-b", &image("graph-b", &[("n1", b"b")])).unwrap();

        assert_eq!(
            export_graph_raw(&shard, "graph-a").unwrap().nodes,
            vec![("n1".to_string(), b"a".to_vec())]
        );
        assert_eq!(
            export_graph_raw(&shard, "graph-b").unwrap().nodes,
            vec![("n1".to_string(), b"b".to_vec())]
        );
        let _ = std::fs::remove_file(&path);
    }

    /// RF-RULING-004 application note 4, at the level this module drives it: after the
    /// cutover the destination serves the graph at the SOURCE's marker-inclusive
    /// version — the Phase-A marker is the one intentional maintenance increment —
    /// rather than a fresh re-admission, and the source no longer serves the graph.
    #[test]
    fn the_destination_serves_the_moved_graph_at_the_sources_version() {
        let source_path = temp_path("graft-src");
        let destination_path = temp_path("graft-dst");
        let source = Shard::open(&source_path).unwrap();
        let destination = Shard::open(&destination_path).unwrap();

        let rows = image("graph-a", &[("n1", b"one")]);
        import_graph_raw(&source, "graph-a", &rows).unwrap();
        let source_version =
            eg_transaction::version(&source.read(&source.graph("graph-a").unwrap()).unwrap())
                .unwrap();

        // Reserve the destination before staging owner rows. The graft then
        // authenticates the reservation and copies the source ledger without
        // a pre-graft maintenance receipt in the destination.
        destination
            .reserve_graft_destination(&source, "graph-a")
            .unwrap();
        import_graph_raw(&destination, "graph-a", &rows).unwrap();
        let grafted = graft_and_retire(&source, &destination, "graph-a").unwrap();

        assert_eq!(grafted.version, source_version + 1);
        assert_eq!(
            read_graph_meta(&source, "graph-a").unwrap(),
            None,
            "the source still advertises a graph it no longer serves"
        );
        assert_eq!(
            export_graph_raw(&destination, "graph-a").unwrap().nodes,
            vec![("n1".to_string(), b"one".to_vec())]
        );
        let destination_handle = destination.graph("graph-a").unwrap();
        let destination_read = destination.read(&destination_handle).unwrap();
        assert!(
            eg_transaction::read_batches(&destination_read)
                .unwrap()
                .iter()
                .all(|record| !record.batch.batch_id.starts_with("online_reshard/")),
            "owner staging must not manufacture a destination mutation receipt"
        );

        // A retry after Phase C must authenticate the destination marker without
        // rebinding the retired source scope.
        let resumed = graft_and_retire(&source, &destination, "graph-a").unwrap();
        assert!(resumed.resumed);
        assert_eq!(resumed.version, grafted.version);

        let _ = std::fs::remove_file(&source_path);
        let _ = std::fs::remove_file(&destination_path);
    }

    /// Same-authority refusal happens before the source catalog or binding is
    /// touched. This guards the cutover ordering: catalog retirement follows
    /// a successful graft, never its preflight.
    #[test]
    fn same_authority_graft_refuses_without_retiring_the_source() {
        let path = temp_path("graft-same-authority");
        let shard = Shard::open(&path).unwrap();
        let identity = crate::redb_store::shard::graph_scope_identity("graph-a").unwrap();
        let rows = image("graph-a", &[("n1", b"one")]);
        import_graph_raw(&shard, "graph-a", &rows).unwrap();
        let before_meta = read_graph_meta(&shard, "graph-a").unwrap();
        let before_rows = export_graph_raw(&shard, "graph-a").unwrap();
        let before_version = {
            let handle = shard.graph("graph-a").unwrap();
            let read = shard.read(&handle).unwrap();
            eg_transaction::version(&read).unwrap()
        };
        let before_batches = {
            let handle = shard.graph("graph-a").unwrap();
            let read = shard.read(&handle).unwrap();
            eg_transaction::read_batches(&read)
                .unwrap()
                .into_iter()
                .map(|record| record.batch.batch_id)
                .collect::<Vec<_>>()
        };

        let error = graft_and_retire(&shard, &shard, "graph-a").unwrap_err();
        assert!(error.contains("same authority"), "got: {error}");
        assert!(
            shard.kernel().scope_binding_exists(&identity).unwrap(),
            "same-authority refusal retired the source binding"
        );
        assert_eq!(read_graph_meta(&shard, "graph-a").unwrap(), before_meta);
        assert_eq!(export_graph_raw(&shard, "graph-a").unwrap(), before_rows);
        let after_version = {
            let handle = shard.graph("graph-a").unwrap();
            let read = shard.read(&handle).unwrap();
            eg_transaction::version(&read).unwrap()
        };
        let after_batches = {
            let handle = shard.graph("graph-a").unwrap();
            let read = shard.read(&handle).unwrap();
            eg_transaction::read_batches(&read)
                .unwrap()
                .into_iter()
                .map(|record| record.batch.batch_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(after_version, before_version);
        assert_eq!(after_batches, before_batches);
        let _ = std::fs::remove_file(&path);
    }
}
