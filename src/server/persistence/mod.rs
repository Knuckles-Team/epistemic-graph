//! Authoritative durable graph-store contract (CONCEPT:EG-KG.storage.kg-kg).
//!
//! The served engine has one implementation, [`redb_backend::RedbBackend`]. The
//! trait keeps mutation, recovery, read-through, backup, and Raft consumers on one
//! contract without exposing storage internals. Every public mutation is committed
//! before acknowledgement; bounded queues apply backpressure and never drop work.

use crate::mutation_batch::MutationBatch;
use crate::protocol::Method;

mod contract;
pub use contract::PersistenceBackend;

pub mod read_through;

/// Bounded waits on the shard-owner writer thread's command replies.
pub(crate) mod writer_reply;

// The canonical registry of every durable redb store the engine opens under a persist
// dir, plus each store's backup scope (bundled / deliberately excluded, with a reason).
// Consumed by `backup` so a bundle is self-describing about what a restore restores.
#[cfg(feature = "redb")]
pub mod durable_stores;

// The three-layer native agent hierarchy (RF-ADR-008) is a set of durable redb
// owners; every consumer is itself `redb`-gated.
#[cfg(feature = "redb")]
pub mod agent_component;
#[cfg(all(test, feature = "redb"))]
pub(crate) mod agent_fixtures;
#[cfg(feature = "redb")]
pub mod agent_graph;
#[cfg(feature = "redb")]
pub mod agent_library;
#[cfg(feature = "redb")]
pub mod agent_pin_resolution;
#[cfg(feature = "redb")]
mod agent_revision;
#[cfg(feature = "redb")]
pub mod agent_row;
#[cfg(feature = "redb")]
pub mod agent_template;
#[cfg(feature = "redb")]
pub mod connector_pack;
#[cfg(feature = "redb")]
pub mod decision_jobs;
#[cfg(feature = "redb")]
pub mod decision_record;
// EH-345: the fleet catalog's one read over the AgentComponent owner.
#[cfg(feature = "redb")]
pub mod fleet_components;
#[cfg(feature = "redb")]
pub mod write_back;

#[cfg(feature = "redb")]
pub mod redb_backend;

// M3 — catalog-driven resharding (CONCEPT:EG-KG.sharding.atomic-shard-swap / EG-031). Both are redb-only:
//   * `shard_migrate` — OFFLINE K-shard migration tool that rewrites an existing
//     canonical `graph-<n>.redb` set into a new K using the SAME routing,
//     preserving every durable row verbatim (incl. the tamper-evident audit chain).
//   * `tenant_catalog` — durable graph/tenant→shard (and future →node) map + a
//     read-only routing-override seam, defaulting to EG-026 FNV-1a when empty.
#[cfg(feature = "redb")]
pub mod shard_migrate;

#[cfg(feature = "redb")]
pub mod tenant_catalog;

// Cluster node-info store (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1): durable
// node_id -> {raft_addr, advertised_client_addr, tls_server_name} map that backs
// `Method::ClusterMembers`/`PlacementRoute.endpoints`, replacing the static
// `GRAPH_RAFT_GROUP_ENDPOINTS` client map. Mirrors `tenant_catalog`'s own-file,
// in-memory-cache shape. Redb-only, like its M3 siblings above.
#[cfg(feature = "redb")]
pub(crate) mod node_info_store;

// VIZ-1: durable cache for the computed hierarchical-Leiden cluster tree
// (CONCEPT:EG-KG.compute.leiden-hierarchy) one graph's `Method::ClusterHierarchyRefresh`
// produces. Own-file, own-table, keyed by graph name -- mirrors `node_info_store`'s
// shape (own redb file under the same `persist_dir`) but carries NO replication
// story (unlike node_info's Raft self-report): this is a plain per-node cache of a
// value that is always re-derivable from the live graph, never authoritative,
// never a KG node/edge write (see the `Method::ClusterHierarchyRefresh` doc for
// why membership is deliberately NOT represented as graph edges). Redb-only, like
// every other own-file side store in this module.
#[cfg(feature = "redb")]
pub mod cluster_hierarchy_store;

// M3 keystone (CONCEPT:EG-KG.backend.catalog-shard-resolve / EG-034). Both redb-only:
//   * `online_reshard` — move ONE graph between shards while the engine RUNS (verbatim
//     row copy + catalog route flip + source GC), building on EG-030's copy + EG-031's
//     catalog. The keystone the offline EG-030 tool skips.
//   * `cold_offload` — time-windowed whole-graph offload of idle tenants (hibernate +
//     read-through serve) to bound RAM across many tenants.
#[cfg(feature = "redb")]
pub mod online_reshard;

#[cfg(feature = "redb")]
pub mod cold_offload;

// Provenance anchoring (CONCEPT:EG-KG.sharding.row-level-security): the periodic sweep that Merkle-anchors
// each resident graph's `:ToolCall`/`:RunTrace` provenance-node window into the
// SAME tamper-evident audit chain `security` already maintains. Redb-only (the
// audit chain and its `PROVENANCE_ANCHOR_MEMBERS` side table are redb tables).
#[cfg(feature = "security")]
pub mod provenance_anchor;

// M3 R3 — rebalancing planner (CONCEPT:EG-KG.sharding.even-load-rebalance). A PURE, deterministic policy layer
// over observable per-shard/per-graph load + the EG-031 catalog that EMITS a plan of
// `{graph, from_shard, to_shard}` moves to even out load. It does NOT execute the
// plan — that is R1 online resharding (online_reshard above). Parallel-safe, no M2 dep.
#[cfg(feature = "redb")]
pub mod rebalance;

// Background node-payload scrub (EH-384, CONCEPT:EG-KG.storage.node-payload-scrub):
// bounded, resumable, read-only detection of unreadable node rows between restarts.
#[cfg(feature = "redb")]
pub mod storage_scrub;

// EG-090 — online consistent backup/restore + PITR foundation. Redb-only:
//   * `backup` — per-shard `begin_read()` MVCC snapshot (EG-027) streamed verbatim
//     (reusing EG-030's raw-row copy) into a portable bundle + manifest, ONLINE (no
//     stop-the-world), with stable admin/cross-shard recovery-boundary fingerprints;
//     preserves at-rest ciphertext + the KG-2.231 audit chain byte-for-byte.
//   * `restore_bundle` — rebuilds a persist-dir from a bundle (verbatim import via the
//     EG-030 migration engine; supports re-shard-on-restore). Backing the DR / PITR story.
#[cfg(feature = "redb")]
pub mod backup;

/// Borrowed carrier for [`PersistenceBackend::commit_mutation_batch_crossmodal`]'s
/// arguments, bundled so the trait method (and every implementation of it) stays
/// under the clippy argument-count ceiling.
pub struct CrossModalCommitArgs<'a> {
    pub graph_fname: &'a str,
    pub batch: &'a MutationBatch,
    pub methods: &'a [Method],
    pub vectors: &'a [(String, Vec<f32>)],
    pub blob_refs: &'a [(String, String)],
    pub measurements: &'a [crate::MeasurementBatch],
    pub result_msgpack: Option<&'a [u8]>,
    pub committed_at_ms: u64,
}
