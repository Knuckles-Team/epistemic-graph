//! Offline K-shard MIGRATION tool (CONCEPT:EG-KG.sharding.atomic-shard-swap, M3 catalog-driven resharding).
//!
//! ## What it solves
//!
//! EG-026 fixes the durable shard count K per persist-dir once created: a graph routes
//! to `graph-<FNV-1a(name) % K>.redb` and `reconcile_shard_layout` HONORS the on-disk
//! layout at open. It also refuses the retired unindexed `graph.redb` K=1 layout.
//! **This is the bounded one-time offline reader** that converts that retired file to
//! canonical `graph-0.redb`, and it is also the tool for changing K.
//!
//! Run OFFLINE (engine stopped — redb holds an exclusive per-file lock), it reads an
//! existing shard set and rewrites every durable row into `graph-<n>.redb` for the NEW
//! K, routing each graph with the **same** EG-026 `shard_index`, so every graph lands
//! in exactly the shard the running engine will look for it in.
//!
//! ## Correctness — verbatim row copy
//!
//! The tool copies stored bytes **verbatim** (it does NOT decode/unseal/re-derive):
//!
//! * Per-graph projection rows plus MutationBatch replay/outbox/fence rows and
//!   governed ChangeEnvelope content, typed cursor/version, policy, evidence,
//!   feature, blob, and lineage rows are moved row for row, value blob unchanged —
//!   so encryption-at-rest blobs survive WITHOUT the key.
//! * The tamper-evident hash-chained `AUDIT` log (CONCEPT:EG-KG.sharding.row-level-security) is copied
//!   verbatim `(graph, seq) → prev_hash|entry_hash|line`, so the chain stays valid:
//!   re-deriving it would break verification, copying preserves it.
//! * Global, non-per-graph records — the Raft log/meta (`RAFT_LOG`/`RAFT_META`), the
//!   cross-shard 2PC records (`XSHARD_PREPARE`/`XSHARD_DECISION`) and materialized views
//!   (`MATVIEWS`) — live in shard 0 (EG-026's `shard0()` home), so they are routed to
//!   the NEW shard 0 regardless of graph.
//!
//! Because routing keys on the SAME sanitized graph name the engine uses, a migrated
//! dir reopens at the new K with every graph reachable + its audit chain verifiable.
//! See the round-trip test `roundtrip_k1_to_k4_preserves_all_graphs`.
//!
//! This module intentionally remains the whole-store, uniform-K OFFLINE tool.
//! Running per-tenant moves use [`super::online_reshard`]; cross-node distribution
//! uses the Raft placement/reshard path. All three preserve the same auxiliary
//! authority rather than copying only the serving projection.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use redb::{Database, Durability, ReadableDatabase, ReadableTable};

use super::redb_backend::{shard_index, RAFT_META};
use crate::redb_layout::{
    discover_indexed_shards, retired_single_shard, shard_filename, validate_shard_count,
};
#[cfg(feature = "compute-dist")]
use crate::redb_store::MATVIEWS;
use crate::redb_store::{
    AUDIT, CHANGE_BLOBS, CHANGE_CURSORS, CHANGE_ENVELOPES, CHANGE_EVIDENCE, CHANGE_FEATURES,
    CHANGE_LINEAGE, CHANGE_POLICIES, CONTENT_VERSIONS, EDGES, GRAPH_META, LEDGER, MUTATION_BATCHES,
    MUTATION_FENCE, MUTATION_GRAPH_VERSION, MUTATION_IDEMPOTENCY, MUTATION_LIFECYCLE_HEAD,
    MUTATION_OUTBOX, MUTATION_OUTBOX_DELIVERY, MUTATION_PROJECTION_CURSOR, NODES, RAFT_LOG,
    SEMANTIC, XSHARD_DECISION, XSHARD_PREPARE,
};

/// Outcome of a migration run (CONCEPT:EG-KG.sharding.atomic-shard-swap) — totals copied + the layout change.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Source shard file count (the OLD K).
    pub source_shards: usize,
    /// Destination shard file count (the NEW K).
    pub dest_shards: usize,
    /// Distinct graphs migrated (rows in `GRAPH_META`).
    pub graphs: usize,
    /// Node rows copied.
    pub nodes: u64,
    /// Edge rows copied.
    pub edges: u64,
    /// Ledger rows copied.
    pub ledger: u64,
    /// Semantic-store rows copied.
    pub semantic: u64,
    /// Audit-chain rows copied (verbatim — chain preserved).
    pub audit: u64,
    /// Mutation replay/outbox and governed ChangeEnvelope rows copied.
    pub auxiliary: u64,
    /// Global rows copied to the new shard 0 (raft log/meta + 2PC + matviews).
    pub global: u64,
}

/// Discover a migration source under `dir` (CONCEPT:EG-KG.sharding.atomic-shard-swap).
/// This OFFLINE-only reader accepts either one retired `graph.redb`, or a contiguous
/// canonical `graph-<n>.redb` set. Mixed, malformed, and sparse layouts fail closed.
pub fn discover_source_shards(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let indexed = discover_indexed_shards(dir)?;
    let retired = retired_single_shard(dir)?;
    if retired.is_some() && !indexed.is_empty() {
        return Err(
            "mixed retired and current redb shard layouts; isolate one source layout before migrating"
                .to_string(),
        );
    }
    if !indexed.is_empty() {
        return Ok(indexed);
    }
    if let Some(retired) = retired {
        return Ok(vec![retired]);
    }
    Err(format!("no redb shard files found under {}", dir.display()))
}

/// Migrate the durable store under `src_dir` into a NEW shard count `new_k`, writing
/// canonical `graph-<n>.redb` files into `dst_dir`
/// (CONCEPT:EG-KG.sharding.atomic-shard-swap).
///
/// OFFLINE only — the engine must be stopped (exclusive redb file lock). `dst_dir` must
/// not already contain a target shard file (the tool refuses to clobber). Use a fresh
/// dir, or [`migrate_in_place`] for an atomic in-dir swap.
pub fn migrate_shards(
    src_dir: &Path,
    dst_dir: &Path,
    new_k: usize,
) -> Result<MigrationReport, String> {
    let new_k = validate_shard_count(new_k)?;
    let src_paths = discover_source_shards(src_dir)?;
    validate_shard_count(src_paths.len())?;
    std::fs::create_dir_all(dst_dir).map_err(|e| e.to_string())?;

    // Refuse every existing current or retired destination shard. This prevents an
    // out-of-place K decrease from leaving stale high-numbered files behind.
    let entries = std::fs::read_dir(dst_dir)
        .map_err(|error| format!("read migration destination directory failed: {error}"))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| format!("read migration destination entry failed: {error}"))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "graph.redb" || (name.starts_with("graph-") && name.ends_with(".redb")) {
            return Err(format!(
                "destination shard file already exists: {} (refusing to overwrite)",
                entry.path().display()
            ));
        }
    }

    // Open every source DB once (reused across the K destination passes). Offline ⇒
    // the exclusive per-file lock is free.
    let mut src_dbs = Vec::with_capacity(src_paths.len());
    for p in &src_paths {
        src_dbs.push(Database::open(p).map_err(|e| format!("open source {}: {e}", p.display()))?);
    }

    let mut report = MigrationReport {
        source_shards: src_paths.len(),
        dest_shards: new_k,
        ..Default::default()
    };
    let mut seen_graphs: HashSet<String> = HashSet::new();

    // One destination shard at a time: scan all sources, copy only the rows routing to
    // THIS dest. Keeps exactly one write txn open ⇒ simple lifetimes; K passes over the
    // sources are fine for a one-time OFFLINE migration.
    for dest_idx in 0..new_k {
        let dst_path = dst_dir.join(shard_filename(dest_idx));
        let dst_db = Database::create(&dst_path)
            .map_err(|e| format!("create dest {}: {e}", dst_path.display()))?;
        let mut wtx = dst_db.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(Durability::Immediate)
            .map_err(|e| e.to_string())?;
        {
            // Open (create) every per-graph table on the destination so the migrated
            // file matches what `Shard::open` expects (it also backfills any missing
            // table at open, but creating them here keeps the file self-consistent).
            let mut d_nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
            let mut d_edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
            let mut d_ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
            let mut d_semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            let mut d_meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
            let mut d_audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
            let mut d_mutation_batches = wtx
                .open_table(MUTATION_BATCHES)
                .map_err(|e| e.to_string())?;
            let mut d_mutation_idempotency = wtx
                .open_table(MUTATION_IDEMPOTENCY)
                .map_err(|e| e.to_string())?;
            let mut d_mutation_outbox =
                wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
            let mut d_mutation_delivery = wtx
                .open_table(MUTATION_OUTBOX_DELIVERY)
                .map_err(|e| e.to_string())?;
            let mut d_mutation_projection = wtx
                .open_table(MUTATION_PROJECTION_CURSOR)
                .map_err(|e| e.to_string())?;
            let mut d_mutation_version = wtx
                .open_table(MUTATION_GRAPH_VERSION)
                .map_err(|e| e.to_string())?;
            let mut d_mutation_fence = wtx.open_table(MUTATION_FENCE).map_err(|e| e.to_string())?;
            let mut d_mutation_lifecycle = wtx
                .open_table(MUTATION_LIFECYCLE_HEAD)
                .map_err(|e| e.to_string())?;
            let mut d_change_envelopes = wtx
                .open_table(CHANGE_ENVELOPES)
                .map_err(|e| e.to_string())?;
            let mut d_content_versions = wtx
                .open_table(CONTENT_VERSIONS)
                .map_err(|e| e.to_string())?;
            let mut d_change_cursors = wtx.open_table(CHANGE_CURSORS).map_err(|e| e.to_string())?;
            let mut d_change_blobs = wtx.open_table(CHANGE_BLOBS).map_err(|e| e.to_string())?;
            let mut d_change_features =
                wtx.open_table(CHANGE_FEATURES).map_err(|e| e.to_string())?;
            let mut d_change_evidence =
                wtx.open_table(CHANGE_EVIDENCE).map_err(|e| e.to_string())?;
            let mut d_change_policies =
                wtx.open_table(CHANGE_POLICIES).map_err(|e| e.to_string())?;
            let mut d_change_lineage = wtx.open_table(CHANGE_LINEAGE).map_err(|e| e.to_string())?;

            for src in &src_dbs {
                let rtx = src.begin_read().map_err(|e| e.to_string())?;

                // graph_meta — routes by graph, also enumerates graphs.
                if let Ok(t) = rtx.open_table(GRAPH_META) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let g = k.value();
                        if shard_index(g, new_k) == dest_idx {
                            d_meta.insert(g, v.value()).map_err(|e| e.to_string())?;
                            if seen_graphs.insert(g.to_string()) {
                                report.graphs += 1;
                            }
                        }
                    }
                }
                // nodes — (graph, id) -> blob
                if let Ok(t) = rtx.open_table(NODES) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (g, id) = k.value();
                        if shard_index(g, new_k) == dest_idx {
                            d_nodes
                                .insert((g, id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.nodes += 1;
                        }
                    }
                }
                // edges — (graph, src, tgt, ord) -> blob
                if let Ok(t) = rtx.open_table(EDGES) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (g, s, t2, o) = k.value();
                        if shard_index(g, new_k) == dest_idx {
                            d_edges
                                .insert((g, s, t2, o), v.value())
                                .map_err(|e| e.to_string())?;
                            report.edges += 1;
                        }
                    }
                }
                // ledger — (graph, seq) -> line
                if let Ok(t) = rtx.open_table(LEDGER) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (g, seq) = k.value();
                        if shard_index(g, new_k) == dest_idx {
                            d_ledger
                                .insert((g, seq), v.value())
                                .map_err(|e| e.to_string())?;
                            report.ledger += 1;
                        }
                    }
                }
                // semantic — graph -> blob
                if let Ok(t) = rtx.open_table(SEMANTIC) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let g = k.value();
                        if shard_index(g, new_k) == dest_idx {
                            d_semantic.insert(g, v.value()).map_err(|e| e.to_string())?;
                            report.semantic += 1;
                        }
                    }
                }
                // audit — (graph, seq) -> chained blob (copy VERBATIM to keep the chain).
                if let Ok(t) = rtx.open_table(AUDIT) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (g, seq) = k.value();
                        if shard_index(g, new_k) == dest_idx {
                            d_audit
                                .insert((g, seq), v.value())
                                .map_err(|e| e.to_string())?;
                            report.audit += 1;
                        }
                    }
                }

                // MutationBatch state is split between graph-addressed indexes and
                // batch-addressed payload/outbox tables. Route the graph indexes
                // first, collect their batch ids, then copy the dependent rows
                // verbatim so replay and delivery state survive a K change.
                let mut routed_batch_ids = HashSet::new();
                if let Ok(t) = rtx.open_table(MUTATION_IDEMPOTENCY) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (tenant, graph, idempotency_key) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            let batch_id = v.value();
                            d_mutation_idempotency
                                .insert((tenant, graph, idempotency_key), batch_id)
                                .map_err(|e| e.to_string())?;
                            routed_batch_ids.insert(batch_id.to_string());
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(MUTATION_BATCHES) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        if routed_batch_ids.contains(k.value()) {
                            d_mutation_batches
                                .insert(k.value(), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(MUTATION_OUTBOX) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (batch_id, ordinal) = k.value();
                        if routed_batch_ids.contains(batch_id) {
                            d_mutation_outbox
                                .insert((batch_id, ordinal), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(MUTATION_OUTBOX_DELIVERY) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (batch_id, ordinal, sink_id) = k.value();
                        if routed_batch_ids.contains(batch_id) {
                            d_mutation_delivery
                                .insert((batch_id, ordinal, sink_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(MUTATION_PROJECTION_CURSOR) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (tenant, graph, projection) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_mutation_projection
                                .insert((tenant, graph, projection), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(MUTATION_GRAPH_VERSION) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let graph = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_mutation_version
                                .insert(graph, v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(MUTATION_FENCE) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let graph = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_mutation_fence
                                .insert(graph, v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(MUTATION_LIFECYCLE_HEAD) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let graph = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_mutation_lifecycle
                                .insert(graph, v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }

                // Governed ChangeEnvelope tables are deliberately graph-first,
                // making their complete materialization independently routable.
                if let Ok(t) = rtx.open_table(CHANGE_ENVELOPES) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, envelope_id) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_change_envelopes
                                .insert((graph, envelope_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(CONTENT_VERSIONS) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, source_id, content_id) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_content_versions
                                .insert((graph, source_id, content_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(CHANGE_CURSORS) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, source_id, stream, partition) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_change_cursors
                                .insert((graph, source_id, stream, partition), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(CHANGE_BLOBS) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, envelope_id, object_id) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_change_blobs
                                .insert((graph, envelope_id, object_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(CHANGE_FEATURES) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, envelope_id, object_id) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_change_features
                                .insert((graph, envelope_id, object_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(CHANGE_EVIDENCE) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, envelope_id, object_id) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_change_evidence
                                .insert((graph, envelope_id, object_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(CHANGE_POLICIES) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, envelope_id, object_id) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_change_policies
                                .insert((graph, envelope_id, object_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
                if let Ok(t) = rtx.open_table(CHANGE_LINEAGE) {
                    for row in t.iter().map_err(|e| e.to_string())? {
                        let (k, v) = row.map_err(|e| e.to_string())?;
                        let (graph, envelope_id, object_id) = k.value();
                        if shard_index(graph, new_k) == dest_idx {
                            d_change_lineage
                                .insert((graph, envelope_id, object_id), v.value())
                                .map_err(|e| e.to_string())?;
                            report.auxiliary += 1;
                        }
                    }
                }
            }

            // Global (non-per-graph) records live in shard 0 only.
            if dest_idx == 0 {
                report.global += copy_global_tables(&src_dbs, &wtx)?;
            }
        }
        wtx.commit().map_err(|e| e.to_string())?;
    }

    Ok(report)
}

/// Copy the GLOBAL (non-per-graph) durable tables from every source into the new
/// shard-0 write txn (CONCEPT:EG-KG.sharding.atomic-shard-swap): the Raft log/meta, the cross-shard 2PC records,
/// and the materialized views. These are EG-026 "shard 0 home" records.
fn copy_global_tables(src_dbs: &[Database], wtx: &redb::WriteTransaction) -> Result<u64, String> {
    let mut count = 0u64;
    let mut d_raft_log = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut d_raft_meta = wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
    let mut d_xprep = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
    let mut d_xdec = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
    #[cfg(feature = "compute-dist")]
    let mut d_matviews = wtx.open_table(MATVIEWS).map_err(|e| e.to_string())?;

    for src in src_dbs {
        let rtx = src.begin_read().map_err(|e| e.to_string())?;
        if let Ok(t) = rtx.open_table(RAFT_LOG) {
            for row in t.iter().map_err(|e| e.to_string())? {
                let (k, v) = row.map_err(|e| e.to_string())?;
                let (g, i) = k.value();
                d_raft_log
                    .insert((g, i), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
        if let Ok(t) = rtx.open_table(RAFT_META) {
            for row in t.iter().map_err(|e| e.to_string())? {
                let (k, v) = row.map_err(|e| e.to_string())?;
                let (g, s) = k.value();
                d_raft_meta
                    .insert((g, s), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
        if let Ok(t) = rtx.open_table(XSHARD_PREPARE) {
            for row in t.iter().map_err(|e| e.to_string())? {
                let (k, v) = row.map_err(|e| e.to_string())?;
                let (txn, gid) = k.value();
                d_xprep
                    .insert((txn, gid), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
        if let Ok(t) = rtx.open_table(XSHARD_DECISION) {
            for row in t.iter().map_err(|e| e.to_string())? {
                let (k, v) = row.map_err(|e| e.to_string())?;
                d_xdec
                    .insert(k.value(), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
        #[cfg(feature = "compute-dist")]
        if let Ok(t) = rtx.open_table(MATVIEWS) {
            for row in t.iter().map_err(|e| e.to_string())? {
                let (k, v) = row.map_err(|e| e.to_string())?;
                d_matviews
                    .insert(k.value(), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Migrate the store under `persist_dir` to `new_k` IN PLACE (CONCEPT:EG-KG.sharding.atomic-shard-swap): the new
/// shards are written to a temp subdir, the OLD shard files are moved aside to a
/// timestamped `.shard-migrate-backup-<ts>` dir, and the new files are moved into
/// place. The backup is left for the operator to delete once the engine reopens cleanly
/// (so an interrupted run never strands data — the originals are recoverable).
pub fn migrate_in_place(persist_dir: &str, new_k: usize) -> Result<MigrationReport, String> {
    let new_k = validate_shard_count(new_k)?;
    let base = Path::new(persist_dir);
    let src_paths = discover_source_shards(base)?;

    let tmp = base.join(".shard-migrate-tmp");
    let _ = std::fs::remove_dir_all(&tmp);
    let report = migrate_shards(base, &tmp, new_k)?;

    // Move the OLD shard files aside (recoverable backup).
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let backup = base.join(format!(".shard-migrate-backup-{ts}"));
    std::fs::create_dir_all(&backup).map_err(|e| e.to_string())?;
    for p in &src_paths {
        if let Some(name) = p.file_name() {
            std::fs::rename(p, backup.join(name)).map_err(|e| e.to_string())?;
        }
    }

    // Move the NEW shard files from tmp into the persist dir, then drop tmp.
    for i in 0..new_k {
        let name = shard_filename(i);
        std::fs::rename(tmp.join(&name), base.join(&name)).map_err(|e| e.to_string())?;
    }
    let _ = std::fs::remove_dir_all(&tmp);

    tracing::info!(
        "shard migration complete: {} -> {} shards, {} graphs; old files backed up to {}",
        report.source_shards,
        report.dest_shards,
        report.graphs,
        backup.display()
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::DurabilityPolicy;
    use crate::protocol::{GraphType, Method};
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Write G graphs (each with nodes + an edge) through a K=1 backend, durably.
    async fn seed_k1(dir: &str, graphs: &[&str]) {
        let backend = RedbBackend::open(dir.to_string(), DurabilityPolicy::Each, 256)
            .expect("open K=1 backend");
        for g in graphs {
            backend
                .register_graph(g, g, GraphType::Global)
                .await
                .expect("register");
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: "a".into(),
                        properties_msgpack: props(serde_json::json!({"type": "Task", "g": g})),
                    },
                )
                .await
                .expect("node a");
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: "b".into(),
                        properties_msgpack: props(serde_json::json!({"type": "Task"})),
                    },
                )
                .await
                .expect("node b");
            backend
                .record_durable(
                    g,
                    &Method::AddEdge {
                        source_id: "a".into(),
                        target_id: "b".into(),
                        properties_msgpack: props(serde_json::json!({"w": 1})),
                    },
                )
                .await
                .expect("edge");
        }
        backend.shutdown();
    }

    /// CONCEPT:EG-KG.sharding.atomic-shard-swap — migrate a K=1 store with G graphs to K=4, reopen at K=4, and
    /// confirm every graph + its nodes/edges survive AND route to the shard the engine
    /// looks for them in. The round-trip proof.
    #[tokio::test(flavor = "multi_thread")]
    async fn roundtrip_k1_to_k4_preserves_all_graphs() {
        let root = std::env::temp_dir().join(format!("eg-migrate-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("k1");
        let dst = root.join("k4");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let graphs = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"];
        seed_k1(&src_s, &graphs).await;

        // K=1 uses the canonical indexed layout.
        assert!(src.join("graph-0.redb").exists(), "K=1 shard written");

        // ── migrate K=1 -> K=4 ──
        let report = migrate_shards(&src, &dst, 4).expect("migrate");
        assert_eq!(report.source_shards, 1);
        assert_eq!(report.dest_shards, 4);
        assert_eq!(report.graphs, graphs.len());
        assert_eq!(report.nodes, (graphs.len() * 2) as u64);
        assert_eq!(report.edges, graphs.len() as u64);

        // The 4 new shard files exist; the old single file was NOT touched.
        for i in 0..4 {
            assert!(
                dst.join(format!("graph-{i}.redb")).exists(),
                "graph-{i}.redb"
            );
        }

        // ── reopen at K=4 and verify each graph routes + reads back intact ──
        let dst_s = dst.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dst_s.clone(), DurabilityPolicy::Each, 256).expect("reopen K=4");
        assert_eq!(backend.shard_count(), 4, "on-disk layout honored as K=4");

        for g in &graphs {
            let dump = backend
                .read_graph_dump_blocking(g)
                .expect("read")
                .unwrap_or_else(|| panic!("graph {g} missing after migration"));
            assert_eq!(dump.name, *g);
            assert_eq!(dump.nodes.len(), 2, "graph {g} nodes");
            assert_eq!(dump.edges.len(), 1, "graph {g} edges");
            // The node 'a' carries the graph tag — proves no cross-graph mixing.
            let a = dump
                .nodes
                .iter()
                .find(|(id, _)| id == "a")
                .map(|(_, blob)| blob.clone())
                .expect("node a present");
            let val: serde_json::Value = rmp_serde::from_slice(&a).unwrap();
            assert_eq!(val.get("g").and_then(|x| x.as_str()), Some(*g));
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CONCEPT:EG-KG.sharding.atomic-shard-swap — the in-place migration swaps shard files atomically and leaves a
    /// recoverable backup; reopening picks up the new K.
    #[tokio::test(flavor = "multi_thread")]
    async fn in_place_migration_swaps_and_backs_up() {
        let dir = std::env::temp_dir().join(format!("eg-migrate-inplace-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();

        let graphs = ["one", "two", "three", "four", "five"];
        seed_k1(&dir_s, &graphs).await;

        let report = migrate_in_place(&dir_s, 4).expect("in-place migrate");
        assert_eq!(report.dest_shards, 4);
        assert_eq!(report.graphs, graphs.len());

        // New shard files are in place and the old shard set is in the backup dir.
        for i in 0..4 {
            assert!(dir.join(format!("graph-{i}.redb")).exists());
        }
        assert!(
            !dir.join("graph.redb").exists(),
            "retired layout was not created"
        );
        let has_backup = std::fs::read_dir(&dir).unwrap().flatten().any(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".shard-migrate-backup-")
        });
        assert!(has_backup, "recoverable backup dir present");

        // Reopen in place at K=4 and confirm all graphs are reachable.
        let backend =
            RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256).expect("reopen");
        assert_eq!(backend.shard_count(), 4);
        for g in &graphs {
            assert!(
                backend.read_graph_dump_blocking(g).unwrap().is_some(),
                "graph {g}"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The sole reader for the retired unindexed K=1 layout is this explicit offline
    /// migration path. Even a K=1 target is rewritten to canonical `graph-0.redb`.
    #[test]
    fn retired_k1_layout_migrates_to_canonical_k1() {
        let dir = std::env::temp_dir().join(format!(
            "eg-migrate-retired-k1-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::create(dir.join("graph.redb")).unwrap();
        let wtx = db.begin_write().unwrap();
        wtx.open_table(GRAPH_META).unwrap();
        wtx.commit().unwrap();
        drop(db);

        let report = migrate_in_place(&dir.to_string_lossy(), 1).unwrap();
        assert_eq!(report.source_shards, 1);
        assert_eq!(report.dest_shards, 1);
        assert!(!dir.join("graph.redb").exists());
        assert!(dir.join("graph-0.redb").exists());

        let backend = RedbBackend::open_with_shards(
            dir.to_string_lossy().to_string(),
            DurabilityPolicy::Each,
            64,
            1,
        )
        .expect("canonical migrated layout reopens");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn migration_discovery_rejects_mixed_and_sparse_layouts() {
        let root = std::env::temp_dir().join(format!(
            "eg-migrate-invalid-layout-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mixed = root.join("mixed");
        let sparse = root.join("sparse");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&mixed).unwrap();
        std::fs::create_dir_all(&sparse).unwrap();
        drop(Database::create(mixed.join("graph.redb")).unwrap());
        drop(Database::create(mixed.join("graph-0.redb")).unwrap());
        drop(Database::create(sparse.join("graph-0.redb")).unwrap());
        drop(Database::create(sparse.join("graph-2.redb")).unwrap());

        let mixed_err = discover_source_shards(&mixed).unwrap_err();
        assert!(
            mixed_err.contains("mixed retired and current"),
            "{mixed_err}"
        );
        let sparse_err = discover_source_shards(&sparse).unwrap_err();
        assert!(sparse_err.contains("non-contiguous"), "{sparse_err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn migration_routes_mutation_and_change_authority_with_its_graph() {
        let root = std::env::temp_dir().join(format!(
            "eg-migrate-aux-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        let db = Database::create(src.join("graph-0.redb")).unwrap();
        let wtx = db.begin_write().unwrap();
        {
            wtx.open_table(GRAPH_META)
                .unwrap()
                .insert("aux-graph", &b"meta"[..])
                .unwrap();
            wtx.open_table(MUTATION_IDEMPOTENCY)
                .unwrap()
                .insert(("tenant-a", "aux-graph", "idem-1"), "batch-1")
                .unwrap();
            wtx.open_table(MUTATION_BATCHES)
                .unwrap()
                .insert("batch-1", &b"batch"[..])
                .unwrap();
            wtx.open_table(MUTATION_OUTBOX)
                .unwrap()
                .insert(("batch-1", 0), &b"outbox"[..])
                .unwrap();
            wtx.open_table(CHANGE_ENVELOPES)
                .unwrap()
                .insert(("aux-graph", "envelope-1"), &b"envelope"[..])
                .unwrap();
            wtx.open_table(CONTENT_VERSIONS)
                .unwrap()
                .insert(("aux-graph", "tenant-a", "object-1"), &b"version"[..])
                .unwrap();
            wtx.open_table(CHANGE_CURSORS)
                .unwrap()
                .insert(
                    ("aux-graph", "tenant-a", "source-a", "partition-a"),
                    &b"cursor"[..],
                )
                .unwrap();
        }
        wtx.commit().unwrap();
        drop(db);

        let report = migrate_shards(&src, &dst, 4).unwrap();
        assert_eq!(report.graphs, 1);
        assert_eq!(report.auxiliary, 6);
        let target =
            Database::open(dst.join(format!("graph-{}.redb", shard_index("aux-graph", 4))))
                .unwrap();
        let rtx = target.begin_read().unwrap();
        assert!(rtx
            .open_table(MUTATION_BATCHES)
            .unwrap()
            .get("batch-1")
            .unwrap()
            .is_some());
        assert!(rtx
            .open_table(MUTATION_OUTBOX)
            .unwrap()
            .get(("batch-1", 0))
            .unwrap()
            .is_some());
        assert!(rtx
            .open_table(CHANGE_ENVELOPES)
            .unwrap()
            .get(("aux-graph", "envelope-1"))
            .unwrap()
            .is_some());
        assert!(rtx
            .open_table(CONTENT_VERSIONS)
            .unwrap()
            .get(("aux-graph", "tenant-a", "object-1"))
            .unwrap()
            .is_some());
        assert!(rtx
            .open_table(CHANGE_CURSORS)
            .unwrap()
            .get(("aux-graph", "tenant-a", "source-a", "partition-a"))
            .unwrap()
            .is_some());
        drop(rtx);
        drop(target);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Refuses to clobber an existing destination shard file.
    #[test]
    fn refuses_existing_destination() {
        let dir = std::env::temp_dir().join(format!("eg-migrate-clobber-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let src = dir.join("src");
        let dst = dir.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&dst).unwrap();
        // a canonical source shard
        Database::create(src.join("graph-0.redb")).unwrap();
        // a pre-existing destination graph-0.redb
        Database::create(dst.join("graph-0.redb")).unwrap();
        let err = migrate_shards(&src, &dst, 4).unwrap_err();
        assert!(err.contains("already exists"), "got: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
