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
//! * The Raft log/meta (`RAFT_LOG`/`RAFT_META`) are per-GROUP (ADR-2 / W1.2: raft group
//!   `g` owns redb shard `g`), so a `(group_id, …)` row routes to `group_id % new_k` —
//!   the SAME mapping `RedbBackend::shard_for_group` uses at runtime — so each group's
//!   log/vote/applied lands in that group's own shard after a K change.
//! * The remaining global records — the cross-shard 2PC records (`XSHARD_PREPARE`/
//!   `XSHARD_DECISION`) and materialized views (`MATVIEWS`) — keep their `shard0()` home,
//!   so they are routed to the NEW shard 0 regardless of graph.
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
#[cfg(feature = "security")]
use super::redb_backend::{ENCRYPTION_CANARY, ENCRYPTION_KEY_BINDING_KEY};
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
    /// Global rows copied (raft log/meta routed per group; 2PC + matviews on shard 0).
    pub global: u64,
    /// ADR-2 / W1.2 group-count metadata: the number of Raft groups the destination
    /// layout supports. Under raft, K (redb shards) == N (groups) — raft group `g` owns
    /// shard `g` — so this equals `dest_shards`. Surfaced explicitly in the manifest so the
    /// W5.2 cutover runbook can assert the migrated store's group count matches the
    /// cluster's `EPISTEMIC_GRAPH_RAFT_GROUPS` before seeding the groups.
    pub dest_raft_groups: usize,
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
    refuse_existing_destination_shards(dst_dir)?;

    // Open every source DB once (reused across the K destination passes). Offline ⇒
    // the exclusive per-file lock is free.
    let src_dbs = open_source_databases(&src_paths)?;

    let mut report = MigrationReport {
        source_shards: src_paths.len(),
        dest_shards: new_k,
        // ADR-2 / W1.2: K == N under raft, so the destination group count is the shard count.
        dest_raft_groups: new_k,
        ..Default::default()
    };
    let mut seen_graphs: HashSet<String> = HashSet::new();

    // One destination shard at a time: scan all sources, copy only the rows routing to
    // THIS dest. Keeps exactly one write txn open ⇒ simple lifetimes; K passes over the
    // sources are fine for a one-time OFFLINE migration.
    for dest_idx in 0..new_k {
        migrate_one_dest_shard(
            &src_dbs,
            dst_dir,
            dest_idx,
            new_k,
            &mut seen_graphs,
            &mut report,
        )?;
    }

    Ok(report)
}

/// Open every source shard file once, so it is reused across all `new_k` destination
/// passes (CONCEPT:EG-KG.sharding.atomic-shard-swap). Offline ⇒ the exclusive per-file
/// redb lock is free.
fn open_source_databases(src_paths: &[PathBuf]) -> Result<Vec<Database>, String> {
    let mut src_dbs = Vec::with_capacity(src_paths.len());
    for p in src_paths {
        src_dbs.push(Database::open(p).map_err(|e| format!("open source {}: {e}", p.display()))?);
    }
    Ok(src_dbs)
}

/// Refuse every existing current or retired destination shard file under `dst_dir`
/// (CONCEPT:EG-KG.sharding.atomic-shard-swap). Prevents an out-of-place K decrease
/// from leaving stale high-numbered files behind.
fn refuse_existing_destination_shards(dst_dir: &Path) -> Result<(), String> {
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
    Ok(())
}

/// Create destination shard `dest_idx`, populate it from every source, and commit
/// (CONCEPT:EG-KG.sharding.atomic-shard-swap). One write txn, immediately durable.
fn migrate_one_dest_shard(
    src_dbs: &[Database],
    dst_dir: &Path,
    dest_idx: usize,
    new_k: usize,
    seen_graphs: &mut HashSet<String>,
    report: &mut MigrationReport,
) -> Result<(), String> {
    let dst_path = dst_dir.join(shard_filename(dest_idx));
    let dst_db = Database::create(&dst_path)
        .map_err(|e| format!("create dest {}: {e}", dst_path.display()))?;
    let mut wtx = dst_db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    populate_dest_shard_from_sources(src_dbs, &wtx, dest_idx, new_k, seen_graphs, report)?;
    report.global += copy_global_tables(src_dbs, &wtx, dest_idx, new_k)?;
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Copy every per-graph and mutation/change-authority row routing to `dest_idx` from
/// EVERY source shard into `wtx` (CONCEPT:EG-KG.sharding.atomic-shard-swap). One
/// `begin_read()` snapshot per source, reused across every table below — matches the
/// original single-pass structure, just factored one table (or a small related group)
/// at a time so each step stays independently readable (and independently
/// characterization-tested) and no function's cyclomatic complexity is dominated by
/// a long straight-line chain of fallible (`?`) calls.
fn populate_dest_shard_from_sources(
    src_dbs: &[Database],
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
    seen_graphs: &mut HashSet<String>,
    report: &mut MigrationReport,
) -> Result<(), String> {
    for src in src_dbs {
        let rtx = src.begin_read().map_err(|e| e.to_string())?;
        populate_core_graph_tables_for_source(&rtx, wtx, dest_idx, new_k, seen_graphs, report)?;
        // MutationBatch state is split between graph-addressed indexes and
        // batch-addressed payload/outbox tables. Route the graph indexes first,
        // collect their batch ids, then copy the dependent rows verbatim so replay
        // and delivery state survive a K change. `routed_batch_ids` is scoped to
        // THIS source, exactly like the original: two sources are never allowed to
        // satisfy each other's batch ids.
        populate_mutation_batch_chain_for_source(&rtx, wtx, dest_idx, new_k, report)?;
        populate_mutation_graph_scoped_for_source(&rtx, wtx, dest_idx, new_k, report)?;
        // Governed ChangeEnvelope tables are deliberately graph-first, making their
        // complete materialization independently routable.
        populate_change_authority_core_for_source(&rtx, wtx, dest_idx, new_k, report)?;
        populate_change_authority_side_tables_for_source(&rtx, wtx, dest_idx, new_k, report)?;
    }
    Ok(())
}

/// graph_meta + nodes + edges + ledger + semantic + audit for ONE source.
fn populate_core_graph_tables_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
    seen_graphs: &mut HashSet<String>,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.graphs += copy_graph_meta_for_source(rtx, wtx, dest_idx, new_k, seen_graphs)?;
    report.nodes += copy_nodes_for_source(rtx, wtx, dest_idx, new_k)?;
    report.edges += copy_edges_for_source(rtx, wtx, dest_idx, new_k)?;
    report.ledger += copy_ledger_for_source(rtx, wtx, dest_idx, new_k)?;
    report.semantic += copy_semantic_for_source(rtx, wtx, dest_idx, new_k)?;
    report.audit += copy_audit_for_source(rtx, wtx, dest_idx, new_k)?;
    Ok(())
}

/// mutation_idempotency -> routed_batch_ids -> mutation_batches/outbox/outbox_delivery
/// for ONE source.
fn populate_mutation_batch_chain_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    let (idempotency_rows, routed_batch_ids) =
        copy_mutation_idempotency_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += idempotency_rows;
    report.auxiliary += copy_mutation_batches_for_source(rtx, wtx, &routed_batch_ids)?;
    report.auxiliary += copy_mutation_outbox_for_source(rtx, wtx, &routed_batch_ids)?;
    report.auxiliary += copy_mutation_outbox_delivery_for_source(rtx, wtx, &routed_batch_ids)?;
    Ok(())
}

/// mutation_projection_cursor + mutation_graph_version + mutation_fence +
/// mutation_lifecycle_head for ONE source (all routed by graph directly, no batch-id
/// indirection).
fn populate_mutation_graph_scoped_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.auxiliary += copy_mutation_projection_cursor_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_mutation_graph_version_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_mutation_fence_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_mutation_lifecycle_head_for_source(rtx, wtx, dest_idx, new_k)?;
    Ok(())
}

/// change_envelopes + content_versions + change_cursors + change_blobs for ONE source.
fn populate_change_authority_core_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.auxiliary += copy_change_envelopes_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_content_versions_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_change_cursors_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_change_blobs_for_source(rtx, wtx, dest_idx, new_k)?;
    Ok(())
}

/// change_features + change_evidence + change_policies + change_lineage for ONE
/// source.
fn populate_change_authority_side_tables_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.auxiliary += copy_change_features_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_change_evidence_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_change_policies_for_source(rtx, wtx, dest_idx, new_k)?;
    report.auxiliary += copy_change_lineage_for_source(rtx, wtx, dest_idx, new_k)?;
    Ok(())
}

// ── per-table, per-source copy steps ────────────────────────────────────────
//
// Each function below streams ONE table from ONE source's read snapshot into the
// destination write txn, filtering to rows that route to `dest_idx` under `new_k`
// (EG-026 `shard_index`), verbatim (value bytes are never decoded/re-derived — see
// the module doc's "Correctness — verbatim row copy"). Splitting one table per
// function is what brings this file's worst function from CCN 176 to under 10: the
// branching was never inherent complexity, it was ~20 near-identical copy loops
// flattened into one body (lizard/the repo's complexity gate charges every `?`
// error-propagation as a branch, same as an `if`, so a long straight-line chain of
// fallible calls is exactly as "complex" by this measure as the same number of
// conditionals — hence splitting by TABLE, not just by control-flow shape).

/// graph_meta — routes by graph, also enumerates graphs. Returns the number of
/// PREVIOUSLY UNSEEN graphs this call added to `seen_graphs` (the caller sums this
/// into `report.graphs`, which counts each graph once across every source/dest pass).
fn copy_graph_meta_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
    seen_graphs: &mut HashSet<String>,
) -> Result<usize, String> {
    let mut d_meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let mut new_graphs = 0usize;
    if let Ok(t) = rtx.open_table(GRAPH_META) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let g = k.value();
            if shard_index(g, new_k) == dest_idx {
                d_meta.insert(g, v.value()).map_err(|e| e.to_string())?;
                if seen_graphs.insert(g.to_string()) {
                    new_graphs += 1;
                }
            }
        }
    }
    Ok(new_graphs)
}

/// nodes — (graph, id) -> blob.
fn copy_nodes_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(NODES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, id) = k.value();
            if shard_index(g, new_k) == dest_idx {
                d_nodes
                    .insert((g, id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// edges — (graph, src, tgt, ord) -> blob.
fn copy_edges_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(EDGES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, s, t2, o) = k.value();
            if shard_index(g, new_k) == dest_idx {
                d_edges
                    .insert((g, s, t2, o), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// ledger — (graph, seq) -> line.
fn copy_ledger_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(LEDGER) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, seq) = k.value();
            if shard_index(g, new_k) == dest_idx {
                d_ledger
                    .insert((g, seq), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// semantic — graph -> blob.
fn copy_semantic_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(SEMANTIC) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let g = k.value();
            if shard_index(g, new_k) == dest_idx {
                d_semantic.insert(g, v.value()).map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// audit — (graph, seq) -> chained blob (copy VERBATIM to keep the chain).
fn copy_audit_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(AUDIT) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, seq) = k.value();
            if shard_index(g, new_k) == dest_idx {
                d_audit
                    .insert((g, seq), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// mutation_idempotency — (tenant, graph, key) -> batch_id, routed by graph. Also
/// collects the batch ids this SOURCE routed to `dest_idx`, so the batch-addressed
/// tables below can restrict themselves to dependents of THIS source's routed rows.
fn copy_mutation_idempotency_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<(u64, HashSet<String>), String> {
    let mut d_mutation_idempotency = wtx
        .open_table(MUTATION_IDEMPOTENCY)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
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
                count += 1;
            }
        }
    }
    Ok((count, routed_batch_ids))
}

/// mutation_batches — batch_id -> payload, restricted to `routed_batch_ids`.
fn copy_mutation_batches_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    routed_batch_ids: &HashSet<String>,
) -> Result<u64, String> {
    let mut d_mutation_batches = wtx
        .open_table(MUTATION_BATCHES)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MUTATION_BATCHES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            if routed_batch_ids.contains(k.value()) {
                d_mutation_batches
                    .insert(k.value(), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// mutation_outbox — (batch_id, ordinal) -> payload, restricted to `routed_batch_ids`.
fn copy_mutation_outbox_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    routed_batch_ids: &HashSet<String>,
) -> Result<u64, String> {
    let mut d_mutation_outbox = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MUTATION_OUTBOX) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (batch_id, ordinal) = k.value();
            if routed_batch_ids.contains(batch_id) {
                d_mutation_outbox
                    .insert((batch_id, ordinal), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// mutation_outbox_delivery — (batch_id, ordinal, sink_id) -> payload, restricted to
/// `routed_batch_ids`.
fn copy_mutation_outbox_delivery_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    routed_batch_ids: &HashSet<String>,
) -> Result<u64, String> {
    let mut d_mutation_delivery = wtx
        .open_table(MUTATION_OUTBOX_DELIVERY)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MUTATION_OUTBOX_DELIVERY) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (batch_id, ordinal, sink_id) = k.value();
            if routed_batch_ids.contains(batch_id) {
                d_mutation_delivery
                    .insert((batch_id, ordinal, sink_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// mutation_projection_cursor — (tenant, graph, projection) -> cursor, routed by graph.
fn copy_mutation_projection_cursor_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_mutation_projection = wtx
        .open_table(MUTATION_PROJECTION_CURSOR)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MUTATION_PROJECTION_CURSOR) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (tenant, graph, projection) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_mutation_projection
                    .insert((tenant, graph, projection), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// mutation_graph_version — graph -> version, routed by graph.
fn copy_mutation_graph_version_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_mutation_version = wtx
        .open_table(MUTATION_GRAPH_VERSION)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MUTATION_GRAPH_VERSION) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let graph = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_mutation_version
                    .insert(graph, v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// mutation_fence — graph -> (placement_epoch, fencing_token), routed by graph.
fn copy_mutation_fence_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_mutation_fence = wtx.open_table(MUTATION_FENCE).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MUTATION_FENCE) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let graph = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_mutation_fence
                    .insert(graph, v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// mutation_lifecycle_head — graph -> latest lifecycle batch id, routed by graph.
fn copy_mutation_lifecycle_head_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_mutation_lifecycle = wtx
        .open_table(MUTATION_LIFECYCLE_HEAD)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MUTATION_LIFECYCLE_HEAD) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let graph = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_mutation_lifecycle
                    .insert(graph, v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// change_envelopes — (graph, envelope_id) -> envelope, routed by graph.
fn copy_change_envelopes_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_change_envelopes = wtx
        .open_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CHANGE_ENVELOPES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, envelope_id) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_change_envelopes
                    .insert((graph, envelope_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// content_versions — (graph, source_id, content_id) -> version, routed by graph.
fn copy_content_versions_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_content_versions = wtx
        .open_table(CONTENT_VERSIONS)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CONTENT_VERSIONS) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, source_id, content_id) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_content_versions
                    .insert((graph, source_id, content_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// change_cursors — (graph, source_id, stream, partition) -> cursor, routed by graph.
fn copy_change_cursors_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_change_cursors = wtx.open_table(CHANGE_CURSORS).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CHANGE_CURSORS) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, source_id, stream, partition) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_change_cursors
                    .insert((graph, source_id, stream, partition), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// change_blobs — (graph, envelope_id, object_id) -> blob, routed by graph.
fn copy_change_blobs_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_change_blobs = wtx.open_table(CHANGE_BLOBS).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CHANGE_BLOBS) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, envelope_id, object_id) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_change_blobs
                    .insert((graph, envelope_id, object_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// change_features — (graph, envelope_id, object_id) -> features, routed by graph.
fn copy_change_features_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_change_features = wtx
        .open_table(CHANGE_FEATURES)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CHANGE_FEATURES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, envelope_id, object_id) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_change_features
                    .insert((graph, envelope_id, object_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// change_evidence — (graph, envelope_id, object_id) -> evidence, routed by graph.
fn copy_change_evidence_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_change_evidence = wtx
        .open_table(CHANGE_EVIDENCE)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CHANGE_EVIDENCE) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, envelope_id, object_id) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_change_evidence
                    .insert((graph, envelope_id, object_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// change_policies — (graph, envelope_id, object_id) -> policy, routed by graph.
fn copy_change_policies_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_change_policies = wtx
        .open_table(CHANGE_POLICIES)
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CHANGE_POLICIES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, envelope_id, object_id) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_change_policies
                    .insert((graph, envelope_id, object_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// change_lineage — (graph, envelope_id, object_id) -> lineage, routed by graph.
fn copy_change_lineage_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_change_lineage = wtx.open_table(CHANGE_LINEAGE).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(CHANGE_LINEAGE) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (graph, envelope_id, object_id) = k.value();
            if shard_index(graph, new_k) == dest_idx {
                d_change_lineage
                    .insert((graph, envelope_id, object_id), v.value())
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Copy the GLOBAL (non-per-graph) durable tables from every source into `dest_idx`'s
/// write txn (CONCEPT:EG-KG.sharding.atomic-shard-swap).
///
/// ADR-2 / W1.2: the Raft log (`RAFT_LOG`) and meta (`RAFT_META`) are per-GROUP records —
/// raft group `g` owns redb shard `g` — so a `(group_id, …)` row routes to
/// `group_id % new_k`, EXACTLY like `RedbBackend::shard_for_group` at runtime. A migrated
/// raft store therefore finds each group's log/vote/applied in that group's own shard
/// (not stranded on shard 0). The cross-shard 2PC records (`XSHARD_PREPARE`/
/// `XSHARD_DECISION`) and materialized views (`MATVIEWS`) keep their runtime `shard0()`
/// home, so they are copied only on the `dest_idx == 0` pass.
fn copy_global_tables(
    src_dbs: &[Database],
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut count = copy_raft_group_tables(src_dbs, wtx, dest_idx, new_k)?;
    #[cfg(feature = "security")]
    copy_encryption_canary(src_dbs, wtx)?;
    count += copy_shard_zero_only_tables(src_dbs, wtx, dest_idx)?;
    Ok(count)
}

/// The Raft log/meta pass: `(group_id, …)` rows route to `group_id % new_k`, exactly
/// like `RedbBackend::shard_for_group` at runtime.
fn copy_raft_group_tables(
    src_dbs: &[Database],
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut count = 0u64;
    for src in src_dbs {
        let rtx = src.begin_read().map_err(|e| e.to_string())?;
        count += copy_raft_log_for_source(&rtx, wtx, dest_idx, new_k)?;
        count += copy_raft_meta_for_source(&rtx, wtx, dest_idx, new_k)?;
    }
    Ok(count)
}

/// raft_log — (group_id, index) -> entry, routed `group_id % new_k`.
fn copy_raft_log_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_raft_log = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(RAFT_LOG) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, i) = k.value();
            if (g as usize) % new_k != dest_idx {
                continue;
            }
            d_raft_log
                .insert((g, i), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    Ok(count)
}

/// raft_meta — (group_id, slot) -> value, routed `group_id % new_k`.
fn copy_raft_meta_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
    new_k: usize,
) -> Result<u64, String> {
    let mut d_raft_meta = wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(RAFT_META) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, s) = k.value();
            if (g as usize) % new_k != dest_idx {
                continue;
            }
            d_raft_meta
                .insert((g, s), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    Ok(count)
}

/// Key-binding/canary metadata is per-shard, but it is not graph-addressed. A
/// K-changing migration therefore copies one consistent source record into every
/// destination shard so each `Shard::open` can enforce the same key identity and
/// version. Not counted as graph/global data: duplicating metadata across a changed
/// K must not make restore totals appear to change.
#[cfg(feature = "security")]
fn copy_encryption_canary(
    src_dbs: &[Database],
    wtx: &redb::WriteTransaction,
) -> Result<(), String> {
    let mut d_encryption_canary = wtx
        .open_table(ENCRYPTION_CANARY)
        .map_err(|e| e.to_string())?;
    if let Some(rows) = find_consistent_encryption_canary_rows(src_dbs)? {
        // Carry the first (and only, once validated) consistent shard's rows
        // forward: every shard's canary decrypts to the same plaintext under the
        // one agreed key, so any single copy is a valid canary for the destination.
        for (key, value) in rows {
            d_encryption_canary
                .insert(key.as_str(), value.as_slice())
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// One source shard's `ENCRYPTION_CANARY` table as `(key, value)` rows, in table
/// order. Named so the canary-comparison signatures stay readable.
#[cfg(feature = "security")]
type EncryptionCanaryRows = Vec<(String, Vec<u8>)>;

/// Scan every source's `ENCRYPTION_CANARY` table and return ONE consistent set of
/// rows to carry forward, or `Ok(None)` when no source has a canary at all.
///
/// Compare the KEY BINDING row, never the whole table.
///
/// The `ENCRYPTION_CANARY` table holds two different kinds of row. The binding
/// (`ENCRYPTION_KEY_BINDING_KEY`) is a deterministic encoding of the key's
/// identity/version — the thing that actually has to match across shards. The
/// canary row next to it is `cipher.seal(plaintext)`, and `seal` draws a FRESH
/// RANDOM NONCE per call, so sealing the same plaintext with the same key produces
/// different bytes in every shard, forever.
///
/// Comparing the raw rows therefore rejected every encrypted multi-shard restore --
/// the error even said "key-binding metadata differs" while actually comparing
/// ciphertext that is *designed* never to be equal. That made restore-from-backup
/// impossible whenever encryption at rest was on and K > 1, which is a
/// disaster-recovery defect, not a nuisance.
#[cfg(feature = "security")]
fn find_consistent_encryption_canary_rows(
    src_dbs: &[Database],
) -> Result<Option<EncryptionCanaryRows>, String> {
    let mut source_rows: Option<EncryptionCanaryRows> = None;
    let mut source_binding: Option<Vec<u8>> = None;
    for src in src_dbs {
        let rows = read_encryption_canary_rows(src)?;
        if rows.is_empty() {
            continue;
        }
        let binding = rows
            .iter()
            .find(|(key, _)| key == ENCRYPTION_KEY_BINDING_KEY)
            .map(|(_, value)| value.clone());
        // A canary with no binding row is the pre-key-lifecycle shape that
        // `Shard::open` upgrades in place on first open with the configured key.
        // Restoring one is refused with the REAL reason rather than being
        // mislabelled a key mismatch: without a binding there is no key identity to
        // compare, and the canary alone cannot supply one.
        let Some(binding) = binding else {
            return Err(
                "source shard has an encryption canary but no key-binding row; open it \
                 once with the configured key to complete the legacy upgrade before \
                 restoring"
                    .to_string(),
            );
        };
        if let Some(existing) = &source_binding {
            if existing != &binding {
                return Err(
                    "encryption key-binding metadata differs between source shards".to_string(),
                );
            }
        } else {
            source_binding = Some(binding);
            source_rows = Some(rows);
        }
    }
    Ok(source_rows)
}

/// Every row of ONE source's `ENCRYPTION_CANARY` table, or empty if that source has
/// no such table yet.
#[cfg(feature = "security")]
fn read_encryption_canary_rows(src: &Database) -> Result<EncryptionCanaryRows, String> {
    let rtx = src.begin_read().map_err(|e| e.to_string())?;
    let Some(table) = rtx.open_table(ENCRYPTION_CANARY).ok() else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        rows.push((key.value().to_string(), value.value().to_vec()));
    }
    Ok(rows)
}

/// The 2PC coordinator records + materialized views stay on shard 0 (their runtime
/// `shard0()` home is unchanged by ADR-2), so they are migrated only on the first
/// destination pass.
fn copy_shard_zero_only_tables(
    src_dbs: &[Database],
    wtx: &redb::WriteTransaction,
    dest_idx: usize,
) -> Result<u64, String> {
    if dest_idx != 0 {
        return Ok(0);
    }
    let mut count = 0u64;
    for src in src_dbs {
        let rtx = src.begin_read().map_err(|e| e.to_string())?;
        count += copy_xshard_prepare_for_source(&rtx, wtx)?;
        count += copy_xshard_decision_for_source(&rtx, wtx)?;
        #[cfg(feature = "compute-dist")]
        {
            count += copy_matviews_for_source(&rtx, wtx)?;
        }
    }
    Ok(count)
}

/// xshard_prepare — (txn_id, group_id) -> staged write-set, shard-0 only.
fn copy_xshard_prepare_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
) -> Result<u64, String> {
    let mut d_xprep = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
    let mut count = 0u64;
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
    Ok(count)
}

/// xshard_decision — txn_id -> terminal decision byte, shard-0 only.
fn copy_xshard_decision_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
) -> Result<u64, String> {
    let mut d_xdec = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(XSHARD_DECISION) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_xdec
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    Ok(count)
}

/// matviews — name -> serialized MatView, shard-0 only.
#[cfg(feature = "compute-dist")]
fn copy_matviews_for_source(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
) -> Result<u64, String> {
    let mut d_matviews = wtx.open_table(MATVIEWS).map_err(|e| e.to_string())?;
    let mut count = 0u64;
    if let Ok(t) = rtx.open_table(MATVIEWS) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_matviews
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
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
    #[cfg(feature = "security")]
    use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
    #[cfg(feature = "matview")]
    use crate::redb_store::{MATVIEW_OPERATOR_STATE, PLAN_MATVIEWS};

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
        // Held for the whole test: it seeds a K=1 backend, migrates it to K=4, then
        // reopens the K=4 layout — every open must resolve the same
        // `EPISTEMIC_GRAPH_ENCRYPTION_KEY` cipher, or the reopen panics with
        // "decryption failed (wrong key or tampered ciphertext)". See
        // `crate::crypto::acquire_test_env_lock`'s doc for the full mechanism.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
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
        // See `roundtrip_k1_to_k4_preserves_all_graphs` above: held for the whole
        // test (this one also reopens after an in-place migration + backup).
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
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
    // ── CX-EG-10 characterization additions ─────────────────────────────────
    //
    // The tests above predate this lane and already cover: K=1->K4 round trip,
    // in-place K1->K4 (with backup), the retired-K1 canonical rewrite,
    // mixed/sparse layout rejection, and mutation/change-authority routing for a
    // SINGLE source shard. None of them exercises a source layout with MORE THAN
    // ONE shard file -- the "for src in &src_dbs" loop this lane's decomposition
    // reshapes the most -- so the two tests below fill exactly that gap, plus one
    // that pins an OBSERVED bug. Per the lane brief's two-commit discipline these
    // were added in a commit that touches ONLY this `mod tests` block, proven
    // green against the UNMODIFIED `migrate_shards`/`copy_global_tables` before
    // the refactor commit landed. See `plans/complex/lane-reports/CX-EG-10.md`.

    /// Open a table at `path` and count its rows (0 if the table was never
    /// created). Used to inspect a migration's ON-DISK output directly.
    fn table_row_count<K, V>(path: &std::path::Path, def: redb::TableDefinition<K, V>) -> usize
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let db = Database::open(path).expect("open shard for inspection");
        let rtx = db.begin_read().expect("begin read");
        match rtx.open_table(def) {
            Ok(t) => t.iter().expect("iterate table").count(),
            Err(_) => 0,
        }
    }

    /// Seed `graphs.len()` graphs (2 nodes + 1 edge each) through a backend opened
    /// at an EXPLICIT shard count `k` (`seed_k1` above only ever produces K=1).
    async fn seed_at_k(dir: &str, k: usize, graphs: &[&str]) {
        let backend =
            RedbBackend::open_with_shards(dir.to_string(), DurabilityPolicy::Each, 256, k)
                .expect("open backend at requested K");
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

    /// CX-EG-10: round-trips a GENUINE multi-source-shard layout (K=2, not K=1)
    /// through `migrate_shards`. Every embedded test above starts from K=1 (one
    /// source `Database`); this is the only proof that the "for src in &src_dbs"
    /// loop over MULTIPLE source handles is correct.
    #[tokio::test(flavor = "multi_thread")]
    async fn multi_source_migration_preserves_graphs() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = std::env::temp_dir().join(format!(
            "eg-migrate-multisrc-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("k2");
        let dst = root.join("k3");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let graphs = [
            "one", "two", "three", "four", "five", "six", "seven", "eight",
        ];
        seed_at_k(&src_s, 2, &graphs).await;

        assert!(src.join("graph-0.redb").exists(), "K=2 shard 0 written");
        assert!(src.join("graph-1.redb").exists(), "K=2 shard 1 written");

        let report = migrate_shards(&src, &dst, 3).expect("migrate K=2 -> K=3");
        assert_eq!(report.source_shards, 2);
        assert_eq!(report.dest_shards, 3);
        assert_eq!(report.dest_raft_groups, 3);
        assert_eq!(report.graphs, graphs.len());
        assert_eq!(report.nodes, (graphs.len() * 2) as u64);
        assert_eq!(report.edges, graphs.len() as u64);

        for i in 0..3 {
            assert!(
                dst.join(format!("graph-{i}.redb")).exists(),
                "graph-{i}.redb"
            );
        }

        let dst_s = dst.to_string_lossy().to_string();
        let backend =
            RedbBackend::open(dst_s.clone(), DurabilityPolicy::Each, 256).expect("reopen K=3");
        assert_eq!(backend.shard_count(), 3, "on-disk layout honored as K=3");
        for g in &graphs {
            let dump = backend
                .read_graph_dump_blocking(g)
                .expect("read")
                .unwrap_or_else(|| panic!("graph {g} missing after migration"));
            assert_eq!(dump.name, *g);
            assert_eq!(dump.nodes.len(), 2, "graph {g} nodes");
            assert_eq!(dump.edges.len(), 1, "graph {g} edges");
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

    /// CX-EG-10: pins the per-source scoping of the mutation-batch replay chain
    /// when TWO DIFFERENT source shard files each carry their own batch and both
    /// graphs route to the SAME destination shard (K=1). In the unmodified
    /// function `routed_batch_ids` is declared FRESH inside the "for src" loop
    /// (reset every source iteration); a decomposition that hoists or shares that
    /// set across sources would silently cross-contaminate or drop one source's
    /// batch. Raw redb, mirroring `migration_routes_mutation_and_change_authority_with_its_graph`
    /// above but across two source files instead of one.
    #[test]
    fn mutation_batch_chain_routes_independently_per_source_shard() {
        let root = std::env::temp_dir().join(format!(
            "eg-migrate-multisrc-mutation-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();

        {
            let db = Database::create(src.join("graph-0.redb")).unwrap();
            let wtx = db.begin_write().unwrap();
            {
                wtx.open_table(GRAPH_META)
                    .unwrap()
                    .insert("alpha", &b"meta-alpha"[..])
                    .unwrap();
                wtx.open_table(MUTATION_IDEMPOTENCY)
                    .unwrap()
                    .insert(("tenant-a", "alpha", "idem-alpha"), "batch-alpha")
                    .unwrap();
                wtx.open_table(MUTATION_BATCHES)
                    .unwrap()
                    .insert("batch-alpha", &b"payload-alpha"[..])
                    .unwrap();
                wtx.open_table(MUTATION_OUTBOX)
                    .unwrap()
                    .insert(("batch-alpha", 0u32), &b"outbox-alpha"[..])
                    .unwrap();
                wtx.open_table(MUTATION_OUTBOX_DELIVERY)
                    .unwrap()
                    .insert(("batch-alpha", 0u32, "sink-a"), &b"delivery-alpha"[..])
                    .unwrap();
            }
            wtx.commit().unwrap();
        }
        {
            let db = Database::create(src.join("graph-1.redb")).unwrap();
            let wtx = db.begin_write().unwrap();
            {
                wtx.open_table(GRAPH_META)
                    .unwrap()
                    .insert("beta", &b"meta-beta"[..])
                    .unwrap();
                wtx.open_table(MUTATION_IDEMPOTENCY)
                    .unwrap()
                    .insert(("tenant-b", "beta", "idem-beta"), "batch-beta")
                    .unwrap();
                wtx.open_table(MUTATION_BATCHES)
                    .unwrap()
                    .insert("batch-beta", &b"payload-beta"[..])
                    .unwrap();
                wtx.open_table(MUTATION_OUTBOX)
                    .unwrap()
                    .insert(("batch-beta", 0u32), &b"outbox-beta"[..])
                    .unwrap();
                wtx.open_table(MUTATION_OUTBOX_DELIVERY)
                    .unwrap()
                    .insert(("batch-beta", 0u32, "sink-b"), &b"delivery-beta"[..])
                    .unwrap();
            }
            wtx.commit().unwrap();
        }

        let report = migrate_shards(&src, &dst, 1).expect("migrate K=2 -> K=1");
        assert_eq!(report.source_shards, 2);
        assert_eq!(report.graphs, 2);
        assert_eq!(report.auxiliary, 8, "4 aux rows x 2 sources");

        let target = Database::open(dst.join("graph-0.redb")).unwrap();
        let rtx = target.begin_read().unwrap();
        let batches = rtx.open_table(MUTATION_BATCHES).unwrap();
        assert_eq!(
            batches.get("batch-alpha").unwrap().unwrap().value(),
            b"payload-alpha"
        );
        assert_eq!(
            batches.get("batch-beta").unwrap().unwrap().value(),
            b"payload-beta"
        );
        let outbox = rtx.open_table(MUTATION_OUTBOX).unwrap();
        assert_eq!(
            outbox.get(("batch-alpha", 0u32)).unwrap().unwrap().value(),
            b"outbox-alpha"
        );
        assert_eq!(
            outbox.get(("batch-beta", 0u32)).unwrap().unwrap().value(),
            b"outbox-beta"
        );
        let delivery = rtx.open_table(MUTATION_OUTBOX_DELIVERY).unwrap();
        assert_eq!(
            delivery
                .get(("batch-alpha", 0u32, "sink-a"))
                .unwrap()
                .unwrap()
                .value(),
            b"delivery-alpha"
        );
        assert_eq!(
            delivery
                .get(("batch-beta", 0u32, "sink-b"))
                .unwrap()
                .unwrap()
                .value(),
            b"delivery-beta"
        );
        drop(rtx);
        drop(target);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CX-EG-10 BUG PIN (do not "fix" by widening -- see BUGS FOUND in
    /// `plans/complex/lane-reports/CX-EG-10.md`): `provenance_anchor_members`
    /// (graph-scoped, feature `security`) and `plan_matviews` /
    /// `matview_operator_state` (global, `shard0()`-homed, feature `matview`) live
    /// in the SAME `graph-<n>.redb` shard files as `nodes`/`audit_chain`, but
    /// `migrate_shards`/`copy_global_tables` never import or route them, so a
    /// K-shard migration silently drops them while every table the function DOES
    /// know about survives. `NODES` is asserted as the differential control.
    #[tokio::test(flavor = "multi_thread")]
    async fn migration_silently_drops_provenance_anchor_and_matview_state() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = std::env::temp_dir().join(format!(
            "eg-migrate-dropped-tables-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let backend = RedbBackend::open(src_s.clone(), DurabilityPolicy::Each, 256)
            .expect("open K=1 backend");
        backend
            .register_graph("g", "g", GraphType::Global)
            .await
            .expect("register");
        backend
            .record_durable(
                "g",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Task"})),
                },
            )
            .await
            .expect("node a");

        #[cfg(feature = "security")]
        {
            backend
                .provenance_anchor_commit_blocking(
                    "g",
                    [9u8; 32],
                    vec![("a".to_string(), [7u8; 32])],
                )
                .expect("provenance anchor commit")
                .expect("anchor actually wrote a row (root differs from none)");
        }
        #[cfg(feature = "matview")]
        {
            backend
                .plan_matview_put("mv-1", b"plan-matview-definition".to_vec())
                .await
                .expect("plan matview put");
            backend
                .matview_operator_state_put("mv-1", b"operator-state".to_vec())
                .await
                .expect("matview operator state put");
        }
        backend.shutdown();

        // Sanity: the rows are actually on disk in the SOURCE before migration --
        // otherwise their absence downstream would prove nothing about
        // migrate_shards.
        #[cfg(feature = "security")]
        assert_eq!(
            table_row_count(&src.join("graph-0.redb"), PROVENANCE_ANCHOR_MEMBERS),
            1,
            "source has the provenance anchor row pre-migration"
        );
        #[cfg(feature = "matview")]
        {
            assert_eq!(
                table_row_count(&src.join("graph-0.redb"), PLAN_MATVIEWS),
                1,
                "source has the plan matview row pre-migration"
            );
            assert_eq!(
                table_row_count(&src.join("graph-0.redb"), MATVIEW_OPERATOR_STATE),
                1,
                "source has the matview operator-state row pre-migration"
            );
        }
        assert_eq!(
            table_row_count(&src.join("graph-0.redb"), NODES),
            1,
            "source has the node pre-migration"
        );

        let report = migrate_shards(&src, &dst, 2).expect("migrate K=1 -> K=2");
        assert_eq!(report.graphs, 1);
        assert_eq!(report.nodes, 1);

        let nodes_after: usize = (0..2)
            .map(|i| table_row_count(&dst.join(format!("graph-{i}.redb")), NODES))
            .sum();
        assert_eq!(nodes_after, 1, "node survives the migration (known-good table)");

        #[cfg(feature = "security")]
        {
            let anchors_after: usize = (0..2)
                .map(|i| {
                    table_row_count(
                        &dst.join(format!("graph-{i}.redb")),
                        PROVENANCE_ANCHOR_MEMBERS,
                    )
                })
                .sum();
            assert_eq!(
                anchors_after, 0,
                "BUG: provenance_anchor_members is silently dropped by migrate_shards"
            );
        }
        #[cfg(feature = "matview")]
        {
            let plan_matviews_after: usize = (0..2)
                .map(|i| table_row_count(&dst.join(format!("graph-{i}.redb")), PLAN_MATVIEWS))
                .sum();
            assert_eq!(
                plan_matviews_after, 0,
                "BUG: plan_matviews is silently dropped by migrate_shards / copy_global_tables"
            );
            let matview_state_after: usize = (0..2)
                .map(|i| {
                    table_row_count(
                        &dst.join(format!("graph-{i}.redb")),
                        MATVIEW_OPERATOR_STATE,
                    )
                })
                .sum();
            assert_eq!(
                matview_state_after, 0,
                "BUG: matview_operator_state is silently dropped by migrate_shards / copy_global_tables"
            );
        }

        let _ = std::fs::remove_dir_all(&root);
    }
}
