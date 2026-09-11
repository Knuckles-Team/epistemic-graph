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
//! Run OFFLINE (engine stopped — the kernel holds an exclusive per-file lock), it opens
//! an existing shard set as [`Shard`]s and moves every durable row into
//! `graph-<n>.redb` for the NEW K, routing each graph with the **same** EG-026
//! [`shard_index`], so every graph lands in exactly the shard the running engine will
//! look for it in.
//!
//! ## The move, one graph at a time
//!
//! A shard file is one `OwnerLayout::GraphShard` owner store and each graph it hosts is
//! one bound serving scope on it, so "move a graph" is no longer "filter a table scan by
//! a key prefix". It is two halves, in this order:
//!
//! 1. **Owner rows** — the 41 scope-prefixed tables (`nodes`, `edges`, `ledger`,
//!    `semantic_store`, `audit_chain`, the `resource_*`, `change_*`, `capacity_*`,
//!    `development_lane_*` and work-item tables). Read through the source graph's own
//!    [`ScopedRead`], staged through the destination's exact graft reservation and
//!    finally committed with the Phase-B ledger transplant. Values are never decoded,
//!    so encryption-at-rest blobs survive WITHOUT the key and the tamper-evident
//!    hash-chained `audit_chain` stays verifiable — re-deriving it would break
//!    verification, copying preserves it.
//! 2. **The ledger** — [`Shard::graft_graph_from`]. RF-RULING-004 application note 4: a
//!    domain crate cannot write a ledger row, and re-admitting each moved batch is not
//!    equivalent to moving it (it would re-execute effects, re-emit outbox rows, stamp
//!    new receipts and reset the version). The graft copies every ledger row verbatim
//!    and retires the source scope — its binding AND its owner payload — in the same
//!    operation, which is why the owner rows are copied FIRST. The destination serves
//!    the SOURCE's marker-inclusive version: Phase A contributes the one intentional
//!    maintenance increment, while the existing ledger history is copied verbatim.
//!
//! The eight retired private `mutation_*` tables this tool used to route by hand
//! (batches, idempotency, outbox, outbox delivery, projection cursor, graph version,
//! fence, lifecycle head) are gone with the private ledger they belonged to; the one
//! graft call replaces all eight movers, and it replaces the source clear as well as the
//! destination import.
//!
//! **The move consumes the migration snapshot, not the live source.** Because the graft
//! retires each source scope, a migrated graph no longer exists in the files used to
//! build the destination. [`migrate_in_place`]'s `.shard-migrate-backup-<ts>/source`
//! is an immutable byte-for-byte snapshot made before the first graft; the live old-K
//! files stay in place until the complete destination has been built and validated.
//! An interrupted build leaves both the live source and the partial destination
//! accounted for, and an interrupted swap leaves the snapshot plus the partial files
//! for a named recovery/resume path. A retry never deletes an unaccounted temp tree.
//!
//! ## What stays file-wide
//!
//! The 12 file-wide tables belong to the shard file rather than to any one graph and
//! ride the control member of a control-only admitted write:
//!
//! * The Raft log/meta (`raft_log`/`raft_meta`) are per-GROUP (ADR-2 / W1.2: raft group
//!   `g` owns redb shard `g`), so a `(group_id, …)` row routes to `group_id % new_k` —
//!   the SAME mapping `RedbBackend::shard_for_group` uses at runtime.
//! * The graph catalog (`graph_meta`) routes by graph name, like the graph it names.
//! * The cross-shard 2PC records (`xshard_prepare`/`xshard_decision`) and the matview
//!   tables keep their `shard0()` home, so they are routed to the NEW shard 0 regardless
//!   of graph.
//! * The encryption canary is per-shard metadata: one consistent source record is
//!   carried into EVERY destination shard.
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
use std::io::Read;
use std::path::{Path, PathBuf};

use redb::{ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};

use eg_storage::{GraphShardOwner, OwnerRowScope, OwnerRowScopeStart, ScopedRead};
use eg_transaction::{GraftOwnerWrite, OwnerPayloadTransfer, OwnerPayloadWrite};

use super::redb_backend::{shard_index, RAFT_META};
#[cfg(feature = "security")]
use super::redb_backend::{ENCRYPTION_CANARY, ENCRYPTION_KEY_BINDING_KEY};
use crate::redb_layout::{
    discover_indexed_shards, retired_single_shard, shard_filename, validate_shard_count,
};
use crate::redb_store::capacity_lease;
use crate::redb_store::development_lane;
use crate::redb_store::shard::{Shard, ShardWrite};
use crate::redb_store::work_item_capability;
#[cfg(feature = "compute-dist")]
use crate::redb_store::MATVIEWS;
#[cfg(feature = "security")]
use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
use crate::redb_store::{
    AUDIT, CHANGE_BLOBS, CHANGE_CURSORS, CHANGE_ENVELOPES, CHANGE_EVIDENCE, CHANGE_FEATURES,
    CHANGE_LINEAGE, CHANGE_POLICIES, CONTENT_VERSIONS, EDGES, GRAPH_META, LEDGER, NODES, RAFT_LOG,
    RESOURCE_ANTI_AFFINITY, RESOURCE_CONCURRENCY, RESOURCE_DISK_POLICIES, RESOURCE_EXCLUSIVITY,
    RESOURCE_FAIRNESS, RESOURCE_HOSTS, RESOURCE_RESERVATIONS, RESOURCE_RESERVATION_ATTEMPTS,
    RESOURCE_RESERVATION_TENANT_INDEX, SEMANTIC, WORK_ITEM_COMMAND_SEQUENCE, XSHARD_DECISION,
    XSHARD_PREPARE,
};
#[cfg(feature = "matview")]
use crate::redb_store::{MATVIEW_OPERATOR_STATE, PLAN_MATVIEWS};

/// Outcome of a migration run (CONCEPT:EG-KG.sharding.atomic-shard-swap) — totals moved + the layout change.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Source shard file count (the OLD K).
    pub source_shards: usize,
    /// Destination shard file count (the NEW K).
    pub dest_shards: usize,
    /// Distinct graphs migrated (rows in `graph_meta`).
    pub graphs: usize,
    /// Node rows copied.
    pub nodes: u64,
    /// Edge rows copied.
    pub edges: u64,
    /// Ledger rows copied — the graph's own `(graph, seq)` operation ledger, not the
    /// kernel's mutation ledger (that one is counted under `auxiliary`).
    pub ledger: u64,
    /// Semantic-store rows copied.
    pub semantic: u64,
    /// Audit-chain rows copied (verbatim — chain preserved).
    pub audit: u64,
    /// Governed ChangeEnvelope rows copied, plus every kernel ledger row the graft
    /// moved (`GraftedScope::rows`): receipts, idempotency, classes, replay evidence,
    /// fences, outbox events and the delivery-side rows. Those fifteen tables replaced
    /// the eight retired `mutation_*` tables whose hand-written movers used to feed
    /// this same total.
    pub auxiliary: u64,
    /// File-wide rows copied (raft log/meta routed per group; 2PC + matviews on shard 0).
    pub global: u64,
    /// Rows copied from the tables this lane found missing from migration (BUG-CX-016,
    /// BUG-CX-054, and the undocumented capacity-lease/work-item-capability gap in the
    /// same class): provenance anchors, every `RESOURCE_*` table, every
    /// `development_lane_*` table, capacity-lease, and the work-item-capability tables.
    /// Counted separately from `auxiliary` so a migration report makes this coverage
    /// independently auditable.
    pub capability_and_resource: u64,
    /// ADR-2 / W1.2 group-count metadata: the number of Raft groups the destination
    /// layout supports. Under raft, K (redb shards) == N (groups) — raft group `g` owns
    /// shard `g` — so this equals `dest_shards`. Surfaced explicitly in the manifest so the
    /// W5.2 cutover runbook can assert the migrated store's group count matches the
    /// cluster's `EPISTEMIC_GRAPH_RAFT_GROUPS` before seeding the groups.
    pub dest_raft_groups: usize,
}

/// One opened source shard file and the graphs its catalog declares.
struct SourceShard {
    shard: Shard,
    graphs: Vec<String>,
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
/// OFFLINE only — the engine must be stopped (exclusive per-file lock). `dst_dir` must
/// not already contain a target shard file (the tool refuses to clobber). Use a fresh
/// dir, or [`migrate_in_place`] for an atomic in-dir swap. Every migrated graph is
/// MOVED: see the module doc's note on the graft retiring the source scope.
pub fn migrate_shards(
    src_dir: &Path,
    dst_dir: &Path,
    new_k: usize,
) -> Result<MigrationReport, String> {
    migrate_shards_inner(src_dir, dst_dir, new_k, None)
}

fn migrate_shards_inner(
    src_dir: &Path,
    dst_dir: &Path,
    new_k: usize,
    fault: Option<MigrationFault>,
) -> Result<MigrationReport, String> {
    let new_k = validate_shard_count(new_k)?;
    let src_paths = prepare_destination(src_dir, dst_dir, new_k)?;

    // Open every source file once, as a kernel-owned store, and read its catalog:
    // offline ⇒ the exclusive per-file lock is free, and the catalog is what tells
    // each destination pass which graphs it has to pull.
    let sources = open_source_shards(&src_paths)?;

    let mut report = MigrationReport {
        source_shards: src_paths.len(),
        dest_shards: new_k,
        // ADR-2 / W1.2: K == N under raft, so the destination group count is the shard count.
        dest_raft_groups: new_k,
        graphs: source_graph_catalog(&sources)?.len(),
        ..Default::default()
    };

    // One destination shard at a time: pull the graphs that route to THIS dest out of
    // every source, then its file-wide rows. K passes over the source catalogs are fine
    // for a one-time OFFLINE migration.
    let mut moved_graphs = 0usize;
    for dest_idx in 0..new_k {
        migrate_one_dest_shard(
            &sources,
            dst_dir,
            dest_idx,
            new_k,
            &mut report,
            &mut moved_graphs,
            fault,
        )?;
    }

    Ok(report)
}

/// Validate the source layout and make `dst_dir` ready to receive it, returning the
/// source shard paths.
fn prepare_destination(
    src_dir: &Path,
    dst_dir: &Path,
    new_k: usize,
) -> Result<Vec<PathBuf>, String> {
    let src_paths = discover_source_shards(src_dir)?;
    validate_shard_count(src_paths.len())?;
    validate_shard_count(new_k)?;
    std::fs::create_dir_all(dst_dir).map_err(|e| e.to_string())?;
    refuse_existing_destination_shards(dst_dir)?;
    Ok(src_paths)
}

/// Open every source shard file once as a kernel-owned store, with the graphs its
/// control scope's catalog declares (CONCEPT:EG-KG.sharding.atomic-shard-swap).
fn open_source_shards(src_paths: &[PathBuf]) -> Result<Vec<SourceShard>, String> {
    let mut sources = Vec::with_capacity(src_paths.len());
    for path in src_paths {
        let shard =
            Shard::open(path).map_err(|e| format!("open source {}: {e}", path.display()))?;
        let graphs = source_graph_names(&shard)?;
        sources.push(SourceShard { shard, graphs });
    }
    Ok(sources)
}

/// Every graph one source file hosts, read from the catalog on its control scope —
/// the scope that exists before any graph is bound, which is exactly what a boot scan
/// (and this tool) needs.
fn source_graph_names(shard: &Shard) -> Result<Vec<String>, String> {
    let read = shard.control_read()?;
    let catalog = read.open_owner_table(GRAPH_META)?;
    let mut graphs = Vec::new();
    for row in catalog.iter().map_err(|e| e.to_string())? {
        let (name, _) = row.map_err(|e| e.to_string())?;
        graphs.push(name.value().to_string());
    }
    Ok(graphs)
}

/// Every graph the whole source set holds, refusing a name two source shards both
/// claim.
///
/// One graph is one serving scope with one authority; two files claiming it is a
/// corrupt source layout, and merging them silently — which a key-prefix filter used
/// to do — would pick one generation's rows per collision.
fn source_graph_catalog(sources: &[SourceShard]) -> Result<HashSet<&str>, String> {
    let mut seen: HashSet<&str> = HashSet::new();
    for source in sources {
        for graph in &source.graphs {
            if !seen.insert(graph.as_str()) {
                return Err(format!(
                    "two source shards both hold graph '{graph}'; one graph has exactly one authority"
                ));
            }
        }
    }
    Ok(seen)
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

/// Wall-clock milliseconds, for the commit timestamp every admitted group carries.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

/// Create destination shard `dest_idx` and populate it from every source
/// (CONCEPT:EG-KG.sharding.atomic-shard-swap).
///
/// `Shard::open` materializes the whole declared `OwnerLayout::GraphShard` census, so
/// there is no hand-written canonical-table bootstrap here and no table can be missing
/// from a destination this tool wrote.
fn migrate_one_dest_shard(
    sources: &[SourceShard],
    dst_dir: &Path,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
    moved_graphs: &mut usize,
    fault: Option<MigrationFault>,
) -> Result<(), String> {
    let dst_path = dst_dir.join(shard_filename(dest_idx));
    let dest =
        Shard::open(&dst_path).map_err(|e| format!("create dest {}: {e}", dst_path.display()))?;
    for (index, source) in sources.iter().enumerate() {
        for graph in &source.graphs {
            if shard_index(graph, new_k) == dest_idx {
                move_one_graph(index, &source.shard, &dest, graph, report)?;
                *moved_graphs = (*moved_graphs).saturating_add(1);
                if fault
                    .is_some_and(|fault| matches!(fault, MigrationFault::AfterGraph(limit) if *moved_graphs >= limit))
                {
                    return Err(format!(
                        "injected migration fault after {} graph graft(s)",
                        *moved_graphs
                    ));
                }
            }
        }
    }
    copy_file_wide_rows(sources, &dest, dest_idx, new_k, report)
}

/// Move ONE graph out of `source` into `dest`: its owner rows, then its ledger.
///
/// The order is fixed by the graft: it retires the source scope's owner payload along
/// with its binding, so the owner rows have to be across before it runs. The graft
/// itself is the only thing that can move a ledger row — a domain crate has no ledger
/// write authority at all — and it preserves the source's marker-inclusive version
/// rather than re-admitting batches; Phase A contributes the one intentional
/// maintenance increment (RF-RULING-004 application note 4).
fn move_one_graph(
    _source_index: usize,
    source: &Shard,
    dest: &Shard,
    graph: &str,
    report: &mut MigrationReport,
) -> Result<(), String> {
    let mut transfer = GraphShardPayloadTransfer::for_migration(report);
    let grafted = dest.graft_graph_from_with_payload(source, graph, Some(&mut transfer))?;
    report.auxiliary = report.auxiliary.saturating_add(grafted.rows);
    Ok(())
}

/// Copy the graph-shard payload under the destination's exact graft
/// reservation.  The callback is invoked by the mutation kernel while the
/// destination ledger transaction is still open; no pre-graft maintenance row
/// is created.
pub(crate) struct GraphShardPayloadTransfer<'a> {
    report: Option<&'a mut MigrationReport>,
}

impl<'a> GraphShardPayloadTransfer<'a> {
    pub(crate) fn for_migration(report: &'a mut MigrationReport) -> Self {
        Self {
            report: Some(report),
        }
    }

    pub(crate) fn without_report() -> Self {
        Self { report: None }
    }
}

impl OwnerPayloadTransfer<GraphShardOwner, GraphShardOwner> for GraphShardPayloadTransfer<'_> {
    fn transfer_owner_payload(
        &mut self,
        source: &ScopedRead<'_, GraphShardOwner>,
        destination: &GraftOwnerWrite<'_, '_, GraphShardOwner>,
        _identity: &eg_types::MutationScopeIdentity,
    ) -> Result<u64, String> {
        clear_every_owner_table(destination)?;
        let mut discarded = MigrationReport::default();
        let report = self.report.as_deref_mut().unwrap_or(&mut discarded);
        let before = counted_owner_rows(report)?;
        copy_every_owner_table(source, destination, report)?;
        let after = counted_owner_rows(report)?;
        after
            .checked_sub(before)
            .ok_or_else(|| "owner-row migration counters moved backwards".to_string())
    }
}

/// Remove a previous partial payload stage.  The owner accessor is already
/// restricted to the destination graph, and the surrounding kernel transaction
/// has proved the graft reservation before this sweep is reachable.
fn clear_every_owner_table(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    macro_rules! clear {
        ($table:expr) => {{
            write.open_scoped_table($table)?.purge_scope_rows()?;
        }};
    }
    clear!(NODES);
    clear!(EDGES);
    clear!(LEDGER);
    clear!(SEMANTIC);
    #[cfg(feature = "security")]
    {
        clear!(AUDIT);
        clear!(PROVENANCE_ANCHOR_MEMBERS);
    }
    clear!(WORK_ITEM_COMMAND_SEQUENCE);
    clear!(RESOURCE_RESERVATIONS);
    clear!(RESOURCE_RESERVATION_TENANT_INDEX);
    clear!(RESOURCE_RESERVATION_ATTEMPTS);
    clear!(RESOURCE_HOSTS);
    clear!(RESOURCE_EXCLUSIVITY);
    clear!(RESOURCE_FAIRNESS);
    clear!(RESOURCE_CONCURRENCY);
    clear!(RESOURCE_ANTI_AFFINITY);
    clear!(RESOURCE_DISK_POLICIES);
    clear!(CHANGE_ENVELOPES);
    clear!(CONTENT_VERSIONS);
    clear!(CHANGE_CURSORS);
    clear!(CHANGE_BLOBS);
    clear!(CHANGE_FEATURES);
    clear!(CHANGE_EVIDENCE);
    clear!(CHANGE_POLICIES);
    clear!(CHANGE_LINEAGE);
    clear!(capacity_lease::CELLS);
    clear!(capacity_lease::LEASES);
    clear!(capacity_lease::USAGE);
    clear!(capacity_lease::IDEMPOTENCY);
    clear!(work_item_capability::CAPABILITIES);
    clear!(work_item_capability::INVOCATIONS);
    clear!(work_item_capability::NATIVE_WORK_ITEMS);
    clear!(development_lane::HOLDS);
    clear!(development_lane::TENANT_INDEX);
    clear!(development_lane::LANE_INDEX);
    clear!(development_lane::REPOSITORY_BRANCH_INDEX);
    clear!(development_lane::WORKTREE_INDEX);
    clear!(development_lane::WORK_ITEM_INDEX);
    clear!(development_lane::COUNTERS);
    clear!(development_lane::PRESSURE_INDEX);
    clear!(development_lane::POLICIES);
    clear!(development_lane::INVOCATIONS);
    Ok(())
}

/// Every scope-prefixed owner table of one graph, in three cohesive passes.
fn copy_every_owner_table(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    copy_core_graph_rows(read, write, report)?;
    copy_change_authority_rows(read, write, report)?;
    copy_capability_and_resource_rows(read, write, report)
}

/// The canonical payload-transfer result is the number of owner rows copied.
/// Keep it derived from the same counters that the migration report exposes so
/// the graft callback cannot invent a zero (or maintain a second row counter).
fn counted_owner_rows(report: &MigrationReport) -> Result<u64, String> {
    let mut total = 0_u64;
    for rows in [
        report.nodes,
        report.edges,
        report.ledger,
        report.semantic,
        report.audit,
        report.auxiliary,
        report.capability_and_resource,
    ] {
        total = total
            .checked_add(rows)
            .ok_or_else(|| "owner-row migration counters overflowed".to_string())?;
    }
    Ok(total)
}

/// Copy one scope-prefixed owner table's rows for the graph both sides are bound to.
///
/// The routing predicate every per-table copy used to carry is gone: the read is
/// bounded to one graph by its capability, so `scope_rows` yields that graph's rows and
/// no other's, and the destination write refuses a key that names anything else. Values
/// are re-inserted exactly as stored — never decoded, re-derived or re-sealed.
fn copy_scoped_table<K, V>(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    table: TableDefinition<'static, K, V>,
) -> Result<u64, String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope + OwnerRowScopeStart<'k>,
    V: redb::Value + 'static,
{
    let source = read.scoped_owner_table(table)?;
    let mut destination = write.open_scoped_table(table)?;
    let mut rows = 0u64;
    for row in source.scope_rows()? {
        let (key, value) = row?;
        destination.insert(key.value(), value.value())?;
        rows = rows.saturating_add(1);
    }
    Ok(rows)
}

/// nodes + edges + ledger + semantic_store + audit_chain — the tables the report
/// counts individually.
fn copy_core_graph_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.nodes += copy_scoped_table(read, write, NODES)?;
    report.edges += copy_scoped_table(read, write, EDGES)?;
    report.ledger += copy_scoped_table(read, write, LEDGER)?;
    report.semantic += copy_scoped_table(read, write, SEMANTIC)?;
    report.audit += copy_scoped_table(read, write, AUDIT)?;
    Ok(())
}

/// The 8 governed ChangeEnvelope tables, in two passes so neither unit's complexity is
/// dominated by a straight-line chain of fallible calls.
fn copy_change_authority_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.auxiliary += copy_change_core_rows(read, write)?;
    report.auxiliary += copy_change_side_rows(read, write)?;
    Ok(())
}

/// change_envelopes + content_versions + change_cursors + change_blobs.
fn copy_change_core_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, CHANGE_ENVELOPES)?;
    rows += copy_scoped_table(read, write, CONTENT_VERSIONS)?;
    rows += copy_scoped_table(read, write, CHANGE_CURSORS)?;
    rows += copy_scoped_table(read, write, CHANGE_BLOBS)?;
    Ok(rows)
}

/// change_features + change_evidence + change_policies + change_lineage.
fn copy_change_side_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, CHANGE_FEATURES)?;
    rows += copy_scoped_table(read, write, CHANGE_EVIDENCE)?;
    rows += copy_scoped_table(read, write, CHANGE_POLICIES)?;
    rows += copy_scoped_table(read, write, CHANGE_LINEAGE)?;
    Ok(rows)
}

// ── BUG-CX-016 / BUG-CX-054 / undocumented-gap tables ───────────────────────
//
// Everything below was found missing from this file by the mechanical table-vs-migrate
// inventory an earlier lane ran: every table constant reachable from `redb_store.rs`,
// `capacity_lease.rs`, `development_lane.rs` and `work_item_capability.rs`,
// cross-checked against what this file actually moved. `provenance_anchor_members`
// (BUG-CX-016), all 9 `RESOURCE_*` tables + `development_lane`'s full 10 +
// `NATIVE_WORK_ITEMS` (BUG-CX-054), and — not named in any existing bug — the 4
// `capacity_lease` tables and the 2 remaining `work_item_capability` tables are the
// SAME "a table gets a writer and never gets a migrator" defect class, in two more
// subsystems. Under the kernel the census itself is the backstop: `Shard::open`
// materializes every declared table, so a table missing HERE is the only way a row can
// still be left behind, which is why the list stays grouped and readable.

/// Every table found missing from `migrate_shards` beyond the pre-existing core/change
/// coverage, grouped as its own pass so a reshard cannot drop provenance-anchor,
/// resource-reservation, development-lane, capacity-lease or work-item-capability
/// authority the way it silently did before this fix.
fn copy_capability_and_resource_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    let mut rows = copy_provenance_and_sequence_rows(read, write)?;
    rows += copy_resource_rows(read, write)?;
    rows += copy_development_lane_rows(read, write)?;
    rows += copy_capacity_lease_rows(read, write)?;
    rows += copy_work_item_capability_rows(read, write)?;
    report.capability_and_resource += rows;
    Ok(())
}

/// provenance_anchor_members + work_item_command_sequence (BUG-CX-016/BUG-CX-054 class).
fn copy_provenance_and_sequence_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let sequence = copy_scoped_table(read, write, WORK_ITEM_COMMAND_SEQUENCE)?;
    #[cfg(feature = "security")]
    let anchors = copy_scoped_table(read, write, PROVENANCE_ANCHOR_MEMBERS)?;
    #[cfg(not(feature = "security"))]
    let anchors = 0u64;
    Ok(sequence + anchors)
}

/// Every RESOURCE_* table (BUG-CX-054): reservations, tenant index, attempts, hosts,
/// exclusivity, fairness, concurrency, anti-affinity, disk policies.
fn copy_resource_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    Ok(copy_resource_core_rows(read, write)? + copy_resource_policy_rows(read, write)?)
}

/// resource_reservations + tenant_index + attempts + hosts (BUG-CX-054).
fn copy_resource_core_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, RESOURCE_RESERVATIONS)?;
    rows += copy_scoped_table(read, write, RESOURCE_RESERVATION_TENANT_INDEX)?;
    rows += copy_scoped_table(read, write, RESOURCE_RESERVATION_ATTEMPTS)?;
    rows += copy_scoped_table(read, write, RESOURCE_HOSTS)?;
    Ok(rows)
}

/// resource_exclusivity + fairness + concurrency + anti_affinity + disk_policies
/// (BUG-CX-054).
fn copy_resource_policy_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, RESOURCE_EXCLUSIVITY)?;
    rows += copy_scoped_table(read, write, RESOURCE_FAIRNESS)?;
    rows += copy_scoped_table(read, write, RESOURCE_CONCURRENCY)?;
    rows += copy_scoped_table(read, write, RESOURCE_ANTI_AFFINITY)?;
    rows += copy_scoped_table(read, write, RESOURCE_DISK_POLICIES)?;
    Ok(rows)
}

/// All 10 development_lane_* tables (BUG-CX-054/BUG-CX-096), so a reshard carries the
/// same authority the lane's own writers treat as live.
fn copy_development_lane_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    Ok(copy_lane_core_rows(read, write)? + copy_lane_index_rows(read, write)?)
}

/// development_lane holds + counters + pressure_index + policies + invocations
/// (BUG-CX-054/096).
fn copy_lane_core_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, development_lane::HOLDS)?;
    rows += copy_scoped_table(read, write, development_lane::COUNTERS)?;
    rows += copy_scoped_table(read, write, development_lane::PRESSURE_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::POLICIES)?;
    rows += copy_scoped_table(read, write, development_lane::INVOCATIONS)?;
    Ok(rows)
}

/// The 5 development_lane secondary indexes: tenant, lane, repository/branch, worktree,
/// work item (BUG-CX-096).
fn copy_lane_index_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, development_lane::TENANT_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::LANE_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::REPOSITORY_BRANCH_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::WORKTREE_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::WORK_ITEM_INDEX)?;
    Ok(rows)
}

/// capacity_cells + leases + usage + idempotency (undocumented BUG-CX-054-class gap).
fn copy_capacity_lease_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, capacity_lease::CELLS)?;
    rows += copy_scoped_table(read, write, capacity_lease::LEASES)?;
    rows += copy_scoped_table(read, write, capacity_lease::USAGE)?;
    rows += copy_scoped_table(read, write, capacity_lease::IDEMPOTENCY)?;
    Ok(rows)
}

/// work_item_claim_capabilities + its invocations + native_work_item_authority
/// (BUG-CX-054 + undocumented gap).
fn copy_work_item_capability_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, work_item_capability::CAPABILITIES)?;
    rows += copy_scoped_table(read, write, work_item_capability::INVOCATIONS)?;
    rows += copy_scoped_table(read, write, work_item_capability::NATIVE_WORK_ITEMS)?;
    Ok(rows)
}

// ── the file-wide rows ──────────────────────────────────────────────────────

/// Copy the FILE-WIDE (non-per-graph) durable tables from every source into `dest_idx`
/// (CONCEPT:EG-KG.sharding.atomic-shard-swap), as ONE control-only admitted write.
///
/// `members` is empty here, which is exactly the control-only write the shard seam
/// defines: these rows belong to the file, not to any graph, and the control member is
/// the only thing that may address them.
fn copy_file_wide_rows(
    sources: &[SourceShard],
    dest: &Shard,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    let members = dest.graph_members::<&str>(&[])?;
    let (group, batches) = dest.admit_maintenance(&members, "shard-migrate/file-wide")?;
    let write = ShardWrite::open(dest, &group, &members, &batches)?;
    copy_every_sources_file_wide_rows(sources, write.control(), dest_idx, new_k, report)?;
    write.finish()?;
    dest.commit_drain(group, &batches, now_ms())
}

/// The catalog, Raft and shard-0 passes over every source, then the one canary record.
fn copy_every_sources_file_wide_rows(
    sources: &[SourceShard],
    control: &impl OwnerPayloadWrite,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    for source in sources {
        let read = source.shard.control_read()?;
        copy_catalog_and_raft_rows(&read, control, dest_idx, new_k, report)?;
        copy_shard_zero_rows(&read, control, dest_idx, report)?;
    }
    #[cfg(feature = "security")]
    copy_encryption_canary(sources, control)?;
    Ok(())
}

/// Copy one file-wide table's rows, keeping the route callback the file-wide key
/// shapes still need: `graph_meta` routes by graph name, the Raft tables by group id,
/// and the 2PC/matview tables not at all.
///
/// A file-wide table is a raw `redb` table on both sides — its key carries no scope
/// component, so there is nothing to bound it to but the layout.
fn copy_file_wide_table<K, V, F>(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    table: TableDefinition<'static, K, V>,
    route: F,
) -> Result<u64, String>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
    F: for<'a> Fn(&K::SelfType<'a>) -> bool,
{
    let source = read.open_owner_table(table)?;
    let mut destination = write.open_table(table)?;
    let mut rows = 0u64;
    for row in source.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let key = key.value();
        if route(&key) {
            destination
                .insert(key, value.value())
                .map_err(|e| e.to_string())?;
            rows = rows.saturating_add(1);
        }
    }
    Ok(rows)
}

/// graph_meta (routed by graph, like the graph it names) + the Raft log and meta.
///
/// ADR-2 / W1.2: a `(group_id, …)` Raft row routes to `group_id % new_k`, EXACTLY like
/// `RedbBackend::shard_for_group` at runtime, so a migrated raft store finds each
/// group's log/vote/applied in that group's own shard rather than stranded on shard 0.
/// The catalog rows are not counted: `report.graphs` already counts each graph once.
fn copy_catalog_and_raft_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    control: &impl OwnerPayloadWrite,
    dest_idx: usize,
    new_k: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    copy_file_wide_table(read, control, GRAPH_META, |graph| {
        shard_index(graph, new_k) == dest_idx
    })?;
    report.global += copy_file_wide_table(read, control, RAFT_LOG, |key| {
        (key.0 as usize) % new_k == dest_idx
    })?;
    report.global += copy_file_wide_table(read, control, RAFT_META, |key| {
        (key.0 as usize) % new_k == dest_idx
    })?;
    Ok(())
}

/// The 2PC coordinator records + the matview tables keep their runtime `shard0()` home
/// (unchanged by ADR-2), so they are migrated only on the first destination pass.
///
/// BUG-CX-016: `plan_matviews`/`matview_operator_state` are disjoint from `matviews`
/// (definitions vs plan-backed definitions and incremental operator state) but share
/// that home.
fn copy_shard_zero_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    control: &impl OwnerPayloadWrite,
    dest_idx: usize,
    report: &mut MigrationReport,
) -> Result<(), String> {
    if dest_idx != 0 {
        return Ok(());
    }
    report.global += copy_file_wide_table(read, control, XSHARD_PREPARE, |_| true)?;
    report.global += copy_file_wide_table(read, control, XSHARD_DECISION, |_| true)?;
    #[cfg(feature = "compute-dist")]
    {
        report.global += copy_file_wide_table(read, control, MATVIEWS, |_| true)?;
    }
    #[cfg(feature = "matview")]
    {
        report.global += copy_file_wide_table(read, control, PLAN_MATVIEWS, |_| true)?;
        report.global += copy_file_wide_table(read, control, MATVIEW_OPERATOR_STATE, |_| true)?;
    }
    Ok(())
}

/// Key-binding/canary metadata is per-shard, but it is not graph-addressed. A
/// K-changing migration therefore copies one consistent source record into every
/// destination shard so each open can enforce the same key identity and version. Not
/// counted as graph/file-wide data: duplicating metadata across a changed K must not
/// make restore totals appear to change.
#[cfg(feature = "security")]
fn copy_encryption_canary(
    sources: &[SourceShard],
    control: &impl OwnerPayloadWrite,
) -> Result<(), String> {
    let Some(rows) = find_consistent_encryption_canary_rows(sources)? else {
        return Ok(());
    };
    let mut destination = control.open_table(ENCRYPTION_CANARY)?;
    // Carry the first (and only, once validated) consistent shard's rows forward: every
    // shard's canary decrypts to the same plaintext under the one agreed key, so any
    // single copy is a valid canary for the destination.
    for (key, value) in rows {
        destination
            .insert(key.as_str(), value.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// One source shard's `encryption_canary` table as `(key, value)` rows, in table
/// order. Named so the canary-comparison signatures stay readable.
#[cfg(feature = "security")]
type EncryptionCanaryRows = Vec<(String, Vec<u8>)>;

/// Scan every source's `encryption_canary` table and return ONE consistent set of
/// rows to carry forward, or `Ok(None)` when no source has a canary at all.
///
/// Compare the KEY BINDING row, never the whole table.
///
/// The `encryption_canary` table holds two different kinds of row. The binding
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
    sources: &[SourceShard],
) -> Result<Option<EncryptionCanaryRows>, String> {
    let mut source_rows: Option<EncryptionCanaryRows> = None;
    let mut source_binding: Option<Vec<u8>> = None;
    for source in sources {
        let rows = read_encryption_canary_rows(&source.shard)?;
        if rows.is_empty() {
            continue;
        }
        let binding = rows
            .iter()
            .find(|(key, _)| key == ENCRYPTION_KEY_BINDING_KEY)
            .map(|(_, value)| value.clone());
        // A canary with no binding row is the pre-key-lifecycle shape the open path
        // upgrades in place on first open with the configured key. Restoring one is
        // refused with the REAL reason rather than being mislabelled a key mismatch:
        // without a binding there is no key identity to compare, and the canary alone
        // cannot supply one.
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

/// Every row of ONE source's `encryption_canary` table. The table always exists: the
/// declared census is materialized at open, so an absent table is no longer a case.
#[cfg(feature = "security")]
fn read_encryption_canary_rows(shard: &Shard) -> Result<EncryptionCanaryRows, String> {
    let read = shard.control_read()?;
    let table = read.open_owner_table(ENCRYPTION_CANARY)?;
    let mut rows = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        rows.push((key.value().to_string(), value.value().to_vec()));
    }
    Ok(rows)
}

/// Migrate the store under `persist_dir` to `new_k` IN PLACE
/// (CONCEPT:EG-KG.sharding.atomic-shard-swap).  The source files are copied to
/// an immutable backup before any graft can retire a scope.  The destination is
/// then built from that snapshot in a named temp tree.  Only after the complete
/// tree is marked ready are the live files moved aside and the destination files
/// installed.
const IN_PLACE_READY_MARKER: &str = ".migration-ready";
const READY_MARKER_VERSION: &str = "eg-shard-migration-v2";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadyTarget {
    name: String,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadyMigration {
    backup: PathBuf,
    source_names: Vec<String>,
    new_k: usize,
    targets: Vec<ReadyTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationFault {
    AfterGraph(usize),
    BeforeSwap,
    AfterOldMove(usize),
    AfterInstall(usize),
}

fn file_sha256(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("open migration artifact {} failed: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            format!("read migration artifact {} failed: {error}", path.display())
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn expected_target_name(index: usize) -> String {
    shard_filename(index)
}

fn parse_ready_marker(path: &Path, requested_k: usize) -> Result<ReadyMigration, String> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "read shard migration readiness marker {} failed: {error}",
            path.display()
        )
    })?;
    let mut lines = text.lines();
    if lines.next() != Some(READY_MARKER_VERSION) {
        return Err(format!(
            "shard migration readiness marker {} has an unsupported version",
            path.display()
        ));
    }
    let backup = lines
        .next()
        .and_then(|line| line.strip_prefix("backup="))
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            format!(
                "shard migration readiness marker {} has no backup",
                path.display()
            )
        })?;
    let new_k = lines
        .next()
        .and_then(|line| line.strip_prefix("new_k="))
        .ok_or_else(|| {
            format!(
                "shard migration readiness marker {} has no target K",
                path.display()
            )
        })?
        .parse::<usize>()
        .map_err(|_| {
            format!(
                "shard migration readiness marker {} has an invalid target K",
                path.display()
            )
        })?;
    if new_k != requested_k {
        return Err(format!(
            "shard migration readiness target K {new_k} does not match requested K {requested_k}; preserving live source and temp"
        ));
    }

    let mut source_names = Vec::new();
    let mut targets = Vec::new();
    for line in lines {
        if let Some(name) = line.strip_prefix("source=") {
            if name.is_empty()
                || Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name)
                || source_names.iter().any(|existing| existing == name)
            {
                return Err(format!(
                    "shard migration readiness marker {} has an invalid source manifest",
                    path.display()
                ));
            }
            source_names.push(name.to_string());
            continue;
        }
        let Some(target) = line.strip_prefix("target=") else {
            return Err(format!(
                "shard migration readiness marker {} has an unknown entry",
                path.display()
            ));
        };
        let (name, sha256) = target.split_once('\t').ok_or_else(|| {
            format!(
                "shard migration readiness marker {} has an invalid target manifest",
                path.display()
            )
        })?;
        if name.is_empty()
            || Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name)
            || targets
                .iter()
                .any(|existing: &ReadyTarget| existing.name == name)
            || sha256.len() != 64
            || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format!(
                "shard migration readiness marker {} has an invalid target manifest",
                path.display()
            ));
        }
        targets.push(ReadyTarget {
            name: name.to_string(),
            sha256: sha256.to_ascii_lowercase(),
        });
    }
    if source_names.is_empty() || targets.len() != new_k {
        return Err(format!(
            "shard migration readiness marker {} has an incomplete file manifest",
            path.display()
        ));
    }
    for (index, target) in targets.iter().enumerate() {
        if target.name != expected_target_name(index) {
            return Err(format!(
                "shard migration readiness marker {} has a non-contiguous target manifest",
                path.display()
            ));
        }
    }
    Ok(ReadyMigration {
        backup,
        source_names,
        new_k,
        targets,
    })
}

fn write_ready_marker(
    tmp: &Path,
    backup: &Path,
    src_paths: &[PathBuf],
    new_k: usize,
) -> Result<ReadyMigration, String> {
    let mut marker = format!(
        "{READY_MARKER_VERSION}\nbackup={}\nnew_k={new_k}\n",
        backup.display()
    );
    for source in src_paths {
        let name = source
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
        marker.push_str("source=");
        marker.push_str(name);
        marker.push('\n');
    }
    for index in 0..new_k {
        let name = expected_target_name(index);
        let staged = tmp.join(&name);
        let sha256 = file_sha256(&staged)?;
        marker.push_str("target=");
        marker.push_str(&name);
        marker.push('\t');
        marker.push_str(&sha256);
        marker.push('\n');
    }
    let path = tmp.join(IN_PLACE_READY_MARKER);
    std::fs::write(&path, marker.as_bytes()).map_err(|error| {
        format!(
            "write shard migration readiness marker {} failed: {error}",
            path.display()
        )
    })?;
    parse_ready_marker(&path, new_k)
}

fn unique_backup_dir(base: &Path) -> Result<PathBuf, String> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    for attempt in 0..100u32 {
        let candidate = base.join(format!(".shard-migrate-backup-{}-{attempt}", stamp));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err("could not allocate a unique shard migration backup directory".to_string())
}

fn copy_source_snapshot(src_paths: &[PathBuf], snapshot: &Path) -> Result<(), String> {
    std::fs::create_dir_all(snapshot).map_err(|error| {
        format!(
            "create immutable shard migration source snapshot {} failed: {error}",
            snapshot.display()
        )
    })?;
    for source in src_paths {
        let name = source
            .file_name()
            .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
        std::fs::copy(source, snapshot.join(name)).map_err(|error| {
            format!(
                "copy source shard {} into immutable snapshot failed: {error}",
                source.display()
            )
        })?;
    }
    Ok(())
}

fn swap_ready_shards(
    base: &Path,
    tmp: &Path,
    ready: &ReadyMigration,
    fault: Option<MigrationFault>,
) -> Result<(), String> {
    if ready.new_k != ready.targets.len() {
        return Err("ready migration target manifest does not match its target K".to_string());
    }
    let backup = &ready.backup;
    let old = backup.join("old");
    std::fs::create_dir_all(&old).map_err(|error| error.to_string())?;
    let source_backup = backup.join("source");

    // Validate every staged/installed target before moving any live source.
    // A stale or foreign target must leave both the old layout and the ready
    // temp tree untouched.
    for target in &ready.targets {
        let staged = tmp.join(&target.name);
        let installed = base.join(&target.name);
        if staged.exists() && file_sha256(&staged)? != target.sha256 {
            return Err(format!(
                "staged shard {} does not match its ready digest",
                staged.display()
            ));
        }
        if installed.exists() {
            let installed_digest = file_sha256(&installed)?;
            if installed_digest != target.sha256 {
                let source_digest = if ready.source_names.iter().any(|name| name == &target.name) {
                    file_sha256(&source_backup.join(&target.name))?
                } else {
                    String::new()
                };
                if installed_digest != source_digest {
                    return Err(format!(
                        "installed shard {} is neither its ready target nor the original source",
                        installed.display()
                    ));
                }
            }
        } else if !staged.exists() && !ready.source_names.iter().any(|name| name == &target.name) {
            return Err(format!(
                "ready migration is missing staged and installed target {}",
                target.name
            ));
        }
    }

    // Move every old live file out of the way, but recognize an already
    // installed target by its authenticated target digest. A retry after a
    // crash must never move that target back into `old` and then lose the only
    // installed copy from the staged tree.
    let mut moved_old = 0usize;
    for name in &ready.source_names {
        let source = base.join(name);
        let old_path = old.join(name);
        let source_snapshot = source_backup.join(name);
        let source_digest = file_sha256(&source_snapshot)?;
        if old_path.exists() {
            if file_sha256(&old_path)? != source_digest {
                return Err(format!(
                    "migration recovery backup {} does not match immutable source",
                    old_path.display()
                ));
            }
            if source.exists() {
                let installed_target = ready
                    .targets
                    .iter()
                    .find(|target| target.name == *name)
                    .is_some_and(|target| {
                        file_sha256(&source).ok().as_deref() == Some(target.sha256.as_str())
                    });
                if !installed_target {
                    return Err(format!(
                        "migration recovery has both old and live source file {}",
                        source.display()
                    ));
                }
            }
            continue;
        }
        if !source.exists() {
            continue;
        }
        let source_digest_now = file_sha256(&source)?;
        let already_installed = ready
            .targets
            .iter()
            .find(|target| target.name == *name)
            .is_some_and(|target| source_digest_now == target.sha256);
        if already_installed {
            continue;
        }
        if source_digest_now != source_digest {
            return Err(format!(
                "live shard {} is neither the immutable source nor its ready target",
                source.display()
            ));
        }
        std::fs::rename(&source, &old_path).map_err(|error| {
            format!(
                "move old shard {} into recovery backup failed: {error}",
                source.display()
            )
        })?;
        moved_old = moved_old.saturating_add(1);
        if fault.is_some_and(
            |fault| matches!(fault, MigrationFault::AfterOldMove(limit) if moved_old >= limit),
        ) {
            return Err(format!(
                "injected migration fault after {moved_old} old shard move(s)"
            ));
        }
    }

    let mut installed_targets = 0usize;
    for target in &ready.targets {
        let staged = tmp.join(&target.name);
        let installed = base.join(&target.name);
        if installed.exists() {
            if file_sha256(&installed)? != target.sha256 {
                return Err(format!(
                    "installed shard {} does not match its ready digest",
                    installed.display()
                ));
            }
            if staged.exists() && file_sha256(&staged)? != target.sha256 {
                return Err(format!(
                    "staged shard {} does not match its ready digest",
                    staged.display()
                ));
            }
        } else if staged.exists() {
            if file_sha256(&staged)? != target.sha256 {
                return Err(format!(
                    "staged shard {} does not match its ready digest",
                    staged.display()
                ));
            }
            std::fs::rename(&staged, &installed).map_err(|error| {
                format!(
                    "install migrated shard {} failed: {error}",
                    installed.display()
                )
            })?;
            installed_targets = installed_targets.saturating_add(1);
            if fault.is_some_and(|fault| {
                matches!(fault, MigrationFault::AfterInstall(limit) if installed_targets >= limit)
            }) {
                return Err(format!(
                    "injected migration fault after {installed_targets} target install(s)"
                ));
            }
        } else {
            return Err(format!(
                "ready migration is missing both staged and installed shard {}",
                target.name
            ));
        }
    }
    std::fs::remove_dir_all(tmp).map_err(|error| {
        format!(
            "remove completed shard migration temp tree {} failed: {error}",
            tmp.display()
        )
    })?;
    Ok(())
}

pub fn migrate_in_place(persist_dir: &str, new_k: usize) -> Result<MigrationReport, String> {
    migrate_in_place_inner(persist_dir, new_k, None)
}

#[cfg(test)]
fn migrate_in_place_after_graph_for_test(
    persist_dir: &str,
    new_k: usize,
    after_graphs: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(
        persist_dir,
        new_k,
        Some(MigrationFault::AfterGraph(after_graphs)),
    )
}

#[cfg(test)]
fn migrate_in_place_before_swap_for_test(
    persist_dir: &str,
    new_k: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(persist_dir, new_k, Some(MigrationFault::BeforeSwap))
}

#[cfg(test)]
fn migrate_in_place_after_old_move_for_test(
    persist_dir: &str,
    new_k: usize,
    after_files: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(
        persist_dir,
        new_k,
        Some(MigrationFault::AfterOldMove(after_files)),
    )
}

#[cfg(test)]
fn migrate_in_place_after_install_for_test(
    persist_dir: &str,
    new_k: usize,
    after_files: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(
        persist_dir,
        new_k,
        Some(MigrationFault::AfterInstall(after_files)),
    )
}

fn migrate_in_place_inner(
    persist_dir: &str,
    new_k: usize,
    fault: Option<MigrationFault>,
) -> Result<MigrationReport, String> {
    let new_k = validate_shard_count(new_k)?;
    let base = Path::new(persist_dir);
    let tmp = base.join(".shard-migrate-tmp");

    let (ready, report) = if tmp.exists() {
        let marker = tmp.join(IN_PLACE_READY_MARKER);
        if !marker.exists() {
            return Err(format!(
                "shard migration temp tree {} is incomplete; preserving it and the live source for recovery",
                tmp.display()
            ));
        }
        let ready = parse_ready_marker(&marker, new_k)?;
        if ready.backup.parent() != Some(base) {
            return Err(format!(
                "ready shard migration backup {} is outside the live store; preserving source and temp",
                ready.backup.display()
            ));
        }
        if !ready.backup.join("source").is_dir() {
            return Err(format!(
                "ready shard migration references missing source backup {}",
                ready.backup.display()
            ));
        }
        // The report is rebuilt from the ready destination's durable files only
        // after the swap.  A resume therefore returns a conservative report with
        // source/destination topology; row counters are not used for recovery.
        let report = MigrationReport {
            source_shards: ready.source_names.len(),
            dest_shards: new_k,
            dest_raft_groups: new_k,
            ..MigrationReport::default()
        };
        (ready, report)
    } else {
        let src_paths = discover_source_shards(base)?;
        let backup = unique_backup_dir(base)?;
        let snapshot = backup.join("source");
        copy_source_snapshot(&src_paths, &snapshot)?;
        let build_source = backup.join("build-source");
        let snapshot_paths: Vec<PathBuf> = src_paths
            .iter()
            .map(|source| {
                let name = source
                    .file_name()
                    .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
                Ok(snapshot.join(name))
            })
            .collect::<Result<_, String>>()?;
        // The build source is the copy the migration READS from, so unlike the
        // preserved `source` snapshot it has to be openable. A store's physical
        // root is derived from its canonical path and checked on every open --
        // that is what stops a byte copy being served as the original -- so a
        // copy is unopenable at its new path until its root is rebound. Without
        // this, every in-place migration failed with "mutation store root
        // incarnation mismatch" before reading a single row.
        //
        // The preserved `source` snapshot is deliberately NOT rebound: it is
        // evidence, never opened, and rebinding it would make it look like a
        // store that legitimately lives there.
        copy_source_snapshot(&snapshot_paths, &build_source)?;
        // BOTH copies are rebound, for different reasons.
        //
        // The build source has to be, or the migration cannot read a single row:
        // a store's physical root is derived from its canonical path and checked
        // on every open -- that is what stops a byte copy being served as the
        // original -- so a copy is unopenable at its new path until rebound.
        //
        // The preserved snapshot is deliberately NOT rebound. Rebinding it was
        // tried -- the argument being that this function promises "a later
        // operator can inspect or remove the named tree", and a backup that
        // cannot be opened cannot be inspected -- and it REGRESSED four tests
        // (1680/42 -> 1676/46). Something downstream depends on the snapshot
        // still carrying the original root, so it stays evidence.
        //
        // Resolved 2026-09-11 (eg-f3 burndown): "inspect" does not require
        // opening the evidence WHERE IT LIES, and it must not, because
        // `rebind_copied_store` rewrites the file it is given -- inspecting by
        // rebinding in place would destroy the very byte-identity that makes the
        // tree evidence. The recovery step is: copy the tree out, rebind the
        // COPY against the original it was taken from, open the copy. Both
        // requirements then hold at once, and
        // `in_place_fault_after_first_graft_keeps_live_source_and_backup` proves
        // it -- including that the evidence is byte-identical afterwards.
        //
        // `copied_from` is the ORIGINAL, not the intermediate snapshot: these are
        // plain byte copies, so the build source still carries the root of the
        // store the chain started at.
        for source in &src_paths {
            let name = source
                .file_name()
                .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
            let copy = build_source.join(name);
            eg_storage::rebind_copied_store(&copy, source).map_err(|error| {
                format!(
                    "rebind migration build source {} failed: {error}",
                    copy.display()
                )
            })?;
        }
        std::fs::create_dir_all(&tmp).map_err(|error| error.to_string())?;
        let report = match migrate_shards_inner(&build_source, &tmp, new_k, fault) {
            Ok(report) => report,
            Err(error) => {
                // Preserve both the immutable source snapshot and any partial
                // destination rows.  A later operator can inspect or remove
                // the named tree explicitly; this function never destroys it.
                return Err(format!(
                    "in-place shard migration build failed; source snapshot {}, build source {} and temp {} preserved: {error}",
                    snapshot.display(),
                    build_source.display(),
                    tmp.display()
                ));
            }
        };
        if let Err(error) = std::fs::remove_dir_all(&build_source) {
            tracing::warn!(
                "leaving consumed shard migration build snapshot {} after cleanup failure: {error}",
                build_source.display()
            );
        }
        let ready = write_ready_marker(&tmp, &backup, &src_paths, new_k)?;
        if matches!(fault, Some(MigrationFault::BeforeSwap)) {
            return Err(format!(
                "injected migration fault before swap; readiness marker {} preserved",
                tmp.join(IN_PLACE_READY_MARKER).display()
            ));
        }
        (ready, report)
    };

    swap_ready_shards(base, &tmp, &ready, fault)?;
    tracing::info!(
        "shard migration complete: {} -> {} shards, {} graphs; immutable source backup at {}",
        report.source_shards,
        report.dest_shards,
        report.graphs,
        ready.backup.display()
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{GraphType, Method};
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn temp_root(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "eg-migrate-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    /// Create one empty kernel-owned shard file at `path`.
    ///
    /// The layout tests below need a FILE to exist under a given name; what they assert
    /// is filename discovery, which never opens the file. `Shard::open` is how a shard
    /// file comes into existence after the cut, so that is how these fixtures make one.
    fn empty_shard(path: &std::path::Path) {
        drop(Shard::open(path).expect("create shard fixture"));
    }

    /// Rows of one scope-prefixed owner table for ONE graph, read back through that
    /// graph's own bound scope. Used to inspect a migration's ON-DISK output directly.
    fn graph_row_count<K, V>(
        path: &std::path::Path,
        graph: &str,
        def: TableDefinition<'static, K, V>,
    ) -> usize
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: OwnerRowScope + OwnerRowScopeStart<'k>,
        V: redb::Value + 'static,
    {
        let shard = Shard::open(path).expect("open shard for inspection");
        let handle = shard.graph(graph).expect("bind graph for inspection");
        let read = shard.read(&handle).expect("scoped read");
        let table = read
            .scoped_owner_table(def)
            .expect("open scoped owner table");
        table.scope_rows().expect("scan the scope's rows").count()
    }

    /// Rows of one FILE-WIDE owner table, read back through the file's control scope.
    #[cfg(feature = "matview")]
    fn control_row_count<K, V>(path: &std::path::Path, def: TableDefinition<'static, K, V>) -> usize
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let shard = Shard::open(path).expect("open shard for inspection");
        let read = shard.control_read().expect("control read");
        read.open_owner_table(def)
            .expect("open file-wide owner table")
            .iter()
            .expect("iterate table")
            .count()
    }

    /// The authoritative version and one receipt of `graph` on the shard file at `path`.
    fn graph_ledger(path: &std::path::Path, graph: &str, batch_id: &str) -> (u64, bool) {
        let shard = Shard::open(path).expect("open shard for inspection");
        let handle = shard.graph(graph).expect("bind graph for inspection");
        let read = shard.read(&handle).expect("scoped read");
        let version = eg_transaction::version(&read).expect("scope version");
        let receipt = eg_transaction::read_ledger(&read, batch_id)
            .expect("read receipt")
            .is_some();
        (version, receipt)
    }

    /// Seed one graph on a shard file: its `graph_meta` catalog row, the governed
    /// ChangeEnvelope rows a migration must carry with it, and ONE committed
    /// maintenance batch, so the graph has a real kernel ledger, receipt and version.
    ///
    /// Returns that receipt's batch id and the version the source ends at.
    fn seed_graph(path: &std::path::Path, graph: &str, tag: &str) -> (String, u64) {
        let shard = Shard::open(path).expect("open source shard");
        let members = shard.graph_members(&[graph]).expect("bind graph");
        let op_id = format!("seed-{tag}");
        let (group, batches) = shard.admit_maintenance(&members, &op_id).expect("admit");
        let write = ShardWrite::open(&shard, &group, &members, &batches).expect("open write");
        write
            .control()
            .open_table(GRAPH_META)
            .expect("graph_meta")
            .insert(graph, format!("meta-{tag}").as_bytes())
            .expect("catalog row");
        {
            let rows = write.graph(graph).expect("graph member");
            rows.open_scoped_table(CHANGE_ENVELOPES)
                .expect("change_envelopes")
                .insert((graph, "envelope-1"), format!("envelope-{tag}").as_bytes())
                .expect("envelope row");
            rows.open_scoped_table(CONTENT_VERSIONS)
                .expect("content_versions")
                .insert((graph, "tenant-a", "object-1"), &b"version"[..])
                .expect("content version row");
            rows.open_scoped_table(CHANGE_CURSORS)
                .expect("change_cursors")
                .insert(
                    (graph, "tenant-a", "source-a", "partition-a"),
                    &b"cursor"[..],
                )
                .expect("cursor row");
        }
        write.finish().expect("finish");
        shard.commit_drain(group, &batches, 1).expect("commit");

        let handle = shard.graph(graph).expect("graph handle");
        let read = shard.read(&handle).expect("scoped read");
        let version = eg_transaction::version(&read).expect("scope version");
        (format!("shard_drain/{graph}:{op_id}"), version)
    }

    /// Insert one owner row directly into `table` for `graph`, bypassing every request/
    /// validation path — the same "seed the durable table, not the API" technique
    /// `graph_row_count` already uses for reads, through the one admitted write path a
    /// domain has. `second_key` doubles as the write's operation id, so seeding several
    /// tables on one shard never re-presents an identity the ledger already holds.
    fn seed_owner_row(
        shard_path: &std::path::Path,
        table: TableDefinition<'static, (&str, &str), &[u8]>,
        graph: &str,
        second_key: &str,
        value: &[u8],
    ) {
        let shard = Shard::open(shard_path).expect("open shard for seeding");
        let members = shard.graph_members(&[graph]).expect("bind graph");
        let op_id = format!("seed-owner-row/{second_key}");
        let (group, batches) = shard.admit_maintenance(&members, &op_id).expect("admit");
        let write = ShardWrite::open(&shard, &group, &members, &batches).expect("open write");
        write
            .graph(graph)
            .expect("graph member")
            .open_scoped_table(table)
            .expect("open table for seeding")
            .insert((graph, second_key), value)
            .expect("insert seed row");
        write.finish().expect("finish");
        shard.commit_drain(group, &batches, 1).expect("commit");
    }

    /// Write G graphs (each with nodes + an edge) through a K=1 backend, durably.
    async fn seed_k1(dir: &str, graphs: &[&str]) {
        seed_at_k(dir, 1, graphs).await;
    }

    /// Seed `graphs.len()` graphs (2 nodes + 1 edge each) through a backend opened at
    /// an EXPLICIT shard count `k`.
    async fn seed_at_k(dir: &str, k: usize, graphs: &[&str]) {
        let backend = RedbBackend::open_with_shards(dir.to_string(), 256, k)
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
        let root = temp_root("rt");
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

        for i in 0..4 {
            assert!(
                dst.join(format!("graph-{i}.redb")).exists(),
                "graph-{i}.redb"
            );
        }

        // ── reopen at K=4 and verify each graph routes + reads back intact ──
        let dst_s = dst.to_string_lossy().to_string();
        let backend = RedbBackend::open(dst_s.clone(), 256).expect("reopen K=4");
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

    /// CONCEPT:EG-KG.sharding.atomic-shard-swap — the in-place migration swaps shard files atomically and leaves the
    /// old files aside; reopening picks up the new K.
    #[tokio::test(flavor = "multi_thread")]
    async fn in_place_migration_swaps_and_backs_up() {
        // See `roundtrip_k1_to_k4_preserves_all_graphs` above: held for the whole
        // test (this one also reopens after an in-place migration + backup).
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = temp_root("inplace");
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
        assert!(has_backup, "the old shard files were moved aside");

        // Reopen in place at K=4 and confirm all graphs are reachable.
        let backend = RedbBackend::open(dir_s.clone(), 256).expect("reopen");
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

    /// A partial destination is an accounted recovery artifact.  A retry must
    /// refuse it rather than deleting the only remaining copy of any graph
    /// whose source graft may already have retired its snapshot scope.
    #[test]
    fn interrupted_in_place_build_is_preserved_for_recovery() {
        // Every store this test builds or reopens resolves the value cipher from
        // the process-global encryption env vars, and `cargo test` runs the whole
        // crate concurrently. Without this lock an unrelated test's transient
        // set_var/remove_var lands between two of those resolutions and flips the
        // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
        // sibling in-place migration tests already hold it; these did not.
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        let dir = temp_root("inplace-preserve");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        empty_shard(&dir.join("graph-0.redb"));
        let tmp = dir.join(".shard-migrate-tmp");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("partial-copy"), b"accounted partial destination").unwrap();

        let error = migrate_in_place(&dir.to_string_lossy(), 2).unwrap_err();
        assert!(error.contains("preserving"), "{error}");
        assert!(tmp.join("partial-copy").exists());
        assert!(dir.join("graph-0.redb").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A failure after one graph has completed Phase C must not strand the live
    /// source in a partially retired state. In-place migration builds from the
    /// copied working snapshot, so the original K=1 file and immutable backup
    /// remain complete while the partial destination is retained for recovery.
    #[tokio::test(flavor = "multi_thread")]
    async fn in_place_fault_after_first_graft_keeps_live_source_and_backup() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = temp_root("inplace-fault-after-graft");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();
        let graphs = ["fault-one", "fault-two"];
        seed_k1(&dir_s, &graphs).await;

        let error = migrate_in_place_after_graph_for_test(&dir_s, 2, 1).unwrap_err();
        assert!(
            error.contains("injected migration fault after 1 graph graft"),
            "{error}"
        );
        assert!(error.contains("source snapshot"), "{error}");
        assert!(dir.join("graph-0.redb").exists(), "live source was removed");
        assert!(dir.join(".shard-migrate-tmp").exists());
        assert!(!dir
            .join(".shard-migrate-tmp")
            .join(IN_PLACE_READY_MARKER)
            .exists());

        // The original source remains a complete, usable store after the first
        // graft has retired only the working copy.
        let backend = RedbBackend::open(dir_s.clone(), 256).expect("live source remains usable");
        for graph in &graphs {
            assert!(
                backend.read_graph_dump_blocking(graph).unwrap().is_some(),
                "live source lost graph {graph}"
            );
        }
        backend.shutdown();

        let backup = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".shard-migrate-backup-"))
            })
            .expect("immutable migration backup");
        assert!(backup.join("source").join("graph-0.redb").exists());

        // "Usable" is proved the way an operator actually recovers from this
        // tree, not by opening it where it lies (eg-f3 burndown, 2026-09-11).
        //
        // A store's physical root is `(dev, ino)`-derived and checked on every
        // open, so a byte copy cannot be served at a new path until it is
        // rebound -- and `rebind_copied_store` REWRITES the file it is given.
        // Opening `.shard-migrate-backup-*/source` in place therefore had only
        // two outcomes: refuse with "mutation store root incarnation mismatch"
        // (what this test hit), or mutate the evidence. `migrate_in_place_inner`
        // records that rebinding the snapshot in place was tried and regressed
        // four tests, because something downstream depends on the snapshot still
        // carrying the ORIGINAL root -- so the evidence stays pristine, by
        // design, and the recovery step is: copy it out, rebind the COPY against
        // the original it was taken from, open the copy.
        //
        // That is what is exercised here. The assertion is not weakened: it still
        // proves both graphs are fully readable out of the preserved backup, and
        // it additionally proves the evidence survives the read unmodified.
        let evidence = backup.join("source");
        let evidence_before: Vec<(std::path::PathBuf, Vec<u8>)> = std::fs::read_dir(&evidence)
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "redb"))
            .map(|path| {
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
        assert!(
            !evidence_before.is_empty(),
            "the preserved backup must contain at least one store file"
        );
        let recovery = dir.join(".recovery-open");
        let _ = std::fs::remove_dir_all(&recovery);
        std::fs::create_dir_all(&recovery).unwrap();
        for (path, _) in &evidence_before {
            let name = path.file_name().unwrap();
            std::fs::copy(path, recovery.join(name)).unwrap();
            // The snapshot is a plain byte copy of the live source, so it still
            // carries that store's root -- which is what `copied_from` must name.
            eg_storage::rebind_copied_store(&recovery.join(name), &dir.join(name))
                .expect("rebind the recovery copy of the immutable backup");
        }
        let backup_backend = RedbBackend::open(recovery.to_string_lossy().to_string(), 256)
            .expect("immutable source backup remains usable");
        for graph in &graphs {
            assert!(
                backup_backend
                    .read_graph_dump_blocking(graph)
                    .unwrap()
                    .is_some(),
                "immutable backup lost graph {graph}"
            );
        }
        backup_backend.shutdown();
        drop(backup_backend);
        for (path, bytes) in &evidence_before {
            assert_eq!(
                &std::fs::read(path).unwrap(),
                bytes,
                "reading the backup must leave the evidence byte-identical: {}",
                path.display()
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fault after the first live source file has moved must be resumable:
    /// the immutable target manifest and the old-file backup identify exactly
    /// which side of the rename already completed.
    #[tokio::test(flavor = "multi_thread")]
    async fn in_place_fault_after_old_move_resumes_without_losing_graphs() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = temp_root("inplace-fault-after-old-move");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();
        let graphs = ["old-move-one", "old-move-two"];
        seed_k1(&dir_s, &graphs).await;

        let error = migrate_in_place_after_old_move_for_test(&dir_s, 2, 1).unwrap_err();
        assert!(
            error.contains("injected migration fault after 1 old shard move"),
            "{error}"
        );
        assert!(
            !dir.join("graph-0.redb").exists(),
            "old source was not moved"
        );
        assert!(
            dir.join(".shard-migrate-tmp")
                .join(IN_PLACE_READY_MARKER)
                .exists(),
            "ready marker must survive the fault"
        );

        migrate_in_place(&dir_s, 2).expect("retry after old-file move");
        let backend = RedbBackend::open(dir_s.clone(), 256).expect("reopen resumed migration");
        for graph in &graphs {
            assert!(
                backend.read_graph_dump_blocking(graph).unwrap().is_some(),
                "graph {graph} lost after old-file retry"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fault after installing the first destination target must not move that
    /// authenticated target back into the old-file backup on retry.
    #[tokio::test(flavor = "multi_thread")]
    async fn in_place_fault_after_first_install_resumes_idempotently() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = temp_root("inplace-fault-after-install");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();
        let graphs = ["install-one", "install-two"];
        seed_k1(&dir_s, &graphs).await;

        let error = migrate_in_place_after_install_for_test(&dir_s, 2, 1).unwrap_err();
        assert!(
            error.contains("injected migration fault after 1 target install"),
            "{error}"
        );
        assert!(
            dir.join("graph-0.redb").exists(),
            "first target was not installed"
        );
        assert!(
            dir.join(".shard-migrate-tmp")
                .join(IN_PLACE_READY_MARKER)
                .exists(),
            "ready marker must survive the fault"
        );

        migrate_in_place(&dir_s, 2).expect("retry after target install");
        let backend = RedbBackend::open(dir_s.clone(), 256).expect("reopen resumed migration");
        for graph in &graphs {
            assert!(
                backend.read_graph_dump_blocking(graph).unwrap().is_some(),
                "graph {graph} lost after target-install retry"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The readiness marker is bound to the target K.  A retry with a changed
    /// target set must refuse before renaming either the live source or a
    /// staged target.
    #[tokio::test(flavor = "multi_thread")]
    async fn in_place_changed_target_k_refuses_before_swap() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = temp_root("inplace-changed-target-k");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();
        let graphs = ["changed-k-one", "changed-k-two"];
        seed_k1(&dir_s, &graphs).await;

        let source = dir.join("graph-0.redb");
        let marker = dir.join(".shard-migrate-tmp").join(IN_PLACE_READY_MARKER);
        let source_before = file_sha256(&source).unwrap();
        assert!(migrate_in_place_before_swap_for_test(&dir_s, 2)
            .unwrap_err()
            .contains("injected migration fault before swap"));
        let marker_before = file_sha256(&marker).unwrap();

        let error = migrate_in_place(&dir_s, 1).unwrap_err();
        assert!(
            error.contains("target K 2 does not match requested K 1"),
            "{error}"
        );
        assert_eq!(file_sha256(&source).unwrap(), source_before);
        assert_eq!(file_sha256(&marker).unwrap(), marker_before);
        assert!(source.exists(), "changed-K refusal moved the live source");
        assert!(
            marker.exists(),
            "changed-K refusal removed the ready marker"
        );

        let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, 1)
            .expect("live source remains readable after changed-K refusal");
        for graph in &graphs {
            assert!(
                backend.read_graph_dump_blocking(graph).unwrap().is_some(),
                "graph {graph} lost during changed-K refusal"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The sole reader for the retired unindexed K=1 FILENAME layout is this explicit
    /// offline migration path. Even a K=1 target is rewritten to canonical
    /// `graph-0.redb`.
    #[test]
    fn retired_k1_layout_migrates_to_canonical_k1() {
        // Every store this test builds or reopens resolves the value cipher from
        // the process-global encryption env vars, and `cargo test` runs the whole
        // crate concurrently. Without this lock an unrelated test's transient
        // set_var/remove_var lands between two of those resolutions and flips the
        // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
        // sibling in-place migration tests already hold it; these did not.
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        let dir = temp_root("retired-k1");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        empty_shard(&dir.join("graph.redb"));

        let report = migrate_in_place(&dir.to_string_lossy(), 1).unwrap();
        assert_eq!(report.source_shards, 1);
        assert_eq!(report.dest_shards, 1);
        assert!(!dir.join("graph.redb").exists());
        assert!(dir.join("graph-0.redb").exists());

        let backend = RedbBackend::open_with_shards(dir.to_string_lossy().to_string(), 64, 1)
            .expect("canonical migrated layout reopens");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn migration_discovery_rejects_mixed_and_sparse_layouts() {
        // Every store this test builds or reopens resolves the value cipher from
        // the process-global encryption env vars, and `cargo test` runs the whole
        // crate concurrently. Without this lock an unrelated test's transient
        // set_var/remove_var lands between two of those resolutions and flips the
        // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
        // sibling in-place migration tests already hold it; these did not.
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        let root = temp_root("invalid-layout");
        let mixed = root.join("mixed");
        let sparse = root.join("sparse");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&mixed).unwrap();
        std::fs::create_dir_all(&sparse).unwrap();
        empty_shard(&mixed.join("graph.redb"));
        empty_shard(&mixed.join("graph-0.redb"));
        empty_shard(&sparse.join("graph-0.redb"));
        empty_shard(&sparse.join("graph-2.redb"));

        let mixed_err = discover_source_shards(&mixed).unwrap_err();
        assert!(
            mixed_err.contains("mixed retired and current"),
            "{mixed_err}"
        );
        let sparse_err = discover_source_shards(&sparse).unwrap_err();
        assert!(sparse_err.contains("non-contiguous"), "{sparse_err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Re-pinned from `migration_routes_mutation_and_change_authority_with_its_graph`:
    /// the eight private `mutation_*` tables it planted rows in are retired, and the
    /// replay/outbox/version state they held is now the KERNEL ledger, moved by the
    /// graft. The property is unchanged and the assertions are stronger — instead of
    /// counting copied rows, this reads the moved authority back:
    ///
    /// * the governed ChangeEnvelope rows arrive with their graph, and
    /// * the graph's own receipt is readable at the destination at the SOURCE's
    ///   marker-inclusive version, not a fresh re-admission, and
    /// * the source no longer serves the graph — a migration is a MOVE.
    #[test]
    fn migration_moves_change_authority_and_the_kernel_ledger_with_its_graph() {
        let root = temp_root("aux");
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        let source_path = src.join("graph-0.redb");
        let (batch_id, source_version) = seed_graph(&source_path, "aux-graph", "aux");

        let report = migrate_shards(&src, &dst, 4).unwrap();
        assert_eq!(report.graphs, 1);

        let home = dst.join(format!("graph-{}.redb", shard_index("aux-graph", 4)));
        assert_eq!(
            graph_row_count(&home, "aux-graph", CHANGE_ENVELOPES),
            1,
            "change_envelopes moved with its graph"
        );
        assert_eq!(graph_row_count(&home, "aux-graph", CONTENT_VERSIONS), 1);
        assert_eq!(graph_row_count(&home, "aux-graph", CHANGE_CURSORS), 1);

        let (version, receipt) = graph_ledger(&home, "aux-graph", &batch_id);
        assert!(receipt, "the graph's receipt moved to the destination");
        assert_eq!(
            version,
            source_version + 1,
            "the destination serves the SOURCE's marker-inclusive version"
        );

        // The move consumed the source: a fresh binding of the same name finds nothing.
        assert_eq!(
            graph_row_count(&source_path, "aux-graph", CHANGE_ENVELOPES),
            0
        );
        let (source_after, receipt_after) = graph_ledger(&source_path, "aux-graph", &batch_id);
        assert_eq!(source_after, 0, "the source scope was retired");
        assert!(!receipt_after, "the source no longer holds the receipt");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Refuses to clobber an existing destination shard file.
    #[test]
    fn refuses_existing_destination() {
        // Every store this test builds or reopens resolves the value cipher from
        // the process-global encryption env vars, and `cargo test` runs the whole
        // crate concurrently. Without this lock an unrelated test's transient
        // set_var/remove_var lands between two of those resolutions and flips the
        // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
        // sibling in-place migration tests already hold it; these did not.
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        let dir = temp_root("clobber");
        let _ = std::fs::remove_dir_all(&dir);
        let src = dir.join("src");
        let dst = dir.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&dst).unwrap();
        // a canonical source shard, and a pre-existing destination graph-0.redb
        empty_shard(&src.join("graph-0.redb"));
        empty_shard(&dst.join("graph-0.redb"));
        let err = migrate_shards(&src, &dst, 4).unwrap_err();
        assert!(err.contains("already exists"), "got: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A GENUINE multi-source-shard layout (K=2, not K=1) round-tripped through
    /// `migrate_shards`. Every other backend-driven test starts from K=1 (one source
    /// file); this is the only proof that the loop over MULTIPLE source shards is
    /// correct.
    #[tokio::test(flavor = "multi_thread")]
    async fn multi_source_migration_preserves_graphs() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = temp_root("multisrc");
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
        let backend = RedbBackend::open(dst_s.clone(), 256).expect("reopen K=3");
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

    /// Re-pinned from `mutation_batch_chain_routes_independently_per_source_shard`:
    /// the `routed_batch_ids` set that test guarded is gone with the private
    /// batch-addressed tables, but the property it protected is not. TWO source shard
    /// files each carry their own graph and their own ledger, both graphs route to the
    /// SAME destination (K=1), and each must arrive with ITS OWN receipt and version —
    /// never the other's, and never one of them dropped.
    #[test]
    fn two_source_shards_move_their_own_ledgers_independently() {
        let root = temp_root("multisrc-ledger");
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();

        let (alpha_batch, alpha_version) = seed_graph(&src.join("graph-0.redb"), "alpha", "alpha");
        let (beta_batch, beta_version) = seed_graph(&src.join("graph-1.redb"), "beta", "beta");

        let report = migrate_shards(&src, &dst, 1).expect("migrate K=2 -> K=1");
        assert_eq!(report.source_shards, 2);
        assert_eq!(report.graphs, 2);

        let home = dst.join("graph-0.redb");
        let (alpha_at_dest, alpha_receipt) = graph_ledger(&home, "alpha", &alpha_batch);
        assert!(alpha_receipt, "alpha's own receipt arrived");
        assert_eq!(
            alpha_at_dest,
            alpha_version + 1,
            "alpha's ledger version plus the graft marker survived the move"
        );
        let (beta_at_dest, beta_receipt) = graph_ledger(&home, "beta", &beta_batch);
        assert!(beta_receipt, "beta's own receipt arrived");
        assert_eq!(
            beta_at_dest,
            beta_version + 1,
            "beta's ledger version plus the graft marker survived the move"
        );

        // Neither source's ledger leaked into the other's scope.
        assert!(
            !graph_ledger(&home, "alpha", &beta_batch).1,
            "beta's receipt is readable inside alpha's scope"
        );
        assert!(
            !graph_ledger(&home, "beta", &alpha_batch).1,
            "alpha's receipt is readable inside beta's scope"
        );

        // Each graph's owner rows came with it, from its own source file.
        assert_eq!(graph_row_count(&home, "alpha", CHANGE_ENVELOPES), 1);
        assert_eq!(graph_row_count(&home, "beta", CHANGE_ENVELOPES), 1);

        let _ = std::fs::remove_dir_all(&root);
    }

    /// BUG-CX-016, FIXED: `provenance_anchor_members` (scope-prefixed, feature
    /// `security`) and `plan_matviews` / `matview_operator_state` (file-wide,
    /// `shard0()`-homed, feature `matview`) live in the SAME `graph-<n>.redb` shard
    /// files as `nodes`/`audit_chain`. `migrate_shards` routes all three, so a K-shard
    /// migration carries them across exactly like `nodes`/`audit_chain` do. `NODES`
    /// stays asserted as the differential control. Confirmed FAILING before the fix
    /// (`anchors`/`plan_matviews`/`matview_state` were all `0`).
    #[tokio::test(flavor = "multi_thread")]
    async fn migration_preserves_provenance_anchor_and_matview_state() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = temp_root("dropped-tables");
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let backend = RedbBackend::open(src_s.clone(), 256).expect("open K=1 backend");
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
        // otherwise their absence downstream would prove nothing about migrate_shards.
        let source_path = src.join("graph-0.redb");
        #[cfg(feature = "security")]
        assert_eq!(
            graph_row_count(&source_path, "g", PROVENANCE_ANCHOR_MEMBERS),
            1,
            "source has the provenance anchor row pre-migration"
        );
        #[cfg(feature = "matview")]
        {
            assert_eq!(
                control_row_count(&source_path, PLAN_MATVIEWS),
                1,
                "source has the plan matview row pre-migration"
            );
            assert_eq!(
                control_row_count(&source_path, MATVIEW_OPERATOR_STATE),
                1,
                "source has the matview operator-state row pre-migration"
            );
        }
        assert_eq!(
            graph_row_count(&source_path, "g", NODES),
            1,
            "source has the node pre-migration"
        );

        let report = migrate_shards(&src, &dst, 2).expect("migrate K=1 -> K=2");
        assert_eq!(report.graphs, 1);
        assert_eq!(report.nodes, 1);

        let home = dst.join(format!("graph-{}.redb", shard_index("g", 2)));
        assert_eq!(
            graph_row_count(&home, "g", NODES),
            1,
            "node survives the migration (known-good table)"
        );
        #[cfg(feature = "security")]
        assert_eq!(
            graph_row_count(&home, "g", PROVENANCE_ANCHOR_MEMBERS),
            1,
            "FIXED (BUG-CX-016): provenance_anchor_members survives migrate_shards"
        );
        #[cfg(feature = "matview")]
        {
            // File-wide and shard0()-homed, so they land on destination shard 0
            // regardless of where the graph routed.
            assert_eq!(
                control_row_count(&dst.join("graph-0.redb"), PLAN_MATVIEWS),
                1,
                "FIXED (BUG-CX-016): plan_matviews survives migrate_shards"
            );
            assert_eq!(
                control_row_count(&dst.join("graph-0.redb"), MATVIEW_OPERATOR_STATE),
                1,
                "FIXED (BUG-CX-016): matview_operator_state survives migrate_shards"
            );
        }

        let _ = std::fs::remove_dir_all(&root);
    }

    /// BUG-CX-054 (RESOURCE_*/development_lane/NATIVE_WORK_ITEMS) plus the two
    /// undocumented gaps in the SAME class (capacity_lease, the remaining
    /// work_item_capability tables): one representative table per subsystem, seeded
    /// directly through the admitted write path (bypassing the request APIs those
    /// subsystems would otherwise require), migrated K=1 -> K=2, and confirmed present
    /// afterward. Each of these tables is exercised individually by the type checker (a
    /// key-shape or table-name transcription error fails to compile), but a wrong FIELD
    /// ORDER within a correctly-typed tuple would still compile — this test is the
    /// semantic check compilation cannot provide, across all four subsystems in one
    /// migration run.
    #[tokio::test(flavor = "multi_thread")]
    async fn migration_preserves_resource_lane_capacity_and_capability_tables() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = temp_root("cx054-tables");
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let backend = RedbBackend::open(src_s.clone(), 256).expect("open K=1 backend");
        backend
            .register_graph("g", "g", GraphType::Global)
            .await
            .expect("register");
        backend.shutdown();

        let shard0 = src.join("graph-0.redb");
        seed_owner_row(&shard0, RESOURCE_RESERVATIONS, "g", "r1", b"reservation");
        seed_owner_row(&shard0, development_lane::HOLDS, "g", "h1", b"hold");
        seed_owner_row(&shard0, capacity_lease::CELLS, "g", "c1", b"cell");
        seed_owner_row(
            &shard0,
            work_item_capability::CAPABILITIES,
            "g",
            "digest1",
            b"capability",
        );
        seed_owner_row(
            &shard0,
            work_item_capability::NATIVE_WORK_ITEMS,
            "g",
            "wi1",
            b"native-work-item",
        );

        // Sanity: every seeded row landed in the source before migration.
        assert_eq!(graph_row_count(&shard0, "g", RESOURCE_RESERVATIONS), 1);
        assert_eq!(graph_row_count(&shard0, "g", development_lane::HOLDS), 1);
        assert_eq!(graph_row_count(&shard0, "g", capacity_lease::CELLS), 1);
        assert_eq!(
            graph_row_count(&shard0, "g", work_item_capability::CAPABILITIES),
            1
        );
        assert_eq!(
            graph_row_count(&shard0, "g", work_item_capability::NATIVE_WORK_ITEMS),
            1
        );

        let report = migrate_shards(&src, &dst, 2).expect("migrate K=1 -> K=2");
        assert_eq!(report.graphs, 1);
        assert_eq!(
            report.capability_and_resource, 5,
            "all 5 seeded rows counted under the coverage bucket"
        );

        let home = dst.join(format!("graph-{}.redb", shard_index("g", 2)));
        assert_eq!(
            graph_row_count(&home, "g", RESOURCE_RESERVATIONS),
            1,
            "resource_reservations survives migrate_shards (BUG-CX-054)"
        );
        assert_eq!(
            graph_row_count(&home, "g", development_lane::HOLDS),
            1,
            "development_lane_holds survives migrate_shards (BUG-CX-054)"
        );
        assert_eq!(
            graph_row_count(&home, "g", capacity_lease::CELLS),
            1,
            "capacity_cells survives migrate_shards (undocumented gap, same class as BUG-CX-054)"
        );
        assert_eq!(
            graph_row_count(&home, "g", work_item_capability::CAPABILITIES),
            1,
            "work_item_claim_capabilities survives migrate_shards (undocumented gap, same class as BUG-CX-054)"
        );
        assert_eq!(
            graph_row_count(&home, "g", work_item_capability::NATIVE_WORK_ITEMS),
            1,
            "native_work_item_authority survives migrate_shards (BUG-CX-054)"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}
