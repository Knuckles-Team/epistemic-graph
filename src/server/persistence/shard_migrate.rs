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

use super::redb_backend::{shard_index, RAFT_META};
#[cfg(feature = "security")]
use super::redb_backend::{ENCRYPTION_CANARY, ENCRYPTION_KEY_BINDING_KEY};
use crate::redb_layout::{
    discover_indexed_shards, retired_single_shard, shard_filename, validate_shard_count,
};
use crate::redb_store::shard::{Shard, ShardWrite};
#[cfg(feature = "compute-dist")]
use crate::redb_store::MATVIEWS;
use crate::redb_store::{GRAPH_META, RAFT_LOG, XSHARD_DECISION, XSHARD_PREPARE};
#[cfg(feature = "matview")]
use crate::redb_store::{MATVIEW_OPERATOR_STATE, PLAN_MATVIEWS};
use eg_storage::{GraphShardOwner, ScopedRead};
use eg_transaction::OwnerPayloadWrite;
use redb::{ReadableTable, TableDefinition};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

mod inplace;
mod payload;
mod readiness;
mod swap;
pub use inplace::migrate_in_place;
pub(crate) use payload::GraphShardPayloadTransfer;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationFault {
    AfterGraph(usize),
    BeforeSwap,
    AfterOldMove(usize),
    AfterInstall(usize),
}

#[cfg(test)]
mod tests;
