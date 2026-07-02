//! Online consistent BACKUP + RESTORE + PITR foundation (CONCEPT:EG-090).
//!
//! ## What it solves
//!
//! EG-030 (`shard_migrate`) can copy a durable store verbatim, but only OFFLINE — the
//! engine must be stopped because it opens each `graph.redb` with `Database::open`
//! (an exclusive per-file lock). A disaster-recovery story needs a **consistent backup
//! taken while the engine RUNS**, and a matching restore. This is that.
//!
//! ## Online consistent backup — no quiesce
//!
//! [`RedbBackend::backup`](super::redb_backend::RedbBackend::backup) takes, PER SHARD, a
//! `Database::begin_read()` MVCC snapshot (CONCEPT:EG-027) on the LIVE writer's shared
//! handle — the same snapshot mechanism the read-through path uses. redb 4.1 is MVCC, so
//! that snapshot sees the shard's LATEST COMMITTED state and runs CONCURRENTLY with the
//! single writer (no writer involvement, no group-commit, no quiesce). Every table is
//! then streamed **verbatim** into a fresh bundle shard file, reusing EG-030's raw-row
//! copy: value blobs are copied byte-for-byte, so
//!
//! * encryption-at-rest blobs survive WITHOUT the key (no decrypt), and
//! * the tamper-evident hash-chained `AUDIT` log (CONCEPT:KG-2.231) stays verifiable
//!   (re-deriving it would break verification; copying preserves it).
//!
//! **Cross-shard consistency** rides the commit-before-ack guarantee (CONCEPT:KG-2.187):
//! any ACKED write is already durably committed, so each per-shard snapshot — opened
//! independently — sees a self-consistent committed prefix of the durable history. There
//! is no global stop-the-world; the bundle is a crash-consistent point-in-time image.
//!
//! ## Bundle format — a portable shard set + manifest
//!
//! A backup bundle is a directory holding:
//!
//! * `graph.redb` (K=1) or `graph-<n>.redb` (K>1) — one verbatim redb file per shard,
//!   using the EG-026 [`shard_filename`](super::redb_backend::shard_filename) names, so
//!   the bundle IS a valid durable shard set on its own.
//! * `MANIFEST.json` — [`BackupManifest`]: format version, engine version, shard count K,
//!   caller-supplied timestamp + label, and the copied row totals.
//!
//! ## Restore — verbatim import, re-shard-on-restore
//!
//! [`restore_bundle`] validates the manifest, then rebuilds a persist-dir from the bundle
//! by DELEGATING to EG-030's [`shard_migrate::migrate_shards`] — the bundle's shard files
//! are exactly the `graph.redb`/`graph-<n>.redb` set that tool consumes. Restoring at the
//! manifest's own K is a 1:1 verbatim row import; restoring at a DIFFERENT K re-shards on
//! restore (each graph re-routed by the SAME EG-026 `FNV-1a % K`). No decode/re-derive —
//! the audit chain and at-rest ciphertext survive the round trip.
//!
//! ## Point-in-time recovery (PITR)
//!
//! The bundle plus the durable ledger/WAL tail are the low-RPO/RTO DR primitives:
//! restore the latest bundle, then replay the durable ledger tail forward to a target
//! instant. The replay-to-timestamp mechanism is documented in `docs/deployment.md`
//! ("Point-in-time recovery"); this module builds the backup + restore halves it rides.

use std::path::Path;

use redb::{Database, Durability, ReadableDatabase, ReadableTable};

#[cfg(feature = "compute-dist")]
use crate::redb_store::MATVIEWS;
use crate::redb_store::{
    AUDIT, EDGES, GRAPH_META, LEDGER, NODES, RAFT_LOG, SEMANTIC, XSHARD_DECISION, XSHARD_PREPARE,
};
use crate::server::persistence::redb_backend::RAFT_META;
use crate::server::persistence::shard_migrate;

/// The bundle manifest file name.
pub const MANIFEST_FILE: &str = "MANIFEST.json";

/// The backup bundle on-disk format version. Bumped only on an incompatible layout
/// change; `restore_bundle` refuses a newer format than it understands.
pub const BUNDLE_FORMAT_VERSION: u32 = 1;

/// Row totals copied for ONE shard (CONCEPT:EG-090) — the per-shard slice of a
/// [`BackupReport`]. Mirrors the dimensions EG-030's `MigrationReport` tracks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShardCounts {
    /// Distinct graphs (rows in `GRAPH_META`) in this shard.
    pub graphs: u64,
    /// Node rows.
    pub nodes: u64,
    /// Edge rows.
    pub edges: u64,
    /// Ledger rows.
    pub ledger: u64,
    /// Semantic-store rows.
    pub semantic: u64,
    /// Audit-chain rows (verbatim — chain preserved).
    pub audit: u64,
    /// Global rows (raft log/meta + 2PC + matviews) — non-zero only for shard 0.
    pub global: u64,
}

impl std::ops::AddAssign for ShardCounts {
    fn add_assign(&mut self, o: Self) {
        self.graphs += o.graphs;
        self.nodes += o.nodes;
        self.edges += o.edges;
        self.ledger += o.ledger;
        self.semantic += o.semantic;
        self.audit += o.audit;
        self.global += o.global;
    }
}

/// Outcome of a backup run (CONCEPT:EG-090) — the shard count + copied totals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackupReport {
    /// Number of shard files written into the bundle (= K).
    pub shards: usize,
    /// Distinct graphs across all shards.
    pub graphs: u64,
    /// Node rows copied.
    pub nodes: u64,
    /// Edge rows copied.
    pub edges: u64,
    /// Ledger rows copied.
    pub ledger: u64,
    /// Semantic-store rows copied.
    pub semantic: u64,
    /// Audit-chain rows copied.
    pub audit: u64,
    /// Global rows copied (shard-0 raft/2PC/matviews).
    pub global: u64,
}

impl BackupReport {
    /// Fold one shard's counts into the running totals.
    pub fn add_shard(&mut self, c: ShardCounts) {
        self.graphs += c.graphs;
        self.nodes += c.nodes;
        self.edges += c.edges;
        self.ledger += c.ledger;
        self.semantic += c.semantic;
        self.audit += c.audit;
        self.global += c.global;
    }
}

/// The bundle manifest (CONCEPT:EG-090) — serialized to `MANIFEST.json` at backup and
/// validated at restore. All non-derived fields (timestamp, label, engine version) are
/// CALLER-SUPPLIED — this module never calls `Date::now` (no wall-clock / randomness in
/// library code).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupManifest {
    /// On-disk bundle format version ([`BUNDLE_FORMAT_VERSION`]).
    pub format_version: u32,
    /// Engine version that produced the bundle (caller-supplied, e.g. `CARGO_PKG_VERSION`).
    pub engine_version: String,
    /// Shard count K — the number of `graph*.redb` files in the bundle.
    pub shard_count: usize,
    /// Caller-supplied Unix-seconds timestamp of the backup.
    pub timestamp: u64,
    /// Caller-supplied human label (may be empty).
    pub label: String,
    /// Distinct graphs captured.
    pub graphs: u64,
    /// Node rows captured.
    pub nodes: u64,
    /// Edge rows captured.
    pub edges: u64,
    /// Ledger rows captured.
    pub ledger: u64,
    /// Semantic-store rows captured.
    pub semantic: u64,
    /// Audit-chain rows captured.
    pub audit: u64,
    /// Global rows captured.
    pub global: u64,
}

impl BackupManifest {
    fn from_report(
        report: &BackupReport,
        engine_version: &str,
        timestamp: u64,
        label: &str,
    ) -> Self {
        Self {
            format_version: BUNDLE_FORMAT_VERSION,
            engine_version: engine_version.to_string(),
            shard_count: report.shards,
            timestamp,
            label: label.to_string(),
            graphs: report.graphs,
            nodes: report.nodes,
            edges: report.edges,
            ledger: report.ledger,
            semantic: report.semantic,
            audit: report.audit,
            global: report.global,
        }
    }
}

/// Copy EVERY durable table from a source read snapshot into a fresh destination write
/// txn, VERBATIM (CONCEPT:EG-090). Unlike EG-030's migration (which routes rows by
/// `shard_index`), backup mirrors ONE shard 1:1 — the source snapshot already holds
/// exactly the rows that shard owns, so no routing/filter is applied. `is_shard0`
/// includes the global (non-per-graph) tables, which live on shard 0 only (EG-026).
///
/// Value blobs are copied byte-for-byte (no decode/unseal), so encryption-at-rest blobs
/// and the KG-2.231 hash-chained audit log survive without the key and stay verifiable.
pub(crate) fn copy_snapshot_verbatim(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    is_shard0: bool,
) -> Result<ShardCounts, String> {
    let mut counts = ShardCounts::default();

    // Open (create) every per-graph table on the destination so the bundle file matches
    // what `Shard::open` / `migrate_shards` expect.
    let mut d_nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let mut d_edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let mut d_ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let mut d_semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    let mut d_meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let mut d_audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;

    if let Ok(t) = rtx.open_table(GRAPH_META) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_meta.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            counts.graphs += 1;
        }
    }
    if let Ok(t) = rtx.open_table(NODES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_nodes.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            counts.nodes += 1;
        }
    }
    if let Ok(t) = rtx.open_table(EDGES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_edges.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            counts.edges += 1;
        }
    }
    if let Ok(t) = rtx.open_table(LEDGER) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_ledger.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            counts.ledger += 1;
        }
    }
    if let Ok(t) = rtx.open_table(SEMANTIC) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_semantic.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            counts.semantic += 1;
        }
    }
    // Audit — verbatim to keep the hash chain verifiable (CONCEPT:KG-2.231).
    if let Ok(t) = rtx.open_table(AUDIT) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_audit.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            counts.audit += 1;
        }
    }

    if is_shard0 {
        counts.global += copy_global_verbatim(rtx, wtx)?;
    }
    Ok(counts)
}

/// Copy the GLOBAL (non-per-graph) tables verbatim — the Raft log/meta, the cross-shard
/// 2PC records, and the materialized views (CONCEPT:EG-090). These are EG-026 "shard 0
/// home" records, captured only from the shard-0 snapshot.
fn copy_global_verbatim(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
) -> Result<u64, String> {
    let mut count = 0u64;
    let mut d_raft_log = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut d_raft_meta = wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
    let mut d_xprep = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
    let mut d_xdec = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
    #[cfg(feature = "compute-dist")]
    let mut d_matviews = wtx.open_table(MATVIEWS).map_err(|e| e.to_string())?;

    if let Ok(t) = rtx.open_table(RAFT_LOG) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_raft_log.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    if let Ok(t) = rtx.open_table(RAFT_META) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_raft_meta.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    if let Ok(t) = rtx.open_table(XSHARD_PREPARE) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_xprep.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    if let Ok(t) = rtx.open_table(XSHARD_DECISION) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_xdec.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    #[cfg(feature = "compute-dist")]
    if let Ok(t) = rtx.open_table(MATVIEWS) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_matviews.insert(k.value(), v.value()).map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    Ok(count)
}

/// Write ONE shard's `begin_read()` snapshot verbatim into `dst_path` (a fresh bundle
/// redb file) and return the copied counts (CONCEPT:EG-090). Called once per shard by
/// [`RedbBackend::backup`](super::redb_backend::RedbBackend::backup).
pub(crate) fn write_bundle_shard(
    src_db: &Database,
    dst_path: &Path,
    is_shard0: bool,
) -> Result<ShardCounts, String> {
    if dst_path.exists() {
        return Err(format!(
            "bundle shard file already exists: {} (refusing to overwrite)",
            dst_path.display()
        ));
    }
    let rtx = src_db.begin_read().map_err(|e| e.to_string())?;
    let dst_db =
        Database::create(dst_path).map_err(|e| format!("create {}: {e}", dst_path.display()))?;
    let mut wtx = dst_db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let counts = copy_snapshot_verbatim(&rtx, &wtx, is_shard0)?;
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(counts)
}

/// Serialize + write the bundle manifest to `<dir>/MANIFEST.json` (CONCEPT:EG-090).
pub(crate) fn write_manifest(
    dir: &Path,
    report: &BackupReport,
    engine_version: &str,
    timestamp: u64,
    label: &str,
) -> Result<BackupManifest, String> {
    let manifest = BackupManifest::from_report(report, engine_version, timestamp, label);
    let json = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
    std::fs::write(dir.join(MANIFEST_FILE), json).map_err(|e| e.to_string())?;
    Ok(manifest)
}

/// Read + validate a bundle manifest from `<dir>/MANIFEST.json` (CONCEPT:EG-090).
pub fn read_manifest(dir: &Path) -> Result<BackupManifest, String> {
    let path = dir.join(MANIFEST_FILE);
    let bytes = std::fs::read(&path)
        .map_err(|e| format!("read bundle manifest {}: {e}", path.display()))?;
    let manifest: BackupManifest =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse bundle manifest: {e}"))?;
    if manifest.format_version > BUNDLE_FORMAT_VERSION {
        return Err(format!(
            "bundle format version {} is newer than this build understands ({})",
            manifest.format_version, BUNDLE_FORMAT_VERSION
        ));
    }
    Ok(manifest)
}

/// Outcome of a restore (CONCEPT:EG-090) — the validated manifest + the verbatim import
/// totals produced by the EG-030 migration engine.
#[derive(Debug, Clone)]
pub struct RestoreReport {
    /// The bundle's validated manifest.
    pub manifest: BackupManifest,
    /// Shard count the persist-dir was rebuilt at (manifest K unless overridden).
    pub restored_shards: usize,
    /// Verbatim row-import totals (EG-030 `MigrationReport`).
    pub migration: shard_migrate::MigrationReport,
}

/// Rebuild a persist-dir from a backup bundle (CONCEPT:EG-090). Validates the manifest,
/// then verbatim-imports every bundle shard into `persist_dir` by delegating to EG-030's
/// [`shard_migrate::migrate_shards`] (the bundle IS a valid `graph*.redb` shard set).
///
/// `target_shards`:
///   * `None` (the default) ⇒ restore at the manifest's own K (a 1:1 verbatim import).
///   * `Some(k)` ⇒ RE-SHARD ON RESTORE — every graph re-routed by the SAME EG-026
///     `FNV-1a % k`, so the store reopens at a different shard count.
///
/// `persist_dir` must not already hold target shard files (the migration refuses to
/// clobber) — restore into a FRESH dir. OFFLINE with respect to the TARGET: nothing may
/// be serving out of `persist_dir` while it is rebuilt.
pub fn restore_bundle(
    bundle_dir: &Path,
    persist_dir: &Path,
    target_shards: Option<usize>,
) -> Result<RestoreReport, String> {
    let manifest = read_manifest(bundle_dir)?;
    let k = target_shards.unwrap_or(manifest.shard_count).max(1);
    // The bundle's graph*.redb files are exactly the source shard set EG-030 consumes.
    let migration = shard_migrate::migrate_shards(bundle_dir, persist_dir, k)?;
    Ok(RestoreReport {
        manifest,
        restored_shards: k,
        migration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{GraphType, Method};
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use crate::wal_service::FsyncPolicy;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Write G graphs (each with two nodes + an edge) durably through a backend.
    async fn seed(dir: &str, shards: usize, graphs: &[&str]) {
        let backend = RedbBackend::open_with_shards(dir.to_string(), FsyncPolicy::Each, 256, shards)
            .expect("open backend");
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

    /// CONCEPT:EG-090 — the DR round trip: populate a durable dir → ONLINE backup (live,
    /// no quiesce) → restore into a FRESH dir → reopen and assert every graph's
    /// nodes/edges/ledger survive identically.
    #[tokio::test(flavor = "multi_thread")]
    async fn backup_restore_roundtrip_preserves_everything() {
        let root = std::env::temp_dir().join(format!("eg-backup-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("live");
        let bundle = root.join("bundle");
        let restored = root.join("restored");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let graphs = ["alpha", "beta", "gamma", "delta", "epsilon"];
        seed(&src_s, 3, &graphs).await;

        // ── ONLINE backup: reopen the SAME dir (K=3) and back it up while it is live ──
        let backend =
            RedbBackend::open_with_shards(src_s.clone(), FsyncPolicy::Each, 256, 3).expect("reopen");
        assert_eq!(backend.shard_count(), 3);
        let report = backend
            .backup(&bundle, "test-engine", 1_700_000_000, "nightly")
            .expect("backup");
        assert_eq!(report.shards, 3);
        assert_eq!(report.graphs, graphs.len() as u64);
        assert_eq!(report.nodes, (graphs.len() * 2) as u64);
        assert_eq!(report.edges, graphs.len() as u64);
        // Capture the LIVE per-graph shape so restore can be proven identical.
        let mut source: std::collections::HashMap<String, (usize, usize, usize)> =
            std::collections::HashMap::new();
        for g in &graphs {
            let d = backend.read_graph_dump_blocking(g).unwrap().unwrap();
            source.insert(
                (*g).to_string(),
                (d.nodes.len(), d.edges.len(), d.ledger.len()),
            );
        }
        backend.shutdown();

        // Bundle holds K=3 shard files + a manifest.
        for i in 0..3 {
            assert!(bundle.join(format!("graph-{i}.redb")).exists(), "shard {i}");
        }
        let manifest = read_manifest(&bundle).expect("manifest");
        assert_eq!(manifest.shard_count, 3);
        assert_eq!(manifest.engine_version, "test-engine");
        assert_eq!(manifest.timestamp, 1_700_000_000);
        assert_eq!(manifest.label, "nightly");
        assert_eq!(manifest.graphs, graphs.len() as u64);

        // ── restore into a FRESH dir at the same K ──
        let rr = restore_bundle(&bundle, &restored, None).expect("restore");
        assert_eq!(rr.restored_shards, 3);
        assert_eq!(rr.migration.graphs, graphs.len());
        assert_eq!(rr.migration.nodes, (graphs.len() * 2) as u64);

        // ── reopen the restored dir and verify every graph is intact ──
        let restored_s = restored.to_string_lossy().to_string();
        let rb =
            RedbBackend::open_with_shards(restored_s, FsyncPolicy::Each, 256, 3).expect("reopen");
        assert_eq!(rb.shard_count(), 3);
        for g in &graphs {
            let dump = rb
                .read_graph_dump_blocking(g)
                .expect("read")
                .unwrap_or_else(|| panic!("graph {g} missing after restore"));
            assert_eq!(dump.name, *g);
            assert_eq!(dump.nodes.len(), 2, "graph {g} nodes");
            assert_eq!(dump.edges.len(), 1, "graph {g} edges");
            // Restored shape is IDENTICAL to the live source (nodes, edges, ledger).
            let src = source.get(*g).copied().expect("source shape");
            assert_eq!(
                (dump.nodes.len(), dump.edges.len(), dump.ledger.len()),
                src,
                "graph {g} shape identical after restore"
            );
            let a = dump
                .nodes
                .iter()
                .find(|(id, _)| id == "a")
                .map(|(_, blob)| blob.clone())
                .expect("node a present");
            let val: serde_json::Value = rmp_serde::from_slice(&a).unwrap();
            assert_eq!(val.get("g").and_then(|x| x.as_str()), Some(*g));
        }
        rb.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CONCEPT:EG-090 — RE-SHARD ON RESTORE: a K=1 bundle restores into a K=4 persist-dir,
    /// every graph re-routed by EG-026 and still fully readable.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_can_reshard() {
        let root = std::env::temp_dir().join(format!("eg-backup-reshard-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("live");
        let bundle = root.join("bundle");
        let restored = root.join("restored");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let graphs = ["one", "two", "three", "four", "five", "six", "seven"];
        seed(&src_s, 1, &graphs).await;

        let backend =
            RedbBackend::open_with_shards(src_s.clone(), FsyncPolicy::Each, 256, 1).expect("reopen");
        let report = backend
            .backup(&bundle, "test-engine", 42, "")
            .expect("backup");
        assert_eq!(report.shards, 1);
        assert!(bundle.join("graph.redb").exists(), "K=1 bundle file");
        backend.shutdown();

        // Restore at K=4 (re-shard on restore).
        let rr = restore_bundle(&bundle, &restored, Some(4)).expect("restore reshard");
        assert_eq!(rr.restored_shards, 4);
        assert_eq!(rr.migration.graphs, graphs.len());

        let restored_s = restored.to_string_lossy().to_string();
        let rb =
            RedbBackend::open_with_shards(restored_s, FsyncPolicy::Each, 256, 4).expect("reopen");
        assert_eq!(rb.shard_count(), 4, "restored at K=4");
        for g in &graphs {
            assert!(
                rb.read_graph_dump_blocking(g).unwrap().is_some(),
                "graph {g} after reshard-restore"
            );
        }
        rb.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A restore refuses a manifest with a future format version.
    #[test]
    fn rejects_future_format() {
        let dir = std::env::temp_dir().join(format!("eg-backup-fmt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let m = serde_json::json!({
            "format_version": BUNDLE_FORMAT_VERSION + 1,
            "engine_version": "x", "shard_count": 1, "timestamp": 0, "label": "",
            "graphs": 0, "nodes": 0, "edges": 0, "ledger": 0, "semantic": 0, "audit": 0, "global": 0
        });
        std::fs::write(dir.join(MANIFEST_FILE), serde_json::to_vec(&m).unwrap()).unwrap();
        let err = read_manifest(&dir).unwrap_err();
        assert!(err.contains("newer than this build"), "got: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
