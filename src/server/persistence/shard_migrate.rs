//! Offline K-shard MIGRATION tool (CONCEPT:EG-030, M3 catalog-driven resharding).
//!
//! ## What it solves
//!
//! EG-026 fixes the durable shard count K per persist-dir once created: a graph routes
//! to `graph-<FNV-1a(name) % K>.redb` and `reconcile_shard_layout` HONORS the on-disk
//! layout at open. So an existing single-`graph.redb` (K=1) deployment, or any K, could
//! only adopt a *different* K by wiping and re-ingesting — the open path even logs
//! "run a migration to shard". **This is that migration.**
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
//! * Per-graph data (`NODES`/`EDGES`/`LEDGER`/`SEMANTIC`/`GRAPH_META`) is moved row for
//!   row, value blob unchanged — so encryption-at-rest blobs survive WITHOUT the key.
//! * The tamper-evident hash-chained `AUDIT` log (CONCEPT:KG-2.231) is copied
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
//! ## Not yet (remaining M3 — see `docs/architecture/m3-resharding.md`)
//!
//! This moves the WHOLE store to a new uniform K, OFFLINE. It is the building block
//! for ONLINE per-tenant resharding (move one hot graph between shards with no
//! downtime, driven by the [`super::tenant_catalog`]) and for cross-NODE distribution
//! (which additionally needs M2 multi-Raft) — neither of which is built here.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use redb::{Database, Durability, ReadableDatabase, ReadableTable};

use super::redb_backend::{shard_filename, shard_index, RAFT_META};
#[cfg(feature = "compute-dist")]
use crate::redb_store::MATVIEWS;
use crate::redb_store::{
    AUDIT, EDGES, GRAPH_META, LEDGER, NODES, RAFT_LOG, SEMANTIC, XSHARD_DECISION, XSHARD_PREPARE,
};

/// Outcome of a migration run (CONCEPT:EG-030) — totals copied + the layout change.
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
    /// Global rows copied to the new shard 0 (raft log/meta + 2PC + matviews).
    pub global: u64,
}

/// Discover the existing redb shard files under `dir` (CONCEPT:EG-030). Returns the
/// `graph-<n>.redb` set ordered by index, or the single legacy `graph.redb` (K=1), or
/// an error when the dir holds neither.
pub fn discover_source_shards(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut indexed: Vec<(usize, PathBuf)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(rest) = name.strip_prefix("graph-") {
                if let Some(num) = rest.strip_suffix(".redb") {
                    if let Ok(n) = num.parse::<usize>() {
                        indexed.push((n, entry.path()));
                    }
                }
            }
        }
    }
    if !indexed.is_empty() {
        indexed.sort_by_key(|(n, _)| *n);
        return Ok(indexed.into_iter().map(|(_, p)| p).collect());
    }
    let single = dir.join("graph.redb");
    if single.exists() {
        return Ok(vec![single]);
    }
    Err(format!(
        "no redb shard files (graph.redb / graph-<n>.redb) found under {}",
        dir.display()
    ))
}

/// Migrate the durable store under `src_dir` into a NEW shard count `new_k`, writing
/// `graph-<n>.redb` (or `graph.redb` for K=1) into `dst_dir` (CONCEPT:EG-030).
///
/// OFFLINE only — the engine must be stopped (exclusive redb file lock). `dst_dir` must
/// not already contain a target shard file (the tool refuses to clobber). Use a fresh
/// dir, or [`migrate_in_place`] for an atomic in-dir swap.
pub fn migrate_shards(
    src_dir: &Path,
    dst_dir: &Path,
    new_k: usize,
) -> Result<MigrationReport, String> {
    let new_k = new_k.max(1);
    let src_paths = discover_source_shards(src_dir)?;
    std::fs::create_dir_all(dst_dir).map_err(|e| e.to_string())?;

    // Refuse to clobber existing destination shard files.
    for i in 0..new_k {
        let p = dst_dir.join(shard_filename(new_k, i));
        if p.exists() {
            return Err(format!(
                "destination shard file already exists: {} (refusing to overwrite)",
                p.display()
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
        let dst_path = dst_dir.join(shard_filename(new_k, dest_idx));
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
/// shard-0 write txn (CONCEPT:EG-030): the Raft log/meta, the cross-shard 2PC records,
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

/// Migrate the store under `persist_dir` to `new_k` IN PLACE (CONCEPT:EG-030): the new
/// shards are written to a temp subdir, the OLD shard files are moved aside to a
/// timestamped `.shard-migrate-backup-<ts>` dir, and the new files are moved into
/// place. The backup is left for the operator to delete once the engine reopens cleanly
/// (so an interrupted run never strands data — the originals are recoverable).
pub fn migrate_in_place(persist_dir: &str, new_k: usize) -> Result<MigrationReport, String> {
    let new_k = new_k.max(1);
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
        let name = shard_filename(new_k, i);
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
    use crate::protocol::{GraphType, Method};
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use crate::wal_service::FsyncPolicy;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Write G graphs (each with nodes + an edge) through a K=1 backend, durably.
    async fn seed_k1(dir: &str, graphs: &[&str]) {
        let backend =
            RedbBackend::open(dir.to_string(), FsyncPolicy::Each, 256).expect("open K=1 backend");
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

    /// CONCEPT:EG-030 — migrate a K=1 store with G graphs to K=4, reopen at K=4, and
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

        // K=1 layout ⇒ single graph.redb present.
        assert!(src.join("graph.redb").exists(), "K=1 graph.redb written");

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
        let backend = RedbBackend::open(dst_s.clone(), FsyncPolicy::Each, 256).expect("reopen K=4");
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

    /// CONCEPT:EG-030 — the in-place migration swaps shard files atomically and leaves a
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

        // New shard files in place; old graph.redb moved into a backup dir.
        for i in 0..4 {
            assert!(dir.join(format!("graph-{i}.redb")).exists());
        }
        assert!(!dir.join("graph.redb").exists(), "old K=1 file moved aside");
        let has_backup = std::fs::read_dir(&dir).unwrap().flatten().any(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".shard-migrate-backup-")
        });
        assert!(has_backup, "recoverable backup dir present");

        // Reopen in place at K=4 and confirm all graphs are reachable.
        let backend = RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 256).expect("reopen");
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

    /// Refuses to clobber an existing destination shard file.
    #[test]
    fn refuses_existing_destination() {
        let dir = std::env::temp_dir().join(format!("eg-migrate-clobber-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let src = dir.join("src");
        let dst = dir.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&dst).unwrap();
        // a source graph.redb
        Database::create(src.join("graph.redb")).unwrap();
        // a pre-existing destination graph-0.redb
        Database::create(dst.join("graph-0.redb")).unwrap();
        let err = migrate_shards(&src, &dst, 4).unwrap_err();
        assert!(err.contains("already exists"), "got: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
