//! Pure redb durable-row machinery (CONCEPT:KG-2.177 / KG-2.195 / KG-2.216).
//!
//! This is the SERVER-INDEPENDENT half of the redb durable tier: the on-disk
//! table layout, the `Method → redb rows` apply, the group-commit, and the
//! full checkpoint/load read-back. It has NO Tokio and NO `ServerState`
//! dependency, so it compiles under `--features redb` ALONE (no `server`).
//!
//! Two callers share it — ONE durable format, never duplicated:
//!   * the out-of-process server's `server::persistence::redb_backend::RedbBackend`
//!     (gated on `server`), which wraps these in its off-reactor group-commit
//!     writer thread + the `PersistenceBackend` async trait; and
//!   * the in-process [`crate::embedded::EmbeddedEngine`] (gated on `embedded`),
//!     which commits through them DIRECTLY (the caller is the writer — durable,
//!     commit-before-return, no Tokio runtime).
//!
//! The redb `Database` and every table key/value shape here are byte-identical to
//! what the server writes, so a graph written by the embedded API reopens in the
//! server and vice-versa.
//!
//! ## Tables (all keyed by graph prefix)
//!   * `nodes`          `(graph, id)            -> node properties msgpack`
//!   * `edges`          `(graph, src, tgt, ord) -> edge properties msgpack`
//!   * `ledger`         `(graph, seq)           -> ledger line`
//!   * `semantic_store` `graph                  -> semantic store blob (msgpack)`
//!   * `graph_meta`     `graph                  -> {name, graph_type} blob`

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;

use crate::protocol::{GraphType, Method};

pub(crate) const NODES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("nodes");
pub(crate) const EDGES: TableDefinition<(&str, &str, &str, u32), &[u8]> =
    TableDefinition::new("edges");
pub(crate) const LEDGER: TableDefinition<(&str, u64), &str> = TableDefinition::new("ledger");
pub(crate) const SEMANTIC: TableDefinition<&str, &[u8]> = TableDefinition::new("semantic_store");
pub(crate) const GRAPH_META: TableDefinition<&str, &[u8]> = TableDefinition::new("graph_meta");
// Durable Raft log table (CONCEPT:KG-2.204). Defined here so `commit_ops` (shared
// with the server's group-commit writer, which folds replicated log appends into
// the SAME `WriteTransaction` as graph mutations) is self-contained. The embedded
// path never appends log ops, so this table stays empty for an embedded-only DB —
// the const costs nothing and keeps the two callers on one durable layout.
pub(crate) const RAFT_LOG: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("raft_log");
// Cross-shard 2PC prepare records (CONCEPT:KG-2.222). One row per participant group
// of an in-flight cross-shard transaction, keyed by `(txn_id, group_id)`, holding
// that group's PREPARED-but-not-applied slice (its staged write-set). Durable so an
// in-doubt txn survives a coordinator/participant crash between PREPARE and COMMIT
// and is resolved on restart. Lives in `graph.redb` for the same single-file reason
// as the Raft log; the PURE put/clear/scan logic lives here (shared store) next to
// NODES/EDGES/purge_graph_rows, while the writer-thread `Cmd` arms in `redb_backend`
// call into it (mirrors how the graph-row machinery is shared, CONCEPT:KG-2.216).
pub(crate) const XSHARD_PREPARE: TableDefinition<(&str, u64), &[u8]> =
    TableDefinition::new("xshard_prepare");
// The coordinator's durable DECISION record for a cross-shard txn, keyed by `txn_id`
// (CONCEPT:KG-2.222). The value is `1` = COMMIT, `0` = ABORT. Writing this row is the
// ATOMIC COMMIT POINT: once it reads COMMIT every participant will apply on recovery;
// absent/ABORT ⇒ no participant applies (presumed-abort). Cleared after resolution.
pub(crate) const XSHARD_DECISION: TableDefinition<&str, u8> =
    TableDefinition::new("xshard_decision");
// Named distributed-compute MATERIALIZED VIEWS (CONCEPT:KG-2.227). One row per matview
// keyed by `name`, holding the MessagePack-serialized `MatView` (its definition +
// current result rows). Durable so a matview survives restart; the handler reloads the
// in-RAM `MatViewStore` from this table on boot and refreshes incrementally on a delta.
// Lives in `graph.redb` for the same single-file reason as the Raft log + xshard rows.
#[cfg(feature = "compute-dist")]
pub(crate) const MATVIEWS: TableDefinition<&str, &[u8]> = TableDefinition::new("matviews");

/// In-doubt cross-shard prepare records `(txn_id, group_id, slice-blob)` returned by
/// the recovery scan (CONCEPT:KG-2.222).
pub(crate) type XshardPrepareScan = Result<Vec<(String, u64, Vec<u8>)>, String>;

/// Persisted materialized views `(name, blob)` returned by the boot reload scan
/// (CONCEPT:KG-2.227).
#[cfg(feature = "compute-dist")]
pub(crate) type MatViewScanResult = Result<Vec<(String, Vec<u8>)>, String>;

/// Map a logical graph name (which may contain `:` / `/`) to a safe filename /
/// durable key. Identical to `persist::sanitize`; lives here so the durable store
/// has its own server-independent copy (the embedded path links no `persist.rs`).
pub fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// An owned, off-lock dump of one graph used by the checkpoint + load paths.
pub struct GraphDump {
    pub graph: String,
    pub name: String,
    pub graph_type: GraphType,
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub ledger: Vec<String>,
    pub semantic: Vec<u8>,
}

/// Commit all buffered mutations (and any Raft log appends) in ONE write
/// transaction at the given durability (CONCEPT:KG-2.204). A graph mutation and a
/// Raft log entry in the same batch therefore share ONE `WriteTransaction` and
/// ONE fsync. The embedded path passes an empty `raft_log_ops`.
pub(crate) fn commit_ops(
    db: &Database,
    ops: &mut Vec<(String, Method)>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    durability: Durability,
) -> Result<(), String> {
    if ops.is_empty() && raft_log_ops.is_empty() {
        return Ok(());
    }
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(durability).map_err(|e| e.to_string())?;
    // Graphs touched by this batch — used to backfill a graph_meta row for any
    // graph that received writes but was never explicitly registered (e.g. the
    // pre-created `__commons__`), so authoritative `load_all` recovers it even with
    // no checkpoint.
    let mut touched: std::collections::HashSet<String> = std::collections::HashSet::new();
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (graph, method) in ops.drain(..) {
            touched.insert(graph.clone());
            apply_method_rows(&graph, &method, &mut nodes, &mut edges, &mut ledger)?;
        }
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        for g in &touched {
            if meta.get(g.as_str()).map_err(|e| e.to_string())?.is_none() {
                meta.insert(g.as_str(), encode_meta(g, GraphType::Global).as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        if !raft_log_ops.is_empty() {
            let mut log = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
            for (gid, idx, blob) in raft_log_ops.drain(..) {
                log.insert((gid, idx), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// One node's vector upsert for a cross-modal commit (CONCEPT:KG-2.225).
pub type VectorUpsert = (String, Vec<f32>);

/// A blob-reference for a cross-modal commit (CONCEPT:KG-2.225): a `(node_id, digest)`
/// pair recorded as a durable graph-side link to an already-stored blob. The blob
/// BYTES live in the content-addressed `blob.redb` (pre-uploaded); THIS is the durable
/// graph pointer that must land atomically with the node/vector/property.
pub type BlobRefRow = (String, String);

/// **Cross-modal ACID commit (CONCEPT:KG-2.225)** — land a graph + vector + blob-ref +
/// property write-set for ONE graph in ONE redb [`WriteTransaction`], all-or-nothing.
///
/// This is the durable barrier the single-graph cross-modal txn commits through. Every
/// modality writes into the SAME `graph.redb` transaction so the commit is atomic:
///   * **graph** ops (`AddNode`/`AddEdge`/`CompareAndSetNodeFields`/…) → NODES/EDGES,
///     via the shared [`apply_method_rows`] (the SAME rows the single-modal path writes);
///   * **vectors** → the graph's `SEMANTIC` blob is read-modify-written inside the txn
///     (deserialize → `add_embedding` each upsert → reserialize), so a node and its
///     embedding are durable together — never a node without its vector or vice-versa;
///   * **blob refs** → a `__blob__` reserved property on the node carrying the digest,
///     written into NODES, so the graph-side link to the (separately content-addressed)
///     blob lands in the SAME transaction as everything else.
///
/// If ANY step errors, the `WriteTransaction` is DROPPED without `commit()` — redb
/// discards every staged write, so NONE of the modalities land (a true rollback, no
/// partial). On success the txn commits at `Durability::Immediate` (commit-before-ack:
/// the cross-modal write is on disk before the client is told it succeeded).
pub(crate) fn commit_crossmodal(
    db: &Database,
    graph: &str,
    methods: &[Method],
    vectors: &[VectorUpsert],
    blob_refs: &[BlobRefRow],
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;

        // 1. Graph mutations (nodes/edges/properties) — the SAME row apply.
        for method in methods {
            apply_method_rows(graph, method, &mut nodes, &mut edges, &mut ledger)?;
        }

        // 2. Blob refs — a reserved `__blob__` node property pointing at the digest.
        // Read-modify-write the node's property blob so the ref rides the node row.
        for (node_id, digest) in blob_refs {
            let mut props: serde_json::Map<String, serde_json::Value> = nodes
                .get((graph, node_id.as_str()))
                .map_err(|e| e.to_string())?
                .and_then(|v| rmp_serde::from_slice(v.value()).ok())
                .unwrap_or_default();
            props.insert(
                "__blob__".to_string(),
                serde_json::Value::String(digest.clone()),
            );
            let blob = rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?;
            nodes
                .insert((graph, node_id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }

        // 3. Vectors — read-modify-write the graph's SEMANTIC store blob in-txn.
        if !vectors.is_empty() {
            let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            let mut store = semantic
                .get(graph)
                .map_err(|e| e.to_string())?
                .and_then(|v| {
                    rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(v.value()).ok()
                })
                .unwrap_or_default();
            for (node_id, embedding) in vectors {
                store.add_embedding(node_id.clone(), embedding.clone());
            }
            let blob = rmp_serde::to_vec_named(&store).map_err(|e| e.to_string())?;
            semantic
                .insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }

        // Backfill a graph_meta identity row so authoritative load_all recovers it.
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        if meta.get(graph).map_err(|e| e.to_string())?.is_none() {
            meta.insert(graph, encode_meta(graph, GraphType::Global).as_slice())
                .map_err(|e| e.to_string())?;
        }
    }
    // The atomic commit point: every modality lands here, or (on any `?` above) the
    // dropped wtx discards them all.
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Durably write/overwrite a graph_meta identity row in its OWN transaction.
pub(crate) fn write_graph_meta(
    db: &Database,
    graph: &str,
    name: &str,
    graph_type: GraphType,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        meta.insert(graph, encode_meta(name, graph_type).as_slice())
            .map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Point-read a single node's stored properties (read-through path).
pub(crate) fn read_one_node(
    db: &Database,
    graph: &str,
    node_id: &str,
) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let nodes = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let v = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|g| g.value().to_vec());
    Ok(v)
}

/// Translate ONE applied method into redb row writes inside an open transaction.
/// Mirrors `crate::wal::apply`'s method set: the durable DATA mutations only.
pub(crate) fn apply_method_rows(
    graph: &str,
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            nodes
                .insert((graph, node_id.as_str()), properties_msgpack.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Method::RemoveNode { node_id } => {
            nodes
                .remove((graph, node_id.as_str()))
                .map_err(|e| e.to_string())?;
            // Remove this node's incident edges (best-effort prefix sweep on src).
            let to_del: Vec<(String, String, u32)> = edges
                .range((graph, node_id.as_str(), "", 0u32)..)
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .take_while(|(k, _)| {
                    let (g, s, _, _) = k.value();
                    g == graph && s == node_id.as_str()
                })
                .map(|(k, _)| {
                    let (_, s, t, o) = k.value();
                    (s.to_string(), t.to_string(), o)
                })
                .collect();
            for (s, t, o) in to_del {
                let _ = edges.remove((graph, s.as_str(), t.as_str(), o));
            }
        }
        Method::CompareAndSetNodeFields {
            node_id,
            updates_msgpack,
            ..
        } => {
            // Write-through best-effort: persist the post-update node properties.
            nodes
                .insert((graph, node_id.as_str()), updates_msgpack.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let ord = next_edge_ordinal(edges, graph, source_id, target_id)?;
            edges
                .insert(
                    (graph, source_id.as_str(), target_id.as_str(), ord),
                    properties_msgpack.as_slice(),
                )
                .map_err(|e| e.to_string())?;
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            let ords: Vec<u32> = edges
                .range((graph, source_id.as_str(), target_id.as_str(), 0u32)..)
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .take_while(|(k, _)| {
                    let (g, s, t, _) = k.value();
                    g == graph && s == source_id.as_str() && t == target_id.as_str()
                })
                .map(|(k, _)| k.value().3)
                .collect();
            for o in ords {
                let _ = edges.remove((graph, source_id.as_str(), target_id.as_str(), o));
            }
        }
        Method::BatchUpdate { operations_msgpack } => {
            apply_batch_rows(graph, operations_msgpack, nodes, edges)?;
        }
        Method::ClearGraph => {
            clear_graph_rows(graph, nodes, edges, ledger)?;
        }
        _ => {}
    }
    Ok(())
}

/// Next free edge ordinal for a (src,tgt) pair in this graph.
fn next_edge_ordinal(
    edges: &redb::Table<(&str, &str, &str, u32), &[u8]>,
    graph: &str,
    src: &str,
    tgt: &str,
) -> Result<u32, String> {
    let max = edges
        .range((graph, src, tgt, 0u32)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| {
            let (g, s, t, _) = k.value();
            g == graph && s == src && t == tgt
        })
        .map(|(k, _)| k.value().3)
        .max();
    Ok(max.map(|m| m + 1).unwrap_or(0))
}

/// Apply a decoded `BatchUpdate` op-list as row writes.
fn apply_batch_rows(
    graph: &str,
    operations_msgpack: &[u8],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    let ops: Vec<serde_json::Value> = match rmp_serde::from_slice(operations_msgpack) {
        Ok(o) => o,
        Err(_) => return Ok(()), // opaque batch — skip rather than fail the commit
    };
    for op in ops {
        let kind = op.get("op").and_then(|v| v.as_str()).unwrap_or("");
        match kind {
            "add_node" | "upsert_node" => {
                if let Some(id) = op.get("node_id").and_then(|v| v.as_str()) {
                    let props = op
                        .get("properties")
                        .map(|p| rmp_serde::to_vec_named(p).unwrap_or_default())
                        .unwrap_or_default();
                    nodes
                        .insert((graph, id), props.as_slice())
                        .map_err(|e| e.to_string())?;
                }
            }
            "remove_node" => {
                if let Some(id) = op.get("node_id").and_then(|v| v.as_str()) {
                    nodes.remove((graph, id)).map_err(|e| e.to_string())?;
                }
            }
            "add_edge" => {
                if let (Some(s), Some(t)) = (
                    op.get("source_id").and_then(|v| v.as_str()),
                    op.get("target_id").and_then(|v| v.as_str()),
                ) {
                    let props = op
                        .get("properties")
                        .map(|p| rmp_serde::to_vec_named(p).unwrap_or_default())
                        .unwrap_or_default();
                    let ord = next_edge_ordinal(edges, graph, s, t)?;
                    edges
                        .insert((graph, s, t, ord), props.as_slice())
                        .map_err(|e| e.to_string())?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Drop every row for `graph` across nodes/edges/ledger (ClearGraph).
fn clear_graph_rows(
    graph: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
) -> Result<(), String> {
    let node_keys: Vec<String> = nodes
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| k.value().1.to_string())
        .collect();
    for id in node_keys {
        let _ = nodes.remove((graph, id.as_str()));
    }
    let edge_keys: Vec<(String, String, u32)> = edges
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| {
            let (_, s, t, o) = k.value();
            (s.to_string(), t.to_string(), o)
        })
        .collect();
    for (s, t, o) in edge_keys {
        let _ = edges.remove((graph, s.as_str(), t.as_str(), o));
    }
    let seqs: Vec<u64> = ledger
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| k.value().1)
        .collect();
    for seq in seqs {
        let _ = ledger.remove((graph, seq));
    }
    Ok(())
}

/// Drop EVERY durable row for `graph` in ONE durable transaction (CONCEPT:KG-2.221,
/// the tenant-DELETE path). Unlike `clear_graph_rows` (which empties a LIVE graph's
/// data but keeps its `graph_meta` identity), this ALSO removes the `semantic_store`
/// blob and the `graph_meta` row, so the graph ceases to exist durably — a recreate
/// of the same name then starts from a clean slate instead of inheriting the deleted
/// incarnation's rows on a read-through / `load_all`. Lives in the SHARED redb_store
/// so the embedded engine's delete path purges correctly too (CONCEPT:KG-2.216).
pub(crate) fn purge_graph_rows(db: &Database, graph: &str) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        // nodes/edges/ledger — reuse the same range-scan-and-remove as ClearGraph.
        clear_graph_rows(graph, &mut nodes, &mut edges, &mut ledger)?;
        // semantic store blob (keyed by graph) + the identity row.
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        let _ = semantic.remove(graph).map_err(|e| e.to_string())?;
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        let _ = meta.remove(graph).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

// ── Cross-shard 2PC durable rows (CONCEPT:KG-2.222) — pure, server-INDEPENDENT ──
// Shared store helpers (mirroring NODES/EDGES/purge_graph_rows): the `Cmd` arms in
// `redb_backend`'s off-reactor writer thread call straight into these.

/// Durably persist one participant group's prepared slice (its own transaction).
pub(crate) fn put_xshard_prepare(
    db: &Database,
    txn_id: &str,
    gid: u64,
    slice: &[u8],
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
        t.insert((txn_id, gid), slice).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Durably write the coordinator's decision row (the atomic commit point).
pub(crate) fn put_xshard_decision(db: &Database, txn_id: &str, commit: bool) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
        t.insert(txn_id, if commit { 1u8 } else { 0u8 })
            .map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Clear one participant's prepare record after resolution.
pub(crate) fn clear_xshard_prepare(db: &Database, txn_id: &str, gid: u64) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
        t.remove((txn_id, gid)).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Clear a resolved txn's decision record.
pub(crate) fn clear_xshard_decision(db: &Database, txn_id: &str) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
        t.remove(txn_id).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Scan every in-doubt prepare record `(txn_id, group_id, slice)` for recovery.
pub(crate) fn scan_xshard_prepares(db: &Database) -> XshardPrepareScan {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for kv in t.iter().map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        let (txn_id, gid) = k.value();
        out.push((txn_id.to_string(), gid, v.value().to_vec()));
    }
    Ok(out)
}

/// Read a txn's durable decision (Some(true)=commit, Some(false)=abort, None=undecided).
pub(crate) fn get_xshard_decision(db: &Database, txn_id: &str) -> Result<Option<bool>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
    Ok(t.get(txn_id)
        .map_err(|e| e.to_string())?
        .map(|v| v.value() == 1))
}

/// Durably upsert a named materialized view's serialized blob (CONCEPT:KG-2.227).
#[cfg(feature = "compute-dist")]
pub(crate) fn put_matview(db: &Database, name: &str, blob: &[u8]) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(MATVIEWS).map_err(|e| e.to_string())?;
        t.insert(name, blob).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Scan every persisted materialized view `(name, blob)` for reload on boot.
#[cfg(feature = "compute-dist")]
pub(crate) fn scan_matviews(db: &Database) -> Result<Vec<(String, Vec<u8>)>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    // A fresh DB may not have the table yet — treat "table missing" as "no views".
    let t = match rtx.open_table(MATVIEWS) {
        Ok(t) => t,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for kv in t.iter().map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        out.push((k.value().to_string(), v.value().to_vec()));
    }
    Ok(out)
}

/// Snapshot the full registry dump into redb, overwriting each graph's rows, and
/// commit durably. Folds any buffered mutations into the SAME transaction first.
pub(crate) fn apply_checkpoint(
    db: &Database,
    pending: &mut Vec<(String, Method)>,
    graphs: Vec<GraphDump>,
) -> Result<usize, String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let mut count = 0usize;
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;

        for (graph, method) in pending.drain(..) {
            apply_method_rows(&graph, &method, &mut nodes, &mut edges, &mut ledger)?;
        }

        for dump in graphs {
            clear_graph_rows(&dump.graph, &mut nodes, &mut edges, &mut ledger)?;
            for (id, props) in &dump.nodes {
                nodes
                    .insert((dump.graph.as_str(), id.as_str()), props.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            for (src, tgt, props) in &dump.edges {
                let ord = next_edge_ordinal(&edges, &dump.graph, src, tgt)?;
                edges
                    .insert(
                        (dump.graph.as_str(), src.as_str(), tgt.as_str(), ord),
                        props.as_slice(),
                    )
                    .map_err(|e| e.to_string())?;
            }
            for (seq, line) in dump.ledger.iter().enumerate() {
                ledger
                    .insert((dump.graph.as_str(), seq as u64), line.as_str())
                    .map_err(|e| e.to_string())?;
            }
            semantic
                .insert(dump.graph.as_str(), dump.semantic.as_slice())
                .map_err(|e| e.to_string())?;
            meta.insert(
                dump.graph.as_str(),
                encode_meta(&dump.name, dump.graph_type).as_slice(),
            )
            .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(count)
}

/// Read ONE graph's durable rows back into an owned [`GraphDump`] (CONCEPT:KG-2.224 —
/// tenant rehydration). Range-scans each table by the `graph` key prefix, so a cold
/// tenant rehydrates from redb without reading the whole store. `None` when the graph
/// has no durable identity (`graph_meta`) row — a genuine absence, not a hibernation.
pub(crate) fn read_graph_dump(db: &Database, graph: &str) -> Result<Option<GraphDump>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let (name, graph_type) = match meta_table.get(graph).map_err(|e| e.to_string())? {
        Some(v) => decode_meta(v.value()),
        None => return Ok(None),
    };
    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;

    let mut nodes = Vec::new();
    for row in nodes_table
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        if g != graph {
            break;
        }
        nodes.push((id.to_string(), v.value().to_vec()));
    }
    let mut edges = Vec::new();
    for row in edges_table
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, _) = k.value();
        if g != graph {
            break;
        }
        edges.push((s.to_string(), t.to_string(), v.value().to_vec()));
    }
    let mut ledger = Vec::new();
    for row in ledger_table
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        if k.value().0 != graph {
            break;
        }
        ledger.push(v.value().to_string());
    }
    let semantic = semantic_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec())
        .unwrap_or_default();

    Ok(Some(GraphDump {
        graph: graph.to_string(),
        name,
        graph_type,
        nodes,
        edges,
        ledger,
        semantic,
    }))
}

/// Read the entire store into owned per-graph dumps. Each graph's rows are
/// collected by iterating the whole table once and bucketing by the graph prefix.
pub(crate) fn read_all_dumps(db: &Database) -> Result<Vec<GraphDump>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;

    let mut dumps: HashMap<String, GraphDump> = HashMap::new();
    for row in meta_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let graph = k.value().to_string();
        let (name, graph_type) = decode_meta(v.value());
        dumps.insert(
            graph.clone(),
            GraphDump {
                graph,
                name,
                graph_type,
                nodes: Vec::new(),
                edges: Vec::new(),
                ledger: Vec::new(),
                semantic: Vec::new(),
            },
        );
    }

    for row in nodes_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        if let Some(d) = dumps.get_mut(g) {
            d.nodes.push((id.to_string(), v.value().to_vec()));
        }
    }
    for row in edges_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, _) = k.value();
        if let Some(d) = dumps.get_mut(g) {
            d.edges
                .push((s.to_string(), t.to_string(), v.value().to_vec()));
        }
    }
    for row in ledger_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, _) = k.value();
        if let Some(d) = dumps.get_mut(g) {
            d.ledger.push(v.value().to_string());
        }
    }
    for row in semantic_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        if let Some(d) = dumps.get_mut(k.value()) {
            d.semantic = v.value().to_vec();
        }
    }
    Ok(dumps.into_values().collect())
}

pub(crate) fn encode_meta(name: &str, gtype: GraphType) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "name": name, "graph_type": gtype }))
        .unwrap_or_default()
}

fn decode_meta(blob: &[u8]) -> (String, GraphType) {
    let v: serde_json::Value = rmp_serde::from_slice(blob).unwrap_or(serde_json::Value::Null);
    let name = v
        .get("name")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string();
    let gtype = v
        .get("graph_type")
        .cloned()
        .and_then(|x| serde_json::from_value(x).ok())
        .unwrap_or(GraphType::Global);
    (name, gtype)
}
