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
// Tamper-evident hash-chained audit log (CONCEPT:KG-2.231, feature `security`). One
// row per durable mutation, keyed `(graph, seq)`, value = `prev_hash | entry_hash |
// line` (see `crate::audit`). Appended in the SAME WriteTransaction as the mutation
// it records, so the audit entry and the data it audits are durable together. The
// table const is always defined (so the layout is stable) but only WRITTEN/READ under
// `security`.
pub(crate) const AUDIT: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("audit_chain");
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

/// Encryption-at-rest cipher handle threaded through the durable read/write paths
/// (CONCEPT:KG-2.231). A thin wrapper so the SAME function signatures carry it
/// whether or not the `security` feature is compiled: without `security` it is a
/// zero-sized no-op (every `seal`/`unseal` is the identity), so the durable format
/// and code path are byte-for-byte unchanged; with `security` + a configured key it
/// holds the AEAD that seals value blobs on write and unseals on read.
#[derive(Clone, Copy, Default)]
pub struct DurableCrypto<'a> {
    #[cfg(feature = "security")]
    cipher: Option<&'a crate::crypto::ValueCipher>,
    #[cfg(not(feature = "security"))]
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> DurableCrypto<'a> {
    /// A no-op handle (encryption off / not compiled).
    pub fn none() -> Self {
        DurableCrypto::default()
    }

    /// Wrap an optional cipher (the `security` path).
    #[cfg(feature = "security")]
    pub fn new(cipher: Option<&'a crate::crypto::ValueCipher>) -> Self {
        DurableCrypto { cipher }
    }

    /// Seal a value blob for storage. Identity when no cipher is active.
    #[inline]
    fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
        #[cfg(feature = "security")]
        if let Some(c) = self.cipher {
            return c.seal(plaintext);
        }
        plaintext.to_vec()
    }

    /// Unseal a stored value blob. Identity when no cipher is active; a sealed blob
    /// read by a no-cipher handle is returned as-is (caller sees ciphertext — only
    /// possible if a key was removed, which is operator error). With a cipher, a
    /// legacy plaintext blob passes through and a sealed blob is decrypted (wrong key
    /// ⇒ Err).
    #[inline]
    fn unseal(&self, stored: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "security")]
        if let Some(c) = self.cipher {
            return c.unseal(stored);
        }
        Ok(stored.to_vec())
    }
}

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
    crypto: DurableCrypto<'_>,
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
        #[cfg(feature = "security")]
        let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
        for (graph, method) in ops.drain(..) {
            touched.insert(graph.clone());
            apply_method_rows(&graph, &method, &mut nodes, &mut edges, &mut ledger, crypto)?;
            #[cfg(feature = "security")]
            append_audit_entry(&mut audit, &graph, &method)?;
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
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;

        // 1. Graph mutations (nodes/edges/properties) — the SAME row apply.
        #[cfg(feature = "security")]
        let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
        for method in methods {
            apply_method_rows(graph, method, &mut nodes, &mut edges, &mut ledger, crypto)?;
            #[cfg(feature = "security")]
            append_audit_entry(&mut audit, graph, method)?;
        }

        // 2. Blob refs — a reserved `__blob__` node property pointing at the digest.
        // Read-modify-write the node's property blob so the ref rides the node row.
        // Unseal the current blob before merging, re-seal the merged result.
        for (node_id, digest) in blob_refs {
            let current = nodes
                .get((graph, node_id.as_str()))
                .map_err(|e| e.to_string())?
                .map(|v| crypto.unseal(v.value()))
                .transpose()?;
            let mut props: serde_json::Map<String, serde_json::Value> = current
                .and_then(|b| rmp_serde::from_slice(&b).ok())
                .unwrap_or_default();
            props.insert(
                "__blob__".to_string(),
                serde_json::Value::String(digest.clone()),
            );
            let blob = crypto.seal(&rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?);
            nodes
                .insert((graph, node_id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }

        // 3. Vectors — read-modify-write the graph's SEMANTIC store blob in-txn.
        if !vectors.is_empty() {
            let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            let current = semantic
                .get(graph)
                .map_err(|e| e.to_string())?
                .map(|v| crypto.unseal(v.value()))
                .transpose()?;
            let mut store = current
                .and_then(|b| {
                    rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(&b).ok()
                })
                .unwrap_or_default();
            for (node_id, embedding) in vectors {
                store.add_embedding(node_id.clone(), embedding.clone());
            }
            let blob = crypto.seal(&rmp_serde::to_vec_named(&store).map_err(|e| e.to_string())?);
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
    crypto: DurableCrypto<'_>,
) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let nodes = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let v = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|g| crypto.unseal(g.value()))
        .transpose()?;
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
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let blob = crypto.seal(properties_msgpack);
            nodes
                .insert((graph, node_id.as_str()), blob.as_slice())
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
            let blob = crypto.seal(updates_msgpack);
            nodes
                .insert((graph, node_id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let ord = next_edge_ordinal(edges, graph, source_id, target_id)?;
            let blob = crypto.seal(properties_msgpack);
            edges
                .insert(
                    (graph, source_id.as_str(), target_id.as_str(), ord),
                    blob.as_slice(),
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
            apply_batch_rows(graph, operations_msgpack, nodes, edges, crypto)?;
        }
        Method::ClearGraph => {
            clear_graph_rows(graph, nodes, edges, ledger)?;
        }
        _ => {}
    }
    Ok(())
}

/// Append ONE tamper-evident audit-chain entry for a durable mutation, inside the
/// caller's open WriteTransaction (CONCEPT:KG-2.231). Reads the graph's current chain
/// tail (last `(graph, seq)`) to get `prev_hash` + next `seq`, links the new entry,
/// and inserts it. A method with no canonical audit line (e.g. a pure-compute op that
/// slipped through) is skipped. The audit row rides the SAME transaction as the data
/// mutation, so they are durable together. Only compiled/called under `security`.
#[cfg(feature = "security")]
pub(crate) fn append_audit_entry(
    audit: &mut redb::Table<(&str, u64), &[u8]>,
    graph: &str,
    method: &Method,
) -> Result<(), String> {
    let line = match crate::audit::audit_line(method) {
        Some(l) => l,
        None => return Ok(()),
    };
    // Find the chain tail for this graph: the highest existing seq + its hash. Extract
    // OWNED values so the read access-guards drop before the mutable `insert` below.
    let tail: Option<(u64, crate::audit::Hash)> = {
        let last = audit
            .range((graph, 0u64)..)
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .take_while(|(k, _)| k.value().0 == graph)
            .last();
        match last {
            Some((k, v)) => {
                let seq = k.value().1;
                let (_, hash, _) = crate::audit::decode_entry(v.value())
                    .ok_or_else(|| "corrupt audit tail entry".to_string())?;
                Some((seq, hash))
            }
            None => None,
        }
    };
    let (prev, next_seq) = match tail {
        Some((seq, hash)) => (hash, seq + 1),
        None => (crate::audit::GENESIS, 0u64),
    };
    let hash = crate::audit::link_hash(&prev, graph, next_seq, line.as_bytes());
    let blob = crate::audit::encode_entry(&prev, &hash, line.as_bytes());
    audit
        .insert((graph, next_seq), blob.as_slice())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Verify a graph's hash-chained audit log (CONCEPT:KG-2.231). Range-scans
/// `(graph, 0..)` in seq order and walks the chain via `crate::audit::verify_chain`.
#[cfg(feature = "security")]
pub(crate) fn verify_audit(
    db: &Database,
    graph: &str,
) -> Result<crate::protocol::AuditReport, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let audit = rtx.open_table(AUDIT).map_err(|e| e.to_string())?;
    let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
    for r in audit.range((graph, 0u64)..).map_err(|e| e.to_string())? {
        let (k, v) = r.map_err(|e| e.to_string())?;
        if k.value().0 != graph {
            break;
        }
        rows.push((k.value().1, v.value().to_vec()));
    }
    Ok(crate::audit::verify_chain(
        graph,
        rows.iter().map(|(s, b)| (*s, b.as_slice())),
    ))
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
    crypto: DurableCrypto<'_>,
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
                    let props = crypto.seal(&props);
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
                    let props = crypto.seal(&props);
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
    crypto: DurableCrypto<'_>,
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
            apply_method_rows(&graph, &method, &mut nodes, &mut edges, &mut ledger, crypto)?;
        }

        for dump in graphs {
            // The dump's node/edge/semantic blobs are plaintext (from the live
            // GraphCore snapshot) — SEAL them on the way to disk (no-op when
            // encryption is off). The ledger lines stay plaintext (operational mirror
            // / audit-chain input).
            clear_graph_rows(&dump.graph, &mut nodes, &mut edges, &mut ledger)?;
            for (id, props) in &dump.nodes {
                let blob = crypto.seal(props);
                nodes
                    .insert((dump.graph.as_str(), id.as_str()), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            for (src, tgt, props) in &dump.edges {
                let ord = next_edge_ordinal(&edges, &dump.graph, src, tgt)?;
                let blob = crypto.seal(props);
                edges
                    .insert(
                        (dump.graph.as_str(), src.as_str(), tgt.as_str(), ord),
                        blob.as_slice(),
                    )
                    .map_err(|e| e.to_string())?;
            }
            for (seq, line) in dump.ledger.iter().enumerate() {
                ledger
                    .insert((dump.graph.as_str(), seq as u64), line.as_str())
                    .map_err(|e| e.to_string())?;
            }
            let sem = crypto.seal(&dump.semantic);
            semantic
                .insert(dump.graph.as_str(), sem.as_slice())
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
pub(crate) fn read_graph_dump(
    db: &Database,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<GraphDump>, String> {
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
        nodes.push((id.to_string(), crypto.unseal(v.value())?));
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
        edges.push((s.to_string(), t.to_string(), crypto.unseal(v.value())?));
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
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
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
pub(crate) fn read_all_dumps(
    db: &Database,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<GraphDump>, String> {
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
        let plain = crypto.unseal(v.value())?;
        if let Some(d) = dumps.get_mut(g) {
            d.nodes.push((id.to_string(), plain));
        }
    }
    for row in edges_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, _) = k.value();
        let plain = crypto.unseal(v.value())?;
        if let Some(d) = dumps.get_mut(g) {
            d.edges.push((s.to_string(), t.to_string(), plain));
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
        let plain = crypto.unseal(v.value())?;
        if let Some(d) = dumps.get_mut(k.value()) {
            d.semantic = plain;
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

#[cfg(all(test, feature = "security"))]
mod security_tests {
    //! Encryption-at-rest + tamper-evident audit proofs over the durable store
    //! (CONCEPT:KG-2.231), exercised through the SAME `commit_ops`/read/`verify_audit`
    //! the server + embedded engine use.
    use super::*;
    use crate::crypto::ValueCipher;

    fn open_db(dir: &std::path::Path) -> Database {
        let path = dir.join("graph.redb");
        let db = Database::create(&path).unwrap();
        let wtx = db.begin_write().unwrap();
        wtx.open_table(NODES).unwrap();
        wtx.open_table(EDGES).unwrap();
        wtx.open_table(LEDGER).unwrap();
        wtx.open_table(SEMANTIC).unwrap();
        wtx.open_table(GRAPH_META).unwrap();
        wtx.open_table(AUDIT).unwrap();
        wtx.commit().unwrap();
        db
    }

    fn add_node_method(node_id: &str, props: serde_json::Value) -> Method {
        Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&props).unwrap(),
        }
    }

    #[test]
    fn encryption_no_plaintext_on_disk_round_trips_and_wrong_key_fails() {
        let dir = tempdir();
        let db_path = dir.join("graph.redb");
        let cipher = ValueCipher::from_key_material(b"correct-horse-battery-staple");
        let crypto = DurableCrypto::new(Some(&cipher));

        // Write a node carrying a recognizable SECRET via the durable write path.
        {
            let db = open_db(&dir);
            let mut ops = vec![(
                "g".to_string(),
                add_node_method("n1", serde_json::json!({"ssn": "SECRET-123-45-6789"})),
            )];
            let mut log = Vec::new();
            commit_ops(&db, &mut ops, &mut log, Durability::Immediate, crypto).unwrap();
        }

        // The raw on-disk redb bytes must NOT contain the plaintext secret.
        let raw = std::fs::read(&db_path).unwrap();
        let needle = b"SECRET-123-45-6789";
        assert!(
            !raw.windows(needle.len()).any(|w| w == needle),
            "plaintext node property leaked into raw redb file"
        );

        // It round-trips with the right key.
        {
            let db = open_db(&dir);
            let dumps = read_all_dumps(&db, crypto).unwrap();
            let g = dumps.iter().find(|d| d.graph == "g").expect("graph g");
            let (_, props) = &g.nodes[0];
            let m: serde_json::Value = rmp_serde::from_slice(props).unwrap();
            assert_eq!(m["ssn"], "SECRET-123-45-6789");
        }

        // A WRONG key fails to decrypt (never silent plaintext).
        {
            let db = open_db(&dir);
            let wrong = ValueCipher::from_key_material(b"totally-different-key");
            let res = read_all_dumps(&db, DurableCrypto::new(Some(&wrong)));
            assert!(res.is_err(), "wrong key must not decrypt");
        }
    }

    #[test]
    fn audit_chain_verifies_clean_and_detects_tampering() {
        let dir = tempdir();
        let crypto = DurableCrypto::none();
        let db = open_db(&dir);

        // Three durable mutations → three chained audit entries.
        for (i, m) in [
            add_node_method("a", serde_json::json!({"v": 1})),
            add_node_method("b", serde_json::json!({"v": 2})),
            Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let _ = i;
            let mut ops = vec![("g".to_string(), m)];
            let mut log = Vec::new();
            commit_ops(&db, &mut ops, &mut log, Durability::Immediate, crypto).unwrap();
        }

        // A clean chain verifies.
        let report = verify_audit(&db, "g").unwrap();
        assert!(report.ok, "{report:?}");
        assert_eq!(report.entries, 3);

        // Tamper entry seq=1: flip its stored line/hash bytes directly in the table.
        {
            let wtx = db.begin_write().unwrap();
            {
                let mut audit = wtx.open_table(AUDIT).unwrap();
                let original = audit.get(("g", 1u64)).unwrap().unwrap().value().to_vec();
                let mut mutated = original.clone();
                let last = mutated.len() - 1;
                mutated[last] ^= 0xFF;
                audit.insert(("g", 1u64), mutated.as_slice()).unwrap();
            }
            wtx.commit().unwrap();
        }

        let broken = verify_audit(&db, "g").unwrap();
        assert!(!broken.ok, "tamper undetected");
        assert_eq!(broken.first_broken_seq, Some(1), "wrong break position");
    }

    /// A throwaway temp dir under the scratch space.
    fn tempdir() -> std::path::PathBuf {
        let base = std::env::temp_dir().join(format!(
            "eg-sec-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }
}
