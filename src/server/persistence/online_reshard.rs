//! Online single-node per-tenant resharding execution (CONCEPT:EG-KG.backend.catalog-shard-resolve, M3 keystone).
//!
//! ## What it solves
//!
//! [`super::shard_migrate`] (CONCEPT:EG-KG.sharding.atomic-shard-swap) moves shards OFFLINE — the engine must be
//! stopped because it rewrites the whole store to a new uniform K. This module moves ONE
//! graph between shards while the engine RUNS, with no data loss and no stop: the graph's
//! rows are copied verbatim to the destination shard, the [`super::tenant_catalog`]
//! (CONCEPT:EG-KG.sharding.empty-catalog-routing) route is flipped so reads/writes follow the graph to its new shard,
//! and the source rows are GC'd — all without touching any OTHER graph's writers.
//!
//! ## Correctness — the same verbatim row copy as EG-030
//!
//! Like the offline tool, the per-graph copy is **verbatim** (stored bytes are NOT
//! decoded / unsealed / re-derived):
//!
//! * Per-graph data (`NODES`/`EDGES`/`LEDGER`/`SEMANTIC`/`GRAPH_META`) is moved row for
//!   row, value blob unchanged — so encryption-at-rest blobs survive WITHOUT the key.
//! * The tamper-evident hash-chained `AUDIT` log (CONCEPT:EG-KG.sharding.row-level-security) is copied verbatim
//!   `(graph, seq) -> blob`, so the chain stays verifiable (re-deriving would break it).
//!
//! ## Correctness — the quiesce / flip window
//!
//! The move is driven by [`RedbBackend::reshard_graph`](super::redb_backend::RedbBackend),
//! which holds the backend's `routing_epoch` WRITE guard for the duration of the move.
//! Every catalog-attached durable write (`record_durable` / `commit_crossmodal`) resolves
//! its shard and enqueues its op while holding a SHARED `routing_epoch` READ guard, so the
//! exclusive flip cannot interleave: when the flip holds the write guard, no write is
//! mid-resolve, and once it releases, every subsequent write resolves the catalog AFTER
//! the route flip — so a write is never lost or routed to the stale shard. The ordering
//! inside the move — `import(dst) durably committed -> catalog flip durable -> purge(src)`
//! — is crash-consistent: a crash before the flip leaves the data in BOTH shards with the
//! route still on `src` (reads find it on `src`); a crash after the flip leaves the route
//! on `dst` where `import` already landed the data; the `src` GC is the last, idempotent
//! step. See `RedbBackend::reshard_graph` for the quiesce wiring and the round-trip tests.

use std::sync::mpsc::SyncSender;

use redb::{Database, Durability, ReadableDatabase};

use super::redb_backend::Cmd;
use super::tenant_catalog::TenantCatalog;
#[cfg(feature = "security")]
use crate::redb_store::AUDIT;
use crate::redb_store::{EDGES, GRAPH_META, LEDGER, NODES, SEMANTIC};

/// One graph's durable rows captured VERBATIM for an online shard move (CONCEPT:EG-KG.backend.catalog-shard-resolve).
/// Value blobs are the raw on-disk bytes (encrypted if encryption-at-rest is on, the
/// audit chain untouched) so re-inserting them on the destination shard preserves both
/// encryption and audit-chain verifiability — exactly as EG-030's offline copy does.
#[derive(Default, Clone)]
pub(crate) struct RawGraphRows {
    /// `graph_meta` identity blob (`{name, graph_type}`), or `None` if the graph has no
    /// durable identity (nothing to move — the reshard becomes a pure route flip).
    pub meta: Option<Vec<u8>>,
    /// `(node_id, raw_value_blob)`.
    pub nodes: Vec<(String, Vec<u8>)>,
    /// `(src_id, tgt_id, ordinal, raw_value_blob)`.
    pub edges: Vec<(String, String, u32, Vec<u8>)>,
    /// `(seq, ledger_line)`.
    pub ledger: Vec<(u64, String)>,
    /// The semantic-store blob, if any.
    pub semantic: Option<Vec<u8>>,
    /// `(seq, chained_audit_blob)` — copied verbatim to keep the hash chain valid.
    #[cfg(feature = "security")]
    pub audit: Vec<(u64, Vec<u8>)>,
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
    /// `Some(Some(blob))` = re-write the semantic blob; `Some(None)` = it was cleared;
    /// `None` = unchanged.
    #[allow(clippy::option_option)]
    pub semantic: Option<Option<Vec<u8>>>,
    #[cfg(feature = "security")]
    pub upsert_audit: Vec<(u64, Vec<u8>)>,
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

/// Diff the LATEST source rows against the BULK snapshot already on `dst` (CONCEPT:EG-KG.backend.flush-pending-first).
/// Emits only what changed: new/changed-blob rows to upsert + vanished rows to remove.
pub(crate) fn compute_delta(bulk: &RawGraphRows, latest: &RawGraphRows) -> RawGraphDelta {
    use std::collections::HashMap;
    let mut delta = RawGraphDelta::default();

    if bulk.meta != latest.meta {
        delta.meta = latest.meta.clone();
    }
    if bulk.semantic != latest.semantic {
        delta.semantic = Some(latest.semantic.clone());
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

    delta
}

/// Apply a [`RawGraphDelta`] to the destination `Database` in ONE durable transaction
/// (CONCEPT:EG-KG.backend.flush-pending-first). Runs on the destination shard's writer thread (via
/// [`Cmd::ImportGraphDelta`](super::redb_backend::Cmd)); the single `Durability::Immediate`
/// commit is the delta's commit-before-flip point. O(delta) rows — the short under-quiesce
/// write, vs the full O(graph) copy the bulk pass already did unquiesced.
pub(crate) fn import_graph_delta(
    db: &Database,
    graph: &str,
    delta: &RawGraphDelta,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        if let Some(blob) = &delta.meta {
            let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
            meta.insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        for (id, blob) in &delta.upsert_nodes {
            nodes
                .insert((graph, id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        for id in &delta.remove_nodes {
            nodes
                .remove((graph, id.as_str()))
                .map_err(|e| e.to_string())?;
        }
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        for (s, t, o, blob) in &delta.upsert_edges {
            edges
                .insert((graph, s.as_str(), t.as_str(), *o), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        for (s, t, o) in &delta.remove_edges {
            edges
                .remove((graph, s.as_str(), t.as_str(), *o))
                .map_err(|e| e.to_string())?;
        }
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (seq, line) in &delta.upsert_ledger {
            ledger
                .insert((graph, *seq), line.as_str())
                .map_err(|e| e.to_string())?;
        }
        if let Some(sem) = &delta.semantic {
            let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            match sem {
                Some(blob) => {
                    semantic
                        .insert(graph, blob.as_slice())
                        .map_err(|e| e.to_string())?;
                }
                None => {
                    semantic.remove(graph).map_err(|e| e.to_string())?;
                }
            }
        }
        #[cfg(feature = "security")]
        {
            let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
            for (seq, blob) in &delta.upsert_audit {
                audit
                    .insert((graph, *seq), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Scan ONE graph's durable rows VERBATIM off a `Database` (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on the
/// owning shard's writer thread (via [`Cmd::ExportGraphRaw`]) AFTER it has flushed pending
/// writes, so the snapshot reflects every committed mutation. Value blobs are copied raw
/// (no `crypto.unseal`) so encryption-at-rest and the audit chain survive the move.
pub(crate) fn export_graph_raw(db: &Database, graph: &str) -> Result<RawGraphRows, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let mut out = RawGraphRows::default();

    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    out.meta = meta_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec());

    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    for row in nodes_table
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        if g != graph {
            break;
        }
        out.nodes.push((id.to_string(), v.value().to_vec()));
    }

    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    for row in edges_table
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, o) = k.value();
        if g != graph {
            break;
        }
        out.edges
            .push((s.to_string(), t.to_string(), o, v.value().to_vec()));
    }

    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    for row in ledger_table
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, seq) = k.value();
        if g != graph {
            break;
        }
        out.ledger.push((seq, v.value().to_string()));
    }

    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    out.semantic = semantic_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec());

    #[cfg(feature = "security")]
    {
        let audit_table = rtx.open_table(AUDIT).map_err(|e| e.to_string())?;
        for row in audit_table
            .range((graph, 0u64)..)
            .map_err(|e| e.to_string())?
        {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, seq) = k.value();
            if g != graph {
                break;
            }
            out.audit.push((seq, v.value().to_vec()));
        }
    }

    Ok(out)
}

/// Insert ONE graph's verbatim rows into a destination `Database` in ONE durable
/// transaction (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on the destination shard's writer thread (via
/// [`Cmd::ImportGraphRaw`]); the single `Durability::Immediate` commit is the
/// commit-before-ack point of the move. Idempotent (keyed upserts), so a re-run after an
/// interrupted move overwrites rather than duplicates.
pub(crate) fn import_graph_raw(
    db: &Database,
    graph: &str,
    rows: &RawGraphRows,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        if let Some(blob) = &rows.meta {
            meta.insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        for (id, blob) in &rows.nodes {
            nodes
                .insert((graph, id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        for (s, t, o, blob) in &rows.edges {
            edges
                .insert((graph, s.as_str(), t.as_str(), *o), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (seq, line) in &rows.ledger {
            ledger
                .insert((graph, *seq), line.as_str())
                .map_err(|e| e.to_string())?;
        }
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        if let Some(blob) = &rows.semantic {
            semantic
                .insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        #[cfg(feature = "security")]
        {
            let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
            for (seq, blob) in &rows.audit {
                audit
                    .insert((graph, *seq), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Export the graph's rows VERBATIM off the source shard's flush-then-scan snapshot.
fn export_from(src_tx: &SyncSender<Cmd>, graph: &str) -> Result<RawGraphRows, String> {
    let (rtx, rrx) = std::sync::mpsc::channel();
    src_tx
        .send(Cmd::ExportGraphRaw {
            graph: graph.to_string(),
            reply: rtx,
        })
        .map_err(|_| "redb writer thread is gone".to_string())?;
    rrx.recv()
        .map_err(|_| "redb writer dropped export reply".to_string())?
}

/// PHASE 1 of the online move (CONCEPT:EG-KG.backend.flush-pending-first, R1 delta-copy) — the BULK pass, run WITHOUT
/// the exclusive routing quiesce so writes keep flowing to `src` and the graph is NOT
/// paused. Export the graph's rows verbatim off a src snapshot and import them into `dst`
/// in one durable commit. Returns the bulk snapshot so [`delta_flip_purge`] can diff the
/// final state against it under the quiesce. Idempotent (keyed upserts) — a re-run after an
/// interrupted move overwrites rather than duplicates.
pub(crate) fn bulk_copy(
    src_tx: &SyncSender<Cmd>,
    dst_tx: &SyncSender<Cmd>,
    graph: &str,
) -> Result<RawGraphRows, String> {
    let gone = || "redb writer thread is gone".to_string();
    let rows = export_from(src_tx, graph)?;
    let (itx, irx) = std::sync::mpsc::channel();
    dst_tx
        .send(Cmd::ImportGraphRaw {
            graph: graph.to_string(),
            rows: Box::new(rows.clone()),
            reply: itx,
        })
        .map_err(|_| gone())?;
    irx.recv()
        .map_err(|_| "redb writer dropped import reply".to_string())??;
    Ok(rows)
}

/// PHASE 2 of the online move (CONCEPT:EG-KG.backend.flush-pending-first, R1) — run on a blocking thread WHILE the
/// exclusive `routing_epoch` WRITE guard is held, so writes to this graph are quiesced for
/// only this short window. Re-export the LATEST source state, copy just the DELTA accrued
/// since the bulk pass into `dst`, flip the catalog route, then GC the source. The pause is
/// O(delta) (the under-quiesce import) rather than O(graph). Crash-consistent ordering:
/// `import(delta) committed -> catalog flip durable -> purge(src)`.
pub(crate) fn delta_flip_purge(
    src_tx: &SyncSender<Cmd>,
    dst_tx: &SyncSender<Cmd>,
    catalog: &TenantCatalog,
    graph: &str,
    src_idx: usize,
    dst_idx: usize,
    bulk: RawGraphRows,
) -> Result<ReshardReport, String> {
    let gone = || "redb writer thread is gone".to_string();

    // 1. Re-export the latest source state (it now reflects every write that landed during
    //    the unquiesced bulk pass) and diff it against the bulk snapshot.
    let latest = export_from(src_tx, graph)?;
    let delta = compute_delta(&bulk, &latest);
    let mut report = ReshardReport::counts(graph, src_idx, dst_idx, &latest);
    report.delta_nodes = delta.nodes();
    report.delta_edges = delta.edges();

    // 2. Import ONLY the delta into `dst`, awaiting the durable commit.
    let (itx, irx) = std::sync::mpsc::channel();
    dst_tx
        .send(Cmd::ImportGraphDelta {
            graph: graph.to_string(),
            delta: Box::new(delta),
            reply: itx,
        })
        .map_err(|_| gone())?;
    irx.recv()
        .map_err(|_| "redb writer dropped delta import reply".to_string())??;

    // 3. Flip the durable route — reads/writes now follow the graph to `dst` (preserving
    //    any cluster-node placement). The atomic commit point of the move.
    let node = catalog.lookup(graph).and_then(|a| a.node);
    catalog.assign(graph, dst_idx as u32, node)?;

    // 4. GC the now-orphaned source rows (idempotent purge). After the flip no writer can
    //    route to `src` for this graph, so the purge can never race a live write.
    let (done, done_rx) = tokio::sync::oneshot::channel();
    src_tx
        .send(Cmd::PurgeGraph {
            graph: graph.to_string(),
            done,
        })
        .map_err(|_| gone())?;
    done_rx
        .blocking_recv()
        .map_err(|_| "redb writer dropped purge reply".to_string())??;

    Ok(report)
}
