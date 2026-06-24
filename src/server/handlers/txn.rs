//! Multi-op OCC ACID transaction handler (CONCEPT:KG-2.180).
//!
//! Owns the `Txn*`/`BeginTxn`/`Commit`/`Rollback` methods. These are STATEFUL
//! (they read/write `ServerState::open_txns`), so unlike the per-graph handlers
//! they take `state`. The staged ops never touch the graph or persistence; the
//! topology write lock is taken ONCE, at commit, where the OCC read-set is
//! validated and the staged write-set applied through a single `GraphTxn`.
//!
//! Composition with the write coalescer (CONCEPT:KG-2.182): staged ops are applied
//! directly via `GraphTxn` at commit and never enter the coalescer's queue, so
//! there is no interaction or deadlock with the per-graph write worker — that
//! worker only batches NON-transactional single-op writes. A long-open txn holds
//! NO lock at all (begin/stage take only the cheap `open_txns` DashMap + per-entry
//! Mutex), so client think-time never blocks readers or writers.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::access::check_graph_access;
use super::super::state::ServerState;
use super::super::txn::{now_ms, parse_isolation, GraphTxnState};
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};

/// Handle the transaction methods. Returns `Err(method)` for any non-txn method so
/// the dispatch chain falls through to the next handler (routing convention).
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::BeginTxn { graph, isolation } => {
            Ok(begin_txn(state, req_id, caller, graph, isolation.as_deref()).await)
        }
        Method::TxnAddNode {
            txn_id,
            node_id,
            properties_msgpack,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            Method::AddNode {
                node_id,
                properties_msgpack,
            },
        )
        .await),
        Method::TxnRemoveNode { txn_id, node_id } => {
            Ok(stage(state, req_id, &txn_id, Method::RemoveNode { node_id }).await)
        }
        Method::TxnAddEdge {
            txn_id,
            source_id,
            target_id,
            properties_msgpack,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            },
        )
        .await),
        Method::TxnRemoveEdge {
            txn_id,
            source_id,
            target_id,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            Method::RemoveEdge {
                source_id,
                target_id,
            },
        )
        .await),
        Method::TxnCas {
            txn_id,
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            },
        )
        .await),
        Method::Commit { txn_id } => Ok(commit(state, req_id, caller, &txn_id).await),
        Method::Rollback { txn_id } => Ok(rollback(state, req_id, &txn_id).await),
        other => Err(other),
    }
}

/// `BeginTxn`: resolve the target graph, enforce the per-graph + per-agent open-txn
/// caps, snapshot the OCC begin-version, and register a fresh staged transaction.
/// Returns the server-issued `txn_id` as a `String` payload.
async fn begin_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    graph: Option<String>,
    isolation: Option<&str>,
) -> Response {
    // Parse the isolation hint up front (CONCEPT:KG-2.183); an unknown value is
    // rejected before any graph/ACL work so the contract is unambiguous.
    let (level, predicate) = match parse_isolation(isolation) {
        Ok(parsed) => parsed,
        Err(msg) => return Response::err(req_id, msg),
    };
    // The request envelope's graph is the default target; `graph` overrides it.
    let s = state.read().await;
    let graph_name = graph.unwrap_or_default();
    let graph_name = if graph_name.is_empty() {
        return Response::err(req_id, "BeginTxn requires a target graph");
    } else {
        graph_name
    };
    let entry = match s.registry.get(&graph_name) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
    };
    // A txn stages writes → require Write access up front (same gate the inline
    // write path applies), so an unauthorized caller cannot even open a txn.
    if let Err(denied) = check_graph_access(
        &s.isolation,
        caller,
        &graph_name,
        entry.graph_type,
        entry.owner.as_deref(),
        AccessLevel::Write,
    ) {
        return Response::err(req_id, denied);
    }
    let begin_version = entry.core.version();

    // Open-txn caps (CONCEPT:KG-2.180): bound memory the way per_graph_inflight
    // bounds request concurrency. Count current open txns for this graph/agent.
    let agent = caller.unwrap_or("").to_string();
    let (mut for_graph, mut for_agent) = (0usize, 0usize);
    for e in s.open_txns.iter() {
        let t = e.value().lock();
        if t.graph == graph_name {
            for_graph += 1;
        }
        if t.agent == agent {
            for_agent += 1;
        }
    }
    if for_graph >= s.txn_max_per_graph {
        return Response::err(
            req_id,
            format!("too many open transactions for graph '{}'", graph_name),
        );
    }
    if for_agent >= s.txn_max_per_agent {
        return Response::err(req_id, "too many open transactions for agent");
    }

    let txn_id = s.txn_id_gen.next();
    // Under serializable with a declared predicate, the constructor captures the
    // predicate read-set fingerprint against `entry.core` at begin (the snapshot the
    // txn reads against). Snapshot level captures nothing extra.
    s.open_txns.insert(
        txn_id.clone(),
        parking_lot::Mutex::new(GraphTxnState::new(
            &entry.core,
            graph_name,
            begin_version,
            level,
            predicate,
            agent,
            now_ms(),
        )),
    );
    Response::ok(req_id, ResultPayload::String(txn_id))
}

/// Stage one durable mutation into the open txn (no graph/persistence touch).
/// Acks `Bool(true)`; errors if the txn id is unknown (expired/committed/rolled).
async fn stage(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    op: Method,
) -> Response {
    let s = state.read().await;
    // Resolve the txn's target core so we can capture the OCC read-set fingerprint
    // of every node the op references at staging time.
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let graph = entry.value().lock().graph.clone();
    let core = match s.registry.get(&graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", graph)),
    };
    entry.value().lock().stage(&core, op, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// `Commit`: the OCC serialization point. Validate the read-set under the held
/// topology write guard; on success apply the staged write-set atomically through
/// ONE `GraphTxn`, drop the guard, then persist each staged method and mark dirty
/// (which bumps the version). On conflict NOTHING is applied/persisted — a true
/// rollback returning `Bool(false)`.
///
/// **Cross-shard routing (CONCEPT:KG-2.222).** A `GraphTxnState` targets exactly ONE
/// graph, which resolves to ONE Raft group ([`crate::raft::multi::GroupRouter`]), so
/// EVERY `Commit` here is single-group and stays on this byte-for-byte-unchanged fast
/// path. A transaction whose write-set spans graphs in ≥2 groups
/// (`GroupRouter::is_cross_shard`) is a CROSS-SHARD txn: it routes through the 2PC
/// [`crate::raft::cross_shard_txn::CrossShardCoordinator`] instead. Accumulating a
/// multi-graph staged write-set into one `BeginTxn` is the remaining wire step (it
/// needs a multi-graph txn model); the coordinator + the span gate + the durable 2PC
/// records + recovery are built and proven by the `raft harness` gauntlet.
async fn commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
) -> Response {
    let s = state.read().await;
    // Remove the txn up front — commit consumes it whether it succeeds or conflicts
    // (a conflicted txn is discarded; the client re-begins).
    let (_id, txn_mutex) = match s.open_txns.remove(txn_id) {
        Some(pair) => pair,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let txn = txn_mutex.into_inner();

    let entry = match s.registry.get(&txn.graph) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Graph '{}' not found", txn.graph)),
    };
    // Re-check Write access at commit (caller may differ from the opener; the gate
    // is cheap and keeps the contract identical to the inline write path).
    if let Err(denied) = check_graph_access(
        &s.isolation,
        caller,
        &txn.graph,
        entry.graph_type,
        entry.owner.as_deref(),
        AccessLevel::Write,
    ) {
        return Response::err(req_id, denied);
    }
    let core = entry.core.clone();
    let persistence = s.persistence.clone();
    let graph_name = txn.graph.clone();
    drop(s); // release the registry read lock before taking the graph write lock.

    // ── Validate-and-apply under ONE topology write guard (the serialization
    // point). This is the ONLY place a transaction holds topo.write(); it is held
    // for the apply only, never across client think-time. ──
    let applied: Vec<Method> = {
        let mut gtxn = core.txn(); // ← topo.write(): OCC commit barrier.
        if !txn.validate(&core) {
            // Conflict: drop the guard, apply nothing, persist nothing. True
            // rollback — the staged write-set is discarded with `txn`.
            return Response::ok(req_id, ResultPayload::Bool(false));
        }
        let mut applied = Vec::with_capacity(txn.write_set.len());
        for m in txn.write_set {
            apply_staged(&mut gtxn, &m);
            applied.push(m);
        }
        applied
        // guard drops here.
    };

    // Bump the version + invalidate caches (one bump per committed txn, regardless
    // of op count) so a concurrent OCC txn sees this commit landed.
    core.mark_dirty();

    // Durability: record each staged method through the configured backend (the
    // same write-behind seam the inline dispatch path uses). The redb backend
    // group-commits; snapshot_wal WAL-appends. In-memory atomicity is already
    // guaranteed by the single GraphTxn above. (A single redb WriteTransaction per
    // commit — a true durability barrier — is a future M2/authoritative
    // enhancement; M6 persists per staged op at commit.)
    if let Some(p) = persistence {
        let fname = crate::persist::sanitize(&graph_name);
        for m in &applied {
            if crate::wal::is_durable_mutation(m) {
                p.record(&fname, m);
            }
        }
    }

    #[cfg(feature = "metrics")]
    {
        let topo = core.topo.read();
        crate::metrics::set_graph_size(
            &graph_name,
            topo.graph.node_count() as i64,
            topo.graph.edge_count() as i64,
        );
    }

    Response::ok(req_id, ResultPayload::Bool(true))
}

/// `Rollback`: discard the staged transaction. Nothing was ever applied or
/// persisted, so this only drops the in-memory state. Returns `Bool(true)` when a
/// txn was removed; an unknown id (already committed/rolled-back/expired) is
/// reported so the client knows the id is gone.
async fn rollback(state: &Arc<RwLock<ServerState>>, req_id: u64, txn_id: &str) -> Response {
    let s = state.read().await;
    if s.open_txns.remove(txn_id).is_some() {
        Response::ok(req_id, ResultPayload::Bool(true))
    } else {
        Response::err(req_id, format!("unknown transaction '{}'", txn_id))
    }
}

/// Apply one staged durable mutation through the held `GraphTxn` (the same engine
/// primitives the write coalescer uses, so behavior is identical). Errors from
/// add_edge / a failed CAS are NOT surfaced per-op in M6: a staged add_edge to a
/// missing endpoint is a no-op (its endpoints were validated into the read-set; if
/// absent the edge simply isn't added), matching the inline best-effort contract.
fn apply_staged(gtxn: &mut crate::graph::GraphTxn<'_>, method: &Method) {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => gtxn.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::RemoveNode { node_id } => gtxn.remove_node(node_id.clone()),
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let _ = gtxn.add_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            );
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => gtxn.remove_edge(source_id.clone(), target_id.clone()),
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Decode the condition/update maps; a decode failure is a no-op CAS
            // (the inline path returns Bool(false) and touches nothing).
            if let (Ok(conditions), Ok(updates)) = (
                rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                    conditions_msgpack,
                ),
                rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                    updates_msgpack,
                ),
            ) {
                let _ = gtxn.compare_and_set_fields(node_id, &conditions, &updates);
            }
        }
        // Only durable mutations are ever staged (the protocol restricts Txn* to
        // this set); any other variant here is unreachable.
        _ => {}
    }
}
