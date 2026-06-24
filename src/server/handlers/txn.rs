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
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::AddNode {
                node_id,
                properties_msgpack,
            },
        )
        .await),
        Method::TxnRemoveNode {
            txn_id,
            node_id,
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::RemoveNode { node_id },
        )
        .await),
        Method::TxnAddEdge {
            txn_id,
            source_id,
            target_id,
            properties_msgpack,
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
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
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
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
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            },
        )
        .await),
        Method::TxnAddEmbedding {
            txn_id,
            node_id,
            embedding,
            graph,
        } => Ok(stage_vector(state, req_id, &txn_id, graph.as_deref(), node_id, embedding).await),
        Method::TxnBlobRef {
            txn_id,
            node_id,
            digest,
            graph,
        } => Ok(stage_blob_ref(state, req_id, &txn_id, graph.as_deref(), node_id, digest).await),
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
///
/// `target_graph` (CONCEPT:KG-2.226): when `None` (or equal to the txn's default
/// graph) the op stages against the default graph through the single-graph OCC
/// read-set path — unchanged. When it names a DIFFERENT graph, the op accumulates in
/// the txn's multi-graph `extra_writes`; the default `core` is still passed so a
/// same-graph op keeps its read-set fingerprint, but a cross-graph op needs no
/// default-core read-set (the 2PC coordinator validates each participant slice).
async fn stage(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    op: Method,
) -> Response {
    let s = state.read().await;
    // Resolve the txn's target core so we can capture the OCC read-set fingerprint
    // of every node the op references at staging time.
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    let target = target_graph.unwrap_or(&default_graph).to_string();
    // A cross-graph op must reference a real graph (it will be a participant at
    // commit). Validate existence up front so a typo fails at stage, not commit.
    if !s.registry.exists(&target) {
        return Response::err(req_id, format!("Graph '{}' not found", target));
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    entry.value().lock().stage_in(&core, &target, op, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Stage a VECTOR upsert into the txn's cross-modal write-set (CONCEPT:KG-2.225). The
/// one-`WriteTransaction` cross-modal barrier is per-graph, so a vector targets the
/// txn's DEFAULT graph; a `graph` naming anything else is rejected (cross-graph
/// cross-modal is a documented follow-up). Acks `Bool(true)`.
async fn stage_vector(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    node_id: String,
    embedding: Vec<f32>,
) -> Response {
    let s = state.read().await;
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Response::err(
                req_id,
                "cross-modal vector must target the txn's default graph",
            );
        }
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    entry
        .value()
        .lock()
        .stage_vector(&core, node_id, embedding, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Stage a BLOB REFERENCE into the txn's cross-modal write-set (CONCEPT:KG-2.225). Same
/// per-graph constraint as [`stage_vector`]. Acks `Bool(true)`.
async fn stage_blob_ref(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    node_id: String,
    digest: String,
) -> Response {
    let s = state.read().await;
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Response::err(
                req_id,
                "cross-modal blob-ref must target the txn's default graph",
            );
        }
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    entry
        .value()
        .lock()
        .stage_blob_ref(&core, node_id, digest, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// `Commit`: the OCC serialization point. Validate the read-set under the held
/// topology write guard; on success apply the staged write-set atomically through
/// ONE `GraphTxn`, drop the guard, then persist each staged method and mark dirty
/// (which bumps the version). On conflict NOTHING is applied/persisted — a true
/// rollback returning `Bool(false)`.
///
/// **Cross-shard routing (CONCEPT:KG-2.222 + KG-2.226).** A txn whose staged ops all
/// target ONE graph (or graphs that resolve to ONE Raft group) stays on this
/// byte-for-byte-unchanged single-group FAST PATH. A MULTI-GRAPH txn whose staged
/// write-set ([`crate::server::txn::GraphTxnState::extra_writes`]) spans graphs in ≥2
/// Raft groups (`GroupRouter::is_cross_shard`) is a CROSS-SHARD txn: `Commit` routes it
/// through the 2PC [`crate::raft::cross_shard_txn::CrossShardCoordinator`]
/// ([`commit_multi_graph`]). The coordinator, the span gate, the durable 2PC records,
/// and recovery are the `raft harness`-proven Lane N machinery; THIS is the
/// user-facing wire that hands a staged multi-graph write-set to it.
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

    // ── Multi-graph span (CONCEPT:KG-2.226 — Lane N) ────────────────────────────
    // If the txn staged ops against a graph other than its default, evaluate the
    // span against the router. A span over ≥2 Raft groups routes through the 2PC
    // coordinator (cross-shard, all-or-nothing across groups). A multi-graph span
    // that collapses to ONE group, OR no active cluster (incl. a non-raft build),
    // applies each graph's slice locally so no staged graph is silently dropped.
    if txn.is_multi_graph() {
        drop(s);
        return commit_multi_graph(state, req_id, caller, txn).await;
    }

    // ── Cross-modal span (CONCEPT:KG-2.225) ─────────────────────────────────────
    // If the txn staged vectors or blob-refs, its single-graph commit must land
    // graph + vectors + blob-refs in ONE redb WriteTransaction (all-or-nothing).
    if txn.is_cross_modal() {
        drop(s);
        return commit_cross_modal(state, req_id, caller, txn).await;
    }

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

/// Commit a CROSS-MODAL single-graph transaction (CONCEPT:KG-2.225). Validates the OCC
/// read-set, then lands the graph methods + vector upserts + blob-refs ATOMICALLY in
/// ONE redb `WriteTransaction` (commit-before-ack) BEFORE touching the in-memory model,
/// so a durable-commit failure applies NOTHING (no partial cross-modal commit) and the
/// in-memory state only ever reflects what is durable. On OCC conflict returns
/// `Bool(false)` (true rollback); on durable failure returns an ERROR.
async fn commit_cross_modal(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn: GraphTxnState,
) -> Response {
    let (core, persistence, graph_type, owner) = {
        let s = state.read().await;
        let entry = match s.registry.get(&txn.graph) {
            Some(e) => e,
            None => return Response::err(req_id, format!("Graph '{}' not found", txn.graph)),
        };
        (
            entry.core.clone(),
            s.persistence.clone(),
            entry.graph_type,
            entry.owner.clone(),
        )
    };
    // Re-check Write access at commit.
    {
        let s = state.read().await;
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            &txn.graph,
            graph_type,
            owner.as_deref(),
            AccessLevel::Write,
        ) {
            return Response::err(req_id, denied);
        }
    }

    // ── OCC validation under the topology write guard (read-only check) ──
    {
        let gtxn = core.txn(); // serialization barrier vs concurrent writers
        let ok = txn.validate(&core);
        drop(gtxn); // release the barrier before returning / the durable commit
        if !ok {
            // Conflict: nothing applied, nothing persisted — true rollback.
            return Response::ok(req_id, ResultPayload::Bool(false));
        }
    }

    let fname = crate::persist::sanitize(&txn.graph);
    let methods: Vec<Method> = txn.write_set.clone();

    // ── THE ATOMIC COMMIT POINT: land ALL modalities in ONE redb WriteTransaction ──
    // commit-before-ack. A failure here means NOTHING is durable AND we apply nothing
    // in-memory → the whole cross-modal txn rolls back (no partial).
    let Some(p) = persistence else {
        return Response::err(req_id, "cross-modal txn requires a persistence backend");
    };
    if let Err(e) = p
        .commit_crossmodal(&fname, &methods, &txn.vectors, &txn.blob_refs)
        .await
    {
        return Response::err(req_id, format!("cross-modal commit failed: {e}"));
    }

    // ── Durable commit succeeded → reflect it in the in-memory model ──
    {
        let mut gtxn = core.txn();
        for m in &methods {
            apply_staged(&mut gtxn, m);
        }
    }
    // Blob refs: mirror the durable `__blob__` property onto the in-memory node so a
    // RAM read matches redb (the durable row already carries it).
    for (node_id, digest) in &txn.blob_refs {
        if let Some(blob) = core.get_node_properties(node_id) {
            if let Ok(mut props) =
                rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(&blob)
            {
                props.insert(
                    "__blob__".to_string(),
                    serde_json::Value::String(digest.clone()),
                );
                if let Ok(updated) = rmp_serde::to_vec_named(&props) {
                    core.add_node(node_id.clone(), updated);
                }
            }
        }
    }
    // Vectors: add to the in-memory semantic store (the durable SEMANTIC blob already
    // carries them).
    {
        let mut store = core.semantic_store.write();
        for (node_id, embedding) in &txn.vectors {
            store.add_embedding(node_id.clone(), embedding.clone());
        }
    }
    core.mark_dirty();

    Response::ok(req_id, ResultPayload::Bool(true))
}

/// One graph's slice of a multi-graph txn (CONCEPT:KG-2.226), resolved at commit:
/// name + sanitized fname + type + the staged ops for it.
struct CommitSlice {
    graph_name: String,
    graph_fname: String,
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    graph_type: crate::protocol::GraphType,
    methods: Vec<Method>,
}

/// Commit a MULTI-GRAPH staged transaction (CONCEPT:KG-2.226 — Lane N wire). Builds a
/// per-graph slice from the default-graph write-set + each `extra_writes` graph
/// (validating existence + Write access on each), then:
///
///   * **Cross-shard (≥2 Raft groups) + active cluster** → route the staged write-set
///     through [`crate::raft::cross_shard_txn::CrossShardCoordinator::commit_cross_shard`]:
///     the 2PC coordinator prepares each participant group durably (commit-before-vote),
///     logs ONE durable decision (the atomic commit point), then applies every slice
///     through its group's Raft `client_write`. All-or-nothing across groups,
///     recovery-resolvable. `Bool(true)` on COMMIT, `Bool(false)` on ABORT.
///   * **Single-group collapse, OR no active cluster (incl. a non-raft build)** →
///     apply each graph's slice locally through its own `GraphTxn` (per-graph atomic;
///     the same primitives the single-graph path uses), so no staged graph is dropped.
async fn commit_multi_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn: GraphTxnState,
) -> Response {
    // Build the per-graph slices: the default graph (its write_set) + every extra
    // graph. Each graph must exist + the caller must hold Write on it.
    let mut slices: Vec<CommitSlice> = Vec::new();
    {
        let s = state.read().await;
        let mut per_graph: Vec<(String, Vec<Method>)> =
            vec![(txn.graph.clone(), txn.write_set.clone())];
        for (g, ops) in &txn.extra_writes {
            per_graph.push((g.clone(), ops.clone()));
        }
        for (graph_name, methods) in per_graph {
            let entry = match s.registry.get(&graph_name) {
                Some(e) => e,
                None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
            };
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
            slices.push(CommitSlice {
                graph_fname: crate::persist::sanitize(&graph_name),
                graph_type: entry.graph_type,
                graph_name,
                methods,
            });
        }
    }

    // ── CROSS-SHARD: a multi-graph span over ≥2 Raft groups routes through 2PC ──
    #[cfg(feature = "raft")]
    {
        let multi = {
            let s = state.read().await;
            s.multi_raft.clone()
        };
        if let Some(multi) = multi {
            if multi.router().is_cross_shard(txn.touched_graphs()) {
                return commit_cross_shard(state, req_id, multi, slices).await;
            }
        }
    }

    // ── Single-group collapse OR no cluster: apply each slice locally ──
    apply_slices_locally(state, req_id, &slices).await
}

/// Route a cross-shard multi-graph txn through the 2PC coordinator (CONCEPT:KG-2.226).
#[cfg(feature = "raft")]
async fn commit_cross_shard(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    multi: std::sync::Arc<crate::raft::multi::MultiRaft>,
    slices: Vec<CommitSlice>,
) -> Response {
    use crate::raft::cross_shard_txn::{
        CrossShardCoordinator, CrossShardTxn, GraphSlice, TxnOutcome,
    };

    let backend = {
        let s = state.read().await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "cross-shard txn requires a persistence backend");
    };
    let x_slices: Vec<GraphSlice> = slices
        .into_iter()
        .map(|s| GraphSlice {
            graph_name: s.graph_name,
            graph_fname: s.graph_fname,
            graph_type: s.graph_type,
            methods: s.methods,
        })
        .collect();
    let coord = CrossShardCoordinator::new(multi, backend);
    let xtxn = CrossShardTxn {
        txn_id: format!("user-{}", txn_id_suffix()),
        slices: x_slices,
    };
    match coord.commit_cross_shard(&xtxn).await {
        Ok(TxnOutcome::Committed) => Response::ok(req_id, ResultPayload::Bool(true)),
        Ok(TxnOutcome::Aborted) => Response::ok(req_id, ResultPayload::Bool(false)),
        Err(e) => Response::err(req_id, format!("cross-shard commit failed: {e}")),
    }
}

/// Apply each graph's slice locally through its own `GraphTxn` (CONCEPT:KG-2.226 — the
/// single-group / single-node multi-graph path). Per-graph atomic; used when the span
/// collapses to one Raft group or no cluster is active. Returns `Bool(true)`.
async fn apply_slices_locally(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    slices: &[CommitSlice],
) -> Response {
    for slice in slices {
        let (core, persistence) = {
            let s = state.read().await;
            let core = match s.registry.get(&slice.graph_name) {
                Some(e) => e.core.clone(),
                None => {
                    return Response::err(req_id, format!("Graph '{}' not found", slice.graph_name))
                }
            };
            (core, s.persistence.clone())
        };
        {
            let mut gtxn = core.txn();
            for m in &slice.methods {
                apply_staged(&mut gtxn, m);
            }
        }
        core.mark_dirty();
        if let Some(p) = persistence {
            for m in &slice.methods {
                if crate::wal::is_durable_mutation(m) {
                    p.record(&slice.graph_fname, m);
                }
            }
        }
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// A process-unique suffix for a user cross-shard txn id (no `rand`/`Date` dep — a
/// monotonic atomic, like `TxnIdGen`). Keeps the durable 2PC records uniquely keyed.
#[cfg(feature = "raft")]
fn txn_id_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed) + 1;
    format!("{:016x}-{}", n, std::process::id())
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
