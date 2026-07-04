//! Multi-op OCC ACID transaction handler (CONCEPT:EG-KG.txn.multi-op-occ-acid).
//!
//! Owns the `Txn*`/`BeginTxn`/`Commit`/`Rollback` methods. These are STATEFUL
//! (they read/write `ServerState::open_txns`), so unlike the per-graph handlers
//! they take `state`. The staged ops never touch the graph or persistence; the
//! topology write lock is taken ONCE, at commit, where the OCC read-set is
//! validated and the staged write-set applied through a single `GraphTxn`.
//!
//! Composition with the write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): staged ops are applied
//! directly via `GraphTxn` at commit and never enter the coalescer's queue, so
//! there is no interaction or deadlock with the per-graph write worker — that
//! worker only batches NON-transactional single-op writes. A long-open txn holds
//! NO lock at all (begin/stage take only the cheap `open_txns` DashMap + per-entry
//! Mutex), so client think-time never blocks readers or writers.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::access::check_graph_access;
use super::super::state::ServerState;
#[cfg(feature = "tsdb")]
use super::super::txn::StagedMeasurement;
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
        // Extended cross-modal staging (CONCEPT:EG-KG.backend.cross-modal-atomic-commit/361/362). Each arm is feature-gated
        // at the FACADE (tsdb/owl/sparql); in a slim build the variant falls through to
        // `other => Err(other)` → the dispatch "not available in this build" catch-all.
        #[cfg(feature = "tsdb")]
        Method::TxnAddMeasurement {
            txn_id,
            series,
            points,
            graph,
        } => Ok(stage_measurement(state, req_id, &txn_id, graph.as_deref(), series, points).await),
        #[cfg(feature = "owl")]
        Method::TxnAxiom {
            txn_id,
            turtle,
            graph,
        } => Ok(stage_axiom(state, req_id, &txn_id, graph.as_deref(), turtle).await),
        #[cfg(feature = "sparql")]
        Method::TxnConstruct {
            txn_id,
            sparql,
            graph,
        } => Ok(stage_construct(state, req_id, &txn_id, graph.as_deref(), sparql).await),
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
    // Parse the isolation hint up front (CONCEPT:EG-KG.txn.serializable-zero-cost); an unknown value is
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

    // Open-txn caps (CONCEPT:EG-KG.txn.multi-op-occ-acid): bound memory the way per_graph_inflight
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
/// `target_graph` (CONCEPT:EG-KG.txn.routes-cross-shard-txn): when `None` (or equal to the txn's default
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

/// Stage a VECTOR upsert into the txn's cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). The
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

/// Stage a BLOB REFERENCE into the txn's cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). Same
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

/// Default bucket width for a NEW cross-modal series (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). The Lane-0
/// `TxnAddMeasurement` wire carries only `series` + `points` (no schema), so a
/// brand-new series is materialized with this 1-hour partition; an EXISTING series'
/// stored meta is authoritative and this is ignored.
#[cfg(feature = "tsdb")]
const DEFAULT_MEASUREMENT_BUCKET_NS: u64 = 3_600_000_000_000;

/// Stage a TIME-SERIES measurement batch into the txn's cross-modal write-set
/// (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Same per-graph constraint as [`stage_vector`]: the batch targets the
/// txn's DEFAULT graph (the one-`WriteTransaction` barrier is per-graph). The points are
/// decoded here (the SAME `Vec<(i64, Vec<f64>)>` MessagePack shape `TsAppend` carries), so
/// the commit path is a pure durable append. Acks `Bool(true)`.
#[cfg(feature = "tsdb")]
async fn stage_measurement(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    series: String,
    points_msgpack: Vec<u8>,
) -> Response {
    let points: Vec<(i64, Vec<f64>)> = match rmp_serde::from_slice(&points_msgpack) {
        Ok(p) => p,
        Err(e) => return Response::err(req_id, format!("invalid measurement points: {e}")),
    };
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
                "cross-modal measurement must target the txn's default graph",
            );
        }
    }
    // Field width is inferred from the first point; an existing series' stored schema wins
    // at commit, so this only seeds a NEW series (with generated `f0..fN` names).
    let n_fields = points.first().map(|(_, v)| v.len()).unwrap_or(0);
    let field_names = (0..n_fields).map(|i| format!("f{i}")).collect();
    let measurement = StagedMeasurement {
        series,
        n_fields,
        bucket_ns: DEFAULT_MEASUREMENT_BUCKET_NS,
        field_names,
        points,
    };
    entry
        .value()
        .lock()
        .stage_measurement(measurement, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Lower a stream of RDF triples to graph-native `AddNode`/`AddEdge` methods
/// (CONCEPT:EG-KG.txn.extended-cross-modal/362), mirroring the canonical `eg_rdf::mapping::load_triples`
/// property-graph projection so the durable rows match the in-memory model:
///   * literal object  → a property `{predicate: literal-cell}` on the subject node;
///   * resource object → subject + object nodes + a typed edge `{"type": predicate}`,
///     and (for `rdf:type`) the subject's `type` label.
///
/// The SAME lowered `Vec<Method>` is applied both durably (`apply_method_rows`) and
/// in-memory (`apply_staged`), so the two are identical by construction.
#[cfg(feature = "sparql")]
pub(crate) fn triples_to_methods(triples: &[eg_rdf::oxrdf::Triple]) -> Vec<Method> {
    use eg_rdf::oxrdf::{NamedOrBlankNode, Term};
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    type Props = serde_json::Map<String, serde_json::Value>;

    let subject_id = |s: &NamedOrBlankNode| -> String {
        match s {
            NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
            NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
            #[allow(unreachable_patterns)]
            other => format!("{other}"),
        }
    };

    let mut node_props: std::collections::HashMap<String, Props> = std::collections::HashMap::new();
    // Preserve stage order for the edges so the emitted method list is deterministic.
    let mut edges: Vec<(String, String, String)> = Vec::new();
    for t in triples {
        let s_id = subject_id(&t.subject);
        let pred = t.predicate.as_str().to_string();
        node_props.entry(s_id.clone()).or_default();
        match &t.object {
            Term::Literal(lit) => {
                node_props
                    .entry(s_id.clone())
                    .or_default()
                    .entry(pred)
                    .or_insert_with(|| eg_rdf::mapping::literal_to_cell(lit));
            }
            Term::NamedNode(n) => {
                let o_id = format!("<{}>", n.as_str());
                node_props.entry(o_id.clone()).or_default();
                if pred == RDF_TYPE {
                    node_props
                        .entry(s_id.clone())
                        .or_default()
                        .entry("type".to_string())
                        .or_insert_with(|| serde_json::Value::String(n.as_str().to_string()));
                }
                edges.push((s_id.clone(), pred, o_id));
            }
            Term::BlankNode(b) => {
                let o_id = format!("_:{}", b.as_str());
                node_props.entry(o_id.clone()).or_default();
                edges.push((s_id.clone(), pred, o_id));
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }

    let mut methods = Vec::with_capacity(node_props.len() + edges.len());
    for (id, props) in node_props {
        let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(props)).unwrap_or_default();
        methods.push(Method::AddNode {
            node_id: id,
            properties_msgpack: blob,
        });
    }
    for (s, p, o) in edges {
        let blob = rmp_serde::to_vec_named(&serde_json::json!({ "type": p })).unwrap_or_default();
        methods.push(Method::AddEdge {
            source_id: s,
            target_id: o,
            properties_msgpack: blob,
        });
    }
    methods
}

/// Lower a SPARQL CONSTRUCT/DESCRIBE query's produced triples to graph-native
/// `AddNode`/`AddEdge` methods (CONCEPT:EG-KG.query.extended-cross-modal/EG-372), evaluating the query against
/// `core`'s committed snapshot. Shared by the RPC [`stage_construct`] and the pgwire
/// cross-modal txn seam so both surfaces lower a CONSTRUCT identically.
#[cfg(feature = "sparql")]
pub(crate) fn construct_to_methods(
    core: &crate::graph::GraphCore,
    sparql: &str,
) -> Result<Vec<Method>, String> {
    let snap = core.analysis_snapshot();
    let proj = eg_rdf::sparql::Projection::from_wire("", "");
    let triples = match eg_rdf::sparql::run_outcome(&snap, sparql, &proj) {
        Ok(eg_rdf::sparql::QueryOutcome::Graph(t)) => t,
        Ok(_) => return Err("SPARQL CONSTRUCT/DESCRIBE query required".to_string()),
        Err(e) => return Err(e),
    };
    Ok(triples_to_methods(&triples))
}

/// Lower a SPARQL UPDATE's `INSERT DATA` triples to graph-native `AddNode`/`AddEdge`
/// methods (CONCEPT:EG-372), reusing the SAME `triples_to_methods` lowering as the OWL
/// axiom path. Used by the pgwire cross-modal txn seam's `SPARQL UPDATE` verb.
#[cfg(feature = "sparql")]
pub(crate) fn sparql_update_to_methods(update_str: &str) -> Result<Vec<Method>, String> {
    let triples = eg_rdf::update::insert_data_triples(update_str)?;
    Ok(triples_to_methods(&triples))
}

/// Resolve the txn's DEFAULT-graph core, enforcing that `target_graph` (if given) is the
/// default (the cross-modal barrier is per-graph). Returns the core clone, or an error
/// `Response` to return directly. Shared by the axiom + CONSTRUCT stagers.
#[cfg(feature = "sparql")]
fn resolve_txn_default_core(
    s: &ServerState,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    kind: &str,
) -> Result<Arc<crate::graph::GraphCore>, Response> {
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => {
            return Err(Response::err(
                req_id,
                format!("unknown transaction '{}'", txn_id),
            ))
        }
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Err(Response::err(
                req_id,
                format!("cross-modal {kind} must target the txn's default graph"),
            ));
        }
    }
    match s.registry.get(&default_graph) {
        Some(g) => Ok(g.core.clone()),
        None => Err(Response::err(
            req_id,
            format!("Graph '{}' not found", default_graph),
        )),
    }
}

/// Stage OWL AXIOMS (Turtle) into the txn's cross-modal write-set (CONCEPT:EG-KG.txn.extended-cross-modal). The
/// axioms are parsed + lowered to `AddNode`/`AddEdge` methods HERE (at stage time) so the
/// commit path treats them as ordinary graph mutations riding the one cross-modal
/// `WriteTransaction`. Acks `Bool(true)`.
#[cfg(feature = "owl")]
async fn stage_axiom(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    turtle: String,
) -> Response {
    let s = state.read().await;
    let core = match resolve_txn_default_core(&s, req_id, txn_id, target_graph, "axiom") {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let triples = match eg_rdf::mapping::parse_turtle(&turtle) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, format!("TxnAxiom: {e}")),
    };
    let methods = triples_to_methods(&triples);
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value().lock().stage_axiom(&core, methods, now_ms());
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Stage a SPARQL CONSTRUCT into the txn's cross-modal write-set (CONCEPT:EG-KG.query.extended-cross-modal). The
/// CONSTRUCT is evaluated NOW against the graph's committed snapshot; its produced triples
/// are lowered to `AddNode`/`AddEdge` methods that land in the SAME cross-modal
/// `WriteTransaction` at commit. (Read-your-own-writes over the txn's OTHER staged writes
/// is Lane A's overlay concern; here the CONSTRUCT reads the committed store.) Acks
/// `Bool(true)`.
#[cfg(feature = "sparql")]
async fn stage_construct(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    sparql: String,
) -> Response {
    let s = state.read().await;
    let core = match resolve_txn_default_core(&s, req_id, txn_id, target_graph, "construct") {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let methods = match construct_to_methods(&core, &sparql) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("TxnConstruct: {e}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value().lock().stage_construct(&core, methods, now_ms());
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// `Commit`: the OCC serialization point. Validate the read-set under the held
/// topology write guard; on success apply the staged write-set atomically through
/// ONE `GraphTxn`, drop the guard, then persist each staged method and mark dirty
/// (which bumps the version). On conflict NOTHING is applied/persisted — a true
/// rollback returning `Bool(false)`.
///
/// **Cross-shard routing (CONCEPT:EG-KG.storage.lane-n-increment + KG-2.226).** A txn whose staged ops all
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

    // ── Multi-graph span (CONCEPT:EG-KG.txn.routes-cross-shard-txn — Lane N) ────────────────────────────
    // If the txn staged ops against a graph other than its default, evaluate the
    // span against the router. A span over ≥2 Raft groups routes through the 2PC
    // coordinator (cross-shard, all-or-nothing across groups). A multi-graph span
    // that collapses to ONE group, OR no active cluster (incl. a non-raft build),
    // applies each graph's slice locally so no staged graph is silently dropped.
    if txn.is_multi_graph() {
        drop(s);
        return commit_multi_graph(state, req_id, caller, txn).await;
    }

    // ── Cross-modal span (CONCEPT:EG-KG.txn.reader-never-sees-node) ─────────────────────────────────────
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

/// Commit a CROSS-MODAL single-graph transaction (CONCEPT:EG-KG.txn.reader-never-sees-node + EG-360/361/362).
/// Validates the OCC read-set, then lands EVERY staged modality ATOMICALLY in ONE redb
/// `WriteTransaction` (commit-before-ack) BEFORE touching the in-memory model:
///   * graph methods + vector upserts + blob-refs (CONCEPT:EG-KG.txn.reader-never-sees-node);
///   * OWL-axiom + SPARQL-CONSTRUCT triples, lowered to `AddNode`/`AddEdge` and folded
///     into `methods` (CONCEPT:EG-KG.txn.extended-cross-modal/362);
///   * time-series measurement batches, written into the graph's SERIES tables in the
///     SAME transaction (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
///
/// A durable-commit failure applies NOTHING (no partial cross-modal commit) and the
/// in-memory state only ever reflects what is durable. On OCC conflict returns
/// `Bool(false)` (true rollback); on durable failure returns an ERROR.
///
/// The graph modalities (nodes/edges/axioms/CONSTRUCT/vectors/blob-refs) are mirrored
/// into the in-memory model after the durable commit. Measurements are DURABLE-ONLY in
/// this lane (they live in `graph.redb`'s SERIES tables, exposed readably on
/// `GraphTxnState.measurements`); wiring them into the served/query read path is the
/// Lane A overlay + Lane C `TsScan` reconcile step.
async fn commit_cross_modal(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn: GraphTxnState,
) -> Response {
    match commit_cross_modal_txn(state, caller, txn).await {
        Ok(committed) => Response::ok(req_id, ResultPayload::Bool(committed)),
        Err(e) => Response::err(req_id, e),
    }
}

/// The reusable core of the cross-modal commit (CONCEPT:EG-KG.txn.reader-never-sees-node + EG-360/361/362),
/// factored out of [`commit_cross_modal`] so BOTH the RPC `Method::Commit` handler AND
/// the pgwire cross-modal txn seam (CONCEPT:EG-372) drive the IDENTICAL commit — no
/// logic duplicated across the RPC + wire surfaces. Returns `Ok(true)` on commit,
/// `Ok(false)` on an OCC conflict (true rollback), `Err(msg)` on an ACL denial or a
/// durable-commit failure.
///
/// Durability tiers:
///   * **persistence present** → the ATOMIC commit point: ALL modalities land in ONE
///     redb `WriteTransaction` (commit-before-ack) BEFORE the in-memory apply. A durable
///     failure applies NOTHING (no partial cross-modal commit).
///   * **persistence absent (in-memory engine)** → the graph/vector modalities are
///     applied in-memory only (measurements are durable-only and are dropped, matching
///     the engine's "no persist dir ⇒ in-memory only, no panic" durability model). A
///     cross-modal txn on an in-memory engine therefore still commits its graph + vector
///     writes rather than erroring.
pub(crate) async fn commit_cross_modal_txn(
    state: &Arc<RwLock<ServerState>>,
    caller: Option<&str>,
    txn: GraphTxnState,
) -> Result<bool, String> {
    let (core, persistence, graph_type, owner) = {
        let s = state.read().await;
        let entry = match s.registry.get(&txn.graph) {
            Some(e) => e,
            None => return Err(format!("Graph '{}' not found", txn.graph)),
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
        check_graph_access(
            &s.isolation,
            caller,
            &txn.graph,
            graph_type,
            owner.as_deref(),
            AccessLevel::Write,
        )?;
    }

    // ── OCC validation under the topology write guard (read-only check) ──
    {
        let gtxn = core.txn(); // serialization barrier vs concurrent writers
        let ok = txn.validate(&core);
        drop(gtxn); // release the barrier before returning / the durable commit
        if !ok {
            // Conflict: nothing applied, nothing persisted — true rollback.
            return Ok(false);
        }
    }

    let fname = crate::persist::sanitize(&txn.graph);
    // Graph-topology methods for the durable + in-memory apply: the ordinary staged
    // write-set PLUS the OWL-axiom and SPARQL-CONSTRUCT triples already lowered to
    // AddNode/AddEdge at stage time (CONCEPT:EG-KG.txn.extended-cross-modal/362). Folding them here means they
    // ride the SAME `apply_method_rows` / `apply_staged` path as any other mutation, so
    // the committed axioms/CONSTRUCT triples are durable + visible atomically with the
    // txn's other modalities.
    let mut methods: Vec<Method> = txn.write_set.clone();
    methods.extend(txn.axioms.iter().cloned());
    methods.extend(txn.constructs.iter().cloned());
    // Time-series measurement batches land into the graph's SERIES tables in the SAME
    // WriteTransaction (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
    let measurements: Vec<crate::MeasurementBatch> =
        txn.measurements.iter().map(|m| m.to_batch()).collect();

    // ── THE ATOMIC COMMIT POINT (when durable): land ALL modalities in ONE redb
    // WriteTransaction, commit-before-ack. A failure means NOTHING is durable AND we
    // apply nothing in-memory → the whole cross-modal txn rolls back (no partial). On an
    // in-memory engine (no persistence) this step is skipped and the graph/vector
    // modalities are applied in-memory only below.
    if let Some(p) = persistence {
        if let Err(e) = p
            .commit_crossmodal(
                &fname,
                &methods,
                &txn.vectors,
                &txn.blob_refs,
                &measurements,
            )
            .await
        {
            return Err(format!("cross-modal commit failed: {e}"));
        }
    }

    // ── Durable commit succeeded (or in-memory-only) → reflect it in the in-memory model ──
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

    Ok(true)
}

/// One graph's slice of a multi-graph txn (CONCEPT:EG-KG.txn.routes-cross-shard-txn), resolved at commit:
/// name + sanitized fname + type + the staged ops for it.
struct CommitSlice {
    graph_name: String,
    graph_fname: String,
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    graph_type: crate::protocol::GraphType,
    methods: Vec<Method>,
}

/// Commit a MULTI-GRAPH staged transaction (CONCEPT:EG-KG.txn.routes-cross-shard-txn — Lane N wire). Builds a
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

/// Route a cross-shard multi-graph txn through the 2PC coordinator (CONCEPT:EG-KG.txn.routes-cross-shard-txn).
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

/// Apply each graph's slice locally through its own `GraphTxn` (CONCEPT:EG-KG.txn.routes-cross-shard-txn — the
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
