//! Private transaction staging implementation.

use super::*;

/// `BeginTxn`: resolve the target graph, enforce the per-graph + per-agent open-txn
/// caps, snapshot the OCC begin-version, and register a fresh staged transaction.
/// Returns the server-issued `txn_id` as a `String` payload.
pub(super) async fn begin_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_owner: &str,
    tenant_scope: &str,
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
    if !consensus_apply_is_authorized() {
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
    }
    let begin_version = entry.core.version();

    // Open-txn caps (CONCEPT:EG-KG.txn.multi-op-occ-acid): bound memory the way per_graph_inflight
    // bounds request concurrency. Count current open txns for this graph/agent.
    let agent = txn_owner.to_string();
    if let Err(response) = check_open_txn_caps(&s, req_id, &graph_name, &agent) {
        return response;
    }

    let txn_id = s.txn_id_gen.next();
    // Under serializable with a declared predicate, the constructor captures the
    // predicate read-set fingerprint against `entry.core` at begin (the snapshot the
    // txn reads against). Snapshot level captures nothing extra.
    s.open_txns.insert(
        txn_id.clone(),
        parking_lot::Mutex::new(GraphTxnState::new(
            &entry.core,
            NewTxnArgs {
                graph: graph_name,
                tenant_scope: tenant_scope.to_string(),
                begin_version,
                isolation: level,
                predicate,
                agent,
                now_ms: now_ms(),
            },
        )),
    );
    Response::ok(req_id, ResultPayload::String(txn_id))
}

/// Enforce the per-graph / per-agent open-txn caps (CONCEPT:EG-KG.txn.multi-op-occ-acid): bound
/// memory the way `per_graph_inflight` bounds request concurrency.
pub(super) fn check_open_txn_caps(
    s: &ServerState,
    req_id: u64,
    graph_name: &str,
    agent: &str,
) -> Result<(), Response> {
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
        return Err(Response::err(
            req_id,
            format!("too many open transactions for graph '{}'", graph_name),
        ));
    }
    if for_agent >= s.txn_max_per_agent {
        return Err(Response::err(
            req_id,
            "too many open transactions for agent",
        ));
    }
    Ok(())
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
pub(super) async fn stage(
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
/// txn's DEFAULT graph; a `graph` naming anything else is rejected because each
/// cross-modal atomic batch has exactly one authoritative graph owner. Acks `Bool(true)`.
pub(super) async fn stage_vector(
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
pub(super) async fn stage_blob_ref(
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
pub(super) const DEFAULT_MEASUREMENT_BUCKET_NS: u64 = 3_600_000_000_000;

/// Stage a TIME-SERIES measurement batch into the txn's cross-modal write-set
/// (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Same per-graph constraint as [`stage_vector`]: the batch targets the
/// txn's DEFAULT graph (the one-`WriteTransaction` barrier is per-graph). The points are
/// decoded here (the SAME `Vec<(i64, Vec<f64>)>` MessagePack shape `TsAppend` carries), so
/// the commit path is a pure durable append. Acks `Bool(true)`.
#[cfg(feature = "tsdb")]
pub(super) async fn stage_measurement(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    series: String,
    points_msgpack: Vec<u8>,
    authority: &CarrierAuthority,
) -> Response {
    let points = match super::timeseries::decode_wire_points(&points_msgpack) {
        Ok(p) => p,
        Err(error) => return Response::err(req_id, error),
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
    let scoped_series = eg_tsdb::store::SeriesKey::new(
        authority.tenant_scope(),
        authority.namespace("timeseries-graph", &default_graph),
        series,
    )
    .encode();
    let measurement = StagedMeasurement {
        series: scoped_series,
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
///   * resource object → subject + object nodes + an edge `{"relationship": predicate}`,
///     and (for `rdf:type`) the subject's `type` label.
///
/// The SAME lowered `Vec<Method>` is applied both durably (`apply_method_rows`) and
/// in-memory (`apply_staged`), so the two are identical by construction.
/// Returns `(methods, schema_refs)` — `schema_refs` (BUG A3, 2026-08-12) is
/// [`eg_rdf::mapping::LoweredTripleGraph::schema_refs`] passed through
/// unchanged: one entry per schema-defining triple occurrence this batch
/// contributed. This lowering produces GENERIC `AddNode`/`AddEdge` methods
/// with no room to carry a "this is schema" signal on the wire, so a caller
/// that auto-commits immediately (the pgwire off-txn `SPARQL UPDATE` seam,
/// `WireSession::stage_or_commit_owl`) MUST mark these on the live core
/// itself right after the methods commit — see that function's doc for why
/// a staged (explicit multi-statement transaction) caller does not yet.
#[cfg(feature = "sparql")]
pub(crate) fn triples_to_methods(
    triples: &[eg_rdf::oxrdf::Triple],
) -> Result<(Vec<Method>, Vec<String>), String> {
    let lowered = eg_rdf::mapping::lower_triples(triples.iter().cloned())?;
    let mut methods = Vec::with_capacity(lowered.nodes.len() + lowered.edges.len());
    for (id, blob) in lowered.nodes {
        methods.push(Method::AddNode {
            node_id: id,
            properties_msgpack: blob,
        });
    }
    for (source, target, blob) in lowered.edges {
        methods.push(Method::AddEdge {
            source_id: source,
            target_id: target,
            properties_msgpack: blob,
        });
    }
    Ok((methods, lowered.schema_refs))
}

/// Lower a SPARQL CONSTRUCT/DESCRIBE query's produced triples to graph-native
/// `AddNode`/`AddEdge` methods (CONCEPT:EG-KG.query.extended-cross-modal/EG-372), evaluating the query against
/// `core`'s committed snapshot. Shared by the RPC [`stage_construct`] and the pgwire
/// cross-modal txn seam so both surfaces lower a CONSTRUCT identically.
/// Returns `(methods, schema_refs)` — see [`triples_to_methods`]'s doc.
#[cfg(feature = "sparql")]
pub(crate) fn construct_to_methods(
    core: &crate::graph::GraphCore,
    sparql: &str,
) -> Result<(Vec<Method>, Vec<String>), String> {
    let snap = core.analysis_snapshot();
    construct_view_to_methods(&snap, sparql)
}

/// Returns `(methods, schema_refs)` — see [`triples_to_methods`]'s doc.
#[cfg(feature = "sparql")]
pub(crate) fn construct_view_to_methods(
    snap: &crate::graph::GraphView,
    sparql: &str,
) -> Result<(Vec<Method>, Vec<String>), String> {
    let proj = eg_rdf::sparql::Projection::from_wire("", "");
    let dataset = eg_rdf::sparql::Dataset::new(snap, Vec::new());
    let triples = match eg_rdf::sparql::execute(&dataset, sparql, &proj, None) {
        Ok(eg_rdf::sparql::QueryOutcome::Graph(t)) => t,
        Ok(_) => return Err("SPARQL CONSTRUCT/DESCRIBE query required".to_string()),
        Err(e) => return Err(e),
    };
    triples_to_methods(&triples)
}

/// Lower a SPARQL UPDATE's `INSERT DATA` triples to graph-native `AddNode`/`AddEdge`
/// methods (CONCEPT:EG-KG.txn.isolation-ryow-begin-set), reusing the SAME `triples_to_methods` lowering as the OWL
/// axiom path. Used by the pgwire cross-modal txn seam's `SPARQL UPDATE` verb.
/// Returns `(methods, schema_refs)` — see [`triples_to_methods`]'s doc.
#[cfg(feature = "sparql")]
pub(crate) fn sparql_update_to_methods(
    update_str: &str,
) -> Result<(Vec<Method>, Vec<String>), String> {
    let triples = eg_rdf::update::insert_data_triples(update_str)?;
    triples_to_methods(&triples)
}

/// Resolve the txn's DEFAULT-graph core, enforcing that `target_graph` (if given) is the
/// default (the cross-modal barrier is per-graph). Returns the core clone, or an error
/// `Response` to return directly. Shared by the axiom + CONSTRUCT + plan-writeback stagers.
#[cfg(any(feature = "sparql", feature = "owl", feature = "query"))]
pub(super) fn resolve_txn_default_core(
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
            ));
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
pub(super) async fn stage_axiom(
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
    // KNOWN GAP (BUG A3, 2026-08-12): `schema_refs` (the 2nd tuple element) is
    // dropped here. This RPC staging surface (`TxnAxiom`, an explicit
    // multi-statement transaction) does not yet carry the schema markers
    // through to its later, separate commit point the way the pgwire
    // off-txn auto-commit seam does (`WireSession::stage_or_commit_owl`) --
    // doing so needs the SAME id set threaded through this txn's OWN staged
    // write-set to its commit function, a larger, separately-scoped change.
    // Fail-closed in the meantime: an axiom staged through THIS RPC surface
    // does not get the TBox exemption (ordinary ABox default-deny), never a
    // widened one.
    let (methods, _schema_refs) = match triples_to_methods(&triples) {
        Ok(result) => result,
        Err(error) => return Response::err(req_id, format!("TxnAxiom: {error}")),
    };
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
pub(super) async fn stage_construct(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    sparql: String,
    read_authority: &GraphReadAuthority,
) -> Response {
    let s = state.read().await;
    let core = match resolve_txn_default_core(&s, req_id, txn_id, target_graph, "construct") {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let projected = read_authority.project_core(&core);
    // KNOWN GAP (BUG A3): see `stage_axiom`'s identical note just above --
    // `schema_refs` is dropped for this staged RPC surface too.
    let (methods, _schema_refs) = match construct_to_methods(&projected, &sparql) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("TxnConstruct: {e}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value().lock().stage_construct(&core, methods, now_ms());
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Lower a planner writeback (CONCEPT:EG-KG.query.plan-dag, D7 — the planner-writeback
/// ACID seam) to graph-native `AddEdge` methods: run `plan` READ-ONLY against `core`'s
/// COMMITTED snapshot (the same "evaluate now, lower the result" shape
/// [`construct_to_methods`] uses for SPARQL CONSTRUCT), then materialize each id in the
/// result `RowSet` as an edge FROM `anchor_id` carrying `relationship` — e.g. a
/// `Reason`/`Traverse`-inferred edge set. Shared by the RPC [`stage_plan_writeback`].
#[cfg(feature = "query")]
pub(crate) fn plan_writeback_to_methods(
    core: &crate::graph::GraphCore,
    plan: eg_plan::Plan,
    anchor_id: &str,
    relationship: &str,
) -> Result<Vec<Method>, String> {
    let view = core.analysis_snapshot();
    let semantic = core.semantic_store.read().clone();
    let ctx = eg_plan::PlanCtx::new(&view, &semantic);
    let rs = eg_plan::execute(&plan, &ctx)?;
    let props = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": relationship }))
        .map_err(|e| format!("plan writeback property encode: {e}"))?;
    Ok(rs
        .ids()
        .into_iter()
        .map(|target_id| Method::AddEdge {
            source_id: anchor_id.to_string(),
            target_id,
            properties_msgpack: props.clone(),
        })
        .collect())
}

/// Stage a PLANNER WRITEBACK into the txn's cross-modal write-set (CONCEPT:EG-KG.query.plan-dag,
/// D7). Mirrors [`stage_construct`]'s shape exactly: evaluate now against the committed
/// snapshot, lower to `AddEdge` methods, stage via `GraphTxnState::stage_plan_writeback`
/// so the materialized edges land in the SAME cross-modal `WriteTransaction` as the
/// txn's other staged modalities. Acks `Bool(true)`.
/// The plan-writeback payload fields of [`Method::TxnPlanWriteback`], bundled so
/// [`stage_plan_writeback`] stays under the clippy argument-count ceiling.
#[cfg(feature = "query")]
pub(super) struct PlanWritebackArgs {
    plan: eg_plan::Plan,
    anchor_id: String,
    relationship: String,
}

#[cfg(feature = "query")]
pub(super) async fn stage_plan_writeback(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    args: PlanWritebackArgs,
    read_authority: &GraphReadAuthority,
) -> Response {
    let PlanWritebackArgs {
        plan,
        anchor_id,
        relationship,
    } = args;
    let s = state.read().await;
    let core = match resolve_txn_default_core(&s, req_id, txn_id, target_graph, "plan writeback") {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let projected = read_authority.project_core(&core);
    if read_authority.is_active() {
        // The anchor may have been staged EARLIER in this SAME txn (read-your-own-writes
        // -- D7's own capstone case is exactly this: "the anchor node ITSELF is staged in
        // the SAME txn as the writeback"). `projected` alone only reflects the RLS-filtered
        // COMMITTED snapshot, so a same-txn-staged anchor would wrongly read as invisible.
        // Overlay the txn's own staged write-set onto that snapshot first -- the same
        // technique `run_unified_overlaid` uses for in-txn reads (CONCEPT:EG-KG.query.overlay-leg-rls-filter)
        // -- before deciding visibility. The PLAN ITSELF still evaluates against `projected`
        // (committed-only, unchanged below): only the anchor's own existence is RYOW-aware.
        let mut anchor_view = projected.analysis_snapshot();
        if let Some(entry) = s.open_txns.get(txn_id) {
            let write_set = entry.value().lock().write_set.clone();
            crate::server::handlers::query::overlay_write_set(&mut anchor_view, &write_set);
        }
        if !anchor_view.has_node(&anchor_id) {
            return Response::err(req_id, "TxnPlanWriteback: anchor is not visible");
        }
    }
    let methods = match plan_writeback_to_methods(&projected, plan, &anchor_id, &relationship) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("TxnPlanWriteback: {e}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value()
            .lock()
            .stage_plan_writeback(&core, methods, now_ms());
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Compute the propagated belief for `node_id` over `core`'s COMMITTED snapshot and
/// lower it to ONE unconditional `CompareAndSetNodeFields` method that writes the
/// derived `BeliefState.confidence` back onto that node's `NodeData.confidence`
/// (CONCEPT:EG-KG.epistemic.epistemic-substrate, D5 — the "explicit, logged
/// materialize belief" op the `eg_epistemic` crate docs call for: the derived belief
/// is otherwise NEVER written back). `conditions_msgpack` is an EMPTY object
/// (vacuously true — `compare_and_set_fields` still requires the node to exist, which
/// is checked HERE so a missing node is rejected with a clear error at STAGE time
/// rather than a silently no-op'd CAS at commit).
///
/// Replay/idempotency: the confidence value is computed ONCE, here, at stage time, and
/// baked verbatim into `updates_msgpack` — exactly like `plan_writeback_to_methods`/
/// `construct_to_methods` freeze their evaluated result before staging. A WAL replay or
/// a duplicate commit of the SAME staged method therefore re-applies the SAME literal
/// bytes (a true idempotent CAS), never re-derives a drifted value from the
/// (by-then-already-updated) stored confidence — which is precisely the ratchet the
/// crate docs warn a naive "always re-propagate from current state" design would risk.
#[cfg(feature = "epistemic")]
pub(crate) fn materialize_belief_to_methods(
    core: &crate::graph::GraphCore,
    node_id: &str,
) -> Result<(Vec<Method>, f64), String> {
    if !core.has_node(node_id) {
        return Err(format!("node '{node_id}' not found"));
    }
    let view = core.analysis_snapshot();
    let bg = eg_epistemic::BeliefGraph::from_graph_view(&view);
    let policy = eg_epistemic::AuthorityPolicy::default();
    let confidence = eg_epistemic::propagate_confidence(&bg, node_id, &policy)
        .confidence
        .clamp(0.0, 1.0);
    let updates_msgpack = rmp_serde::to_vec_named(&serde_json::json!({ "confidence": confidence }))
        .map_err(|e| format!("materialize belief encode: {e}"))?;
    let conditions_msgpack =
        rmp_serde::to_vec_named(&serde_json::Map::<String, serde_json::Value>::new())
            .map_err(|e| format!("materialize belief encode: {e}"))?;
    Ok((
        vec![Method::CompareAndSetNodeFields {
            node_id: node_id.to_string(),
            conditions_msgpack,
            updates_msgpack,
        }],
        confidence,
    ))
}

/// Stage a MATERIALIZE-BELIEF op into the txn's cross-modal write-set (CONCEPT:
/// EG-KG.epistemic.epistemic-substrate, D5). Mirrors [`stage_plan_writeback`]'s shape
/// exactly: evaluate now against the committed snapshot, lower to a durable Method,
/// stage via `GraphTxnState::stage_plan_writeback` so the write lands in the SAME
/// cross-modal `WriteTransaction` as the txn's other staged modalities at commit —
/// where it rides the ALREADY-audited `CompareAndSetNodeFields` path (the
/// tamper-evident hash chain, CONCEPT:EG-KG.sharding.row-level-security, when
/// `security` is built, PLUS the unconditional in-memory ledger every CAS appends to
/// regardless) — never silent, no new audit mechanism. OPT-IN: nothing else stages
/// this op; it only ever runs when a caller explicitly sends `TxnMaterializeBelief`.
/// Returns the computed confidence in the ack payload (`{node_id, confidence}`) so the
/// caller can observe the derived value before deciding to commit.
#[cfg(feature = "epistemic")]
pub(super) async fn stage_materialize_belief(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    node_id: String,
    read_authority: &GraphReadAuthority,
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
                "cross-modal materialize-belief must target the txn's default graph",
            );
        }
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    let projected = read_authority.project_core(&core);
    let (methods, confidence) = match materialize_belief_to_methods(&projected, &node_id) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("TxnMaterializeBelief: {e}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value()
            .lock()
            .stage_plan_writeback(&core, methods, now_ms());
    }
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::transactions::TxnMaterializeBelief>(
            eg_types::result_contract::transactions::BeliefMaterialization {
                node_id,
                confidence,
            },
        ),
    )
}

/// `Commit`: the OCC serialization point. Validate the read-set under the held
/// topology write guard.  On an authoritative redb deployment the ordered write-set
/// is compiled into ONE canonical `MutationBatch`, committed durably with status,
/// idempotency and outbox, and only then published through ONE in-memory `GraphTxn`.
/// On conflict NOTHING is applied/persisted — a true rollback returning
/// `Bool(false)`. Non-authoritative deployments fail closed: a transaction is never
/// acknowledged through a data-only mirror path.
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
/// B-9 (2026-08-13): declare a `Commit` response's boolean outcome as the
/// `Commit` result. With a caller idempotency key it is
/// `{"committed": bool, "replayed": bool}` -- the same `applied`/`idempotent_skip`
/// vocabulary `ApplyChangeEnvelope` reports, extended onto `Commit` rather than
/// inventing a second one. **Without a caller key the body is the bare boolean**,
/// byte-for-byte the wire shape it always had (`ResultPayload` is untagged) -- the
/// VERIFY contract's "without the key the behaviour is unchanged". An error
/// response passes through unchanged; any other outcome is a broken receipt.
pub(crate) struct CommitResponseOptions {
    pub(crate) replayed: bool,
    pub(crate) keyed: bool,
}

pub(crate) fn tag_commit_response(response: Response, options: CommitResponseOptions) -> Response {
    let Response { id, result, error } = response;
    if let Some(error) = error {
        return Response::err(id, error);
    }
    Response::ok(
        id,
        commit_outcome(result, options.keyed.then_some(options.replayed)),
    )
}
