//! Native RDF/SPARQL handler (CONCEPT:KG-2.217 / KG-2.218, features `rdf`/`sparql`).
//!
//! Owns the `// ── RDF/SPARQL ──` protocol section — `AddTriples` / `GetRdf`
//! (feature `rdf`) and `Sparql` (feature `sparql`). These are GRAPH-SCOPED ops: the
//! RDF dataset maps onto the SAME property-graph the rest of the engine uses (a
//! resource object ⇒ a typed edge, a literal object ⇒ a typed property cell,
//! `rdf:type` ⇒ the engine `type` label, a named graph ⇒ the target registry graph).
//! So they route through the normal `dispatch_graph_op` chain like Sql/Cypher.
//!
//! * `AddTriples` is a DURABLE MUTATION (it writes nodes + edges). The dispatch
//!   shell records the `AddTriples` Method into the WAL/Raft like any other durable
//!   write; replay re-parses the source text (deterministic) via `crate::wal::apply`.
//!   The in-RAM apply happens HERE.
//! * `GetRdf` serializes the graph back OUT to N-Triples (read-only).
//! * `Sparql` evaluates a SPARQL 1.1 SELECT over an off-lock GraphView snapshot
//!   (read-only), same idiom as the SQL/Cypher handlers.
//!
//! The optional lossless quad store (`rdf-redb`) lives on `ServerState`; the handler
//! reads it under a brief lock to preserve the one lossy edge (multi-valued literal
//! predicates) and to union those extras back on export.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use tokio::sync::RwLock;

#[cfg(any(feature = "sparql", feature = "owl"))]
use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::graph::GraphCore;
use crate::protocol::{Method, Response, ResultPayload};

/// Handle the RDF/SPARQL methods. `Err(method)` hands a non-RDF method back to the
/// dispatch chain (routing fall-through); dispatch only ever routes RDF methods here.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        #[cfg(feature = "rdf")]
        Method::AddTriples { turtle, ntriples } => {
            Ok(handle_add_triples(state, req_id, graph_name, &core, turtle, ntriples).await)
        }
        #[cfg(feature = "rdf")]
        Method::GetRdf => Ok(handle_get_rdf(state, req_id, graph_name, &core).await),
        #[cfg(feature = "sparql")]
        Method::Sparql { query } => {
            // Off-lock snapshot + blocking-pool idiom, identical to SQL/Cypher.
            let snap = core.analysis_snapshot();
            let resp =
                match compute_off_lock(req_id, move || eg_rdf::sparql::run(&snap, &query)).await {
                    Ok(Ok(result)) => {
                        let (vars, rows) = result.to_rows();
                        let wire = crate::protocol::SparqlResult { vars, rows };
                        Response::ok(req_id, ResultPayload::raw(&wire))
                    }
                    Ok(Err(msg)) => Response::err(req_id, format!("SPARQL error: {msg}")),
                    Err(resp) => resp,
                };
            Ok(resp)
        }
        #[cfg(feature = "owl")]
        Method::OwlReason {
            ontology,
            target_class,
            min_confidence,
        } => Ok(handle_owl_reason(req_id, &core, ontology, target_class, min_confidence).await),
        other => Err(other),
    }
}

/// Top-level routing for the cross-shard `OwlReasonDistributed` method (CONCEPT:KG-2.236).
/// It is NOT graph-scoped (it unions several graphs), so dispatch routes it here directly
/// with `state` rather than through `dispatch_graph_op`. `Err(method)` ⇒ not mine.
#[cfg(feature = "owl")]
pub(crate) async fn try_handle_distributed(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::OwlReasonDistributed {
            graphs,
            ontology,
            target_class,
            min_confidence,
        } => Ok(handle_owl_reason_distributed(
            state,
            req_id,
            graphs,
            ontology,
            target_class,
            min_confidence,
        )
        .await),
        other => Err(other),
    }
}

/// The decay half-life (seconds) the reasoner uses to age type facts — the SAME source
/// of truth as the maintenance decay loop (`GRAPH_SERVICE_DECAY_HALF_LIFE`, default 7d).
#[cfg(feature = "owl")]
fn decay_half_life_secs() -> f64 {
    std::env::var("GRAPH_SERVICE_DECAY_HALF_LIFE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|h| *h > 0.0)
        .unwrap_or(604_800.0)
}

/// Wall-clock unix seconds — the `now` the Ebbinghaus fact-decay is measured against.
#[cfg(feature = "owl")]
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Run the native OWL 2 reasoner over an off-lock snapshot and materialize entailments
/// (CONCEPT:KG-2.219 / KG-2.236). Read-only — confidence-weighted classification /
/// consistency over the graph's axioms (+ any extra `ontology` Turtle); returns the
/// derived subsumptions + per-entailment confidence, the inferred instance memberships
/// (optionally restricted to `target_class`, thresholded by `min_confidence`), and
/// consistency.
#[cfg(feature = "owl")]
async fn handle_owl_reason(
    req_id: u64,
    core: &Arc<GraphCore>,
    ontology: String,
    target_class: String,
    min_confidence: f64,
) -> Response {
    let snap = core.analysis_snapshot();
    let now = now_secs();
    let hl = decay_half_life_secs();
    let resp = match compute_off_lock(req_id, move || {
        owl_reason(&[&snap], &ontology, &target_class, now, hl, min_confidence)
    })
    .await
    {
        Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
        Ok(Err(msg)) => Response::err(req_id, format!("OwlReason error: {msg}")),
        Err(resp) => resp,
    };
    resp
}

/// DISTRIBUTED reasoning over the UNION of `graphs` (CONCEPT:KG-2.236). Gathers each
/// graph's off-lock snapshot (the cross-shard union-read seam), then runs the SAME
/// weighted closure as the single-graph path over the unioned axioms + facts.
#[cfg(feature = "owl")]
async fn handle_owl_reason_distributed(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graphs: Vec<String>,
    ontology: String,
    target_class: String,
    min_confidence: f64,
) -> Response {
    // Gather each shard's snapshot under the registry lock, release before compute.
    let snaps: Vec<crate::graph::GraphView> = {
        let s = state.read().await;
        graphs
            .iter()
            .filter_map(|name| s.registry.get(name).map(|e| e.core.analysis_snapshot()))
            .collect()
    };
    let now = now_secs();
    let hl = decay_half_life_secs();
    let resp = match compute_off_lock(req_id, move || {
        let views: Vec<&crate::graph::GraphView> = snaps.iter().collect();
        owl_reason(&views, &ontology, &target_class, now, hl, min_confidence)
    })
    .await
    {
        Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
        Ok(Err(msg)) => Response::err(req_id, format!("OwlReasonDistributed error: {msg}")),
        Err(resp) => resp,
    };
    resp
}

/// Classify the UNION of `views` (+ optional extra axioms) with confidence propagation
/// and project the wire result. One view = the single-graph fast path; many views = the
/// distributed cross-shard path. Both go through `eg_rdf::owl::reason_distributed_weighted`.
#[cfg(feature = "owl")]
fn owl_reason(
    views: &[&crate::graph::GraphView],
    ontology: &str,
    target_class: &str,
    now: u64,
    half_life: f64,
    min_confidence: f64,
) -> Result<crate::protocol::OwlReasonResult, String> {
    let extra = if ontology.trim().is_empty() {
        Vec::new()
    } else {
        eg_rdf::mapping::parse_turtle(ontology)?
    };
    let res = eg_rdf::owl::reason_distributed_weighted(
        views,
        &extra,
        now,
        half_life,
        target_class,
        min_confidence,
    );

    let mut subclasses = Vec::with_capacity(res.subclasses.len());
    let mut subclass_conf = Vec::with_capacity(res.subclasses.len());
    for (sub, sup, c) in res.subclasses {
        subclasses.push((sub, sup));
        subclass_conf.push(c);
    }
    let mut instances = Vec::with_capacity(res.instances.len());
    let mut instance_conf = Vec::with_capacity(res.instances.len());
    for (inst, class, c) in res.instances {
        instances.push((inst, class));
        instance_conf.push(c);
    }

    Ok(crate::protocol::OwlReasonResult {
        subclasses,
        subclass_conf,
        instances,
        instance_conf,
        consistent: res.consistent,
        unsatisfiable: res.unsatisfiable,
    })
}

/// Parse Turtle/N-Triples and store into the target graph; route multi-valued
/// literal extras to the lossless quad store when configured.
#[cfg(feature = "rdf")]
async fn handle_add_triples(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    turtle: String,
    ntriples: String,
) -> Response {
    let triples = match parse_either(&turtle, &ntriples) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };

    #[cfg(feature = "rdf-redb")]
    let quads = state.read().await.rdf_quads.clone();
    #[cfg(not(feature = "rdf-redb"))]
    let _ = state;

    // Record the named-graph marker linking this RDF dataset to its registry graph.
    eg_rdf::mapping::register_named_graph(core, graph_name);

    let mut iris = eg_rdf::mapping::IriStore::default();
    let report = eg_rdf::mapping::load_triples(
        core,
        &mut iris,
        graph_name,
        triples,
        #[cfg(feature = "rdf-redb")]
        quads.as_deref(),
    );
    match report {
        Ok(r) => Response::ok(req_id, ResultPayload::raw(&r)),
        Err(e) => Response::err(req_id, format!("AddTriples error: {e}")),
    }
}

/// Serialize the target graph back OUT to N-Triples (unioning the lossless extras).
#[cfg(feature = "rdf")]
async fn handle_get_rdf(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
) -> Response {
    #[cfg(feature = "rdf-redb")]
    let quads = state.read().await.rdf_quads.clone();
    #[cfg(not(feature = "rdf-redb"))]
    let _ = state;

    let exported = eg_rdf::mapping::export_triples(
        core,
        graph_name,
        #[cfg(feature = "rdf-redb")]
        quads.as_deref(),
    );
    match exported.and_then(|t| eg_rdf::mapping::to_ntriples(&t)) {
        Ok(nt) => Response::ok(req_id, ResultPayload::raw(&nt)),
        Err(e) => Response::err(req_id, format!("GetRdf error: {e}")),
    }
}

/// Parse exactly one of Turtle / N-Triples (whichever is non-empty).
#[cfg(feature = "rdf")]
fn parse_either(turtle: &str, ntriples: &str) -> Result<Vec<eg_rdf::oxrdf::Triple>, String> {
    match (turtle.trim().is_empty(), ntriples.trim().is_empty()) {
        (false, true) => eg_rdf::mapping::parse_turtle(turtle),
        (true, false) => eg_rdf::mapping::parse_ntriples(ntriples),
        (true, true) => Err("AddTriples: both `turtle` and `ntriples` are empty".into()),
        (false, false) => Err("AddTriples: provide exactly one of `turtle` or `ntriples`".into()),
    }
}
