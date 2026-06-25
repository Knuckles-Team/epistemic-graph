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
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    // `caller`/`rls` are consumed only by the `sparql`-gated read path below; in a
    // `security`-but-no-`sparql` build keep them referenced (no dead-param warning).
    #[cfg(all(feature = "security", not(feature = "sparql")))]
    let _ = (caller, rls);
    match method {
        #[cfg(feature = "rdf")]
        Method::AddTriples { turtle, ntriples } => {
            Ok(handle_add_triples(state, req_id, graph_name, &core, turtle, ntriples).await)
        }
        #[cfg(feature = "rdf")]
        Method::GetRdf => Ok(handle_get_rdf(state, req_id, graph_name, &core).await),
        #[cfg(feature = "sparql")]
        Method::Sparql { query } => {
            // Off-lock snapshot + blocking-pool idiom, identical to SQL/Cypher. RLS
            // (CONCEPT:KG-2.231) filters the snapshot to the caller's visible rows
            // BEFORE SPARQL evaluation, so a SELECT can't exfiltrate a forbidden row.
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let mut snap = core.analysis_snapshot();
            #[cfg(feature = "security")]
            rls.filter_view(caller.unwrap_or(""), &mut snap);
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
        } => Ok(handle_owl_reason(req_id, &core, ontology, target_class).await),
        other => Err(other),
    }
}

/// Run the native OWL 2 reasoner over an off-lock snapshot and materialize entailments
/// (CONCEPT:KG-2.219). Read-only — classification/consistency over the graph's axioms
/// (+ any extra `ontology` Turtle); returns the derived subsumptions, the inferred
/// instance memberships (optionally restricted to `target_class`), and consistency.
#[cfg(feature = "owl")]
async fn handle_owl_reason(
    req_id: u64,
    core: &Arc<GraphCore>,
    ontology: String,
    target_class: String,
) -> Response {
    let snap = core.analysis_snapshot();
    let resp =
        match compute_off_lock(req_id, move || owl_reason(&snap, &ontology, &target_class)).await {
            Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
            Ok(Err(msg)) => Response::err(req_id, format!("OwlReason error: {msg}")),
            Err(resp) => resp,
        };
    resp
}

/// Classify the graph (+ optional extra axioms) and project the wire result.
#[cfg(feature = "owl")]
fn owl_reason(
    view: &crate::graph::GraphView,
    ontology: &str,
    target_class: &str,
) -> Result<crate::protocol::OwlReasonResult, String> {
    use eg_rdf::owl::{asserted_types_from_view, instances_of, tbox_triples_from_view, Reasoner};

    // Axioms: graph's own TBox, plus any supplied Turtle ontology.
    let mut triples = tbox_triples_from_view(view);
    if !ontology.trim().is_empty() {
        triples.extend(eg_rdf::mapping::parse_turtle(ontology)?);
    }
    let mut reasoner = Reasoner::from_triples(&triples);
    let cls = reasoner.classify();

    // Derived named-class subsumptions (the full classification hierarchy).
    let mut subclasses: Vec<(String, String)> = Vec::new();
    for (sub, sups) in &cls.subsumers {
        for sup in sups {
            subclasses.push((sub.clone(), sup.clone()));
        }
    }

    // Inferred instance memberships.
    let asserted = asserted_types_from_view(view);
    let instances: Vec<(String, String)> = if target_class.trim().is_empty() {
        let mat = eg_rdf::owl::materialize_instances(&cls, &asserted);
        let mut out = Vec::new();
        for (inst, classes) in mat {
            for c in classes {
                out.push((inst.clone(), c));
            }
        }
        out
    } else {
        let target = if target_class.starts_with('<') {
            target_class.to_string()
        } else {
            format!(
                "<{}>",
                target_class.trim_start_matches('<').trim_end_matches('>')
            )
        };
        instances_of(&cls, &asserted, &target)
            .into_iter()
            .map(|inst| (inst, target.clone()))
            .collect()
    };

    Ok(crate::protocol::OwlReasonResult {
        subclasses,
        instances,
        consistent: cls.consistent,
        unsatisfiable: cls.unsatisfiable.into_iter().collect(),
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
