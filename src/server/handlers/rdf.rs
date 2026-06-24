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

#[cfg(feature = "sparql")]
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
        other => Err(other),
    }
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
