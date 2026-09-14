use std::sync::Arc;

#[cfg(feature = "shacl")]
use tokio::sync::RwLock;

#[cfg(feature = "shacl")]
use super::super::state::ServerState;
use crate::graph::GraphCore;
use crate::protocol::{Response, ResultPayload};

/// Parse Turtle/N-Triples and store into the target graph; route multi-valued
/// literal extras to the lossless quad store when configured.
#[cfg(feature = "rdf")]
pub(super) async fn handle_add_triples(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    turtle: String,
    ntriples: String,
) -> Response {
    #[cfg(not(feature = "shacl"))]
    return Response::err(
        req_id,
        "AddTriples requires the shacl integrity-guard feature",
    );

    let triples = match parse_either(&turtle, &ntriples) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };

    // X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard): reject BEFORE the write
    // lands. Missing feature, policy, or invalid policy all fail closed.
    #[cfg(feature = "shacl")]
    if let Err(rej) = crate::server::icv_guard::check_before_write(core, graph_name, &triples, &[])
    {
        return Response::err(req_id, format!("AddTriples rejected: {rej}"));
    }

    // Record the named-graph marker linking this RDF dataset to its registry graph.
    eg_rdf::mapping::register_named_graph(core, graph_name);

    let mut iris = eg_rdf::mapping::IriStore::default();
    let report = eg_rdf::mapping::load_triples(core, &mut iris, graph_name, triples);
    match report {
        Ok(r) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::graph::AddTriples>(&r),
        ),
        Err(e) => Response::err(req_id, format!("AddTriples error: {e}")),
    }
}

/// Serialize the target graph back OUT to N-Triples (unioning the lossless extras).
#[cfg(feature = "rdf")]
pub(super) async fn handle_get_rdf(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
) -> Response {
    let exported = eg_rdf::mapping::export_triples(core, graph_name);
    match exported.and_then(|t| eg_rdf::mapping::to_ntriples(&t)) {
        Ok(nt) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::reasoning::GetRdf>(nt),
        ),
        Err(e) => Response::err(req_id, format!("GetRdf error: {e}")),
    }
}

/// Physically RETRACT triples from the target graph (CONCEPT:EG-KG.query.named-graph-support) — the durable
/// inverse of `AddTriples`. Routes through the reusable `eg_rdf::update::remove_triples`
/// engine op (surgical: literal cells + the one matching typed edge). Returns the count.
#[cfg(feature = "rdf")]
pub(super) async fn handle_remove_triples(
    #[cfg(feature = "shacl")] _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    turtle: String,
    ntriples: String,
) -> Response {
    #[cfg(not(feature = "shacl"))]
    return Response::err(
        req_id,
        "RemoveTriples requires the shacl integrity-guard feature",
    );

    let triples = match parse_either(&turtle, &ntriples) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };

    // X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard): reject BEFORE the
    // removal lands — see `handle_add_triples`.
    #[cfg(feature = "shacl")]
    {
        if let Err(rej) =
            crate::server::icv_guard::check_before_write(core, graph_name, &[], &triples)
        {
            return Response::err(req_id, format!("RemoveTriples rejected: {rej}"));
        }
    }

    let removed = eg_rdf::update::remove_triples(core, &triples);
    Response::ok(
        req_id,
        ResultPayload::scalar::<eg_types::result_contract::graph::RemoveTriples>(removed as u64),
    )
}

/// DROP the target named graph's RDF content (CONCEPT:EG-KG.query.named-graph-support).
/// The lossless dataset lives inside the authoritative graph snapshot, so this
/// one clear is staged and committed atomically.
#[cfg(feature = "rdf")]
pub(super) async fn handle_drop_named_graph(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
) -> Response {
    #[cfg(not(feature = "shacl"))]
    return Response::err(
        req_id,
        "DropNamedGraph requires the shacl integrity-guard feature",
    );

    #[cfg(feature = "shacl")]
    {
        let removals = match eg_rdf::mapping::export_triples(core, graph_name) {
            Ok(removals) => removals,
            Err(error) => return Response::err(req_id, error),
        };
        if let Err(rejection) =
            crate::server::icv_guard::check_before_write(core, graph_name, &[], &removals)
        {
            return Response::err(req_id, format!("DropNamedGraph rejected: {rejection}"));
        }
    }
    core.clear();
    Response::ok(req_id, ResultPayload::String("ok".to_string()))
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
