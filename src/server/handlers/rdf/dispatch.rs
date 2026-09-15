use std::sync::Arc;

use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::protocol::{Method, Response};
use crate::server::access::GraphReadAuthority;
use crate::server::handlers::TryHandleContext;
use crate::server::state::ServerState;

/// Route the native RDF family while keeping each protocol surface in its own
/// cohesive handler. An unmatched method is returned for the next dispatcher.
pub(in crate::server) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    ctx: TryHandleContext<'_>,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    let TryHandleContext {
        req_id,
        graph_name,
        read_authority,
        caller,
    } = ctx;

    if let Some(response) =
        try_handle_rdf(state, req_id, graph_name, read_authority, &core, &method).await
    {
        return Ok(response);
    }
    #[cfg(feature = "sparql")]
    if let Some(response) = try_handle_sparql(
        req_id,
        core.clone(),
        &method,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    )
    .await
    {
        return Ok(response);
    }
    #[cfg(feature = "owl")]
    if let Some(response) = try_handle_owl(req_id, read_authority, core.clone(), &method).await {
        return Ok(response);
    }
    #[cfg(feature = "obda")]
    if let Some(response) = try_handle_obda(state, req_id, read_authority, &method).await {
        return Ok(response);
    }
    if let Some(response) = try_handle_rules(
        req_id,
        &core,
        &method,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    )
    .await
    {
        return Ok(response);
    }
    #[cfg(any(feature = "shacl", feature = "shex"))]
    if let Some(response) =
        try_handle_validation(req_id, graph_name, read_authority, &core, &method).await
    {
        return Ok(response);
    }
    Err(method)
}

async fn try_handle_rdf(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    read_authority: Option<&GraphReadAuthority>,
    core: &Arc<GraphCore>,
    method: &Method,
) -> Option<Response> {
    match method {
        Method::AddTriples { turtle, ntriples } => Some(
            super::triples::handle_add_triples(
                req_id,
                graph_name,
                core,
                turtle.clone(),
                ntriples.clone(),
            )
            .await,
        ),
        Method::GetRdf => {
            let authority =
                read_authority.expect("GetRdf must carry the universal served-read authority");
            let projected = authority.project_core(core);
            Some(super::triples::handle_get_rdf(req_id, graph_name, &projected).await)
        }
        Method::RemoveTriples { turtle, ntriples } => Some(
            super::triples::handle_remove_triples(
                #[cfg(feature = "shacl")]
                state,
                req_id,
                graph_name,
                core,
                turtle.clone(),
                ntriples.clone(),
            )
            .await,
        ),
        Method::DropNamedGraph => {
            Some(super::triples::handle_drop_named_graph(req_id, graph_name, core).await)
        }
        _ => None,
    }
}

#[cfg(feature = "sparql")]
async fn try_handle_sparql(
    req_id: u64,
    core: Arc<GraphCore>,
    method: &Method,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Option<Response> {
    match method {
        Method::Sparql {
            query,
            base_iri,
            type_convention,
        } => Some(
            super::sparql::handle_sparql(
                req_id,
                core,
                query.clone(),
                base_iri.clone(),
                type_convention.clone(),
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            )
            .await,
        ),
        _ => None,
    }
}

#[cfg(feature = "owl")]
async fn try_handle_owl(
    req_id: u64,
    read_authority: Option<&GraphReadAuthority>,
    core: Arc<GraphCore>,
    method: &Method,
) -> Option<Response> {
    match method {
        Method::OwlReason {
            ontology,
            target_class,
            class_base,
            min_confidence,
        } => {
            let authority =
                read_authority.expect("OwlReason must carry the universal served-read authority");
            let projected = authority.project_core(&core);
            Some(
                super::reasoning::handle_owl_reason(
                    req_id,
                    &projected,
                    ontology.clone(),
                    target_class.clone(),
                    class_base.clone(),
                    *min_confidence,
                )
                .await,
            )
        }
        Method::OwlExplain { ontology, sub, sup } => {
            let authority =
                read_authority.expect("OwlExplain must carry the universal served-read authority");
            let projected = authority.project_core(&core);
            Some(
                super::reasoning::handle_owl_explain(
                    req_id,
                    &projected,
                    ontology.clone(),
                    sub.clone(),
                    sup.clone(),
                )
                .await,
            )
        }
        _ => None,
    }
}

#[cfg(feature = "obda")]
async fn try_handle_obda(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: Option<&GraphReadAuthority>,
    method: &Method,
) -> Option<Response> {
    let Method::SparqlVirtual {
        query,
        mapping,
        tables,
        external_sources,
    } = method
    else {
        return None;
    };
    let authority = read_authority
        .and_then(GraphReadAuthority::carrier)
        .expect("SparqlVirtual must carry verified tenant authority");
    let persist_dir = match state.read().await.persist_dir.clone() {
        Some(dir) => std::path::PathBuf::from(dir),
        None => {
            return Some(Response::err(
                req_id,
                "owner-scoped SQL catalog requires the configured persistence directory"
                    .to_string(),
            ));
        }
    };
    Some(
        super::obda::handle_sparql_virtual(
            req_id,
            authority.clone(),
            persist_dir,
            query.clone(),
            mapping.clone(),
            tables.clone(),
            external_sources.clone(),
        )
        .await,
    )
}

async fn try_handle_rules(
    req_id: u64,
    core: &Arc<GraphCore>,
    method: &Method,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Option<Response> {
    let Method::RunRules {
        ontology_ttl,
        rules,
        query_predicate,
        min_confidence,
        derived_only,
    } = method
    else {
        return None;
    };
    Some(
        super::reasoning::handle_run_rules(super::reasoning::RunRulesRequest {
            req_id,
            core,
            ontology_ttl: ontology_ttl.clone(),
            rules: rules.clone(),
            query_predicate: query_predicate.clone(),
            min_confidence: *min_confidence,
            derived_only: *derived_only,
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            rls,
        })
        .await,
    )
}

#[cfg(any(feature = "shacl", feature = "shex"))]
async fn try_handle_validation(
    req_id: u64,
    graph_name: &str,
    read_authority: Option<&GraphReadAuthority>,
    core: &Arc<GraphCore>,
    method: &Method,
) -> Option<Response> {
    match method {
        #[cfg(feature = "shacl")]
        Method::ShaclValidate { shapes, data_graph } => {
            let authority = read_authority
                .expect("ShaclValidate must carry the universal served-read authority");
            let projected = authority.project_core(core);
            Some(
                super::validation::handle_shacl_validate(
                    req_id,
                    graph_name,
                    &projected,
                    shapes.clone(),
                    data_graph.clone(),
                )
                .await,
            )
        }
        #[cfg(feature = "shacl")]
        Method::IcvConfigure { .. } => unreachable!(
            "IcvConfigure is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        #[cfg(feature = "shex")]
        Method::ShexValidate {
            schema,
            data_graph,
            shape_map,
        } => {
            let authority =
                read_authority.expect("ShexValidate must carry the universal served-read authority");
            let projected = authority.project_core(core);
            Some(
                super::validation::handle_shex_validate(
                    req_id,
                    graph_name,
                    &projected,
                    schema.clone(),
                    data_graph.clone(),
                    shape_map.clone(),
                )
                .await,
            )
        }
        _ => None,
    }
}
