//! Graph Store HTTP routing and RDF conversion helpers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph::GraphCore;
use crate::server::http1::HttpMessage;
use crate::server::ServerState;

use super::{
    choose_ct, parse_form, percent_decode, serialize_graph, signed_request, DEFAULT_GRAPH_ENV,
    GRAPH_FORMS, SPARQL_HTTP_UPDATE_EVENT,
};

/// W3C SPARQL 1.1 Graph Store HTTP Protocol: route reads and signed writes for one graph.
pub(super) async fn handle_graph_store(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    req: &HttpMessage,
    path: &str,
    query_string: &str,
) -> (&'static str, &'static str, String) {
    let params = parse_form(query_string);
    let Some(graph) = gsp_target(path, &params) else {
        return (
            "400 Bad Request",
            "text/plain",
            "graph store protocol: name the graph via /rdf-graphs/service?graph=<iri> \
             (or ?default) or /rdf-graphs/<name>"
                .to_string(),
        );
    };

    match req.method.as_str() {
        "GET" | "HEAD" => graph_read(state, req, &params, graph).await,
        "PUT" | "POST" => graph_write(state, req, graph).await,
        "DELETE" => graph_delete(state, req, graph).await,
        _ => (
            "405 Method Not Allowed",
            "text/plain",
            "graph store protocol: use GET/PUT/POST/DELETE/HEAD".to_string(),
        ),
    }
}

async fn graph_read(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    req: &HttpMessage,
    params: &HashMap<String, String>,
    graph: String,
) -> (&'static str, &'static str, String) {
    // Unlike writes below, this leg has no verified request-carrier mechanism yet.
    if crate::server::access::unauthenticated_carrier_denied(None) {
        crate::metrics::access_denied();
        return (
            "403 Forbidden",
            "text/plain",
            "ACCESS_DENIED: Graph Store HTTP GET/HEAD has no verified \
             request-carrier mechanism yet"
                .to_string(),
        );
    }
    let core = {
        let s = state.read().await;
        s.registry.get(&graph).map(|e| e.core.clone())
    };
    let Some(core) = core else {
        return (
            "404 Not Found",
            "text/plain",
            format!("no such graph: {graph}"),
        );
    };
    let ct = choose_ct(
        req.header("accept"),
        params
            .get("output")
            .or_else(|| params.get("format"))
            .map(|s| s.as_str()),
        GRAPH_FORMS,
    );
    let head_only = req.method == "HEAD";
    let out = tokio::task::spawn_blocking(move || {
        let triples = export_graph(&core, &graph)?;
        serialize_graph(ct, &triples)
    })
    .await;
    match out {
        Ok(Ok(body)) => ("200 OK", ct, if head_only { String::new() } else { body }),
        Ok(Err(e)) => ("500 Internal Server Error", "text/plain", e),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain",
            format!("compute task failed: {e}"),
        ),
    }
}

async fn graph_write(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    req: &HttpMessage,
    graph: String,
) -> (&'static str, &'static str, String) {
    let ntriples = match graph_body_as_ntriples(req) {
        Ok(value) => value,
        Err(error) => return ("400 Bad Request", "text/plain", error),
    };
    let created = !state.read().await.registry.exists(&graph);
    let query = if req.method == "PUT" {
        format!("CLEAR DEFAULT; INSERT DATA {{\n{ntriples}}}")
    } else {
        format!("INSERT DATA {{\n{ntriples}}}")
    };
    let method = crate::protocol::Method::ApplyMutation {
        event_type: SPARQL_HTTP_UPDATE_EVENT.to_string(),
        query,
    };
    let request = match signed_request(req, graph, method) {
        Ok(request) => request,
        Err(error) => return ("401 Unauthorized", "text/plain", error),
    };
    let response = crate::server::dispatch::dispatch(state, request).await;
    mutation_response(response.error, created)
}

fn graph_body_as_ntriples(req: &HttpMessage) -> Result<String, String> {
    let triples = parse_rdf_body(
        &req.header("content-type").to_ascii_lowercase(),
        &req.text(),
    )
    .map_err(|error| format!("parse RDF body: {error}"))?;
    eg_rdf::mapping::to_ntriples(&triples)
}

async fn graph_delete(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    req: &HttpMessage,
    graph: String,
) -> (&'static str, &'static str, String) {
    if !state.read().await.registry.exists(&graph) {
        return (
            "404 Not Found",
            "text/plain",
            format!("no such graph: {graph}"),
        );
    }
    let method = crate::protocol::Method::ApplyMutation {
        event_type: SPARQL_HTTP_UPDATE_EVENT.to_string(),
        query: "CLEAR DEFAULT".to_string(),
    };
    let request = match signed_request(req, graph, method) {
        Ok(request) => request,
        Err(error) => return ("401 Unauthorized", "text/plain", error),
    };
    let response = crate::server::dispatch::dispatch(state, request).await;
    mutation_response(response.error, false)
}

fn mutation_response(error: Option<String>, created: bool) -> (&'static str, &'static str, String) {
    match error {
        None if created => ("201 Created", "text/plain", String::new()),
        None => ("204 No Content", "text/plain", String::new()),
        Some(error) if error.starts_with("ACCESS_DENIED") => ("403 Forbidden", "text/plain", error),
        Some(error) => ("400 Bad Request", "text/plain", error),
    }
}

/// Resolve the Graph Store target from indirect query or direct path naming.
pub(super) fn gsp_target(path: &str, params: &HashMap<String, String>) -> Option<String> {
    if path == "/rdf-graphs/service" || path == "/rdf-graphs/service/" {
        if params.contains_key("default") {
            return Some(gsp_default_graph());
        }
        return params.get("graph").filter(|g| !g.is_empty()).cloned();
    }
    let name = path.strip_prefix("/rdf-graphs/")?;
    if name.is_empty() {
        return None;
    }
    Some(percent_decode(name))
}

pub(super) fn gsp_default_graph() -> String {
    std::env::var(DEFAULT_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string())
}

pub(super) fn parse_rdf_body(
    content_type: &str,
    body: &str,
) -> Result<Vec<eg_rdf::oxrdf::Triple>, String> {
    if content_type.contains("n-triples") || content_type.contains("ntriples") {
        eg_rdf::mapping::parse_ntriples(body)
    } else {
        eg_rdf::mapping::parse_turtle(body)
    }
}

pub(super) fn export_graph(
    core: &GraphCore,
    name: &str,
) -> Result<Vec<eg_rdf::oxrdf::Triple>, String> {
    eg_rdf::mapping::export_triples(core, name)
}
