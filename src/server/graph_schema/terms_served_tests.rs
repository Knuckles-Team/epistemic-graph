//! `GraphSchemaClasses` over the served dispatch path (EH-389 visibility ruling,
//! 2026-09-24): a page shows declaring source keys, but only those of the REQUEST
//! graph's own schema-source set -- the exact set `GraphSchemaList` lists -- and a
//! request from another tenant never reaches the handler.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::graph::{GraphSchemaSource, GraphSchemaSources, SchemaSourceOrigin};
use crate::protocol::{GraphType, Method, Response, ResultPayload};
use crate::server::auth::{build_current_test_request, dispatch_test_on_heap};
use crate::server::state::ServerState;
use eg_types::graph_schema::{GraphSchemaClassesView, GraphSchemaSourcesView};

const SECRET: &str = "graph-schema-classes-served-secret";
const AGENT: &str = "schema-vocabulary-reader";
const TENANT: &str = "tenant-shared";

fn admin_sources(name: &str, class_iri: &str) -> Arc<GraphSchemaSources> {
    let ontology = format!("<{class_iri}> a <http://www.w3.org/2002/07/owl#Class> .\n");
    let source = GraphSchemaSource::new(
        SchemaSourceOrigin::Admin {
            name: name.to_string(),
        },
        None,
        Some(Arc::from(ontology.as_str())),
        0,
    )
    .expect("a bounded admin source");
    let dynamic = BTreeMap::from([(format!("admin:{name}"), source)]);
    Arc::new(GraphSchemaSources::with_dynamic(dynamic).expect("composes with the core"))
}

async fn state_with_two_graphs() -> Arc<RwLock<ServerState>> {
    let state = ServerState::new_for_test(SECRET, ServerState::test_isolation(AGENT));
    let state = Arc::new(RwLock::new(state));
    {
        let mut guard = state.write().await;
        for (graph, name, iri) in [
            ("vocab-a", "alpha", "https://example.org/a#OnlyInA"),
            ("vocab-b", "beta", "https://example.org/b#OnlyInB"),
        ] {
            guard
                .registry
                .create_graph(graph, GraphType::Global, None)
                .expect("create graph");
            let core = guard.registry.get(graph).expect("resident").core.clone();
            core.install_schema_sources(admin_sources(name, iri));
        }
    }
    state
}

async fn call(
    state: &Arc<RwLock<ServerState>>,
    tenant: &str,
    graph: &str,
    method: Method,
) -> Response {
    let request = build_current_test_request(SECRET, tenant, 1, graph, AGENT, method);
    dispatch_test_on_heap(state, request).await
}

fn raw<T: serde::de::DeserializeOwned>(response: &Response) -> T {
    assert!(response.error.is_none(), "refused: {:?}", response.error);
    match &response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).expect("decodes"),
        other => panic!("expected a Raw result, got {other:?}"),
    }
}

fn classes() -> Method {
    Method::GraphSchemaClasses {
        kind: None,
        cursor: None,
        limit: 1000,
    }
}

async fn every_term(state: &Arc<RwLock<ServerState>>, graph: &str) -> Vec<(String, String)> {
    let mut terms = Vec::new();
    let mut cursor = None;
    loop {
        let method = Method::GraphSchemaClasses {
            kind: None,
            cursor: cursor.take(),
            limit: 1000,
        };
        let page: GraphSchemaClassesView = raw(&call(state, TENANT, graph, method).await);
        terms.extend(
            page.terms
                .as_slice()
                .iter()
                .map(|term| (term.iri.clone(), term.source_id.clone())),
        );
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return terms,
        }
    }
}

#[tokio::test]
async fn a_page_shows_only_the_request_graphs_own_sources() {
    let state = state_with_two_graphs().await;
    let terms = every_term(&state, "vocab-a").await;
    assert!(terms.contains(&(
        "https://example.org/a#OnlyInA".to_string(),
        "admin:alpha".to_string()
    )));
    assert!(
        terms
            .iter()
            .all(|(iri, source)| !iri.contains("/b#") && source != "admin:beta"),
        "graph B's source must never appear on graph A"
    );
    let listed: GraphSchemaSourcesView =
        raw(&call(&state, TENANT, "vocab-a", Method::GraphSchemaList).await);
    let listed: Vec<&str> = listed
        .core_sources
        .as_slice()
        .iter()
        .chain(listed.dynamic_sources.as_slice())
        .map(|source| source.source_id.as_str())
        .collect();
    assert!(
        terms
            .iter()
            .all(|(_, source)| listed.contains(&source.as_str())),
        "every declaring source is one GraphSchemaList shows for the same graph"
    );
}

#[tokio::test]
async fn a_request_from_another_tenant_is_refused() {
    let state = state_with_two_graphs().await;
    let response = call(&state, "tenant-foreign", "vocab-a", classes()).await;
    let error = response.error.expect("a foreign tenant is refused");
    assert!(
        !error.contains("OnlyInA") && !error.contains("admin:alpha"),
        "{error}"
    );
    assert!(response.result.is_none());
}
