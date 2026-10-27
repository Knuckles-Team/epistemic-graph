//! Caddy connector competency coverage over the infrastructure vocabulary.
//!
//! This is a disposable, offline fixture for the R009.1 question.  It keeps
//! source, record identifier, and as-of metadata on every answer row so a
//! future native execution can prove the connector's provenance contract.
#![cfg(feature = "sparql")]

mod common;

use common::parse_all;
use eg_core::graph::{GraphCore, GraphView};
use eg_rdf::mapping::{load_triples, parse_turtle, IriStore};
use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

fn graph(schema: &[&str], fixture: &str) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(schema)).unwrap();
    load_triples(&core, &mut iris, "fixture", parse_turtle(fixture).unwrap()).unwrap();
    core.analysis_snapshot()
}

fn select(view: &GraphView, query: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let outcome = execute(
        &Dataset::new(view, Vec::new()),
        query,
        &Projection::raw(),
        None,
    )
    .expect("query executes");
    let table = match outcome {
        QueryOutcome::Solutions(table) => table,
        other => panic!("expected a SELECT solution table, got {other:?}"),
    };
    let (vars, rows) = table.to_rows();
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.expect("every selected variable binds"))
                .collect()
        })
        .collect();
    (vars, rows)
}

fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

// Caddy R009.1 criterion body; keep unbound until the native Rust test runs.
#[test]
fn caddy_routes_to_upstream_and_serves_hostnames_answer() {
    // "Which routes proxy to a given upstream, and which hostnames are served
    // by each site block?" — answered from caddy alone (Vocabulary:
    // infrastructure-v1.ttl). Frozen as-of: 2026-01-03T00:00:00Z.
    //
    // routeHostname and upstreamAddress are the producer terms tracked by
    // R068.6/R068.7.  The fixture intentionally keeps them local until those
    // ontology declarations land; this source-only body makes the dependency
    // and the required adversarial rows executable once the native test runs.
    let fixture = r#"
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix au: <https://agent-utilities.dev/ontology#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:caddy:fixture/> .

f:backend-api a au:PlatformService .
f:backend-other a au:PlatformService .

# The target upstream has two hostnames in one route/site block.
f:route-api a infra:ProxyRoute ;
    infra:routesTo f:backend-api ;
    infra:routeHostname "api.example.test" ;
    infra:routeHostname "www.api.example.test" ;
    infra:upstreamAddress "http://backend-api:8080" ;
    dc:source "caddy" ;
    dc:identifier "caddy:route:api" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Same hostname, but the wrong upstream: the query must filter it out.
f:route-other a infra:ProxyRoute ;
    infra:routesTo f:backend-other ;
    infra:routeHostname "api.example.test" ;
    infra:upstreamAddress "http://backend-other:8080" ;
    dc:source "caddy" ;
    dc:identifier "caddy:route:other" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Same upstream, but a different site block: it should remain a separate answer
# row because the question asks which hostnames each block serves.
f:route-admin a infra:ProxyRoute ;
    infra:routesTo f:backend-api ;
    infra:routeHostname "admin.example.test" ;
    infra:upstreamAddress "http://backend-api:8080" ;
    dc:source "caddy" ;
    dc:identifier "caddy:route:admin" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;
    let view = graph(
        &[include_str!("../../eg-core/ontology/infrastructure-v1.ttl")],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX infra: <http://knuckles.team/kg/infrastructure#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX f: <urn:eg-cq-conn:caddy:fixture/>
SELECT ?route ?hostname ?upstream ?source ?record ?asof WHERE {
  ?route a infra:ProxyRoute ;
         infra:routeHostname ?hostname ;
         infra:upstreamAddress ?upstream ;
         infra:routesTo f:backend-api ;
         dc:source ?source ;
         dc:identifier ?record ;
         dc:date ?asof .
  FILTER(?upstream = "http://backend-api:8080")
}"#,
    );
    let idx = |column: &str| vars.iter().position(|name| name == column).unwrap();
    let i_route = idx("route");
    let i_hostname = idx("hostname");
    let i_upstream = idx("upstream");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");

    let mut route_hostnames: Vec<(String, String)> = rows
        .iter()
        .map(|row| (local(&row[i_route]).to_string(), row[i_hostname].clone()))
        .collect();
    route_hostnames.sort();
    assert_eq!(
        route_hostnames,
        vec![
            ("route-admin".to_string(), "admin.example.test".to_string()),
            ("route-api".to_string(), "api.example.test".to_string()),
            (
                "route-api".to_string(),
                "www.api.example.test".to_string()
            ),
        ],
        "all site blocks for the target upstream answer: {rows:?}"
    );

    // The wrong-upstream row must not leak into the answer.  The admin row is
    // intentionally retained as a distinct site block for the same upstream.
    assert_eq!(rows.len(), 3, "one row per target hostname: {rows:?}");
    for row in &rows {
        assert_eq!(row[i_upstream], "http://backend-api:8080");
        assert_eq!(row[i_source], "caddy");
        assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
    }
    assert_eq!(
        rows.iter()
            .filter(|row| row[i_record] == "caddy:route:api")
            .count(),
        2
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row[i_record] == "caddy:route:admin")
            .count(),
        1
    );
}
