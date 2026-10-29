//! Connector competency questions (MEDULLA-CQ-CONN), bulk chunk 2 (P12 question pairs,
//! R018–R034): fixture SPARQL answers over the connector's assigned ontology
//! vocabulary, in the established pattern of
//! `crates/eg-rdf/tests/connector_competency.rs`.
//!
//! Each test records a disposable fixture (no live credentials, no network —
//! `specs/connector-competency/test-spec.md`), loads it together with the
//! vocabulary module the question row assigns, executes the question through
//! the native SPARQL surface, and asserts a non-empty answer whose rows carry
//! provenance (source, record id, as-of). The `dc:` provenance triples stand in
//! for the fields the connector's virtual mapping attaches to fetched records
//! at query time.
//!
//! This chunk lands only the rows fully expressible in the vocabulary on main.
//! The remaining R018–R034 question rows are blocked on their producer
//! vocabulary rows (MEDULLA-CQ-CONN-R068–R082 and MEDULLA-SUPPLY-VOCAB-R003), which are
//! not yet on main, and the freshrss / jellyfin / kafka rows (R022, R027, R029)
//! are among the plan's 23 MISSING connectors with no assigned vocabulary.
#![cfg(feature = "sparql")]

mod common;

use common::parse_all;
use eg_core::graph::{GraphCore, GraphView};
use eg_rdf::mapping::{load_triples, parse_turtle, IriStore};
use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

/// Load the assigned vocabulary documents and the fixture into one graph and
/// return the analysis view the SPARQL evaluator scans.
fn graph(schema: &[&str], fixture: &str) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(schema)).unwrap();
    load_triples(&core, &mut iris, "fixture", parse_turtle(fixture).unwrap()).unwrap();
    core.analysis_snapshot()
}

/// Execute a SELECT and return `(columns, rows)` with every cell as its lexical form.
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

/// The local name of a bracketed node-id cell (`<urn:…/client-portal>` → `client-portal`).
fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

// spec: MEDULLA-CQ-CONN-R030.1
#[test]
fn keycloak_user_clients_and_roles_answer() {
    // "Which clients can a given user sign in to, and through which role?" —
    // answered from the keycloak connector alone (Vocabulary: identity-v1.ttl,
    // with :Application from the shared service-operations core). Frozen as-of:
    // 2026-01-03T00:00:00Z.
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:keycloak:fixture/> .

# alice signs in to the portal client through the editor role — the answer row.
f:user-alice a :IdentityUser ;
    :assignedApp f:client-portal ;
    :hasRole f:role-editor ;
    dc:source "keycloak" ;
    dc:identifier "kc:rec:user-alice" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:client-portal a :Application .
f:role-editor a :IdentityRole .

# bob signs in to the archive client through the viewer role — a different user,
# so his assignment must not answer alice's question.
f:user-bob a :IdentityUser ;
    :assignedApp f:client-archive ;
    :hasRole f:role-viewer .
f:client-archive a :Application .
f:role-viewer a :IdentityRole .
"#;
    let view = graph(
        &[
            include_str!("../../eg-core/ontology/identity-v1.ttl"),
            include_str!("../../eg-core/ontology/service_operations-v1.ttl"),
        ],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX f: <urn:eg-cq-conn:keycloak:fixture/>
SELECT ?client ?role ?source ?record ?asof WHERE {
  f:user-alice a :IdentityUser ;
               :assignedApp ?client ;
               :hasRole ?role ;
               dc:source ?source ;
               dc:identifier ?record ;
               dc:date ?asof .
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_client = idx("client");
    let i_role = idx("role");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(
        rows.len(),
        1,
        "exactly one client/role pair answers for alice: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(local(&row[i_client]), "client-portal");
    assert_eq!(local(&row[i_role]), "role-editor");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[i_source], "keycloak");
    assert_eq!(row[i_record], "kc:rec:user-alice");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}
