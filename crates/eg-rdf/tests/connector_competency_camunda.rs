//! Camunda connector competency coverage over the process-intelligence vocabulary.
//!
//! This is a disposable, offline fixture for the R010.1 question.  It keeps
//! source, record identifier, and as-of metadata on the process-instance row
//! so a future native execution can prove the connector's provenance contract.
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

// Camunda R010.1 criterion body; keep unbound until the native Rust test runs.
#[test]
fn camunda_open_incident_stopped_activity_carries_provenance() {
    // "Which process instances have an open incident, and at which activity
    // did they stop?" — answered from Camunda alone (Vocabulary:
    // process_intelligence-v1.ttl). Frozen as-of: 2026-01-03T00:00:00Z.
    //
    // ProcessInstance, instanceOfProcessModel, stoppedAtActivity, and
    // raisedIncident are the producer terms tracked by R072.4-.7.  They are
    // intentionally used in this fixture before their ontology declarations
    // land; this source-only body makes the dependency and adversarial rows
    // executable once the native test runs.
    let fixture = r#"
@prefix proc: <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:camunda:fixture/> .

f:model-order a proc:ProcessModelVersion .
f:activity-approve a proc:ProcessEventType .

# The matching process instance stopped at approval and has an open incident.
f:instance-open a proc:ProcessInstance ;
    proc:instanceOfProcessModel f:model-order ;
    proc:stoppedAtActivity f:activity-approve ;
    proc:raisedIncident f:incident-payment ;
    dc:source "camunda" ;
    dc:identifier "camunda:instance:order-42" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:incident-payment a proc:ApplicationEvent ;
    proc:incidentStatus "open" .

# A stopped instance with a resolved incident must not answer.
f:instance-resolved a proc:ProcessInstance ;
    proc:instanceOfProcessModel f:model-order ;
    proc:stoppedAtActivity f:activity-approve ;
    proc:raisedIncident f:incident-resolved ;
    dc:source "camunda" ;
    dc:identifier "camunda:instance:order-41" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:incident-resolved a proc:ApplicationEvent ;
    proc:incidentStatus "resolved" .

# An open incident without a stopped activity must not answer.
f:instance-running a proc:ProcessInstance ;
    proc:instanceOfProcessModel f:model-order ;
    proc:raisedIncident f:incident-running ;
    dc:source "camunda" ;
    dc:identifier "camunda:instance:order-40" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:incident-running a proc:ApplicationEvent ;
    proc:incidentStatus "open" .
"#;
    let view = graph(
        &[include_str!(
            "../../eg-core/ontology/process_intelligence-v1.ttl"
        )],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX proc: <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?instance ?activity ?incident ?status ?source ?record ?asof WHERE {
  ?instance a proc:ProcessInstance ;
            proc:instanceOfProcessModel ?model ;
            proc:stoppedAtActivity ?activity ;
            proc:raisedIncident ?incident ;
            dc:source ?source ;
            dc:identifier ?record ;
            dc:date ?asof .
  ?incident a proc:ApplicationEvent ;
            proc:incidentStatus ?status .
  FILTER(?status = "open")
}"#,
    );
    let idx = |column: &str| vars.iter().position(|name| name == column).unwrap();
    let i_instance = idx("instance");
    let i_activity = idx("activity");
    let i_incident = idx("incident");
    let i_status = idx("status");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");

    assert_eq!(
        rows.len(),
        1,
        "only the open, stopped instance answers: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(local(&row[i_instance]), "instance-open");
    assert_eq!(local(&row[i_activity]), "activity-approve");
    assert_eq!(local(&row[i_incident]), "incident-payment");
    assert_eq!(row[i_status], "open");
    assert_eq!(row[i_source], "camunda");
    assert_eq!(row[i_record], "camunda:instance:order-42");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}
