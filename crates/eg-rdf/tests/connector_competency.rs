//! Connector competency questions (EG-CQ-CONN): fixture SPARQL answers over the
//! connector's assigned ontology vocabulary.
//!
//! Each test records a disposable fixture (no live credentials, no network —
//! `specs/connector-competency/test-spec.md`), loads it together with the
//! vocabulary module the question row assigns, executes the question through
//! the native SPARQL surface, and asserts a non-empty answer whose rows carry
//! provenance (source, record id, as-of). The `dc:` provenance triples stand in
//! for the fields the connector's virtual mapping attaches to fetched records
//! at query time (`specs/connector-competency/plan.md`: "Results carry source,
//! record ID, and as-of metadata ... from the virtual mapping").
//!
//! Time windows are expressed with the `YEAR()/MONTH()/DAY()/HOURS()/MINUTES()`
//! datetime accessors rather than lexical string comparisons: the engine's
//! ordering comparator parses operands as f64 (`eval_expr_bool_compare`,
//! `crates/eg-rdf/src/sparql/expression.rs`), so `FILTER(STR(?t) >= "…")`
//! silently drops dateTime lexicals.
#![cfg(feature = "sparql")]

mod common;

use common::parse_all;
use eg_core::graph::{GraphCore, GraphView};
use eg_rdf::mapping::{load_triples, parse_turtle, IriStore};
use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

/// Load the assigned vocabulary documents and the fixture into one graph and
/// return the analysis view the SPARQL evaluator scans.
fn graph(schema: &[&str], fixture: &str) -> GraphView {
    graph_with(schema, &[fixture])
}

/// Like [`graph`], loading several disposable fixtures (one per question or
/// connector) into the same graph.
fn graph_with(schema: &[&str], fixtures: &[&str]) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(schema)).unwrap();
    for (index, fixture) in fixtures.iter().enumerate() {
        let source = format!("fixture-{index}");
        load_triples(&core, &mut iris, &source, parse_turtle(fixture).unwrap()).unwrap();
    }
    core.analysis_snapshot()
}

/// A fixture declaring each connector's access contract in the virtual-graph
/// vocabulary: one MCP `vg:SourceConnection` per source with a discovered
/// `vg:MetadataContract` and an approved `vg:VirtualMapping` per mapped class.
fn connection_fixture(connections: &[(&str, &[&str])]) -> String {
    let mut ttl = String::from(
        "@prefix vg: <http://knuckles.team/kg/virtual-graph#> .\n\
         @prefix dc: <http://purl.org/dc/terms/> .\n\
         @prefix k: <urn:eg-cq-conn:contract/> .\n",
    );
    for (source, classes) in connections {
        ttl.push_str(&format!(
            "k:conn-{source} a vg:SourceConnection ; dc:identifier \"{source}\" ; \
             vg:sourceKind \"mcp\" ; vg:endpointRef k:ep-{source} ; vg:declaredCapability \"read\" .\n\
             k:ep-{source} a vg:EndpointRef ; vg:url \"mcp://{source}\" .\n\
             k:contract-{source} a vg:MetadataContract ; vg:contractSource k:conn-{source} ; \
             vg:discoveredEntity \"records\" .\n"
        ));
        for (index, class) in classes.iter().enumerate() {
            ttl.push_str(&format!(
                "k:map-{source}-{index} a vg:VirtualMapping ; vg:mapsContract k:contract-{source} ; \
                 vg:mapsEntity \"records\" ; vg:keyField \"id\" ; vg:approved true ; \
                 vg:mapsOntologyClass <{class}> .\n"
            ));
        }
    }
    ttl
}

/// The `(source, class IRI)` pairs the graph declares as connected: a source
/// with an approved mapping onto the class.
fn connected_pairs(view: &GraphView) -> Vec<(String, String)> {
    let (vars, rows) = select(
        view,
        r#"
PREFIX vg: <http://knuckles.team/kg/virtual-graph#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?name ?class WHERE {
  ?conn a vg:SourceConnection ; dc:identifier ?name .
  ?contract a vg:MetadataContract ; vg:contractSource ?conn .
  ?mapping a vg:VirtualMapping ; vg:mapsContract ?contract ;
           vg:mapsOntologyClass ?class ; vg:approved true .
}"#,
    );
    let name = vars.iter().position(|v| v == "name").unwrap();
    let class = vars.iter().position(|v| v == "class").unwrap();
    rows.iter()
        .map(|row| {
            (
                row[name].clone(),
                row[class]
                    .trim_matches(|c| c == '<' || c == '>')
                    .to_string(),
            )
        })
        .collect()
}

/// Assert an answer row's `source` is served through an approved mapping onto `class`.
fn assert_served_by_contract(connected: &[(String, String)], source: &str, class: &str) {
    assert!(
        connected.iter().any(|(s, c)| s == source && c == class),
        "{source} has no approved mapping onto {class}: {connected:?}"
    );
}

/// The typed abstention of a question: the required sources with no declared
/// connection, named. Empty when every required source is connected.
fn unconnected_sources(connected: &[(String, String)], required: &[&str]) -> Vec<String> {
    required
        .iter()
        .filter(|source| !connected.iter().any(|(s, _)| s == **source))
        .map(|source| source.to_string())
        .collect()
}

const INFRA: &str = "http://knuckles.team/kg/infrastructure#";

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

/// The local name of a bracketed node-id cell (`<urn:…/host-1>` → `host-1`).
fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

const FAN_DUTY_FIXTURE: &str = r#"
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:fan-manager:fixture/> .

# host-1: fan duty above 80 while the temperature sits above its p95 threshold.
f:host-1 a infra:Host .
f:trend-host1 a infra:HealthTrend ;
    infra:affectsEntity f:host-1 ;
    infra:signal "fan_duty_pct" ;
    infra:avgControl "85.0"^^xsd:decimal .
f:anomaly-host1 a infra:HealthAnomaly ;
    infra:affectsEntity f:host-1 ;
    infra:signal "cpu_temp_c" ;
    infra:observed "78.0"^^xsd:decimal ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ;
    dc:identifier "fm:rec:anomaly-host1-1" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:baseline-host1 a infra:HealthBaseline ;
    infra:affectsEntity f:host-1 ;
    infra:signal "cpu_temp_c" ;
    infra:p95 "65.0"^^xsd:decimal .

# host-2: fan duty above 80 but the temperature inside its baseline — no row.
f:host-2 a infra:Host .
f:trend-host2 a infra:HealthTrend ;
    infra:affectsEntity f:host-2 ;
    infra:signal "fan_duty_pct" ;
    infra:avgControl "90.0"^^xsd:decimal .
f:anomaly-host2 a infra:HealthAnomaly ;
    infra:affectsEntity f:host-2 ;
    infra:signal "cpu_temp_c" ;
    infra:observed "50.0"^^xsd:decimal ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ;
    dc:identifier "fm:rec:anomaly-host2-1" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:baseline-host2 a infra:HealthBaseline ;
    infra:affectsEntity f:host-2 ;
    infra:signal "cpu_temp_c" ;
    infra:p95 "65.0"^^xsd:decimal .

# host-3: temperature above its baseline but the fan duty below 80 — no row.
f:host-3 a infra:Host .
f:trend-host3 a infra:HealthTrend ;
    infra:affectsEntity f:host-3 ;
    infra:signal "fan_duty_pct" ;
    infra:avgControl "40.0"^^xsd:decimal .
f:anomaly-host3 a infra:HealthAnomaly ;
    infra:affectsEntity f:host-3 ;
    infra:signal "cpu_temp_c" ;
    infra:observed "78.0"^^xsd:decimal ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ;
    dc:identifier "fm:rec:anomaly-host3-1" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:baseline-host3 a infra:HealthBaseline ;
    infra:affectsEntity f:host-3 ;
    infra:signal "cpu_temp_c" ;
    infra:p95 "65.0"^^xsd:decimal .
"#;

const FAN_DUTY_QUERY: &str = r#"
PREFIX infra: <http://knuckles.team/kg/infrastructure#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?host ?fanDuty ?temp ?p95 ?source ?record ?asof WHERE {
  ?host a infra:Host .
  ?trend a infra:HealthTrend ;
         infra:affectsEntity ?host ;
         infra:signal "fan_duty_pct" ;
         infra:avgControl ?fanDuty .
  ?anomaly a infra:HealthAnomaly ;
           infra:affectsEntity ?host ;
           infra:signal "cpu_temp_c" ;
           infra:observed ?temp ;
           infra:observedAt ?t .
  ?baseline a infra:HealthBaseline ;
            infra:affectsEntity ?host ;
            infra:signal "cpu_temp_c" ;
            infra:p95 ?p95 .
  ?anomaly dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(YEAR(?t) = 2026 && MONTH(?t) = 1 && DAY(?t) >= 2)
  FILTER(?fanDuty > 80)
  FILTER(?temp > ?p95)
}"#;

/// Answer R020.1 over `view`: the `(vars, rows)` of the fan-duty question.
fn fan_duty_answer(view: &GraphView) -> (Vec<String>, Vec<Vec<String>>) {
    select(view, FAN_DUTY_QUERY)
}

// spec: EG-CQ-CONN-R020.1
#[test]
fn fan_manager_duty_above_threshold_answers() {
    // "Which fans ran above 80 percent duty while the host temperature stayed
    // above its threshold?" — answered from the fan-manager connector alone
    // (Vocabulary: infrastructure-v1.ttl). Frozen as-of: 2026-01-03T00:00:00Z.
    let view = graph(
        &[include_str!("../../eg-core/ontology/infrastructure-v1.ttl")],
        FAN_DUTY_FIXTURE,
    );
    let (vars, rows) = fan_duty_answer(&view);
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_host = idx("host");
    let i_duty = idx("fanDuty");
    let i_temp = idx("temp");
    let i_p95 = idx("p95");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(rows.len(), 1, "exactly one host answers: {rows:?}");
    let row = &rows[0];
    assert_eq!(local(&row[i_host]), "host-1");
    assert_eq!(row[i_duty], "85.0");
    assert_eq!(row[i_temp], "78.0");
    assert_eq!(row[i_p95], "65.0");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[i_source], "fan-manager");
    assert_eq!(row[i_record], "fm:rec:anomaly-host1-1");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}

// spec: EG-CQ-CONN-R026.1
#[test]
fn home_assistant_out_of_range_sensors_answer() {
    // "Which sensors reported a value outside their normal range in the last 24
    // hours?" — answered from the home-assistant connector alone (Vocabulary:
    // infrastructure-v1.ttl), the worked example of plan.md. Frozen as-of:
    // 2026-01-03T00:00:00Z, so the last-24-hours window starts 2026-01-02T00:00:00Z.
    let fixture = r#"
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:home-assistant:fixture/> .

# sensor-battery: anomaly observed inside the frozen window.
f:sensor-battery a infra:Entity .
f:anomaly-battery a infra:HealthAnomaly ;
    infra:affectsEntity f:sensor-battery ;
    infra:signal "battery_v" ;
    infra:observed "0.9"^^xsd:decimal ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "home-assistant" ;
    dc:identifier "ha:rec:anomaly-battery-1" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:baseline-battery a infra:HealthBaseline ;
    infra:affectsEntity f:sensor-battery ;
    infra:signal "battery_v" ;
    infra:p95 "0.6"^^xsd:decimal .

# sensor-temp: anomaly observed before the window — no row.
f:sensor-temp a infra:Entity .
f:anomaly-temp a infra:HealthAnomaly ;
    infra:affectsEntity f:sensor-temp ;
    infra:signal "cpu_temp_c" ;
    infra:observed "78.0"^^xsd:decimal ;
    infra:observedAt "2025-12-25T06:00:00Z"^^xsd:dateTime ;
    dc:source "home-assistant" ;
    dc:identifier "ha:rec:anomaly-temp-1" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:baseline-temp a infra:HealthBaseline ;
    infra:affectsEntity f:sensor-temp ;
    infra:signal "cpu_temp_c" ;
    infra:p95 "65.0"^^xsd:decimal .

# sensor-idle: no anomaly at all — no row.
f:sensor-idle a infra:Entity .
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
SELECT ?entity ?signal ?observed ?p95 ?source ?record ?asof WHERE {
  ?entity a infra:Entity .
  ?anomaly a infra:HealthAnomaly ;
           infra:affectsEntity ?entity ;
           infra:observed ?observed ;
           infra:observedAt ?t ;
           infra:signal ?signal .
  ?baseline a infra:HealthBaseline ;
            infra:affectsEntity ?entity ;
            infra:signal ?signal ;
            infra:p95 ?p95 .
  ?anomaly dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(YEAR(?t) = 2026 && MONTH(?t) = 1 && DAY(?t) >= 2)
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_entity = idx("entity");
    let i_signal = idx("signal");
    let i_observed = idx("observed");
    let i_p95 = idx("p95");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(rows.len(), 1, "exactly one sensor answers: {rows:?}");
    let row = &rows[0];
    assert_eq!(local(&row[i_entity]), "sensor-battery");
    assert_eq!(row[i_signal], "battery_v");
    assert_eq!(row[i_observed], "0.9");
    assert_eq!(row[i_p95], "0.6");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[i_source], "home-assistant");
    assert_eq!(row[i_record], "ha:rec:anomaly-battery-1");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}

// spec: EG-CQ-CONN-R039.1
#[test]
fn microsoft_overlapping_meetings_answer() {
    // "Which meetings tomorrow overlap, and which attendees are double-booked?"
    // — answered from the microsoft connector alone (Vocabulary:
    // calendar-v1.ttl; Person from the imported core module). Frozen as-of:
    // 2026-01-02T00:00:00Z, so "tomorrow" is 2026-01-03.
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:microsoft:fixture/> .

f:attendee-alice a :Person .
f:attendee-bob a :Person .

# Two overlapping tomorrow meetings that alice attends — the double-booking.
f:meeting-standup a :CalendarEvent ;
    :scheduledStart "2026-01-03T09:00:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-03T10:00:00Z"^^xsd:dateTime ;
    :attendedBy f:attendee-alice ;
    dc:source "microsoft" ;
    dc:identifier "ms:rec:meeting-standup" ;
    dc:date "2026-01-02T00:00:00Z"^^xsd:dateTime .
f:meeting-review a :CalendarEvent ;
    :scheduledStart "2026-01-03T09:30:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-03T10:30:00Z"^^xsd:dateTime ;
    :attendedBy f:attendee-alice ;
    dc:source "microsoft" ;
    dc:identifier "ms:rec:meeting-review" ;
    dc:date "2026-01-02T00:00:00Z"^^xsd:dateTime .

# Overlaps the standup but is attended by bob only — no shared double-booking.
f:meeting-design a :CalendarEvent ;
    :scheduledStart "2026-01-03T09:45:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-03T10:15:00Z"^^xsd:dateTime ;
    :attendedBy f:attendee-bob ;
    dc:source "microsoft" ;
    dc:identifier "ms:rec:meeting-design" ;
    dc:date "2026-01-02T00:00:00Z"^^xsd:dateTime .

# Alice's lunch tomorrow overlaps nothing.
f:meeting-lunch a :CalendarEvent ;
    :scheduledStart "2026-01-03T12:00:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-03T13:00:00Z"^^xsd:dateTime ;
    :attendedBy f:attendee-alice ;
    dc:source "microsoft" ;
    dc:identifier "ms:rec:meeting-lunch" ;
    dc:date "2026-01-02T00:00:00Z"^^xsd:dateTime .

# Bob's overlapping pair on the day before tomorrow — outside the question.
f:meeting-past-a a :CalendarEvent ;
    :scheduledStart "2026-01-02T09:00:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-02T10:00:00Z"^^xsd:dateTime ;
    :attendedBy f:attendee-bob .
f:meeting-past-b a :CalendarEvent ;
    :scheduledStart "2026-01-02T09:30:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-02T10:30:00Z"^^xsd:dateTime ;
    :attendedBy f:attendee-bob .
"#;
    let view = graph(
        &[
            include_str!("../../eg-core/ontology/calendar-v1.ttl"),
            include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        ],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
SELECT ?m1 ?m2 ?attendee WHERE {
  ?m1 a :CalendarEvent ; :scheduledStart ?s1 ; :scheduledEnd ?e1 ; :attendedBy ?attendee .
  ?m2 a :CalendarEvent ; :scheduledStart ?s2 ; :scheduledEnd ?e2 ; :attendedBy ?attendee .
  FILTER(?m1 != ?m2)
  FILTER(YEAR(?s1) = 2026 && MONTH(?s1) = 1 && DAY(?s1) = 3)
  FILTER(YEAR(?s2) = 2026 && MONTH(?s2) = 1 && DAY(?s2) = 3)
  # Both starts are pinned to the same day, so minutes-of-day order is a
  # faithful overlap test: s1 < e2 and s2 < e1 in same-day time-of-day.
  FILTER((HOURS(?s1) * 60 + MINUTES(?s1)) < (HOURS(?e2) * 60 + MINUTES(?e2)))
  FILTER((HOURS(?s2) * 60 + MINUTES(?s2)) < (HOURS(?e1) * 60 + MINUTES(?e1)))
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_m1 = idx("m1");
    let i_m2 = idx("m2");
    let i_attendee = idx("attendee");
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|row| (local(&row[i_m1]).to_string(), local(&row[i_m2]).to_string()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("meeting-review".to_string(), "meeting-standup".to_string()),
            ("meeting-standup".to_string(), "meeting-review".to_string()),
        ],
        "only the standup/review overlap pairs answer: {rows:?}"
    );
    // The double-booked attendee is alice in both directions; bob's pairs never
    // share an overlapping pair, so he never appears.
    for row in &rows {
        assert_eq!(local(&row[i_attendee]), "attendee-alice", "{rows:?}");
    }
}

const FAN_CROSS_FIXTURE: &str = r#"
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:fan-manager-cross:fixture/> .

# Hosts as systems-manager records: the shared `host` concept the sources link through.
f:host-1 a infra:Host ; dc:source "systems-manager" ; dc:identifier "sm:rec:host-1" .
f:host-2 a infra:Host ; dc:source "systems-manager" ; dc:identifier "sm:rec:host-2" .
f:host-3 a infra:Host ; dc:source "systems-manager" ; dc:identifier "sm:rec:host-3" .
f:host-4 a infra:Host ; dc:source "systems-manager" ; dc:identifier "sm:rec:host-4" .
f:host-5 a infra:Host ; dc:source "systems-manager" ; dc:identifier "sm:rec:host-5" .

# fan-manager: one-hour fan-duty windows starting 2026-01-02T06:00:00Z.
f:trend-host-1 a infra:HealthTrend ; infra:affectsEntity f:host-1 ; infra:signal "fan_duty_pct" ;
    infra:max "100"^^xsd:decimal ; infra:windowS "3600"^^xsd:integer ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ; dc:identifier "fm:rec:trend-host-1" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:trend-host-2 a infra:HealthTrend ; infra:affectsEntity f:host-2 ; infra:signal "fan_duty_pct" ;
    infra:max "100"^^xsd:decimal ; infra:windowS "3600"^^xsd:integer ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ; dc:identifier "fm:rec:trend-host-2" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:trend-host-3 a infra:HealthTrend ; infra:affectsEntity f:host-3 ; infra:signal "fan_duty_pct" ;
    infra:max "70"^^xsd:decimal ; infra:windowS "3600"^^xsd:integer ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ; dc:identifier "fm:rec:trend-host-3" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:trend-host-4 a infra:HealthTrend ; infra:affectsEntity f:host-4 ; infra:signal "fan_duty_pct" ;
    infra:max "100"^^xsd:decimal ; infra:windowS "3600"^^xsd:integer ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ; dc:identifier "fm:rec:trend-host-4" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:trend-host-5 a infra:HealthTrend ; infra:affectsEntity f:host-5 ; infra:signal "fan_duty_pct" ;
    infra:max "100"^^xsd:decimal ; infra:windowS "3600"^^xsd:integer ;
    infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "fan-manager" ; dc:identifier "fm:rec:trend-host-5" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# lgtm: Grafana temperature alerts carried as health anomalies.
# host-1: inside the window (answers).
f:alert-host-1 a infra:HealthAnomaly ; infra:affectsEntity f:host-1 ; infra:signal "cpu_temp_c" ;
    infra:observed "82"^^xsd:decimal ; infra:observedAt "2026-01-02T06:30:00Z"^^xsd:dateTime ;
    dc:source "lgtm" ; dc:identifier "lgtm:rec:alert-host-1" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
# host-2: alert three hours after the window closed.
f:alert-host-2 a infra:HealthAnomaly ; infra:affectsEntity f:host-2 ; infra:signal "cpu_temp_c" ;
    infra:observed "82"^^xsd:decimal ; infra:observedAt "2026-01-02T09:00:00Z"^^xsd:dateTime ;
    dc:source "lgtm" ; dc:identifier "lgtm:rec:alert-host-2" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
# host-3: alert inside the window, but the fan peaked at 70 percent.
f:alert-host-3 a infra:HealthAnomaly ; infra:affectsEntity f:host-3 ; infra:signal "cpu_temp_c" ;
    infra:observed "82"^^xsd:decimal ; infra:observedAt "2026-01-02T06:30:00Z"^^xsd:dateTime ;
    dc:source "lgtm" ; dc:identifier "lgtm:rec:alert-host-3" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
# host-4: no alert at all.
# host-5: an alert inside the window, but on disk usage, not temperature.
f:alert-host-5 a infra:HealthAnomaly ; infra:affectsEntity f:host-5 ; infra:signal "disk_pct" ;
    infra:observed "97"^^xsd:decimal ; infra:observedAt "2026-01-02T06:30:00Z"^^xsd:dateTime ;
    dc:source "lgtm" ; dc:identifier "lgtm:rec:alert-host-5" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;

// The alert instance vocabulary (`:Alert`, `:alertState`, ...) is a producer
// row (EG-CQ-CONN-R068.8..) and is not on main, so the Grafana alert is a
// lgtm-sourced `:HealthAnomaly` on the host; the window is the fan trend's
// `observedAt` .. `observedAt + windowS`, both pinned to one day.
const FAN_CROSS_QUERY: &str = r#"
PREFIX infra: <http://knuckles.team/kg/infrastructure#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?host ?fanMax ?alertTemp ?hostSource ?hostRecord ?fanSource ?fanRecord ?alertSource ?alertRecord ?asof WHERE {
  ?host a infra:Host ; dc:source ?hostSource ; dc:identifier ?hostRecord .
  ?trend a infra:HealthTrend ; infra:affectsEntity ?host ; infra:signal "fan_duty_pct" ;
         infra:max ?fanMax ; infra:windowS ?windowS ; infra:observedAt ?ts ;
         dc:source ?fanSource ; dc:identifier ?fanRecord .
  ?alert a infra:HealthAnomaly ; infra:affectsEntity ?host ; infra:signal "cpu_temp_c" ;
         infra:observed ?alertTemp ; infra:observedAt ?at ;
         dc:source ?alertSource ; dc:identifier ?alertRecord ; dc:date ?asof .
  FILTER(?fanMax >= 100)
  FILTER(YEAR(?ts) = YEAR(?at) && MONTH(?ts) = MONTH(?at) && DAY(?ts) = DAY(?at))
  FILTER((HOURS(?at) * 3600 + MINUTES(?at) * 60) >= (HOURS(?ts) * 3600 + MINUTES(?ts) * 60))
  FILTER((HOURS(?at) * 3600 + MINUTES(?at) * 60) <= (HOURS(?ts) * 3600 + MINUTES(?ts) * 60 + ?windowS))
}"#;

fn assert_fan_cross_row(vars: &[String], rows: &[Vec<String>]) {
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    assert_eq!(rows.len(), 1, "exactly one host answers: {rows:?}");
    let row = &rows[0];
    assert_eq!(local(&row[idx("host")]), "host-1");
    assert_eq!(row[idx("fanMax")], "100");
    assert_eq!(row[idx("alertTemp")], "82");
    // Provenance from each of the three linked sources.
    assert_eq!(row[idx("hostSource")], "systems-manager");
    assert_eq!(row[idx("hostRecord")], "sm:rec:host-1");
    assert_eq!(row[idx("fanSource")], "fan-manager");
    assert_eq!(row[idx("fanRecord")], "fm:rec:trend-host-1");
    assert_eq!(row[idx("alertSource")], "lgtm");
    assert_eq!(row[idx("alertRecord")], "lgtm:rec:alert-host-1");
    assert_eq!(row[idx("asof")], "2026-01-03T00:00:00Z");
}

// spec: EG-CQ-CONN-R020.2
#[test]
fn fan_manager_max_duty_during_grafana_alert_answers() {
    // "Which hosts had fan duty at maximum during a window in which Grafana
    // alerted on temperature?" — fan-manager, systems-manager and lgtm linked
    // through the shared `host` concept (Vocabulary: infrastructure-v1.ttl).
    let view = graph(
        &[include_str!("../../eg-core/ontology/infrastructure-v1.ttl")],
        FAN_CROSS_FIXTURE,
    );
    let (vars, rows) = select(&view, FAN_CROSS_QUERY);
    assert_fan_cross_row(&vars, &rows);
}

// spec: EG-CQ-CONN-R020.3
#[test]
fn fan_manager_acceptance_through_declared_contracts() {
    // Both questions run against records served through the connectors'
    // declared access contracts (approved `vg:VirtualMapping` per class), each
    // answer row's source resolves to a mapping onto the answering class, and a
    // question whose linked source is not connected names it.
    let schema = [
        include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
        include_str!("../../eg-core/ontology/virtual_graph-v1.ttl"),
    ];
    let class = |name: &str| format!("{INFRA}{name}");
    let connections = connection_fixture(&[
        (
            "fan-manager",
            &[
                &class("HealthTrend"),
                &class("HealthAnomaly"),
                &class("HealthBaseline"),
            ],
        ),
        ("lgtm", &[&class("HealthAnomaly")]),
        ("systems-manager", &[&class("Host")]),
    ]);
    let view = graph_with(
        &schema,
        &[FAN_DUTY_FIXTURE, FAN_CROSS_FIXTURE, &connections],
    );
    let connected = connected_pairs(&view);

    let (vars, rows) = fan_duty_answer(&view);
    assert_eq!(rows.len(), 1, "single-source question answers: {rows:?}");
    let source = vars.iter().position(|v| v == "source").unwrap();
    assert_served_by_contract(&connected, &rows[0][source], &class("HealthAnomaly"));
    assert!(unconnected_sources(&connected, &["fan-manager"]).is_empty());

    let (vars, rows) = select(&view, FAN_CROSS_QUERY);
    assert_fan_cross_row(&vars, &rows);
    let col = |name: &str| vars.iter().position(|v| v == name).unwrap();
    assert_served_by_contract(
        &connected,
        &rows[0][col("fanSource")],
        &class("HealthTrend"),
    );
    assert_served_by_contract(
        &connected,
        &rows[0][col("alertSource")],
        &class("HealthAnomaly"),
    );
    assert_served_by_contract(&connected, &rows[0][col("hostSource")], &class("Host"));

    // lgtm not connected: the cross-source question abstains, naming it.
    let without_lgtm = connection_fixture(&[
        (
            "fan-manager",
            &[&class("HealthTrend"), &class("HealthAnomaly")],
        ),
        ("systems-manager", &[&class("Host")]),
    ]);
    let view = graph_with(
        &schema,
        &[FAN_DUTY_FIXTURE, FAN_CROSS_FIXTURE, &without_lgtm],
    );
    let connected = connected_pairs(&view);
    assert_eq!(
        unconnected_sources(&connected, &["fan-manager", "systems-manager", "lgtm"]),
        vec!["lgtm".to_string()]
    );
}

const CONTAINER_RESTART_FIXTURE: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:container-manager:fixture/> .

f:image-web a infra:ContainerImage ; dc:identifier "cm:image:web:1.25" .
f:image-api a infra:ContainerImage ; dc:identifier "cm:image:api:2.0" .
f:image-db a infra:ContainerImage ; dc:identifier "cm:image:db:16" .

# container-web: restarted nine times — repeatedly.
f:container-web a infra:Container ; :usesImage f:image-web .
f:anomaly-web a infra:HealthAnomaly ; infra:affectsEntity f:container-web ; infra:signal "restart_count" ;
    infra:observed "9"^^xsd:decimal ; infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "container-manager" ; dc:identifier "cm:rec:anomaly-web" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# deploy-api: a workload restarting five times.
f:deploy-api a infra:Workload ; :usesImage f:image-api .
f:anomaly-api a infra:HealthAnomaly ; infra:affectsEntity f:deploy-api ; infra:signal "restart_count" ;
    infra:observed "5"^^xsd:decimal ; infra:observedAt "2026-01-02T07:00:00Z"^^xsd:dateTime ;
    dc:source "container-manager" ; dc:identifier "cm:rec:anomaly-api" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# container-db: a single restart — not repeated.
f:container-db a infra:Container ; :usesImage f:image-db .
f:anomaly-db a infra:HealthAnomaly ; infra:affectsEntity f:container-db ; infra:signal "restart_count" ;
    infra:observed "1"^^xsd:decimal ; infra:observedAt "2026-01-02T08:00:00Z"^^xsd:dateTime ;
    dc:source "container-manager" ; dc:identifier "cm:rec:anomaly-db" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# container-idle: no anomaly; container-hot: a high reading of a different signal.
f:container-idle a infra:Container ; :usesImage f:image-db .
f:container-hot a infra:Container ; :usesImage f:image-web .
f:anomaly-hot a infra:HealthAnomaly ; infra:affectsEntity f:container-hot ; infra:signal "cpu_pct" ;
    infra:observed "99"^^xsd:decimal ; infra:observedAt "2026-01-02T08:00:00Z"^^xsd:dateTime ;
    dc:source "container-manager" ; dc:identifier "cm:rec:anomaly-hot" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;

// spec: EG-CQ-CONN-R013.1
#[test]
fn container_manager_repeated_restarts_answer() {
    // "Which containers or pods are restarting repeatedly, and from which
    // image?" — answered from container-manager alone (Vocabulary:
    // infrastructure-v1.ttl; `usesImage` is the shared supply-chain relation
    // from container to image). Repeated is three or more restarts.
    let view = graph(
        &[
            include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
            include_str!("../../eg-core/ontology/supply_chain-v1.ttl"),
        ],
        CONTAINER_RESTART_FIXTURE,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX infra: <http://knuckles.team/kg/infrastructure#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?entity ?restarts ?image ?imageRef ?source ?record ?asof WHERE {
  VALUES ?kind { infra:Container infra:Workload }
  ?entity a ?kind ; :usesImage ?image .
  ?image dc:identifier ?imageRef .
  ?anomaly a infra:HealthAnomaly ; infra:affectsEntity ?entity ; infra:signal "restart_count" ;
           infra:observed ?restarts ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(?restarts >= 3)
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let mut answered: Vec<(String, String, String)> = rows
        .iter()
        .map(|row| {
            (
                local(&row[idx("entity")]).to_string(),
                row[idx("restarts")].clone(),
                row[idx("imageRef")].clone(),
            )
        })
        .collect();
    answered.sort();
    assert_eq!(
        answered,
        vec![
            (
                "container-web".to_string(),
                "9".to_string(),
                "cm:image:web:1.25".to_string()
            ),
            (
                "deploy-api".to_string(),
                "5".to_string(),
                "cm:image:api:2.0".to_string()
            ),
        ],
        "{rows:?}"
    );
    for row in &rows {
        assert_eq!(row[idx("source")], "container-manager");
        assert_eq!(row[idx("asof")], "2026-01-03T00:00:00Z");
        assert!(row[idx("record")].starts_with("cm:rec:anomaly-"));
    }
}

const UPTIME_DOWN_FIXTURE: &str = r#"
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:uptime:fixture/> .

# Frozen as-of 2026-01-03T00:00:00Z: the last-24-hours window starts 2026-01-02.
f:svc-wiki a infra:Service .
f:svc-git a infra:Service .
f:svc-mail a infra:Service .
f:svc-old a infra:Service .
f:svc-idle a infra:Service .
f:svc-slow a infra:Service .

# svc-wiki: down for 30 minutes from 06:00.
f:down-wiki a infra:HealthAnomaly ; infra:affectsEntity f:svc-wiki ; infra:signal "downtime_s" ;
    infra:observed "1800"^^xsd:decimal ; infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:down-wiki" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
# svc-git: down for 10 minutes from 12:00.
f:down-git a infra:HealthAnomaly ; infra:affectsEntity f:svc-git ; infra:signal "downtime_s" ;
    infra:observed "600"^^xsd:decimal ; infra:observedAt "2026-01-02T12:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:down-git" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
# svc-mail: down for 20 minutes from 18:00.
f:down-mail a infra:HealthAnomaly ; infra:affectsEntity f:svc-mail ; infra:signal "downtime_s" ;
    infra:observed "1200"^^xsd:decimal ; infra:observedAt "2026-01-02T18:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:down-mail" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
# svc-old: down, but a week before the window.
f:down-old a infra:HealthAnomaly ; infra:affectsEntity f:svc-old ; infra:signal "downtime_s" ;
    infra:observed "3600"^^xsd:decimal ; infra:observedAt "2025-12-26T06:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:down-old" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
# svc-slow: slow, not down (a different signal). svc-idle: no anomaly.
f:slow-slow a infra:HealthAnomaly ; infra:affectsEntity f:svc-slow ; infra:signal "latency_ms" ;
    infra:observed "900"^^xsd:decimal ; infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:slow-slow" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;

// Monitor state vocabulary is not on main (the `:Alert` / `:entityState`
// producer rows), so a monitor's failed interval is an uptime-sourced
// `:HealthAnomaly` of signal `downtime_s` on the `:Service`: `observedAt` is
// when the monitor went down and `observed` the seconds it stayed down.
const UPTIME_DOWN_QUERY: &str = r#"
PREFIX infra: <http://knuckles.team/kg/infrastructure#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?service ?downSeconds ?source ?record ?asof WHERE {
  ?service a infra:Service .
  ?down a infra:HealthAnomaly ; infra:affectsEntity ?service ; infra:signal "downtime_s" ;
        infra:observed ?downSeconds ; infra:observedAt ?t ;
        dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(YEAR(?t) = 2026 && MONTH(?t) = 1 && DAY(?t) >= 2)
}"#;

// spec: EG-CQ-CONN-R064.1
#[test]
fn uptime_services_down_in_window_answer() {
    // "Which monitored services were down in the last 24 hours and for how
    // long?" — answered from uptime alone (Vocabulary: infrastructure-v1.ttl).
    let view = graph(
        &[include_str!("../../eg-core/ontology/infrastructure-v1.ttl")],
        UPTIME_DOWN_FIXTURE,
    );
    let (vars, rows) = select(&view, UPTIME_DOWN_QUERY);
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let mut answered: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                local(&row[idx("service")]).to_string(),
                row[idx("downSeconds")].clone(),
            )
        })
        .collect();
    answered.sort();
    assert_eq!(
        answered,
        vec![
            ("svc-git".to_string(), "600".to_string()),
            ("svc-mail".to_string(), "1200".to_string()),
            ("svc-wiki".to_string(), "1800".to_string()),
        ],
        "{rows:?}"
    );
    for row in &rows {
        assert_eq!(row[idx("source")], "uptime");
        assert_eq!(row[idx("asof")], "2026-01-03T00:00:00Z");
    }
}

const UPTIME_CROSS_FIXTURE: &str = r#"
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:uptime-cross:fixture/> .

# Uptime Kuma: three services down on 2026-01-02 (start, seconds down).
f:svc-wiki a infra:Service .
f:svc-git a infra:Service .
f:svc-mail a infra:Service .
f:svc-calm a infra:Service .
f:down-wiki a infra:HealthAnomaly ; infra:affectsEntity f:svc-wiki ; infra:signal "downtime_s" ;
    infra:observed "1800"^^xsd:decimal ; infra:observedAt "2026-01-02T06:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:down-wiki" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:down-git a infra:HealthAnomaly ; infra:affectsEntity f:svc-git ; infra:signal "downtime_s" ;
    infra:observed "600"^^xsd:decimal ; infra:observedAt "2026-01-02T12:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:down-git" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:down-mail a infra:HealthAnomaly ; infra:affectsEntity f:svc-mail ; infra:signal "downtime_s" ;
    infra:observed "1200"^^xsd:decimal ; infra:observedAt "2026-01-02T18:00:00Z"^^xsd:dateTime ;
    dc:source "uptime" ; dc:identifier "up:rec:down-mail" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# lgtm: a Grafana latency alert on svc-wiki inside its downtime (answers); one on
# svc-mail two hours after it recovered (no); one on svc-calm, which never went down (no).
f:alert-wiki a infra:HealthAnomaly ; infra:affectsEntity f:svc-wiki ; infra:signal "latency_ms" ;
    infra:observed "4000"^^xsd:decimal ; infra:observedAt "2026-01-02T06:10:00Z"^^xsd:dateTime ;
    dc:source "lgtm" ; dc:identifier "lgtm:rec:alert-wiki" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:alert-mail a infra:HealthAnomaly ; infra:affectsEntity f:svc-mail ; infra:signal "latency_ms" ;
    infra:observed "4000"^^xsd:decimal ; infra:observedAt "2026-01-02T20:30:00Z"^^xsd:dateTime ;
    dc:source "lgtm" ; dc:identifier "lgtm:rec:alert-mail" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:alert-calm a infra:HealthAnomaly ; infra:affectsEntity f:svc-calm ; infra:signal "latency_ms" ;
    infra:observed "4000"^^xsd:decimal ; infra:observedAt "2026-01-02T06:10:00Z"^^xsd:dateTime ;
    dc:source "lgtm" ; dc:identifier "lgtm:rec:alert-calm" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Caddy: a route to svc-git erroring during its downtime (answers) and a route to
# svc-mail erroring on 2026-01-01 (no).
f:route-git a infra:ProxyRoute ; infra:routesTo f:svc-git .
f:route-mail a infra:ProxyRoute ; infra:routesTo f:svc-mail .
f:route-error-git a infra:HealthAnomaly ; infra:affectsEntity f:route-git ; infra:signal "route_error_rate" ;
    infra:observed "0.4"^^xsd:decimal ; infra:observedAt "2026-01-02T12:05:00Z"^^xsd:dateTime ;
    dc:source "caddy" ; dc:identifier "caddy:rec:route-error-git" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:route-error-mail a infra:HealthAnomaly ; infra:affectsEntity f:route-mail ; infra:signal "route_error_rate" ;
    infra:observed "0.4"^^xsd:decimal ; infra:observedAt "2026-01-01T18:05:00Z"^^xsd:dateTime ;
    dc:source "caddy" ; dc:identifier "caddy:rec:route-error-mail" ; dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;

// The concurrent signal falls in `[down start, down start + downtime_s]` on
// one day, on the service itself (Grafana) or on a route to it (Caddy).
const UPTIME_CROSS_QUERY: &str = r#"
PREFIX infra: <http://knuckles.team/kg/infrastructure#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?service ?evidence ?source ?record ?downSource ?downRecord ?asof WHERE {
  ?service a infra:Service .
  ?down a infra:HealthAnomaly ; infra:affectsEntity ?service ; infra:signal "downtime_s" ;
        infra:observed ?downSeconds ; infra:observedAt ?ts ;
        dc:source ?downSource ; dc:identifier ?downRecord .
  {
    ?other a infra:HealthAnomaly ; infra:affectsEntity ?service ; infra:signal "latency_ms" .
    BIND("grafana-alert" AS ?evidence)
  } UNION {
    ?route a infra:ProxyRoute ; infra:routesTo ?service .
    ?other a infra:HealthAnomaly ; infra:affectsEntity ?route ; infra:signal "route_error_rate" .
    BIND("route-error" AS ?evidence)
  }
  ?other infra:observedAt ?at ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(YEAR(?ts) = YEAR(?at) && MONTH(?ts) = MONTH(?at) && DAY(?ts) = DAY(?at))
  FILTER((HOURS(?at) * 3600 + MINUTES(?at) * 60) >= (HOURS(?ts) * 3600 + MINUTES(?ts) * 60))
  FILTER((HOURS(?at) * 3600 + MINUTES(?at) * 60) <= (HOURS(?ts) * 3600 + MINUTES(?ts) * 60 + ?downSeconds))
}"#;

fn assert_uptime_cross_rows(vars: &[String], rows: &[Vec<String>]) {
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let mut answered: Vec<(String, String, String, String)> = rows
        .iter()
        .map(|row| {
            (
                local(&row[idx("service")]).to_string(),
                row[idx("evidence")].clone(),
                row[idx("source")].clone(),
                row[idx("record")].clone(),
            )
        })
        .collect();
    answered.sort();
    assert_eq!(
        answered,
        vec![
            (
                "svc-git".to_string(),
                "route-error".to_string(),
                "caddy".to_string(),
                "caddy:rec:route-error-git".to_string()
            ),
            (
                "svc-wiki".to_string(),
                "grafana-alert".to_string(),
                "lgtm".to_string(),
                "lgtm:rec:alert-wiki".to_string()
            ),
        ],
        "{rows:?}"
    );
    for row in rows {
        assert_eq!(row[idx("downSource")], "uptime");
        assert_eq!(row[idx("asof")], "2026-01-03T00:00:00Z");
    }
}

// spec: EG-CQ-CONN-R064.2
#[test]
fn uptime_down_with_grafana_alert_or_route_error_answers() {
    // "Which services Uptime Kuma saw down had a Grafana alert or a Caddy route
    // error at the same time?" — uptime, lgtm and caddy linked through the
    // shared `service` concept (Vocabulary: infrastructure-v1.ttl).
    let view = graph(
        &[include_str!("../../eg-core/ontology/infrastructure-v1.ttl")],
        UPTIME_CROSS_FIXTURE,
    );
    let (vars, rows) = select(&view, UPTIME_CROSS_QUERY);
    assert_uptime_cross_rows(&vars, &rows);
}

// spec: EG-CQ-CONN-R064.3
#[test]
fn uptime_acceptance_through_declared_contracts() {
    // Both questions run against records served through the connectors'
    // declared access contracts (approved `vg:VirtualMapping` per class), each
    // answer row's source resolves to a mapping onto the answering class, and a
    // question whose linked source is not connected names it.
    let schema = [
        include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
        include_str!("../../eg-core/ontology/virtual_graph-v1.ttl"),
    ];
    let class = |name: &str| format!("{INFRA}{name}");
    let connections = connection_fixture(&[
        ("uptime", &[&class("HealthAnomaly"), &class("Service")]),
        ("lgtm", &[&class("HealthAnomaly")]),
        ("caddy", &[&class("HealthAnomaly"), &class("ProxyRoute")]),
    ]);
    let view = graph_with(
        &schema,
        &[UPTIME_DOWN_FIXTURE, UPTIME_CROSS_FIXTURE, &connections],
    );
    let connected = connected_pairs(&view);

    // The two fixtures share services by IRI namespace only: each question is
    // answered over its own records, so the combined graph keeps both answers.
    let (vars, rows) = select(&view, UPTIME_DOWN_QUERY);
    assert_eq!(
        rows.len(),
        6,
        "single-source answers both fixtures' downtimes: {rows:?}"
    );
    let source = vars.iter().position(|v| v == "source").unwrap();
    for row in &rows {
        assert_served_by_contract(&connected, &row[source], &class("HealthAnomaly"));
    }
    assert!(unconnected_sources(&connected, &["uptime"]).is_empty());

    let (vars, rows) = select(&view, UPTIME_CROSS_QUERY);
    assert_uptime_cross_rows(&vars, &rows);
    let source = vars.iter().position(|v| v == "source").unwrap();
    for row in &rows {
        assert_served_by_contract(&connected, &row[source], &class("HealthAnomaly"));
    }

    // caddy not connected: the cross-source question abstains, naming it.
    let without_caddy = connection_fixture(&[
        ("uptime", &[&class("HealthAnomaly"), &class("Service")]),
        ("lgtm", &[&class("HealthAnomaly")]),
    ]);
    let view = graph_with(
        &schema,
        &[UPTIME_DOWN_FIXTURE, UPTIME_CROSS_FIXTURE, &without_caddy],
    );
    assert_eq!(
        unconnected_sources(&connected_pairs(&view), &["uptime", "lgtm", "caddy"]),
        vec!["caddy".to_string()]
    );
}

// spec: EG-CQ-CONN-R042.1
#[test]
fn okta_assignment_through_group_answers() {
    // "Which users hold a given application assignment through which group?" —
    // answered from the okta connector alone (Vocabulary: identity-v1.ttl:
    // IdentityUser, IdentityGroup, memberOfGroup, assignedApp). Frozen as-of:
    // 2026-01-03T00:00:00Z.
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:okta:fixture/> .

# The given application and the two groups holding assignments.
f:app-portal rdfs:label "Portal" .
f:app-mail rdfs:label "Mail" .
f:group-portal a :IdentityGroup ;
    :assignedApp f:app-portal .
f:group-mail a :IdentityGroup ;
    :assignedApp f:app-mail .

# alice: okta user in group-portal — holds the portal assignment, the row.
f:alice a :IdentityUser ;
    :memberOfGroup f:group-portal ;
    dc:source "okta" ;
    dc:identifier "okta:rec:alice" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# bob: okta user in group-mail — the assignment is for a different app, no row.
f:bob a :IdentityUser ;
    :memberOfGroup f:group-mail ;
    dc:source "okta" ;
    dc:identifier "okta:rec:bob" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# carol: okta user with no group membership — no row.
f:carol a :IdentityUser ;
    dc:source "okta" ;
    dc:identifier "okta:rec:carol" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;
    let view = graph(
        &[include_str!("../../eg-core/ontology/identity-v1.ttl")],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?user ?group ?app ?source ?record ?asof WHERE {
  ?user a :IdentityUser ; :memberOfGroup ?group .
  ?group a :IdentityGroup ; :assignedApp ?app .
  FILTER(?app = <urn:eg-cq-conn:okta:fixture/app-portal>)
  ?user dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_user = idx("user");
    let i_group = idx("group");
    let i_app = idx("app");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(rows.len(), 1, "exactly one user answers: {rows:?}");
    let row = &rows[0];
    assert_eq!(local(&row[i_user]), "alice");
    assert_eq!(local(&row[i_group]), "group-portal");
    assert_eq!(
        row[i_app], "<urn:eg-cq-conn:okta:fixture/app-portal>",
        "the assignment is for the given application: {row:?}"
    );
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[i_source], "okta");
    assert_eq!(row[i_record], "okta:rec:alice");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}

// spec: EG-CQ-CONN-R039.2
#[test]
fn microsoft_attendees_without_idp_or_mattermost_answer() {
    // "Which attendees of an upcoming meeting have no identity-provider account
    // or Mattermost presence?" — federated fixture over the microsoft calendar
    // (Vocabulary: calendar-v1.ttl; Person from the imported core module, the
    // link concept) with the okta and mattermost sides carried on the shared
    // person entity: an okta account is an IdentityUser (an identity-v1 Person
    // subclass) and a Mattermost presence is a Person record sourced from
    // mattermost. Both sides are provenance-carrying records, so each answer
    // row reports the missing half. Frozen as-of: 2026-01-05T00:00:00Z, so
    // "upcoming" is 2026-01-05 or later (day-granular window: the ordering
    // comparator parses operands as f64, so the window uses DAY() accessors).
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:microsoft:fixture/> .

# alice: okta identity-provider account — attends the standup, has an idp
# account, no row.
f:alice a :Person, :IdentityUser ;
    dc:source "okta" ;
    dc:identifier "okta:rec:alice" ;
    dc:date "2026-01-05T00:00:00Z"^^xsd:dateTime .

# bob: Mattermost presence — attends the planning meeting, no row.
f:bob a :Person ;
    dc:source "mattermost" ;
    dc:identifier "mm:rec:bob" ;
    dc:date "2026-01-05T00:00:00Z"^^xsd:dateTime .

# carol and dave: plain persons with no idp account and no Mattermost
# presence — the answer rows.
f:carol a :Person .
f:dave a :Person .

# erin: plain person, but attends a meeting that already happened — outside
# the "upcoming" window.
f:erin a :Person .

# Upcoming meetings, fetched from the microsoft connector at query time.
f:meeting-standup a :CalendarEvent ;
    :scheduledStart "2026-01-06T09:00:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-06T10:00:00Z"^^xsd:dateTime ;
    :attendedBy f:alice, f:carol ;
    dc:source "microsoft" ;
    dc:identifier "ms:rec:meeting-standup" ;
    dc:date "2026-01-05T00:00:00Z"^^xsd:dateTime .
f:meeting-planning a :CalendarEvent ;
    :scheduledStart "2026-01-07T14:00:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-07T15:00:00Z"^^xsd:dateTime ;
    :attendedBy f:bob, f:dave ;
    dc:source "microsoft" ;
    dc:identifier "ms:rec:meeting-planning" ;
    dc:date "2026-01-05T00:00:00Z"^^xsd:dateTime .

# Yesterday's sync — already past the frozen as-of, no row.
f:meeting-sync a :CalendarEvent ;
    :scheduledStart "2026-01-04T16:00:00Z"^^xsd:dateTime ;
    :scheduledEnd "2026-01-04T17:00:00Z"^^xsd:dateTime ;
    :attendedBy f:erin ;
    dc:source "microsoft" ;
    dc:identifier "ms:rec:meeting-sync" ;
    dc:date "2026-01-05T00:00:00Z"^^xsd:dateTime .
"#;
    let view = graph(
        &[
            include_str!("../../eg-core/ontology/calendar-v1.ttl"),
            include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
            include_str!("../../eg-core/ontology/identity-v1.ttl"),
        ],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?attendee ?meeting ?source ?record ?asof WHERE {
  ?meeting a :CalendarEvent ;
           :scheduledStart ?s ;
           :attendedBy ?attendee ;
           dc:source ?source ;
           dc:identifier ?record ;
           dc:date ?asof .
  FILTER(?source = "microsoft")
  FILTER(YEAR(?s) = 2026 && MONTH(?s) = 1 && DAY(?s) >= 5)
  # The absence halves of the question: no identity-provider account and no
  # Mattermost presence on the shared person entity.
  FILTER NOT EXISTS { ?attendee a :IdentityUser }
  FILTER NOT EXISTS { ?attendee dc:source "mattermost" }
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_attendee = idx("attendee");
    let i_meeting = idx("meeting");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(rows.len(), 2, "exactly two attendees answer: {rows:?}");
    let pairs: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                local(&row[i_attendee]).to_string(),
                local(&row[i_meeting]).to_string(),
            )
        })
        .collect();
    let mut sorted = pairs.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![
            ("carol".to_string(), "meeting-standup".to_string()),
            ("dave".to_string(), "meeting-planning".to_string()),
        ],
        "only the unconnected attendees answer: {rows:?}"
    );
    // Provenance: every answer row carries the microsoft meeting record's
    // source, record id and as-of from the virtual mapping.
    for row in &rows {
        assert_eq!(row[i_source], "microsoft", "{rows:?}");
        assert_eq!(row[i_asof], "2026-01-05T00:00:00Z", "{rows:?}");
        let record = row[i_record].as_str();
        assert!(
            record == "ms:rec:meeting-standup" || record == "ms:rec:meeting-planning",
            "unexpected record id {record:?}: {rows:?}"
        );
    }
}

// Atlassian R006.1 criterion body; keep unbound until the native Rust test runs.
#[test]
fn atlassian_blocked_unassigned_issues_answer() {
    // "Which Jira issues in a project are blocked and unassigned, and by which
    // linked issue?" — answered from the Atlassian connector alone. The
    // project's membership edge reuses core :partOf; the lifecycle predicates
    // are the canonical :blockedBy/:assignedTo/:ticketStatus terms. Frozen
    // as-of: 2026-01-03T00:00:00Z.
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix f: <urn:eg-cq-conn:atlassian:fixture/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

f:project-alpha a :SoftwareProject .
f:project-beta a :SoftwareProject .

# The only answer: blocked, open, unassigned, and in the requested project.
f:issue-eng-101 a :JiraIssue, :Ticket ;
    :partOf f:project-alpha ;
    :blockedBy f:issue-eng-099 ;
    :ticketStatus "open" ;
    dc:source "atlassian" ;
    dc:identifier "jira:ENG-101" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:issue-eng-099 a :JiraIssue, :Ticket ;
    :partOf f:project-alpha ;
    :ticketStatus "open" .

# Same project and blocked, but assigned — must be excluded.
f:issue-eng-102 a :JiraIssue, :Ticket ;
    :partOf f:project-alpha ;
    :blockedBy f:issue-eng-099 ;
    :assignedTo f:person-alice ;
    :ticketStatus "open" ;
    dc:source "atlassian" ;
    dc:identifier "jira:ENG-102" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Same project and unassigned, but not blocked — must be excluded.
f:issue-eng-103 a :JiraIssue, :Ticket ;
    :partOf f:project-alpha ;
    :ticketStatus "open" ;
    dc:source "atlassian" ;
    dc:identifier "jira:ENG-103" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Blocked and unassigned, but in another project — must be excluded.
f:issue-eng-201 a :JiraIssue, :Ticket ;
    :partOf f:project-beta ;
    :blockedBy f:issue-eng-199 ;
    :ticketStatus "open" ;
    dc:source "atlassian" ;
    dc:identifier "jira:OPS-201" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:issue-eng-199 a :JiraIssue, :Ticket ;
    :partOf f:project-beta ;
    :ticketStatus "open" .

# The requested project and relationship are present, but the issue is closed.
f:issue-eng-104 a :JiraIssue, :Ticket ;
    :partOf f:project-alpha ;
    :blockedBy f:issue-eng-099 ;
    :ticketStatus "closed" ;
    dc:source "atlassian" ;
    dc:identifier "jira:ENG-104" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;
    let view = graph(
        &[
            include_str!("../../eg-core/ontology/sdlc_lifecycle-v1.ttl"),
            include_str!("../../eg-core/ontology/software-v1.ttl"),
            include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        ],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX f: <urn:eg-cq-conn:atlassian:fixture/>
SELECT ?issue ?project ?blockedBy ?source ?record ?asof WHERE {
  ?issue a :JiraIssue, :Ticket ;
         :partOf ?project ;
         :blockedBy ?blockedBy ;
         :ticketStatus "open" ;
         dc:source ?source ;
         dc:identifier ?record ;
         dc:date ?asof .
  FILTER(?project = f:project-alpha)
  OPTIONAL { ?issue :assignedTo ?assignee }
  FILTER(!BOUND(?assignee))
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_issue = idx("issue");
    let i_project = idx("project");
    let i_blocked_by = idx("blockedBy");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(rows.len(), 1, "exactly one Jira issue answers: {rows:?}");
    let row = &rows[0];
    assert_eq!(local(&row[i_issue]), "issue-eng-101");
    assert_eq!(local(&row[i_project]), "project-alpha");
    assert_eq!(local(&row[i_blocked_by]), "issue-eng-099");
    // Provenance: source, record id, and as-of all carry on the answer row.
    assert_eq!(row[i_source], "atlassian");
    assert_eq!(row[i_record], "jira:ENG-101");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}
