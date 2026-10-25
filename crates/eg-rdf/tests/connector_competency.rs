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

/// The local name of a bracketed node-id cell (`<urn:…/host-1>` → `host-1`).
fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

// spec: EG-CQ-CONN-R020.1
#[test]
fn fan_manager_duty_above_threshold_answers() {
    // "Which fans ran above 80 percent duty while the host temperature stayed
    // above its threshold?" — answered from the fan-manager connector alone
    // (Vocabulary: infrastructure-v1.ttl). Frozen as-of: 2026-01-03T00:00:00Z.
    let fixture = r#"
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
    let view = graph(
        &[include_str!("../../eg-core/ontology/infrastructure-v1.ttl")],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
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
}"#,
    );
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
