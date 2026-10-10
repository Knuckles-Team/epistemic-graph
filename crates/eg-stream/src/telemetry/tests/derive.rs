//! Declared CEP patterns over rollups yield HealthAnomaly and Incident facts (EH-409).

use super::super::{
    correlate_incidents, derive_facts, detect_anomalies, fact_graph, rollup, BehaviourObservation,
    Declarations, EntityClass, ResolutionPolicy, TelemetryPolicy,
};
use super::{
    cross_entity_incident_rule, directory, entity, error_burst_rule, estate, rollup_policy,
    traffic, WINDOW_MS,
};

/// Ten windows of ten spans each; the windows listed in `hot` are 80% errors.
fn window_errors(hot: &[usize]) -> Vec<usize> {
    (0..10)
        .map(|w| if hot.contains(&w) { 8 } else { 0 })
        .collect()
}

fn observations(service: &str, hot: &[usize]) -> Vec<BehaviourObservation> {
    rollup(
        &rollup_policy(),
        &directory(),
        &traffic(service, &window_errors(hot), 10),
    )
    .observations
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn the_pattern_fires_on_a_synthetic_error_burst() {
    let observations = observations("checkout", &[4, 5, 6]);
    let anomalies = detect_anomalies(&[error_burst_rule()], &observations);
    let [anomaly] = anomalies.as_slice() else {
        panic!("expected exactly one anomaly, got {anomalies:?}");
    };
    assert_eq!(anomaly.entity, entity(EntityClass::Service, "svc:checkout"));
    assert_eq!(anomaly.kind, "error_burst");
    assert_eq!(
        (anomaly.start_ms, anomaly.end_ms),
        (4 * WINDOW_MS, 7 * WINDOW_MS)
    );
    assert_eq!(
        anomaly.evidence,
        [40_000, 50_000, 60_000].map(|start| format!("behaviour:Service:svc:checkout:{start}"))
    );
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn a_quiet_service_and_a_short_blip_do_not_fire() {
    assert!(detect_anomalies(&[error_burst_rule()], &observations("checkout", &[])).is_empty());
    assert!(detect_anomalies(&[error_burst_rule()], &observations("checkout", &[3, 4])).is_empty());
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn a_long_burst_coalesces_into_one_anomaly() {
    let anomalies = detect_anomalies(
        &[error_burst_rule()],
        &observations("checkout", &[2, 3, 4, 5, 6, 7]),
    );
    let [anomaly] = anomalies.as_slice() else {
        panic!("expected one coalesced anomaly, got {anomalies:?}");
    };
    assert_eq!(
        (anomaly.start_ms, anomaly.end_ms),
        (2 * WINDOW_MS, 8 * WINDOW_MS)
    );
    assert_eq!(anomaly.evidence.len(), 6);
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn a_pattern_never_stitches_two_entities_windows_together() {
    // Each service has only two bad windows; together they have four.
    let mut both = observations("checkout", &[4, 5]);
    both.extend(observations("payments", &[5, 6]));
    assert!(detect_anomalies(&[error_burst_rule()], &both).is_empty());
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn simultaneous_anomalies_on_two_entities_correlate_into_an_incident() {
    let mut both = observations("checkout", &[4, 5, 6]);
    both.extend(observations("payments", &[5, 6, 7]));
    let anomalies = detect_anomalies(&[error_burst_rule()], &both);
    assert_eq!(anomalies.len(), 2);
    let incidents = correlate_incidents(&[cross_entity_incident_rule(3 * WINDOW_MS)], &anomalies);
    let [incident] = incidents.as_slice() else {
        panic!("expected one incident, got {incidents:?}");
    };
    assert_eq!(
        incident.entities,
        [
            entity(EntityClass::Service, "svc:checkout"),
            entity(EntityClass::Service, "svc:payments"),
        ]
    );
    let mut expected: Vec<String> = anomalies.iter().map(|a| a.id.clone()).collect();
    expected.sort();
    assert_eq!(incident.anomalies, expected);
    assert_eq!(
        (incident.start_ms, incident.end_ms),
        (4 * WINDOW_MS, 8 * WINDOW_MS)
    );
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn one_entity_alone_is_not_an_incident() {
    // Two separate bursts on the SAME entity, both inside one wide incident window.
    let anomalies = detect_anomalies(
        &[error_burst_rule()],
        &observations("checkout", &[0, 1, 2, 6, 7, 8]),
    );
    assert_eq!(anomalies.len(), 2);
    let wide = cross_entity_incident_rule(100 * WINDOW_MS);
    assert!(correlate_incidents(&[wide], &anomalies).is_empty());
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn a_hot_window_after_a_gap_is_not_consecutive() {
    // Windows 1, 2 and 4 are hot: three hot windows, but not three in a row.
    assert!(
        detect_anomalies(&[error_burst_rule()], &observations("checkout", &[1, 2, 4])).is_empty()
    );
}

fn policy() -> TelemetryPolicy {
    TelemetryPolicy {
        resolution: ResolutionPolicy::standard(),
        rollup: rollup_policy(),
        anomalies: vec![error_burst_rule()],
        incidents: vec![cross_entity_incident_rule(3 * WINDOW_MS)],
    }
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn derived_facts_project_onto_typed_linked_graph_nodes_deterministically() {
    let errors = window_errors(&[4, 5, 6]);
    let mut signals = traffic("checkout", &errors, 10);
    signals.extend(traffic("payments", &errors, 10));
    let declarations = Declarations {
        entities: estate(),
        health: Vec::new(),

        ..Declarations::default()
    };
    let facts = derive_facts(&policy(), &declarations, &signals);
    assert_eq!(
        (
            facts.observations.len(),
            facts.anomalies.len(),
            facts.incidents.len()
        ),
        (20, 2, 1)
    );

    let graph = fact_graph(&facts).unwrap();
    signals.reverse();
    let again = fact_graph(&derive_facts(&policy(), &declarations, &signals)).unwrap();
    assert_eq!(graph, again);

    let incident = graph
        .nodes
        .iter()
        .find(|n| n.properties["type"] == "Incident")
        .unwrap();
    assert_eq!(
        incident.class_iri,
        "http://knuckles.team/kg/infrastructure#Incident"
    );
    assert_eq!(incident.properties["evidence_class"], "derived");
    let observation = &graph.nodes[0];
    assert_eq!(observation.properties["type"], "BehaviourObservation");
    assert_eq!(observation.properties["evidence_class"], "observation");
    assert!(graph.edges.iter().any(|e| e.source == observation.id
        && e.target == "svc:checkout"
        && e.relationship == "OBSERVES"));
    let derived_from_anomalies = graph
        .edges
        .iter()
        .filter(|e| e.source == incident.id && e.relationship == "DERIVED_FROM")
        .count();
    assert_eq!(derived_from_anomalies, 2);
}
