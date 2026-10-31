//! Declared-versus-observed conformance detects a mismatch (EH-409).

use super::super::{
    check_conformance, derive_facts, fact_graph, rollup, BehaviourObservation, Declarations,
    DeclaredHealth, DeclaredState, EntityClass, ResolutionPolicy, TelemetryPolicy, ViolationKind,
};
use super::{directory, entity, estate, rollup_policy, span, traffic};

fn healthy(max_error_ratio: f64, max_latency_p95_ms: Option<f64>) -> DeclaredHealth {
    DeclaredHealth::Healthy {
        max_error_ratio,
        max_latency_p95_ms,
    }
}

fn declare(id: &str, health: DeclaredHealth) -> DeclaredState {
    DeclaredState {
        entity: entity(EntityClass::Service, id),
        health,
        declared_by: format!("manifest:{id}"),
    }
}

/// One window of `service` traffic: ten 20 ms spans, `failing` of them errors.
fn one_window(service: &str, failing: usize) -> Vec<BehaviourObservation> {
    rollup(
        &rollup_policy(),
        &directory(),
        &traffic(service, &[failing], 10),
    )
    .observations
}

#[test]
fn a_service_declared_healthy_but_erroring_is_a_violation() {
    let observations = one_window("checkout", 6);
    let declared = [declare("svc:checkout", healthy(0.05, None))];
    let violations = check_conformance(&declared, &observations);
    let [violation] = violations.as_slice() else {
        panic!("expected one violation, got {violations:?}");
    };
    assert_eq!(violation.kind, ViolationKind::ErroringWhileDeclaredHealthy);
    assert_eq!(
        violation.entity,
        entity(EntityClass::Service, "svc:checkout")
    );
    assert_eq!(violation.observation, observations[0].id);
    assert_eq!(violation.declared_by, "manifest:svc:checkout");
    assert_eq!(violation.observed_error_ratio, 0.6);
}

#[test]
fn a_conforming_service_yields_nothing() {
    let declared = [declare("svc:checkout", healthy(0.05, Some(100.0)))];
    assert!(check_conformance(&declared, &one_window("checkout", 0)).is_empty());
    // Silence is not a violation either.
    assert!(check_conformance(&declared, &[]).is_empty());
}

#[test]
fn latency_over_the_declared_ceiling_is_a_violation() {
    let observations = rollup(
        &rollup_policy(),
        &directory(),
        &[span("payments", 1, 900, "OK")],
    )
    .observations;
    let declared = [declare("svc:payments", healthy(0.05, Some(250.0)))];
    let kinds: Vec<_> = check_conformance(&declared, &observations)
        .iter()
        .map(|v| v.kind)
        .collect();
    assert_eq!(kinds, [ViolationKind::SlowWhileDeclaredHealthy]);
}

#[test]
fn a_retired_service_still_serving_is_a_violation() {
    let declared = [declare("svc:payments", DeclaredHealth::Retired)];
    let kinds: Vec<_> = check_conformance(&declared, &one_window("payments", 0))
        .iter()
        .map(|v| v.kind)
        .collect();
    assert_eq!(kinds, [ViolationKind::ActiveWhileDeclaredRetired]);
}

#[test]
fn a_declaration_about_another_entity_does_not_apply() {
    let declared = [declare("svc:payments", healthy(0.0, None))];
    assert!(check_conformance(&declared, &one_window("checkout", 10)).is_empty());
}

#[test]
fn violations_are_graph_facts_linked_to_the_observation() {
    let policy = TelemetryPolicy {
        resolution: ResolutionPolicy::standard(),
        rollup: rollup_policy(),
        anomalies: Vec::new(),
        incidents: Vec::new(),
    };
    let declarations = Declarations {
        entities: estate(),
        health: vec![declare("svc:checkout", healthy(0.05, None))],

        ..Declarations::default()
    };
    let facts = derive_facts(&policy, &declarations, &traffic("checkout", &[9], 10));
    let graph = fact_graph(&facts).unwrap();
    let violation = graph
        .nodes
        .iter()
        .find(|node| node.properties["type"] == "ConformanceViolation")
        .unwrap();
    assert_eq!(
        violation.properties["kind"],
        "erroring_while_declared_healthy"
    );
    assert!(graph.edges.iter().any(|edge| edge.source == violation.id
        && edge.target == facts.observations[0].id
        && edge.relationship == "DERIVED_FROM"));
}
