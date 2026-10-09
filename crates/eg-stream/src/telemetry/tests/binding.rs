//! Resolution is deterministic and never guesses (EH-408).

use std::collections::BTreeMap;

use super::super::{
    EntityClass, EntityDirectory, Resolution, ResolutionPolicy, TelemetrySignal, Unresolved,
};
use super::{attributes, declared, directory, entity, estate, span};

fn log_with(pairs: &[(&str, &str)]) -> TelemetrySignal {
    TelemetrySignal::log(1_000, "default", "INFO", attributes(pairs))
}

fn bound(entity_ref: super::super::EntityRef, rule: &str) -> Resolution {
    Resolution::Bound {
        entity: entity_ref,
        rule: rule.into(),
    }
}

// spec: EG-DECISION-ENGINE-R125, EG-UNIFIED-DATA-PLANE-R036
#[test]
fn every_entity_class_resolves_through_its_declared_key() {
    let directory = directory();
    let cases = [
        (
            log_with(&[("gen_ai.agent.id", "planner")]),
            entity(EntityClass::Agent, "agent:planner"),
            "otel.agent",
        ),
        (
            log_with(&[
                ("k8s.namespace.name", "shop"),
                ("k8s.deployment.name", "cart"),
            ]),
            entity(EntityClass::Deployment, "deploy:shop/cart"),
            "k8s.deployment",
        ),
        (
            span("checkout", 5, 1, "OK"),
            entity(EntityClass::Service, "svc:checkout"),
            "otel.service",
        ),
        (
            log_with(&[("host.name", "r820")]),
            entity(EntityClass::Host, "host:r820"),
            "otel.host",
        ),
        (
            TelemetrySignal::metric(
                1,
                "up{job=\"node\"}",
                attributes(&[("__name__", "up"), ("job", "node")]),
                1.0,
            ),
            entity(EntityClass::Service, "svc:node-exporter"),
            "prometheus.job",
        ),
        (
            log_with(&[
                ("k8s.namespace.name", "shop"),
                ("k8s.deployment.name", "cart"),
                ("k8s.pod.name", "cart-1"),
            ]),
            entity(EntityClass::Pod, "pod:shop/cart-1"),
            "k8s.pod",
        ),
    ];
    for (signal, expected, rule) in cases {
        assert_eq!(directory.resolve(&signal), bound(expected, rule));
    }
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn a_server_resolves_by_its_address() {
    let policy = ResolutionPolicy::standard();
    let entities = [declared(
        EntityClass::Server,
        "srv:gb10",
        &[("server.address", "server.example.test")],
    )];
    let directory = EntityDirectory::build(&policy, &entities);
    assert_eq!(
        directory.resolve(&log_with(&[("server.address", "server.example.test")])),
        bound(entity(EntityClass::Server, "srv:gb10"), "otel.server")
    );
}

// spec: EG-UNIFIED-DATA-PLANE-R036
#[test]
fn resolution_does_not_depend_on_declaration_order() {
    let policy = ResolutionPolicy::standard();
    let mut reversed = estate();
    reversed.reverse();
    let forward = EntityDirectory::build(&policy, &estate());
    let backward = EntityDirectory::build(&policy, &reversed);
    assert_eq!(forward, backward);
    let signal = log_with(&[("service.name", "payments"), ("host.name", "r820")]);
    assert_eq!(forward.resolve(&signal), backward.resolve(&signal));
    assert_eq!(forward.resolve(&signal), forward.resolve(&signal));
}

#[test]
fn an_unknown_entity_is_unresolved_not_guessed_from_a_looser_key() {
    // The host IS known, but the deciding (service) rule names an unknown
    // service: the signal must not fall through to the host.
    let signal = log_with(&[("service.name", "ghost"), ("host.name", "r820")]);
    assert_eq!(
        directory().resolve(&signal),
        Resolution::Unresolved(Unresolved::UnknownEntity {
            rule: "otel.service".into(),
            key: vec!["ghost".into()],
        })
    );
}

#[test]
fn a_key_declared_by_two_individuals_is_ambiguous() {
    let mut entities = estate();
    entities.push(declared(
        EntityClass::Service,
        "svc:checkout-legacy",
        &[("service.name", "checkout")],
    ));
    let directory = EntityDirectory::build(&ResolutionPolicy::standard(), &entities);
    assert_eq!(
        directory.resolve(&span("checkout", 5, 1, "OK")),
        Resolution::Unresolved(Unresolved::Ambiguous {
            rule: "otel.service".into(),
            key: vec!["checkout".into()],
            candidates: vec![
                entity(EntityClass::Service, "svc:checkout"),
                entity(EntityClass::Service, "svc:checkout-legacy"),
            ],
        })
    );
}

#[test]
fn a_signal_without_any_resolution_key_is_unresolved() {
    assert_eq!(
        directory().resolve(&log_with(&[("region", "home")])),
        Resolution::Unresolved(Unresolved::NoResolutionKey)
    );
    // A partial deployment key is not a deployment key.
    assert_eq!(
        directory().resolve(&log_with(&[("k8s.deployment.name", "cart")])),
        Resolution::Unresolved(Unresolved::NoResolutionKey)
    );
}

#[test]
fn an_individual_is_indexed_only_under_rules_of_its_own_class() {
    // A host that (wrongly) declares a service.name is not a service.
    let entities = [declared(
        EntityClass::Host,
        "host:x",
        &[("service.name", "checkout")],
    )];
    let directory = EntityDirectory::build(&ResolutionPolicy::standard(), &entities);
    assert!(matches!(
        directory.resolve(&span("checkout", 5, 1, "OK")),
        Resolution::Unresolved(Unresolved::UnknownEntity { .. })
    ));
}

#[test]
fn a_span_keeps_an_explicit_service_name_attribute() {
    let signal = TelemetrySignal::span(
        "t",
        (0, 0),
        "OK",
        "emitter",
        BTreeMap::from([("service.name".to_string(), "payments".to_string())]),
    );
    assert_eq!(signal.attributes["service.name"], "payments");
}
