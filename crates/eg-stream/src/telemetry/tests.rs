//! Shared fixtures for the telemetry tests. Every test builds its world from
//! these helpers, so the fixtures exist once.

mod binding;
mod conformance;
mod derive;
mod rollup;

use std::collections::BTreeMap;

use super::{
    AnomalyRule, DeclaredEntity, EntityClass, EntityDirectory, EntityRef, IncidentRule,
    ResolutionPolicy, RollupPolicy, TelemetrySignal,
};
use crate::{AttrPredicate, CepPattern, EventMatcher, Window};

/// Rollup window used throughout: ten seconds.
pub(super) const WINDOW_MS: u64 = 10_000;

pub(super) fn attributes(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

pub(super) fn entity(class: EntityClass, id: &str) -> EntityRef {
    EntityRef {
        class,
        id: id.to_string(),
    }
}

pub(super) fn declared(class: EntityClass, id: &str, keys: &[(&str, &str)]) -> DeclaredEntity {
    DeclaredEntity {
        entity: entity(class, id),
        keys: attributes(keys),
    }
}

/// The declared estate: two services, a deployment, a host, an agent and a
/// Prometheus-scraped service.
pub(super) fn estate() -> Vec<DeclaredEntity> {
    vec![
        declared(
            EntityClass::Service,
            "svc:checkout",
            &[("service.name", "checkout")],
        ),
        declared(
            EntityClass::Service,
            "svc:payments",
            &[("service.name", "payments")],
        ),
        declared(
            EntityClass::Deployment,
            "deploy:shop/cart",
            &[
                ("k8s.namespace.name", "shop"),
                ("k8s.deployment.name", "cart"),
            ],
        ),
        declared(EntityClass::Host, "host:r820", &[("host.name", "r820")]),
        declared(
            EntityClass::Agent,
            "agent:planner",
            &[("gen_ai.agent.id", "planner")],
        ),
        declared(
            EntityClass::Service,
            "svc:node-exporter",
            &[("job", "node")],
        ),
    ]
}

pub(super) fn directory() -> EntityDirectory {
    EntityDirectory::build(&ResolutionPolicy::standard(), &estate())
}

pub(super) fn rollup_policy() -> RollupPolicy {
    RollupPolicy {
        window_ms: WINDOW_MS,
        metric_roles: BTreeMap::new(),
    }
}

/// A span from `service` at `ts_ms` lasting `duration_ms`, failed when `status` is `ERROR`.
pub(super) fn span(service: &str, ts_ms: u64, duration_ms: i64, status: &str) -> TelemetrySignal {
    let start_ns = i64::try_from(ts_ms).unwrap() * 1_000_000;
    TelemetrySignal::span(
        format!("trace-{service}-{ts_ms}"),
        (start_ns, duration_ms * 1_000_000),
        status,
        service,
        BTreeMap::new(),
    )
}

/// `per_window` spans from `service` in each of `windows` consecutive windows,
/// `errors[w]` of which fail in window `w`.
pub(super) fn traffic(service: &str, errors: &[usize], per_window: usize) -> Vec<TelemetrySignal> {
    let mut signals = Vec::new();
    for (window, failing) in errors.iter().enumerate() {
        for request in 0..per_window {
            let ts_ms = window as u64 * WINDOW_MS + request as u64 * 10;
            let status = if request < *failing { "ERROR" } else { "OK" };
            signals.push(span(service, ts_ms, 20, status));
        }
    }
    signals
}

/// "Three consecutive windows each over 50% errors": three hot windows whose
/// starts lie within two window widths of each other.
pub(super) fn error_burst_rule() -> AnomalyRule {
    let hot = EventMatcher::key(super::derive::OBSERVATION_EVENT).with_pred(AttrPredicate::Gt {
        field: "error_ratio".into(),
        value: 0.5,
    });
    AnomalyRule {
        id: "error-burst".into(),
        kind: "error_burst".into(),
        pattern: CepPattern::Sequence(vec![hot.clone(), hot.clone(), hot]),
        window: Window::Sliding {
            size: 2 * WINDOW_MS,
        },
    }
}

/// "Two anomalies starting within `window_ms`" — an incident only across two entities.
pub(super) fn cross_entity_incident_rule(window_ms: u64) -> IncidentRule {
    let anomaly = EventMatcher::key(super::derive::ANOMALY_EVENT);
    IncidentRule {
        id: "correlated-anomalies".into(),
        pattern: CepPattern::Sequence(vec![anomaly.clone(), anomaly]),
        window: Window::Sliding { size: window_ms },
        min_entities: 2,
    }
}
