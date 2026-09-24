//! Bound telemetry rolls up into BehaviourObservation facts with provenance (EH-408).

use std::collections::BTreeMap;

use super::super::{
    rollup, EntityClass, MetricRole, RollupPolicy, SignalKind, TelemetrySignal, Unresolved,
};
use super::{attributes, directory, entity, rollup_policy, span, traffic, WINDOW_MS};

fn metric(ts_ms: u64, name: &str, value: f64) -> TelemetrySignal {
    let labels = attributes(&[("__name__", name), ("job", "node")]);
    TelemetrySignal::metric(ts_ms, format!("{name}{{job=\"node\"}}"), labels, value)
}

#[test]
fn spans_and_logs_roll_up_per_entity_and_window() {
    let mut signals = traffic("checkout", &[1, 4], 4);
    signals.push(TelemetrySignal::log(
        12_000,
        "app",
        "error",
        attributes(&[("service.name", "checkout")]),
    ));
    let report = rollup(&rollup_policy(), &directory(), &signals);

    assert!(report.unresolved.is_empty());
    let [first, second] = report.observations.as_slice() else {
        panic!("expected two windows, got {:?}", report.observations);
    };
    assert_eq!(first.entity, entity(EntityClass::Service, "svc:checkout"));
    assert_eq!((first.window_start_ms, first.window_end_ms), (0, WINDOW_MS));
    assert_eq!(
        (first.requests, first.errors, first.error_ratio),
        (4.0, 1.0, 0.25)
    );
    assert_eq!(first.rate_per_sec, 0.4);
    assert_eq!(
        (first.latency_p50_ms, first.latency_p95_ms),
        (Some(20.0), Some(20.0))
    );
    assert_eq!(first.id, "behaviour:Service:svc:checkout:0");

    // Window two: four failing spans plus one error log; the log has no latency.
    assert_eq!(
        (second.requests, second.errors, second.error_ratio),
        (5.0, 5.0, 1.0)
    );
    let provenance = &second.provenance;
    assert_eq!(provenance.signal_counts[&SignalKind::Span], 4);
    assert_eq!(provenance.signal_counts[&SignalKind::Log], 1);
    assert!(provenance.sources.contains("app"));
    assert_eq!(
        provenance.rules.iter().collect::<Vec<_>>(),
        ["otel.service"]
    );
    assert_eq!(
        (provenance.first_ts_ms, provenance.last_ts_ms),
        (10_000, 12_000)
    );
}

#[test]
fn latency_percentiles_use_nearest_rank() {
    let signals: Vec<_> = (1..=20)
        .map(|ms| span("payments", ms, ms as i64, "OK"))
        .collect();
    let report = rollup(&rollup_policy(), &directory(), &signals);
    let observation = &report.observations[0];
    assert_eq!(observation.latency_p50_ms, Some(10.0));
    assert_eq!(observation.latency_p95_ms, Some(19.0));
}

#[test]
fn the_rollup_does_not_depend_on_arrival_order() {
    let mut signals = traffic("checkout", &[1, 3, 0], 5);
    signals.extend(traffic("payments", &[0, 2, 5], 5));
    let forward = rollup(&rollup_policy(), &directory(), &signals);
    signals.reverse();
    let backward = rollup(&rollup_policy(), &directory(), &signals);
    assert_eq!(forward, backward);
    assert_eq!(
        serde_json::to_string(&forward).unwrap(),
        serde_json::to_string(&backward).unwrap()
    );
}

#[test]
fn counters_contribute_their_increase_and_survive_a_reset() {
    let policy = RollupPolicy {
        window_ms: WINDOW_MS,
        metric_roles: BTreeMap::from([
            ("http_requests_total".to_string(), MetricRole::Requests),
            ("http_errors_total".to_string(), MetricRole::Errors),
            ("http_latency_ms".to_string(), MetricRole::LatencyMs),
        ]),
    };
    let signals = vec![
        metric(0, "http_requests_total", 100.0), // baseline only
        metric(5_000, "http_requests_total", 110.0),
        metric(12_000, "http_requests_total", 130.0),
        metric(15_000, "http_requests_total", 4.0), // reset: the increase is 4
        metric(0, "http_errors_total", 7.0),
        metric(12_000, "http_errors_total", 19.0),
        metric(13_000, "http_latency_ms", 250.0),
        metric(13_000, "node_load1", 3.0),           // undeclared
        metric(14_000, "http_latency_ms", f64::NAN), // staleness marker
    ];
    let report = rollup(&policy, &directory(), &signals);
    assert_eq!(report.ignored_metric_samples, 2);
    let [first, second] = report.observations.as_slice() else {
        panic!("expected two windows");
    };
    assert_eq!((first.requests, first.errors), (10.0, 0.0));
    assert_eq!((second.requests, second.errors), (24.0, 12.0));
    assert_eq!(second.error_ratio, 0.5);
    assert_eq!(second.latency_p95_ms, Some(250.0));
    assert_eq!(second.provenance.signal_counts[&SignalKind::Metric], 4);
}

#[test]
fn unbound_signals_are_reported_with_their_reason() {
    let signals = vec![
        span("ghost", 1, 5, "OK"),
        TelemetrySignal::log(2, "raw", "INFO", BTreeMap::new()),
        span("checkout", 3, 5, "OK"),
    ];
    let report = rollup(&rollup_policy(), &directory(), &signals);
    assert_eq!(report.observations.len(), 1);
    let reasons: Vec<_> = report
        .unresolved
        .iter()
        .map(|u| (u.ts_ms, &u.reason))
        .collect();
    assert_eq!(
        reasons,
        [
            (
                1,
                &Unresolved::UnknownEntity {
                    rule: "otel.service".into(),
                    key: vec!["ghost".into()],
                }
            ),
            (2, &Unresolved::NoResolutionKey),
        ]
    );
    assert_eq!(report.unresolved[0].kind, SignalKind::Span);
    assert_eq!(report.unresolved[1].source, "raw");
}
