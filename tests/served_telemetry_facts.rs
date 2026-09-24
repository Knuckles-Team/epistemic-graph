//! EH-408 / EH-409 — the served `TelemetryDerive` path, end to end.
//!
//! Stored telemetry (spans, a tenant log stream and a remote-write metric) sits
//! in the engine's in-process observability store; the request graph declares
//! two services with resolution keys, one of them declared healthy. One served
//! `TelemetryDerive` must:
//!
//! * read only the caller's tenant telemetry (a foreign tenant's failing spans
//!   for the same service change nothing),
//! * bind it to the declared services and roll it up into
//!   `BehaviourObservation` facts,
//! * fire the declared CEP burst pattern on both services and correlate the two
//!   anomalies into one `Incident`,
//! * record the declared-healthy-but-erroring conformance violation,
//! * write all of it into the request graph through the ordinary graph write
//!   path, linked to the entities and to what each fact was derived from,
//! * and write the same facts again, not new ones, when re-run.
//!
//! A stream outside the caller's tenant refuses the whole request, and an
//! engine without an observability store refuses with a typed error.
#![cfg(all(
    feature = "obs",
    feature = "shacl",
    feature = "traces",
    feature = "promql"
))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use std::collections::BTreeMap;
use std::sync::Arc;

use eg_stream::telemetry::{
    AnomalyRule, IncidentRule, MetricRole, ResolutionPolicy, RollupPolicy, TelemetryPolicy,
};
use eg_stream::{AttrPredicate, CepPattern, EventMatcher, Window};
use eg_tsdb::point::Point;
use eg_tsdb::traces::Span;
use eg_types::telemetry_derive::TelemetryDeriveReceipt;
use epistemic_graph::protocol::{Method, Response, ResultPayload};
use epistemic_graph::server::obs::{LogRecord, ObsState};
use serde_json::json;

const SECRET: &str = "served-telemetry-facts-secret";
/// The tenant `tests/common` signs every request for.
const TENANT: &str = "integration-test-tenant";
const GRAPH: &str = "__commons__";
/// Event time of the first window, epoch milliseconds.
const T0_MS: u64 = 1_700_000_000_000;
const WINDOW_MS: u64 = 10_000;
const REQUESTS_METRIC: &str = "http_requests_total";

async fn dispatch(state: &test_support::SharedState, id: u64, method: Method) -> Response {
    test_support::dispatch(state, test_support::commons_request(SECRET, id, method)).await
}

fn json_bytes(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

async fn declare_services(state: &test_support::SharedState) {
    let services = [
        (
            "svc:checkout",
            json!({"type": "Service", "resolution_keys": {"service.name": "checkout"},
                   "declared_health": {"health": "healthy", "max_error_ratio": 0.05}}),
        ),
        (
            "svc:payments",
            // Bound as a Service because the ontology says K8sService ⊑ Service.
            json!({"type": "K8sService", "resolution_keys": {"service.name": "payments"}}),
        ),
    ];
    for (index, (id, properties)) in services.into_iter().enumerate() {
        let response = dispatch(
            state,
            100 + index as u64,
            Method::AddNode {
                node_id: id.to_string(),
                properties_msgpack: json_bytes(properties),
            },
        )
        .await;
        assert!(
            response.error.is_none(),
            "declare {id}: {:?}",
            response.error
        );
    }
}

/// Ten ten-second windows of ten 20 ms spans from `service`; windows 4..=6
/// are 80% errors. Every span carries `tenant` under `eg_tenant`.
fn spans(service: &str, tenant: &str, failing_everywhere: bool) -> Vec<Span> {
    let mut out = Vec::new();
    for window in 0..10u64 {
        for request in 0..10u64 {
            let hot = (4..=6).contains(&window) && request < 8;
            let start_ms = T0_MS + window * WINDOW_MS + request * 10;
            out.push(Span {
                trace_id: format!("{tenant}-{service}-{window}-{request}"),
                span_id: "s".into(),
                parent_span_id: String::new(),
                service: service.into(),
                operation: "handle".into(),
                start_time: (start_ms * 1_000_000) as i64,
                duration: 20_000_000,
                status: if hot || failing_everywhere {
                    "ERROR"
                } else {
                    "OK"
                }
                .into(),
                attributes: BTreeMap::from([("eg_tenant".to_string(), tenant.to_string())]),
                events: Vec::new(),
            });
        }
    }
    out
}

fn seed_telemetry(obs: &ObsState) {
    let store = obs.trace_store();
    store.add_spans(spans("checkout", TENANT, false));
    store.add_spans(spans("payments", TENANT, false));
    // Another tenant's checkout fails constantly; none of it may be read.
    store.add_spans(spans("checkout", "other-tenant", true));
    obs.ingest(vec![LogRecord {
        ts: (T0_MS * 1_000_000) as i64,
        stream: format!("{TENANT}/app"),
        severity: "INFO".into(),
        body: "checkout started".into(),
        attrs: BTreeMap::from([("service.name".to_string(), "checkout".to_string())]),
    }])
    .unwrap();
    // A counter no individual declares a key for: read, rolled, unresolved.
    let series = format!("{REQUESTS_METRIC}{{eg_tenant=\"{TENANT}\",job=\"node\"}}");
    let points: Vec<Point> = (0..3u64)
        .map(|i| {
            Point::single(
                ((T0_MS + i * WINDOW_MS) * 1_000_000) as i64,
                10.0 * i as f64,
            )
        })
        .collect();
    obs.series_store()
        .append_batch(
            &series,
            1,
            3_600_000_000_000,
            &["value".to_string()],
            &points,
        )
        .unwrap();
}

fn policy() -> Vec<u8> {
    let hot = EventMatcher::key("BehaviourObservation").with_pred(AttrPredicate::Gt {
        field: "error_ratio".into(),
        value: 0.5,
    });
    let anomaly = EventMatcher::key("HealthAnomaly");
    let policy = TelemetryPolicy {
        resolution: ResolutionPolicy::standard(),
        rollup: RollupPolicy {
            window_ms: WINDOW_MS,
            metric_roles: BTreeMap::from([(REQUESTS_METRIC.to_string(), MetricRole::Requests)]),
        },
        anomalies: vec![AnomalyRule {
            id: "error-burst".into(),
            kind: "error_burst".into(),
            pattern: CepPattern::Sequence(vec![hot.clone(), hot.clone(), hot]),
            window: Window::Sliding {
                size: 2 * WINDOW_MS,
            },
        }],
        incidents: vec![IncidentRule {
            id: "correlated".into(),
            pattern: CepPattern::Sequence(vec![anomaly.clone(), anomaly]),
            window: Window::Sliding {
                size: 3 * WINDOW_MS,
            },
            min_entities: 2,
        }],
    };
    rmp_serde::to_vec_named(&policy).unwrap()
}

fn derive_method(streams: Vec<String>) -> Method {
    Method::TelemetryDerive {
        from_ms: T0_MS,
        to_ms: T0_MS + 10 * WINDOW_MS,
        streams,
        policy_msgpack: policy(),
    }
}

fn receipt(response: Response) -> TelemetryDeriveReceipt {
    assert!(
        response.error.is_none(),
        "TelemetryDerive: {:?}",
        response.error
    );
    match response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
        other => panic!("TelemetryDerive answered {other:?}"),
    }
}

async fn served_state() -> (test_support::SharedState, Arc<ObsState>) {
    let state = test_support::durable_state(SECRET, common::current_isolation());
    let obs = Arc::new(ObsState::in_memory(1024).unwrap());
    seed_telemetry(&obs);
    state.write().await.obs = Some(obs.clone());
    declare_services(&state).await;
    (state, obs)
}

async fn node(state: &test_support::SharedState, id: &str) -> serde_json::Value {
    let guard = state.read().await;
    let core = &guard.registry.get(GRAPH).expect("request graph").core;
    let bytes = core
        .get_node_properties(id)
        .unwrap_or_else(|| panic!("fact {id} was not written"));
    eg_types::msgpack::decode_property_value(&bytes).unwrap()
}

async fn edge_count(state: &test_support::SharedState, relationship: &str) -> usize {
    let guard = state.read().await;
    let core = &guard.registry.get(GRAPH).expect("request graph").core;
    core.get_edges()
        .into_iter()
        .filter(|(_, _, properties)| {
            eg_types::msgpack::decode_property_value(properties)
                .map(|value| value["relationship"] == relationship)
                .unwrap_or(false)
        })
        .count()
}

#[tokio::test]
async fn served_derivation_writes_bound_facts_with_provenance() {
    let (state, _obs) = served_state().await;
    let first = receipt(dispatch(&state, 1, derive_method(vec![format!("{TENANT}/app")])).await);

    // 200 own spans + 1 log + 3 metric samples; the other tenant's 100 are unread.
    assert_eq!(first.signals_read, 204);
    assert_eq!(first.observations, 20);
    assert_eq!((first.anomalies, first.incidents), (2, 1));
    assert_eq!(
        first.violations, 3,
        "checkout erred over its ceiling in windows 4..=6"
    );
    assert_eq!(first.unresolved, 3, "the job=node counter binds to nothing");
    assert_eq!(first.invalid_declarations, 0);
    assert_eq!(first.nodes_written as usize, first.fact_ids.len());

    let window_four = format!("behaviour:Service:svc:checkout:{}", T0_MS + 4 * WINDOW_MS);
    let observation = node(&state, &window_four).await;
    assert_eq!(observation["type"], "BehaviourObservation");
    assert_eq!(observation["evidence_class"], "observation");
    assert_eq!(observation["error_ratio"], 0.8);
    assert_eq!(observation["tenant_id"], TENANT);
    assert_eq!(observation["provenance"]["rules"], json!(["otel.service"]));
    let window_zero = format!("behaviour:Service:svc:checkout:{T0_MS}");
    assert_eq!(
        node(&state, &window_zero).await["requests"],
        11.0,
        "10 spans + 1 log"
    );

    let incident_id = first
        .fact_ids
        .iter()
        .find(|id| id.starts_with("incident:"))
        .expect("an incident fact");
    let incident = node(&state, incident_id).await;
    assert_eq!(incident["type"], "Incident");
    assert_eq!(incident["entities"].as_array().unwrap().len(), 2);
    assert!(first
        .fact_ids
        .iter()
        .any(|id| id.starts_with("conformance:erroring_while_declared_healthy:")));
    assert_eq!(edge_count(&state, "OBSERVES").await, 20);
    assert_eq!(edge_count(&state, "AFFECTS_ENTITY").await, 2 + 2 + 3);

    // Re-deriving the same telemetry upserts the same facts.
    let second = receipt(dispatch(&state, 2, derive_method(vec![format!("{TENANT}/app")])).await);
    assert_eq!(second.fact_ids, first.fact_ids);
    assert_eq!(edge_count(&state, "OBSERVES").await, 20);
}

#[tokio::test]
async fn a_stream_outside_the_callers_tenant_refuses_the_request() {
    let (state, _obs) = served_state().await;
    let response = dispatch(&state, 3, derive_method(vec!["other-tenant/app".into()])).await;
    let error = response.error.expect("a foreign stream must be refused");
    assert!(error.contains("ACCESS_DENIED"), "{error}");
}

#[tokio::test]
async fn an_engine_without_an_observability_store_says_so() {
    let state = test_support::durable_state(SECRET, common::current_isolation());
    let response = dispatch(&state, 4, derive_method(Vec::new())).await;
    let error = response.error.expect("no store configured");
    assert!(error.contains("TELEMETRY_UNAVAILABLE"), "{error}");
}
