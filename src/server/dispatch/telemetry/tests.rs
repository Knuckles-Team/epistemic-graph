//! The served derivation's own pieces: tenant scope, declarations read from a
//! graph, and the one `BatchUpdate` the facts become. The end-to-end served
//! path is `tests/served_telemetry_facts.rs`.

use std::collections::BTreeMap;

use eg_core::graph::GraphCore;
use eg_stream::telemetry::{
    DeclaredHealth, EntityClass, FactEdge, FactGraph, FactNode, Measure, Outcome,
};
use serde_json::json;

use super::collect::{log_signal, require_tenant_streams, stream_in_tenant, Window};
use super::declarations::declarations_from;
use super::materialize::{fact_batch, FactOwner};
use crate::algorithms::{decode_batch_operations, BatchOperation};
use crate::server::obs::LogRecord;

fn node(id: &str, properties: serde_json::Value) -> (String, Vec<u8>) {
    (
        id.to_string(),
        rmp_serde::to_vec_named(&properties).unwrap(),
    )
}

#[test]
fn a_stream_is_in_a_tenant_only_by_exact_name_or_path_prefix() {
    assert!(stream_in_tenant("acme", "acme"));
    assert!(stream_in_tenant("acme/app", "acme"));
    assert!(!stream_in_tenant("acme-evil/app", "acme"));
    assert!(!stream_in_tenant("other/app", "acme"));
    assert!(!stream_in_tenant("anything", ""));
    assert!(require_tenant_streams("acme", &["acme/app".into()]).is_ok());
    let refused = require_tenant_streams("acme", &["acme/app".into(), "other".into()]);
    assert!(refused.unwrap_err().starts_with("ACCESS_DENIED"));
}

#[test]
fn a_window_must_be_non_empty() {
    assert!(Window::new(10, 10).is_err());
    assert!(Window::new(11, 10).is_err());
    let window = Window::new(1, 2).unwrap();
    assert_eq!((window.start_ns(), window.end_ns()), (1_000_000, 2_000_000));
}

#[test]
fn a_stored_log_record_becomes_a_log_signal() {
    let record = LogRecord {
        ts: 5_000_000_000,
        stream: "acme/app".into(),
        severity: "ERROR".into(),
        body: "boom".into(),
        attrs: BTreeMap::from([("service.name".to_string(), "checkout".to_string())]),
    };
    let signal = log_signal(&record);
    assert_eq!(signal.ts_ms, 5_000);
    assert_eq!(signal.source, "acme/app");
    assert_eq!(
        signal.measure,
        Measure::Log {
            outcome: Outcome::Error
        }
    );
    assert_eq!(signal.attributes["service.name"], "checkout");
}

#[test]
fn declarations_come_from_visible_bindable_individuals_only() {
    let core = GraphCore::new();
    for (id, properties) in [
        node(
            "svc:checkout",
            json!({"type": "Service", "resolution_keys": {"service.name": "checkout"},
                   "declared_health": {"health": "healthy", "max_error_ratio": 0.05}}),
        ),
        node(
            "svc:hidden",
            json!({"type": "Service", "resolution_keys": {"service.name": "hidden"}}),
        ),
        node(
            "svc:bad",
            json!({"type": "Service", "resolution_keys": {"service.name": 7},
                               "declared_health": {"health": "sometimes"}}),
        ),
        node(
            "host:r820",
            json!({"type": "Host", "resolution_keys": {"host.name": "r820"}}),
        ),
        node(
            "doc:1",
            json!({"type": "Document", "resolution_keys": {"service.name": "checkout"}}),
        ),
        node("svc:silent", json!({"type": "Service"})),
    ] {
        core.add_node(id, properties);
    }
    let read = declarations_from(&core, |id, _| id != "svc:hidden");
    let entities: Vec<_> = read
        .declarations
        .entities
        .iter()
        .map(|declared| (declared.entity.class, declared.entity.id.as_str()))
        .collect();
    assert_eq!(
        entities,
        [
            (EntityClass::Service, "svc:checkout"),
            (EntityClass::Host, "host:r820"),
        ]
    );
    let [health] = read.declarations.health.as_slice() else {
        panic!("one declared health");
    };
    assert_eq!(health.declared_by, "svc:checkout");
    assert!(matches!(health.health, DeclaredHealth::Healthy { .. }));
    assert_eq!(
        read.invalid, 2,
        "svc:bad's keys and health are both unusable"
    );
}

#[test]
fn facts_become_one_owned_batch_with_nodes_before_edges() {
    let graph = FactGraph {
        nodes: vec![FactNode {
            id: "behaviour:Service:svc:checkout:0".into(),
            class_iri: "http://knuckles.team/kg/infrastructure#BehaviourObservation".into(),
            properties: json!({"type": "BehaviourObservation", "requests": 3.0})
                .as_object()
                .unwrap()
                .clone(),
        }],
        edges: vec![FactEdge {
            source: "behaviour:Service:svc:checkout:0".into(),
            target: "svc:checkout".into(),
            relationship: "OBSERVES".into(),
        }],
    };
    let owner = FactOwner {
        tenant_id: "acme",
        agent_id: "agent-a",
    };
    let batch = fact_batch(&graph, &owner).unwrap();
    assert_eq!(batch.fact_ids, ["behaviour:Service:svc:checkout:0"]);
    assert_eq!(batch.edges, 1);
    let operations = decode_batch_operations(&batch.operations_msgpack).unwrap();
    let [BatchOperation::AddNode {
        id,
        properties_msgpack,
        upsert: true,
    }, BatchOperation::AddEdge {
        source,
        target,
        upsert: true,
        ..
    }] = operations.as_slice()
    else {
        panic!("one upserted node, then one upserted edge: {operations:?}");
    };
    assert_eq!(id, "behaviour:Service:svc:checkout:0");
    assert_eq!(
        (source.as_str(), target.as_str()),
        (id.as_str(), "svc:checkout")
    );
    let stored = eg_types::msgpack::decode_property_value(properties_msgpack).unwrap();
    assert_eq!(stored["tenant_id"], "acme");
    assert_eq!(stored["_owner"], "agent-a");
    assert_eq!(stored["_visibility"], "private");
    assert_eq!(stored["requests"], 3.0);
}

#[test]
fn an_empty_derivation_writes_nothing() {
    let owner = FactOwner {
        tenant_id: "acme",
        agent_id: "agent-a",
    };
    assert!(fact_batch(&FactGraph::default(), &owner)
        .unwrap()
        .is_empty());
}
