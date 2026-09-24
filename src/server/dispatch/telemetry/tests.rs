//! The served derivation's own pieces: tenant scope, declarations read from a
//! graph, and the one `BatchUpdate` the facts become. The end-to-end served
//! path is `tests/served_telemetry_facts.rs`.

use std::collections::BTreeMap;

use eg_core::graph::GraphCore;
use eg_stream::telemetry::{
    DeclaredHealth, EntityClass, FactEdge, FactGraph, FactNode, Measure, Outcome,
};
use serde_json::json;

use super::classes::{bindable_types, ontology_digest, BindableTypes, ClassificationCache};
use super::collect::{log_signal, require_tenant_streams, stream_in_tenant, Window};
use super::declarations::declarations_from;
use super::materialize::{fact_batch, FactOwner};
use crate::algorithms::{decode_batch_operations, BatchOperation};
use crate::graph::{GraphSchemaSource, GraphSchemaSources, SchemaSourceOrigin};
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

fn targets(node_type: &str) -> Vec<EntityClass> {
    let types = bindable_types(&GraphSchemaSources::default()).unwrap();
    types
        .targets(node_type)
        .map(|set| set.iter().copied().collect())
        .unwrap_or_default()
}

#[test]
fn the_ontology_decides_which_node_types_bind() {
    // Subsumption in infrastructure-v1, not a list: K8sService/SwarmService ⊑
    // Service, Deployment ⊑ Workload.
    assert_eq!(targets("K8sService"), [EntityClass::Service]);
    assert_eq!(targets("SwarmService"), [EntityClass::Service]);
    assert_eq!(targets("Deployment"), [EntityClass::Deployment]);
    assert_eq!(
        targets("http://knuckles.team/kg/infrastructure#K8sService"),
        [EntityClass::Service]
    );
    for (node_type, class) in [
        ("Server", EntityClass::Server),
        ("Service", EntityClass::Service),
        ("Host", EntityClass::Host),
        ("Workload", EntityClass::Deployment),
        ("Agent", EntityClass::Agent),
    ] {
        assert_eq!(targets(node_type), [class], "{node_type}");
    }
    // A Pod is scheduled BY a Workload; it is not one. It binds as a Pod.
    assert_eq!(targets("Pod"), [EntityClass::Pod]);
    assert!(targets("Document").is_empty());
}

/// A graph of fleet individuals: services, a deployment, two pods (one
/// `scheduledBy` the deployment, one only `runsOn` it), a document and a
/// service with no declaration.
fn fleet_core() -> GraphCore {
    let core = GraphCore::new();
    for (id, properties) in [
        node(
            "svc:checkout",
            json!({"type": "K8sService", "resolution_keys": {"service.name": "checkout"},
                   "declared_health": {"health": "healthy", "max_error_ratio": 0.05}}),
        ),
        node(
            "svc:hidden",
            json!({"type": "Service", "resolution_keys": {"service.name": "hidden"}}),
        ),
        node(
            "svc:bad",
            json!({"type": "SwarmService", "resolution_keys": {"service.name": 7},
                   "declared_health": {"health": "sometimes"}}),
        ),
        node(
            "deploy:shop/cart",
            json!({"type": "Deployment", "resolution_keys":
                   {"k8s.namespace.name": "shop", "k8s.deployment.name": "cart"}}),
        ),
        node(
            "pod:cart-1",
            json!({"type": "Pod", "resolution_keys":
                   {"k8s.namespace.name": "shop", "k8s.pod.name": "cart-1"}}),
        ),
        node(
            "pod:orphan",
            json!({"type": "Pod", "resolution_keys":
                   {"k8s.namespace.name": "shop", "k8s.pod.name": "orphan"}}),
        ),
        node(
            "doc:1",
            json!({"type": "Document", "resolution_keys": {"service.name": "checkout"}}),
        ),
        node("svc:silent", json!({"type": "Service"})),
    ] {
        core.add_node(id, properties);
    }
    for (source, target, relationship) in [
        ("pod:cart-1", "deploy:shop/cart", "scheduledBy"),
        ("pod:orphan", "deploy:shop/cart", "runsOn"),
    ] {
        core.add_edge(
            source.into(),
            target.into(),
            rmp_serde::to_vec_named(&json!({ "relationship": relationship })).unwrap(),
        )
        .unwrap();
    }
    core
}

fn fleet_declarations() -> super::declarations::ReadDeclarations {
    let types = bindable_types(&GraphSchemaSources::default()).unwrap();
    declarations_from(&fleet_core(), &types, |id, _| id != "svc:hidden")
}

#[test]
fn declarations_come_from_visible_individuals_of_subsumed_types_only() {
    let read = fleet_declarations();
    let entities: Vec<_> = read
        .declarations
        .entities
        .iter()
        .map(|declared| (declared.entity.class, declared.entity.id.as_str()))
        .collect();
    assert_eq!(
        entities,
        [
            (EntityClass::Deployment, "deploy:shop/cart"),
            (EntityClass::Pod, "pod:cart-1"),
            (EntityClass::Pod, "pod:orphan"),
            (EntityClass::Service, "svc:checkout"),
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
fn a_scheduled_by_edge_declares_that_a_pod_rolls_up_to_its_workload() {
    let read = fleet_declarations();
    // Only the declared `scheduledBy` edge is a roll-up relation.
    let [aggregation] = read.declarations.aggregations.as_slice() else {
        panic!("one aggregation: {:?}", read.declarations.aggregations);
    };
    assert_eq!(
        (aggregation.part.id.as_str(), aggregation.whole.id.as_str()),
        ("pod:cart-1", "deploy:shop/cart")
    );
    assert_eq!(aggregation.whole.class, EntityClass::Deployment);
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

fn operator_source(ontology: Option<&str>, shapes: Option<&str>) -> GraphSchemaSource {
    GraphSchemaSource::new(
        SchemaSourceOrigin::Operator,
        shapes.map(Into::into),
        ontology.map(Into::into),
        0,
    )
    .unwrap()
}

#[test]
fn classification_is_cached_by_ontology_digest() {
    let fresh = || Ok(std::sync::Arc::new(BindableTypes::default()));
    let cache = ClassificationCache::default();
    let core = GraphSchemaSources::default();
    let core_key = ontology_digest(&core).unwrap();

    cache.get_or_classify(core_key, fresh).unwrap();
    cache.get_or_classify(core_key, fresh).unwrap();
    assert_eq!(cache.classifications(), 1, "an unchanged digest is a hit");

    // Shapes do not change the class hierarchy, so they do not change the key.
    let mut shapes_only = core.clone();
    shapes_only.dynamic.insert(
        "shapes".into(),
        operator_source(None, Some("@prefix sh: <http://www.w3.org/ns/shacl#> .")),
    );
    assert_eq!(ontology_digest(&shapes_only).unwrap(), core_key);

    // An attached ontology does.
    let mut extended = core.clone();
    extended.dynamic.insert(
        "extra".into(),
        operator_source(
            Some("<urn:x:Probe> a <http://www.w3.org/2002/07/owl#Class> ."),
            None,
        ),
    );
    let extended_key = ontology_digest(&extended).unwrap();
    assert_ne!(extended_key, core_key);
    cache.get_or_classify(extended_key, fresh).unwrap();
    assert_eq!(cache.classifications(), 2, "a changed digest is a miss");
    cache.get_or_classify(core_key, fresh).unwrap();
    assert_eq!(
        cache.classifications(),
        2,
        "the older entry is still resident"
    );

    // A failed classification is not cached.
    let failing = eg_types::contract::Digest256::sha256(b"failing");
    assert!(cache
        .get_or_classify(failing, || Err("no".to_string()))
        .is_err());
    cache.get_or_classify(failing, fresh).unwrap();
    assert_eq!(cache.classifications(), 4);

    // Bounded: sixteen newer identities evict the least recently used one.
    for index in 0..16u8 {
        let key = eg_types::contract::Digest256::sha256(&[index]);
        cache.get_or_classify(key, fresh).unwrap();
    }
    let before = cache.classifications();
    cache.get_or_classify(extended_key, fresh).unwrap();
    assert_eq!(
        cache.classifications(),
        before + 1,
        "evicted, so classified again"
    );
}
