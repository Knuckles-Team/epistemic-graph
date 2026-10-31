//! Direct AddNode admission of versioned harness-component writes
//! (MEDULLA-ORCH-VOCAB-R801.3).

use super::{decode_props, direct_add_node};
use crate::graph::GraphCore;
use crate::protocol::Method;

fn harness_version_props(
    component_id: &str,
    kind: &str,
    content_ref: &str,
    valid_from: u64,
    confidence: f64,
) -> serde_json::Map<String, serde_json::Value> {
    let digest = medulla_types::orchestration::HarnessComponentVersion::content_digest(content_ref);
    let mut props = serde_json::Map::new();
    props.insert("_harness_component".into(), serde_json::json!(true));
    props.insert("component_id".into(), serde_json::json!(component_id));
    props.insert("kind".into(), serde_json::json!(kind));
    props.insert("content_ref".into(), serde_json::json!(content_ref));
    props.insert("content_digest".into(), serde_json::json!(digest));
    props.insert("valid_from".into(), serde_json::json!(valid_from));
    props.insert("confidence".into(), serde_json::json!(confidence));
    props.insert(
        "evidence_run_id".into(),
        serde_json::json!(format!("run-{component_id}-{valid_from}")),
    );
    props
}

fn harness_node_id(component_id: &str, content_ref: &str) -> String {
    format!(
        "harness-component:{component_id}:{}",
        medulla_types::orchestration::HarnessComponentVersion::content_digest(content_ref)
    )
}

fn add_node(node_id: String, props: &serde_json::Map<String, serde_json::Value>) -> Method {
    Method::AddNode {
        node_id,
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::Value::Object(props.clone()))
            .unwrap(),
    }
}

/// The property blob the direct AddNode admission commits for `method`.
fn admitted_blob(method: &Method, core: &GraphCore, tx_now: u64) -> Vec<u8> {
    let admitted = direct_add_node(method, core, tx_now).unwrap();
    let Method::AddNode {
        properties_msgpack, ..
    } = admitted
    else {
        panic!("AddNode came back")
    };
    properties_msgpack
}

// spec: MEDULLA-ORCH-VOCAB-R801.3
#[test]
fn harness_component_write_stamps_and_links() {
    let core = GraphCore::new();
    let component = "comp:prompt";
    let v1_id = harness_node_id(component, "blob:v1");
    let v2_id = harness_node_id(component, "blob:v2");

    // The first version is admitted, stamped with the transaction time,
    // and carries no supersedes pointer (nothing to supersede).
    let first = add_node(
        v1_id.clone(),
        &harness_version_props(component, "system_prompt", "blob:v1", 10, 0.8),
    );
    let properties_msgpack = &admitted_blob(&first, &core, 1000);
    let stamped = decode_props(properties_msgpack);
    assert_eq!(stamped["tx_from"], serde_json::json!(1000));
    assert!(stamped.get("supersedes_id").is_none());
    core.add_node(v1_id.clone(), properties_msgpack.clone());

    // The second version is stamped and LINKED: the engine fills the
    // supersedes pointer with the current version.
    let second = add_node(
        v2_id.clone(),
        &harness_version_props(component, "system_prompt", "blob:v2", 20, 0.9),
    );
    let properties_msgpack = &admitted_blob(&second, &core, 2000);
    let stamped = decode_props(properties_msgpack);
    assert_eq!(stamped["tx_from"], serde_json::json!(2000));
    assert_eq!(stamped["supersedes_id"], serde_json::json!(v1_id));
    core.add_node(v2_id.clone(), properties_msgpack.clone());
    let v2_blob = properties_msgpack.clone();

    // The same write replays as an idempotent no-op: the stored blob
    // (its original stamp) comes back byte-exact, not a re-stamp.
    let replay = add_node(
        v2_id.clone(),
        &harness_version_props(component, "system_prompt", "blob:v2", 20, 0.9),
    );
    let properties_msgpack = &admitted_blob(&replay, &core, 3000);
    assert_eq!(properties_msgpack, &v2_blob);
    let replayed = decode_props(properties_msgpack);
    assert_eq!(replayed["tx_from"], serde_json::json!(2000));

    // A published version never mutates: same content, other metadata.
    let mutated = add_node(
        v2_id.clone(),
        &harness_version_props(component, "system_prompt", "blob:v2", 20, 0.95),
    );
    let error = direct_add_node(&mutated, &core, 4000).unwrap_err();
    assert!(
        error.starts_with("HarnessVersionImmutable:"),
        "got: {error}"
    );

    // A stale supersession claim refuses: v3 names v1 while v2 is
    // current.
    let v3_id = harness_node_id(component, "blob:v3");
    let mut stale = harness_version_props(component, "system_prompt", "blob:v3", 30, 0.9);
    stale.insert("supersedes_id".into(), serde_json::json!(v1_id));
    let stale_write = add_node(v3_id, &stale);
    let error = direct_add_node(&stale_write, &core, 5000).unwrap_err();
    assert!(
        error.starts_with("HarnessSupersessionStale:"),
        "got: {error}"
    );

    // A non-harness AddNode passes through untouched.
    let mut plain = serde_json::Map::new();
    plain.insert("label".into(), serde_json::json!("Thing"));
    let plain_write = add_node("some:other:node".into(), &plain);
    let Method::AddNode {
        properties_msgpack: plain_blob,
        ..
    } = &plain_write
    else {
        panic!("AddNode was composed")
    };
    let passed = direct_add_node(&plain_write, &core, 6000).unwrap();
    let Method::AddNode {
        node_id,
        properties_msgpack,
    } = &passed
    else {
        panic!("AddNode came back")
    };
    assert_eq!(node_id, "some:other:node");
    assert_eq!(properties_msgpack, plain_blob);
}
