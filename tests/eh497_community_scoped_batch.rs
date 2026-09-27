//! Served EH-497 proof for the opt-in AU community persistence batch.
//! The same signed BatchUpdate and GetEdges/GetNodeProperties routes used by
//! clients must preserve an unrelated parallel fact and governance stamps.
#![cfg(all(feature = "server", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::protocol::{GraphType, Method, ResultPayload};
use epistemic_graph::server::dispatch;
use serde_json::{json, Value};

const SECRET: &str = "eh497-community-scoped-batch-secret";
const GRAPH: &str = "eh497-community";

async fn call(
    state: &test_support::SharedState,
    id: u64,
    method: Method,
) -> epistemic_graph::protocol::Response {
    Box::pin(dispatch(
        state,
        test_support::request(SECRET, id, GRAPH, method),
    ))
    .await
}

fn batch(ops: Value) -> Method {
    Method::BatchUpdate {
        operations_msgpack: rmp_serde::to_vec_named(&ops).unwrap(),
    }
}

#[tokio::test]
async fn signed_community_batch_preserves_parallel_edges_and_actor_stamps() {
    let state = test_support::durable_state(SECRET, common::current_isolation());
    {
        let server = &mut *state.write().await;
        server
            .registry
            .create_graph(GRAPH, GraphType::Commons, None)
            .unwrap();
    }
    let seed = batch(json!([
        {"op": "add_node", "id": "member", "properties": {"type": "Document"}},
        {"op": "add_node", "id": "community_cluster_0", "properties": {
            "type": "Community", "member_count": 1, "coherence_score": 1.0,
            "is_permanent": true, "tenant_id": "integration-test-tenant",
            "_owner_id": "writer:alice", "classification": "confidential"
        }},
        {"op": "add_edge", "source": "member", "target": "community_cluster_0",
            "properties": {"relationship": "OTHER", "weight": 0.25}},
        {"op": "upsert_edge_relationship", "source": "member",
            "target": "community_cluster_0", "properties": {
                "relationship": "PART_OF_COMMUNITY", "weight": 1.0,
                "tenant_id": "integration-test-tenant", "_owner_id": "writer:alice",
                "classification": "confidential"
            }}
    ]));
    let first = call(&state, 1, seed).await;
    assert!(
        first.error.is_none(),
        "signed batch rejected: {:?}",
        first.error
    );
    let repeat = batch(json!([{"op": "upsert_edge_relationship",
    "source": "member", "target": "community_cluster_0", "properties": {
        "relationship": "PART_OF_COMMUNITY", "weight": 0.9,
        "tenant_id": "integration-test-tenant", "_owner_id": "writer:alice",
        "classification": "confidential"
    }}]));
    let second = call(&state, 2, repeat).await;
    assert!(
        second.error.is_none(),
        "signed repeat rejected: {:?}",
        second.error
    );

    let edges = call(&state, 3, Method::GetEdges).await;
    let mut rows: Vec<(String, String, Value)> = test_support::edge_rows(&edges)
        .into_iter()
        .map(|(source, target, blob)| (source, target, rmp_serde::from_slice(&blob).unwrap()))
        .collect();
    rows.sort_by_key(|(_, _, props)| props["relationship"].as_str().unwrap().to_owned());
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|(source, target, _)| source == "member" && target == "community_cluster_0"));
    assert_eq!(rows[0].2["relationship"], "OTHER");
    assert_eq!(rows[0].2["weight"], 0.25);
    assert_eq!(rows[1].2["relationship"], "PART_OF_COMMUNITY");
    assert_eq!(rows[1].2["weight"], 0.9);
    assert_eq!(rows[1].2["tenant_id"], "integration-test-tenant");
    assert_eq!(rows[1].2["_owner_id"], "writer:alice");
    assert_eq!(rows[1].2["classification"], "confidential");

    let node = call(
        &state,
        4,
        Method::GetNodeProperties {
            node_id: "community_cluster_0".to_owned(),
        },
    )
    .await;
    assert!(
        node.error.is_none(),
        "community read rejected: {:?}",
        node.error
    );
    let props: Value = match node.result {
        Some(ResultPayload::Raw(blob)) => rmp_serde::from_slice(&blob).unwrap(),
        other => panic!("community properties missing: {other:?}"),
    };
    assert_eq!(props["type"], "Community");
    assert_eq!(props["member_count"], 1);
    assert_eq!(props["coherence_score"], 1.0);
    assert_eq!(props["is_permanent"], true);
    assert_eq!(props["tenant_id"], "integration-test-tenant");
    assert_eq!(props["_owner_id"], "writer:alice");
    assert_eq!(props["classification"], "confidential");
}

#[tokio::test]
async fn pair_wide_upsert_still_replaces_all_and_unsigned_scoped_write_is_denied() {
    let state = test_support::durable_state(SECRET, common::current_isolation());
    {
        let server = &mut *state.write().await;
        server
            .registry
            .create_graph(GRAPH, GraphType::Commons, None)
            .unwrap();
    }
    let seed = batch(json!([
        {"op": "add_node", "id": "a", "properties": {}},
        {"op": "add_node", "id": "b", "properties": {}},
        {"op": "add_edge", "source": "a", "target": "b",
            "properties": {"relationship": "OTHER"}},
        {"op": "add_edge", "source": "a", "target": "b",
            "properties": {"relationship": "PART_OF_COMMUNITY"}}
    ]));
    assert!(call(&state, 10, seed).await.error.is_none());

    let mut unsigned = test_support::request(
        SECRET,
        11,
        GRAPH,
        batch(
            json!([{"op": "upsert_edge_relationship", "source": "a", "target": "b",
            "properties": {"relationship": "PART_OF_COMMUNITY", "weight": 0.9}}]),
        ),
    );
    unsigned.auth_token.clear();
    let denied = Box::pin(dispatch(&state, unsigned)).await;
    assert!(denied.error.is_some(), "unsigned mutation was accepted");
    let unchanged = call(&state, 12, Method::GetEdges).await;
    assert_eq!(test_support::edge_rows(&unchanged).len(), 2);

    assert!(call(
        &state,
        13,
        batch(json!([{"op": "upsert_edge", "source": "a", "target": "b",
            "properties": {"relationship": "REPLACEMENT"}}])),
    )
    .await
    .error
    .is_none());
    let pair_replaced = call(&state, 14, Method::GetEdges).await;
    let pair_rows = test_support::edge_rows(&pair_replaced);
    assert_eq!(
        pair_rows.len(),
        1,
        "legacy pair-wide upsert must remove both edges"
    );
    let pair_props: Value = rmp_serde::from_slice(&pair_rows[0].2).unwrap();
    assert_eq!(pair_props["relationship"], "REPLACEMENT");

    assert!(call(
        &state,
        15,
        batch(
            json!([{"op": "upsert_edge_relationship", "source": "a", "target": "b",
            "properties": {"relationship": "PART_OF_COMMUNITY"}}])
        ),
    )
    .await
    .error
    .is_none());
    let scoped = call(&state, 16, Method::GetEdges).await;
    let mut relationships: Vec<String> = test_support::edge_rows(&scoped)
        .iter()
        .map(|(_, _, blob)| {
            let props: Value = rmp_serde::from_slice(blob).unwrap();
            props["relationship"].as_str().unwrap().to_owned()
        })
        .collect();
    relationships.sort();
    assert_eq!(
        relationships,
        vec!["PART_OF_COMMUNITY".to_string(), "REPLACEMENT".to_string()]
    );
}
