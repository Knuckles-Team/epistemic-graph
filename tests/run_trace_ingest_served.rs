//! Served RunTrace ingestion: shape refusal and client round trip
//! (EG-ORCH-VOCAB-R009.15, R009.17).
#![cfg(all(feature = "server", feature = "security", feature = "shacl"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::protocol::{CypherMode, GraphType, Method, Response};

const SECRET: &str = "orch-vocab-r009-run-trace-served-secret";
const FIXTURES: &str = include_str!("../crates/eg-core/tests/fixtures/run_traces.json");

fn state() -> test_support::SharedState {
    common::configure_authority();
    test_support::durable_state(SECRET, common::current_isolation())
}

async fn call(state: &test_support::SharedState, id: u64, graph: &str, method: Method) -> Response {
    test_support::dispatch(state, test_support::request(SECRET, id, graph, method)).await
}

fn fixtures() -> Vec<serde_json::Value> {
    let document: serde_json::Value = serde_json::from_str(FIXTURES).unwrap();
    document["traces"].as_array().unwrap().clone()
}

fn add_trace(trace: &serde_json::Value) -> Method {
    let mut props = trace.clone();
    props["type"] = "RunTrace".into();
    Method::AddNode {
        node_id: trace["id"].as_str().unwrap().to_string(),
        properties_msgpack: rmp_serde::to_vec_named(&props).unwrap(),
    }
}

async fn graph(state: &test_support::SharedState, name: &str) {
    let created = call(
        state,
        1,
        name,
        Method::CreateGraph {
            graph_name: name.into(),
            graph_type: GraphType::Global,
        },
    )
    .await;
    assert!(created.error.is_none(), "{:?}", created.error);
}

// spec: EG-ORCH-VOCAB-R009.15
#[tokio::test]
async fn invalid_trace_is_refused_and_not_stored() {
    let state = state();
    graph(&state, "rt-refuse").await;
    let mut bad = fixtures()[1].clone(); // over budget, context_overflow
    bad["terminationReason"] = "error".into();
    let refused = call(&state, 2, "rt-refuse", add_trace(&bad)).await;
    let detail = format!("{:?} {:?}", refused.error, refused.error_detail);
    assert!(refused.result.is_none(), "{detail}");
    assert!(detail.contains("RunTraceBudgetShape"), "{detail}");
    let read = call(
        &state,
        3,
        "rt-refuse",
        Method::CypherQuery {
            query: "MATCH (t:RunTrace) RETURN t.id".into(),
            mode: CypherMode::Read,
        },
    )
    .await;
    assert!(read.error.is_none(), "{:?}", read.error);
    assert!(!format!("{:?}", read.result).contains("trace-context-overflow"));
}

// Supports R009.15's link handling: declared links become typed edges, and a
// link to an unknown node is refused. (R009.17 stays open: the generated client
// RunTrace ingestion method of EG-CONTRACT-R051.1 does not exist yet.)
#[tokio::test]
async fn admitted_trace_links_are_typed_edges() {
    use epistemic_graph::protocol::ResultPayload;
    let state = state();
    graph(&state, "rt-links").await;
    let trace = fixtures()[0].clone();
    let dangling = call(&state, 2, "rt-links", add_trace(&trace)).await;
    assert!(dangling.result.is_none(), "unknown link targets are refused");
    for (index, key) in ["runModel", "runTaskClass", "appliedHarnessConfig"]
        .iter()
        .enumerate()
    {
        let added = call(
            &state,
            10 + index as u64,
            "rt-links",
            Method::AddNode {
                node_id: trace[*key].as_str().unwrap().to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type": "Entity"}))
                    .unwrap(),
            },
        )
        .await;
        assert!(added.error.is_none(), "{:?}", added.error);
    }
    let added = call(&state, 20, "rt-links", add_trace(&trace)).await;
    assert!(added.error.is_none(), "{:?} {:?}", added.error, added.error_detail);
    let edges = call(&state, 21, "rt-links", Method::GetEdges).await;
    let Some(ResultPayload::EdgeList(rows)) = edges.result else {
        panic!("expected an edge list: {:?}", edges.error)
    };
    let mut found: Vec<(String, String)> = rows
        .iter()
        .filter(|(source, _, _)| source == trace["id"].as_str().unwrap())
        .map(|(_, target, blob)| {
            let props: serde_json::Value = rmp_serde::from_slice(blob).unwrap();
            (props["relationship"].as_str().unwrap().to_string(), target.clone())
        })
        .collect();
    found.sort();
    let mut expected: Vec<(String, String)> = ["runModel", "runTaskClass", "appliedHarnessConfig"]
        .iter()
        .map(|k| (k.to_string(), trace[*k].as_str().unwrap().to_string()))
        .collect();
    expected.sort();
    assert_eq!(found, expected);
}
