//! EH-558 — a sealed record row is create-only on the SERVED write path.
//!
//! A sealed record class (`eg_types::sealed_record`) is stored as an ordinary graph
//! node. Every request here goes through the real `dispatch` surface into the durable
//! generic row applier, which refuses any generic write that would change or remove a
//! stored sealed row. Creation, identical rewrites and create-if-absent retries still
//! succeed, and ordinary nodes keep their normal overwrite semantics.
// The durable gateway needs `redb` (pulled in by `security`), and a dispatched request
// needs the secure request context `security` provides.
#![cfg(all(feature = "server", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use serde_json::json;

use epistemic_graph::protocol::{Method, Response, ResultPayload};
use epistemic_graph::server::dispatch;

const SECRET: &str = "sealed-record-guard-secret";

struct Served {
    state: test_support::SharedState,
    request: u64,
}

impl Served {
    fn new() -> Self {
        Self {
            state: test_support::durable_state(SECRET, common::current_isolation()),
            request: 0,
        }
    }

    async fn send(&mut self, method: Method) -> Response {
        self.request += 1;
        let request = test_support::commons_request(SECRET, self.request, method);
        Box::pin(dispatch(&self.state, request)).await
    }

    async fn props(&mut self, node_id: &str) -> Option<serde_json::Value> {
        let response = self
            .send(Method::GetNodeProperties {
                node_id: node_id.to_string(),
            })
            .await;
        match response.result {
            Some(ResultPayload::Raw(bytes)) => Some(rmp_serde::from_slice(&bytes).unwrap()),
            _ => None,
        }
    }
}

fn bytes(value: serde_json::Value) -> Vec<u8> {
    test_support::json_bytes(value)
}

fn snapshot(record: &str) -> serde_json::Value {
    json!({"type": "AnalysisSnapshot", "analysisDigest": "sha256:ab", "record": record})
}

fn add_node(node_id: &str, value: serde_json::Value) -> Method {
    Method::AddNode {
        node_id: node_id.to_string(),
        properties_msgpack: bytes(value),
    }
}

fn batch(operations: serde_json::Value) -> Method {
    Method::BatchUpdate {
        operations_msgpack: bytes(operations),
    }
}

fn refused(response: &Response) -> bool {
    response
        .error
        .as_deref()
        .is_some_and(|error| error.contains("create-only"))
}

/// Creating a sealed record, rewriting it identically and retrying its
/// create-if-absent all succeed; the row keeps its content.
#[tokio::test]
async fn sealed_records_can_be_created_and_idempotently_rewritten() {
    let mut served = Served::new();
    let create = served
        .send(Method::CreateNodeIfAbsent {
            node_id: "snap".to_string(),
            properties_msgpack: bytes(snapshot("{}")),
        })
        .await;
    assert!(create.error.is_none(), "create: {:?}", create.error);
    let retry = served
        .send(Method::CreateNodeIfAbsent {
            node_id: "snap".to_string(),
            properties_msgpack: bytes(snapshot("{\"other\":1}")),
        })
        .await;
    assert!(
        retry.error.is_none(),
        "create-if-absent retry: {:?}",
        retry.error
    );
    let same = served.send(add_node("snap", snapshot("{}"))).await;
    assert!(same.error.is_none(), "identical rewrite: {:?}", same.error);
    assert_eq!(served.props("snap").await, Some(snapshot("{}")));
}

/// Every generic write that would change or remove the stored record is refused,
/// and the stored record is unchanged afterwards.
#[tokio::test]
async fn generic_writes_cannot_change_or_remove_a_sealed_record() {
    let mut served = Served::new();
    let create = served.send(add_node("snap", snapshot("{}"))).await;
    assert!(create.error.is_none(), "create: {:?}", create.error);

    let attempts = [
        add_node("snap", snapshot("{\"forged\":1}")),
        Method::CompareAndSetNodeFields {
            node_id: "snap".to_string(),
            conditions_msgpack: bytes(json!({})),
            updates_msgpack: bytes(json!({"record": "{\"forged\":1}"})),
        },
        Method::RemoveNode {
            node_id: "snap".to_string(),
        },
        batch(json!([{"op": "upsert_node", "id": "snap", "properties": {"note": "x"}}])),
        batch(json!([{"op": "remove_node", "id": "snap"}])),
    ];
    for attempt in attempts {
        let label = format!("{attempt:?}");
        let response = served.send(attempt).await;
        assert!(refused(&response), "{label}: {:?}", response.error);
    }
    assert_eq!(served.props("snap").await, Some(snapshot("{}")));
}

/// The guard is scoped to sealed classes: an ordinary node is still overwritten
/// and removed, and a decay sweep over a graph holding a sealed record succeeds
/// without touching it.
#[tokio::test]
async fn ordinary_nodes_and_maintenance_are_unaffected() {
    let mut served = Served::new();
    for method in [
        add_node("snap", snapshot("{}")),
        add_node("doc", json!({"type": "Doc", "text": "first"})),
        add_node("doc", json!({"type": "Doc", "text": "second"})),
    ] {
        let response = served.send(method).await;
        assert!(response.error.is_none(), "{:?}", response.error);
    }
    assert_eq!(
        served.props("doc").await,
        Some(json!({"type": "Doc", "text": "second"}))
    );
    let sweep = served
        .send(Method::DecaySweep {
            half_life_secs: 1.0,
            floor: 0.0,
            prune: false,
        })
        .await;
    assert!(sweep.error.is_none(), "decay sweep: {:?}", sweep.error);
    assert_eq!(served.props("snap").await, Some(snapshot("{}")));
    let remove = served
        .send(Method::RemoveNode {
            node_id: "doc".to_string(),
        })
        .await;
    assert!(
        remove.error.is_none(),
        "remove ordinary: {:?}",
        remove.error
    );
    assert_eq!(served.props("doc").await, None);
}
