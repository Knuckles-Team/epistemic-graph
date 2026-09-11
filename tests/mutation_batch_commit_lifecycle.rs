//! Characterization tests for `apply_mutation_batch_in_wtx`
//! (`src/redb_store.rs`) — the durable `MutationBatch` commit kernel every
//! graph mutation routes through.
//!
//! These tests pin OBSERVED behaviour of the real served `dispatch` surface
//! for every request that routes through that commit kernel:
//! `apply_mutation_batch_in_wtx` is private to `redb_store.rs`, so it cannot
//! be unit-tested directly from an external integration-test crate — this
//! file exercises it black-box, the same pattern already used by
//! `tests/edge_pagination.rs` / `tests/adopt_workitem_metadata_cas_lifecycle.rs`.
//! Covers: the CreateGraph/AddNode/AddEdge roundtrip, ClearGraph (edges
//! removed, graph survives), DeleteGraph (subsequent writes fail),
//! idempotent replay of an identical signed envelope, and AddEdge against a
//! missing source node.
//!
//! Filename note: the file lives directly under `tests/` (not
//! `tests/characterization/`) because Cargo's test-target auto-discovery only
//! picks up `tests/*.rs` files, not files in a subdirectory — a file placed
//! under `tests/characterization/` would silently never run.
//!
//! Requires `security` (real signed-envelope dispatch path) and `redb`
//! (durable persistence, without which the gateway rejects every mutation) —
//! see `tests/edge_pagination.rs` for the identical precedent.
#![cfg(all(feature = "server", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::protocol::{GraphType, Method, Response};

const SECRET: &str = "cx-eg-05-mutation-batch-secret";

fn state() -> test_support::SharedState {
    test_support::durable_state(SECRET, common::current_isolation())
}

fn req(id: u64, graph: &str, method: Method) -> epistemic_graph::protocol::Request {
    test_support::request(SECRET, id, graph, method)
}

async fn dispatch(
    state: &test_support::SharedState,
    request: epistemic_graph::protocol::Request,
) -> Response {
    test_support::dispatch(state, request).await
}

async fn create_graph(state: &test_support::SharedState, id: u64, name: &str) -> Response {
    test_support::dispatch(
        state,
        test_support::request(
            SECRET,
            id,
            name,
            Method::CreateGraph {
                graph_name: name.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    )
    .await
}

async fn add_node(
    state: &test_support::SharedState,
    id: u64,
    graph: &str,
    node_id: &str,
) -> Response {
    test_support::dispatch(
        state,
        test_support::request(
            SECRET,
            id,
            graph,
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: test_support::json_bytes(serde_json::json!({"type": "Doc"})),
            },
        ),
    )
    .await
}

/// Exercises: `staged_state == None` (native/else branch), the lifecycle
/// `CreateGraph` arm of the outer match, the generic `apply_method_rows`
/// catch-all arm (`AddNode`/`AddEdge` have no bespoke arm), and the final
/// commit block (records/idempotency/version/fence/lifecycle-head/outbox/
/// graph-meta writes all in one write transaction).
#[tokio::test]
async fn t01_create_graph_add_node_add_edge_roundtrip() {
    let state = state();
    let create = create_graph(&state, 1, "cx05-g1").await;
    assert!(create.error.is_none(), "CreateGraph: {:?}", create.error);

    let n1 = add_node(&state, 2, "cx05-g1", "a").await;
    assert!(n1.error.is_none(), "AddNode a: {:?}", n1.error);
    let n2 = add_node(&state, 3, "cx05-g1", "b").await;
    assert!(n2.error.is_none(), "AddNode b: {:?}", n2.error);

    let edge = Box::pin(dispatch(
        &state,
        req(
            4,
            "cx05-g1",
            Method::AddEdge {
                source_id: "a".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: test_support::edge_properties("t"),
            },
        ),
    ))
    .await;
    assert!(edge.error.is_none(), "AddEdge: {:?}", edge.error);

    let dump = Box::pin(dispatch(&state, req(5, "cx05-g1", Method::GetEdges))).await;
    let rows = test_support::edge_rows(&dump);
    assert_eq!(rows.len(), 1, "expected exactly the one committed edge");
    assert_eq!(rows[0].0, "a");
    assert_eq!(rows[0].1, "b");
}

/// Exercises: the native-match `ClearGraph` arm (clears node/edge/ledger rows
/// via `clear_graph_rows` + resource/lane/capacity-lease clears) and the
/// `clears_semantic` branch right after the big match.
#[tokio::test]
async fn t02_clear_graph_removes_edges_but_graph_survives() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g2").await.error.is_none());
    assert!(add_node(&state, 2, "cx05-g2", "a").await.error.is_none());
    assert!(add_node(&state, 3, "cx05-g2", "b").await.error.is_none());
    let edge = Box::pin(dispatch(
        &state,
        req(
            4,
            "cx05-g2",
            Method::AddEdge {
                source_id: "a".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ),
    ))
    .await;
    assert!(edge.error.is_none());

    let clear = Box::pin(dispatch(&state, req(5, "cx05-g2", Method::ClearGraph))).await;
    assert!(clear.error.is_none(), "ClearGraph: {:?}", clear.error);

    let dump = Box::pin(dispatch(&state, req(6, "cx05-g2", Method::GetEdges))).await;
    let rows = test_support::edge_rows(&dump);
    assert_eq!(rows.len(), 0, "ClearGraph must remove every edge row");

    // The graph itself must still exist: a post-clear AddNode must succeed.
    let n = add_node(&state, 7, "cx05-g2", "c").await;
    assert!(
        n.error.is_none(),
        "graph must still be usable after ClearGraph: {:?}",
        n.error
    );
}

/// Exercises: the lifecycle `DeleteGraph` arm (`matches!(lifecycle, Some((false, _, _)))`),
/// `clear_change_material_rows` + `clear_mutation_authority_rows`, and the
/// `GRAPH_META` removal in the final commit block.
#[tokio::test]
async fn t03_delete_graph_then_add_node_fails() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g3").await.error.is_none());
    assert!(add_node(&state, 2, "cx05-g3", "a").await.error.is_none());

    let delete = Box::pin(dispatch(
        &state,
        req(
            3,
            "cx05-g3",
            Method::DeleteGraph {
                graph_name: "cx05-g3".to_string(),
            },
        ),
    ))
    .await;
    assert!(delete.error.is_none(), "DeleteGraph: {:?}", delete.error);

    let n = add_node(&state, 4, "cx05-g3", "b").await;
    // OBSERVED (pinned, not asserted as "correct"): AddNode against a deleted
    // graph is rejected. The exact error text is intentionally NOT pinned
    // byte-for-byte here (it originates above `apply_mutation_batch_in_wtx`,
    // in the registry/dispatch layer) -- only that it fails.
    assert!(
        n.error.is_some(),
        "AddNode against a deleted graph must fail, got: {:?}",
        n.result
    );
}

/// Exercises the idempotency-replay block: dispatching the EXACT same signed
/// envelope (same nonce / idempotency key) twice must not double-apply the
/// mutation.
///
/// The replayed envelope never reaches `apply_mutation_batch_in_wtx`'s own
/// idempotency-key lookup: a duplicated ATTEMPT is refused as a consumed nonce
/// before that.
///
/// WHICH LAYER refuses it moved, deliberately, and this test moved with it. It
/// used to pin `auth.rs`'s transport replay ledger verbatim ("nonce already used
/// (replay rejected)"). That ledger is now explicitly read-only protection for
/// NON-mutating requests (`auth.rs`: "a per-node transport replay ledger would
/// reject legitimate retries before the authoritative scope ledger can resolve
/// operation replay"), so a mutation's duplicated attempt is now refused by the
/// authoritative scope ledger, by its code name `REPLAY_NONCE_CONSUMED`. The
/// code, not the English sentence, is what this pins: the rest of that
/// diagnostic names the idempotency key, which embeds the process id and is
/// therefore not a stable byte string. Same move pinned for `dispatch_inner`'s
/// replayed `CreateGraph` in `tests/protocol_method_routing.rs`
/// (`t05_replayed_identical_signed_envelope_rejected_by_nonce_ledger`).
#[tokio::test]
async fn t04_replayed_add_node_request_is_not_double_applied() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g4").await.error.is_none());

    let add_request = req(
        2,
        "cx05-g4",
        Method::AddNode {
            node_id: "a".to_string(),
            properties_msgpack: test_support::json_bytes(serde_json::json!({"type": "Doc"})),
        },
    );
    let first = Box::pin(dispatch(&state, add_request.clone())).await;
    assert!(first.error.is_none(), "first AddNode: {:?}", first.error);
    let second = Box::pin(dispatch(&state, add_request.clone())).await;
    let refusal = second
        .error
        .as_deref()
        .expect("replaying the identical signed AddNode envelope must be refused");
    assert!(
        refusal.contains("REPLAY_NONCE_CONSUMED"),
        "replaying the identical signed AddNode envelope must be refused by the \
         authoritative scope ledger as a consumed nonce, got: {refusal}"
    );

    // Regardless of how the replay was handled, the node must exist exactly
    // once: add an edge FROM it and confirm the edge is readable (a
    // duplicate-inserted "a" node would not itself be observable via
    // GetEdges, so this also indirectly confirms no error occurred that
    // corrupted graph state).
    assert!(add_node(&state, 3, "cx05-g4", "b").await.error.is_none());
    let edge = Box::pin(dispatch(
        &state,
        req(
            4,
            "cx05-g4",
            Method::AddEdge {
                source_id: "a".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ),
    ))
    .await;
    assert!(
        edge.error.is_none(),
        "AddEdge after replay: {:?}",
        edge.error
    );
}

/// Exercises the generic `apply_method_rows` catch-all arm's error path
/// bubbling all the way back through `apply_mutation_batch_in_wtx` to the
/// caller: an edge whose source node does not exist.
#[tokio::test]
async fn t05_add_edge_with_missing_source_node_fails() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g5").await.error.is_none());
    assert!(add_node(&state, 2, "cx05-g5", "b").await.error.is_none());

    let edge = Box::pin(dispatch(
        &state,
        req(
            3,
            "cx05-g5",
            Method::AddEdge {
                source_id: "missing".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ),
    ))
    .await;
    assert!(
        edge.error.is_some(),
        "AddEdge with a missing source node must fail, got: {:?}",
        edge.result
    );
}
