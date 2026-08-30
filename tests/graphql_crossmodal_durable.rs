//! GraphQL cross-modal DURABLE-commit roundtrip (CONCEPT:EG-KG.query.facade-reconcile-hook).
//!
//! Proves the facade reconcile hook the `eg-graphql` crate left open (EG-383): a GraphQL
//! cross-modal `commitTransaction`, routed through the facade carrier
//! (`handlers::query.rs` `Method::GraphQl` → `handlers::txn::commit_graphql_cross_modal`),
//! lands its staged modalities in the redb durable tier via `commit_cross_modal_txn` — the
//! SAME committed machinery pgwire's `commit_txn_state` drives — so the write SURVIVES a
//! reopen of the persist dir. Before EG-419 the crate committed graph+vector in-memory
//! only, so nothing would be on disk.
//!
//! Driven through the REAL `dispatch` shell over an in-process `ServerState` backed by a
//! `RedbBackend` (persistence present), exactly as a client: begin → stage (sparqlUpdate +
//! stageEmbedding) → commit, each a separate request sharing the process-wide
//! `CrossModalTxnRegistry`. Durability is asserted by SHUTTING DOWN the backend (releasing
//! the per-file lock) and REOPENING a fresh `RedbBackend` on the same dir, then reading the
//! committed node back from the durable tier (`read_node`).
//!
//! Gated on `graphql` (the seam) + `redb` (a durable tier to reopen); runs under `--features full`.

#![cfg(all(feature = "graphql", feature = "redb"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use std::sync::Arc;

use epistemic_graph::durability::DurabilityPolicy;
use epistemic_graph::protocol::{GraphType, Method, Response, ResultPayload};
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;

const SECRET: &str = "gql-crossmodal-durable-secret";
const GRAPH: &str = "gqlxmdurable"; // lowercase-alnum ⇒ sanitize() is identity ⇒ fname == GRAPH
const NODE: &str = "<http://ex/n1>"; // lower_triples subject id for <http://ex/n1>

/// A fully-featured `ServerState` backed by the given redb persistence tier.
fn state_with(backend: Arc<dyn PersistenceBackend>, dir: String) -> test_support::SharedState {
    let isolation = common::current_isolation();
    test_support::state_with(SECRET, isolation, Some(dir), Some(backend))
}

async fn gql(state: &test_support::SharedState, id: u64, query: &str) -> serde_json::Value {
    let r: Response = Box::pin(test_support::dispatch(
        state,
        test_support::request(
            SECRET,
            id,
            GRAPH,
            Method::GraphQl {
                query: query.to_string(),
                variables: None,
            },
        ),
    ))
    .await;
    assert!(r.error.is_none(), "graphql op {id} failed: {:?}", r.error);
    match r.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
        other => panic!("expected Raw graphql result, got {other:?}"),
    }
}

#[tokio::test]
async fn graphql_cross_modal_commit_survives_reopen() {
    // A unique, self-cleaning persist dir under the system temp dir (no tempfile dep).
    let dir = test_support::fresh_dir("eg-gqlxm");
    let dir_s = dir.to_string_lossy().to_string();

    // ── Phase 1: open the durable tier, create the graph, run the cross-modal txn ──
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192).unwrap());
    let state = state_with(backend.clone(), dir_s.clone());

    // Create the graph (dispatch registers it in BOTH the registry and the durable tier).
    let cr = Box::pin(test_support::dispatch(
        &state,
        test_support::request(
            SECRET,
            1,
            GRAPH,
            Method::CreateGraph {
                graph_name: GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await;
    assert!(cr.error.is_none(), "CreateGraph failed: {:?}", cr.error);

    // begin → mint a txnId.
    let begun = gql(&state, 2, "mutation { beginTransaction { txnId } }").await;
    let txn_id = begun["data"]["beginTransaction"]["txnId"]
        .as_str()
        .expect("txnId")
        .to_string();

    // Stage a graph node (SPARQL INSERT DATA — a type triple, no string literals to escape)
    // + its embedding, in the SAME txn.
    let staged = gql(
        &state,
        3,
        &format!(
            "mutation {{ sparqlUpdate(txnId: \"{txn_id}\", update: \"INSERT DATA {{ \
             <http://ex/n1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Widget> }}\") {{ staged }} }}"
        ),
    )
    .await;
    assert_eq!(
        staged["data"]["sparqlUpdate"]["staged"],
        serde_json::json!(true)
    );

    let staged_vec = gql(
        &state,
        4,
        &format!(
            "mutation {{ stageEmbedding(txnId: \"{txn_id}\", id: \"{NODE}\", vector: [0.1, 0.2, 0.3]) {{ staged }} }}"
        ),
    )
    .await;
    assert_eq!(
        staged_vec["data"]["stageEmbedding"]["staged"],
        serde_json::json!(true)
    );

    // Commit — this is the DURABLE path (EG-419).
    let committed = gql(
        &state,
        5,
        &format!("mutation {{ commitTransaction(txnId: \"{txn_id}\") {{ committed }} }}"),
    )
    .await;
    assert_eq!(
        committed["data"]["commitTransaction"]["committed"],
        serde_json::json!(true),
        "GraphQL commitTransaction should report a durable commit"
    );

    // ── Phase 2: shut the tier down (release the file lock) + REOPEN a fresh backend ──
    // `shutdown()` stops the writer thread but does not close the underlying redb
    // `Database` handle -- that only happens when the LAST owning value is dropped, and
    // redb keeps its advisory per-file lock until then (`state` holds its own `Arc`
    // clone of `backend`, so both must go). Reopening the SAME file IN-PROCESS then
    // races that drop's async teardown actually releasing the lock (no `JoinHandle` to
    // await here), so bound it with a short retry rather than a flat sleep -- identical
    // rationale to `redb_backend::tests::delete_then_recreate_same_name_keeps_new_writes`
    // and `advanced_crossmodal_roundtrip.rs::encryption_at_rest_wrong_key_fails_eg394`.
    backend.shutdown();
    drop(backend);
    drop(state);

    let reopened: Arc<dyn PersistenceBackend> = {
        let mut attempt = 0;
        loop {
            match RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192) {
                Ok(backend) => break Arc::new(backend),
                Err(error) if attempt < 100 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    let _ = error;
                }
                Err(error) => panic!("reopen durable tier: {error:?}"),
            }
        }
    };

    // The committed node must be on disk — read it back from the durable tier.
    let blob = reopened
        .read_node(GRAPH, NODE)
        .await
        .expect("read_node ok")
        .expect("committed node must be durable (survives reopen)");
    let props: serde_json::Map<String, serde_json::Value> =
        rmp_serde::from_slice(&blob).expect("decode durable node blob");
    assert_eq!(
        props.get("type").and_then(|v| v.as_str()),
        Some("http://ex/Widget"),
        "the durable node must carry the committed rdf:type property"
    );

    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
