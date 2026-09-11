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

use epistemic_graph::acl::RequestContextClaims;
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};

const SECRET: &str = "gql-crossmodal-durable-secret";
const GRAPH: &str = "gqlxmdurable"; // lowercase-alnum ⇒ sanitize() is identity ⇒ fname == GRAPH
const NODE: &str = "<http://ex/n1>"; // lower_triples subject id for <http://ex/n1>

/// A fully-featured `ServerState` backed by the given redb persistence tier.
fn state_with(backend: test_support::SharedPersistence, dir: String) -> test_support::SharedState {
    let isolation = common::current_isolation();
    test_support::state_with(SECRET, isolation, Some(dir), Some(backend))
}

fn signed_graphql_request(id: u64, query: &str, nonce: &str, idempotency_key: &str) -> Request {
    common::configure_authority();
    let context = RequestContextClaims {
        principal: common::TEST_AGENT.to_string(),
        tenant: "integration-test-tenant".to_string(),
        audience: "epistemic-graph-integration-tests".to_string(),
        agent_id: common::TEST_AGENT.to_string(),
        roles: Vec::new(),
        scopes: vec!["*".to_string()],
        policy_version: "integration-test-policy-v1".to_string(),
        delegation: Vec::new(),
        node: None,
        priority: None,
    };
    let mut request = Request {
        id,
        graph: GRAPH.to_string(),
        auth_token: String::new(),
        agent_id: Some(common::TEST_AGENT.to_string()),
        method: Method::GraphQl {
            query: query.to_string(),
            variables: None,
        },
    };
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs();
    request.auth_token = compute_verified_envelope_token(
        SECRET,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp,
            nonce,
            idempotency_key,
        },
    );
    request
}

async fn gql_request(state: &test_support::SharedState, request: Request) -> serde_json::Value {
    let r = gql_response(state, request).await;
    assert!(
        r.error.is_none(),
        "graphql op {} failed: {:?}",
        r.id,
        r.error
    );
    match r.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
        other => panic!("expected Raw graphql result, got {other:?}"),
    }
}

async fn gql_response(state: &test_support::SharedState, request: Request) -> Response {
    let id = request.id;
    let r: Response = Box::pin(test_support::dispatch(state, request)).await;
    assert_eq!(r.id, id, "graphql response id changed");
    r
}

async fn gql(state: &test_support::SharedState, id: u64, query: &str) -> serde_json::Value {
    gql_request(
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
    )
    .await
}

#[tokio::test]
async fn graphql_cross_modal_commit_survives_reopen() {
    // A unique, self-cleaning persist dir under the system temp dir (no tempfile dep).
    let dir = test_support::fresh_dir("eg-gqlxm");
    let dir_s = dir.to_string_lossy().to_string();

    // A multi-op cross-modal Commit seals its recovery plan, so durability
    // REQUIRES a key: `crypto::resolve_txn_recovery_key` reads
    // `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` first and falls back to
    // `EPISTEMIC_GRAPH_ENCRYPTION_KEY`, and with neither set `commitTransaction`
    // correctly refuses with "transaction durability requires
    // EPISTEMIC_GRAPH_ENCRYPTION_KEY to be configured". This binary provisioned
    // neither, so the refusal -- not the reopen behaviour this test names -- is
    // what it measured. `provision_encryption_key_once` is the shared helper for
    // exactly this (it is `Once`-guarded, so it is safe for both tests in this
    // binary), and it must run BEFORE the backend opens: the cipher is resolved
    // once at open.
    test_support::provision_encryption_key_once("gql-crossmodal-durable-encryption-key");

    // ── Phase 1: open the durable tier, create the graph, run the cross-modal txn ──
    let backend = test_support::open_redb_backend(dir_s.clone()).unwrap();
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

    // Commit — this is the DURABLE path (EG-419). Keep the signed body and
    // stable key while changing only the verified nonce for the replay attempt.
    let commit_query =
        format!("mutation {{ commitTransaction(txnId: \"{txn_id}\") {{ committed }} }}");
    let committed = gql_request(
        &state,
        signed_graphql_request(
            5,
            &commit_query,
            "graphql-crossmodal-commit-nonce-1",
            "graphql-crossmodal-commit-key",
        ),
    )
    .await;
    assert_eq!(
        committed["data"]["commitTransaction"]["committed"],
        serde_json::json!(true),
        "GraphQL commitTransaction should report a durable commit"
    );
    let exact_commit = gql_response(
        &state,
        signed_graphql_request(
            5_1,
            &commit_query,
            "graphql-crossmodal-commit-nonce-1",
            "graphql-crossmodal-commit-key",
        ),
    )
    .await;
    assert!(
        exact_commit
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact GraphQL commit retry must be rejected by the kernel: {:?}",
        exact_commit.error
    );
    let core = state
        .read()
        .await
        .registry
        .get(GRAPH)
        .expect("graph remains registered")
        .core
        .clone();
    let committed_version = core.version();

    // ── Phase 2: replace the persistence handle, then replay the exact commit ──
    // `shutdown()` stops the writer thread but does not close the underlying redb
    // `Database` handle -- that only happens when the LAST owning value is dropped, and
    // redb keeps its advisory per-file lock until then. Clear the state's old handle
    // before reopening the SAME file IN-PROCESS, and use the shared bounded retry rather
    // than a flat sleep -- identical
    // rationale to `redb_backend::tests::delete_then_recreate_same_name_keeps_new_writes`
    // and `advanced_crossmodal_roundtrip.rs::encryption_at_rest_wrong_key_fails_eg394`.
    backend.shutdown();
    state.write().await.persistence = None;
    drop(backend);

    let reopened = test_support::reopen_with_bounded_retry(
        || test_support::open_redb_backend(dir_s.clone()),
        "reopen durable tier",
    )
    .await;
    state.write().await.persistence = Some(reopened.clone());

    // The parent receipt is terminal and authoritative. The retry must return its
    // stored result without taking a second staged transaction or advancing RAM.
    let replayed = gql_request(
        &state,
        signed_graphql_request(
            6,
            &commit_query,
            "graphql-crossmodal-commit-nonce-2",
            "graphql-crossmodal-commit-key",
        ),
    )
    .await;
    assert_eq!(
        replayed["data"]["commitTransaction"]["committed"],
        serde_json::json!(true),
        "replaying the same signed GraphQL commit body must return the terminal result"
    );
    assert_eq!(
        core.version(),
        committed_version,
        "a terminal GraphQL replay must not apply the cross-modal child twice"
    );

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
    state.write().await.persistence = None;
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Native GraphQL begin/stage/rollback requests use the same durable lifecycle
/// authority as `BeginTxn`/`Txn*`/`Rollback`: an exact nonce is rejected, a
/// fresh nonce with the same key replays while the volatile handle is live, and
/// a changed body under that key conflicts before the registry is touched a
/// second time. Aliases and two-root staging exercise parsed field identity;
/// a terminal rollback refuses a stale success after its handle is gone.
#[tokio::test]
async fn graphql_native_staging_consumes_nonce_and_replays_by_key() {
    let dir = test_support::fresh_dir("eg-gqlxm-lifecycle");
    let dir_s = dir.to_string_lossy().to_string();
    let backend = test_support::open_redb_backend(dir_s.clone()).unwrap();
    let state = state_with(backend.clone(), dir_s);

    let created = Box::pin(test_support::dispatch(
        &state,
        test_support::request(
            SECRET,
            10,
            GRAPH,
            Method::CreateGraph {
                graph_name: GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await;
    assert!(
        created.error.is_none(),
        "CreateGraph failed: {:?}",
        created.error
    );

    let begin_query = "mutation { created: beginTransaction { txnId } }";
    let begin_key = "graphql-native-begin-key";
    let first_begin = gql_request(
        &state,
        signed_graphql_request(11, begin_query, "graphql-native-begin-nonce-1", begin_key),
    )
    .await;
    let txn_id = first_begin["data"]["created"]["txnId"]
        .as_str()
        .expect("begin txnId")
        .to_string();

    let exact_begin = gql_response(
        &state,
        signed_graphql_request(12, begin_query, "graphql-native-begin-nonce-1", begin_key),
    )
    .await;
    assert!(
        exact_begin
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact GraphQL begin replay must be rejected by the kernel: {:?}",
        exact_begin.error
    );

    let fresh_begin = gql_request(
        &state,
        signed_graphql_request(13, begin_query, "graphql-native-begin-nonce-2", begin_key),
    )
    .await;
    assert_eq!(
        fresh_begin["data"]["created"]["txnId"],
        serde_json::Value::String(txn_id.clone()),
        "fresh-nonce begin retry must replay the original registry handle"
    );

    let second_begin_query = "mutation { second: beginTransaction { txnId } }";
    let second_begin = gql_request(
        &state,
        signed_graphql_request(
            14,
            second_begin_query,
            "graphql-native-begin-nonce-3",
            "graphql-native-begin-key-2",
        ),
    )
    .await;
    let second_txn_id = second_begin["data"]["second"]["txnId"]
        .as_str()
        .expect("second begin txnId")
        .to_string();

    // The first root deliberately uses `txnId` as an ALIAS and places a string
    // containing `txnId` before the real `txnId` argument.  A raw substring
    // search therefore cannot infer the handle; the parsed field identity must
    // collect both distinct staged handles.
    let stage_query = format!(
        "mutation {{ txnId: stageEmbedding(id: \"txnId-marker\", txnId: \"{txn_id}\", vector: [1.0, 0.0]) {{ staged }} second: stageEmbedding(id: \"native-2\", txnId: \"{second_txn_id}\", vector: [0.0, 1.0]) {{ staged }} }}"
    );
    let stage_key = "graphql-native-stage-key";
    let first_stage = gql_request(
        &state,
        signed_graphql_request(15, &stage_query, "graphql-native-stage-nonce-1", stage_key),
    )
    .await;
    assert_eq!(
        first_stage["data"]["txnId"]["staged"],
        serde_json::json!(true)
    );
    assert_eq!(
        first_stage["data"]["second"]["staged"],
        serde_json::json!(true),
        "multi-root staging must execute both parsed roots"
    );

    let exact_stage = gql_response(
        &state,
        signed_graphql_request(16, &stage_query, "graphql-native-stage-nonce-1", stage_key),
    )
    .await;
    assert!(
        exact_stage
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact GraphQL stage replay must be rejected by the kernel: {:?}",
        exact_stage.error
    );

    let fresh_stage = gql_request(
        &state,
        signed_graphql_request(17, &stage_query, "graphql-native-stage-nonce-2", stage_key),
    )
    .await;
    assert_eq!(
        fresh_stage, first_stage,
        "fresh stage retry must replay its result"
    );

    let changed_stage_query = format!(
        "mutation {{ txnId: stageEmbedding(id: \"txnId-marker\", txnId: \"{txn_id}\", vector: [0.0, 1.0]) {{ staged }} second: stageEmbedding(id: \"native-2\", txnId: \"{second_txn_id}\", vector: [1.0, 0.0]) {{ staged }} }}"
    );
    let changed_stage = gql_response(
        &state,
        signed_graphql_request(
            18,
            &changed_stage_query,
            "graphql-native-stage-nonce-3",
            stage_key,
        ),
    )
    .await;
    assert!(
        changed_stage
            .error
            .as_deref()
            .is_some_and(|error| error.contains("IDEMPOTENCY_CONFLICT")),
        "changed GraphQL stage body must conflict under the same key: {:?}",
        changed_stage.error
    );

    // Retire only the second handle.  A fresh-key replay of the original
    // multi-root stage must now refuse the terminal receipt because one of its
    // two parsed handles is gone, while the first handle remains live for the
    // explicit cleanup below.
    let rollback_query = format!(
        "mutation {{ secondRemoved: rollbackTransaction(txnId: \"{second_txn_id}\") {{ rolledBack }} }}"
    );
    let rollback_key = "graphql-native-rollback-key";
    let first_rollback = gql_request(
        &state,
        signed_graphql_request(
            19,
            &rollback_query,
            "graphql-native-rollback-nonce-1",
            rollback_key,
        ),
    )
    .await;
    assert_eq!(
        first_rollback["data"]["secondRemoved"]["rolledBack"],
        serde_json::json!(true),
        "rollback must retire the second parsed handle"
    );

    let stale_stage = gql_response(
        &state,
        signed_graphql_request(20, &stage_query, "graphql-native-stage-nonce-4", stage_key),
    )
    .await;
    assert!(
        stale_stage
            .error
            .as_deref()
            .is_some_and(|error| error.contains("volatile staging state is unavailable")),
        "multi-root stage replay must refuse when only its second handle was retired: {:?}",
        stale_stage.error
    );

    let exact_rollback = gql_response(
        &state,
        signed_graphql_request(
            21,
            &rollback_query,
            "graphql-native-rollback-nonce-1",
            rollback_key,
        ),
    )
    .await;
    assert!(
        exact_rollback
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact GraphQL rollback replay must be rejected by the kernel: {:?}",
        exact_rollback.error
    );
    let fresh_rollback = gql_response(
        &state,
        signed_graphql_request(
            22,
            &rollback_query,
            "graphql-native-rollback-nonce-2",
            rollback_key,
        ),
    )
    .await;
    assert!(
        fresh_rollback
            .error
            .as_deref()
            .is_some_and(|error| error.contains("volatile staging state is unavailable")),
        "a rollback receipt cannot replay success after its volatile handle was removed: {:?}",
        fresh_rollback.error
    );

    let first_cleanup = gql_request(
        &state,
        signed_graphql_request(
            23,
            &format!(
                "mutation {{ firstRemoved: rollbackTransaction(txnId: \"{txn_id}\") {{ rolledBack }} }}"
            ),
            "graphql-native-rollback-first-nonce-1",
            "graphql-native-rollback-first-key",
        ),
    )
    .await;
    assert_eq!(
        first_cleanup["data"]["firstRemoved"]["rolledBack"],
        serde_json::json!(true),
        "the first handle must remain live until explicit cleanup"
    );

    backend.shutdown();
    state.write().await.persistence = None;
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}
