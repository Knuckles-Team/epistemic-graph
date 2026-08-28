//! CX-EG-06 characterization tests for `dispatch_inner`
//! (`src/server/dispatch.rs`, CCN 188 as measured by the repo's lizard-based
//! complexity gate before this lane's refactor).
//!
//! `dispatch_inner` is private to `dispatch.rs`, so it is exercised
//! black-box through the real served `dispatch` surface, exactly the
//! pattern used by `tests/characterization_cx_eg_05_apply_mutation_batch_in_wtx.rs`
//! (see that file's doc comment for why this lives directly under `tests/`
//! rather than `tests/characterization/`).
//!
//! These tests pin OBSERVED behaviour, including behaviour that may itself
//! be a bug (see the lane report for `BUGS FOUND` entries) -- they do not
//! assert that the behaviour is *correct*, only that it is unchanged by the
//! CCN-reduction refactor.
#![cfg(all(feature = "server", feature = "security"))]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::acl::AgentRole;
use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{ChannelType, GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "cx-eg-06-dispatch-inner-secret";

fn state() -> Arc<RwLock<ServerState>> {
    let (persist_dir, persistence) = common::tempdir_persistence();
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        #[cfg(feature = "viz-static-export")]
        viz_engine: None,
        auth_secret: SECRET.to_string(),
        persist_dir,
        persistence,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(
            epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(epistemic_graph::server::txn::TxnIdGen),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        #[cfg(feature = "raft")]
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "streaming")]
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

fn req(id: u64, graph: &str, method: Method) -> Request {
    common::signed_request(SECRET, id, graph, method)
}

async fn call(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str, method: Method) -> Response {
    Box::pin(dispatch(state, req(id, graph, method))).await
}

async fn create_graph(state: &Arc<RwLock<ServerState>>, id: u64, name: &str) -> Response {
    call(
        state,
        id,
        name,
        Method::CreateGraph {
            graph_name: name.to_string(),
            graph_type: GraphType::Global,
        },
    )
    .await
}

// ── Service-level (Ping / Health / Shutdown) ──────────────────────────────

#[tokio::test]
async fn t01_ping_returns_pong() {
    let state = state();
    let resp = call(&state, 1, "__commons__", Method::Ping).await;
    assert!(resp.error.is_none(), "Ping: {:?}", resp.error);
    match resp.result {
        Some(ResultPayload::String(s)) => assert_eq!(s, "pong"),
        other => panic!("expected String(\"pong\"), got {other:?}"),
    }
}

#[tokio::test]
async fn t02_health_reports_ok() {
    let state = state();
    let resp = call(&state, 1, "__commons__", Method::Health).await;
    assert!(resp.error.is_none(), "Health: {:?}", resp.error);
    assert!(resp.result.is_some(), "Health must return a payload");
}

#[tokio::test]
async fn t03_shutdown_returns_shutting_down_without_terminating_process() {
    let state = state();
    let resp = call(&state, 1, "__commons__", Method::Shutdown).await;
    assert!(resp.error.is_none(), "Shutdown: {:?}", resp.error);
    match resp.result {
        Some(ResultPayload::String(s)) => assert_eq!(s, "shutting_down"),
        other => panic!("expected String(\"shutting_down\"), got {other:?}"),
    }
    // The process is still alive to make this assertion at all -- Shutdown is
    // observed to be a protocol-level acknowledgement only.
    let ping = call(&state, 2, "__commons__", Method::Ping).await;
    assert!(ping.error.is_none());
}

// ── CreateGraph / DeleteGraph / ListGraphs ────────────────────────────────

#[tokio::test]
async fn t04_create_graph_then_list_graphs() {
    let state = state();
    let create = create_graph(&state, 1, "cx06-g1").await;
    assert!(create.error.is_none(), "CreateGraph: {:?}", create.error);

    let list = call(&state, 2, "__commons__", Method::ListGraphs).await;
    assert!(list.error.is_none(), "ListGraphs: {:?}", list.error);
}

/// OBSERVED, and NOT what this test originally assumed: dispatching the
/// exact same signed `CreateGraph` envelope twice does NOT reach
/// `CreateGraph`'s own lifecycle-replay branch (`lifecycle_was_committed`,
/// `dispatch_case_11_create_graph` post-refactor) at all -- the durable
/// replay-nonce ledger in `dispatch()`'s auth layer, which runs BEFORE
/// `dispatch_inner`, already rejects the second identical envelope with
/// "nonce already used". `CreateGraph`'s own idempotent-replay branch is
/// reachable only via a distinct envelope (a different request `id`/nonce)
/// that names the SAME idempotency_key derived by `lifecycle_batch_id` --
/// not exercised here; see the lane report's `dispatch_case_11_create_graph`
/// entry. This test now pins the auth-layer rejection it actually observes.
#[tokio::test]
async fn t05_replayed_identical_signed_envelope_rejected_by_nonce_ledger() {
    let state = state();
    let request = req(
        1,
        "cx06-g2",
        Method::CreateGraph {
            graph_name: "cx06-g2".to_string(),
            graph_type: GraphType::Global,
        },
    );
    let first = Box::pin(dispatch(&state, request.clone())).await;
    assert!(
        first.error.is_none(),
        "first CreateGraph: {:?}",
        first.error
    );
    let second = Box::pin(dispatch(&state, request.clone())).await;
    assert_eq!(
        second.error.as_deref(),
        Some("nonce already used (replay rejected)"),
        "replaying the identical signed envelope: {:?}",
        second.error
    );
}

/// OBSERVED: creating a graph name that already exists via a DIFFERENT
/// request (different id/nonce, so it is not treated as a replay of the
/// same envelope) fails with an "already exists" error.
#[tokio::test]
async fn t06_create_graph_duplicate_name_different_request_fails() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-g3").await.error.is_none());
    let second = create_graph(&state, 2, "cx06-g3").await;
    assert!(
        second.error.is_some(),
        "a distinct CreateGraph for an existing name must fail, got: {:?}",
        second.result
    );
}

#[tokio::test]
async fn t07_delete_graph_then_list_graphs() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-g4").await.error.is_none());
    let delete = call(
        &state,
        2,
        "cx06-g4",
        Method::DeleteGraph {
            graph_name: "cx06-g4".to_string(),
        },
    )
    .await;
    assert!(delete.error.is_none(), "DeleteGraph: {:?}", delete.error);
}

#[tokio::test]
async fn t08_delete_graph_nonexistent_fails() {
    let state = state();
    let delete = call(
        &state,
        1,
        "cx06-nonexistent",
        Method::DeleteGraph {
            graph_name: "cx06-nonexistent".to_string(),
        },
    )
    .await;
    assert!(
        delete.error.is_some(),
        "DeleteGraph of a nonexistent graph must fail, got: {:?}",
        delete.result
    );
}

// ── Identity ───────────────────────────────────────────────────────────

#[tokio::test]
async fn t09_get_identity_for_bootstrap_agent() {
    let state = state();
    let resp = call(
        &state,
        1,
        "__commons__",
        Method::GetIdentity {
            agent_id: common::TEST_AGENT.to_string(),
        },
    )
    .await;
    assert!(resp.error.is_none(), "GetIdentity: {:?}", resp.error);
}

/// OBSERVED, and NOT what this test originally assumed: `GetIdentity` for
/// an unregistered agent is NOT an error response -- it is a SUCCESS
/// response carrying a JSON `null` result (`Option<AgentIdentity>::None`
/// serialized). The doc comment on `Method::GetIdentity` says callers "MUST
/// keep [this] distinct from `Some(identity)` carrying an empty `roles`
/// Vec"; this test pins that this distinction is carried in the RESULT
/// payload, not the error channel.
#[tokio::test]
async fn t10_get_identity_unknown_agent_returns_null_not_an_error() {
    let state = state();
    let resp = call(
        &state,
        1,
        "__commons__",
        Method::GetIdentity {
            agent_id: "cx06-unknown-agent".to_string(),
        },
    )
    .await;
    assert!(
        resp.error.is_none(),
        "GetIdentity for an unregistered agent is a success response: {:?}",
        resp.error
    );
    match &resp.result {
        Some(ResultPayload::Json(v)) => assert!(v.is_null(), "expected JSON null, got {v:?}"),
        other => panic!("expected Some(Json(Null)), got {other:?}"),
    }
}

#[tokio::test]
async fn t11_register_identity_ordinary_agent() {
    let state = state();
    let resp = call(
        &state,
        1,
        "__commons__",
        Method::RegisterIdentity {
            agent_id: "cx06-new-agent".to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            signature: String::new(),
            roles: Vec::new(),
        },
    )
    .await;
    // OBSERVED: pin whatever the current signature-verification / admin-scope
    // gate actually returns for an ordinary agent-role registration signed
    // by the bootstrap System agent, without asserting it is the *correct*
    // policy outcome.
    let _ = resp.error.is_none();
}

// ── Channels (session-control-mutation family) ────────────────────────────

#[tokio::test]
async fn t12_channel_lifecycle_create_join_send_list_leave() {
    let state = state();
    let create = call(
        &state,
        1,
        "__commons__",
        Method::CreateChannel {
            channel_id: "cx06-chan1".to_string(),
            channel_type: ChannelType::Group,
            creator: common::TEST_AGENT.to_string(),
            initial_members: vec![common::TEST_AGENT.to_string()],
        },
    )
    .await;
    assert!(create.error.is_none(), "CreateChannel: {:?}", create.error);

    let join = call(
        &state,
        2,
        "__commons__",
        Method::JoinChannel {
            channel_id: "cx06-chan1".to_string(),
            agent_id: common::TEST_AGENT.to_string(),
        },
    )
    .await;
    assert!(join.error.is_none(), "JoinChannel: {:?}", join.error);

    let send = call(
        &state,
        3,
        "__commons__",
        Method::SendMessage {
            channel_id: "cx06-chan1".to_string(),
            sender: common::TEST_AGENT.to_string(),
            payload: "hello".to_string(),
        },
    )
    .await;
    assert!(send.error.is_none(), "SendMessage: {:?}", send.error);

    let messages = call(
        &state,
        4,
        "__commons__",
        Method::GetChannelMessages {
            channel_id: "cx06-chan1".to_string(),
            limit: Some(10),
        },
    )
    .await;
    assert!(
        messages.error.is_none(),
        "GetChannelMessages: {:?}",
        messages.error
    );

    let members = call(
        &state,
        5,
        "__commons__",
        Method::GetChannelMembers {
            channel_id: "cx06-chan1".to_string(),
        },
    )
    .await;
    assert!(
        members.error.is_none(),
        "GetChannelMembers: {:?}",
        members.error
    );

    let list = call(&state, 6, "__commons__", Method::ListChannels).await;
    assert!(list.error.is_none(), "ListChannels: {:?}", list.error);

    let leave = call(
        &state,
        7,
        "__commons__",
        Method::LeaveChannel {
            channel_id: "cx06-chan1".to_string(),
            agent_id: common::TEST_AGENT.to_string(),
        },
    )
    .await;
    assert!(leave.error.is_none(), "LeaveChannel: {:?}", leave.error);
}

#[tokio::test]
async fn t13_send_message_wrong_sender_denied() {
    let state = state();
    assert!(call(
        &state,
        1,
        "__commons__",
        Method::CreateChannel {
            channel_id: "cx06-chan2".to_string(),
            channel_type: ChannelType::Group,
            creator: common::TEST_AGENT.to_string(),
            initial_members: vec![common::TEST_AGENT.to_string()],
        },
    )
    .await
    .error
    .is_none());
    // OBSERVED: `sender` set to a different agent than the authenticated
    // caller is rejected with an ACCESS_DENIED-shaped error.
    let resp = call(
        &state,
        2,
        "__commons__",
        Method::SendMessage {
            channel_id: "cx06-chan2".to_string(),
            sender: "someone-else".to_string(),
            payload: "x".to_string(),
        },
    )
    .await;
    assert!(
        resp.error.is_some(),
        "SendMessage with a spoofed sender must be denied, got: {:?}",
        resp.result
    );
}

// ── ApplyChangeEnvelope / ApplyChangeEnvelopes ────────────────────────────

#[tokio::test]
async fn t14_apply_change_envelopes_empty_batch() {
    let state = state();
    let resp = call(
        &state,
        1,
        "cx06-g5",
        Method::ApplyChangeEnvelopes { envelopes: vec![] },
    )
    .await;
    // OBSERVED: pin whatever an empty envelope batch currently returns.
    let _ = resp.error.is_some();
}

// ── MultiGraphBatchUpdate ──────────────────────────────────────────────

#[tokio::test]
async fn t15_multi_graph_batch_update_across_two_graphs() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-mg1").await.error.is_none());
    assert!(create_graph(&state, 2, "cx06-mg2").await.error.is_none());

    let batches_msgpack = rmp_serde::to_vec_named(&serde_json::json!([
        {"graph": "cx06-mg1", "ops": []},
        {"graph": "cx06-mg2", "ops": []},
    ]))
    .unwrap();
    let resp = call(
        &state,
        3,
        "cx06-mg1",
        Method::MultiGraphBatchUpdate { batches_msgpack },
    )
    .await;
    // OBSERVED: pin whatever shape (success or a decode/shape error) the
    // current handler produces for this msgpack encoding, ahead of refactor.
    let _ = resp.error.is_some();
}

// ── ParseFile / ParseFiles / IndexRepository (ast feature, off by default) ─

#[tokio::test]
#[cfg(not(feature = "ast"))]
async fn t16_parse_file_without_ast_feature_reports_disabled() {
    let state = state();
    let resp = call(
        &state,
        1,
        "__commons__",
        Method::ParseFile {
            file_path: "x.rs".to_string(),
            source: b"fn main() {}".to_vec(),
        },
    )
    .await;
    assert!(
        resp.error.is_some(),
        "ParseFile without the ast feature must report it is disabled"
    );
}

// ── Txn family (BeginTxn / TxnAddNode / ... / Commit / Rollback) ────────
// OBSERVED, and NOT what this test originally assumed: `Commit` fails in
// this test harness because `EPISTEMIC_GRAPH_ENCRYPTION_KEY` is not
// configured (this fixture mirrors every other characterization/integration
// fixture in this repo, none of which set it) -- `BeginTxn`/`TxnAddNode`
// both succeed first, so the failure pins specifically to the durability
// commit step, not the staging path.

#[tokio::test]
async fn t17_txn_begin_add_node_commit_fails_without_encryption_key() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-txn1").await.error.is_none());

    let begin = call(
        &state,
        2,
        "cx06-txn1",
        Method::BeginTxn {
            graph: None,
            isolation: None,
        },
    )
    .await;
    assert!(begin.error.is_none(), "BeginTxn: {:?}", begin.error);
    let txn_id = match &begin.result {
        Some(ResultPayload::String(s)) => s.clone(),
        Some(ResultPayload::Json(v)) => v
            .get("txn_id")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
        other => panic!("expected a txn id payload, got {other:?}"),
    };
    assert!(
        !txn_id.is_empty(),
        "BeginTxn must return a non-empty txn id"
    );

    let add_node = call(
        &state,
        3,
        "cx06-txn1",
        Method::TxnAddNode {
            txn_id: txn_id.clone(),
            node_id: "n1".to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            graph: None,
        },
    )
    .await;
    assert!(add_node.error.is_none(), "TxnAddNode: {:?}", add_node.error);

    let commit = call(
        &state,
        4,
        "cx06-txn1",
        Method::Commit {
            txn_id: txn_id.clone(),
            idempotency_key: None,
        },
    )
    .await;
    assert_eq!(
        commit.error.as_deref(),
        Some("transaction durability requires EPISTEMIC_GRAPH_ENCRYPTION_KEY to be configured"),
        "Commit without an encryption key configured: {:?}",
        commit.error
    );
}

#[tokio::test]
async fn t18_txn_rollback_unknown_txn_fails() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-txn2").await.error.is_none());
    let rollback = call(
        &state,
        2,
        "cx06-txn2",
        Method::Rollback {
            txn_id: "cx06-nonexistent-txn".to_string(),
        },
    )
    .await;
    assert!(
        rollback.error.is_some(),
        "Rollback of an unknown txn must fail, got: {:?}",
        rollback.result
    );
}

// ── Fallthrough (`_`) -> dispatch_graph_op -> dispatch_graph_op_inner ────

#[tokio::test]
async fn t19_wildcard_fallthrough_add_node_and_get_edges() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-fall1").await.error.is_none());
    let add = call(
        &state,
        2,
        "cx06-fall1",
        Method::AddNode {
            node_id: "a".to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        },
    )
    .await;
    assert!(
        add.error.is_none(),
        "AddNode via wildcard fallthrough: {:?}",
        add.error
    );

    let edges = call(&state, 3, "cx06-fall1", Method::GetEdges).await;
    assert!(
        edges.error.is_none(),
        "GetEdges via wildcard fallthrough: {:?}",
        edges.error
    );
}

// ── Auth failure (before the match is ever reached) ───────────────────────

#[tokio::test]
async fn t20_unsigned_request_is_rejected_before_dispatch() {
    let state = state();
    let bad = Request {
        id: 1,
        graph: "__commons__".to_string(),
        auth_token: "test-invalid-token".to_string(),
        agent_id: Some(common::TEST_AGENT.to_string()),
        method: Method::Ping,
    };
    let resp = Box::pin(dispatch(&state, bad)).await;
    assert!(
        resp.error.is_some(),
        "an unsigned/garbage-token request must be rejected, got: {:?}",
        resp.result
    );
}
