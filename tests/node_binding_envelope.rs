//! CONCEPT:EG-KG.security.node-bound-envelope — ADR-3 / W1.9 node-bound
//! envelopes (`reports/wave1/ADR-scale-trio.md`), exercised over the REAL
//! served `dispatch` surface (not just the `auth` module's own unit tests).
//!
//! No `raft` feature and no `EPISTEMIC_GRAPH_NODE_ID` override are configured
//! here, so this process's `node_identity()` resolves to the documented
//! single-node default: the literal `"single"`.
// This file's whole subject (ADR-3 / W1.9 node-bound envelopes) IS a security
// concept, and every test dispatches through the REAL secure-envelope auth
// path (`common::signed_request_with_node` -> `dispatch`), which links against
// the library WITHOUT `cfg(test)` (integration-test crates are separate
// compilation units), so it always hits `durable_replay_ledger`'s production
// fail-closed branch. That branch requires the `security` feature. Without it,
// every test here fails immediately with "secure request context requires the
// security feature" before it ever reaches node-binding logic -- this is a
// genuine capability requirement, not a mis-asserted slim test (mirrors the
// `redb`+`security` precedent in `tests/txn_recovery_key_decoupled_d_orc_50.rs`).
#![cfg(all(feature = "server", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::protocol::{GraphType, Method};

const SECRET: &str = "node-binding-envelope-secret";

fn state() -> test_support::SharedState {
    test_support::durable_state(SECRET, common::current_isolation())
}

async fn ready_state() -> test_support::SharedState {
    let state = state();
    {
        let s = &mut *state.write().await;
        s.registry
            .create_graph("g", GraphType::Commons, None)
            .unwrap();
    }
    state
}

/// An old client that predates node binding never sends the `node` claim at
/// all. Under the shipped default posture (`EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING`
/// unset ⇒ `warn`), the request must still succeed end-to-end.
#[tokio::test]
async fn old_client_without_node_claim_still_dispatches_under_default_warn_posture() {
    let state = ready_state().await;
    let req = common::signed_request_with_node(SECRET, 1, "g", Method::Ping, None);
    let resp = test_support::dispatch(&state, req).await;
    assert!(resp.error.is_none(), "got: {:?}", resp.error);
}

/// A node claim that exact-matches this process's own identity (the
/// documented single-node default, `"single"`) must dispatch normally.
#[tokio::test]
async fn matching_node_claim_dispatches_normally() {
    let state = ready_state().await;
    let req = common::signed_request_with_node(SECRET, 2, "g", Method::Ping, Some("single"));
    let resp = test_support::dispatch(&state, req).await;
    assert!(resp.error.is_none(), "got: {:?}", resp.error);
}

/// A node claim bound to a DIFFERENT node must be rejected end-to-end, with
/// the distinct `NODE_MISMATCH` error surfacing through the real `dispatch`
/// response -- not merely inside the `auth` module's own unit tests. This is
/// the exact scenario ADR-3 exists for: a captured envelope replayed against
/// the wrong cluster member.
#[tokio::test]
async fn wrong_node_claim_is_rejected_by_the_real_dispatch_path() {
    let state = ready_state().await;
    let req =
        common::signed_request_with_node(SECRET, 3, "g", Method::Ping, Some("some-other-node"));
    let resp = test_support::dispatch(&state, req).await;
    let error = resp
        .error
        .expect("a mismatched node claim must be rejected");
    assert!(error.starts_with("NODE_MISMATCH"), "got: {error}");
}
