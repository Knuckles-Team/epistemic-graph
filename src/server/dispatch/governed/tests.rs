//! EH-560 through the real request boundary: the actor is stamped from the
//! verified context, the approver needs the kind's exact scope and must be a
//! second, direct identity, and a generic control lease of a governed kind
//! is refused.

use super::super::test_support::{refused_with, send, state, verified};
use super::*;
use eg_types::governed_change::{
    GovernedApproval, GovernedProposal, GovernedStatus, SCHEMA_REPAIR_KIND,
};

const PROPOSE: &str = "governance:propose";
const APPROVE: &str = "governance:approve-schema-repair";
const READ: &str = "governance:read";

fn propose() -> Method {
    Method::GovernedChange {
        op: GovernedChangeOp::Propose {
            request: GovernedProposal {
                change_id: "repair-1".to_string(),
                kind: SCHEMA_REPAIR_KIND.to_string(),
                target: "approved:orders".to_string(),
                digest: "digest-1".to_string(),
                justification: "orders drifted".to_string(),
                proposal_ttl_ms: 60_000,
                window_ms: 60_000,
            },
        },
        actor: None,
    }
}

fn approve(forged: Option<GovernedActor>) -> Method {
    Method::GovernedChange {
        op: GovernedChangeOp::Approve {
            request: GovernedApproval {
                change_id: "repair-1".to_string(),
                digest: "digest-1".to_string(),
            },
        },
        actor: forged,
    }
}

async fn status(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
) -> GovernedStatus {
    state
        .read()
        .await
        .isolation
        .rbac()
        .governed()
        .get("repair-1", super::super::authoritative_now_ms())
        .expect("proposed")
        .status
}

#[tokio::test]
async fn a_second_direct_approver_with_the_exact_scope_approves() {
    let state = state();
    let proposed = send(&state, verified("svc:au", &[PROPOSE], &[]), propose()).await;
    assert!(proposed.error.is_none(), "{:?}", proposed.error);
    let own = send(
        &state,
        verified("svc:au", &[APPROVE, READ], &[]),
        approve(None),
    )
    .await;
    assert!(
        refused_with(&own, "GOVERNED_SELF_APPROVAL"),
        "{:?}",
        own.error
    );
    let via_agent = send(
        &state,
        verified("usr:bob", &[APPROVE, READ], &["usr:bob", "agent:helper"]),
        approve(None),
    )
    .await;
    assert!(
        refused_with(&via_agent, "GOVERNED_DELEGATED_APPROVER"),
        "{:?}",
        via_agent.error
    );
    let admin = send(
        &state,
        verified("usr:root", &["kg:admin", "*"], &[]),
        approve(None),
    )
    .await;
    assert!(
        refused_with(&admin, "GOVERNED_NOT_AUTHORIZED"),
        "{:?}",
        admin.error
    );
    assert_eq!(status(&state).await, GovernedStatus::Proposed);
    let bob = send(
        &state,
        verified("usr:bob", &[APPROVE, READ], &[]),
        approve(None),
    )
    .await;
    assert!(bob.error.is_none(), "{:?}", bob.error);
    assert_eq!(status(&state).await, GovernedStatus::Approved);
}

#[tokio::test]
async fn a_forged_actor_in_the_body_is_overwritten() {
    let state = state();
    send(&state, verified("svc:au", &[PROPOSE], &[]), propose()).await;
    let forged =
        GovernedActor::from_identities("usr:bob", ["usr:bob"], false, [APPROVE.to_string()].into());
    let response = send(
        &state,
        verified("svc:au", &[READ], &[]),
        approve(Some(forged)),
    )
    .await;
    assert!(refused_with(&response, "GOVERNED_"), "{:?}", response.error);
    assert_eq!(status(&state).await, GovernedStatus::Proposed);
}
