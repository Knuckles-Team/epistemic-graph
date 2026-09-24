//! EH-404 through the real request boundary: the actor is stamped from the
//! verified context, the approval needs the exact scope and a second
//! identity, a replayed approval is refused, and revocation closes the
//! chokepoint at once.

use super::*;
use crate::acl::{AgentIdentity, RequestContextClaims};
use crate::isolation::{AccessLevel, AgentRole, IsolationLayer};
use crate::protocol::{GraphType, Request};
use eg_types::rbac_elevation::{
    ElevationAction, ElevationApproval, ElevationRequest, ElevationRevoke, ElevationScope,
};
use std::sync::Arc;
use tokio::sync::RwLock;

const GRAPH: &str = "tenant__acme__default";
const REQUEST_SCOPE: &str = "rbac:elevation";

fn state() -> Arc<RwLock<super::super::ServerState>> {
    let mut isolation = IsolationLayer::new();
    for agent_id in ["alice", "bob"] {
        isolation.register_agent(AgentIdentity {
            agent_id: agent_id.to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
    }
    Arc::new(RwLock::new(super::super::ServerState::new_for_test(
        "elevation-test-secret",
        isolation,
    )))
}

fn context(agent: &str, scopes: &[&str], delegation: &[&str]) -> VerifiedRequestContext {
    static KEY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let key = KEY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    VerifiedRequestContext::from_verified_claims(
        RequestContextClaims {
            principal: format!("user:{agent}"),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: agent.to_string(),
            roles: Vec::new(),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            policy_version: "policy-test".to_string(),
            delegation: delegation.iter().map(|hop| hop.to_string()).collect(),
            node: None,
            priority: None,
        },
        format!("elevation-test-{key}"),
    )
}

/// Dispatch one elevation op as `agent` holding `scopes`, carrying `forged`
/// as the body's actor to prove it is never trusted.
async fn send(
    state: &Arc<RwLock<super::super::ServerState>>,
    context: VerifiedRequestContext,
    op: RbacElevationOp,
    forged: Option<ElevationActor>,
) -> Response {
    let request = Request {
        id: 7,
        graph: "__commons__".to_string(),
        auth_token: String::new(),
        agent_id: Some(context.agent_id().to_string()),
        method: Method::RbacElevation { op, actor: forged },
    };
    Box::pin(super::super::request_boundary::dispatch_with_context(
        state,
        request,
        Some(context),
    ))
    .await
}

fn request_op() -> RbacElevationOp {
    RbacElevationOp::Request {
        request: ElevationRequest {
            elevation_id: "e1".to_string(),
            scopes: vec![ElevationScope {
                graph: GRAPH.to_string(),
                action: ElevationAction::Write,
            }],
            span_ms: 60_000,
            justification: "incident 9".to_string(),
        },
    }
}

async fn approve_op(state: &Arc<RwLock<super::super::ServerState>>) -> RbacElevationOp {
    let digest = state
        .read()
        .await
        .isolation
        .rbac()
        .elevations()
        .get("e1")
        .expect("requested")
        .request_digest
        .clone();
    RbacElevationOp::Approve {
        request: ElevationApproval {
            elevation_id: "e1".to_string(),
            request_digest: digest,
        },
    }
}

async fn alice_writes(state: &Arc<RwLock<super::super::ServerState>>) -> bool {
    state.read().await.isolation.check_access(
        "alice",
        GRAPH,
        GraphType::Agent,
        None,
        AccessLevel::Write,
    )
}

fn refused_with(response: &Response, code: &str) -> bool {
    response
        .error
        .as_deref()
        .is_some_and(|error| error.contains(code))
}

#[test]
fn the_actor_comes_from_the_verified_context_never_the_body() {
    let forged = ElevationActor::from_identities(
        "bob",
        ["user:bob"],
        ElevationDelegation::Direct,
        ElevationStanding::Approver,
    );
    let mut method = Method::RbacElevation {
        op: RbacElevationOp::List,
        actor: Some(forged.clone()),
    };
    let alice = context("alice", &["*"], &[]);
    stamp_elevation_actor(&mut method, &alice, ElevationStampAuthority::External);
    let Method::RbacElevation {
        actor: Some(stamped),
        ..
    } = &method
    else {
        panic!("stamp must leave an actor");
    };
    assert_eq!(stamped.agent_id, "alice");
    assert_eq!(stamped.standing, ElevationStanding::Requester);
    // A replicated apply keeps what its leader stamped.
    stamp_elevation_actor(&mut method, &alice, ElevationStampAuthority::Replicated);
    assert!(
        matches!(&method, Method::RbacElevation { actor: Some(a), .. } if a.agent_id == "alice")
    );
}

#[test]
fn approval_standing_needs_the_exact_scope_and_delegation_is_recorded() {
    for scopes in [&["*"][..], &["rbac:*"], &["kg:admin"], &["kg:write"]] {
        let actor = elevation_actor(&context("bob", scopes, &[]));
        assert_eq!(actor.standing, ElevationStanding::Requester, "{scopes:?}");
    }
    let approver = elevation_actor(&context("bob", &[APPROVE_ELEVATION_SCOPE], &[]));
    assert_eq!(approver.standing, ElevationStanding::Approver);
    assert_eq!(approver.delegation, ElevationDelegation::Direct);
    let delegated = elevation_actor(&context(
        "bot",
        &[APPROVE_ELEVATION_SCOPE],
        &["user:carol", "bot"],
    ));
    assert_eq!(delegated.delegation, ElevationDelegation::Delegated);
    assert!(delegated.shares_party(&[eg_types::rbac_elevation::party_id("user:carol")]));
}

#[tokio::test(flavor = "multi_thread")]
async fn two_person_elevation_through_the_request_boundary() {
    let state = state();
    let bob_approver = [APPROVE_ELEVATION_SCOPE, REQUEST_SCOPE];
    let forged_bob = elevation_actor(&context("bob", &bob_approver, &[]));
    let requested = send(
        &state,
        context("alice", &[REQUEST_SCOPE], &[]),
        request_op(),
        Some(forged_bob),
    )
    .await;
    assert!(requested.error.is_none(), "{:?}", requested.error);
    assert_eq!(
        state
            .read()
            .await
            .isolation
            .rbac()
            .elevations()
            .get("e1")
            .unwrap()
            .grantee,
        "alice",
        "a forged body actor must not become the grantee"
    );
    assert!(!alice_writes(&state).await, "denied before approval");

    let own = send(
        &state,
        context("alice", &bob_approver, &[]),
        approve_op(&state).await,
        None,
    )
    .await;
    assert!(
        refused_with(&own, "ELEVATION_SELF_APPROVAL"),
        "{:?}",
        own.error
    );
    let wildcard = send(
        &state,
        context("bob", &["*"], &[]),
        approve_op(&state).await,
        None,
    )
    .await;
    assert!(
        refused_with(&wildcard, "ELEVATION_NOT_APPROVER"),
        "{:?}",
        wildcard.error
    );
    assert!(!alice_writes(&state).await);

    let approval = approve_op(&state).await;
    let approved = send(
        &state,
        context("bob", &bob_approver, &[]),
        approval.clone(),
        None,
    )
    .await;
    assert!(approved.error.is_none(), "{:?}", approved.error);
    assert!(alice_writes(&state).await, "allowed during the window");

    let replay = send(&state, context("bob", &bob_approver, &[]), approval, None).await;
    assert!(
        refused_with(&replay, "ELEVATION_CONFLICT"),
        "{:?}",
        replay.error
    );

    let revoke = RbacElevationOp::Revoke {
        request: ElevationRevoke {
            elevation_id: "e1".to_string(),
        },
    };
    let revoked = send(&state, context("bob", &bob_approver, &[]), revoke, None).await;
    assert!(revoked.error.is_none(), "{:?}", revoked.error);
    assert!(
        !alice_writes(&state).await,
        "denied the instant it is revoked"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wildcard_request_is_refused_at_the_boundary() {
    let state = state();
    let mut op = request_op();
    if let RbacElevationOp::Request { request } = &mut op {
        request.scopes[0].graph = "tenant__acme__*".to_string();
    }
    let response = send(&state, context("alice", &[REQUEST_SCOPE], &[]), op, None).await;
    assert!(
        refused_with(&response, "ELEVATION_WILDCARD"),
        "{:?}",
        response.error
    );
    assert!(state.read().await.isolation.rbac().elevations().is_empty());
}
