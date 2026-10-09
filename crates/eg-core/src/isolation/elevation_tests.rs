//! EH-404: the access chokepoint consults just-in-time elevations.

use super::{
    access_clock_ms, AccessBasis, AccessLevel, AccessQuery, AgentIdentity, AgentRole,
    ElevationError, IsolationLayer,
};
use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
use crate::protocol::GraphType;
use eg_types::rbac_elevation::{
    ElevationAction, ElevationActor, ElevationApproval, ElevationDelegation, ElevationRefusal,
    ElevationRequest, ElevationRevoke, ElevationScope, ElevationStanding,
};

const GRAPH: &str = "tenant__acme__default";
const MINUTE: u64 = 60 * 1000;

fn agent(id: &str) -> AgentIdentity {
    AgentIdentity {
        agent_id: id.to_string(),
        role: AgentRole::Agent,
        teams: Vec::new(),
        roles: vec!["worker".to_string()],
    }
}

fn layer() -> IsolationLayer {
    let mut layer = IsolationLayer::new();
    layer.register_agent(agent("agent:alice"));
    layer.register_agent(agent("agent:bob"));
    layer.add_role(Role::new("worker"));
    layer
}

fn actor(id: &str, standing: ElevationStanding) -> ElevationActor {
    ElevationActor::from_identities(id, [id], ElevationDelegation::Direct, standing)
}

fn basis(layer: &IsolationLayer, access: AccessLevel, now_ms: u64) -> AccessBasis {
    layer.access_basis(&AccessQuery {
        agent_id: "agent:alice",
        graph_name: GRAPH,
        graph_type: GraphType::Agent,
        graph_owner: None,
        access,
        now_ms,
    })
}

fn request() -> ElevationRequest {
    ElevationRequest {
        elevation_id: "e1".to_string(),
        scopes: vec![ElevationScope {
            graph: GRAPH.to_string(),
            action: ElevationAction::Write,
        }],
        span_ms: 10 * MINUTE,
        justification: "incident 7".to_string(),
    }
}

/// Request (alice) and approve (bob) at `now_ms`; returns the hard expiry.
fn elevate(layer: &mut IsolationLayer, now_ms: u64) -> u64 {
    let requested = layer
        .try_request_elevation(
            &actor("agent:alice", ElevationStanding::Requester),
            &request(),
            now_ms,
        )
        .unwrap();
    let approval = ElevationApproval {
        elevation_id: "e1".to_string(),
        request_digest: requested.request_digest,
    };
    layer
        .try_approve_elevation(
            &actor("agent:bob", ElevationStanding::Approver),
            &approval,
            now_ms,
        )
        .unwrap()
        .hard_expires_at_ms
        .unwrap()
}

#[test]
fn check_access_denies_before_allows_during_and_denies_at_expiry() {
    let mut layer = layer();
    let now = access_clock_ms();
    let owner = None;
    assert!(!layer.check_access(
        "agent:alice",
        GRAPH,
        GraphType::Agent,
        owner,
        AccessLevel::Write
    ));
    let end = elevate(&mut layer, now);
    // The real chokepoint, on the real clock.
    assert!(layer.check_access(
        "agent:alice",
        GRAPH,
        GraphType::Agent,
        owner,
        AccessLevel::Write
    ));
    assert_eq!(
        basis(&layer, AccessLevel::Write, end - 1),
        AccessBasis::Elevation("e1".to_string())
    );
    assert_eq!(basis(&layer, AccessLevel::Write, end), AccessBasis::Denied);
    // Exactly the named action: write was granted, read was not.
    assert_eq!(basis(&layer, AccessLevel::Read, now), AccessBasis::Denied);
}

#[test]
fn revocation_takes_effect_at_the_chokepoint_immediately() {
    let mut layer = layer();
    let now = access_clock_ms();
    elevate(&mut layer, now);
    assert!(basis(&layer, AccessLevel::Write, now).is_allowed());
    let revoke = ElevationRevoke {
        elevation_id: "e1".to_string(),
    };
    layer
        .try_revoke_elevation(
            &actor("agent:bob", ElevationStanding::Approver),
            &revoke,
            now,
        )
        .unwrap();
    assert_eq!(basis(&layer, AccessLevel::Write, now), AccessBasis::Denied);
    assert!(!layer.check_access(
        "agent:alice",
        GRAPH,
        GraphType::Agent,
        None,
        AccessLevel::Write
    ));
}

#[test]
fn an_explicit_deny_is_never_overridden_by_an_elevation() {
    let mut layer = layer();
    layer.add_grant(Grant {
        role: "worker".to_string(),
        resource: ResourceSelector::Graph(GRAPH.to_string()),
        action: RbacAction::Write,
        effect: GrantEffect::Deny,
    });
    let now = access_clock_ms();
    elevate(&mut layer, now);
    assert_eq!(basis(&layer, AccessLevel::Write, now), AccessBasis::Denied);
}

#[test]
fn self_approval_and_unregistered_actors_are_refused_without_a_trace() {
    let mut layer = layer();
    let now = access_clock_ms();
    let alice = actor("agent:alice", ElevationStanding::Approver);
    let requested = layer
        .try_request_elevation(&alice, &request(), now)
        .unwrap();
    let approval = ElevationApproval {
        elevation_id: "e1".to_string(),
        request_digest: requested.request_digest,
    };
    let before = layer.rbac().elevations().clone();
    assert_eq!(
        layer.try_approve_elevation(&alice, &approval, now),
        Err(ElevationError::Refused(ElevationRefusal::SelfApproval))
    );
    let stranger = actor("agent:mallory", ElevationStanding::Approver);
    assert_eq!(
        layer.try_approve_elevation(&stranger, &approval, now),
        Err(ElevationError::Refused(ElevationRefusal::UnknownActor))
    );
    assert_eq!(layer.rbac().elevations(), &before);
    assert!(!basis(&layer, AccessLevel::Write, now).is_allowed());
}

#[test]
fn an_elevation_never_confers_admin_capability() {
    let mut layer = layer();
    elevate(&mut layer, access_clock_ms());
    assert!(!layer.has_admin_capability("agent:alice"));
}

#[test]
fn an_elevation_survives_a_restart_of_the_durable_store() {
    use crate::test_scope_grant::{TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF};
    let now = access_clock_ms();
    let dir = std::env::temp_dir().join(format!("eg-elevation-{}-{now}", std::process::id()));
    let open = || {
        IsolationLayer::with_persist_dir(
            &dir,
            &TestScopeVerifier {
                layout: eg_storage::OwnerLayout::Rbac,
            },
            TEST_PRINCIPAL,
            TEST_PROOF,
        )
        .unwrap()
    };
    {
        let mut durable = open();
        durable.register_agent(agent("agent:alice"));
        durable.register_agent(agent("agent:bob"));
        elevate(&mut durable, now);
    }
    let reopened = open();
    assert_eq!(
        basis(&reopened, AccessLevel::Write, now),
        AccessBasis::Elevation("e1".to_string())
    );
    assert!(reopened.rbac().elevations().verify_audit_chain());
    let _ = std::fs::remove_dir_all(&dir);
}
