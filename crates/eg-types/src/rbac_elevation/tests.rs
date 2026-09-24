use super::*;

const HOUR: u64 = 60 * 60 * 1000;
const T0: u64 = 1_000_000;

fn actor(agent: &str, principal: &str, standing: ElevationStanding) -> ElevationActor {
    ElevationActor::from_identities(agent, [principal], ElevationDelegation::Direct, standing)
}

fn requester() -> ElevationActor {
    actor("agent:alice", "user:alice", ElevationStanding::Requester)
}

fn approver() -> ElevationActor {
    actor("agent:bob", "user:bob", ElevationStanding::Approver)
}

fn scope(graph: &str, action: ElevationAction) -> ElevationScope {
    ElevationScope {
        graph: graph.to_string(),
        action,
    }
}

fn request(id: &str) -> ElevationRequest {
    ElevationRequest {
        elevation_id: id.to_string(),
        scopes: vec![scope("tenant__acme__default", ElevationAction::Write)],
        span_ms: HOUR,
        justification: "incident 42: repair a corrupted node".to_string(),
    }
}

fn approval(lease: &ElevationLease) -> ElevationApproval {
    ElevationApproval {
        elevation_id: lease.elevation_id.clone(),
        request_digest: lease.request_digest.clone(),
    }
}

fn writes(ledger: &ElevationLedger, now_ms: u64) -> bool {
    ledger
        .permitting(
            "agent:alice",
            "tenant__acme__default",
            ElevationAction::Write,
            now_ms,
        )
        .is_some()
}

/// Request then approve at `T0`, returning the active lease.
fn active_ledger() -> (ElevationLedger, ElevationLease) {
    let mut ledger = ElevationLedger::default();
    let requested = ledger.request(&requester(), &request("e1"), T0).unwrap();
    let active = ledger
        .approve(&approver(), &approval(&requested), T0 + 10)
        .unwrap();
    (ledger, active)
}

#[test]
fn denied_before_grant_allowed_during_denied_the_instant_it_expires() {
    let mut ledger = ElevationLedger::default();
    let requested = ledger.request(&requester(), &request("e1"), T0).unwrap();
    assert_eq!(requested.status, ElevationStatus::Requested);
    assert!(!writes(&ledger, T0), "a request alone grants nothing");
    let active = ledger
        .approve(&approver(), &approval(&requested), T0 + 10)
        .unwrap();
    let end = active.hard_expires_at_ms.unwrap();
    assert_eq!(end, T0 + 10 + HOUR);
    assert!(writes(&ledger, T0 + 10));
    assert!(writes(&ledger, end - 1));
    // Expiry is decided at check time: no sweep has run.
    assert!(!writes(&ledger, end));
    assert!(!writes(&ledger, end + 1));
    assert_eq!(ledger.get("e1").unwrap().status, ElevationStatus::Active);
}

#[test]
fn revocation_is_immediate() {
    let (mut ledger, _) = active_ledger();
    assert!(writes(&ledger, T0 + 20));
    let revoke = ElevationRevoke {
        elevation_id: "e1".to_string(),
    };
    let revoked = ledger.revoke(&approver(), &revoke, T0 + 20).unwrap();
    assert_eq!(revoked.status, ElevationStatus::Revoked);
    assert!(!writes(&ledger, T0 + 20));
    // A second revoke of an ended lease is a conflict, not a silent success.
    assert_eq!(
        ledger.revoke(&approver(), &revoke, T0 + 21),
        Err(ElevationRefusal::Conflict)
    );
}

#[test]
fn the_grantee_may_revoke_its_own_elevation_but_a_stranger_may_not() {
    let (mut ledger, _) = active_ledger();
    let revoke = ElevationRevoke {
        elevation_id: "e1".to_string(),
    };
    let stranger = actor("agent:eve", "user:eve", ElevationStanding::Requester);
    assert_eq!(
        ledger.revoke(&stranger, &revoke, T0 + 20),
        Err(ElevationRefusal::NotParty)
    );
    assert!(ledger.revoke(&requester(), &revoke, T0 + 20).is_ok());
}

#[test]
fn self_approval_is_refused() {
    let mut ledger = ElevationLedger::default();
    let requested = ledger.request(&requester(), &request("e1"), T0).unwrap();
    let same_identity = actor("agent:alice", "user:alice", ElevationStanding::Approver);
    assert_eq!(
        ledger.approve(&same_identity, &approval(&requested), T0 + 1),
        Err(ElevationRefusal::SelfApproval)
    );
    // The human behind a delegated agent is the same party: an agent acting
    // for user:alice asks, user:alice may not approve it.
    let delegated_request = ElevationActor::from_identities(
        "agent:alice-bot",
        ["user:alice", "agent:alice-bot"],
        ElevationDelegation::Delegated,
        ElevationStanding::Requester,
    );
    let requested = ledger
        .request(&delegated_request, &request("e2"), T0)
        .unwrap();
    let the_human = actor("user:alice", "user:alice", ElevationStanding::Approver);
    assert_eq!(
        ledger.approve(&the_human, &approval(&requested), T0 + 1),
        Err(ElevationRefusal::SelfApproval)
    );
    assert!(!writes(&ledger, T0 + 2));
}

#[test]
fn delegated_or_unscoped_approvers_are_refused() {
    let mut ledger = ElevationLedger::default();
    let requested = ledger.request(&requester(), &request("e1"), T0).unwrap();
    let delegated = ElevationActor::from_identities(
        "agent:bot",
        ["user:bob"],
        ElevationDelegation::Delegated,
        ElevationStanding::Approver,
    );
    assert_eq!(
        ledger.approve(&delegated, &approval(&requested), T0 + 1),
        Err(ElevationRefusal::DelegatedApprover)
    );
    let unscoped = actor("agent:bob", "user:bob", ElevationStanding::Requester);
    assert_eq!(
        ledger.approve(&unscoped, &approval(&requested), T0 + 1),
        Err(ElevationRefusal::NotApprover)
    );
}

#[test]
fn a_wildcard_or_reserved_scope_is_refused() {
    let mut ledger = ElevationLedger::default();
    for graph in ["tenant__acme__*", "*", "tenant?", "*commons*"] {
        let mut wild = request("w");
        wild.scopes = vec![scope(graph, ElevationAction::Read)];
        assert_eq!(
            ledger.request(&requester(), &wild, T0),
            Err(ElevationRefusal::Wildcard),
            "{graph}"
        );
    }
    let mut admin = request("a");
    admin.scopes = vec![scope("__admin__", ElevationAction::Write)];
    assert_eq!(
        ledger.request(&requester(), &admin, T0),
        Err(ElevationRefusal::ReservedGraph)
    );
    assert!(ledger.is_empty());
}

#[test]
fn admin_is_not_an_elevation_action() {
    let parsed: Result<ElevationScope, _> =
        serde_json::from_str(r#"{"graph":"g","action":"admin"}"#);
    assert!(parsed.is_err());
}

#[test]
fn a_replayed_approval_is_refused() {
    let mut ledger = ElevationLedger::default();
    let requested = ledger.request(&requester(), &request("e1"), T0).unwrap();
    let decision = approval(&requested);
    ledger.approve(&approver(), &decision, T0 + 1).unwrap();
    assert_eq!(
        ledger.approve(&approver(), &decision, T0 + 2),
        Err(ElevationRefusal::Conflict)
    );
    let third = actor("agent:carol", "user:carol", ElevationStanding::Approver);
    assert_eq!(
        ledger.approve(&third, &decision, T0 + 2),
        Err(ElevationRefusal::Conflict)
    );
    // Replayed after a revoke: still refused, and still grants nothing.
    let revoke = ElevationRevoke {
        elevation_id: "e1".to_string(),
    };
    ledger.revoke(&approver(), &revoke, T0 + 3).unwrap();
    assert_eq!(
        ledger.approve(&approver(), &decision, T0 + 4),
        Err(ElevationRefusal::Conflict)
    );
    assert!(!writes(&ledger, T0 + 4));
}

#[test]
fn an_approval_names_one_exact_request() {
    let mut ledger = ElevationLedger::default();
    ledger.request(&requester(), &request("e1"), T0).unwrap();
    let forged = ElevationApproval {
        elevation_id: "e1".to_string(),
        request_digest: "0".repeat(64),
    };
    assert_eq!(
        ledger.approve(&approver(), &forged, T0 + 1),
        Err(ElevationRefusal::DigestMismatch)
    );
    // A re-request under a retired id is a collision while the old record is
    // retained, so an old approval can never activate a new request.
    assert_eq!(
        ledger.request(&requester(), &request("e1"), T0 + 2),
        Err(ElevationRefusal::Collision)
    );
}

#[test]
fn an_unapproved_request_expires_and_cannot_be_approved_late() {
    let mut ledger = ElevationLedger::default();
    let requested = ledger.request(&requester(), &request("e1"), T0).unwrap();
    let late = T0 + ELEVATION_REQUEST_TTL_MS;
    assert_eq!(
        ledger.approve(&approver(), &approval(&requested), late),
        Err(ElevationRefusal::Conflict)
    );
    assert_eq!(ledger.get("e1").unwrap().status, ElevationStatus::Expired);
}

#[test]
fn an_elevation_grants_exactly_its_scopes_to_exactly_its_grantee() {
    let (ledger, _) = active_ledger();
    let now = T0 + 20;
    let graph = "tenant__acme__default";
    assert!(ledger
        .permitting("agent:alice", graph, ElevationAction::Read, now)
        .is_none());
    assert!(ledger
        .permitting(
            "agent:alice",
            "tenant__acme__other",
            ElevationAction::Write,
            now
        )
        .is_none());
    assert!(ledger
        .permitting("agent:bob", graph, ElevationAction::Write, now)
        .is_none());
    assert_eq!(
        ledger.permitting("agent:alice", graph, ElevationAction::Write, now),
        Some("e1")
    );
}

#[test]
fn the_span_is_bounded_and_there_is_no_extension() {
    let mut ledger = ElevationLedger::default();
    let mut long = request("long");
    long.span_ms = MAX_ELEVATION_SPAN_MS + 1;
    assert_eq!(
        ledger.request(&requester(), &long, T0),
        Err(ElevationRefusal::InvalidRequest)
    );
    let mut duplicate = request("dup");
    duplicate.scopes.push(duplicate.scopes[0].clone());
    assert_eq!(
        ledger.request(&requester(), &duplicate, T0),
        Err(ElevationRefusal::InvalidRequest)
    );
    let op: Result<RbacElevationOp, _> =
        serde_json::from_str(r#"{"op":"extend","request":{"elevation_id":"e1"}}"#);
    assert!(op.is_err(), "no op can extend a window");
}

#[test]
fn every_lifecycle_event_is_audited_on_a_verifiable_chain() {
    let (mut ledger, active) = active_ledger();
    let end = active.hard_expires_at_ms.unwrap();
    // The next mutation settles the lapsed lease and audits its expiry.
    ledger
        .request(&requester(), &request("e2"), end + 5)
        .unwrap();
    let events: Vec<_> = ledger
        .audit()
        .iter()
        .map(|entry| (entry.event, entry.elevation_id.as_str()))
        .collect();
    assert_eq!(
        events,
        vec![
            (ElevationEvent::Requested, "e1"),
            (ElevationEvent::Approved, "e1"),
            (ElevationEvent::Expired, "e1"),
            (ElevationEvent::Requested, "e2"),
        ]
    );
    assert_eq!(ledger.get("e1").unwrap().ended_at_ms, Some(end));
    assert!(ledger.verify_audit_chain());
    let mut tampered = ledger.clone();
    tampered.audit_for_test()[1].at_ms += 1;
    assert!(!tampered.verify_audit_chain());
}

#[test]
fn ended_leases_are_pruned_after_retention() {
    let (mut ledger, active) = active_ledger();
    let end = active.hard_expires_at_ms.unwrap();
    let later = end + ELEVATION_RETENTION_MS + 1;
    ledger.request(&requester(), &request("e2"), later).unwrap();
    assert!(ledger.get("e1").is_none());
}

#[test]
fn the_op_carries_its_own_authorization_action() {
    let approve = RbacElevationOp::Approve {
        request: ElevationApproval {
            elevation_id: "e1".to_string(),
            request_digest: String::new(),
        },
    };
    assert_eq!(approve.authz_action(), APPROVE_ELEVATION_SCOPE);
    assert!(approve.is_mutation());
    assert!(!RbacElevationOp::List.is_mutation());
    assert_eq!(RbacElevationOp::List.name(), "list");
}
