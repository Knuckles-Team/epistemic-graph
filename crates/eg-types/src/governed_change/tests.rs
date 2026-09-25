//! EH-560: every governed-change rule, both ways.

use super::*;

const NOW: u64 = 1_800_000_000_000;
const PROPOSE: &str = "governance:propose";
const APPROVE: &str = "governance:approve-schema-repair";

fn actor(who: &str, scopes: &[&str], delegated: bool) -> GovernedActor {
    GovernedActor::from_identities(
        who,
        [who],
        delegated,
        scopes.iter().map(|scope| scope.to_string()).collect(),
    )
}

fn proposal() -> GovernedProposal {
    GovernedProposal {
        change_id: "c1".to_string(),
        kind: SCHEMA_REPAIR_KIND.to_string(),
        target: "approved:orders".to_string(),
        digest: "d1".to_string(),
        justification: "drift in orders".to_string(),
        proposal_ttl_ms: 60_000,
        window_ms: 60_000,
    }
}

fn approval(digest: &str) -> GovernedApproval {
    GovernedApproval {
        change_id: "c1".to_string(),
        digest: digest.to_string(),
    }
}

fn consumption(digest: &str) -> GovernedConsumption {
    GovernedConsumption {
        change_id: "c1".to_string(),
        kind: SCHEMA_REPAIR_KIND.to_string(),
        target: "approved:orders".to_string(),
        digest: digest.to_string(),
    }
}

fn proposed() -> GovernedLedger {
    let mut ledger = GovernedLedger::default();
    ledger
        .propose(&actor("svc:au", &[PROPOSE], false), &proposal(), NOW)
        .unwrap();
    ledger
}

#[test]
fn proposing_needs_the_proposer_scope_and_a_registered_kind() {
    let mut ledger = GovernedLedger::default();
    assert_eq!(
        ledger.propose(&actor("svc:au", &[], false), &proposal(), NOW),
        Err(GovernedRefusal::NotAuthorized)
    );
    let mut unknown = proposal();
    unknown.kind = "governed.unknown".to_string();
    assert_eq!(
        ledger.propose(&actor("svc:au", &[PROPOSE], false), &unknown, NOW),
        Err(GovernedRefusal::UnknownKind)
    );
    assert!(ledger
        .propose(&actor("svc:au", &[PROPOSE], false), &proposal(), NOW)
        .is_ok());
    assert_eq!(
        ledger.propose(&actor("svc:au", &[PROPOSE], false), &proposal(), NOW),
        Err(GovernedRefusal::Collision)
    );
}

#[test]
fn the_approver_must_be_a_second_direct_person_with_the_exact_scope() {
    let mut ledger = proposed();
    let same = actor("svc:au", &[APPROVE], false);
    assert_eq!(
        ledger.approve(&same, &approval("d1"), NOW),
        Err(GovernedRefusal::SelfApproval)
    );
    let delegated = actor("usr:bob", &[APPROVE], true);
    assert_eq!(
        ledger.approve(&delegated, &approval("d1"), NOW),
        Err(GovernedRefusal::DelegatedApprover)
    );
    let unscoped = actor("usr:bob", &[PROPOSE, "kg:admin", "*"], false);
    assert_eq!(
        ledger.approve(&unscoped, &approval("d1"), NOW),
        Err(GovernedRefusal::NotAuthorized)
    );
    let bob = actor("usr:bob", &[APPROVE], false);
    assert_eq!(
        ledger.approve(&bob, &approval("d2"), NOW),
        Err(GovernedRefusal::DigestMismatch)
    );
    let approved = ledger.approve(&bob, &approval("d1"), NOW).unwrap();
    assert_eq!(approved.status, GovernedStatus::Approved);
    assert_eq!(
        ledger.approve(&bob, &approval("d1"), NOW),
        Err(GovernedRefusal::Conflict),
        "a replayed approval is refused"
    );
}

#[test]
fn an_approval_is_consumed_once_for_the_exact_candidate_inside_its_window() {
    let mut ledger = proposed();
    assert_eq!(
        ledger.consume(&consumption("d1"), "engine", NOW),
        Err(GovernedRefusal::Conflict),
        "an unapproved proposal cannot be consumed"
    );
    ledger
        .approve(&actor("usr:bob", &[APPROVE], false), &approval("d1"), NOW)
        .unwrap();
    assert_eq!(
        ledger.consume(&consumption("other"), "engine", NOW),
        Err(GovernedRefusal::DigestMismatch)
    );
    let mut late = ledger.clone();
    assert_eq!(
        late.consume(&consumption("d1"), "engine", NOW + 60_000),
        Err(GovernedRefusal::Conflict),
        "the window is closed"
    );
    assert_eq!(
        ledger
            .consume(&consumption("d1"), "engine", NOW + 1)
            .unwrap()
            .status,
        GovernedStatus::Consumed
    );
    assert_eq!(
        ledger.consume(&consumption("d1"), "engine", NOW + 2),
        Err(GovernedRefusal::Conflict),
        "single use"
    );
    assert!(ledger.audit().verify().is_ok());
    assert_eq!(ledger.audit().entries().count(), 3);
}

#[test]
fn a_stranger_cannot_revoke_but_the_proposer_or_an_approver_can() {
    let mut ledger = proposed();
    assert_eq!(
        ledger.revoke(&actor("usr:eve", &[PROPOSE], false), "c1", NOW),
        Err(GovernedRefusal::NotAuthorized)
    );
    let mut by_approver = ledger.clone();
    assert!(by_approver
        .revoke(&actor("usr:bob", &[APPROVE], false), "c1", NOW)
        .is_ok());
    let revoked = ledger
        .revoke(&actor("svc:au", &[], false), "c1", NOW)
        .unwrap();
    assert_eq!(revoked.status, GovernedStatus::Revoked);
    assert_eq!(
        ledger.approve(&actor("usr:bob", &[APPROVE], false), &approval("d1"), NOW),
        Err(GovernedRefusal::Conflict)
    );
}

#[test]
fn an_unapproved_proposal_expires() {
    let ledger = proposed();
    assert_eq!(
        ledger.get("c1", NOW + 60_000).unwrap().status,
        GovernedStatus::Expired
    );
    assert_eq!(
        ledger.get("c1", NOW).unwrap().status,
        GovernedStatus::Proposed
    );
}
