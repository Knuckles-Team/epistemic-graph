//! Store-level proof of the control-lease lifecycle over a real shard.

use super::*;
use eg_types::control_lease::ControlLeaseStatus;
use eg_types::control_lease::ControlLeaseTarget;

use super::super::test_shard::{open, with_nodes, GRAPH};

fn issue(lease_id: &str, tenant: &str) -> IssueControlLeaseRequest {
    let mut grant = serde_json::Map::new();
    grant.insert("tool_ids".into(), serde_json::json!(["click", "type"]));
    IssueControlLeaseRequest {
        tenant: tenant.into(),
        lease_id: lease_id.into(),
        kind: "browser.control".into(),
        grant,
        issued_at_ms: 1_000,
        expires_at_ms: 301_000,
        hard_expires_at_ms: 901_000,
        idempotency_key: format!("issue:{lease_id}"),
    }
}

fn end(
    lease_id: &str,
    expected_revision: u64,
    to: ControlLeaseTarget,
) -> TransitionControlLeaseRequest {
    TransitionControlLeaseRequest {
        tenant: "tenant-a".into(),
        lease_id: lease_id.into(),
        expected_revision,
        to,
        idempotency_key: format!("end:{lease_id}:{expected_revision}:{to:?}"),
    }
}

fn decode<T: serde::de::DeserializeOwned>(payload: Option<crate::protocol::ResultPayload>) -> T {
    match payload.expect("a lease transition always answers") {
        crate::protocol::ResultPayload::Json(value) => serde_json::from_value(value).unwrap(),
        other => panic!("control-lease results are JSON, got {other:?}"),
    }
}

fn issued(shard: &Shard, tag: &str, request: IssueControlLeaseRequest) -> ControlLeaseIssued {
    decode(with_nodes(shard, tag, |nodes| {
        apply_issue_control_lease_row(GRAPH, &request, nodes, DurableCrypto::none())
    }))
}

fn transitioned(
    shard: &Shard,
    tag: &str,
    request: TransitionControlLeaseRequest,
) -> ControlLeaseTransition {
    transitioned_at(shard, tag, request, 1_500, "principal:sha256:test-approver")
}

fn transitioned_at(
    shard: &Shard,
    tag: &str,
    request: TransitionControlLeaseRequest,
    committed_at_ms: u64,
    actor: &str,
) -> ControlLeaseTransition {
    decode(with_nodes(shard, tag, |nodes| {
        apply_transition_control_lease_row(
            GRAPH,
            &request,
            nodes,
            DurableCrypto::none(),
            committed_at_ms,
            actor,
        )
    }))
}

fn get(shard: &Shard, tenant: &str, lease_id: &str) -> Option<ControlLeaseView> {
    read_control_lease(shard, GRAPH, tenant, lease_id, DurableCrypto::none()).unwrap()
}

#[test]
fn an_issued_lease_is_readable_only_by_its_tenant_and_an_id_is_issued_once() {
    let temp = open("lease-issue");
    let first = issued(&temp.shard, "issue", issue("lease-1", "tenant-a"));
    assert_eq!(first.outcome, ControlLeaseIssueOutcome::Issued);
    assert_eq!(first.changed_work_item_ids, ["lease-1"]);
    let view = get(&temp.shard, "tenant-a", "lease-1").expect("own lease is visible");
    assert_eq!(view.status, ControlLeaseStatus::Active);
    assert_eq!(view.revision, 1);
    assert_eq!(view.transition_actor, None);
    assert_eq!(view.transitioned_at_ms, None);
    assert_eq!(Some(view), first.lease);
    assert_eq!(
        get(&temp.shard, "tenant-b", "lease-1"),
        None,
        "cross-tenant read"
    );

    let again = issued(&temp.shard, "collide", issue("lease-1", "tenant-b"));
    assert_eq!(again.outcome, ControlLeaseIssueOutcome::Collision);
    assert!(again.changed_work_item_ids.is_empty());
}

#[test]
fn a_lease_ends_once_on_its_read_revision_and_never_reactivates() {
    let temp = open("lease-end");
    issued(&temp.shard, "issue", issue("lease-2", "tenant-a"));

    let stale = transitioned(
        &temp.shard,
        "stale",
        end("lease-2", 7, ControlLeaseTarget::Revoked),
    );
    assert_eq!(stale.outcome, ControlLeaseTransitionOutcome::Conflict);
    assert_eq!(stale.lease.map(|lease| lease.revision), Some(1));

    let revoked = transitioned(
        &temp.shard,
        "revoke",
        end("lease-2", 1, ControlLeaseTarget::Revoked),
    );
    assert_eq!(revoked.outcome, ControlLeaseTransitionOutcome::Applied);
    assert_eq!(revoked.changed_work_item_ids, ["lease-2"]);
    let view = get(&temp.shard, "tenant-a", "lease-2").unwrap();
    assert_eq!(
        (view.status, view.revision),
        (ControlLeaseStatus::Revoked, 2)
    );
    assert_eq!(
        view.transition_actor.as_deref(),
        Some("principal:sha256:test-approver")
    );
    assert_eq!(view.transitioned_at_ms, Some(1_500));

    let expire = transitioned(
        &temp.shard,
        "expire",
        end("lease-2", 2, ControlLeaseTarget::Expired),
    );
    assert_eq!(
        expire.outcome,
        ControlLeaseTransitionOutcome::Conflict,
        "terminal is terminal"
    );

    let missing = transitioned(
        &temp.shard,
        "missing",
        end("lease-9", 1, ControlLeaseTarget::Expired),
    );
    assert_eq!(missing.outcome, ControlLeaseTransitionOutcome::NotFound);
}

#[test]
fn consume_is_refused_at_expiry_inside_the_revision_cas() {
    let temp = open("lease-expiry-cas");
    issued(&temp.shard, "issue", issue("lease-expiry", "tenant-a"));
    let request = end("lease-expiry", 1, ControlLeaseTarget::Consumed);
    let at_boundary = transitioned_at(
        &temp.shard,
        "at-boundary",
        request.clone(),
        301_000,
        "actor-a",
    );
    assert_eq!(at_boundary.outcome, ControlLeaseTransitionOutcome::Conflict);
    assert!(at_boundary.changed_work_item_ids.is_empty());
    let after_hard = transitioned_at(&temp.shard, "after-hard", request, 901_001, "actor-a");
    assert_eq!(after_hard.outcome, ControlLeaseTransitionOutcome::Conflict);
    let view = get(&temp.shard, "tenant-a", "lease-expiry").unwrap();
    assert_eq!(
        (view.status, view.revision),
        (ControlLeaseStatus::Active, 1)
    );
    assert_eq!(
        view.transition_actor, None,
        "a refused decision writes no actor"
    );

    let revoked = transitioned_at(
        &temp.shard,
        "revoke-expired",
        end("lease-expiry", 1, ControlLeaseTarget::Revoked),
        901_002,
        "actor-b",
    );
    assert_eq!(revoked.outcome, ControlLeaseTransitionOutcome::Applied);
    assert_eq!(
        revoked.lease.unwrap().transition_actor.as_deref(),
        Some("actor-b")
    );
}

#[test]
fn a_single_use_lease_is_consumed_once_and_can_still_be_revoked() {
    let temp = open("lease-consume");
    issued(&temp.shard, "issue", issue("arm-1", "tenant-a"));
    let consumed = transitioned(
        &temp.shard,
        "consume",
        end("arm-1", 1, ControlLeaseTarget::Consumed),
    );
    assert_eq!(consumed.outcome, ControlLeaseTransitionOutcome::Applied);
    let again = transitioned(
        &temp.shard,
        "again",
        end("arm-1", 2, ControlLeaseTarget::Consumed),
    );
    assert_eq!(
        again.outcome,
        ControlLeaseTransitionOutcome::Conflict,
        "single use"
    );
    let revoked = transitioned(
        &temp.shard,
        "revoke",
        end("arm-1", 2, ControlLeaseTarget::Revoked),
    );
    assert_eq!(revoked.outcome, ControlLeaseTransitionOutcome::Applied);
    let view = get(&temp.shard, "tenant-a", "arm-1").unwrap();
    assert_eq!(
        (view.status, view.revision),
        (ControlLeaseStatus::Revoked, 3)
    );
}

#[test]
fn the_pending_approval_queue_lists_only_the_tenants_active_approvals() {
    use eg_types::control_lease::ListControlLeasesRequest;
    let temp = open("lease-list");
    let mut approval = issue("action_approval:1", "tenant-a");
    approval.kind = "action.approval".into();
    issued(&temp.shard, "a1", approval.clone());
    issued(
        &temp.shard,
        "a2",
        IssueControlLeaseRequest {
            lease_id: "action_approval:2".into(),
            idempotency_key: "issue:a2".into(),
            ..approval.clone()
        },
    );
    issued(&temp.shard, "b1", issue("browserlease_1", "tenant-a"));
    issued(
        &temp.shard,
        "c1",
        IssueControlLeaseRequest {
            tenant: "tenant-b".into(),
            lease_id: "action_approval:3".into(),
            ..approval
        },
    );
    transitioned(
        &temp.shard,
        "deny",
        end("action_approval:2", 1, ControlLeaseTarget::Revoked),
    );

    let request = ListControlLeasesRequest {
        tenant: "tenant-a".into(),
        kind: Some("action.approval".into()),
        status: Some(ControlLeaseStatus::Active),
        grant_match: None,
        cursor: None,
        limit: 10,
    };
    let page = list_control_leases(&temp.shard, GRAPH, &request, DurableCrypto::none()).unwrap();
    let ids: Vec<&str> = page
        .leases
        .iter()
        .map(|lease| lease.lease_id.as_str())
        .collect();
    assert_eq!(ids, ["action_approval:1"]);
    assert_eq!(page.next_cursor, None);
}
