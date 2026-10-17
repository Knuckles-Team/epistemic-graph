//! Store-level proof of the control-lease lifecycle over a real shard.

use super::*;
use eg_types::control_lease::ControlLeaseStatus;
use eg_types::control_lease::ControlLeaseTarget;

use super::super::test_shard::{open, with_nodes, GRAPH};

fn issue(lease_id: &str, tenant: &str) -> IssueControlLeaseRequest {
    let mut grant = serde_json::Map::new();
    grant.insert("tool_ids".into(), serde_json::json!(["click", "type"]));
    IssueControlLeaseRequest {
        pending_call_registration: None,
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
        pending_call_consumption: None,
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

fn native_call(
    shard: &Shard,
    tag: &str,
    method: &Method,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    use super::super::test_shard::with_write;
    use crate::redb_store::store_rows::GraphRowTables;
    with_write(shard, tag, 2_000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = GraphRowTables::open(member)?;
        apply_control_lease_rows(
            GRAPH,
            method,
            &mut rows.nodes,
            &rows.native_work_items,
            2_000,
            DurableCrypto::none(),
        )
    })
}

fn issued(shard: &Shard, tag: &str, request: IssueControlLeaseRequest) -> ControlLeaseIssued {
    decode(native_call(shard, tag, &Method::IssueControlLease { request }).unwrap())
}

fn transitioned(
    shard: &Shard,
    tag: &str,
    request: TransitionControlLeaseRequest,
) -> ControlLeaseTransition {
    decode(native_call(shard, tag, &Method::TransitionControlLease { request }).unwrap())
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
            pending_call_registration: None,
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
            pending_call_registration: None,
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

// spec: EG-DURABLE-KERNEL-R039.2
#[test]
fn native_record_consumption_checks_commit_time_and_preserves_expired_rows() {
    use super::super::market::{apply_native_record_rows, NativeRecordRequest};
    use super::super::test_shard::with_write;
    use crate::redb_store::store_rows::GraphRowTables;

    let temp = open("lease-commit-expiry");
    for (index, now_ms) in [300_999, 301_000, 301_001, 901_000].into_iter().enumerate() {
        let id = format!("lease-expiry-{index}");
        issued(
            &temp.shard,
            &format!("issue-{index}"),
            issue(&id, "tenant-a"),
        );
        let before = get(&temp.shard, "tenant-a", &id).unwrap();
        let method = Method::TransitionControlLease {
            request: end(&id, before.revision, ControlLeaseTarget::Consumed),
        };
        let result: ControlLeaseTransition = decode(
            with_write(
                &temp.shard,
                &format!("consume-{index}"),
                now_ms,
                |admitted| {
                    let member = admitted.graph(GRAPH).unwrap();
                    let mut rows = GraphRowTables::open(member).unwrap();
                    apply_native_record_rows(NativeRecordRequest {
                        graph: GRAPH,
                        batch_id: "lease-expiry-test",
                        method: &method,
                        nodes: &mut rows.nodes,
                        native_work_items: &rows.native_work_items,
                        edges: &mut rows.edges,
                        command_sequences: &mut rows.command_sequences,
                        committed_at_ms: now_ms,
                        crypto: DurableCrypto::none(),
                    })
                },
            )
            .unwrap(),
        );
        if now_ms < before.expires_at_ms {
            assert_eq!(result.outcome, ControlLeaseTransitionOutcome::Applied);
            assert_eq!(result.lease.unwrap().status, ControlLeaseStatus::Consumed);
        } else {
            assert_eq!(result.outcome, ControlLeaseTransitionOutcome::Conflict);
            assert!(result.changed_work_item_ids.is_empty());
            assert_eq!(result.lease, Some(before.clone()));
            assert_eq!(get(&temp.shard, "tenant-a", &id), Some(before));
            let revoked: ControlLeaseTransition = decode(with_nodes(
                &temp.shard,
                &format!("revoke-{index}"),
                |nodes| {
                    apply_transition_control_lease_row(
                        GRAPH,
                        &end(&id, 1, ControlLeaseTarget::Revoked),
                        nodes,
                        now_ms,
                        DurableCrypto::none(),
                    )
                },
            ));
            assert_eq!(revoked.outcome, ControlLeaseTransitionOutcome::Applied);
        }
    }
}

fn pending_fixture(shard: &Shard) -> IssueControlLeaseRequest {
    let request = pending_nodes_fixture(shard);
    use super::super::test_shard::with_write;
    use crate::redb_store::store_rows::GraphRowTables;
    with_write(shard, "seed-native-provenance", 1_000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = GraphRowTables::open(member)?;
        let work = load_row(&rows.nodes, GRAPH, "work-a", DurableCrypto::none())?.unwrap();
        crate::redb_store::work_item_capability::record_native_claim(
            &mut rows.native_work_items,
            GRAPH,
            "work-a",
            &work,
            1_000,
            DurableCrypto::none(),
        )
    })
    .unwrap();
    request
}

fn pending_nodes_fixture(shard: &Shard) -> IssueControlLeaseRequest {
    use eg_types::control_lease::{PendingCallFence, PendingCallRegistration, PENDING_CALL_KIND};
    with_nodes(shard, "seed-pending-work", |nodes| {
        let mut work = serde_json::json!({
            "node_type": "WorkItem", "tenant": "tenant-a", "status": "running",
            "lease_owner": "worker-a", "lease_epoch": 2, "fencing_token": 3,
            "attempt": 1, "work_item_fence": "native-fence",
            "lease_expires_at": 301.0, "metadata": {"task": "task-a"}
        })
        .as_object()
        .unwrap()
        .clone();
        write_work_item_props(nodes, GRAPH, "work-a", &mut work, DurableCrypto::none())
    });
    let mut request = issue("pending-a", "tenant-a");
    request.kind = PENDING_CALL_KIND.into();
    request.grant = serde_json::json!({
        "task_id": "task-a", "parent_work_item_id": "parent-a", "child_work_item_id": "work-a",
        "op_id": "tool-a", "params_digest": "immutable-source-digest"
    })
    .as_object()
    .unwrap()
    .clone();
    request.pending_call_registration = Some(PendingCallRegistration {
        work_item: PendingCallFence {
            work_item_id: "work-a".into(),
            expected_revision: 1,
            expected_metadata: serde_json::json!({"task": "task-a"})
                .as_object()
                .unwrap()
                .clone(),
        },
        expected_lease: eg_types::epistemic_operations_ext::CasWorkItemMetadataLeaseFence {
            worker_ref: "worker-a".into(),
            lease_epoch: 2,
            fencing_token: 3,
        },
    });
    request
}

fn pending_consume(issue: &IssueControlLeaseRequest) -> TransitionControlLeaseRequest {
    let mut request = end("pending-a", 1, ControlLeaseTarget::Consumed);
    let mut work_item = issue
        .pending_call_registration
        .as_ref()
        .unwrap()
        .work_item
        .clone();
    work_item.expected_revision = 2;
    request.pending_call_consumption = Some(eg_types::control_lease::PendingCallConsumption {
        work_item,
        expected_grant: issue.grant.clone(),
    });
    request
}

/// Read the WorkItem row back. Each read is its own maintenance write with a
/// per-attempt op id: a repeated op id is a first-wins replay, and a replayed
/// member refuses owner rows.
fn stored_work(shard: &Shard) -> NodeRow {
    static READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let read = READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    with_nodes(shard, &format!("read-pending-work-{read}"), |nodes| {
        load_row(nodes, GRAPH, "work-a", DurableCrypto::none()).map(Option::unwrap)
    })
}

fn registered_pending_fixture(
    tag: &str,
) -> (
    super::super::test_shard::TempShard,
    IssueControlLeaseRequest,
) {
    let temp = open(tag);
    let request = pending_fixture(&temp.shard);
    issued(&temp.shard, "register", request.clone());
    (temp, request)
}

fn assert_registration_unchanged(shard: &Shard, before: &NodeRow) {
    assert_eq!(&stored_work(shard), before);
    assert!(get(shard, "tenant-a", "pending-a").is_none());
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_registration_and_consumption_change_both_rows_once() {
    let temp = open("pending-pair");
    let request = pending_fixture(&temp.shard);
    let result = issued(&temp.shard, "register", request.clone());
    assert_eq!(result.changed_work_item_ids, ["work-a", "pending-a"]);
    let before = stored_work(&temp.shard);
    assert_eq!(before["pending_call_lease_ref"], "pending-a");
    assert_eq!(
        issued(&temp.shard, "duplicate", request.clone()).outcome,
        ControlLeaseIssueOutcome::Collision
    );
    assert_eq!(stored_work(&temp.shard), before);
    let consume = pending_consume(&request);
    let result = transitioned(&temp.shard, "consume", consume.clone());
    assert_eq!(result.outcome, ControlLeaseTransitionOutcome::Applied);
    assert_eq!(result.changed_work_item_ids, ["work-a", "pending-a"]);
    let after = stored_work(&temp.shard);
    assert!(!after.contains_key("pending_call_lease_ref"));
    assert_eq!(after["row_revision"], 3);
    assert_eq!(
        transitioned(&temp.shard, "replay", consume).outcome,
        ControlLeaseTransitionOutcome::Conflict
    );
    assert_eq!(stored_work(&temp.shard), after);
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_registration_refuses_stale_worker_without_writing_either_row() {
    let temp = open("pending-register-fenced");
    let mut request = pending_fixture(&temp.shard);
    request
        .pending_call_registration
        .as_mut()
        .unwrap()
        .expected_lease
        .fencing_token = 99;
    let before = stored_work(&temp.shard);
    let result = with_nodes(&temp.shard, "refused-register", |nodes| {
        Ok(apply_issue_control_lease_row(
            GRAPH,
            &request,
            nodes,
            1_000,
            DurableCrypto::none(),
        ))
    });
    assert!(result.is_err());
    assert_registration_unchanged(&temp.shard, &before);
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_consume_refuses_missing_fence_changed_envelope_or_metadata_atomically() {
    let (temp, request) = registered_pending_fixture("pending-consume-fenced");
    let work = stored_work(&temp.shard);
    let lease = get(&temp.shard, "tenant-a", "pending-a");
    let valid = pending_consume(&request);
    let mut cases = vec![end("pending-a", 1, ControlLeaseTarget::Consumed)];
    let mut changed_grant = valid.clone();
    changed_grant
        .pending_call_consumption
        .as_mut()
        .unwrap()
        .expected_grant
        .insert("op_id".into(), "other".into());
    cases.push(changed_grant);
    let mut changed_metadata = valid.clone();
    changed_metadata
        .pending_call_consumption
        .as_mut()
        .unwrap()
        .work_item
        .expected_metadata
        .clear();
    cases.push(changed_metadata);
    let mut changed_revision = valid;
    changed_revision
        .pending_call_consumption
        .as_mut()
        .unwrap()
        .work_item
        .expected_revision = 1;
    cases.push(changed_revision);
    for (index, consume) in cases.iter().enumerate() {
        let result = with_nodes(&temp.shard, &format!("refuse-{index}"), |nodes| {
            Ok(apply_transition_control_lease_row(
                GRAPH,
                consume,
                nodes,
                1_500,
                DurableCrypto::none(),
            ))
        });
        assert!(result.is_err());
        assert_eq!(stored_work(&temp.shard), work);
        assert_eq!(get(&temp.shard, "tenant-a", "pending-a"), lease);
    }
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_expiry_preserves_pair_and_revocation_clears_it() {
    let (temp, request) = registered_pending_fixture("pending-expired");
    let work = stored_work(&temp.shard);
    let lease = get(&temp.shard, "tenant-a", "pending-a");
    let consume = pending_consume(&request);
    let result: ControlLeaseTransition =
        decode(with_nodes(&temp.shard, "expired-consume", |nodes| {
            apply_transition_control_lease_row(
                GRAPH,
                &consume,
                nodes,
                request.expires_at_ms,
                DurableCrypto::none(),
            )
        }));
    assert_eq!(result.outcome, ControlLeaseTransitionOutcome::Conflict);
    assert_eq!(stored_work(&temp.shard), work);
    assert_eq!(get(&temp.shard, "tenant-a", "pending-a"), lease);
    let result = transitioned(
        &temp.shard,
        "revoke",
        end("pending-a", 1, ControlLeaseTarget::Revoked),
    );
    assert_eq!(result.outcome, ControlLeaseTransitionOutcome::Applied);
    assert!(!stored_work(&temp.shard).contains_key("pending_call_lease_ref"));
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_consume_refuses_a_reclaimed_worker_even_with_fresh_row_revision() {
    for expired in [false, true] {
        let temp = open(if expired {
            "pending-worker-expired"
        } else {
            "pending-reclaimed"
        });
        let request = pending_fixture(&temp.shard);
        issued(&temp.shard, "register", request.clone());
        with_nodes(&temp.shard, "reclaim", |nodes| {
            let mut work = load_row(nodes, GRAPH, "work-a", DurableCrypto::none())?.unwrap();
            if expired {
                work.insert("lease_expires_at".into(), 1.5.into());
            } else {
                work.insert("fencing_token".into(), 4.into());
            }
            write_work_item_props(nodes, GRAPH, "work-a", &mut work, DurableCrypto::none())
        });
        let before = stored_work(&temp.shard);
        let mut consume = pending_consume(&request);
        consume
            .pending_call_consumption
            .as_mut()
            .unwrap()
            .work_item
            .expected_revision = 3;
        let result = with_nodes(&temp.shard, "reclaimed-consume", |nodes| {
            Ok(apply_transition_control_lease_row(
                GRAPH,
                &consume,
                nodes,
                1_500,
                DurableCrypto::none(),
            ))
        });
        assert!(result.is_err());
        assert_eq!(stored_work(&temp.shard), before);
        assert_eq!(
            get(&temp.shard, "tenant-a", "pending-a").unwrap().status,
            ControlLeaseStatus::Active
        );
    }
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_pair_refuses_foreign_tenant_and_generic_pointer_writes() {
    use super::super::test_shard::check_guard;
    let (temp, request) = registered_pending_fixture("pending-tenant-guard");
    let before = stored_work(&temp.shard);
    let mut consume = pending_consume(&request);
    consume.tenant = "tenant-b".into();
    assert_eq!(
        transitioned(&temp.shard, "foreign", consume).outcome,
        ControlLeaseTransitionOutcome::NotFound
    );
    let forged = Method::CompareAndSetNodeFields {
        node_id: "work-a".into(),
        conditions_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        updates_msgpack: rmp_serde::to_vec_named(
            &serde_json::json!({"pending_call_lease_ref": "forged"}),
        )
        .unwrap(),
    };
    assert!(check_guard(&temp.shard, "repoint", forged).is_err());
    let plant = Method::AddNode {
        node_id: "forged-work".into(),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
            "node_type": "WorkItem", "pending_call_lease_ref": "pending-a"
        }))
        .unwrap(),
    };
    assert!(check_guard(&temp.shard, "plant", plant).is_err());
    assert_eq!(stored_work(&temp.shard), before);
    assert_eq!(
        get(&temp.shard, "tenant-a", "pending-a").unwrap().status,
        ControlLeaseStatus::Active
    );
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_registration_refuses_a_nodes_only_lease_tuple() {
    let temp = open("pending-no-native-claim");
    let request = pending_nodes_fixture(&temp.shard);
    let before = stored_work(&temp.shard);
    let result = native_call(
        &temp.shard,
        "refuse-no-claim",
        &Method::IssueControlLease { request },
    );
    assert!(result.is_err());
    assert_registration_unchanged(&temp.shard, &before);
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_consumption_refuses_missing_native_provenance() {
    use super::super::test_shard::with_write;
    use crate::redb_store::store_rows::GraphRowTables;
    let (temp, request) = registered_pending_fixture("pending-lost-native-claim");
    with_write(&temp.shard, "remove-native-provenance", 2_000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = GraphRowTables::open(member)?;
        rows.native_work_items.remove((GRAPH, "work-a"))?;
        Ok(())
    })
    .unwrap();
    let work = stored_work(&temp.shard);
    let lease = get(&temp.shard, "tenant-a", "pending-a");
    let result = native_call(
        &temp.shard,
        "refuse-lost-claim",
        &Method::TransitionControlLease {
            request: pending_consume(&request),
        },
    );
    assert!(result.is_err());
    assert_eq!(stored_work(&temp.shard), work);
    assert_eq!(get(&temp.shard, "tenant-a", "pending-a"), lease);
}

// spec: EG-DURABLE-KERNEL-R039.3
#[test]
fn pending_worker_refuses_incomplete_fences_and_fractional_deadline() {
    let temp = open("pending-worker-shape");
    let request = pending_fixture(&temp.shard);
    let work = stored_work(&temp.shard);
    let fence = &request.pending_call_registration.unwrap().expected_lease;
    for field in ["attempt", "lease_epoch", "fencing_token", "work_item_fence"] {
        let mut incomplete = work.clone();
        incomplete.remove(field);
        assert!(!pending_call::live_worker(&incomplete, fence, 2_000));
    }
    let mut fractional = work;
    fractional.insert("lease_expires_at".into(), serde_json::json!(2.0009));
    assert!(!pending_call::live_worker(&fractional, fence, 2_000));
}
