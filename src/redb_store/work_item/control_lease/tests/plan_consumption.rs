use super::*;
use medulla_types::control_lease::PendingPlanPreimage;

fn fixture(
    tag: &str,
) -> (
    super::super::super::test_shard::TempShard,
    IssueControlLeaseRequest,
) {
    let temp = open(tag);
    let mut plan = issue("plan-a", "tenant-a");
    plan.kind = "deployment.plan-kind".into();
    issued(&temp.shard, "plan", plan.clone());
    let mut pending = pending_fixture(&temp.shard);
    pending.pending_call_registration.as_mut().unwrap().plan = Some(PendingPlanPreimage {
        lease_id: plan.lease_id,
        expected_revision: 1,
        expected_kind: plan.kind,
        expected_grant: plan.grant,
    });
    (temp, pending)
}

fn snapshot(shard: &Shard) -> (NodeRow, Option<ControlLeaseView>, Option<ControlLeaseView>) {
    snapshot_for(shard, "work-a", "pending-a")
}

fn snapshot_for(
    shard: &Shard,
    work_id: &str,
    pending_id: &str,
) -> (NodeRow, Option<ControlLeaseView>, Option<ControlLeaseView>) {
    let work =
        super::super::super::read::snapshot_row(shard, GRAPH, work_id, DurableCrypto::none())
            .unwrap()
            .1
            .unwrap();
    (
        work,
        get(shard, "tenant-a", pending_id),
        get(shard, "tenant-a", "plan-a"),
    )
}

// spec: MEDULLA-DURABLE-KERNEL-R039.4, MEDULLA-DURABLE-KERNEL-R039.4.1
#[test]
fn paired_consumption_changes_three_rows_and_replay_changes_none() {
    let (temp, pending) = fixture("plan-paired-consume");
    issued(&temp.shard, "register", pending.clone());
    let consume = pending_consume(&pending);
    let result = transitioned(&temp.shard, "consume", consume.clone());
    assert_eq!(
        result.changed_work_item_ids,
        ["work-a", "plan-a", "pending-a"]
    );
    assert_eq!(
        get(&temp.shard, "tenant-a", "plan-a").unwrap().status,
        ControlLeaseStatus::Consumed
    );
    assert!(!stored_work(&temp.shard).contains_key("pending_call_lease_ref"));
    let after = snapshot(&temp.shard);
    assert_eq!(
        transitioned(&temp.shard, "replay", consume).outcome,
        ControlLeaseTransitionOutcome::Conflict
    );
    assert_eq!(snapshot(&temp.shard), after);
}

// spec: MEDULLA-DURABLE-KERNEL-R039.4, MEDULLA-DURABLE-KERNEL-R039.4.1
#[test]
fn paired_consumption_refuses_omission_and_every_changed_preimage() {
    let (temp, pending) = fixture("plan-paired-preimage");
    issued(&temp.shard, "register", pending.clone());
    let before = snapshot(&temp.shard);
    for field in ["omitted", "id", "revision", "kind", "grant"] {
        let mut request = pending_consume(&pending);
        let pair = &mut request.pending_call_consumption.as_mut().unwrap().plan;
        match field {
            "omitted" => *pair = None,
            "id" => pair.as_mut().unwrap().lease_id = "other".into(),
            "revision" => pair.as_mut().unwrap().expected_revision += 1,
            "kind" => pair.as_mut().unwrap().expected_kind = "other.kind".into(),
            _ => {
                pair.as_mut().unwrap().expected_grant.clear();
            }
        }
        assert!(native_call(
            &temp.shard,
            field,
            &Method::TransitionControlLease { request }
        )
        .is_err());
        assert_eq!(snapshot(&temp.shard), before);
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R039.4, MEDULLA-DURABLE-KERNEL-R039.4.1
#[test]
fn revoked_plan_blocks_pending_consumption_without_partial_changes() {
    let (temp, pending) = fixture("plan-paired-revocation");
    issued(&temp.shard, "register", pending.clone());
    transitioned(
        &temp.shard,
        "revoke-plan",
        end("plan-a", 1, ControlLeaseTarget::Revoked),
    );
    let before = snapshot(&temp.shard);
    assert!(native_call(
        &temp.shard,
        "consume-revoked",
        &Method::TransitionControlLease {
            request: pending_consume(&pending),
        }
    )
    .is_err());
    assert_eq!(snapshot(&temp.shard), before);
}

// spec: MEDULLA-DURABLE-KERNEL-R039.4, MEDULLA-DURABLE-KERNEL-R039.4.1
#[test]
fn registration_refuses_foreign_plan_and_excessive_lifetime() {
    for foreign in [false, true] {
        let (temp, mut pending) = fixture(if foreign {
            "foreign-plan"
        } else {
            "short-plan"
        });
        if foreign {
            let mut other = issue("foreign-plan", "tenant-b");
            other.kind = "deployment.plan-kind".into();
            issued(&temp.shard, "foreign", other);
            pending
                .pending_call_registration
                .as_mut()
                .unwrap()
                .plan
                .as_mut()
                .unwrap()
                .lease_id = "foreign-plan".into();
        } else {
            pending.expires_at_ms += 1;
        }
        let before = snapshot(&temp.shard);
        assert!(native_call(
            &temp.shard,
            "refuse-registration",
            &Method::IssueControlLease { request: pending }
        )
        .is_err());
        assert_eq!(snapshot(&temp.shard), before);
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R039.4, MEDULLA-DURABLE-KERNEL-R039.4.1
#[test]
fn expired_pair_refuses_without_consuming_either_lease() {
    let (temp, pending) = fixture("plan-paired-expiry");
    issued(&temp.shard, "register", pending.clone());
    let before = snapshot(&temp.shard);
    let result: ControlLeaseTransition = decode(
        native_call_at(
            &temp.shard,
            "consume-expired",
            &Method::TransitionControlLease {
                request: pending_consume(&pending),
            },
            pending.expires_at_ms,
        )
        .unwrap(),
    );
    assert_eq!(result.outcome, ControlLeaseTransitionOutcome::Conflict);
    assert_eq!(snapshot(&temp.shard), before);
}

// spec: MEDULLA-DURABLE-KERNEL-R039.4, MEDULLA-DURABLE-KERNEL-R039.4.1
#[test]
fn shared_plan_has_one_winner_and_losing_pending_pair_is_unchanged() {
    use super::super::super::test_shard::with_write;
    use crate::redb_store::store_rows::GraphRowTables;
    let (temp, first) = fixture("shared-plan");
    let mut second = first.clone();
    second.lease_id = "pending-b".into();
    second
        .pending_call_registration
        .as_mut()
        .unwrap()
        .work_item
        .work_item_id = "work-b".into();
    with_write(&temp.shard, "seed-second-native-work", 1_000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = GraphRowTables::open(member)?;
        let mut work = load_row(&rows.nodes, GRAPH, "work-a", DurableCrypto::none())?.unwrap();
        work.remove("row_revision");
        write_work_item_props(
            &mut rows.nodes,
            GRAPH,
            "work-b",
            &mut work,
            DurableCrypto::none(),
        )?;
        crate::redb_store::work_item_capability::record_native_claim(
            &mut rows.native_work_items,
            GRAPH,
            "work-b",
            &work,
            1_000,
            DurableCrypto::none(),
        )
    })
    .unwrap();
    issued(&temp.shard, "register-first", first.clone());
    issued(&temp.shard, "register-second", second.clone());
    assert_eq!(
        transitioned(&temp.shard, "winner", pending_consume(&first)).outcome,
        ControlLeaseTransitionOutcome::Applied
    );
    let before = snapshot_for(&temp.shard, "work-b", "pending-b");
    let mut losing = pending_consume(&second);
    losing.lease_id = "pending-b".into();
    assert!(native_call(
        &temp.shard,
        "loser",
        &Method::TransitionControlLease { request: losing }
    )
    .is_err());
    assert_eq!(snapshot_for(&temp.shard, "work-b", "pending-b"), before);
}

// spec: MEDULLA-DURABLE-KERNEL-R039.4, MEDULLA-DURABLE-KERNEL-R039.4.1
#[test]
fn persisted_plan_drift_refuses_before_any_paired_write() {
    // Fault-inject persisted drift. Legal registration bounds make natural plan
    // expiry no earlier than pending expiry; this isolates the plan-only check.
    for field in ["row_revision", "expires_at_ms", "kind", "grant"] {
        let (temp, pending) = fixture(field);
        issued(&temp.shard, "register", pending.clone());
        with_nodes(&temp.shard, "drift-plan", |nodes| {
            let mut plan = load_row(nodes, GRAPH, "plan-a", DurableCrypto::none())?.unwrap();
            let value = match field {
                "row_revision" => serde_json::json!(99),
                "expires_at_ms" => serde_json::json!(1_999),
                "kind" => serde_json::json!("drifted.kind"),
                _ => serde_json::json!({}),
            };
            plan.insert(field.into(), value);
            // Writing a row normally also advances its revision. Restore the
            // expected revision for non-revision cases to isolate each predicate.
            if field != "row_revision" {
                plan.insert("row_revision".into(), 0.into());
            }
            write_work_item_props(nodes, GRAPH, "plan-a", &mut plan, DurableCrypto::none())
        });
        let before = snapshot(&temp.shard);
        assert!(native_call(
            &temp.shard,
            "consume-drifted",
            &Method::TransitionControlLease {
                request: pending_consume(&pending),
            }
        )
        .is_err());
        assert_eq!(snapshot(&temp.shard), before);
    }
}
