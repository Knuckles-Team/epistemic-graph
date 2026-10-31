use super::*;
use medulla_types::current_plan::sample_task_plan_publish_request;
use medulla_types::decision::task_plan::plan_explicit_tasks;

fn attached_request(revision: u64) -> PublishCurrentPlanRequest {
    let mut request = request(revision);
    let fixture = sample_task_plan_publish_request();
    request.snapshot.task_plan = fixture.snapshot.task_plan;
    request.task_plan_proposal = fixture.task_plan_proposal;
    request
}

// spec: MEDULLA-DURABLE-KERNEL-R028.5.2
#[test]
fn current_plan_attachment_is_immutable_and_scaffold_updates_preserve_it() {
    for first_revision in [0, 1] {
        let tmp = open(&format!("current-plan-attachment-{first_revision}"));
        let shard = &tmp.shard;
        seed(shard);
        if first_revision > 0 {
            apply(shard, "zero", &request(0), "owner", 2000).unwrap();
        }
        let first = attached_request(first_revision);
        apply(shard, "attach", &first, "owner", 2000).unwrap();
        assert_eq!(read(shard, "owner", 2000), Some(first.snapshot.clone()));

        let mut explicit = first.clone();
        explicit.snapshot.revision += 1;
        explicit.idempotency_key = "same-plan-new-revision".into();
        apply(shard, "same-plan", &explicit, "owner", 2000).unwrap();

        let scaffold = request(first_revision + 2);
        let receipt = apply(shard, "scaffold", &scaffold, "owner", 2000).unwrap();
        let mut resolved = scaffold.clone();
        resolved.snapshot.task_plan = first.snapshot.task_plan;
        assert_eq!(read(shard, "owner", 2000), Some(resolved.snapshot.clone()));
        assert_eq!(
            receipt.payload_digest,
            hex::encode(Sha256::digest(serde_json::to_vec(&resolved).unwrap()))
        );
        assert_ne!(
            receipt.payload_digest,
            hex::encode(Sha256::digest(serde_json::to_vec(&scaffold).unwrap()))
        );

        // Expiry hides the view, but never deletes its persisted attachment.
        let persisted = stored_plan(shard);
        assert!(read(shard, "owner", 9000).is_none());
        assert_eq!(persisted, stored_plan(shard));
        let mut refreshed = request(first_revision + 3);
        refreshed.snapshot.observed_at_ms = 9000;
        refreshed.snapshot.expires_at_ms = 9500;
        apply(shard, "refresh-scaffold", &refreshed, "owner", 9000).unwrap();
        assert_eq!(
            read(shard, "owner", 9000).unwrap().task_plan,
            resolved.snapshot.task_plan
        );
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R028.5.2
#[test]
fn current_plan_attachment_refusals_preserve_rows() {
    let tmp = open("current-plan-attachment-refusals");
    let shard = &tmp.shard;
    seed(shard);
    apply(shard, "attach", &attached_request(0), "owner", 2000).unwrap();
    let id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
    let row_before =
        super::super::super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none())
            .unwrap()
            .1;
    let private_before = stored_plan(shard);

    let mut changed = attached_request(1);
    changed.task_plan_proposal.as_mut().unwrap().max_concurrency = 1;
    changed.snapshot.task_plan =
        Some(plan_explicit_tasks(changed.task_plan_proposal.as_ref().unwrap()).unwrap());
    assert!(changed.validate().is_ok());
    let mut reordered = attached_request(1);
    let proposal = reordered.task_plan_proposal.as_mut().unwrap();
    proposal.tasks =
        medulla_types::contract::BoundedVec::new(proposal.tasks.iter().cloned().rev().collect())
            .unwrap();
    reordered.snapshot.task_plan = Some(plan_explicit_tasks(proposal).unwrap());
    assert_eq!(
        reordered.snapshot.task_plan.as_ref().unwrap().plan_digest,
        attached_request(0).snapshot.task_plan.unwrap().plan_digest
    );
    assert!(reordered.validate().is_ok());
    let mut malformed = attached_request(1);
    malformed.snapshot.task_plan.as_mut().unwrap().input_digest =
        format!("sha256:{}", "0".repeat(64));
    let mut wrong_worker = attached_request(1);
    wrong_worker.worker_id = "other".into();
    let mut wrong_epoch = attached_request(1);
    wrong_epoch.lease_epoch += 1;
    let mut wrong_token = attached_request(1);
    wrong_token.fencing_token += 1;
    let mut wrong_tenant = attached_request(1);
    wrong_tenant.tenant = "other".into();
    let mut wrong_run = attached_request(1);
    wrong_run.snapshot.run_id = "other".into();
    for (index, req) in [
        changed,
        reordered,
        malformed,
        wrong_worker,
        wrong_epoch,
        wrong_token,
        wrong_tenant,
        wrong_run,
    ]
    .into_iter()
    .enumerate()
    {
        assert!(apply(shard, &format!("refuse-{index}"), &req, "owner", 2000).is_err());
    }
    assert!(apply(shard, "foreign", &attached_request(1), "other", 2000).is_err());
    assert!(apply(shard, "expired", &attached_request(1), "owner", 9000).is_err());
    assert!(apply(
        shard,
        "exact-lease-deadline",
        &attached_request(1),
        "owner",
        10000
    )
    .is_err());
    assert_eq!(private_before, stored_plan(shard));
    assert_eq!(
        row_before,
        super::super::super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none())
            .unwrap()
            .1
    );
    assert!(read(shard, "other", 2000).is_none());
}

// spec: MEDULLA-DURABLE-KERNEL-R028.5.2
#[test]
fn terminal_or_superseded_lease_hides_but_does_not_erase_attachment() {
    for (field, value) in [
        ("status", serde_json::json!("succeeded")),
        ("status", serde_json::json!("failed")),
        ("status", serde_json::json!("cancelled")),
        ("status", serde_json::json!("dead_letter")),
        ("fencing_token", serde_json::json!(3)),
        ("lease_expires_at", serde_json::json!(2.0)),
    ] {
        let tmp = open(&format!("current-plan-lifecycle-{field}"));
        let shard = &tmp.shard;
        seed(shard);
        apply(shard, "attach", &attached_request(0), "owner", 2000).unwrap();
        let private_before = stored_plan(shard);
        with_nodes(shard, "lifecycle", |nodes| {
            let id = run_owner::work_item_id(GRAPH, "tenant", "run")?;
            let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
            let mut row: Row = decode_durable(bytes.value())?;
            drop(bytes);
            assert!(!row.contains_key("task_plan"));
            row.insert(field.into(), value);
            write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
        });
        assert!(read(shard, "owner", 2000).is_none());
        assert!(apply(
            shard,
            "stale-publication",
            &attached_request(1),
            "owner",
            2000
        )
        .is_err());
        assert_eq!(private_before, stored_plan(shard));
    }
}
