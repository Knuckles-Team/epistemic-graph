use super::super::test_shard::{open, with_nodes, GRAPH};
use super::*;

fn request(revision: u64) -> PublishCurrentPlanRequest {
    PublishCurrentPlanRequest {
        tenant: "tenant".into(),
        worker_id: "worker".into(),
        lease_epoch: 1,
        fencing_token: 2,
        idempotency_key: format!("publish-{revision}"),
        snapshot: CurrentPlanSnapshot {
            run_id: "run".into(),
            revision,
            observed_at_ms: 1000,
            expires_at_ms: 9000,
            items: vec![],
        },
    }
}
fn seed(shard: &Shard) {
    let id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
    super::super::test_shard::with_write(shard, "seed", 1000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        let mut row = serde_json::json!({"node_type":"WorkItem", "tenant":"tenant", "status":"running", "lease_owner":"worker", "lease_epoch":1, "fencing_token":2, "lease_expires_at":10.0, "run_owner_binding":{"graph":GRAPH,"tenant":"tenant","run_id":"run","work_item_id":id,"principal":"owner"}}).as_object().unwrap().clone();
        work_item_capability::record_native_claim_in_wtx(&mut rows.native_work_items, GRAPH, &id, &row, 1000, DurableCrypto::none())?;
        write_work_item_props(&mut rows.nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    }).unwrap();
}
fn apply(
    shard: &Shard,
    tag: &str,
    req: &PublishCurrentPlanRequest,
    actor: &str,
    now: u64,
) -> Result<CurrentPlanPublished, String> {
    super::super::test_shard::with_write(shard, tag, now, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        publish(
            GRAPH,
            req,
            Some(actor),
            &mut rows,
            DurableCrypto::none(),
            now,
        )
    })
}
fn read(shard: &Shard, actor: &str, now: u64) -> Option<CurrentPlanSnapshot> {
    read_current(
        shard,
        GRAPH,
        "tenant",
        "run",
        actor,
        DurableCrypto::none(),
        now,
    )
    .unwrap()
}
// spec: EG-DURABLE-KERNEL-R028.5
#[test]
fn current_plan_publication_readback() {
    let tmp = open("current-plan-roundtrip");
    let shard = &tmp.shard;
    seed(shard);
    assert!(read(shard, "owner", 2000).is_none());
    apply(shard, "zero", &request(0), "owner", 2000).unwrap();
    let mut next = request(2);
    next.snapshot.items = vec![
        eg_types::current_plan::PlanItem {
            text: "first".into(),
            status: eg_types::current_plan::PlanStatus::Done,
        },
        eg_types::current_plan::PlanItem {
            text: "second".into(),
            status: eg_types::current_plan::PlanStatus::Pending,
        },
    ];
    apply(shard, "two", &next, "owner", 2000).unwrap();
    assert_eq!(read(shard, "owner", 2000), Some(next.snapshot));
    assert!(read(shard, "other", 2000).is_none());
    assert!(read(shard, "owner", 9000).is_none());
    with_nodes(shard, "supersede", |nodes| {
        let id = run_owner::work_item_id(GRAPH, "tenant", "run")?;
        let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
        let mut row: Row = decode_durable(bytes.value())?;
        drop(bytes);
        row.insert("fencing_token".into(), 3.into());
        write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    });
    assert!(read(shard, "owner", 2000).is_none());
}
// spec: EG-DURABLE-KERNEL-R028.5
#[test]
fn current_plan_refusals_preserve_row() {
    let tmp = open("current-plan-refusals");
    let shard = &tmp.shard;
    seed(shard);
    assert!(apply(shard, "late-initial", &request(1), "owner", 2000).is_err());
    apply(shard, "zero", &request(0), "owner", 2000).unwrap();
    let id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
    let before = super::super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none())
        .unwrap()
        .1;
    let private_before = stored_plan(shard);
    let mut wrong_worker = request(1);
    wrong_worker.worker_id = "other".into();
    let mut wrong_epoch = request(1);
    wrong_epoch.lease_epoch = 2;
    let mut wrong_token = request(1);
    wrong_token.fencing_token = 3;
    let mut future = request(1);
    future.snapshot.observed_at_ms = 3000;
    for (i, req) in [request(0), wrong_worker, wrong_epoch, wrong_token, future]
        .into_iter()
        .enumerate()
    {
        assert!(apply(shard, &format!("bad-{i}"), &req, "owner", 2000).is_err());
    }
    assert!(apply(shard, "foreign", &request(1), "other", 2000).is_err());
    assert!(apply(shard, "deadline", &request(1), "owner", 10000).is_err());
    let after = super::super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none())
        .unwrap()
        .1;
    assert_eq!(before, after);
    assert_eq!(private_before, stored_plan(shard));
}

// spec: EG-DURABLE-KERNEL-R028.5
#[test]
fn renewal_does_not_extend_snapshot_and_generic_nodes_carry_no_plan() {
    let tmp = open("current-plan-private");
    let shard = &tmp.shard;
    seed(shard);
    let mut too_long = request(0);
    too_long.snapshot.expires_at_ms = 11000;
    assert!(apply(shard, "too-long", &too_long, "owner", 2000).is_err());
    apply(shard, "zero", &request(0), "owner", 2000).unwrap();
    let id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
    with_nodes(shard, "renew", |nodes| {
        let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
        let mut row: Row = decode_durable(bytes.value())?;
        drop(bytes);
        assert!(!row.contains_key("current_plan"));
        row.insert("lease_expires_at".into(), 20.0.into());
        write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    });
    assert!(read(shard, "owner", 9000).is_none());
}

fn stored_plan(shard: &Shard) -> Vec<u8> {
    let handle = shard.graph(GRAPH).unwrap();
    let read = shard.read(&handle).unwrap();
    let plans = read.scoped_owner_table(CURRENT_PLANS).unwrap();
    let id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
    let value = plans.get((GRAPH, id.as_str())).unwrap().unwrap();
    value.value().to_vec()
}
