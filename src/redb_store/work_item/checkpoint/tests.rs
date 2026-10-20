use super::super::test_shard::{open, with_nodes, with_write, GRAPH};
use super::*;

fn request(expected_revision: Option<u64>) -> PublishRunCheckpointRequest {
    let content = serde_json::json!({"phase":"completed","next_step":expected_revision.unwrap_or(0),"history":["private prompt"],"usage":{}}).to_string();
    PublishRunCheckpointRequest {
        tenant: "tenant".into(),
        work_item_id: run_owner::work_item_id(GRAPH, "tenant", "run").unwrap(),
        run_id: "run".into(),
        worker_id: "worker".into(),
        lease_epoch: 1,
        fencing_token: 2,
        expected_revision,
        idempotency_key: format!("checkpoint-{expected_revision:?}"),
        snapshot: CheckpointContent {
            payload_digest: hex::encode(Sha256::digest(content.as_bytes())),
            content,
            short_term: vec![],
            long_term: vec![],
        },
    }
}
fn seed(shard: &Shard) {
    let id = request(None).work_item_id;
    with_write(shard,"seed",1000,|admitted| {
        let member=admitted.graph(GRAPH)?;
        let mut rows=crate::redb_store::store_rows::GraphRowTables::open(member)?;
        let mut row=serde_json::json!({"node_type":"WorkItem","tenant":"tenant","kind":"orchestrator_task","status":"running","lease_owner":"worker","lease_epoch":1,"fencing_token":2,"lease_expires_at":10.0,"metadata":{"context_id":"context"},"run_owner_binding":{"graph":GRAPH,"tenant":"tenant","run_id":"run","work_item_id":id,"principal":"owner"}}).as_object().unwrap().clone();
        work_item_capability::record_native_claim_in_wtx(&mut rows.native_work_items,GRAPH,&id,&row,1000,DurableCrypto::none())?;
        write_work_item_props(&mut rows.nodes,GRAPH,&id,&mut row,DurableCrypto::none())
    }).unwrap();
}
fn apply(
    shard: &Shard,
    tag: &str,
    req: &PublishRunCheckpointRequest,
    actor: &str,
    now: u64,
) -> Result<RunCheckpointPublished, String> {
    with_write(shard, tag, now, |admitted| {
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
fn get(
    shard: &Shard,
    actor: &str,
    claim: bool,
    now: u64,
) -> Result<Option<RunCheckpointRead>, String> {
    let req = request(None);
    read_current(
        shard,
        GRAPH,
        &GetRunCheckpointRequest {
            tenant: req.tenant,
            work_item_id: req.work_item_id,
            run_id: req.run_id,
            current_claim: claim.then_some(CheckpointClaim {
                worker_id: "worker".into(),
                lease_epoch: 1,
                fencing_token: 2,
            }),
        },
        actor,
        DurableCrypto::none(),
        now,
    )
}
fn count(shard: &Shard) -> usize {
    let handle = shard.graph(GRAPH).unwrap();
    let read = shard.read(&handle).unwrap();
    read.scoped_owner_table(RUN_CHECKPOINTS)
        .unwrap()
        .iter()
        .unwrap()
        .count()
}
// spec: EG-DURABLE-KERNEL-R028.1
#[test]
fn checkpoint_cas_is_atomic_private_and_owner_bound() {
    let tmp = open("checkpoint-roundtrip");
    let shard = &tmp.shard;
    seed(shard);
    assert!(get(shard, "owner", true, 2000).unwrap().is_none());
    let first = apply(shard, "first", &request(None), "owner", 2000).unwrap();
    assert_eq!(first.revision, 1);
    assert!(!first.replayed);
    let view = get(shard, "owner", true, 2000).unwrap().unwrap();
    assert_eq!(view.snapshot.content, request(None).snapshot.content);
    assert_eq!(view.work_item.metadata["context_id"], "context");
    assert!(get(shard, "foreign", false, 2000).unwrap().is_none());
    let before = view.clone();
    for (index, mut bad) in [
        request(None),
        request(Some(1)),
        request(Some(1)),
        request(Some(1)),
    ]
    .into_iter()
    .enumerate()
    {
        match index {
            1 => bad.worker_id = "other".into(),
            2 => bad.lease_epoch = 2,
            3 => bad.fencing_token = 3,
            _ => (),
        }
        assert!(apply(shard, &format!("bad-{index}"), &bad, "owner", 2000).is_err());
        assert_eq!(
            get(shard, "owner", true, 2000).unwrap(),
            Some(before.clone())
        );
        assert_eq!(count(shard), 1);
    }
    assert!(apply(shard, "foreign", &request(Some(1)), "other", 2000).is_err());
    assert!(apply(shard, "expired", &request(Some(1)), "owner", 10000).is_err());
    apply(shard, "second", &request(Some(1)), "owner", 2000).unwrap();
    assert_eq!(count(shard), 2); // immutable prior revision remains retained
    let (_, row) = super::super::read::snapshot_row(
        shard,
        GRAPH,
        &request(None).work_item_id,
        DurableCrypto::none(),
    )
    .unwrap();
    assert!(!serde_json::to_string(&row)
        .unwrap()
        .contains("private prompt"));
    assert_eq!(
        get(shard, "owner", true, 2000)
            .unwrap()
            .unwrap()
            .snapshot
            .revision,
        2
    );
}
// spec: EG-DURABLE-KERNEL-R028.1
#[test]
fn completed_history_survives_claim_expiry_but_resume_requires_live_fence() {
    let tmp = open("checkpoint-retention");
    let shard = &tmp.shard;
    seed(shard);
    apply(shard, "first", &request(None), "owner", 2000).unwrap();
    assert!(get(shard, "owner", true, 10000).is_err());
    assert!(get(shard, "owner", false, 10000).unwrap().is_some());
    with_nodes(shard, "terminal", |nodes| {
        let id = request(None).work_item_id;
        let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
        let mut row: Row = decode_durable(bytes.value())?;
        drop(bytes);
        row.insert("status".into(), "succeeded".into());
        write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    });
    assert!(get(shard, "owner", true, 2000).is_err());
    assert!(get(shard, "owner", false, 2000).unwrap().is_some());
    assert_eq!(count(shard), 1);
}

// spec: EG-DURABLE-KERNEL-R028.1
#[test]
fn aborted_transaction_retains_neither_snapshot_nor_pointer() {
    let tmp = open("checkpoint-rollback");
    let shard = &tmp.shard;
    seed(shard);
    let failure: Result<(), String> = with_write(shard, "abort", 2000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        publish(
            GRAPH,
            &request(None),
            Some("owner"),
            &mut rows,
            DurableCrypto::none(),
            2000,
        )?;
        Err("injected failure before atomic transaction commit".into())
    });
    assert!(failure.is_err());
    assert_eq!(count(shard), 0);
    assert!(get(shard, "owner", true, 2000).unwrap().is_none());
    apply(shard, "retry", &request(None), "owner", 2000).unwrap();
    assert_eq!(count(shard), 1);
}
