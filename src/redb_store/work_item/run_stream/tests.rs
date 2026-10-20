use super::super::super::test_shard::{open, with_write, GRAPH};
use super::*;

fn actor() -> String {
    format!("principal:sha256:{}", "a".repeat(64))
}
fn seed(shard: &Shard) -> String {
    let id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
    with_write(shard, "seed-run-stream", 1000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut tables = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        let mut row = serde_json::json!({"node_type":"WorkItem","tenant":"tenant","kind":"orchestrator_task","status":"running","lease_owner":"worker","lease_epoch":1,"fencing_token":2,"lease_expires_at":10.0,"run_owner_binding":{"graph":GRAPH,"tenant":"tenant","run_id":"run","work_item_id":id,"principal":actor()}}).as_object().unwrap().clone();
        work_item_capability::record_native_claim_in_wtx(&mut tables.native_work_items, GRAPH, &id, &row, 1000, DurableCrypto::none())?;
        write_work_item_props(&mut tables.nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    }).unwrap();
    id
}
fn request(id: &str, index: u64) -> AppendRunEventRequest {
    let content = format!("{{\"text\":\"chunk {index}\"}}");
    AppendRunEventRequest {
        tenant: "tenant".into(),
        work_item_id: id.into(),
        run_id: "run".into(),
        worker_id: "worker".into(),
        lease_epoch: 1,
        fencing_token: 2,
        expected_index: index,
        idempotency_key: format!("event-{index}"),
        event: RunStreamPayload {
            kind: RunStreamKind::TextDelta,
            payload_digest: hex::encode(Sha256::digest(content.as_bytes())),
            content,
        },
    }
}
fn batch(shard: &Shard, req: &AppendRunEventRequest, key: &str) -> MutationBatch {
    let handle = shard.graph(GRAPH).unwrap();
    let read = shard.read(&handle).unwrap();
    let mut batch =
        crate::redb_store::control_fixture_batch(crate::redb_store::ControlFixtureBatch {
            identity: read.scope().clone(),
            batch_id: key.into(),
            actor: actor(),
            request_id: 1,
            idempotency_key: key.into(),
            expected_version: eg_transaction::version(&read).unwrap(),
            method: Method::AppendRunEvent {
                request: req.clone(),
            },
            created_at_ms: 2000,
        });
    batch.outbox = vec![eg_types::MutationOutboxIntent {
        topic: RUN_STREAM_TOPIC.into(),
        key: key.into(),
        payload: rmp_serde::to_vec_named(&RunEventReference {
            work_item_id: req.work_item_id.clone(),
            run_id: req.run_id.clone(),
            event_index: req.expected_index + 1,
            payload_digest: req.event.payload_digest.clone(),
        })
        .unwrap(),
        headers: Default::default(),
    }];
    batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1; 32]))
        .unwrap();
    batch
}
fn commit(
    shard: &Shard,
    batch: &MutationBatch,
) -> Result<crate::redb_store::MutationBatchCommit, String> {
    #[cfg(feature = "security")]
    let mut audit = crate::redb_store::AuditTailCache::new();
    crate::redb_store::commit_mutation_batch(
        shard,
        GRAPH,
        batch,
        None,
        2000,
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut audit,
    )
}
fn read_request(id: &str) -> GetRunEventsRequest {
    GetRunEventsRequest {
        tenant: "tenant".into(),
        work_item_id: id.into(),
        run_id: "run".into(),
        after_cursor: None,
        limit: 1,
    }
}

// spec: EG-DURABLE-KERNEL-R028.2
#[test]
fn real_commit_replays_after_reopen_and_isolates_owner() {
    let tmp = open("durable-lifecycle-reopen");
    let id = seed(&tmp.shard);
    let first = batch(&tmp.shard, &request(&id, 0), "event-one");
    assert!(!commit(&tmp.shard, &first).unwrap().replayed);
    assert!(commit(&tmp.shard, &first).unwrap().replayed);
    let second = batch(&tmp.shard, &request(&id, 1), "event-two");
    commit(&tmp.shard, &second).unwrap();
    let req = read_request(&id);
    let page = read_events(&tmp.shard, GRAPH, &req, &actor(), DurableCrypto::none())
        .unwrap()
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.next_index, 2);
    assert!(page.has_more);
    assert_eq!(
        page.events[0].event,
        first
            .operations
            .iter()
            .find_map(|operation| match &operation.method {
                Method::AppendRunEvent { request } => Some(request.event.clone()),
                _ => None,
            })
            .unwrap()
    );
    assert!(
        read_events(&tmp.shard, GRAPH, &req, "other", DurableCrypto::none())
            .unwrap()
            .is_none()
    );
    let mut next = req.clone();
    next.after_cursor = page.next_cursor.clone();
    let second_page = read_events(&tmp.shard, GRAPH, &next, &actor(), DurableCrypto::none())
        .unwrap()
        .unwrap();
    assert_eq!(second_page.events[0].event_index, 2);
    assert!(second_page.events[0].commit_sequence > page.events[0].commit_sequence);
    let path = tmp.path.clone();
    let reopened = Shard::open(&path).unwrap();
    assert_eq!(
        page,
        read_events(&reopened, GRAPH, &req, &actor(), DurableCrypto::none())
            .unwrap()
            .unwrap()
    );
    let mut forged = next;
    forged.after_cursor = Some(format!("eg-run-v1:1:{}", "0".repeat(64)));
    assert!(read_events(&reopened, GRAPH, &forged, &actor(), DurableCrypto::none()).is_err());
}

// spec: EG-DURABLE-KERNEL-R028.2
#[test]
fn stale_cas_and_bad_intent_rollback_private_payload_and_head() {
    let tmp = open("durable-lifecycle-cas");
    let id = seed(&tmp.shard);
    let first = batch(&tmp.shard, &request(&id, 0), "one");
    commit(&tmp.shard, &first).unwrap();
    let stale = batch(&tmp.shard, &request(&id, 0), "stale");
    assert!(commit(&tmp.shard, &stale).is_err());
    let mut wrong = request(&id, 1);
    wrong.fencing_token = 3;
    assert!(commit(&tmp.shard, &batch(&tmp.shard, &wrong, "fenced")).is_err());
    let mut corrupt = batch(&tmp.shard, &request(&id, 1), "corrupt");
    corrupt.outbox[0].payload = vec![0];
    corrupt
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1; 32]))
        .unwrap();
    assert!(commit(&tmp.shard, &corrupt).is_err());
    let page = read_events(
        &tmp.shard,
        GRAPH,
        &read_request(&id),
        &actor(),
        DurableCrypto::none(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(page.next_index, 1);
    commit(&tmp.shard, &batch(&tmp.shard, &request(&id, 1), "good")).unwrap();
    assert_eq!(
        read_events(
            &tmp.shard,
            GRAPH,
            &read_request(&id),
            &actor(),
            DurableCrypto::none()
        )
        .unwrap()
        .unwrap()
        .next_index,
        2
    );
}
