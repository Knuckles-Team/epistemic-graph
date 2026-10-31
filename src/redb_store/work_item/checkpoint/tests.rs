use super::super::test_shard::run_fixture::{admission, seed_claim, with_rows};
use super::super::test_shard::{open, with_nodes, with_write, GRAPH};
use super::*;

fn request(expected_revision: Option<u64>) -> PublishRunCheckpointRequest {
    let content = serde_json::json!({"phase":"completed","next_step":expected_revision.unwrap_or(0),"history":["private prompt"],"usage":{}}).to_string();
    PublishRunCheckpointRequest {
        tenant: "tenant".into(),
        work_item_id: run_owner::work_item_id(GRAPH, "tenant", "run").unwrap(),
        run_id: "run".into(),
        context_id: "context".into(),
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
    seed_admission(
        shard,
        admission(serde_json::json!({"a2a_context_id":"context"})),
    );
}

fn seed_admission(shard: &Shard, admitted: medulla_types::native_control::SubmitWorkItemRequest) {
    seed_claim(shard, |row| {
        let metadata = serde_json::to_value(&admitted.metadata).unwrap();
        row.insert("kind".into(), "orchestrator_task".into());
        row.insert("metadata".into(), metadata.clone());
        row.insert(
            run_owner::ADMISSION_REQUEST.into(),
            serde_json::to_value(admitted).unwrap(),
        );
    });
}

fn apply(
    shard: &Shard,
    tag: &str,
    req: &PublishRunCheckpointRequest,
    actor: &str,
    now: u64,
) -> Result<RunCheckpointPublished, String> {
    with_rows(shard, tag, now, |rows| {
        publish(GRAPH, req, Some(actor), rows, DurableCrypto::none(), now)
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
            context_id: req.context_id,
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
        .scope_rows()
        .unwrap()
        .count()
}
// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
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
    assert_eq!(view.work_item.metadata["a2a_context_id"], "context");
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
        let refusal = apply(shard, &format!("bad-{index}"), &bad, "owner", 2000).unwrap_err();
        assert!(refusal.starts_with("CONFLICT: "), "{refusal}");
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
// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn completed_history_survives_claim_expiry_but_resume_requires_live_fence() {
    let tmp = open("checkpoint-retention");
    let shard = &tmp.shard;
    seed(shard);
    apply(shard, "first", &request(None), "owner", 2000).unwrap();
    assert_eq!(
        get(shard, "owner", true, 10000).unwrap_err(),
        "CONFLICT: checkpoint live claim conflict"
    );
    assert!(get(shard, "owner", false, 10000).unwrap().is_some());
    with_nodes(shard, "terminal", |nodes| {
        let id = request(None).work_item_id;
        let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
        let mut row: Row = decode_durable(bytes.value())?;
        drop(bytes);
        row.insert("status".into(), "succeeded".into());
        write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    });
    assert_eq!(
        get(shard, "owner", true, 2000).unwrap_err(),
        "CONFLICT: checkpoint live claim conflict"
    );
    assert!(get(shard, "owner", false, 2000).unwrap().is_some());
    assert_eq!(count(shard), 1);
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
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

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn mutable_metadata_cannot_rebind_checkpoint_context() {
    let tmp = open("checkpoint-context-rebinding");
    let shard = &tmp.shard;
    seed(shard);
    apply(shard, "first", &request(None), "owner", 2000).unwrap();
    let id = request(None).work_item_id;
    with_nodes(shard, "replace-metadata", |nodes| {
        let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
        let mut row: Row = decode_durable(bytes.value())?;
        drop(bytes);
        row.insert(
            "metadata".into(),
            serde_json::json!({"a2a_context_id":"foreign"}),
        );
        write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    });
    let valid = get(shard, "owner", true, 2000).unwrap().unwrap();
    assert_eq!(valid.context_id, "context");
    assert_eq!(valid.work_item.metadata["a2a_context_id"], "foreign");
    let mut foreign = request(Some(1));
    foreign.context_id = "foreign".into();
    assert!(apply(shard, "foreign-context", &foreign, "owner", 2000).is_err());
    let read = GetRunCheckpointRequest {
        tenant: foreign.tenant,
        work_item_id: foreign.work_item_id,
        run_id: foreign.run_id,
        context_id: foreign.context_id,
        current_claim: None,
    };
    assert!(!is_owner(shard, GRAPH, &read, "owner", DurableCrypto::none()).unwrap());
    assert!(
        read_current(shard, GRAPH, &read, "owner", DurableCrypto::none(), 2000)
            .unwrap()
            .is_none()
    );
    assert_eq!(get(shard, "owner", true, 2000).unwrap(), Some(valid));
    assert_eq!(count(shard), 1);
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn checkpoint_rejects_missing_admission_and_non_work_item_owner_rows() {
    for remove_admission in [false, true] {
        let tmp = open("checkpoint-original-admission");
        let shard = &tmp.shard;
        seed(shard);
        let id = request(None).work_item_id;
        with_nodes(shard, "invalidate-admission", |nodes| {
            let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
            let mut row: Row = decode_durable(bytes.value())?;
            drop(bytes);
            if remove_admission {
                row.remove(run_owner::ADMISSION_REQUEST);
            } else {
                row.insert("node_type".into(), "Other".into());
            }
            write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
        });
        assert!(apply(shard, "invalid", &request(None), "owner", 2000).is_err());
        assert!(get(shard, "owner", false, 2000).unwrap().is_none());
        assert_eq!(count(shard), 0);
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn checkpoint_read_never_crosses_native_graph_tenant_or_run() {
    let tmp = open("checkpoint-scope");
    let shard = &tmp.shard;
    seed(shard);
    apply(shard, "first", &request(None), "owner", 2000).unwrap();
    for field in ["graph", "tenant", "run"] {
        let source = request(None);
        let mut read = GetRunCheckpointRequest {
            tenant: source.tenant,
            work_item_id: source.work_item_id,
            run_id: source.run_id,
            context_id: source.context_id,
            current_claim: None,
        };
        let graph = if field == "graph" {
            "foreign-graph"
        } else {
            GRAPH
        };
        if field == "tenant" {
            read.tenant = "foreign".into();
        }
        if field == "run" {
            read.run_id = "foreign".into();
        }
        assert!(
            read_current(shard, graph, &read, "owner", DurableCrypto::none(), 2000)
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(count(shard), 1);
}

fn typed_admission(metadata: serde_json::Value) -> medulla_types::native_control::SubmitWorkItemRequest {
    let mut request = admission(metadata);
    request.conversation = Some(Box::new(
        medulla_types::native_control::ConversationTurnAdmission {
            schema_version: medulla_types::native_control::NativeControlSchemaVersion::V1,
            context_id: "context".into(),
            expected_revision: 0,
            predecessor_run_id: None,
        },
    ));
    request
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn checkpoint_typed_admission_is_owner_bound_with_optional_matching_legacy_context() {
    for metadata in [
        serde_json::json!({}),
        serde_json::json!({"a2a_context_id":"context"}),
    ] {
        let tmp = open("checkpoint-typed-context");
        let shard = &tmp.shard;
        seed_admission(shard, typed_admission(metadata));
        apply(shard, "first", &request(None), "owner", 2000).unwrap();
        let before = get(shard, "owner", true, 2000).unwrap().unwrap();
        assert_eq!(before.context_id, "context");
        assert!(get(shard, "foreign", false, 2000).unwrap().is_none());
        assert!(apply(shard, "foreign", &request(Some(1)), "foreign", 2000).is_err());
        assert_eq!(get(shard, "owner", true, 2000).unwrap(), Some(before));
        assert_eq!(count(shard), 1);
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn conflicting_original_context_bindings_refuse_without_mutation() {
    for value in [
        serde_json::json!("foreign"),
        serde_json::json!(7),
        serde_json::Value::Null,
    ] {
        let tmp = open("checkpoint-conflicting-context");
        let shard = &tmp.shard;
        seed_admission(
            shard,
            typed_admission(serde_json::json!({"a2a_context_id":value})),
        );
        let id = request(None).work_item_id;
        let (_, before) =
            super::super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none()).unwrap();
        assert!(get(shard, "owner", false, 2000).unwrap().is_none());
        for context in ["context", "foreign"] {
            let mut req = request(None);
            req.context_id = context.into();
            assert!(apply(shard, context, &req, "owner", 2000).is_err());
        }
        assert_eq!(count(shard), 0);
        assert_eq!(
            super::super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none())
                .unwrap()
                .1,
            before
        );
    }
}
