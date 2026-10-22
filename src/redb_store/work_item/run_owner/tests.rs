use super::super::test_shard::{open, with_nodes, with_write, GRAPH};
use super::*;
use eg_types::native_control::SubmitWorkItemRequest;

type Row = serde_json::Map<String, serde_json::Value>;

fn request() -> SubmitWorkItemRequest {
    serde_json::from_value(serde_json::json!({
        "schema_version":"1", "run_id":"run", "work_item_id":null,
        "context": {"schema_version":"2", "request_id":"request", "subject_id":"owner",
            "tenant_id":"tenant", "agent_id":"worker", "scopes":["work:*"],
            "audience":"engine", "authentication_method":"local_process", "policy_version":"v1",
            "graph":GRAPH, "placement_epoch":null, "trace_id":"trace", "issued_at_ms":1,
            "expires_at_ms":2},
        "idempotency_key":"run-key", "command_digest":"A".repeat(64), "kind":"test",
        "priority":2, "depends_on":[], "input_ref":"input", "policy_digest":"policy",
        "catalog_digest":"catalog", "model_digest":"model", "max_attempts":3,
        "deadline_unix":null, "metadata":{"original":true}, "provenance_refs":["source"],
        "max_tenant_in_flight":0
    }))
    .unwrap()
}

fn admit(shard: &Shard) -> String {
    with_write(shard, "admit", 1000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        super::super::apply_submit_work_item_rows(
            GRAPH,
            &request(),
            &mut rows.nodes,
            &mut rows.edges,
            &mut rows.command_sequences,
            WorkItemCommitScope {
                crypto: DurableCrypto::none(),
                authoritative_now_ms: 1000,
                admitted_actor: Some("owner"),
                outbox_id: "outbox",
            },
        )
        .map(|result| result.work_item_id)
    })
    .unwrap()
}

fn read(
    shard: &Shard,
    tenant: &str,
    run: &str,
    actor: &str,
) -> Result<Option<eg_types::work_item_read::RunWorkItemView>, String> {
    read_admission(shard, GRAPH, tenant, run, actor, DurableCrypto::none())
}

fn change(shard: &Shard, id: &str, tag: &str, update: impl FnOnce(&mut Row)) {
    with_nodes(shard, tag, |nodes| {
        let bytes = nodes.get((GRAPH, id))?.unwrap();
        let mut row: Row = decode_durable(bytes.value())?;
        drop(bytes);
        update(&mut row);
        write_work_item_props(nodes, GRAPH, id, &mut row, DurableCrypto::none())
    });
}

// spec: EG-DURABLE-KERNEL-R028.4.1
#[test]
fn run_admission_recovery_preserves_original_request() {
    let path = crate::redb_store::temp_path("eg-run-recovery", "restart");
    let shard = Shard::open(&path).unwrap();
    let id = admit(&shard);
    let first = read(&shard, "tenant", "run", "owner").unwrap().unwrap();
    assert_eq!(first.admission, request());
    assert_eq!(first.work_item.work_item_id, id);
    // Native execution may change the caller view, but never the original admission.
    change(&shard, &id, "finish", |row| {
        row.insert("metadata".into(), serde_json::json!({"new":true}));
        row.insert("status".into(), "succeeded".into());
        row.insert("lease_expires_at".into(), 0.into());
    });
    drop(shard);
    let shard = Shard::open(&path).unwrap();
    let restored = read(&shard, "tenant", "run", "owner").unwrap().unwrap();
    assert_eq!(restored.admission, first.admission);
    assert_eq!(restored.work_item.status.as_stored(), "succeeded");
    assert_ne!(
        serde_json::to_value(&restored.work_item.metadata).unwrap(),
        serde_json::to_value(&restored.admission.metadata).unwrap()
    );
    let value = serde_json::to_value(&restored).unwrap();
    for key in [
        "lease_owner",
        "lease_epoch",
        "fencing_token",
        "execution_grant",
    ] {
        assert!(value.get(key).is_none());
        assert!(value["work_item"].get(key).is_none());
        assert!(value["admission"].get(key).is_none());
    }
    drop(shard);
    std::fs::remove_file(path).unwrap();
}

// spec: EG-DURABLE-KERNEL-R028.4.1
#[test]
fn run_admission_recovery_checks_complete_owner() {
    let temp = open("run-recovery-owner");
    let id = admit(&temp.shard);
    for (tenant, run, actor) in [
        ("tenant", "run", "foreign"),
        ("other", "run", "owner"),
        ("tenant", "missing", "owner"),
        ("tenant", "run", ""),
    ] {
        assert!(read(&temp.shard, tenant, run, actor).unwrap().is_none());
    }
    assert!(read(&temp.shard, "tenant", "", "owner").is_err());
    assert!(read(&temp.shard, "tenant", &"x".repeat(513), "owner").is_err());
    for key in ["graph", "tenant", "run_id", "work_item_id", "principal"] {
        change(&temp.shard, &id, key, |row| {
            let mut binding = serde_json::json!({"graph":GRAPH, "tenant":"tenant", "run_id":"run",
                "work_item_id":id, "principal":"owner"});
            binding[key] = "wrong".into();
            row.insert("run_owner_binding".into(), binding);
        });
        assert!(read(&temp.shard, "tenant", "run", "owner")
            .unwrap()
            .is_none());
    }
}

// spec: EG-DURABLE-KERNEL-R028.4.1
#[test]
fn run_admission_recovery_refuses_missing_evidence() {
    let temp = open("run-recovery-missing");
    let id = admit(&temp.shard);
    change(&temp.shard, &id, "legacy", |row| {
        row.remove(ADMISSION_REQUEST);
    });
    assert!(read(&temp.shard, "tenant", "run", "owner")
        .unwrap_err()
        .contains("RUN_ADMISSION_UNAVAILABLE"));
    assert!(read(&temp.shard, "tenant", "run", "foreign")
        .unwrap()
        .is_none());
    for (i, value) in [serde_json::json!({}), serde_json::json!(null), {
        let mut original = request();
        original.run_id = Some("wrong".into());
        serde_json::to_value(original).unwrap()
    }]
    .into_iter()
    .enumerate()
    {
        change(&temp.shard, &id, &format!("malformed-{i}"), |row| {
            row.insert(ADMISSION_REQUEST.into(), value);
        });
        assert!(read(&temp.shard, "tenant", "run", "owner")
            .unwrap_err()
            .contains("RUN_ADMISSION_UNAVAILABLE"));
    }
}

// Exercise the real native backend and handler, then the MessagePack wire codec.
// spec: EG-DURABLE-KERNEL-R028.4.1
#[cfg(feature = "server")]
#[tokio::test]
async fn run_admission_unavailable_is_a_declared_wire_refusal() {
    use crate::server::handlers::work_item_read::{answer, WorkItemRead};
    use crate::server::persistence::{redb_backend::RedbBackend, PersistenceBackend};
    use std::sync::Arc;
    for (tag, evidence) in [("legacy", None), ("malformed", Some(serde_json::json!({})))] {
        let dir = crate::redb_store::temp_path("eg-run-wire-refusal", tag);
        std::fs::create_dir_all(&dir).unwrap();
        let shard = Shard::open(&dir.join("graph-0.redb")).unwrap();
        let id = admit(&shard);
        change(&shard, &id, "old-evidence", |row| {
            row["run_owner_binding"]["principal"] =
                crate::server::mutation_batch::principal_fingerprint("owner")
                    .unwrap()
                    .into();
            row.remove(ADMISSION_REQUEST);
            if let Some(evidence) = evidence {
                row.insert(ADMISSION_REQUEST.into(), evidence);
            }
        });
        drop(shard);
        let backend =
            Arc::new(RedbBackend::open(dir.to_string_lossy().into_owned(), 8192).unwrap());
        let persistence: Option<Arc<dyn PersistenceBackend>> = Some(backend.clone());
        let response = answer(
            77,
            GRAPH,
            "tenant",
            &persistence,
            WorkItemRead::RunAdmission {
                tenant: "tenant".into(),
                run_id: "run".into(),
                actor: "owner".into(),
            },
        )
        .await;
        let wire = rmp_serde::to_vec_named(&response).unwrap();
        let decoded: crate::protocol::Response = rmp_serde::from_slice(&wire).unwrap();
        assert_eq!(decoded.error.as_deref(), Some("RUN_ADMISSION_UNAVAILABLE"));
        assert_eq!(
            decoded.error_detail.as_deref(),
            Some("immutable original submission is unavailable")
        );
        assert!(decoded.result.is_none());
        drop(persistence);
        backend.shutdown();
        drop(backend);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
