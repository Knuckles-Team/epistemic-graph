use super::super::test_shard::run_request as request;
use super::super::test_shard::{apply, open, with_nodes, with_write, GRAPH};
use super::*;
use eg_types::native_control::SubmitWorkItemRequest;

type Row = serde_json::Map<String, serde_json::Value>;

fn admit(shard: &Shard) -> String {
    admit_request(shard, &request())
}

fn admit_request(shard: &Shard, request: &SubmitWorkItemRequest) -> String {
    with_write(shard, "admit", 1000, |admitted| {
        super::super::test_shard::submit_run_rows(admitted, request, "owner")
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

// spec: EG-DURABLE-KERNEL-R028.6
#[test]
fn ordinary_completion_requires_original_run_owner() {
    let temp = open("completion-owner");
    let id = admit(&temp.shard);
    let (_, row) =
        super::super::read::snapshot_row(&temp.shard, GRAPH, &id, DurableCrypto::none()).unwrap();
    let row = row.unwrap();
    assert!(validate_completion_owner(&row, GRAPH, "tenant", "run", &id).is_ok());
    for (graph, tenant, run, item) in [
        ("foreign", "tenant", "run", id.as_str()),
        (GRAPH, "foreign", "run", id.as_str()),
        (GRAPH, "tenant", "foreign", id.as_str()),
        (GRAPH, "tenant", "run", "foreign"),
    ] {
        assert!(validate_completion_owner(&row, graph, tenant, run, item).is_err());
    }
    assert!(read(&temp.shard, "tenant", "run", "foreign")
        .unwrap()
        .is_none());
    let mut changed = row.clone();
    changed.remove(ADMISSION_REQUEST);
    assert!(validate_completion_owner(&changed, GRAPH, "tenant", "run", &id).is_err());
}

// spec: EG-DURABLE-KERNEL-R028.6
#[test]
fn ordinary_completion_binding_corruption_is_not_legacy_unavailability() {
    let temp = open("completion-corrupt");
    let id = admit(&temp.shard);
    assert!(read(&temp.shard, "tenant", "run", "owner")
        .unwrap()
        .unwrap()
        .run_completion
        .is_none());
    change(&temp.shard, &id, "corrupt", |row| {
        row.insert(
            eg_types::run_completion::RUN_COMPLETION_BATCH.into(),
            "missing".into(),
        );
    });
    assert!(read(&temp.shard, "tenant", "run", "owner")
        .unwrap_err()
        .contains("incomplete"));
    assert!(read(&temp.shard, "tenant", "run", "foreign")
        .unwrap()
        .is_none());
}

// spec: EG-DURABLE-KERNEL-R028.6
#[test]
fn ordinary_completion_projection_requires_exact_committed_payload() {
    use eg_types::run_completion::*;
    use sha2::{Digest, Sha256};
    let temp = open("completion-project");
    let id = admit(&temp.shard);
    let handle = temp.shard.graph(GRAPH).unwrap();
    let snapshot = temp.shard.read(&handle).unwrap();
    let mut receipt = eg_transaction::read_batches(&snapshot).unwrap().remove(0);
    let payload = RunCompletionPayload {
        completion: RunCompletionInput {
            schema_version: 1,
            run_id: "run".into(),
            usage: Some(RunUsage::default()),
        },
        tenant: "tenant".into(),
        work_item_id: id.clone(),
        batch_id: receipt.batch.batch_id.clone(),
        outcome: "failed".into(),
        result_ref: None,
        error_ref: Some("error".into()),
    };
    let intent = eg_types::MutationOutboxIntent {
        topic: RUN_COMPLETION_TOPIC.into(),
        key: receipt.batch.batch_id.clone(),
        payload: rmp_serde::to_vec_named(&payload).unwrap(),
        headers: Default::default(),
    };
    let digest = hex::encode(Sha256::digest(&intent.payload));
    receipt.batch.outbox = vec![intent.clone()];
    let event = eg_types::MutationOutboxRecord {
        schema_version: 1,
        batch_id: receipt.batch.batch_id.clone(),
        ordinal: 0,
        identity: receipt.identity.clone(),
        committed_version: receipt.committed_version,
        commit_sequence: Some(receipt.committed_version.target().unwrap_or(1)),
        intent,
        created_at_ms: receipt.committed_at_ms,
    };
    let project = |tenant: &str,
                   run: &str,
                   status: &str,
                   digest: &str,
                   event: &eg_types::MutationOutboxRecord| {
        project_completion(
            snapshot.scope(),
            tenant,
            run,
            &id,
            status,
            &receipt.batch.batch_id,
            digest,
            &receipt,
            std::slice::from_ref(event),
        )
    };
    assert_eq!(
        project("tenant", "run", "failed", &digest, &event)
            .unwrap()
            .payload,
        payload
    );
    assert!(project("foreign", "run", "failed", &digest, &event).is_err());
    assert!(project("tenant", "foreign", "failed", &digest, &event).is_err());
    assert!(project("tenant", "run", "succeeded", &digest, &event).is_err());
    assert!(project("tenant", "run", "failed", "forged", &event).is_err());
    let mut corrupt = event.clone();
    corrupt.ordinal = 1;
    assert!(project("tenant", "run", "failed", &digest, &corrupt).is_err());
    corrupt = event.clone();
    corrupt.commit_sequence = None;
    assert!(project("tenant", "run", "failed", &digest, &corrupt).is_err());
}

// spec: EG-DURABLE-KERNEL-R028.6
// Row-authority regression; this does not qualify ledger/outbox integration.
#[test]
fn ordinary_completion_native_fence_and_terminal_noop_preserve_binding() {
    use eg_types::epistemic_operations::{
        ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion, ClaimWorkItemResult,
    };
    use eg_types::run_completion::*;
    let path = crate::redb_store::temp_path("eg-completion", "native-fence");
    let shard = Shard::open(&path).unwrap();
    let id = admit(&shard);
    let claim = Method::ClaimWorkItem {
        request: ClaimWorkItemRequest {
            schema_version: ClaimWorkItemRequestSchemaVersion::V1,
            tenant_ref: "tenant".into(),
            work_item_id: Some(id.clone()),
            queue_ref: None,
            resource_class: None,
            fairness_group: None,
            worker_ref: "worker".into(),
            now_ms: 2000,
            lease_ms: 60000,
            max_tenant_in_flight: 64,
        },
    };
    let hold: ClaimWorkItemResult = match apply(&shard, "claim", claim, 2000).unwrap() {
        crate::protocol::ResultPayload::Raw(bytes) => decode_durable(&bytes).unwrap(),
        other => panic!("unexpected claim {other:?}"),
    };
    assert!(hold.claimed);
    let commit = |fence, result_ref| {
        completion_method(
            &id,
            hold.lease_epoch.unwrap(),
            fence,
            "failed",
            result_ref,
            None,
        )
    };

    let row = |shard: &Shard| {
        super::super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none())
            .unwrap()
            .1
            .unwrap()
    };
    let before = row(&shard);
    for tag in ["wrong-worker", "wrong-epoch", "expired"] {
        let mut method = commit(hold.fencing_token.unwrap(), None);
        if let Method::CommitWorkItemResult {
            worker_id,
            lease_epoch,
            ..
        } = &mut method
        {
            if tag == "wrong-worker" {
                *worker_id = "intruder".into();
            }
            if tag == "wrong-epoch" {
                *lease_epoch += 1;
            }
        }
        let response = apply(
            &shard,
            tag,
            method,
            if tag == "expired" { 62_001 } else { 3000 },
        )
        .unwrap();
        let crate::protocol::ResultPayload::Json(refusal) = response else {
            panic!("expected a typed commit refusal");
        };
        assert_eq!(refusal["status"], "fenced", "{tag} must be fenced");
        assert_eq!(row(&shard), before, "{tag} must not alter terminal binding");
    }
    apply(
        &shard,
        "stale",
        commit(hold.fencing_token.unwrap() + 1, None),
        3000,
    )
    .unwrap();
    assert_eq!(row(&shard), before);
    assert!(apply(
        &shard,
        "bad-answer",
        commit(hold.fencing_token.unwrap(), Some("answer".into())),
        3000
    )
    .is_err());
    assert_eq!(row(&shard), before);
    apply(
        &shard,
        "finish",
        commit(hold.fencing_token.unwrap(), None),
        4000,
    )
    .unwrap();
    let finished = row(&shard);
    assert_eq!(finished["status"], "failed");
    // Backdated caller time cannot choose the durable terminal timestamp.
    assert_eq!(finished["completed_at"], serde_json::json!(4.0));
    assert!(finished["result_ref"].is_null());
    assert_eq!(finished[RUN_COMPLETION_BATCH], "finish");
    assert!(finished[RUN_COMPLETION_DIGEST].as_str().unwrap().len() == 64);
    apply(
        &shard,
        "duplicate",
        commit(hold.fencing_token.unwrap(), None),
        3000,
    )
    .unwrap();
    assert_eq!(row(&shard), finished);
    drop(shard);
    let shard = Shard::open(&path).unwrap();
    assert_eq!(row(&shard), finished);
    drop(shard);
    std::fs::remove_file(path).unwrap();
}

// spec: EG-DURABLE-KERNEL-R028.6, EG-DURABLE-KERNEL-R028.4.1
// Exercise the production compiler and atomic ledger, not the row-only helper.
#[cfg(feature = "server")]
#[test]
fn ordinary_completion_native_ledger_outbox_restart_atomicity() {
    use crate::mutation_batch::{MutationBatch, MutationSurface};
    use crate::server::mutation_batch::{compile_methods, principal_fingerprint, CompileBatch};
    use eg_types::epistemic_operations::{ClaimWorkItemRequest, ClaimWorkItemResult};
    use eg_types::run_completion::*;
    let compile = |shard: &Shard, tag: &str, method: Method, now| {
        compile_methods(
            CompileBatch {
                batch_id: tag,
                request_id: now,
                attempt_nonce: None,
                principal: Some("owner"),
                tenant: "tenant",
                graph: GRAPH,
                placement_epoch: 0,
                idempotency_key: tag,
                expected_graph_version: Some(
                    crate::redb_store::read_mutation_graph_version(shard, GRAPH).unwrap(),
                ),
                fencing_token: None,
                created_at_ms: now,
                default_surface: MutationSurface::Job,
                authoritative_state: None,
            },
            vec![method],
        )
        .unwrap()
    };
    let commit = |shard: &Shard, batch: &MutationBatch| {
        crate::redb_store::commit_mutation_batch(
            shard,
            GRAPH,
            batch,
            None,
            batch.created_at_ms,
            DurableCrypto::none(),
            &mut crate::redb_store::AuditTailCache::new(),
        )
        .unwrap()
    };
    let decode_result = |bytes: &[u8]| -> serde_json::Value {
        match rmp_serde::from_slice::<crate::protocol::ResultPayload>(bytes).unwrap() {
            crate::protocol::ResultPayload::Raw(bytes) => rmp_serde::from_slice(&bytes).unwrap(),
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("unexpected native result {other:?}"),
        }
    };
    let events = |shard: &Shard| {
        let handle = shard.graph(GRAPH).unwrap();
        let snapshot = shard.read(&handle).unwrap();
        eg_transaction::read_batches(&snapshot)
            .unwrap()
            .iter()
            .flat_map(|b| eg_transaction::read_outbox(&snapshot, &b.batch.batch_id).unwrap())
            .filter(|e| e.intent.topic == RUN_COMPLETION_TOPIC)
            .collect::<Vec<_>>()
    };
    let counters = |shard: &Shard| {
        let handle = shard.graph(GRAPH).unwrap();
        let snapshot = shard.read(&handle).unwrap();
        let table = snapshot
            .scoped_owner_table(crate::redb_store::development_lane::COUNTERS)
            .unwrap();
        table
            .range_inclusive((GRAPH, ""), (GRAPH, "\u{10ffff}"))
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                let (graph, counter) = key.value();
                (
                    graph.to_string(),
                    counter.to_string(),
                    value.value().to_vec(),
                )
            })
            .collect::<Vec<_>>()
    };
    let row = |shard: &Shard, id: &str| {
        super::super::read::snapshot_row(shard, GRAPH, id, DurableCrypto::none())
            .unwrap()
            .1
            .unwrap()
    };
    for (tag, outcome, usage) in [
        (
            "measured",
            "failed",
            RunUsage {
                requests: Some(1),
                total_tokens: Some(5),
                ..Default::default()
            },
        ),
        (
            "zero",
            "succeeded",
            RunUsage {
                requests: Some(0),
                ..Default::default()
            },
        ),
        ("unknown", "failed", RunUsage::default()),
        ("cancelled", "cancelled", RunUsage::default()),
        (
            "success",
            "succeeded",
            RunUsage {
                requests: Some(5),
                input_tokens: Some(15),
                output_tokens: Some(10),
                total_tokens: Some(25),
                ..Default::default()
            },
        ),
    ] {
        let path = crate::redb_store::temp_path("eg-completion-ledger", tag);
        let shard = Shard::open(&path).unwrap();
        let mut original = request();
        original
            .metadata
            .insert("a2a_context_id".into(), "context".into());
        let submitted = compile(
            &shard,
            "submit",
            Method::SubmitWorkItem { request: original },
            1000,
        );
        commit(&shard, &submitted);
        let owner = principal_fingerprint("owner").unwrap();
        let id = read(&shard, "tenant", "run", &owner)
            .unwrap()
            .unwrap()
            .work_item
            .work_item_id;
        let claim: ClaimWorkItemRequest = serde_json::from_value(serde_json::json!({
            "schema_version":"1", "tenant_ref":"tenant", "work_item_id":id,
            "queue_ref":null, "resource_class":null, "fairness_group":null,
            "worker_ref":"worker", "now_ms":2000, "lease_ms":60000, "max_tenant_in_flight":64
        }))
        .unwrap();
        let claimed = commit(
            &shard,
            &compile(
                &shard,
                "claim",
                Method::ClaimWorkItem { request: claim },
                2000,
            ),
        );
        let hold: ClaimWorkItemResult = serde_json::from_value(decode_result(
            claimed.record.result_msgpack.as_ref().unwrap(),
        ))
        .unwrap();
        assert!(hold.claimed);
        let finish = |fence| {
            completion_method(
                &id,
                hold.lease_epoch.unwrap(),
                fence,
                outcome,
                (outcome == "succeeded").then(|| "answer:verified".into()),
                Some(usage.clone()),
            )
        };

        let before = row(&shard, &id);
        let counters_before = counters(&shard);
        let stale = commit(
            &shard,
            &compile(
                &shard,
                "stale",
                finish(hold.fencing_token.unwrap() + 1),
                3000,
            ),
        );
        let refusal: serde_json::Value =
            decode_result(stale.record.result_msgpack.as_ref().unwrap());
        assert_eq!(refusal["status"], "fenced");
        assert_eq!(row(&shard, &id), before);
        assert!(events(&shard).is_empty());
        assert_eq!(counters(&shard), counters_before);
        let batch = compile(&shard, "finish", finish(hold.fencing_token.unwrap()), 3000);
        // Failure after row staging must roll back row, receipt and outbox together.
        let failed = crate::redb_store::commit_mutation_batch_inner(
            &shard,
            crate::redb_store::BatchCommitInput::compact(GRAPH, &batch, None, 3000)
                .with_crashpoint(Some(
                    crate::redb_store::MutationBatchCrashpoint::AfterRowsBeforeMetadata,
                )),
            DurableCrypto::none(),
            &mut crate::redb_store::AuditTailCache::new(),
        );
        assert!(failed.is_err());
        assert_eq!(row(&shard, &id), before);
        assert_eq!(counters(&shard), counters_before);
        assert!(events(&shard).is_empty());
        assert!(
            crate::redb_store::read_mutation_batch_for_graph(&shard, GRAPH, "finish")
                .unwrap()
                .is_none()
        );
        let accepted = commit(&shard, &batch);
        assert!(!accepted.replayed);
        let summary = assert_session_measurements(&shard, &owner, &usage);
        let finished = row(&shard, &id);
        let counters_finished = counters(&shard);
        assert_eq!(finished["status"], outcome);
        assert_eq!(
            finished["result_ref"].as_str(),
            (outcome == "succeeded").then_some("answer:verified")
        );
        let evidence = events(&shard);
        assert_eq!(evidence.len(), 1);
        // A transport retry retains the idempotency key but mints a fresh
        // attempt nonce; replaying a consumed nonce is intentionally refused.
        let replay = compile(&shard, "finish", finish(hold.fencing_token.unwrap()), 3000);
        assert!(commit(&shard, &replay).replayed);
        assert_eq!(events(&shard), evidence);
        assert_eq!(row(&shard, &id), finished);
        assert_eq!(counters(&shard), counters_finished);
        // A fresh command against an already terminal row is also event-free.
        let noop = commit(
            &shard,
            &compile(
                &shard,
                "duplicate",
                finish(hold.fencing_token.unwrap()),
                4000,
            ),
        );
        let result: serde_json::Value = decode_result(noop.record.result_msgpack.as_ref().unwrap());
        assert_eq!(result["status"], "noop");
        assert_eq!(events(&shard), evidence);
        assert_eq!(row(&shard, &id), finished);
        assert_eq!(counters(&shard), counters_finished);
        assert_eq!(assert_session_measurements(&shard, &owner, &usage), summary);
        // Mutable annotations cannot move historical usage to another context.
        change(&shard, &id, "metadata", |row| {
            row.insert(
                "metadata".into(),
                serde_json::json!({"a2a_context_id":"foreign"}),
            );
        });
        assert_eq!(assert_session_measurements(&shard, &owner, &usage), summary);
        drop(shard);
        let shard = Shard::open(&path).unwrap();
        assert_eq!(assert_session_measurements(&shard, &owner, &usage), summary);
        let restored = read(&shard, "tenant", "run", &owner).unwrap().unwrap();
        let receipt = restored.run_completion.unwrap();
        assert_eq!(receipt.payload.completion.usage, Some(usage.clone()));
        assert_eq!(receipt.payload.outcome, outcome);
        assert_eq!(
            receipt.payload.result_ref.as_deref(),
            (outcome == "succeeded").then_some("answer:verified")
        );
        assert_eq!(receipt.payload.batch_id, "finish");
        assert_eq!(events(&shard), evidence);
        if tag == "unknown" {
            assert_eq!(serde_json::to_value(usage).unwrap(), serde_json::json!({}));
        }
        assert!(read(&shard, "tenant", "run", "intruder").unwrap().is_none());
        drop(shard);
        std::fs::remove_file(path).unwrap();
    }
}

/// Construct the same owner-bound completion for row and atomic-ledger tests.
fn completion_method(
    id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    outcome: &str,
    result_ref: Option<String>,
    usage: Option<eg_types::run_completion::RunUsage>,
) -> Method {
    Method::CommitWorkItemResult {
        tenant: "tenant".into(),
        work_item_id: id.into(),
        worker_id: "worker".into(),
        lease_epoch,
        fencing_token,
        idempotency_key: "finish".into(),
        outcome: outcome.into(),
        result_ref,
        error_ref: Some("error".into()),
        retryable: false,
        now_ms: 3000,
        outcome_extension: None,
        run_completion: Some(Box::new(eg_types::run_completion::RunCompletionInput {
            schema_version: 1,
            run_id: "run".into(),
            usage,
        })),
    }
}

fn session_summary(
    shard: &Shard,
    tenant: &str,
    actor: &str,
    context: &str,
) -> Result<eg_types::run_usage::SessionUsage, String> {
    super::super::read_session_usage(shard, GRAPH, tenant, context, actor, DurableCrypto::none())
}

fn assert_session_measurements(
    shard: &Shard,
    owner: &str,
    usage: &eg_types::run_completion::RunUsage,
) -> eg_types::run_usage::SessionUsage {
    let result = session_summary(shard, "tenant", owner, "context").unwrap();
    let expected: std::collections::BTreeMap<String, u64> =
        serde_json::from_value(serde_json::to_value(usage).unwrap()).unwrap();
    assert_eq!(result.counts, expected);
    assert_eq!(result.expected_runs, 1);
    assert_eq!(result.recorded_runs, usize::from(!expected.is_empty()));
    assert_eq!(result.missing_runs, usize::from(expected.is_empty()));
    assert!(result.inventory_complete);
    assert!(result.count_coverage.values().all(|n| *n == 1));
    for (tenant, actor, context) in [
        ("tenant", "intruder", "context"),
        ("foreign", owner, "context"),
        ("tenant", owner, "foreign"),
    ] {
        let hidden = session_summary(shard, tenant, actor, context).unwrap();
        assert_eq!(hidden.expected_runs, 0);
        assert!(hidden.counts.is_empty());
    }
    result
}

#[test]
fn session_inventory_uses_original_context_and_marks_missing_admission_partial() {
    let temp = open("session-inventory");
    let mut original = request();
    original
        .metadata
        .insert("a2a_context_id".into(), "context".into());
    let id = admit_request(&temp.shard, &original);
    assert_eq!(
        session_summary(&temp.shard, "tenant", "owner", "context")
            .unwrap()
            .expected_runs,
        0
    );
    change(&temp.shard, &id, "terminal", |row| {
        row.insert("status".into(), "failed".into());
        row.insert(
            "metadata".into(),
            serde_json::json!({"a2a_context_id":"foreign"}),
        );
    });
    let missing = session_summary(&temp.shard, "tenant", "owner", "context").unwrap();
    assert_eq!(missing.expected_runs, 1);
    assert_eq!(missing.missing_runs, 1);
    assert!(missing.counts.is_empty());
    assert!(missing.inventory_complete);
    assert_eq!(
        session_summary(&temp.shard, "tenant", "owner", "foreign")
            .unwrap()
            .expected_runs,
        0
    );
    change(&temp.shard, &id, "corrupt-completion", |row| {
        row.insert(
            eg_types::run_completion::RUN_COMPLETION_BATCH.into(),
            "absent".into(),
        );
        row.insert(
            eg_types::run_completion::RUN_COMPLETION_DIGEST.into(),
            "forged".into(),
        );
    });
    assert!(session_summary(&temp.shard, "tenant", "owner", "context").is_err());
    change(&temp.shard, &id, "unavailable", |row| {
        row.remove(ADMISSION_REQUEST);
    });
    let unavailable = session_summary(&temp.shard, "tenant", "owner", "context").unwrap();
    assert!(!unavailable.inventory_complete);
    assert_eq!(unavailable.expected_runs, 0);
}

#[test]
fn session_inventory_bound_counts_unrelated_rows() {
    let temp = open("session-bound");
    with_nodes(&temp.shard, "inventory", |nodes| {
        for n in 0..=eg_types::run_usage::MAX_SESSION_USAGE_RUNS {
            let id = format!("unrelated:{n:05}");
            let mut row = serde_json::json!({"node_type":"Other","tenant":"tenant"})
                .as_object()
                .unwrap()
                .clone();
            write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())?;
        }
        Ok(())
    });
    let result = session_summary(&temp.shard, "tenant", "owner", "context").unwrap();
    assert!(!result.inventory_complete);
    assert!(result.counts.is_empty());
    assert_eq!(result.expected_runs, 0);
}
