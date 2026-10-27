use super::*;
use super::super::test_shard::{apply, check_guard, open, with_write, GRAPH};
use eg_types::inline_history::{content_digest, CompletedTextInput};

fn with_history<R>(shard: &Shard, tag: &str,
    write: impl FnOnce(&mut NativeNodeRows<'_>) -> Result<R, String>) -> R {
    with_write(shard, tag, 0, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = member.open_scoped_table(INLINE_HISTORY)?;
        write(&mut rows)
    }).unwrap()
}

fn admit_profile(shard: &Shard, authorized: bool) -> Result<String, String> {
    admit_profile_as(shard, authorized, "run", "owner")
}

fn admit_profile_as(shard: &Shard, authorized: bool, run: &str, actor: &str) -> Result<String, String> {
    let mut request = super::super::test_shard::run_request();
    request.run_id = Some(run.into());
    request.idempotency_key = run.into();
    let context = if run == "run" { "conversation".to_string() } else { format!("conversation-{run}") };
    request.conversation = Some(Box::new(serde_json::from_value(serde_json::json!({
        "schema_version":"1", "context_id":context, "expected_revision":0,
        "predecessor_run_id":null, "history_profile":{"version":1,"user_text_digest":content_digest("hello")}
    })).unwrap()));
    with_write(shard, &format!("inline-admit-{run}"), 1000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        apply_submit_work_item_rows(GRAPH, &request, &mut rows.nodes, &mut rows.inline_history, &mut rows.edges,
            &mut rows.command_sequences, WorkItemCommitScope { inline_history_enable_authorized: authorized, crypto: DurableCrypto::none(),
                authoritative_now_ms: 1000, admitted_actor: Some(actor), outbox_id: "outbox" })
            .map(|value| value.work_item_id)
    })
}

fn seed(shard: &Shard) -> (String, u64, u64) {
    let id = admit_profile(shard, true).unwrap();
    let claim = Method::ClaimWorkItem {
        request: eg_types::epistemic_operations::ClaimWorkItemRequest {
            schema_version: eg_types::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V1,
            tenant_ref: "tenant".into(), work_item_id: Some(id.clone()), queue_ref: None,
            resource_class: None, fairness_group: None, worker_ref: "worker".into(),
            now_ms: 2000, lease_ms: 60000, max_tenant_in_flight: 64,
        },
    };
    let result = apply(shard, "inline-claim", claim, 2000).unwrap();
    let crate::protocol::ResultPayload::Raw(bytes) = result else { panic!("claim payload") };
    let claim: eg_types::inline_history::NativeClaimWorkItemResult = decode_durable(&bytes).unwrap();
    (id, claim.lease_epoch.unwrap(), claim.fencing_token.unwrap())
}

fn candidate(user: &str) -> CompletedTextInput {
    let mut value = CompletedTextInput { version: 1, user_text: user.into(),
        assistant_text: "world".into(), content_digest: String::new() };
    value.content_digest = content_digest(&value.content("run").unwrap());
    value
}
fn complete(id: &str, epoch: u64, fence: u64, value: CompletedTextInput) -> Method {
    Method::CommitWorkItemResult { tenant: "tenant".into(), work_item_id: id.into(),
        worker_id: "worker".into(), lease_epoch: epoch, fencing_token: fence,
        idempotency_key: "finish".into(), outcome: "succeeded".into(), result_ref: None,
        error_ref: None, retryable: false, now_ms: 3000, outcome_extension: None,
        run_completion: Some(Box::new(eg_types::run_completion::RunCompletionInput {
            schema_version: 1, run_id: "run".into(), usage: None,
            completed_checkpoint: Some(Box::new(value)),
        })) }
}
fn row(shard: &Shard, id: &str) -> Option<Row> {
    let handle = shard.graph(GRAPH).unwrap();
    let snapshot = shard.read(&handle).unwrap();
    let table = if id.starts_with("inline-checkpoint:") || id.starts_with("inline-history-quota:") { INLINE_HISTORY } else { NODES };
    let nodes = snapshot.scoped_owner_table(table).unwrap();
    let value = nodes.get((GRAPH, id)).unwrap().map(|v| decode_durable(v.value()).unwrap());
    value
}

#[test]
fn quota_boundary_does_not_partially_charge() {
    let mut quota = Quota { node_type: "InlineHistoryQuota".into(), tenant: "tenant".into(),
        bytes: MAX_RETAINED_BYTES - 1, records: MAX_RETAINED_RECORDS - 1 };
    charge(&mut quota, 1).unwrap();
    assert!(charge(&mut quota, 1).is_err());
    assert_eq!(quota.bytes, MAX_RETAINED_BYTES);
    assert_eq!(quota.records, MAX_RETAINED_RECORDS);
    quota.bytes = 0;
    assert!(charge(&mut quota, 1).is_err());
    assert_eq!(quota.bytes, 0);
}

// spec: EG-DURABLE-KERNEL-R028.4.6
#[test]
fn incomplete_profile_predecessor_refuses_without_advancing_head() {
    for status in ["running", "failed"] {
        let temp = open(&format!("inline-predecessor-{status}"));
        let (id, _, _) = seed(&temp.shard);
        super::super::test_shard::with_nodes(&temp.shard, "predecessor-state", |nodes| {
            let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
            let mut props: Row = decode_durable(bytes.value())?;
            drop(bytes);
            props.insert("status".into(), status.into());
            write_work_item_props(nodes, GRAPH, &id, &mut props, DurableCrypto::none())
        });
        let head = || super::super::history_selection::read_head(&temp.shard,
            GRAPH, "tenant", "conversation", "owner", DurableCrypto::none())
            .unwrap().unwrap();
        let before = serde_json::to_value(head()).unwrap();
        let version = crate::redb_store::read_mutation_graph_version(&temp.shard, GRAPH).unwrap();
        let mut request = super::super::test_shard::run_request();
        request.run_id = Some("next".into());
        request.idempotency_key = "next".into();
        request.conversation = Some(Box::new(serde_json::from_value(serde_json::json!({
            "schema_version":"1", "context_id":"conversation", "expected_revision":1,
            "predecessor_run_id":"run",
            "history_profile":{"version":1,"user_text_digest":content_digest("hello")}
        })).unwrap()));
        let denied = with_write(&temp.shard, "premature-next", 3000, |write| {
            let member = write.graph(GRAPH)?;
            let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
            apply_submit_work_item_rows(GRAPH, &request, &mut rows.nodes,
                &mut rows.inline_history, &mut rows.edges, &mut rows.command_sequences,
                WorkItemCommitScope { inline_history_enable_authorized: false,
                    crypto: DurableCrypto::none(), authoritative_now_ms: 3000,
                    admitted_actor: Some("owner"), outbox_id: "next-outbox" })
        });
        assert!(denied.unwrap_err().contains("CONVERSATION_UNAVAILABLE"));
        assert_eq!(serde_json::to_value(head()).unwrap(), before);
        assert_eq!(crate::redb_store::read_mutation_graph_version(&temp.shard, GRAPH).unwrap(), version);
        let next = super::super::run_owner::work_item_id(GRAPH, "tenant", "next").unwrap();
        assert!(row(&temp.shard, &next).is_none());
        assert!(row(&temp.shard, &quota_id("tenant")).is_none());
    }
}

// spec: EG-DURABLE-KERNEL-R028.4.5
#[test]
fn terminal_text_and_pointer_are_atomic_and_protected() {
    let temp = open("inline-terminal");
    let (id, epoch, fence) = seed(&temp.shard);
    let before = row(&temp.shard, &id);
    assert!(apply(&temp.shard, "wrong-prompt", complete(&id, epoch, fence, candidate("other")), 3000).is_err());
    assert_eq!(row(&temp.shard, &id), before);
    assert!(row(&temp.shard, &quota_id("tenant")).is_none());
    apply(&temp.shard, "finish", complete(&id, epoch, fence, candidate("hello")), 3000).unwrap();
    let work = row(&temp.shard, &id).unwrap();
    assert_eq!(work["status"], "succeeded");
    let pointer: InlineCheckpointRef = serde_json::from_value(work[POINTER].clone()).unwrap();
    let stored = row(&temp.shard, &pointer.document_id).unwrap();
    assert_eq!(stored["content"], candidate("hello").content("run").unwrap());
    assert_eq!(row(&temp.shard, &quota_id("tenant")).unwrap()["records"], 1);
    let handle = temp.shard.graph(GRAPH).unwrap();
    let snapshot = temp.shard.read(&handle).unwrap();
    let public_nodes = snapshot.scoped_owner_table(NODES).unwrap();
    assert!(public_nodes.get((GRAPH, pointer.document_id.as_str())).unwrap().is_none());
    assert!(public_nodes.get((GRAPH, quota_id("tenant").as_str())).unwrap().is_none());
    drop(public_nodes);
    drop(snapshot);
    // Generic writers cannot manufacture a lookalike checkpoint in public nodes.
    assert!(check_guard(&temp.shard, "fake-inline", Method::AddNode {
        node_id: pointer.document_id.clone(),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
            "node_type":"InlineRunCheckpoint", "content":"forged" })).unwrap(),
    }).is_err());
    // A terminal replay may not charge again or change accepted bytes.
    apply(&temp.shard, "finish-again", complete(&id, epoch, fence, candidate("hello")), 4000).unwrap();
    assert_eq!(row(&temp.shard, &quota_id("tenant")).unwrap()["records"], 1);
    assert_eq!(row(&temp.shard, &pointer.document_id).unwrap(), stored);
}

#[test]
fn exact_expiry_and_foreign_tenant_cannot_attach() {
    let temp = open("inline-fences");
    let (id, epoch, fence) = seed(&temp.shard);
    let before = row(&temp.shard, &id);
    assert!(apply(&temp.shard, "exact-expiry", complete(&id, epoch, fence, candidate("hello")), 62000).is_err());
    assert_eq!(row(&temp.shard, &id), before);
    let mut foreign = complete(&id, epoch, fence, candidate("hello"));
    if let Method::CommitWorkItemResult { tenant, .. } = &mut foreign { *tenant = "foreign".into(); }
    let _ = apply(&temp.shard, "foreign", foreign, 3000);
    assert_eq!(row(&temp.shard, &id), before);
    assert!(row(&temp.shard, &quota_id("tenant")).is_none());
}

#[test]
fn retained_text_and_quota_survive_reopen_and_refuse_retirement() {
    let path = crate::redb_store::temp_path("eg-inline-history", "reopen");
    let expected;
    let checkpoint_id;
    {
        let shard = Shard::open(&path).unwrap();
        let (id, epoch, fence) = seed(&shard);
        apply(&shard, "finish", complete(&id, epoch, fence, candidate("hello")), 3000).unwrap();
        checkpoint_id = candidate("hello").checkpoint_ref(&id).document_id;
        expected = row(&shard, &checkpoint_id).unwrap();
    }
    {
        let shard = Shard::open(&path).unwrap();
        assert_eq!(row(&shard, &checkpoint_id).unwrap(), expected);
        assert_eq!(row(&shard, &quota_id("tenant")).unwrap()["records"], 1);
        // V1 cannot safely reuse a profiled run identity after retirement.
        assert!(crate::redb_store::purge_graph_rows(&shard, GRAPH).is_err());
        assert_eq!(row(&shard, &checkpoint_id).unwrap(), expected);
        assert_eq!(row(&shard, &quota_id("tenant")).unwrap()["records"], 1);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn capacity_failure_rolls_back_pointer_and_snapshot() {
    let temp = open("inline-capacity");
    let (id, epoch, fence) = seed(&temp.shard);
    with_history(&temp.shard, "full-quota", |nodes| {
        let quota = Quota { node_type: "InlineHistoryQuota".into(), tenant: "tenant".into(),
            bytes: MAX_RETAINED_BYTES, records: MAX_RETAINED_RECORDS };
        let bytes = rmp_serde::to_vec_named(&quota).unwrap();
        nodes.insert((GRAPH, quota_id("tenant").as_str()), bytes.as_slice())?;
        Ok(())
    });
    let before = row(&temp.shard, &id);
    let quota_before = row(&temp.shard, &quota_id("tenant"));
    assert!(apply(&temp.shard, "full", complete(&id, epoch, fence, candidate("hello")), 3000).is_err());
    assert_eq!(row(&temp.shard, &id), before);
    assert_eq!(row(&temp.shard, &quota_id("tenant")), quota_before);
    assert!(row(&temp.shard, &candidate("hello").checkpoint_ref(&id).document_id).is_none());
}

#[test]
fn reacquired_attempt_and_bad_digest_do_not_become_completed_history() {
    let temp = open("inline-attempt");
    let (id, epoch, fence) = seed(&temp.shard);
    let before = row(&temp.shard, &id);
    let mut bad = candidate("hello");
    bad.content_digest = "0".repeat(64);
    assert!(apply(&temp.shard, "bad-digest", complete(&id, epoch, fence, bad), 3000).is_err());
    assert_eq!(row(&temp.shard, &id), before);
    super::super::test_shard::with_nodes(&temp.shard, "second-attempt", |nodes| {
        let mut props = before.clone().unwrap();
        props.insert("attempt".into(), 2.into());
        write_work_item_props(nodes, GRAPH, &id, &mut props, DurableCrypto::none())
    });
    let second = row(&temp.shard, &id);
    assert!(apply(&temp.shard, "reacquired", complete(&id, epoch, fence, candidate("hello")), 3000).is_err());
    assert_eq!(row(&temp.shard, &id), second);
    assert!(row(&temp.shard, &quota_id("tenant")).is_none());
}

#[test]
fn first_selection_requires_owner_and_refuses_any_current_pointer() {
    use eg_types::inline_history::RunSelectionRequest;
    let temp = open("inline-first-selection");
    let (id, epoch, fence) = seed(&temp.shard);
    let request = RunSelectionRequest { version: 1, worker_id: "worker".into(),
        lease_epoch: epoch, fencing_token: fence };
    let read = |owner| super::super::history_selection::read_selection(&temp.shard,
        GRAPH, "tenant", "run", owner, &request, DurableCrypto::none(), 3000);
    let selected = read("owner").unwrap().unwrap().checkpoint_selection.unwrap();
    assert!(selected.current_run_checkpoint.is_none());
    assert!(selected.completed_history.is_none());
    assert!(read("foreign").unwrap().is_none());
    let head = super::super::history_selection::read_head(&temp.shard, GRAPH, "tenant",
        "conversation", "owner", DurableCrypto::none()).unwrap().unwrap();
    assert_eq!(head.revision, 1);
    assert_eq!(head.run_id, "run");
    assert!(super::super::history_selection::read_head(&temp.shard, GRAPH, "tenant",
        "conversation", "foreign", DurableCrypto::none()).unwrap().is_none());
    super::super::test_shard::with_nodes(&temp.shard, "unknown-current", |nodes| {
        let mut props = row(&temp.shard, &id).unwrap();
        props.insert(POINTER.into(), serde_json::Value::Null);
        write_work_item_props(nodes, GRAPH, &id, &mut props, DurableCrypto::none())
    });
    assert!(read("owner").is_err());
}

// Real native compiler/ledger/outbox and close/reopen; still not a served client
// journey or proof that Arbora delivered these bytes to a model.
#[cfg(feature = "server")]
// spec: EG-DURABLE-KERNEL-R028.4.6
#[test]
fn two_turn_selection_uses_committed_snapshot_after_restart() {
    use crate::server::mutation_batch::{compile_methods, principal_fingerprint, CompileBatch};
    use eg_types::inline_history::RunSelectionRequest;
    let path = crate::redb_store::temp_path("eg-inline-selection", "restart");
    let execute = |shard: &Shard, tag: &str, method: Method, now| {
        let mut batch = compile_methods(CompileBatch {
            batch_id: tag, request_id: now, attempt_nonce: None, principal: Some("owner"),
            tenant: "tenant", graph: GRAPH, placement_epoch: 0, idempotency_key: tag,
            expected_graph_version: Some(crate::redb_store::read_mutation_graph_version(shard, GRAPH).unwrap()),
            fencing_token: None, created_at_ms: now,
            default_surface: crate::mutation_batch::MutationSurface::Job, authoritative_state: None,
        }, vec![method]).unwrap();
        // This fixture stands in for authenticated operator admission. Ordinary
        // low-level compilation itself does not mint this capability.
        assert!(!batch.verified_capabilities().unwrap().contains(
            &eg_types::mutation_batch::MutationCapability::EnableInlineHistoryProfile));
        if tag == "first-submit" {
            let version = crate::redb_store::read_mutation_graph_version(shard, GRAPH).unwrap();
            let denied = crate::redb_store::commit_mutation_batch(shard, GRAPH, &batch, None, now,
                DurableCrypto::none(), &mut crate::redb_store::AuditTailCache::new());
            assert!(denied.unwrap_err().contains("operator graph enablement"));
            assert_eq!(crate::redb_store::read_mutation_graph_version(shard, GRAPH).unwrap(), version);
        }
        if let eg_types::MutationEnvelope::Operation(envelope) = &mut batch.envelope {
            envelope.verified_capabilities.insert(eg_types::mutation_batch::MutationCapability::EnableInlineHistoryProfile);
        }
        crate::redb_store::commit_mutation_batch(shard, GRAPH, &batch, None, now,
            DurableCrypto::none(), &mut crate::redb_store::AuditTailCache::new()).unwrap()
    };
    let owner = principal_fingerprint("owner").unwrap();
    let request = |run: &str, revision, predecessor: Option<&str>| {
        let mut request = super::super::test_shard::run_request();
        request.run_id = Some(run.into());
        request.idempotency_key = run.into();
        request.conversation = Some(Box::new(serde_json::from_value(serde_json::json!({
            "schema_version":"1", "context_id":"conversation", "expected_revision":revision,
            "predecessor_run_id":predecessor,
            "history_profile":{"version":1,"user_text_digest":content_digest("hello")}
        })).unwrap()));
        request
    };
    let claim = |id: &str| Method::ClaimWorkItem {
        request: eg_types::epistemic_operations::ClaimWorkItemRequest {
            schema_version: eg_types::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V1,
            tenant_ref: "tenant".into(), work_item_id: Some(id.into()), queue_ref: None,
            resource_class: None, fairness_group: None, worker_ref: "worker".into(),
            now_ms: 2000, lease_ms: 60000, max_tenant_in_flight: 64,
        },
    };
    let lease = |shard: &Shard, id: &str| {
        let props = row(shard, id).unwrap();
        (property_u64(&props, "lease_epoch"), property_u64(&props, "fencing_token"))
    };
    let second_id;
    {
        let shard = Shard::open(&path).unwrap();
        execute(&shard, "first-submit", Method::SubmitWorkItem { request: request("run", 0, None) }, 1000);
        let first_id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
        execute(&shard, "first-claim", claim(&first_id), 2000);
        let (epoch, fence) = lease(&shard, &first_id);
        execute(&shard, "first-finish", complete(&first_id, epoch, fence, candidate("hello")), 3000);
        execute(&shard, "second-submit", Method::SubmitWorkItem { request: request("second", 1, Some("run")) }, 4000);
        second_id = run_owner::work_item_id(GRAPH, "tenant", "second").unwrap();
        execute(&shard, "second-claim", claim(&second_id), 5000);
    }
    {
        let shard = Shard::open(&path).unwrap();
        // Historical owner display requires no live lease: it resolves the
        // exact immutable terminal receipt, never a generic RunEvent fallback.
        let historical = |actor| run_owner::read_admission(&shard, GRAPH, "tenant",
            "run", actor, DurableCrypto::none());
        let displayed = historical(&owner).unwrap().unwrap();
        assert_eq!(displayed.completed_checkpoint.as_deref(),
            Some(candidate("hello").content("run").unwrap().as_str()));
        assert!(historical("foreign").unwrap().is_none());
        let (epoch, fence) = lease(&shard, &second_id);
        let selection = RunSelectionRequest { version: 1, worker_id: "worker".into(), lease_epoch: epoch, fencing_token: fence };
        let read = || super::super::history_selection::read_selection(&shard, GRAPH, "tenant",
            "second", &owner, &selection, DurableCrypto::none(), 6000);
        let selected = read().unwrap().unwrap().checkpoint_selection.unwrap();
        assert_eq!(selected.completed_history.as_deref(), Some(candidate("hello").content("run").unwrap().as_str()));
        assert_eq!(selected.checkpoints.len(), 1);
        let checkpoint_id = selected.checkpoints[0].document_id.clone();
        with_history(&shard, "corrupt-checkpoint", |nodes| {
            let mut stored = row(&shard, &checkpoint_id).unwrap();
            stored.insert("content".into(), "tampered".into());
            let bytes = rmp_serde::to_vec_named(&stored).unwrap();
            nodes.insert((GRAPH, checkpoint_id.as_str()), bytes.as_slice())?;
            Ok(())
        });
        assert!(read().is_err());
        assert!(historical(&owner).is_err());
        // Owner authorization is still checked before corrupted private data.
        assert!(historical("foreign").unwrap().is_none());
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn verified_foreign_actor_cannot_use_copied_lease_tuple() {
    let temp = open("inline-caller");
    let (id, epoch, fence) = seed(&temp.shard);
    let before = row(&temp.shard, &id);
    for (tag, actor) in [("foreign-actor", Some("foreign")), ("missing-actor", None)] {
        assert!(super::super::test_shard::apply_as_actor(&temp.shard, tag,
            complete(&id, epoch, fence, candidate("hello")), 3000, actor).is_err());
        assert_eq!(row(&temp.shard, &id), before);
        assert!(row(&temp.shard, &quota_id("tenant")).is_none());
    }
    let mut failed = complete(&id, epoch, fence, candidate("hello"));
    if let Method::CommitWorkItemResult { outcome, run_completion, .. } = &mut failed {
        *outcome = "failed".into();
        run_completion.as_mut().unwrap().completed_checkpoint = None;
    }
    assert!(super::super::test_shard::apply_as_actor(&temp.shard, "foreign-failed",
        failed, 3000, Some("foreign")).is_err());
    assert_eq!(row(&temp.shard, &id), before);
}

#[test]
fn shared_and_crossmodal_row_clear_and_delete_refuse_profile_state() {
    for crossmodal in [false, true] {
        for delete in [false, true] {
            let temp = open(&format!("inline-clear-{crossmodal}-{delete}"));
            let (id, epoch, fence) = seed(&temp.shard);
            apply(&temp.shard, "finish", complete(&id, epoch, fence, candidate("hello")), 3000).unwrap();
            let checkpoint = candidate("hello").checkpoint_ref(&id).document_id;
            assert!(row(&temp.shard, &checkpoint).is_some());
            let clear = if delete { Method::DeleteGraph { graph_name: GRAPH.into() } }
                else { Method::ClearGraph };
            let methods = vec![clear, Method::AddNode {
                node_id: "recreated-public-node".into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"fresh":true})).unwrap(),
            }];
            let previous = row(&temp.shard, &checkpoint);
            let quota = row(&temp.shard, &quota_id("tenant"));
            let result = with_write(&temp.shard, "shared-clear-recreate", 4000, |write| {
                if crossmodal {
                    crate::redb_store::apply_crossmodal_graph_rows(write, GRAPH,
                        &crate::redb_store::CrossModalBatchRows {
                            methods: &methods, vectors: &[], blob_refs: &[], measurements: &[],
                        }, DurableCrypto::none())
                } else {
                    crate::redb_store::apply_graph_methods(write, GRAPH, &methods, DurableCrypto::none(),
                        #[cfg(feature = "security")]
                        &mut crate::redb_store::AuditTailCache::new())
                }
            });
            assert!(result.is_err());
            assert_eq!(row(&temp.shard, &checkpoint), previous);
            assert_eq!(row(&temp.shard, &quota_id("tenant")), quota);
            assert!(row(&temp.shard, &id).is_some());
            assert!(row(&temp.shard, "recreated-public-node").is_none());
        }
    }
}

#[test]
fn snapshot_replacement_refuses_retained_or_imported_inline_bindings() {
    let temp = open("inline-replacement");
    let (id, epoch, fence) = seed(&temp.shard);
    assert!(with_write(&temp.shard, "replace-active", 2500, |write| {
        refuse_snapshot_replacement(write, GRAPH, &[], DurableCrypto::none())
    }).is_err());
    apply(&temp.shard, "finish", complete(&id, epoch, fence, candidate("hello")), 3000).unwrap();
    let checkpoint = candidate("hello").checkpoint_ref(&id).document_id;
    let before = row(&temp.shard, &checkpoint);
    assert!(with_write(&temp.shard, "replace-held", 4000, |write| {
        refuse_snapshot_replacement(write, GRAPH, &[], DurableCrypto::none())
    }).is_err());
    assert_eq!(row(&temp.shard, &checkpoint), before);
    // No retained content is necessary for a forged imported pointer/profile
    // to be refused; a public snapshot cannot manufacture private authority.
    let empty = open("inline-import");
    for (tag, value) in [
        ("pointer", serde_json::json!({"inline_checkpoint":null})),
        ("profile", serde_json::json!({"run_admission_request":{"conversation":{
            "history_profile":{"version":1,"user_text_digest":content_digest("hello")}
        }}})),
    ] {
        let incoming = vec![("imported".into(), rmp_serde::to_vec_named(&value).unwrap())];
        assert!(with_write(&empty.shard, tag, 1000, |write| {
            refuse_snapshot_replacement(write, GRAPH, &incoming, DurableCrypto::none())
        }).is_err());
    }
    with_write(&empty.shard, "ordinary-empty", 2000, |write| {
        refuse_snapshot_replacement(write, GRAPH, &[], DurableCrypto::none())
    }).unwrap();
}

#[test]
fn clear_refuses_profiled_admission_before_any_checkpoint_exists() {
    let temp = open("inline-pending-clear");
    let (id, _, _) = seed(&temp.shard);
    let before = row(&temp.shard, &id);
    assert!(row(&temp.shard, &quota_id("tenant")).is_none());
    assert!(with_write(&temp.shard, "pending-clear", 2500, |write| {
        let member = write.graph(GRAPH)?;
        let mut tables = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        crate::redb_store::apply_method_rows(GRAPH, &Method::ClearGraph, &mut tables, DurableCrypto::none())
    }).is_err());
    assert_eq!(row(&temp.shard, &id), before);
}

#[test]
fn pending_failed_and_cancelled_profiles_refuse_scope_retirement() {
    for outcome in ["pending", "failed", "cancelled"] {
        let temp = open(&format!("inline-retire-{outcome}"));
        let (id, epoch, fence) = seed(&temp.shard);
        if outcome != "pending" {
            let mut finish = complete(&id, epoch, fence, candidate("hello"));
            if let Method::CommitWorkItemResult { outcome: target, run_completion, .. } = &mut finish {
                *target = outcome.into();
                run_completion.as_mut().unwrap().completed_checkpoint = None;
            }
            apply(&temp.shard, "non-success", finish, 3000).unwrap();
        }
        let before = row(&temp.shard, &id);
        assert!(row(&temp.shard, &quota_id("tenant")).is_none());
        assert!(crate::redb_store::purge_graph_rows(&temp.shard, GRAPH).is_err());
        assert_eq!(row(&temp.shard, &id), before);
        let handle = temp.shard.graph(GRAPH).unwrap();
        let snapshot = temp.shard.read(&handle).unwrap();
        let history = snapshot.scoped_owner_table(INLINE_HISTORY).unwrap();
        assert!(history.get((GRAPH, PROFILE_PRESENT)).unwrap().is_some());
    }
}

#[test]
fn unprofiled_scope_retirement_remains_supported() {
    let temp = open("inline-legacy-retirement");
    super::super::test_shard::with_nodes(&temp.shard, "legacy-row", |nodes| {
        let bytes = rmp_serde::to_vec_named(&serde_json::json!({"legacy":true})).unwrap();
        nodes.insert((GRAPH, "legacy-row"), bytes.as_slice())?;
        Ok(())
    });
    crate::redb_store::purge_graph_rows(&temp.shard, GRAPH).unwrap();
    assert!(row(&temp.shard, "legacy-row").is_none());
}

#[test]
fn batched_profile_admission_marks_retirement_presence() {
    let temp = open("inline-batch-presence");
    let mut child = super::super::test_shard::run_request();
    child.conversation = Some(Box::new(serde_json::from_value(serde_json::json!({
        "schema_version":"1", "context_id":"batch-context", "expected_revision":0,
        "predecessor_run_id":null,
        "history_profile":{"version":1,"user_text_digest":content_digest("hello")}
    })).unwrap()));
    let request = eg_types::native_control::SubmitWorkItemsRequest {
        schema_version: eg_types::native_control::NativeControlSchemaVersion::V1,
        context: child.context.clone(), idempotency_key: "batch".into(),
        requests: vec![child], enrichment_budget: None,
    };
    with_write(&temp.shard, "batch-profile", 1000, |write| {
        let member = write.graph(GRAPH)?;
        let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        apply_submit_work_items_rows(GRAPH, &request, &mut rows.nodes,
            &mut rows.inline_history, &mut rows.edges, &mut rows.command_sequences,
            WorkItemCommitScope { inline_history_enable_authorized: true, crypto: DurableCrypto::none(), authoritative_now_ms: 1000,
                admitted_actor: Some("owner"), outbox_id: "batch-outbox" })
    }).unwrap();
    assert!(crate::redb_store::purge_graph_rows(&temp.shard, GRAPH).is_err());
    let handle = temp.shard.graph(GRAPH).unwrap();
    let snapshot = temp.shard.read(&handle).unwrap();
    let history = snapshot.scoped_owner_table(INLINE_HISTORY).unwrap();
    assert_eq!(history.scope_rows().unwrap().count(), 1);
    assert!(history.get((GRAPH, PROFILE_PRESENT)).unwrap().is_some());
}

#[cfg(feature = "server")]
#[test]
fn existing_profile_graft_refuses_before_source_fence_or_destination_reservation() {
    let source = open("inline-graft-source");
    let destination = open("inline-graft-destination");
    let (id, _, _) = seed(&source.shard);
    let before = row(&source.shard, &id);
    let version = crate::redb_store::read_mutation_graph_version(&source.shard, GRAPH).unwrap();
    let identity = crate::redb_store::shard::graph_scope_identity(GRAPH).unwrap();
    assert!(!destination.shard.kernel().scope_binding_exists(&identity).unwrap());
    let error = destination.shard.graft_graph_from(&source.shard, GRAPH).unwrap_err();
    assert_eq!(error, "inline history graph graft is unsupported");
    assert_eq!(row(&source.shard, &id), before);
    assert_eq!(crate::redb_store::read_mutation_graph_version(&source.shard, GRAPH).unwrap(), version);
    assert!(!destination.shard.kernel().scope_binding_exists(&identity).unwrap());
}

#[cfg(feature = "server")]
#[test]
fn admission_before_atomic_graft_validation_leaves_no_fence_or_reservation() {
    let source = open("inline-atomic-graft-source");
    let destination = open("inline-atomic-graft-target");
    // Simulate the ordering where admission wins after an empty preflight.
    let source_handle = source.shard.graph(GRAPH).unwrap();
    let destination_handle = destination.shard.graph(GRAPH).unwrap();
    seed(&source.shard);
    let version = crate::redb_store::read_mutation_graph_version(&source.shard, GRAPH).unwrap();
    let target = eg_transaction::GraftDestination::new(destination.shard.mutations(),
        destination.shard.kernel(), destination_handle.as_ref())
        .with_owner_payload(&crate::redb_store::GraphShardRetirement);
    let error = source.shard.mutations().graft_begin_with_validation(
        source_handle.as_ref(), &target,
        |read| Shard::validate_inline_history_graft(read, GRAPH),
    ).unwrap_err();
    assert_eq!(error, "inline history graph graft is unsupported");
    assert_eq!(crate::redb_store::read_mutation_graph_version(&source.shard, GRAPH).unwrap(), version);
    assert!(source.shard.mutations().graft_source_marker(source_handle.as_ref()).unwrap().is_none());
    assert_eq!(crate::redb_store::read_mutation_graph_version(&destination.shard, GRAPH).unwrap(), 0);
    // Authenticated cancellation restored a writable destination baseline;
    // source also remains writable rather than stuck under maximum fencing.
    with_history(&source.shard, "after-refused-graft", |_| Ok(()));
    with_history(&destination.shard, "after-refused-reservation", |_| Ok(()));
}

#[cfg(feature = "server")]
#[test]
fn atomic_graft_before_profile_admission_fences_the_native_write() {
    let source = open("inline-graft-first-source");
    let destination = open("inline-graft-first-target");
    let source_handle = source.shard.graph(GRAPH).unwrap();
    let destination_handle = destination.shard.graph(GRAPH).unwrap();
    let target = eg_transaction::GraftDestination::new(destination.shard.mutations(),
        destination.shard.kernel(), destination_handle.as_ref())
        .with_owner_payload(&crate::redb_store::GraphShardRetirement);
    source.shard.mutations().graft_begin_with_validation(source_handle.as_ref(), &target,
        |read| Shard::validate_inline_history_graft(read, GRAPH)).unwrap();
    // The row admission cannot acquire a normal admitted owner window after
    // the maximum fence; no profile marker can be inserted behind validation.
    let members = source.shard.graph_members(&[GRAPH]).unwrap();
    let result = source.shard.admit_maintenance(&members, "late-profile-admission");
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("source accepted an owner write after graft fencing"),
    };
    assert!(error.contains("STALE_FENCE"), "{error}");
    let snapshot = source.shard.read(&source_handle).unwrap();
    assert!(snapshot.scoped_owner_table(INLINE_HISTORY).unwrap().scope_rows().unwrap().next().is_none());
}

#[test]
fn first_profile_requires_operator_consent_before_any_row_write() {
    let temp = open("inline-operator-enable");
    let before = crate::redb_store::read_mutation_graph_version(&temp.shard, GRAPH).unwrap();
    assert!(admit_profile(&temp.shard, false).unwrap_err().contains("operator graph enablement"));
    assert_eq!(crate::redb_store::read_mutation_graph_version(&temp.shard, GRAPH).unwrap(), before);
    let handle = temp.shard.graph(GRAPH).unwrap();
    let read = temp.shard.read(&handle).unwrap();
    assert!(read.scoped_owner_table(INLINE_HISTORY).unwrap().scope_rows().unwrap().next().is_none());
    assert!(read.scoped_owner_table(NODES).unwrap().scope_rows().unwrap().next().is_none());
    drop(read);
    admit_profile(&temp.shard, true).unwrap();
    let read = temp.shard.read(&handle).unwrap();
    let history = read.scoped_owner_table(INLINE_HISTORY).unwrap();
    assert!(history.get((GRAPH, PROFILE_PRESENT)).unwrap().is_some());
    drop(history);
    drop(read);
    assert!(admit_profile_as(&temp.shard, false, "foreign", "foreign").is_err());
    assert!(admit_profile_as(&temp.shard, false, "ordinary", "owner").is_ok());
}

#[test]
fn outer_failure_after_attachment_rolls_back_content_quota_pointer_and_receipt() {
    let temp = open("inline-after-attachment-abort");
    let (id, epoch, fence) = seed(&temp.shard);
    let before = row(&temp.shard, &id);
    let version = crate::redb_store::read_mutation_graph_version(&temp.shard, GRAPH).unwrap();
    let checkpoint = candidate("hello").checkpoint_ref(&id).document_id;
    let reached = std::cell::Cell::new(false);
    let result = super::super::test_shard::apply_then(&temp.shard, "abort-after-attach",
        complete(&id, epoch, fence, candidate("hello")), 3000, Some("owner"),
        |nodes, history| {
            let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
            let staged: Row = decode_durable(bytes.value())?;
            assert!(staged.contains_key(POINTER));
            assert!(staged.contains_key(eg_types::run_completion::RUN_COMPLETION_BATCH));
            assert!(staged.contains_key(eg_types::run_completion::RUN_COMPLETION_DIGEST));
            assert!(history.get((GRAPH, checkpoint.as_str()))?.is_some());
            assert!(history.get((GRAPH, quota_id("tenant").as_str()))?.is_some());
            reached.set(true);
            Err("test-only outer transaction failure".into())
        });
    assert!(reached.get());
    assert_eq!(result.unwrap_err(), "test-only outer transaction failure");
    assert_eq!(row(&temp.shard, &id), before);
    assert!(row(&temp.shard, &checkpoint).is_none());
    assert!(row(&temp.shard, &quota_id("tenant")).is_none());
    assert_eq!(crate::redb_store::read_mutation_graph_version(&temp.shard, GRAPH).unwrap(), version);
}
