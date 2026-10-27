use super::super::test_shard::{check_guard, open, with_nodes, with_write, GRAPH};
use super::*;

fn request(run: &str, revision: u64, predecessor: Option<&str>) -> SubmitWorkItemRequest {
    let mut request = super::super::test_shard::run_request();
    request.run_id = Some(run.into());
    request.idempotency_key = run.into();
    request.conversation = Some(Box::new(ConversationTurnAdmission {
        schema_version: NativeControlSchemaVersion::V1,
        context_id: "context".into(),
        expected_revision: revision,
        predecessor_run_id: predecessor.map(str::to_owned),
        history_profile: None,
    }));
    request
}

fn admit(shard: &Shard, request: &SubmitWorkItemRequest, owner: &str) -> Result<String, String> {
    with_write(shard, &request.idempotency_key, 1000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
        super::super::apply_submit_work_item_rows(
            GRAPH,
            request,
            &mut rows.nodes,
            &mut rows.inline_history,
            &mut rows.edges,
            &mut rows.command_sequences,
            WorkItemCommitScope { inline_history_enable_authorized: false,
                crypto: DurableCrypto::none(),
                authoritative_now_ms: 1000,
                admitted_actor: Some(owner),
                outbox_id: "outbox",
            },
        )
        .map(|result| {
            if request.conversation.is_some() {
                assert!(result
                    .changed_work_item_ids
                    .contains(&head_id("tenant", "context")));
            }
            result.work_item_id
        })
    })
}

fn state(shard: &Shard, run: &str, status: &str) {
    let id = super::super::run_owner::work_item_id(GRAPH, "tenant", run).unwrap();
    with_nodes(shard, &format!("state-{run}-{status}"), |nodes| {
        let bytes = nodes.get((GRAPH, id.as_str()))?.unwrap();
        let mut row: serde_json::Map<String, serde_json::Value> = decode_durable(bytes.value())?;
        drop(bytes);
        row.insert("status".into(), status.into());
        // Deliberately reverse wall-clock order: it must not decide continuity.
        row.insert("updated_at".into(), 0.into());
        write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    });
}

fn read_head(shard: &Shard) -> Head {
    let handle = shard.graph(GRAPH).unwrap();
    let snapshot = shard.read(&handle).unwrap();
    let nodes = snapshot.scoped_owner_table(NODES).unwrap();
    let id = head_id("tenant", "context");
    decode_durable(nodes.get((GRAPH, id.as_str())).unwrap().unwrap().value()).unwrap()
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[test]
fn admission_reserves_one_ordered_turn_and_refusals_do_not_advance_head() {
    let temp = open("conversation-admission-cas");
    admit(&temp.shard, &request("one", 0, None), "owner").unwrap();
    let first = read_head(&temp.shard);
    assert_eq!(first.revision, 1);
    assert_eq!(first.run_id, "one");
    for (next, owner, refusal) in [
        (request("stale", 0, None), "owner", "CONVERSATION_CONFLICT"),
        (
            request("wrong", 1, Some("other")),
            "owner",
            "CONVERSATION_CONFLICT",
        ),
        (
            request("foreign", 1, Some("one")),
            "foreign",
            "ACCESS_DENIED",
        ),
        (
            request("busy", 1, Some("one")),
            "owner",
            "CONVERSATION_BUSY",
        ),
    ] {
        assert!(admit(&temp.shard, &next, owner)
            .unwrap_err()
            .contains(refusal));
        assert_eq!(read_head(&temp.shard).revision, 1);
        assert!(super::super::run_owner::read_admission(
            &temp.shard,
            GRAPH,
            "tenant",
            next.run_id.as_deref().unwrap(),
            owner,
            DurableCrypto::none()
        )
        .unwrap()
        .is_none());
    }
    state(&temp.shard, "one", "succeeded");
    admit(&temp.shard, &request("two", 1, Some("one")), "owner").unwrap();
    assert_eq!(read_head(&temp.shard).revision, 2);
    let recovered = super::super::run_owner::read_admission(
        &temp.shard,
        GRAPH,
        "tenant",
        "two",
        "owner",
        DurableCrypto::none(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        recovered
            .admission
            .conversation
            .unwrap()
            .predecessor_run_id
            .as_deref(),
        Some("one")
    );
    // Another caller based on the same head loses, even after the first finishes.
    state(&temp.shard, "two", "succeeded");
    assert!(
        admit(&temp.shard, &request("race", 1, Some("one")), "owner")
            .unwrap_err()
            .contains("CONVERSATION_CONFLICT")
    );
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[test]
fn admission_refuses_failed_cancelled_and_unknown_predecessors() {
    let temp = open("conversation-admission-state");
    admit(&temp.shard, &request("one", 0, None), "owner").unwrap();
    for status in ["failed", "cancelled", "dead_letter", "unknown"] {
        state(&temp.shard, "one", status);
        let refusal = if status == "unknown" {
            "CONVERSATION_UNAVAILABLE"
        } else {
            "CONVERSATION_PREDECESSOR_FAILED"
        };
        assert!(
            admit(&temp.shard, &request(status, 1, Some("one")), "owner")
                .unwrap_err()
                .contains(refusal)
        );
        assert_eq!(read_head(&temp.shard).revision, 1);
    }
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[test]
fn reservation_survives_restart_without_reading_clock_order() {
    let path = crate::redb_store::temp_path("eg-conversation-head", "restart");
    let shard = Shard::open(&path).unwrap();
    admit(&shard, &request("one", 0, None), "owner").unwrap();
    state(&shard, "one", "succeeded");
    drop(shard);
    let shard = Shard::open(&path).unwrap();
    assert_eq!(read_head(&shard).revision, 1);
    admit(&shard, &request("two", 1, Some("one")), "owner").unwrap();
    assert_eq!(read_head(&shard).run_id, "two");
    drop(shard);
    std::fs::remove_file(path).unwrap();
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[test]
fn generic_writes_cannot_forge_replace_or_delete_conversation_heads() {
    let temp = open("conversation-admission-guard");
    for label in ["node_type", "type", "label", "labels"] {
        let value = if label == "labels" {
            serde_json::json!(["ConversationHead"])
        } else {
            serde_json::json!("ConversationHead")
        };
        let properties = serde_json::json!({label: value});
        let method = Method::CreateNodeIfAbsent {
            node_id: "forged".into(),
            properties_msgpack: rmp_serde::to_vec_named(&properties).unwrap(),
        };
        assert!(check_guard(&temp.shard, label, method).is_err());
    }
    admit(&temp.shard, &request("one", 0, None), "owner").unwrap();
    let id = head_id("tenant", "context");
    for (index, method) in [
        Method::RemoveNode {
            node_id: id.clone(),
        },
        Method::AddNode {
            node_id: id.clone(),
            properties_msgpack: vec![0xc0],
        },
        Method::CompareAndSetNodeFields {
            node_id: id,
            conditions_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            updates_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"revision":0})).unwrap(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        assert!(check_guard(&temp.shard, &format!("protected-{index}"), method).is_err());
    }
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[test]
fn typed_admission_validates_context_without_changing_legacy_serialization() {
    let mut next = request("one", 0, None);
    next.conversation = None;
    let legacy = serde_json::to_value(&next).unwrap();
    assert!(legacy.get("conversation").is_none());
    assert_eq!(
        serde_json::from_value::<SubmitWorkItemRequest>(legacy).unwrap(),
        next
    );
    next = request("one", 0, None);
    next.metadata
        .insert("a2a_context_id".into(), "different".into());
    assert!(validate(&next, next.conversation.as_ref().unwrap(), Some("owner")).is_err());
    let mut value = serde_json::to_value(request("one", 0, None)).unwrap();
    value["conversation"]["unknown"] = true.into();
    assert!(serde_json::from_value::<SubmitWorkItemRequest>(value).is_err());
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[test]
fn legacy_context_metadata_cannot_bypass_a_reserved_head() {
    let temp = open("conversation-admission-legacy");
    admit(&temp.shard, &request("one", 0, None), "owner").unwrap();
    let mut legacy = request("legacy", 0, None);
    legacy.conversation = None;
    legacy
        .metadata
        .insert("a2a_context_id".into(), "context".into());
    assert!(admit(&temp.shard, &legacy, "owner")
        .unwrap_err()
        .contains("CONVERSATION_INVALID"));
    assert_eq!(read_head(&temp.shard).revision, 1);
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[test]
fn native_metadata_cas_cannot_mutate_a_conversation_head() {
    let temp = open("conversation-admission-native-cas");
    admit(&temp.shard, &request("one", 0, None), "owner").unwrap();
    let original = read_head(&temp.shard);
    let props = serde_json::to_value(&original)
        .unwrap()
        .as_object()
        .unwrap()
        .clone();
    let cas = crate::epistemic_operations_ext::CasWorkItemMetadataRequest {
        schema_version:
            crate::epistemic_operations_ext::CasWorkItemMetadataRequestSchemaVersion::V1,
        now_ms: 1000,
        tenant_ref: "tenant".into(),
        work_item_id: head_id("tenant", "context"),
        expected_status: vec![String::new()],
        expected_lease: None,
        expected_checkpoint_id: None,
        set_checkpoint_id: None,
        expected_metadata_msgpack: None,
        set_metadata_msgpack: None,
        expected_prio_bucket: None,
        set_prio_bucket: Some(1),
    };
    assert!(!super::super::cas_work_item_metadata_preconditions_ok(
        &cas, &props, 1000
    ));
    super::super::test_shard::apply(
        &temp.shard,
        "native-cas-refused",
        Method::CasWorkItemMetadata { request: cas },
        1000,
    )
    .unwrap();
    assert_eq!(read_head(&temp.shard).revision, 1);
}
