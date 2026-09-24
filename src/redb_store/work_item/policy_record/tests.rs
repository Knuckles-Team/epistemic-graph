//! Store-level proof over a real shard: the kernel is the sole writer of a
//! policy-evolution row, and a store is create-only.

use super::super::test_shard::{open, with_nodes, GRAPH};
use super::*;
use eg_types::policy_evolution::PolicyEvolutionRecord;

fn stamped(tenant: &str) -> StoredPolicyRecord {
    let record: PolicyEvolutionRecord = serde_json::from_value(serde_json::json!({
        "kind": "model_policy_version",
        "record": {
            "checkpoint_digest": "11".repeat(32), "tokenizer_digest": "22".repeat(32),
            "artifact_ref": "artifacts:base", "origin": {"origin": "base"},
        },
    }))
    .unwrap();
    StoredPolicyRecord {
        record_id: record.record_id(tenant).unwrap(),
        tenant_id: tenant.to_string(),
        recorded_by: "principal:sha256:ab".to_string(),
        recorded_at_ms: 7,
        record,
    }
}

fn store(
    shard: &Shard,
    tag: &str,
    request: StoredPolicyRecord,
) -> Result<PolicyRecordStored, String> {
    let method = Method::PolicyEvolutionStore {
        request: Box::new(request),
    };
    let payload = with_nodes(shard, tag, |nodes| {
        Ok(apply_policy_record_rows(
            GRAPH,
            &method,
            nodes,
            DurableCrypto::none(),
        ))
    })?;
    match payload.expect("a store always answers") {
        crate::protocol::ResultPayload::Raw(bytes) => Ok(rmp_serde::from_slice(&bytes).unwrap()),
        other => panic!("policy record stores are raw, got {other:?}"),
    }
}

fn check(shard: &Shard, tag: &str, method: Method) -> Result<(), String> {
    with_nodes(shard, tag, |nodes| {
        Ok(refuse_generic_native_row_write(
            GRAPH,
            &method,
            nodes,
            DurableCrypto::none(),
        ))
    })
}

#[test]
fn the_kernel_stores_once_and_refuses_a_forged_identity() {
    let temp = open("policy-record-store");
    let first = store(&temp.shard, "first", stamped("tenant-a")).unwrap();
    assert!(first.created);
    assert_eq!(first.changed_work_item_ids.len(), 1);
    let again = store(&temp.shard, "again", stamped("tenant-a")).unwrap();
    assert!(!again.created && again.changed_work_item_ids.is_empty());
    let mut forged = stamped("tenant-a");
    forged.tenant_id = "tenant-b".to_string();
    let refused = store(&temp.shard, "forged", forged).unwrap_err();
    assert!(refused.starts_with("POLICY_RECORD_TAMPERED"), "{refused}");
}

#[test]
fn generic_writers_cannot_create_change_or_remove_a_policy_row() {
    let temp = open("policy-record-guard");
    let stored = stamped("tenant-a");
    let id = stored.record_id.clone();
    let row = stored.row().unwrap();
    store(&temp.shard, "seed", stored).unwrap();
    let msgpack = |value: serde_json::Value| rmp_serde::to_vec_named(&value).unwrap();
    let edit = Method::CompareAndSetNodeFields {
        node_id: id.clone(),
        conditions_msgpack: msgpack(serde_json::json!({})),
        updates_msgpack: msgpack(serde_json::json!({"recorded_by": "someone-else"})),
    };
    let remove = Method::RemoveNode { node_id: id };
    let forge = Method::CreateNodeIfAbsent {
        node_id: "policy-forged".into(),
        properties_msgpack: msgpack(serde_json::Value::Object(row)),
    };
    for (tag, method) in [("edit", edit), ("remove", remove), ("forge", forge)] {
        let refused = check(&temp.shard, tag, method).unwrap_err();
        assert!(
            refused.starts_with("POLICY_NATIVE_AUTHORITY_REQUIRED"),
            "{tag}: {refused}"
        );
    }
}
