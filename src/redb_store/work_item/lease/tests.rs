//! Fenced metadata writes use the mutation clock inside the durable row boundary.
use super::super::test_shard::{open, with_nodes, GRAPH};
use super::*;
use crate::epistemic_operations_ext::{
    CasWorkItemMetadataLeaseFence, CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
    CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
};

fn request(fenced: bool) -> CasWorkItemMetadataRequest {
    CasWorkItemMetadataRequest {
        schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
        tenant_ref: "tenant-a".into(),
        work_item_id: "work-a".into(),
        expected_lease: fenced.then(|| CasWorkItemMetadataLeaseFence {
            worker_ref: "worker-a".into(),
            lease_epoch: 2,
            fencing_token: 3,
        }),
        expected_status: vec!["running".into()],
        expected_checkpoint_id: None,
        set_checkpoint_id: None,
        expected_metadata_msgpack: None,
        set_metadata_msgpack: None,
        expected_prio_bucket: Some(0),
        set_prio_bucket: Some(7),
        // A stale caller clock must not revive an expired worker fence.
        now_ms: 1,
    }
}

// spec: EG-DURABLE-KERNEL-R028.3
#[test]
fn fenced_metadata_cas_uses_mutation_time_and_preserves_refused_rows() {
    for (now_ms, expiry, fenced, applied) in [
        (999, Some(1.0), true, true),
        (1000, Some(1.0), true, false),
        (1001, Some(1.0), true, false),
        (1000, None, true, false),
        (1001, Some(1.0), false, true),
        (1000, None, false, true),
    ] {
        let temp = open("metadata-expiry");
        let mut props = serde_json::json!({
            "tenant": "tenant-a", "status": "running", "lease_owner": "worker-a",
            "lease_epoch": 2, "fencing_token": 3, "prio_bucket": 0
        })
        .as_object()
        .unwrap()
        .clone();
        if let Some(expiry) = expiry {
            props.insert("lease_expires_at".into(), serde_json::json!(expiry));
        }
        with_nodes(&temp.shard, "seed", |nodes| {
            write_work_item_props(nodes, GRAPH, "work-a", &mut props, DurableCrypto::none())
        });
        let before = with_nodes(&temp.shard, "before", |nodes| {
            Ok(nodes.get((GRAPH, "work-a"))?.unwrap().value().to_vec())
        });
        let payload = with_nodes(&temp.shard, "cas", |nodes| {
            apply_cas_work_item_metadata_row(
                GRAPH,
                &request(fenced),
                now_ms,
                nodes,
                DurableCrypto::none(),
            )
        })
        .unwrap();
        let crate::protocol::ResultPayload::Json(value) = payload else {
            panic!("expected JSON result")
        };
        let result: CasWorkItemMetadataResult = serde_json::from_value(value).unwrap();
        let after = with_nodes(&temp.shard, "after", |nodes| {
            Ok(nodes.get((GRAPH, "work-a"))?.unwrap().value().to_vec())
        });
        if applied {
            assert_eq!(result.outcome, CasWorkItemMetadataOutcome::Applied);
            assert_eq!(result.changed_work_item_ids, ["work-a"]);
            assert_ne!(before, after);
        } else {
            assert_eq!(result.outcome, CasWorkItemMetadataOutcome::Conflict);
            assert!(result.changed_work_item_ids.is_empty());
            assert_eq!(before, after);
        }
    }
}
