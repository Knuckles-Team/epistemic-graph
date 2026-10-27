use super::*;
use eg_types::inline_history::NativeClaimWorkItemResult as ClaimWorkItemResult;

fn body(marker: Option<bool>) -> serde_json::Value {
    let mut value = serde_json::json!({
        "schema_version":"1", "claimed":true, "reason":"claimed",
        "work_item_id":"work-item", "kind":"run", "payload_ref":"input",
        "lease_holder_ref":"worker", "lease_epoch":1, "fencing_token":1,
        "lease_expires_at_ms":2000, "attempt":1, "max_attempts":3,
        "tenant_in_flight":1, "changed_work_item_ids":["work-item"]
    });
    if let Some(marker) = marker {
        value["fresh_execution_grant"] = marker.into();
    }
    value
}

fn decode(result: ResultPayload) -> ClaimWorkItemResult {
    match result {
        ResultPayload::Raw(bytes) => rmp_serde::from_slice(&bytes).unwrap(),
        ResultPayload::Json(value) => serde_json::from_value(value).unwrap(),
        _ => panic!("expected typed claim result"),
    }
}

// spec: EG-DURABLE-KERNEL-R028.4.7
#[test]
fn replay_clears_profile_grant_without_changing_lease_identity() {
    for marker in [true, false] {
        let original: ClaimWorkItemResult = serde_json::from_value(body(Some(marker))).unwrap();
        for result in [
            ResultPayload::Json(serde_json::to_value(&original).unwrap()),
            ResultPayload::of::<eg_types::result_contract::coordination::ClaimWorkItem>(
                original.clone(),
            )
            .unwrap(),
        ] {
            let replay = decode(project_claim_result(result, true, true).unwrap());
            let mut expected = original.clone();
            expected.fresh_execution_grant = Some(false);
            assert_eq!(replay, expected);
        }
    }
}

#[test]
fn legacy_claim_replay_does_not_acquire_profile_fields() {
    let original = body(None);
    let replay =
        decode(project_claim_result(ResultPayload::Json(original.clone()), true, false).unwrap());
    assert_eq!(serde_json::to_value(&replay).unwrap(), original);
    assert_eq!(replay.fresh_execution_grant, None);
}

#[test]
fn malformed_claim_replay_cannot_be_an_execution_grant() {
    let mut invalid = body(Some(true));
    invalid["fresh_execution_grant"] = "true".into();
    assert!(project_claim_result(ResultPayload::Json(invalid), true, true).is_err());
}

#[test]
fn pre_opt_in_profile_receipt_cannot_leak_a_freshness_witness() {
    for marker in [true, false] {
        for replayed in [true, false] {
            let result = ResultPayload::Json(body(Some(marker)));
            let error = project_claim_result(result, replayed, false).unwrap_err();
            assert!(error.starts_with("RUN_ADMISSION_UNAVAILABLE:"));
        }
    }
}

#[test]
fn explicit_opt_in_is_part_of_claim_identity_without_changing_legacy_shape() {
    let request: eg_types::epistemic_operations::ClaimWorkItemRequest =
        serde_json::from_value(serde_json::json!({
            "schema_version":"1", "tenant_ref":"tenant", "work_item_id":"item",
            "queue_ref":null, "resource_class":null, "fairness_group":null,
            "worker_ref":"worker", "now_ms":1000, "lease_ms":60000,
            "max_tenant_in_flight":64
        }))
        .unwrap();
    #[derive(serde::Serialize)]
    #[serde(tag = "method", content = "params")]
    enum OldClaim {
        ClaimWorkItem {
            request: eg_types::epistemic_operations::ClaimWorkItemRequest,
        },
    }
    let old = OldClaim::ClaimWorkItem {
        request: request.clone(),
    };
    let legacy = Method::ClaimWorkItem {
        request,
        completed_text_history: false,
    };
    let legacy_json = serde_json::to_value(&legacy).unwrap();
    assert_eq!(legacy_json, serde_json::to_value(&old).unwrap());
    assert_eq!(
        rmp_serde::to_vec_named(&legacy).unwrap(),
        rmp_serde::to_vec_named(&old).unwrap()
    );
    assert!(legacy_json["params"]
        .get("completed_text_history")
        .is_none());
    let mut opted_in = legacy.clone();
    if let Method::ClaimWorkItem {
        completed_text_history,
        ..
    } = &mut opted_in
    {
        *completed_text_history = true;
    }
    assert_ne!(
        super::super::super::digest::opaque_request_key("work", "graph", 1, &legacy),
        super::super::super::digest::opaque_request_key("work", "graph", 1, &opted_in),
    );
    let (legacy_id, _) =
        derive_work_item_identity("graph", "tenant", 1, Some("retry"), &legacy).unwrap();
    let (opted_in_id, _) =
        derive_work_item_identity("graph", "tenant", 1, Some("retry"), &opted_in).unwrap();
    assert_ne!(legacy_id.batch_id, opted_in_id.batch_id);
}
