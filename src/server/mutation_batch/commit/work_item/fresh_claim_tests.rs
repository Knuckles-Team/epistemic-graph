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
            let replay = decode(mark_claim_replayed(result).unwrap());
            let mut expected = original.clone();
            expected.fresh_execution_grant = Some(false);
            assert_eq!(replay, expected);
        }
    }
}

#[test]
fn legacy_claim_replay_does_not_acquire_profile_fields() {
    let original = body(None);
    let replay = decode(mark_claim_replayed(ResultPayload::Json(original.clone())).unwrap());
    assert_eq!(serde_json::to_value(&replay).unwrap(), original);
    assert_eq!(replay.fresh_execution_grant, None);
}

#[test]
fn malformed_claim_replay_cannot_be_an_execution_grant() {
    let mut invalid = body(Some(true));
    invalid["fresh_execution_grant"] = "true".into();
    assert!(mark_claim_replayed(ResultPayload::Json(invalid)).is_err());
}
