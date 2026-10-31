//! Census: every capability row declared `Stability::Internal` is refused at
//! the public boundary. The capability row is the one source; this test fails
//! when an internal row has no fixture here, so a new internal method cannot
//! skip the check.

use super::*;

/// One wire-shaped instance per internal method id.
fn fixture(id: &str) -> Option<Method> {
    let json = match id {
        "PolicyEvolutionStore" => serde_json::json!({"method": "PolicyEvolutionStore", "params": {"request": {
            "record_id": "polver:x", "tenant_id": "tenant-shared", "recorded_by": "p",
            "recorded_at_ms": 1,
            "record": {"kind": "model_policy_version", "record": {
                "checkpoint_digest": "11".repeat(32), "tokenizer_digest": "22".repeat(32),
                "artifact_ref": "artifacts:base", "origin": {"origin": "base"}}},
        }}}),
        _ => return None,
    };
    Some(serde_json::from_value(json).expect("fixture decodes"))
}

#[tokio::test]
async fn every_internal_capability_is_refused_at_the_public_boundary() {
    let state = test_state();
    let ids: Vec<&str> = medulla_capabilities::engine_internal_method_ids().collect();
    assert!(!ids.is_empty(), "the census must see the internal rows");
    for (n, id) in ids.into_iter().enumerate() {
        let method = fixture(id)
            .unwrap_or_else(|| panic!("internal method {id} has no boundary fixture here"));
        // The carrier half (`ChangeEnvelope::validate`) agrees with the row.
        assert!(!method.is_wire_callable(), "{id} must not be wire-callable");
        let resp = dispatch_on_heap(&state, request(900 + n as u64, "__commons__", None, method)).await;
        assert_eq!(
            resp.error.as_deref(),
            Some("ENGINE_INTERNAL_METHOD"),
            "{id} must be refused at the public boundary: {:?}",
            resp.error_detail
        );
    }
}
