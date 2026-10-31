//! Cohesive receipt-property and run-event identity validation for the
//! parent outcome-bundle wire contract.

use super::{CommitOutcomeBundle, ReceiptNode, RunEvent};

fn validate_receipt_reference_properties(
    bundle: &CommitOutcomeBundle,
    node: &ReceiptNode,
    properties: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<(), String> {
    let kind = properties
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("receipt node '{}' is missing 'kind'", node.node_id))?;
    if kind != super::receipt_kind_name(node.kind) {
        return Err(format!("receipt node '{}' kind is not bound", node.node_id));
    }
    let fence = properties
        .get("fence_token")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("receipt node '{}' is missing 'fence_token'", node.node_id))?;
    if fence != bundle.fence_token {
        return Err(format!(
            "receipt node '{}' fence is not bound",
            node.node_id
        ));
    }
    validate_optional_string_property(
        node,
        properties,
        "result_ref",
        bundle.result_ref.as_deref(),
    )?;
    validate_optional_string_property(
        node,
        properties,
        "result_digest",
        bundle.result_digest.as_deref(),
    )
}

/// A receipt property bound to an optional bundle string: present on the node,
/// and either the same string or JSON null when the bundle carries none.
fn validate_optional_string_property(
    node: &ReceiptNode,
    properties: &std::collections::BTreeMap<String, serde_json::Value>,
    name: &str,
    expected: Option<&str>,
) -> Result<(), String> {
    let actual = properties
        .get(name)
        .ok_or_else(|| format!("receipt node '{}' is missing '{name}'", node.node_id))?;
    match (expected, actual) {
        (Some(expected), serde_json::Value::String(actual)) if actual.as_str() == expected => {
            Ok(())
        }
        (None, serde_json::Value::Null) => Ok(()),
        _ => Err(format!(
            "receipt node '{}' {name} is not bound",
            node.node_id
        )),
    }
}

fn validate_receipt_completion_properties(
    bundle: &CommitOutcomeBundle,
    node: &ReceiptNode,
    properties: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<(), String> {
    let event_sequence = properties
        .get("event_sequence")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            format!(
                "receipt node '{}' is missing 'event_sequence'",
                node.node_id
            )
        })?;
    if event_sequence != bundle.event_sequence {
        return Err(format!(
            "receipt node '{}' event_sequence is not bound",
            node.node_id
        ));
    }
    let completeness = properties
        .get("completeness")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("receipt node '{}' is missing 'completeness'", node.node_id))?;
    if completeness != super::completeness_name(bundle.completeness) {
        return Err(format!(
            "receipt node '{}' completeness is not bound",
            node.node_id
        ));
    }
    let missing_refs = properties
        .get("missing_refs")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("receipt node '{}' is missing 'missing_refs'", node.node_id))?;
    let expected_missing_refs: Vec<serde_json::Value> = bundle
        .missing_refs
        .iter()
        .cloned()
        .map(serde_json::Value::String)
        .collect();
    if missing_refs.as_slice() != expected_missing_refs.as_slice() {
        return Err(format!(
            "receipt node '{}' missing_refs are not bound",
            node.node_id
        ));
    }
    if let Some(caller) = properties.get("caller") {
        let empty = caller.is_null()
            || caller.as_str().is_some_and(|value| value.trim().is_empty())
            || caller.as_object().is_some_and(serde_json::Map::is_empty);
        if empty {
            return Err(format!(
                "receipt node '{}' has an empty caller binding",
                node.node_id
            ));
        }
    }
    Ok(())
}

/// Receipt node bytes are durable graph payloads, so the binding must survive
/// after the extension object is gone.  Require a compact MessagePack map that
/// carries every authority and execution-currency identity alongside the
/// caller payload; a free-form `{}` or an empty `caller` object cannot satisfy
/// the receipt contract.
pub(super) fn validate_receipt_properties(
    bundle: &CommitOutcomeBundle,
    node: &ReceiptNode,
) -> Result<(), String> {
    let properties: std::collections::BTreeMap<String, serde_json::Value> =
        rmp_serde::from_slice(&node.properties_msgpack).map_err(|_| {
            format!(
                "receipt node '{}' properties must be a MessagePack map",
                node.node_id
            )
        })?;
    for (field, expected) in [
        ("node_id", node.node_id.as_str()),
        ("delegation_id", bundle.delegation_id.as_str()),
        ("delegator_id", bundle.delegator_id.as_str()),
        ("selected_agent_id", bundle.selected_agent_id.as_str()),
        ("executor_lease_actor", bundle.executor_lease_actor.as_str()),
        ("outcome", bundle.outcome.as_str()),
        ("work_item_id", bundle.work_item_id.as_str()),
        ("run_id", bundle.run_id.as_str()),
        ("outbox_id", bundle.outbox_id.as_str()),
        ("payload_ref", node.payload_ref.as_str()),
        ("capability_digest", bundle.capability_digest.as_str()),
        ("catalog_digest", bundle.catalog_digest.as_str()),
        ("policy_digest", bundle.policy_digest.as_str()),
        ("model_digest", bundle.model_digest.as_str()),
    ] {
        let actual = properties
            .get(field)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("receipt node '{}' is missing '{field}'", node.node_id))?;
        if actual != expected {
            return Err(format!(
                "receipt node '{}' property '{field}' is not bound to the terminal bundle",
                node.node_id
            ));
        }
    }
    validate_receipt_reference_properties(bundle, node, &properties)?;
    validate_receipt_completion_properties(bundle, node, &properties)?;
    Ok(())
}

fn event_execution_identity_matches_bundle(event: &RunEvent, bundle: &CommitOutcomeBundle) -> bool {
    event.delegation_id == bundle.delegation_id
        && event.delegator_id == bundle.delegator_id
        && event.selected_agent_id == bundle.selected_agent_id
        && event.executor_lease_actor == bundle.executor_lease_actor
        && event.outcome == bundle.outcome
        && event.work_item_id == bundle.work_item_id
        && event.run_id == bundle.run_id
        && event.fence_token == bundle.fence_token
        && event.outbox_id == bundle.outbox_id
}

fn event_result_provenance_matches_bundle(event: &RunEvent, bundle: &CommitOutcomeBundle) -> bool {
    event.result_ref == bundle.result_ref
        && event.capability_digest == bundle.capability_digest
        && event.catalog_digest == bundle.catalog_digest
        && event.policy_digest == bundle.policy_digest
        && event.model_digest == bundle.model_digest
        && event.event_sequence == bundle.event_sequence
        && event.completeness == bundle.completeness
        && event.missing_refs == bundle.missing_refs
}

pub(super) fn event_identity_matches_bundle(
    event: &RunEvent,
    bundle: &CommitOutcomeBundle,
) -> bool {
    event_execution_identity_matches_bundle(event, bundle)
        && event_result_provenance_matches_bundle(event, bundle)
}
