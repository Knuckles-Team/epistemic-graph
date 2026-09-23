//! Policy-evolution records as request-graph nodes.
//!
//! One node per record, keyed by the record's content-addressed id. The node
//! carries the tenant, the server-stamped writer and time, and the typed
//! record. Decoding re-derives the id from the stored body: a node whose body
//! no longer hashes to its id -- a generic graph write edited it -- is refused
//! as tampered, so an immutable record can be proven unchanged on every read.

use eg_types::policy_evolution::{PolicyEvolutionRecord, PolicyRecordView, PolicyRefusal};
use serde::Deserialize;

use super::gate::RecordLookup;
use crate::graph::GraphCore;

/// `node_type` of every policy-evolution record node.
const RECORD_NODE_TYPE: &str = "PolicyEvolutionRecord";
/// The `type` marker `StartTrajectory` stamps on a trajectory node.
const TRAJECTORY_NODE_TYPE: &str = "Trajectory";

/// Who wrote a record, and when, as the verified context and the
/// authoritative clock say.
pub(super) struct RecordStamp<'a> {
    pub(super) tenant_id: &'a str,
    pub(super) recorded_by: String,
    pub(super) recorded_at_ms: u64,
}

/// The complete property blob of one record node.
pub(super) fn record_properties(
    record_id: &str,
    record: &PolicyEvolutionRecord,
    stamp: &RecordStamp<'_>,
) -> Result<Vec<u8>, String> {
    let properties = serde_json::json!({
        "node_type": RECORD_NODE_TYPE,
        "tenant_id": stamp.tenant_id,
        "record_id": record_id,
        "recorded_by": stamp.recorded_by,
        "recorded_at_ms": stamp.recorded_at_ms,
        "record": record,
    });
    rmp_serde::to_vec_named(&properties)
        .map_err(|error| format!("policy record encoding failed: {error}"))
}

#[derive(Deserialize)]
struct StoredNode {
    node_type: String,
    tenant_id: String,
    recorded_by: String,
    recorded_at_ms: u64,
    record: serde_json::Value,
}

/// Decode one node as `tenant_id`'s record `record_id`.
///
/// `Ok(None)` for anything that is not this tenant's policy record; a
/// policy-record node whose body is undecodable or no longer hashes to its id
/// is [`PolicyRefusal::RecordTampered`].
pub(super) fn decode_view(
    record_id: &str,
    properties_msgpack: &[u8],
    tenant_id: &str,
) -> Result<Option<PolicyRecordView>, PolicyRefusal> {
    let tampered = || PolicyRefusal::RecordTampered(record_id.to_string());
    let Ok(value) = eg_types::msgpack::decode_property_value(properties_msgpack) else {
        return Ok(None);
    };
    let Ok(node) = serde_json::from_value::<StoredNode>(value) else {
        return Ok(None);
    };
    if node.node_type != RECORD_NODE_TYPE || node.tenant_id != tenant_id {
        return Ok(None);
    }
    let record: PolicyEvolutionRecord =
        serde_json::from_value(node.record).map_err(|_| tampered())?;
    if record.record_id(tenant_id).ok().as_deref() != Some(record_id) {
        return Err(tampered());
    }
    Ok(Some(PolicyRecordView {
        record_id: record_id.to_string(),
        recorded_by: node.recorded_by,
        recorded_at_ms: node.recorded_at_ms,
        record,
    }))
}

/// The committed request-graph image one tenant's admission reads.
pub(super) struct GraphRecords<'a> {
    pub(super) core: Option<&'a GraphCore>,
    pub(super) tenant_id: &'a str,
}

impl GraphRecords<'_> {
    /// The verified view of `record_id`, `None` when absent.
    pub(super) fn view(&self, record_id: &str) -> Result<Option<PolicyRecordView>, PolicyRefusal> {
        let Some(properties) = self
            .core
            .and_then(|core| core.get_node_properties(record_id))
        else {
            return Ok(None);
        };
        decode_view(record_id, &properties, self.tenant_id)
    }
}

impl RecordLookup for GraphRecords<'_> {
    fn record(&self, record_id: &str) -> Result<Option<PolicyEvolutionRecord>, PolicyRefusal> {
        Ok(self.view(record_id)?.map(|view| view.record))
    }

    fn trajectory_steps(&self, trajectory_id: &str) -> Option<u64> {
        let properties = self.core?.get_node_properties(trajectory_id)?;
        let value = eg_types::msgpack::decode_property_value(&properties).ok()?;
        (value.get("type")?.as_str()? == TRAJECTORY_NODE_TYPE)
            .then(|| value.get("step_count")?.as_u64())
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::policy_evolution::ModelPolicyVersion;

    fn version() -> PolicyEvolutionRecord {
        let record: ModelPolicyVersion = serde_json::from_value(serde_json::json!({
            "checkpoint_digest": "11".repeat(32), "tokenizer_digest": "22".repeat(32),
            "artifact_ref": "artifacts:base", "origin": {"origin": "base"},
        }))
        .unwrap();
        PolicyEvolutionRecord::ModelPolicyVersion { record }
    }

    fn stamp() -> RecordStamp<'static> {
        RecordStamp {
            tenant_id: "tenant-a",
            recorded_by: "principal:sha256:ab".to_string(),
            recorded_at_ms: 42,
        }
    }

    #[test]
    fn a_record_round_trips_through_its_node_and_stays_tenant_scoped() {
        let record = version();
        let id = record.record_id("tenant-a").unwrap();
        let blob = record_properties(&id, &record, &stamp()).unwrap();
        let view = decode_view(&id, &blob, "tenant-a").unwrap().unwrap();
        assert_eq!(view.record, record);
        assert_eq!(view.recorded_at_ms, 42);
        assert_eq!(decode_view(&id, &blob, "tenant-b"), Ok(None));
    }

    #[test]
    fn an_edited_record_is_refused_as_tampered() {
        let record = version();
        let id = record.record_id("tenant-a").unwrap();
        let blob = record_properties(&id, &record, &stamp()).unwrap();
        let mut node = eg_types::msgpack::decode_property_value(&blob).unwrap();
        node["record"]["record"]["artifact_ref"] = serde_json::json!("artifacts:swapped");
        let edited = rmp_serde::to_vec_named(&node).unwrap();
        assert_eq!(
            decode_view(&id, &edited, "tenant-a").unwrap_err().code(),
            "POLICY_RECORD_TAMPERED"
        );
        // A node under someone else's id never verifies either.
        let other = format!("polver:{}", "00".repeat(32));
        assert!(decode_view(&other, &blob, "tenant-a").is_err());
    }
}
